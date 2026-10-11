//! Slave states: the runtime state machine and what is persisted (DESIGN.md §16.3).

use std::time::SystemTime;

use crate::error::{Error, Result};
use crate::store::bootstrap::{NODE_BOOTSTRAPPING, NODE_READY};
use crate::types::{DbIdentity, Seq};
use crate::verify::VerifyReport;

/// Why a slave stopped advancing. Reads keep working while halted. More reasons may be added,
/// hence `non_exhaustive`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum HaltReason {
    // Data-related: persisted, survive restart, cleared only by `clear_halt`.
    IdentityChanged,
    Diverged,
    LogGap,
    Corrupt,
    SpecMismatch,
    // Configuration-related: not persisted, re-evaluated on restart.
    ProtocolMismatch,
    EnvelopeMismatch,
    AuthRejected,
    LoopDetected,
}

const ALL: [HaltReason; 9] = [
    HaltReason::IdentityChanged,
    HaltReason::Diverged,
    HaltReason::LogGap,
    HaltReason::Corrupt,
    HaltReason::SpecMismatch,
    HaltReason::ProtocolMismatch,
    HaltReason::EnvelopeMismatch,
    HaltReason::AuthRejected,
    HaltReason::LoopDetected,
];

impl HaltReason {
    /// True for data-related halts, which survive a restart.
    pub fn is_persisted(self) -> bool {
        matches!(
            self,
            HaltReason::IdentityChanged
                | HaltReason::Diverged
                | HaltReason::LogGap
                | HaltReason::Corrupt
                | HaltReason::SpecMismatch
        )
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            HaltReason::IdentityChanged => "IdentityChanged",
            HaltReason::Diverged => "Diverged",
            HaltReason::LogGap => "LogGap",
            HaltReason::Corrupt => "Corrupt",
            HaltReason::SpecMismatch => "SpecMismatch",
            HaltReason::ProtocolMismatch => "ProtocolMismatch",
            HaltReason::EnvelopeMismatch => "EnvelopeMismatch",
            HaltReason::AuthRejected => "AuthRejected",
            HaltReason::LoopDetected => "LoopDetected",
        }
    }

    pub(crate) fn from_name(name: &str) -> Option<HaltReason> {
        ALL.into_iter().find(|r| r.name() == name)
    }
}

/// Runtime state of a slave. More states may be added (verification in Plan 3), hence
/// `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum SlaveState {
    Connecting,
    Bootstrapping { target_seq: Seq, records: u64 },
    Streaming,
    Halted(HaltReason),
}

/// Operator action for [`Slave::clear_halt`](crate::Slave::clear_halt).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaltAction {
    /// Handshake again with the current data (after fixing the cause).
    Retry,
    /// Discard local data and bootstrap from the upstream. Discarding data takes an explicit
    /// [`AcceptDataLoss`](crate::AcceptDataLoss), written out by the caller:
    /// `HaltAction::Rebootstrap { accept_loss: AcceptDataLoss }`.
    Rebootstrap {
        accept_loss: crate::slave::AcceptDataLoss,
    },
}

/// `__meta.node_state`: `ready`, `bootstrapping` or `halted:<Reason>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeState {
    Ready,
    Bootstrapping,
    Halted(HaltReason),
}

impl NodeState {
    pub(crate) fn encode(self) -> Vec<u8> {
        match self {
            NodeState::Ready => NODE_READY.to_vec(),
            NodeState::Bootstrapping => NODE_BOOTSTRAPPING.to_vec(),
            NodeState::Halted(r) => {
                debug_assert!(r.is_persisted(), "{r:?} is never persisted");
                format!("halted:{}", r.name()).into_bytes()
            }
        }
    }

    /// `None` (key absent) is `Ready`.
    pub(crate) fn decode(raw: Option<&[u8]>) -> Result<NodeState> {
        let Some(raw) = raw else {
            return Ok(NodeState::Ready);
        };
        if raw == NODE_READY {
            return Ok(NodeState::Ready);
        }
        if raw == NODE_BOOTSTRAPPING {
            return Ok(NodeState::Bootstrapping);
        }
        std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.strip_prefix("halted:"))
            .and_then(HaltReason::from_name)
            .filter(|r| r.is_persisted())
            .map(NodeState::Halted)
            .ok_or_else(|| {
                Error::corrupt(format!(
                    "unknown node_state {:?}",
                    String::from_utf8_lossy(raw)
                ))
            })
    }
}

/// A snapshot of a slave's replication status. More fields may be added, hence `non_exhaustive`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SlaveStatus {
    pub state: SlaveState,
    pub origin_identity: Option<DbIdentity>,
    pub applied_seq: Seq,
    pub first_retained_seq: Seq,
    pub upstream_last_seq: Seq,
    pub origin_last_seq: Seq,
    /// 0 when `applied_seq >= origin_last_seq`. While behind: the age (`now − ts_ms`) of the
    /// oldest entry received but not yet applied. If nothing is received yet, the age of the
    /// last applied entry instead, an upper bound that includes the master's idle gap.
    /// `None` while behind with no timestamp known (after a bootstrap, until the next entry).
    pub lag_ms: Option<u64>,
    /// The address of the current upstream (the target of the latest `Slave::repoint`, if any).
    pub upstream: String,
    /// Node ids from the upstream to the master, closest first.
    pub upstream_chain: Vec<String>,
    pub last_contact: Option<SystemTime>,
    /// The last connection or handshake problem, e.g. `STALE_EPOCH`.
    pub last_error: Option<String>,
    /// What kind of problem `last_error` is; `Some` exactly when `last_error` is. Tells a wrong
    /// address or a wrong endpoint apart from an upstream that is only busy.
    pub last_error_kind: Option<UpstreamErrorKind>,
    /// True after the listener panicked; it is not called again until restart.
    pub listener_failed: bool,
    /// The last verify report (scheduled or `verify_now`), if any run produced one.
    pub last_verify: Option<VerifyReport>,
}

/// What kind of problem [`SlaveStatus::last_error`] describes. The slave retries every one of
/// them with backoff (none halts it): the kind only tells the application what it is waiting
/// for. It describes the last attempt: an endpoint that closes the connection right after a
/// garbage reply can show as `Unreachable` on one attempt and `NotReplicationService` on the
/// next. More kinds may be added, hence `non_exhaustive`.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum UpstreamErrorKind {
    /// The upstream address is invalid (it does not parse, or the TLS settings are unusable).
    /// Only a `repoint` or a reopen with another configuration fixes it.
    Config,
    /// Nothing could be reached, or the connection broke: connection refused, DNS failure,
    /// connect timeout, reset, or no message within the idle timeout. A wrong address, a
    /// firewall, or an upstream that is down or restarting.
    Unreachable,
    /// TLS failed: a handshake or certificate error, or a TLS client talking to a plaintext
    /// endpoint.
    Tls,
    /// Something answered that is not a replication server of this crate: it does not speak
    /// HTTP/2 (an HTTP/1 server, another protocol, a plaintext client talking to a TLS port),
    /// or it is a gRPC server without this service. Most likely a wrong address or port.
    NotReplicationService,
    /// The replication server answered but this slave cannot go on with it yet: it asked to
    /// retry (`NOT_READY`, `BUSY`, `STALE_EPOCH`, ...), or sent something unusable.
    Upstream,
    /// A local problem, for example a failed write to the local store.
    Local,
}

/// Where a slave stood when [`Slave::wait_streaming`](crate::Slave::wait_streaming) or
/// [`Slave::wait_for_seq`](crate::Slave::wait_for_seq) gave up ([`Error::WaitTimeout`]).
/// A timeout is not a failure by itself: `progressed` tells a slave that is still moving (a
/// long bootstrap or catch-up: wait again) from one that is stuck, and
/// `status.last_error_kind` says on what (a wrong address shows as `Unreachable` or
/// `NotReplicationService`, with `status.last_contact == None` if it never reached anything).
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct WaitProgress {
    /// The seq `wait_for_seq` waited for; `None` for `wait_streaming`.
    pub target_seq: Option<Seq>,
    /// The slave applied entries, or received bootstrap records, during the wait.
    pub progressed: bool,
    /// The slave's status when the wait gave up.
    pub status: SlaveStatus,
}

impl WaitProgress {
    /// What counts as progress: the applied seq and, while bootstrapping, the records received.
    pub(crate) fn marker(status: &SlaveStatus) -> (Seq, u64) {
        let records = match status.state {
            SlaveState::Bootstrapping { records, .. } => records,
            _ => 0,
        };
        (status.applied_seq, records)
    }
}

impl std::fmt::Display for WaitProgress {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let s = &self.status;
        match self.target_seq {
            Some(t) => write!(f, "timed out waiting for seq {t}")?,
            None => f.write_str("timed out waiting for streaming")?,
        }
        write!(f, " (state {:?}, applied seq {}", s.state, s.applied_seq)?;
        f.write_str(if self.progressed {
            ", progressing"
        } else {
            ", no progress"
        })?;
        if s.last_contact.is_none() {
            f.write_str(", upstream never reached")?;
        }
        if let (Some(kind), Some(e)) = (s.last_error_kind, &s.last_error) {
            write!(f, ", last error {kind:?}: {e}")?;
        }
        f.write_str(")")
    }
}

/// A retriable problem: its kind and the message shown in `SlaveStatus::last_error`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Problem {
    pub(crate) kind: UpstreamErrorKind,
    pub(crate) message: String,
}

impl Problem {
    pub(crate) fn new(kind: UpstreamErrorKind, message: impl Into<String>) -> Self {
        Problem {
            kind,
            message: message.into(),
        }
    }

    pub(crate) fn upstream(message: impl Into<String>) -> Self {
        Self::new(UpstreamErrorKind::Upstream, message)
    }

    pub(crate) fn local(message: impl Into<String>) -> Self {
        Self::new(UpstreamErrorKind::Local, message)
    }

    /// Classifies a failed RPC. tonic reports a failed connection as `Unavailable` (or
    /// `Unknown` with a transport error as the source); a peer that does not speak HTTP/2 as
    /// an `h2 protocol error`; a TLS failure as an `InvalidData` I/O error in the source chain;
    /// a gRPC server without this service as `Unimplemented`. The message keeps the code and
    /// adds the innermost cause (`Unavailable: tcp connect error: Connection refused`).
    pub(crate) fn from_status(s: &tonic::Status) -> Self {
        use tonic::Code;
        let source = std::error::Error::source(s);
        let mut root = None;
        let mut cur = source;
        while let Some(e) = cur {
            root = Some(e);
            cur = e.source();
        }
        let mut message = format!("{:?}: {}", s.code(), s.message());
        if let Some(root) = root.map(ToString::to_string)
            && !message.ends_with(&root)
        {
            message = format!("{message}: {root}");
        }
        let transport = matches!(
            s.code(),
            Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled
        ) || (s.code() == Code::Unknown && source.is_some());
        let kind = if s.code() == Code::Unimplemented {
            UpstreamErrorKind::NotReplicationService
        } else if source.is_some_and(crate::admin::is_tls_rejection) {
            UpstreamErrorKind::Tls
        } else if transport && s.message().starts_with("h2 protocol error") {
            UpstreamErrorKind::NotReplicationService
        } else if transport {
            UpstreamErrorKind::Unreachable
        } else {
            UpstreamErrorKind::Upstream
        };
        Problem { kind, message }
    }
}

impl Problem {
    /// Refines a problem from an upstream that has never answered (since the latest repoint).
    /// hyper sometimes reports a peer that does not speak HTTP/2 to the pending call only as
    /// `Cancelled: connection closed` instead of the `h2 protocol error`: the TCP connection
    /// was accepted (a refused one is `Unavailable`), then closed without a valid answer.
    /// From an address that never answered, that is a wrong endpoint, not an outage.
    pub(crate) fn unanswered(mut self, s: &tonic::Status) -> Self {
        if s.code() == tonic::Code::Cancelled && self.message.ends_with("connection closed") {
            self.kind = UpstreamErrorKind::NotReplicationService;
        }
        self
    }
}

impl std::fmt::Display for Problem {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.message)
    }
}

/// Replication lag in ms; see [`SlaveStatus::lag_ms`]. A timestamp of 0 means unknown.
pub(crate) fn lag_ms(
    applied: Seq,
    origin_last: Seq,
    pending_ts: u64,
    last_applied_ts: u64,
    now_ms: u64,
) -> Option<u64> {
    if applied >= origin_last {
        return Some(0);
    }
    let since = if pending_ts > 0 {
        pending_ts
    } else {
        last_applied_ts
    };
    (since > 0).then(|| now_ms.saturating_sub(since))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_state_round_trips_and_only_data_halts_persist() {
        for s in [NodeState::Ready, NodeState::Bootstrapping] {
            assert_eq!(NodeState::decode(Some(&s.encode())).unwrap(), s);
        }
        for r in ALL {
            if r.is_persisted() {
                let encoded = NodeState::Halted(r).encode();
                assert_eq!(
                    NodeState::decode(Some(&encoded)).unwrap(),
                    NodeState::Halted(r)
                );
            } else {
                // encode() debug-asserts on these; a hand-written value must not decode.
                let raw = format!("halted:{}", r.name());
                assert!(
                    NodeState::decode(Some(raw.as_bytes())).is_err(),
                    "{r:?} is never persisted"
                );
            }
        }
        assert_eq!(NodeState::decode(None).unwrap(), NodeState::Ready);
        assert_eq!(
            NodeState::Halted(HaltReason::LogGap).encode(),
            b"halted:LogGap"
        );
        assert!(NodeState::decode(Some(b"halted:Nope")).is_err());
    }

    #[test]
    fn persisted_halts_are_exactly_the_data_related_ones() {
        let persisted: Vec<_> = ALL.into_iter().filter(|r| r.is_persisted()).collect();
        assert_eq!(
            persisted,
            vec![
                HaltReason::IdentityChanged,
                HaltReason::Diverged,
                HaltReason::LogGap,
                HaltReason::Corrupt,
                HaltReason::SpecMismatch
            ]
        );
    }
}

#[cfg(test)]
mod lag_tests {
    use super::lag_ms;

    #[test]
    fn lag_semantics() {
        // lag_ms(applied, origin_last, pending_ts, last_applied_ts, now)
        assert_eq!(lag_ms(10, 10, 0, 0, 5), Some(0), "caught up, no timestamps");
        assert_eq!(lag_ms(10, 10, 900, 100, 5_000), Some(0), "caught up wins");
        assert_eq!(
            lag_ms(10, 12, 3_000, 1_000, 4_000),
            Some(1_000),
            "behind with a received entry: its age, not the last applied one"
        );
        assert_eq!(
            lag_ms(10, 12, 0, 1_000, 4_000),
            Some(3_000),
            "nothing received: upper bound from the last applied entry"
        );
        assert_eq!(
            lag_ms(10, 12, 0, 0, 4_000),
            None,
            "behind, nothing known (after a bootstrap)"
        );
        assert_eq!(lag_ms(10, 12, 5_000, 0, 4_000), Some(0), "skew clamps");
        assert_eq!(lag_ms(10, 12, 0, 5_000, 4_000), Some(0), "skew clamps");
    }
}
