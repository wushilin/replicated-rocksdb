//! Slave states: the runtime state machine and what is persisted (DESIGN.md §16.3).

use std::time::SystemTime;

use crate::error::{Error, Result};
use crate::store::bootstrap::{NODE_BOOTSTRAPPING, NODE_READY};
use crate::types::{DbIdentity, Seq};

/// Why a slave stopped advancing. Reads keep working while halted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
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

/// Runtime state of a slave.
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// Discard local data and bootstrap from the upstream.
    Rebootstrap,
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
        let Some(raw) = raw else { return Ok(NodeState::Ready) };
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
            .ok_or_else(|| Error::corrupt(format!("unknown node_state {:?}", String::from_utf8_lossy(raw))))
    }
}

/// A snapshot of a slave's replication status. More fields will be added (`lag_ms` and
/// `last_verify` in Plan 3), hence `non_exhaustive`.
#[derive(Clone, Debug)]
#[non_exhaustive]
pub struct SlaveStatus {
    pub state: SlaveState,
    pub origin_identity: Option<DbIdentity>,
    pub applied_seq: Seq,
    pub first_retained_seq: Seq,
    pub upstream_last_seq: Seq,
    pub origin_last_seq: Seq,
    /// The address of the current upstream (the target of the latest `Slave::repoint`, if any).
    pub upstream: String,
    /// Node ids from the upstream to the master, closest first.
    pub upstream_chain: Vec<String>,
    pub last_contact: Option<SystemTime>,
    /// The last connection or handshake problem, e.g. `STALE_EPOCH`.
    pub last_error: Option<String>,
    /// True after the listener panicked; it is not called again until restart.
    pub listener_failed: bool,
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
                assert_eq!(NodeState::decode(Some(&encoded)).unwrap(), NodeState::Halted(r));
            } else {
                // encode() debug-asserts on these; a hand-written value must not decode.
                let raw = format!("halted:{}", r.name());
                assert!(NodeState::decode(Some(raw.as_bytes())).is_err(), "{r:?} is never persisted");
            }
        }
        assert_eq!(NodeState::decode(None).unwrap(), NodeState::Ready);
        assert_eq!(NodeState::Halted(HaltReason::LogGap).encode(), b"halted:LogGap");
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
