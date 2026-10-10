//! Operator tooling for failover (addendum §4): node status, comparing nodes, and choosing
//! which replica to promote.
//!
//! # Failover runbook
//!
//! 1. Fence the old master (stop it, or cut it off from clients and replicas).
//! 2. Gather the status of every replica with [`gather`], which also probes the entry CRCs
//!    that [`compare`] checks as a second line of defence ([`node_status`] reads one node).
//! 3. Promote the most advanced compatible one: [`choose_promotion_candidate`], then
//!    [`Slave::promote_checked`](crate::Slave::promote_checked), which refuses if a reachable
//!    peer is later.
//! 4. `Slave::repoint` every other replica at the new master.
//!
//! With `WriteConcern::min_insync_replicas >= 1`, the most advanced in-sync replica holds
//! every acknowledged write.

use std::collections::BTreeMap;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::net::convert::{epochs_from_pb, identity_from_pb};
use crate::pb;
use crate::slave::UpstreamConfig;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

/// What a node reports about itself (`Status` RPC, `Slave::promotion_info`). Fields may be
/// added, hence `non_exhaustive`: build one with [`NodeInfo::new`] and set the fields.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct NodeInfo {
    pub node_id: String,
    pub role: Role,
    /// `None` while the node has no data (fresh, or bootstrapping).
    pub identity: Option<DbIdentity>,
    pub epoch_history: Vec<EpochStart>,
    pub last_seq: Seq,
    pub last_entry_crc: Option<u32>,
    /// `ts_ms` of the entry at `last_seq`; 0 if it is no longer retained.
    pub last_ts_ms: u64,
    /// `ready`, `bootstrapping`, `halted:<Reason>` or `repairing:<seq>` (a repair replays up
    /// to `seq`; the node is not ready and refuses promotion until then, DESIGN.md §12.1).
    pub node_state: String,
    /// The upstream address; `None` on the master.
    pub upstream: Option<String>,
    pub replicas: Vec<ReplicaSummary>,
    /// CRCs of entries this node was asked about (`Status.probe_seq`): seq → `crc32c` of its
    /// entry there, `None` if it does not hold that entry. [`compare`] uses the longer node's
    /// probe at the shorter node's `last_seq`; [`gather`] and `Slave::check_promotion` fill it.
    pub probes: BTreeMap<Seq, Option<u32>>,
    /// The highest epoch number this node started as a master and gave up (`Master::demote`,
    /// or the empty trailing epochs `Slave::rejoin` dropped; 0 if none). Replicas that followed that promotion may hold it, so a
    /// promotion never reuses it (`Slave::promote_checked` chooses a higher epoch).
    pub max_burned_epoch: u64,
}

/// One direct downstream replica, as reported by `Status`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaSummary {
    pub node_id: String,
    pub applied_seq: Seq,
    pub in_sync: bool,
}

impl NodeInfo {
    /// A node without data: no identity, empty history, `last_seq` 0, state `ready`, no
    /// upstream, replicas, probes or burned epoch. Set the public fields to describe a node
    /// (for example to call [`compare`] on information kept elsewhere).
    pub fn new(node_id: impl Into<String>, role: Role) -> NodeInfo {
        NodeInfo {
            node_id: node_id.into(),
            role,
            identity: None,
            epoch_history: Vec::new(),
            last_seq: 0,
            last_entry_crc: None,
            last_ts_ms: 0,
            node_state: "ready".into(),
            upstream: None,
            replicas: Vec::new(),
            probes: BTreeMap::new(),
            max_burned_epoch: 0,
        }
    }

    pub(crate) fn from_pb(p: pb::StatusResponse, probe_seq: Option<Seq>) -> Result<NodeInfo> {
        let role = match p.role.as_str() {
            "master" => Role::Master,
            "slave" => Role::Slave,
            other => return Err(Error::corrupt(format!("unknown role {other:?} in Status"))),
        };
        Ok(NodeInfo {
            node_id: p.node_id,
            role,
            identity: p.identity.as_ref().map(identity_from_pb).transpose()?,
            epoch_history: epochs_from_pb(&p.epoch_history),
            last_seq: p.last_seq,
            last_entry_crc: p.last_entry_crc,
            last_ts_ms: p.last_ts_ms,
            node_state: p.node_state,
            upstream: Some(p.upstream).filter(|u| !u.is_empty()),
            replicas: p
                .replicas
                .into_iter()
                .map(|r| ReplicaSummary {
                    node_id: r.node_id,
                    applied_seq: r.applied_seq,
                    in_sync: r.in_sync,
                })
                .collect(),
            probes: probe_seq
                .map(|seq| (seq, p.probe_crc))
                .into_iter()
                .collect(),
            max_burned_epoch: p.max_burned_epoch,
        })
    }

    /// The highest epoch number this node knows of: its identity, every history record, and
    /// the epoch it burned by demoting. A new epoch must be above it.
    pub fn max_epoch_seen(&self) -> u64 {
        let history = self
            .epoch_history
            .iter()
            .map(|e| e.epoch)
            .max()
            .unwrap_or(0);
        history
            .max(self.identity.map_or(0, |id| id.epoch))
            .max(self.max_burned_epoch)
    }

    fn newest(&self) -> (u64, Seq) {
        (
            self.epoch_history.last().map_or(0, |e| e.epoch),
            self.last_seq,
        )
    }
}

/// Which side of a comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    A,
    B,
}

/// How two nodes' data relate. More outcomes may be added, hence `non_exhaustive`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Comparison {
    /// Compatible histories; this side holds everything the other holds, and more.
    Later(Side),
    Equal,
    /// Both hold different entries from `fork` on: the epoch histories (nonces included) fork
    /// below both positions, or a CRC mismatch was proven.
    Diverged {
        fork: Seq,
    },
    /// Different databases (different `db_uuid`).
    DifferentDatabase,
}

/// The first seq whose epoch differs between two histories, if any. Records are compared with
/// their nonces: the same epoch number with another nonce is another epoch.
pub(crate) fn fork_between(a: &[EpochStart], b: &[EpochStart]) -> Option<Seq> {
    for i in 0..a.len().max(b.len()) {
        match (a.get(i), b.get(i)) {
            (Some(x), Some(y)) if x == y => continue,
            (Some(x), Some(y)) => return Some(x.start_seq.min(y.start_seq)),
            (Some(x), None) | (None, Some(x)) => return Some(x.start_seq),
            (None, None) => break,
        }
    }
    None
}

/// Compares two nodes of a topology (addendum §4). A node without data is behind any node
/// with data; different `db_uuid`s are `DifferentDatabase`.
///
/// Otherwise the epoch histories decide (addendum §7): every epoch has a random nonce and
/// exactly one writer process, so two nodes whose histories (nonces included) agree below
/// both positions hold the same entries up to the shorter node's `last_seq`, and two whose
/// histories differ below both positions are `Diverged` at the first differing record. Entry
/// CRCs are a second line of defence only: equal `last_seq`s with different
/// `last_entry_crc`s, or the longer node's probe at the shorter node's `last_seq`
/// ([`NodeInfo::probes`]) differing from it, is `Diverged` at that seq; a missing CRC or
/// probe never makes nodes incomparable. The later of two compatible nodes has the newer
/// epoch, then the higher `last_seq`: a node whose newest epoch is still empty (a promotion
/// that took no writes) is later than a node of the older epoch that holds *fewer* entries
/// than its epoch's start, and **diverged** from one that holds entries at or past that start
/// (`fork` is that epoch's `start_seq`; demoting the empty-epoch node and promoting the other
/// would lose nothing).
pub fn compare(a: &NodeInfo, b: &NodeInfo) -> Comparison {
    let (ia, ib) = match (a.identity, b.identity) {
        (None, None) => return Comparison::Equal,
        (None, Some(_)) => return Comparison::Later(Side::B),
        (Some(_), None) => return Comparison::Later(Side::A),
        (Some(ia), Some(ib)) => (ia, ib),
    };
    if ia.db_uuid != ib.db_uuid {
        return Comparison::DifferentDatabase;
    }
    if let Some(fork) = fork_between(&a.epoch_history, &b.epoch_history) {
        if a.last_seq.min(b.last_seq) >= fork {
            return Comparison::Diverged { fork };
        }
    }
    // The side that is later by position: newest epoch first, then `last_seq`.
    let by_position = match a.newest().cmp(&b.newest()) {
        std::cmp::Ordering::Greater => Some(Side::A),
        std::cmp::Ordering::Less => Some(Side::B),
        std::cmp::Ordering::Equal => None,
    };
    let (short, long, long_side) = match a.last_seq.cmp(&b.last_seq) {
        std::cmp::Ordering::Equal => {
            return match (a.last_entry_crc, b.last_entry_crc) {
                (Some(x), Some(y)) if a.last_seq > 0 && x != y => {
                    Comparison::Diverged { fork: a.last_seq }
                }
                _ => by_position.map_or(Comparison::Equal, Comparison::Later),
            };
        }
        std::cmp::Ordering::Less => (a, b, Side::B),
        std::cmp::Ordering::Greater => (b, a, Side::A),
    };
    // The shorter node has the newer epoch, which is still empty (a promotion that has taken no
    // writes: its newest epoch starts after its `last_seq`). The longer node holding entries
    // from that epoch's start on holds entries the shorter node's epoch will never contain.
    let newest_epoch = |n: &NodeInfo| n.epoch_history.last().copied();
    if let (Some(se), Some(le)) = (newest_epoch(short), newest_epoch(long)) {
        if se.epoch > le.epoch && long.last_seq >= se.start_seq {
            return Comparison::Diverged { fork: se.start_seq };
        }
    }
    // Second line of defence: a CRC mismatch at the shorter node's last entry.
    let held = long.probes.get(&short.last_seq).copied().flatten();
    if let (Some(x), Some(y)) = (short.last_entry_crc, held) {
        if short.last_seq > 0 && x != y {
            return Comparison::Diverged {
                fork: short.last_seq,
            };
        }
    }
    // Compatible: the longer node holds everything the shorter one holds. Order by position:
    // the node with the newer epoch (even an empty one) is later, otherwise the longer one.
    Comparison::Later(by_position.unwrap_or(long_side))
}

/// The node to promote, and how the others relate to it. Fields may be added, hence
/// `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct Choice {
    pub candidate: NodeInfo,
    /// Compatible nodes: behind the candidate or equal to it. They can follow it.
    pub compatible: Vec<String>,
    /// Nodes holding entries the candidate does not (`node_id`, fork seq): repointing them
    /// needs an explicit decision to discard those entries.
    pub diverged: Vec<(String, Seq)>,
    /// Nodes of another database.
    pub foreign: Vec<String>,
    /// Nodes that compare as later than the candidate although the candidate is the latest by
    /// position. This cannot happen with consistent inputs; such a node is not known to be safe
    /// to follow the candidate.
    pub unverifiable: Vec<String>,
    /// Nodes whose state refuses promotion (`node_id`, `node_state`): a repair replaying
    /// (`repairing:<seq>`, DESIGN.md §12.1) or a `Corrupt` / `SpecMismatch` halt. They are
    /// never the candidate, even when they are the most advanced, and are not compared with it.
    pub not_promotable: Vec<(String, String)>,
}

/// Whether a node's `node_state` refuses promotion (`Slave::promote` would refuse it).
fn refuses_promotion(node_state: &str) -> bool {
    node_state.starts_with("repairing:")
        || node_state == "halted:Corrupt"
        || node_state == "halted:SpecMismatch"
}

/// Compatibility is judged by epoch histories (see [`compare`]). A node whose newest epoch is
/// empty and differs from the candidate's (for example a wrongly promoted master with no writes) is not safe to follow the
/// candidate as it is: `demote` it (or rebootstrap it) first.
///
/// Picks the most advanced node holding data whose state allows promotion (newest epoch, then
/// highest `last_seq`, then lowest `node_id`) and classifies the others against it. A node
/// whose state refuses promotion (a repair replaying, a `Corrupt` or `SpecMismatch` halt) is
/// listed in [`Choice::not_promotable`] instead. `Err(Config)` if no such node holds data.
pub fn choose_promotion_candidate(nodes: &[NodeInfo]) -> Result<Choice> {
    let candidate = nodes
        .iter()
        .filter(|n| n.identity.is_some() && !refuses_promotion(&n.node_state))
        .max_by(|x, y| {
            x.newest()
                .cmp(&y.newest())
                .then_with(|| y.node_id.cmp(&x.node_id))
        })
        .ok_or_else(|| Error::Config("no promotable node holds data".into()))?
        .clone();
    let mut choice = Choice {
        candidate,
        compatible: Vec::new(),
        diverged: Vec::new(),
        foreign: Vec::new(),
        unverifiable: Vec::new(),
        not_promotable: Vec::new(),
    };
    for n in nodes
        .iter()
        .filter(|n| n.node_id != choice.candidate.node_id)
    {
        if refuses_promotion(&n.node_state) {
            choice
                .not_promotable
                .push((n.node_id.clone(), n.node_state.clone()));
            continue;
        }
        match compare(&choice.candidate, n) {
            Comparison::Later(Side::A) | Comparison::Equal => {
                choice.compatible.push(n.node_id.clone())
            }
            // The candidate is the latest by position, so this cannot happen; if it ever does,
            // the node is not known to be safe to follow the candidate.
            Comparison::Later(Side::B) => choice.unverifiable.push(n.node_id.clone()),
            Comparison::Diverged { fork } => choice.diverged.push((n.node_id.clone(), fork)),
            Comparison::DifferentDatabase => choice.foreign.push(n.node_id.clone()),
        }
    }
    Ok(choice)
}

/// How long [`node_status`] waits for a peer's answer; a peer that does not answer in time is
/// unreachable.
pub const STATUS_TIMEOUT: Duration = Duration::from_secs(5);

/// Queries a node's `Status` over the network (within [`STATUS_TIMEOUT`]). `client_id`
/// identifies the caller (it must be a valid node identifier); `upstream` carries the
/// address, TLS and credentials. A peer that cannot be reached is
/// `Err(Error::PeerUnreachable { peer, reason, source })`, with a one-line `reason`.
pub async fn node_status(upstream: &UpstreamConfig, client_id: &str) -> Result<NodeInfo> {
    node_status_probed(upstream, client_id, None).await
}

/// [`node_status`], also asking for the CRC of the node's entry at `probe_seq`.
///
/// `Err(PeerUnreachable { peer, reason, source })` when no answer could be had (connect error, the
/// [`STATUS_TIMEOUT`], a transport failure); `Err(Config)` with the status text when the peer
/// answered with an error (permission denied, another protocol, ...) or an undecodable answer,
/// when the call could not be made because of a local configuration error (an invalid
/// address, a TLS configuration that does not build, an invalid `client_id`), or when the TLS
/// handshake was rejected (the peer's certificate does not verify, or it does not speak TLS).
pub async fn node_status_probed(
    upstream: &UpstreamConfig,
    client_id: &str,
    probe_seq: Option<Seq>,
) -> Result<NodeInfo> {
    status_classified(upstream, client_id, probe_seq)
        .await
        .map_err(|f| match f {
            StatusFailure::Unreachable { reason, source } => Error::PeerUnreachable {
                peer: upstream.uri(),
                reason,
                source,
            },
            StatusFailure::Answered(m) => Error::Config(m),
        })
}

/// Why a `Status` call failed: the peer could not be reached, or it was reached and answered
/// with an error (or an undecodable answer), so its state is unknown but it is alive.
///
/// Only a failure that says nothing about the peer is `Unreachable`. A local configuration
/// error (the peer was never contacted) and a rejected TLS handshake are `Answered`, so a
/// consent to skip unreachable peers can never skip a peer that is misconfigured.
pub(crate) enum StatusFailure {
    /// No answer: connect errors, the timeout, a transport failure. `reason` is one short
    /// line; `source` keeps the whole error.
    Unreachable {
        reason: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// An error answer, an undecodable answer, a local configuration error or a rejected TLS
    /// handshake: the text says which.
    Answered(String),
}

impl StatusFailure {
    fn unreachable(e: impl std::error::Error + Send + Sync + 'static) -> StatusFailure {
        StatusFailure::Unreachable {
            reason: short_reason(&e),
            source: Some(Box::new(e)),
        }
    }
}

/// One short line for an operator: the innermost cause of `e` (transport errors wrap the
/// useful message, such as "Connection refused", several levels deep), first line only.
fn short_reason(e: &(dyn std::error::Error + 'static)) -> String {
    let mut root = e;
    while let Some(next) = root.source() {
        root = next;
    }
    let text = root.to_string();
    let line = text.lines().next().unwrap_or("").trim();
    if line.is_empty() {
        e.to_string()
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_string()
    } else {
        line.to_string()
    }
}

/// Unreachable: connect errors, the timeout, and statuses `Unavailable`, `DeadlineExceeded`,
/// `Cancelled` and `Unknown` with a transport error as its source, unless that error is a
/// rejected TLS handshake. Every other status (`PermissionDenied`, `Unimplemented`,
/// `FailedPrecondition`, ...), a TLS rejection and a local configuration error (invalid
/// address or TLS configuration, see [`StatusFailure`]) are `Answered`.
pub(crate) async fn status_classified(
    upstream: &UpstreamConfig,
    client_id: &str,
    probe_seq: Option<Seq>,
) -> std::result::Result<NodeInfo, StatusFailure> {
    let uri = upstream.uri();
    // A local configuration error: the peer was never contacted, so it is not unreachable.
    let mut c = crate::slave::connect_upstream(upstream, client_id, STATUS_TIMEOUT)
        .map_err(|e| StatusFailure::Answered(format!("Status to {uri} not sent: {e}")))?;
    let call = c.status(pb::StatusRequest {
        client_id: client_id.to_string(),
        probe_seq,
    });
    let r = tokio::time::timeout(STATUS_TIMEOUT, call)
        .await
        .map_err(|_| StatusFailure::Unreachable {
            reason: format!("no answer within {STATUS_TIMEOUT:?}"),
            source: None,
        })?
        .map_err(|s| {
            if is_tls_rejection(&s) {
                StatusFailure::Answered(format!(
                    "Status from {uri}: TLS handshake rejected: {}",
                    short_reason(&s)
                ))
            } else if is_transport_failure(&s) {
                StatusFailure::unreachable(s)
            } else {
                StatusFailure::Answered(format!("Status from {uri}: {}: {}", s.code(), s.message()))
            }
        })?;
    NodeInfo::from_pb(r.into_inner(), probe_seq)
        .map_err(|e| StatusFailure::Answered(format!("Status from {uri}: {e}")))
}

/// A TLS handshake the client rejected: rustls reports it as an `io::Error` of kind
/// `InvalidData` (a certificate that does not verify, a peer that does not speak TLS). A
/// server rejecting the client's certificate after a TLS 1.3 handshake shows up only as a
/// broken connection, which cannot be told apart from a network failure, so it stays
/// unreachable.
fn is_tls_rejection(e: &(dyn std::error::Error + 'static)) -> bool {
    let mut cur = Some(e);
    while let Some(err) = cur {
        if err
            .downcast_ref::<std::io::Error>()
            .is_some_and(|io| io.kind() == std::io::ErrorKind::InvalidData)
        {
            return true;
        }
        cur = err.source();
    }
    false
}

fn is_transport_failure(s: &tonic::Status) -> bool {
    use tonic::Code;
    // tonic reports connection failures as `Unavailable` or, for lower-level transport
    // errors, `Unknown` with a transport error as the source.
    match s.code() {
        Code::Unavailable | Code::DeadlineExceeded | Code::Cancelled => true,
        Code::Unknown => std::error::Error::source(s).is_some(),
        _ => false,
    }
}

/// What [`gather`] collected.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Gathered {
    /// Every node that answered, with probes at the other nodes' `last_seq`s below its own.
    pub nodes: Vec<NodeInfo>,
    /// Nodes that did not answer, or answered with an error (address, a one-line reason for
    /// the former, the error text for the latter); `Slave::check_promotion` treats the latter
    /// as blocking instead.
    pub unreachable: Vec<(String, String)>,
}

/// Step 2 of the failover runbook: reads every node's `Status`, then probes each node at the
/// `last_seq` of every node that is behind it, so [`compare`] and
/// [`choose_promotion_candidate`] can check that the shorter node's last entry is present.
pub async fn gather(nodes: &[UpstreamConfig], client_id: &str) -> Gathered {
    let mut out = Gathered::default();
    let mut reached = Vec::new();
    for up in nodes {
        match node_status(up, client_id).await {
            Ok(info) => reached.push((up, info)),
            // One short line: the address is already in the pair.
            Err(Error::PeerUnreachable { reason, .. }) => out.unreachable.push((up.uri(), reason)),
            Err(e) => out.unreachable.push((up.uri(), e.to_string())),
        }
    }
    let seqs: Vec<Seq> = reached.iter().map(|(_, i)| i.last_seq).collect();
    for (up, mut info) in reached {
        for &seq in seqs.iter().filter(|&&s| s > 0 && s < info.last_seq) {
            if info.probes.contains_key(&seq) {
                continue;
            }
            match node_status_probed(up, client_id, Some(seq)).await {
                Ok(probed) => info.probes.extend(probed.probes),
                Err(e) => tracing::warn!(node = %info.node_id, seq, error = %e, "probe failed"),
            }
        }
        out.nodes.push(info);
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A throttled answer (`BUSY`: `RESOURCE_EXHAUSTED`) comes from a live peer whose state is
    /// unknown: answered with an error, never a transport failure (unreachable).
    #[test]
    fn a_throttled_status_answer_is_not_a_transport_failure() {
        let busy = crate::net::reason_status(pb::Reason::Busy, "too many failed attempts");
        assert_eq!(busy.code(), tonic::Code::ResourceExhausted);
        assert!(!is_transport_failure(&busy));
        assert!(is_transport_failure(&tonic::Status::unavailable(
            "connection refused"
        )));
    }

    fn is_answered(r: &std::result::Result<NodeInfo, StatusFailure>) -> bool {
        matches!(r, Err(StatusFailure::Answered(_)))
    }

    /// A peer that was never contacted because of a local configuration error is not
    /// unreachable: consent to skip unreachable peers must not skip it.
    #[tokio::test]
    async fn local_configuration_errors_are_answered_not_unreachable() {
        let bad_address = UpstreamConfig::new("not a host", 1);
        assert!(is_answered(
            &status_classified(&bad_address, "me", None).await
        ));
        let mut bad_tls = UpstreamConfig::new("127.0.0.1", 1);
        bad_tls.tls = Some(crate::config::TlsClientConfig {
            ca_pem: b"not a certificate".to_vec(),
            domain_name: None,
            identity: Some(crate::config::TlsIdentity {
                cert_pem: b"not a certificate".to_vec(),
                key_pem: b"not a key".to_vec(),
            }),
        });
        assert!(is_answered(&status_classified(&bad_tls, "me", None).await));
        // Control: a refused connection (nothing listens on port 1) is unreachable.
        let refused = UpstreamConfig::new("127.0.0.1", 1);
        assert!(matches!(
            status_classified(&refused, "me", None).await,
            Err(StatusFailure::Unreachable { .. })
        ));
    }

    /// A rejected TLS handshake (rustls: `io::Error` of kind `InvalidData`) is an answer, not
    /// a transport failure; a broken connection stays unreachable.
    #[test]
    fn a_tls_rejection_is_not_unreachable() {
        let mut rejected = tonic::Status::unavailable("invalid peer certificate: BadSignature");
        rejected.set_source(std::sync::Arc::new(std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            "invalid peer certificate: BadSignature",
        )));
        assert!(is_tls_rejection(&rejected));
        let mut broken = tonic::Status::unknown("transport error");
        broken.set_source(std::sync::Arc::new(std::io::Error::new(
            std::io::ErrorKind::BrokenPipe,
            "broken pipe",
        )));
        assert!(!is_tls_rejection(&broken));
        assert!(is_transport_failure(&broken));
    }

    fn node(id: &str, uuid: u128, history: &[(u64, Seq)], last: Seq, crc: Option<u32>) -> NodeInfo {
        let epoch_history: Vec<EpochStart> = history
            .iter()
            // The same (epoch, start) is the same epoch in these tests: nonce = start.
            .map(|&(epoch, start_seq)| EpochStart {
                epoch,
                start_seq,
                nonce: start_seq,
            })
            .collect();
        NodeInfo {
            node_id: id.into(),
            role: Role::Slave,
            identity: epoch_history.last().map(|e| DbIdentity {
                db_uuid: uuid::Uuid::from_u128(uuid),
                epoch: e.epoch,
            }),
            epoch_history,
            last_seq: last,
            last_entry_crc: crc,
            last_ts_ms: 0,
            node_state: "ready".into(),
            upstream: None,
            replicas: vec![],
            probes: BTreeMap::new(),
            max_burned_epoch: 0,
        }
    }

    fn probed(mut n: NodeInfo, seq: Seq, crc: Option<u32>) -> NodeInfo {
        n.probes.insert(seq, crc);
        n
    }

    #[derive(Debug)]
    struct Wrapped(&'static str, Option<Box<Wrapped>>);
    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    /// The reason for an unreachable peer is the root cause, one line.
    #[test]
    fn unreachable_reasons_are_the_root_cause_on_one_line() {
        let chain = Wrapped(
            "transport error",
            Some(Box::new(Wrapped(
                "tcp connect error",
                Some(Box::new(Wrapped(
                    "Connection refused (os error 111)\nat line 2",
                    None,
                ))),
            ))),
        );
        assert_eq!(short_reason(&chain), "Connection refused (os error 111)");
        assert_eq!(short_reason(&Wrapped("just this", None)), "just this");
    }

    /// M8: an unreachable peer is reported as such, not as a configuration error.
    #[tokio::test]
    async fn an_unreachable_peer_is_peer_unreachable() {
        let port = std::net::TcpListener::bind("127.0.0.1:0")
            .unwrap()
            .local_addr()
            .unwrap()
            .port();
        let up = UpstreamConfig::new("127.0.0.1", port);
        match node_status(&up, "operator").await {
            Err(Error::PeerUnreachable {
                peer,
                reason,
                source,
            }) => {
                assert_eq!(peer, up.uri());
                assert!(!reason.is_empty() && !reason.contains('\n'), "{reason:?}");
                assert!(source.is_some(), "the whole chain is kept");
            }
            other => panic!("expected PeerUnreachable, got {other:?}"),
        }
    }

    #[test]
    fn compare_orders_compatible_nodes_and_detects_divergence() {
        // a holds b's last entry (8) with b's CRC: a is later.
        let a = probed(node("a", 1, &[(1, 1)], 10, Some(1)), 8, Some(2));
        let b = node("b", 1, &[(1, 1)], 8, Some(2));
        assert_eq!(compare(&a, &b), Comparison::Later(Side::A));
        assert_eq!(compare(&b, &a), Comparison::Later(Side::B));
        assert_eq!(compare(&a, &a.clone()), Comparison::Equal);
        assert_eq!(
            compare(&a, &node("a2", 1, &[(1, 1)], 10, Some(9))),
            Comparison::Diverged { fork: 10 }
        );
        // Same epoch, but entry 8 differs (a restore without an epoch bump, or two masters in
        // one epoch): diverged. Without a probe it cannot be shown compatible: diverged too.
        let other_8 = probed(node("a", 1, &[(1, 1)], 10, Some(1)), 8, Some(7));
        assert_eq!(compare(&other_8, &b), Comparison::Diverged { fork: 8 });
        // a no longer holds entry 8 (or was not probed): the shared history (epoch 1, same
        // nonce) proves b's entries are a's; a is later.
        assert_eq!(
            compare(&node("a", 1, &[(1, 1)], 10, Some(1)), &b),
            Comparison::Later(Side::A)
        );
        assert_eq!(
            compare(&b, &probed(node("a", 1, &[(1, 1)], 10, Some(1)), 8, None)),
            Comparison::Later(Side::B)
        );
        // The same epoch number and start with another nonce is another epoch: a master
        // restored from a backup and reopened. Diverged at that epoch's start.
        let mut restored = node("r", 1, &[(1, 1), (2, 5)], 9, None);
        restored.epoch_history[1].nonce = 77;
        assert_eq!(
            compare(&node("a", 1, &[(1, 1), (2, 5)], 10, None), &restored),
            Comparison::Diverged { fork: 5 }
        );
        // Promoted at 11: the old-epoch node behind the fork is behind (its last entry matches);
        // past the fork, diverged.
        let promoted = probed(node("p", 1, &[(1, 1), (2, 11)], 12, Some(3)), 10, Some(4));
        assert_eq!(
            compare(&node("c", 1, &[(1, 1)], 10, Some(4)), &promoted),
            Comparison::Later(Side::B)
        );
        assert_eq!(
            compare(&node("d", 1, &[(1, 1)], 11, Some(5)), &promoted),
            Comparison::Diverged { fork: 11 }
        );
        // Two promotions of the same epoch at different points.
        let other = node("q", 1, &[(1, 1), (2, 15)], 16, None);
        assert_eq!(
            compare(&promoted, &other),
            Comparison::Diverged { fork: 11 }
        );
        assert_eq!(
            compare(&a, &node("x", 2, &[(1, 1)], 10, None)),
            Comparison::DifferentDatabase
        );
        let empty = NodeInfo {
            identity: None,
            epoch_history: vec![],
            ..node("e", 1, &[(1, 1)], 0, None)
        };
        assert_eq!(compare(&empty, &a), Comparison::Later(Side::B));
    }

    /// Final review m3: a node that refuses promotion by its state (a repair replaying, a
    /// `Corrupt` or `SpecMismatch` halt) is never the candidate, even when it is the most
    /// advanced; it is listed with its state instead.
    #[test]
    fn the_chooser_skips_nodes_that_refuse_promotion() {
        let state = |mut n: NodeInfo, s: &str| {
            n.node_state = s.into();
            n
        };
        let nodes = vec![
            node("b", 1, &[(1, 1)], 10, Some(1)),
            state(node("c", 1, &[(1, 1)], 12, Some(2)), "repairing:15"),
            state(node("d", 1, &[(1, 1)], 11, Some(3)), "halted:Corrupt"),
            state(node("e", 1, &[(1, 1)], 11, Some(3)), "halted:SpecMismatch"),
            state(node("f", 1, &[(1, 1)], 9, Some(4)), "halted:Diverged"),
        ];
        let choice = choose_promotion_candidate(&nodes).unwrap();
        assert_eq!(choice.candidate.node_id, "b");
        assert_eq!(
            choice.not_promotable,
            vec![
                ("c".to_string(), "repairing:15".to_string()),
                ("d".to_string(), "halted:Corrupt".to_string()),
                ("e".to_string(), "halted:SpecMismatch".to_string()),
            ]
        );
        assert_eq!(choice.compatible, vec!["f".to_string()]);
        assert!(choice.unverifiable.is_empty());
        let only = vec![state(node("c", 1, &[(1, 1)], 12, Some(2)), "repairing:15")];
        assert!(matches!(
            choose_promotion_candidate(&only),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn the_chooser_picks_the_most_advanced_and_flags_the_rest() {
        let nodes = vec![
            node("b", 1, &[(1, 1)], 10, Some(1)),
            node("c", 1, &[(1, 1)], 12, Some(2)),
            probed(node("d", 1, &[(1, 1), (2, 11)], 11, Some(3)), 10, Some(1)),
            node("x", 9, &[(1, 1)], 50, None),
        ];
        // d has the newest epoch: it wins; c holds entries past d's fork (11, 12): diverged.
        let choice = choose_promotion_candidate(&nodes).unwrap();
        assert_eq!(choice.candidate.node_id, "d");
        assert_eq!(choice.compatible, vec!["b".to_string()]);
        assert_eq!(choice.diverged, vec![("c".to_string(), 11)]);
        assert_eq!(choice.foreign, vec!["x".to_string()]);

        let same_epoch = vec![
            node("b", 1, &[(1, 1)], 10, Some(1)),
            probed(node("c", 1, &[(1, 1)], 12, None), 10, Some(1)),
        ];
        let choice = choose_promotion_candidate(&same_epoch).unwrap();
        assert_eq!(
            (choice.candidate.node_id.as_str(), choice.compatible.clone()),
            ("c", vec!["b".to_string()]),
            "highest seq"
        );
        // Same epoch, b's entry 10 differs on c: b is diverged, not compatible.
        let forked = vec![
            node("b", 1, &[(1, 1)], 10, Some(1)),
            probed(node("c", 1, &[(1, 1)], 12, None), 10, Some(9)),
        ];
        assert_eq!(
            choose_promotion_candidate(&forked).unwrap().diverged,
            vec![("b".to_string(), 10)]
        );
        // c no longer holds entry 10: the shared history still proves b compatible.
        let trimmed = vec![
            node("b", 1, &[(1, 1)], 10, Some(1)),
            probed(node("c", 1, &[(1, 1)], 12, None), 10, None),
        ];
        let choice = choose_promotion_candidate(&trimmed).unwrap();
        assert_eq!(
            (choice.unverifiable, choice.compatible, choice.diverged),
            (vec![], vec!["b".to_string()], vec![])
        );
        let tie = vec![
            node("c", 1, &[(1, 1)], 12, Some(1)),
            node("b", 1, &[(1, 1)], 12, Some(1)),
        ];
        assert_eq!(
            choose_promotion_candidate(&tie).unwrap().candidate.node_id,
            "b",
            "lowest id on a tie"
        );
        let empty = NodeInfo {
            identity: None,
            epoch_history: vec![],
            ..node("e", 1, &[(1, 1)], 0, None)
        };
        assert!(matches!(
            choose_promotion_candidate(&[empty]),
            Err(Error::Config(_))
        ));
        assert!(matches!(
            choose_promotion_candidate(&[]),
            Err(Error::Config(_))
        ));
    }

    #[test]
    fn an_empty_newest_epoch_is_later_by_position_but_diverged_from_entries_past_its_start() {
        // Wrongly promoted: history [(1,1),(2,11)], last_seq 10, the new epoch has no entries.
        let w = node("w", 1, &[(1, 1), (2, 11)], 10, Some(5));
        let b = node("b", 1, &[(1, 1)], 10, Some(5));
        // Equal seq and CRC: ordered by epoch, not Equal.
        assert_eq!(compare(&w, &b), Comparison::Later(Side::A));
        assert_eq!(compare(&b, &w), Comparison::Later(Side::B));
        // The other node is longer but still before the new epoch's start: the newer epoch wins.
        let w9 = node("w", 1, &[(1, 1), (2, 11)], 9, Some(3));
        let long = probed(node("b", 1, &[(1, 1)], 10, Some(5)), 9, Some(3));
        assert_eq!(compare(&w9, &long), Comparison::Later(Side::A));
        assert_eq!(compare(&long, &w9), Comparison::Later(Side::B));
        // The other node holds entries at or past the start (11, 12): diverged at the start.
        let past = probed(node("b", 1, &[(1, 1)], 12, Some(6)), 10, Some(5));
        assert_eq!(compare(&w, &past), Comparison::Diverged { fork: 11 });
        assert_eq!(compare(&past, &w), Comparison::Diverged { fork: 11 });
        // Equal position without a CRC: the history decides, ordered by epoch.
        let no_crc = node("b", 1, &[(1, 1)], 10, None);
        assert_eq!(compare(&w, &no_crc), Comparison::Later(Side::A));
    }

    #[test]
    fn the_chooser_never_lists_a_later_node_as_compatible() {
        let w = node("w", 1, &[(1, 1), (2, 11)], 10, Some(5));
        let past = probed(node("b", 1, &[(1, 1)], 12, Some(6)), 10, Some(5));
        let choice = choose_promotion_candidate(&[past.clone(), w]).unwrap();
        assert_eq!(choice.candidate.node_id, "w");
        assert_eq!(choice.diverged, vec![("b".to_string(), 11)]);
        assert!(choice.compatible.is_empty() && choice.unverifiable.is_empty());
        // Equal position, older epoch on the other node: compatible, candidate is the newer epoch.
        let b = node("b", 1, &[(1, 1)], 10, Some(5));
        let w = node("w", 1, &[(1, 1), (2, 11)], 10, Some(5));
        let choice = choose_promotion_candidate(&[b, w]).unwrap();
        assert_eq!(
            (choice.candidate.node_id.as_str(), choice.compatible),
            ("w", vec!["b".to_string()])
        );
    }
}
