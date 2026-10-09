//! Operator tooling for failover (addendum §4): node status, comparing nodes, and choosing
//! which replica to promote.
//!
//! # Failover runbook
//!
//! 1. Fence the old master (stop it, or cut it off from clients and replicas).
//! 2. Gather the status of every replica with [`gather`], which also probes the entry CRCs
//!    that [`compare`] needs ([`node_status`] reads one node).
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

/// What a node reports about itself (`Status` RPC, `Slave::promotion_info`).
#[derive(Clone, Debug, PartialEq, Eq)]
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
    /// `ready`, `bootstrapping` or `halted:<Reason>`.
    pub node_state: String,
    /// The upstream address; `None` on the master.
    pub upstream: Option<String>,
    pub replicas: Vec<ReplicaSummary>,
    /// CRCs of entries this node was asked about (`Status.probe_seq`): seq → `crc32c` of its
    /// entry there, `None` if it does not hold that entry. [`compare`] uses the longer node's
    /// probe at the shorter node's `last_seq`; [`gather`] and `Slave::check_promotion` fill it.
    pub probes: BTreeMap<Seq, Option<u32>>,
}

/// One direct downstream replica, as reported by `Status`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaSummary {
    pub node_id: String,
    pub applied_seq: Seq,
    pub in_sync: bool,
}

impl NodeInfo {
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
                .map(|r| ReplicaSummary { node_id: r.node_id, applied_seq: r.applied_seq, in_sync: r.in_sync })
                .collect(),
            probes: probe_seq.map(|seq| (seq, p.probe_crc)).into_iter().collect(),
        })
    }

    fn newest(&self) -> (u64, Seq) {
        (self.epoch_history.last().map_or(0, |e| e.epoch), self.last_seq)
    }
}

/// Which side of a comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    A,
    B,
}

/// How two nodes' data relate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    /// Compatible histories; this side holds everything the other holds, and more.
    Later(Side),
    Equal,
    /// Both hold different entries from `fork` on: a CRC mismatch was proven (or the epoch
    /// histories fork below both positions).
    Diverged { fork: Seq },
    /// Compatibility cannot be proven: the node that would have to hold the other's last
    /// entry no longer does (trimmed, or bootstrapped past it), or no probe was taken.
    /// `later` is the side that is later by position (newest epoch, then `last_seq`); `None`
    /// when both positions are equal.
    Unverifiable { later: Option<Side> },
    /// Different databases (different `db_uuid`).
    DifferentDatabase,
}

/// The first seq whose epoch differs between two histories, if any.
fn fork_between(a: &[EpochStart], b: &[EpochStart]) -> Option<Seq> {
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
/// Otherwise the nodes are compatible when no seq both hold lies at or past a point where
/// their epoch histories differ, **and** the shorter node's last entry is present in the
/// longer node with the same CRC: equal `last_entry_crc` at equal `last_seq`, or the longer
/// node's probe at the shorter node's `last_seq` ([`NodeInfo::probes`]). The later of two
/// compatible nodes has the newer epoch, then the higher `last_seq`. Anything that cannot be
/// shown compatible because of a CRC mismatch is `Diverged`; when the
/// histories agree, `fork` is then the shorter node's `last_seq` (the first seq known not to
/// match may be earlier). A comparison that needs a CRC nobody can supply is `Unverifiable`,
/// never `Compatible` or `Diverged`.
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
    let (short, long, long_side) = match a.last_seq.cmp(&b.last_seq) {
        std::cmp::Ordering::Equal => {
            return match (a.last_entry_crc, b.last_entry_crc) {
                _ if a.last_seq == 0 => Comparison::Equal,
                (Some(x), Some(y)) if x == y => Comparison::Equal,
                (Some(_), Some(_)) => Comparison::Diverged { fork: a.last_seq },
                _ => Comparison::Unverifiable { later: None },
            };
        }
        std::cmp::Ordering::Less => (a, b, Side::B),
        std::cmp::Ordering::Greater => (b, a, Side::A),
    };
    if short.last_seq > 0 {
        let held = long.probes.get(&short.last_seq).copied().flatten();
        match (short.last_entry_crc, held) {
            (Some(x), Some(y)) if x == y => {}
            (Some(_), Some(_)) => return Comparison::Diverged { fork: short.last_seq },
            _ => return Comparison::Unverifiable { later: Some(long_side) },
        }
    }
    // Compatible: the longer node holds everything the shorter one holds. Its newest epoch is
    // at least as new (a newer epoch on the shorter node would have forked above).
    Comparison::Later(long_side)
}

/// The node to promote, and how the others relate to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub candidate: NodeInfo,
    /// Compatible nodes: behind the candidate or equal to it. They can follow it.
    pub compatible: Vec<String>,
    /// Nodes holding entries the candidate does not (`node_id`, fork seq): repointing them
    /// needs an explicit decision to discard those entries.
    pub diverged: Vec<(String, Seq)>,
    /// Nodes of another database.
    pub foreign: Vec<String>,
    /// Nodes whose compatibility with the candidate cannot be proven (see
    /// [`Comparison::Unverifiable`]). They are not known to be safe to follow it.
    pub unverifiable: Vec<String>,
}

/// Picks the most advanced node holding data (newest epoch, then highest `last_seq`, then
/// lowest `node_id`) and classifies the others against it. `Err(Config)` if no node holds data.
pub fn choose_promotion_candidate(nodes: &[NodeInfo]) -> Result<Choice> {
    let candidate = nodes
        .iter()
        .filter(|n| n.identity.is_some())
        .max_by(|x, y| x.newest().cmp(&y.newest()).then_with(|| y.node_id.cmp(&x.node_id)))
        .ok_or_else(|| Error::Config("no node holds data".into()))?
        .clone();
    let mut choice =
        Choice { candidate, compatible: Vec::new(), diverged: Vec::new(), foreign: Vec::new(), unverifiable: Vec::new() };
    for n in nodes.iter().filter(|n| n.node_id != choice.candidate.node_id) {
        match compare(&choice.candidate, n) {
            Comparison::Later(Side::A) | Comparison::Equal => choice.compatible.push(n.node_id.clone()),
            // Not reachable for the most advanced node, but never lose a node silently.
            Comparison::Later(Side::B) => choice.compatible.push(n.node_id.clone()),
            Comparison::Diverged { fork } => choice.diverged.push((n.node_id.clone(), fork)),
            Comparison::DifferentDatabase => choice.foreign.push(n.node_id.clone()),
            Comparison::Unverifiable { .. } => choice.unverifiable.push(n.node_id.clone()),
        }
    }
    Ok(choice)
}

/// How long [`node_status`] waits for a peer's answer; a peer that does not answer in time is
/// unreachable.
pub const STATUS_TIMEOUT: Duration = Duration::from_secs(5);

/// Queries a node's `Status` over the network (within [`STATUS_TIMEOUT`]). `client_id`
/// identifies the caller (it must be a valid node identifier); `upstream` carries the
/// address, TLS and credentials.
pub async fn node_status(upstream: &UpstreamConfig, client_id: &str) -> Result<NodeInfo> {
    node_status_probed(upstream, client_id, None).await
}

/// [`node_status`], also asking for the CRC of the node's entry at `probe_seq`.
pub async fn node_status_probed(upstream: &UpstreamConfig, client_id: &str, probe_seq: Option<Seq>) -> Result<NodeInfo> {
    let mut c = crate::slave::connect_upstream(upstream, client_id, STATUS_TIMEOUT)?;
    let call = c.status(pb::StatusRequest { client_id: client_id.to_string(), probe_seq });
    let r = tokio::time::timeout(STATUS_TIMEOUT, call)
        .await
        .map_err(|_| Error::Config(format!("Status from {}: no answer within {STATUS_TIMEOUT:?}", upstream.uri())))?
        .map_err(|s| Error::Config(format!("Status from {}: {}: {}", upstream.uri(), s.code(), s.message())))?;
    NodeInfo::from_pb(r.into_inner(), probe_seq)
}

/// What [`gather`] collected.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Gathered {
    /// Every node that answered, with probes at the other nodes' `last_seq`s below its own.
    pub nodes: Vec<NodeInfo>,
    /// Nodes that did not answer (address, error).
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

    fn node(id: &str, uuid: u128, history: &[(u64, Seq)], last: Seq, crc: Option<u32>) -> NodeInfo {
        let epoch_history: Vec<EpochStart> = history.iter().map(|&(epoch, start_seq)| EpochStart { epoch, start_seq }).collect();
        NodeInfo {
            node_id: id.into(),
            role: Role::Slave,
            identity: epoch_history.last().map(|e| DbIdentity { db_uuid: uuid::Uuid::from_u128(uuid), epoch: e.epoch }),
            epoch_history,
            last_seq: last,
            last_entry_crc: crc,
            last_ts_ms: 0,
            node_state: "ready".into(),
            upstream: None,
            replicas: vec![],
            probes: BTreeMap::new(),
        }
    }

    fn probed(mut n: NodeInfo, seq: Seq, crc: Option<u32>) -> NodeInfo {
        n.probes.insert(seq, crc);
        n
    }

    #[test]
    fn compare_orders_compatible_nodes_and_detects_divergence() {
        // a holds b's last entry (8) with b's CRC: a is later.
        let a = probed(node("a", 1, &[(1, 1)], 10, Some(1)), 8, Some(2));
        let b = node("b", 1, &[(1, 1)], 8, Some(2));
        assert_eq!(compare(&a, &b), Comparison::Later(Side::A));
        assert_eq!(compare(&b, &a), Comparison::Later(Side::B));
        assert_eq!(compare(&a, &a.clone()), Comparison::Equal);
        assert_eq!(compare(&a, &node("a2", 1, &[(1, 1)], 10, Some(9))), Comparison::Diverged { fork: 10 });
        // Same epoch, but entry 8 differs (a restore without an epoch bump, or two masters in
        // one epoch): diverged. Without a probe it cannot be shown compatible: diverged too.
        let other_8 = probed(node("a", 1, &[(1, 1)], 10, Some(1)), 8, Some(7));
        assert_eq!(compare(&other_8, &b), Comparison::Diverged { fork: 8 });
        // a no longer holds entry 8 (or was not probed): unverifiable, a still later by position.
        assert_eq!(compare(&node("a", 1, &[(1, 1)], 10, Some(1)), &b), Comparison::Unverifiable { later: Some(Side::A) });
        assert_eq!(compare(&b, &probed(node("a", 1, &[(1, 1)], 10, Some(1)), 8, None)), Comparison::Unverifiable { later: Some(Side::B) });
        // Promoted at 11: the old-epoch node behind the fork is behind (its last entry matches);
        // past the fork, diverged.
        let promoted = probed(node("p", 1, &[(1, 1), (2, 11)], 12, Some(3)), 10, Some(4));
        assert_eq!(compare(&node("c", 1, &[(1, 1)], 10, Some(4)), &promoted), Comparison::Later(Side::B));
        assert_eq!(compare(&node("d", 1, &[(1, 1)], 11, Some(5)), &promoted), Comparison::Diverged { fork: 11 });
        // Two promotions of the same epoch at different points.
        let other = node("q", 1, &[(1, 1), (2, 15)], 16, None);
        assert_eq!(compare(&promoted, &other), Comparison::Diverged { fork: 11 });
        assert_eq!(compare(&a, &node("x", 2, &[(1, 1)], 10, None)), Comparison::DifferentDatabase);
        let empty = NodeInfo { identity: None, epoch_history: vec![], ..node("e", 1, &[(1, 1)], 0, None) };
        assert_eq!(compare(&empty, &a), Comparison::Later(Side::B));
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

        let same_epoch = vec![node("b", 1, &[(1, 1)], 10, Some(1)), probed(node("c", 1, &[(1, 1)], 12, None), 10, Some(1))];
        let choice = choose_promotion_candidate(&same_epoch).unwrap();
        assert_eq!((choice.candidate.node_id.as_str(), choice.compatible.clone()), ("c", vec!["b".to_string()]), "highest seq");
        // Same epoch, b's entry 10 differs on c: b is diverged, not compatible.
        let forked = vec![node("b", 1, &[(1, 1)], 10, Some(1)), probed(node("c", 1, &[(1, 1)], 12, None), 10, Some(9))];
        assert_eq!(choose_promotion_candidate(&forked).unwrap().diverged, vec![("b".to_string(), 10)]);
        // c no longer holds entry 10: b is listed as unverifiable, neither compatible nor diverged.
        let trimmed = vec![node("b", 1, &[(1, 1)], 10, Some(1)), probed(node("c", 1, &[(1, 1)], 12, None), 10, None)];
        let choice = choose_promotion_candidate(&trimmed).unwrap();
        assert_eq!((choice.unverifiable, choice.compatible, choice.diverged), (vec!["b".to_string()], vec![], vec![]));
        let tie = vec![node("c", 1, &[(1, 1)], 12, Some(1)), node("b", 1, &[(1, 1)], 12, Some(1))];
        assert_eq!(choose_promotion_candidate(&tie).unwrap().candidate.node_id, "b", "lowest id on a tie");
        let empty = NodeInfo { identity: None, epoch_history: vec![], ..node("e", 1, &[(1, 1)], 0, None) };
        assert!(matches!(choose_promotion_candidate(&[empty]), Err(Error::Config(_))));
        assert!(matches!(choose_promotion_candidate(&[]), Err(Error::Config(_))));
    }
}
