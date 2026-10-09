//! The read-only `Status` RPC (addendum §4) and the in-process equivalent.

use std::time::Duration;

use super::replicas::ReplicaRegistry;
use crate::admin::{NodeInfo, ReplicaSummary};
use crate::net::convert::{epochs_to_pb, identity_to_pb};
use crate::pb;
use crate::source::LogSource;
use crate::types::{Role, Seq};

/// What this node reports about itself.
/// `probes`: seqs whose entry CRC to report as well (`NodeInfo::probes`).
pub(crate) fn node_info(src: &dyn LogSource, replicas: &ReplicaRegistry, lag_max: Duration, probes: &[Seq]) -> NodeInfo {
    let ident = src.identity();
    let pos = src.store().position();
    let last_ts_ms = src
        .store()
        .read_entries(pos.last_seq, 1, usize::MAX)
        .ok()
        .and_then(|v| v.first().filter(|e| e.seq == pos.last_seq).map(|e| e.ts_ms))
        .unwrap_or(0);
    let epoch = ident.as_ref().map_or(0, |(id, _)| id.epoch);
    NodeInfo {
        node_id: src.node_id().to_string(),
        role: src.role(),
        identity: ident.as_ref().map(|(id, _)| *id),
        epoch_history: ident.map(|(_, h)| h).unwrap_or_default(),
        last_seq: pos.last_seq,
        last_entry_crc: pos.last_entry_crc,
        last_ts_ms,
        node_state: src.node_state(),
        upstream: src.upstream(),
        replicas: replicas
            .summary(epoch, lag_max)
            .into_iter()
            .map(|(node_id, applied_seq, in_sync)| ReplicaSummary { node_id, applied_seq, in_sync })
            .collect(),
        // `entry_crc` includes this node's own `last_entry_crc` at `last_seq`; a read error
        // reports the entry as not held.
        probes: probes.iter().map(|&seq| (seq, src.store().entry_crc(seq).ok().flatten())).collect(),
    }
}

/// The response for a request that asked about `probe_seq`.
pub(crate) fn to_pb(i: &NodeInfo, probe_seq: Option<Seq>) -> pb::StatusResponse {
    pb::StatusResponse {
        node_id: i.node_id.clone(),
        role: match i.role {
            Role::Master => "master".into(),
            Role::Slave => "slave".into(),
        },
        identity: i.identity.as_ref().map(identity_to_pb),
        epoch_history: epochs_to_pb(&i.epoch_history),
        last_seq: i.last_seq,
        last_entry_crc: i.last_entry_crc,
        last_ts_ms: i.last_ts_ms,
        node_state: i.node_state.clone(),
        upstream: i.upstream.clone().unwrap_or_default(),
        replicas: i
            .replicas
            .iter()
            .map(|r| pb::ReplicaSummary { node_id: r.node_id.clone(), applied_seq: r.applied_seq, in_sync: r.in_sync })
            .collect(),
        probe_crc: probe_seq.and_then(|seq| i.probes.get(&seq).copied().flatten()),
    }
}
