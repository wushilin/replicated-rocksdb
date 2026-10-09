//! Operator tooling for failover (addendum §4). This task adds the node report served by the
//! `Status` RPC; comparing nodes and choosing a promotion candidate follow with promotion.

use std::collections::BTreeMap;

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
