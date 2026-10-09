//! What the replication server needs from the node it runs on. Implemented by the master
//! and by slaves that serve downstream replicas (DESIGN.md §11).

use std::time::Duration;

use tokio::sync::watch;

use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

pub(crate) trait LogSource: Send + Sync + 'static {
    /// This node's configured identifier (`server_id` in the handshake).
    fn node_id(&self) -> &str;
    fn role(&self) -> Role;
    /// False while this node is bootstrapping itself (it has no consistent state).
    fn is_ready(&self) -> bool;
    /// Origin identity and epoch history, read together.
    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)>;
    fn store(&self) -> &Store;
    /// The highest master seq this node knows of. On the master, its `last_seq`.
    fn origin_last_seq(&self) -> Seq;
    /// Node ids between this node and the master, closest first. Empty on the master.
    fn upstream_chain(&self) -> Vec<String>;
    /// The master's `max_entry_bytes`, forwarded down the chain.
    fn max_entry_bytes(&self) -> u64;
    /// Interval of Tail heartbeats this node sends.
    fn heartbeat(&self) -> Duration;
    /// Receives this node's `last_seq` after each commit.
    fn subscribe(&self) -> watch::Receiver<Seq>;
    /// Bumped whenever every served stream must end with `UPSTREAM_RESET` (an epoch bump on
    /// the master, an epoch adoption or a bootstrap on a slave).
    fn resets(&self) -> watch::Receiver<u64>;
    /// `ready`, `bootstrapping` or `halted:<Reason>` (the `Status` RPC).
    fn node_state(&self) -> String {
        "ready".into()
    }
    /// The upstream address; `None` on the master.
    fn upstream(&self) -> Option<String> {
        None
    }
}
