//! A slave as a replication source for downstream replicas (chaining, DESIGN.md §11).

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;

use super::inner::Inner;
use super::state::SlaveState;
use crate::source::LogSource;
use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

impl LogSource for Inner {
    fn node_id(&self) -> &str {
        &self.cfg.identifier
    }

    fn role(&self) -> Role {
        Role::Slave
    }

    fn is_ready(&self) -> bool {
        Inner::is_ready(self)
    }

    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)> {
        self.ident()
    }

    fn store(&self) -> &Store {
        &self.store
    }

    fn origin_last_seq(&self) -> Seq {
        self.origin_last_seq.load(Ordering::SeqCst).max(self.store.position().last_seq)
    }

    fn upstream_chain(&self) -> Vec<String> {
        Inner::upstream_chain(self)
    }

    fn max_entry_bytes(&self) -> u64 {
        self.max_entry_bytes.load(Ordering::SeqCst)
    }

    fn heartbeat(&self) -> Duration {
        self.cfg.heartbeat
    }

    fn subscribe(&self) -> watch::Receiver<Seq> {
        self.seq_tx.subscribe()
    }

    fn resets(&self) -> watch::Receiver<u64> {
        self.reset_tx.subscribe()
    }

    fn node_state(&self) -> String {
        match self.state() {
            SlaveState::Bootstrapping { .. } => "bootstrapping".into(),
            SlaveState::Halted(r) => format!("halted:{}", r.name()),
            SlaveState::Connecting | SlaveState::Streaming => {
                if Inner::is_ready(self) { "ready".into() } else { "bootstrapping".into() }
            }
        }
    }

    fn upstream(&self) -> Option<String> {
        Some(self.upstream_uri())
    }
}
