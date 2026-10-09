//! Shared state of one slave: the store plus everything the driver, the API and the
//! replication server (when serving) read.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::SystemTime;

use tokio::sync::{watch, Notify};

use super::config::{SlaveConfig, UpstreamConfig};
use super::policy::Policies;
use super::repoint::PendingRepoint;
use super::state::{HaltReason, NodeState, SlaveState, SlaveStatus};
use crate::error::{Error, Result};
use crate::listener::ReplicationListener;
use crate::store::meta::{key, MetaSnapshot};
use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Seq};

/// A requested bootstrap, tagged with the repoint generation it was requested for. A request
/// of an older generation (a session of the previous upstream still winding down) is inert:
/// only the consent given for the current upstream counts.
#[derive(Default)]
pub(crate) struct ForcedBootstrap(AtomicU64);

impl ForcedBootstrap {
    /// 0 means none, so generation `g` is stored as `g + 1`.
    pub(crate) fn request(&self, generation: u64) {
        self.0.store(generation + 1, Ordering::SeqCst);
    }

    pub(crate) fn is_requested_for(&self, generation: u64) -> bool {
        self.0.load(Ordering::SeqCst) == generation + 1
    }

    /// Clears the request of `generation` (a request of another generation stays).
    pub(crate) fn clear(&self, generation: u64) {
        let _ = self.0.compare_exchange(generation + 1, 0, Ordering::SeqCst, Ordering::SeqCst);
    }
}

pub(crate) struct Inner {
    pub store: Store,
    pub cfg: SlaveConfig,
    /// `None` until the first bootstrap completes, and during a bootstrap.
    ident: RwLock<Option<(DbIdentity, Vec<EpochStart>)>>,
    pub state_tx: watch::Sender<SlaveState>,
    /// Last applied seq, published after each commit.
    pub seq_tx: watch::Sender<Seq>,
    /// Bumped to end every stream this slave serves with `UPSTREAM_RESET`.
    pub reset_tx: watch::Sender<u64>,
    pub shutdown_tx: watch::Sender<bool>,
    pub origin_last_seq: AtomicU64,
    pub upstream_last_seq: AtomicU64,
    upstream_chain: RwLock<Vec<String>>,
    pub max_entry_bytes: AtomicU64,
    last_contact: Mutex<Option<SystemTime>>,
    last_error: Mutex<Option<String>>,
    /// Set by `clear_halt(Rebootstrap)`, by a gap seen mid-stream, and by a `DIVERGED` answer
    /// under a `Rebootstrap` policy. Scoped to the repoint generation that requested it.
    pub force_bootstrap: ForcedBootstrap,
    /// Shared with the driver, which waits on it without holding the `Inner`.
    pub halt_cleared: Arc<Notify>,
    listener_failed: AtomicBool,
    /// Downstream replicas served by this slave.
    pub(crate) replicas: Arc<crate::server::replicas::ReplicaRegistry>,
    /// The current upstream: `cfg.upstream` until `Slave::repoint` replaces it.
    upstream: RwLock<UpstreamConfig>,
    /// Bumped by every repoint; the driver ends its session and reconnects.
    pub repoint_tx: watch::Sender<u64>,
    pending_repoint: Mutex<Option<PendingRepoint>>,
    /// The divergence mode of the latest repoint. It replaces `cfg.on_divergence` for every
    /// session from then on (until the next repoint), not only for the first handshake.
    pub(crate) repoint_divergence: Mutex<Option<super::repoint::RepointDivergence>>,
    /// A repoint cleared a halt in memory only: `node_state = ready` is persisted once the new
    /// upstream is accepted (so a restart before that reopens halted).
    pub halt_clear_pending: AtomicBool,
}

impl Inner {
    pub(crate) fn new(store: Store, cfg: SlaveConfig, meta: &MetaSnapshot) -> Result<Inner> {
        let node_state = NodeState::decode(meta.node_state.as_deref())?;
        let ident = match (meta.identity, node_state) {
            (_, NodeState::Bootstrapping) | (None, _) => None,
            (Some(_), _) if meta.epoch_history.is_empty() => {
                return Err(Error::corrupt("origin_identity without epoch_history in __meta"));
            }
            (Some(id), _) => Some((id, meta.epoch_history.clone())),
        };
        let state = match node_state {
            NodeState::Halted(r) => SlaveState::Halted(r),
            NodeState::Ready | NodeState::Bootstrapping => SlaveState::Connecting,
        };
        let last_seq = store.position().last_seq;
        Ok(Inner {
            store,
            upstream: RwLock::new(cfg.upstream.clone()),
            cfg,
            ident: RwLock::new(ident),
            state_tx: watch::channel(state).0,
            seq_tx: watch::channel(last_seq).0,
            reset_tx: watch::channel(0).0,
            shutdown_tx: watch::channel(false).0,
            origin_last_seq: AtomicU64::new(last_seq),
            upstream_last_seq: AtomicU64::new(0),
            upstream_chain: RwLock::new(Vec::new()),
            max_entry_bytes: AtomicU64::new(meta.max_entry_bytes.unwrap_or(0)),
            last_contact: Mutex::new(None),
            last_error: Mutex::new(None),
            force_bootstrap: ForcedBootstrap::default(),
            halt_cleared: Arc::new(Notify::new()),
            listener_failed: AtomicBool::new(false),
            replicas: Arc::default(),
            repoint_tx: watch::channel(0).0,
            pending_repoint: Mutex::new(None),
            repoint_divergence: Mutex::new(None),
            halt_clear_pending: AtomicBool::new(false),
        })
    }

    /// The current upstream's address.
    pub(crate) fn upstream_uri(&self) -> String {
        self.upstream().uri()
    }

    pub(crate) fn upstream(&self) -> UpstreamConfig {
        self.upstream.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(crate) fn set_upstream(&self, up: UpstreamConfig) {
        *self.upstream.write().unwrap_or_else(PoisonError::into_inner) = up;
    }

    pub(crate) fn pending_repoint(&self) -> std::sync::MutexGuard<'_, Option<PendingRepoint>> {
        self.pending_repoint.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn policies(&self) -> Policies {
        Policies { identity: self.cfg.on_identity_change, divergence: self.cfg.on_divergence, gap: self.cfg.on_log_gap }
    }

    pub(crate) fn ident(&self) -> Option<(DbIdentity, Vec<EpochStart>)> {
        self.ident.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(crate) fn set_ident(&self, v: Option<(DbIdentity, Vec<EpochStart>)>) {
        *self.ident.write().unwrap_or_else(PoisonError::into_inner) = v;
    }

    /// Readable and servable: the slave holds a consistent state at some seq.
    pub(crate) fn is_ready(&self) -> bool {
        self.ident.read().unwrap_or_else(PoisonError::into_inner).is_some()
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.cfg.clock.now_ms()
    }

    pub(crate) fn state(&self) -> SlaveState {
        self.state_tx.borrow().clone()
    }

    /// Publishes a state change and tells the listener (no-op if unchanged).
    pub(crate) fn set_state(&self, s: SlaveState) {
        let changed = self.state_tx.send_if_modified(|cur| {
            if *cur == s {
                false
            } else {
                *cur = s.clone();
                true
            }
        });
        if changed {
            self.notify(|l| l.on_state_change(&s));
        }
    }

    /// Stops advancing. Data-related reasons are persisted first (a synced write: call it on a
    /// blocking thread), so they survive restart.
    pub(crate) fn halt(&self, reason: HaltReason) {
        if reason.is_persisted() {
            let state = NodeState::Halted(reason).encode();
            if let Err(e) = self.store.write_meta(&[(key::NODE_STATE, &state)], true) {
                tracing::error!(node = %self.cfg.identifier, error = %e, "could not persist the halt");
            }
        }
        tracing::error!(
            node = %self.cfg.identifier,
            reason = reason.name(),
            applied_seq = self.store.position().last_seq,
            upstream_last_seq = self.upstream_last_seq.load(Ordering::SeqCst),
            "replication halted"
        );
        self.set_state(SlaveState::Halted(reason));
    }

    /// Ends every stream this slave serves with `UPSTREAM_RESET`.
    pub(crate) fn reset_downstream(&self) {
        self.reset_tx.send_modify(|n| *n += 1);
    }

    /// Forgets the chain learned from the previous upstream, until the new one answers.
    pub(crate) fn clear_upstream_chain(&self) {
        self.upstream_chain.write().unwrap_or_else(PoisonError::into_inner).clear();
    }

    pub(crate) fn upstream_chain(&self) -> Vec<String> {
        self.upstream_chain.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Records what the upstream said about itself in a handshake. Returns the upstream's
    /// `max_entry_bytes` if it changed, for the caller to persist off the async thread.
    pub(crate) fn note_upstream(&self, r: &crate::pb::HandshakeResponse) -> Option<u64> {
        let mut chain = vec![r.server_id.clone()];
        chain.extend(r.upstream_chain.iter().cloned());
        *self.upstream_chain.write().unwrap_or_else(PoisonError::into_inner) = chain;
        self.note_positions(r.last_seq, r.origin_last_seq);
        self.note_contact();
        let changed = r.max_entry_bytes > 0 && self.max_entry_bytes.swap(r.max_entry_bytes, Ordering::SeqCst) != r.max_entry_bytes;
        changed.then_some(r.max_entry_bytes)
    }

    pub(crate) fn note_positions(&self, upstream_last_seq: Seq, origin_last_seq: Seq) {
        self.upstream_last_seq.store(upstream_last_seq, Ordering::SeqCst);
        self.origin_last_seq.fetch_max(origin_last_seq, Ordering::SeqCst);
    }

    pub(crate) fn note_contact(&self) {
        *self.last_contact.lock().unwrap_or_else(PoisonError::into_inner) = Some(SystemTime::now());
    }

    pub(crate) fn set_last_error(&self, e: Option<String>) {
        *self.last_error.lock().unwrap_or_else(PoisonError::into_inner) = e;
    }

    /// Calls the listener, if any. A panic is caught, logged, and disables the listener.
    pub(crate) fn notify(&self, f: impl FnOnce(&dyn ReplicationListener)) {
        let Some(l) = &self.cfg.listener else { return };
        if self.listener_failed.load(Ordering::SeqCst) {
            return;
        }
        if catch_unwind(AssertUnwindSafe(|| f(l.as_ref()))).is_err() {
            self.listener_failed.store(true, Ordering::SeqCst);
            tracing::error!(node = %self.cfg.identifier, "replication listener panicked; it is disabled until restart");
        }
    }

    pub(crate) fn status(&self) -> SlaveStatus {
        let pos = self.store.position();
        SlaveStatus {
            state: self.state(),
            origin_identity: self.ident().map(|(id, _)| id),
            applied_seq: pos.last_seq,
            first_retained_seq: self.store.first_retained_seq(),
            upstream_last_seq: self.upstream_last_seq.load(Ordering::SeqCst),
            origin_last_seq: self.origin_last_seq.load(Ordering::SeqCst).max(pos.last_seq),
            upstream: self.upstream_uri(),
            upstream_chain: self.upstream_chain(),
            last_contact: *self.last_contact.lock().unwrap_or_else(PoisonError::into_inner),
            last_error: self.last_error.lock().unwrap_or_else(PoisonError::into_inner).clone(),
            listener_failed: self.listener_failed.load(Ordering::SeqCst),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::ForcedBootstrap;

    #[test]
    fn a_forced_bootstrap_only_counts_for_the_generation_that_requested_it() {
        let f = ForcedBootstrap::default();
        assert!(!f.is_requested_for(0));
        f.request(0);
        assert!(f.is_requested_for(0));
        // A repoint moved on to generation 1: the request is inert there.
        assert!(!f.is_requested_for(1));
        // A late request of the old generation does not turn into consent for the new one.
        f.request(0);
        assert!(!f.is_requested_for(1));
        f.request(1);
        assert!(f.is_requested_for(1) && !f.is_requested_for(0));
        f.clear(0);
        assert!(f.is_requested_for(1), "clearing another generation is a no-op");
        f.clear(1);
        assert!(!f.is_requested_for(1));
    }
}
