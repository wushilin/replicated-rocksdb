//! Shared state of one slave: the store plus everything the driver, the API and the
//! replication server (when serving) read.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::SystemTime;

use tokio::sync::{Notify, oneshot, watch};

use super::config::{SlaveConfig, UpstreamConfig};
use super::policy::Policies;
use super::repoint::PendingRepoint;
use super::state::{HaltReason, NodeState, SlaveState, SlaveStatus};
use crate::error::{Error, Result};
use crate::listener::ReplicationListener;
use crate::read::ReadSnapshot;
use crate::store::Store;
use crate::store::meta::{MetaSnapshot, key};
use crate::types::{DbIdentity, EpochStart, Seq, newest_records};
use crate::verify::VerifyReport;

/// A requested bootstrap, tagged with the repoint generation it was requested for. A request
/// of an older generation (a session of the previous upstream still winding down) is inert:
/// only the consent given for the current upstream counts.
#[derive(Default)]
pub(crate) struct ForcedBootstrap(AtomicU64);

impl ForcedBootstrap {
    /// 0 means none, so generation `g` is stored as `g + 1`.
    pub(crate) fn request(&self, generation: u64) {
        // An older generation never overwrites a newer request.
        self.0.fetch_max(generation + 1, Ordering::SeqCst);
    }

    pub(crate) fn is_requested_for(&self, generation: u64) -> bool {
        self.0.load(Ordering::SeqCst) == generation + 1
    }

    /// Clears the request of `generation` (a request of another generation stays).
    pub(crate) fn clear(&self, generation: u64) {
        let _ = self
            .0
            .compare_exchange(generation + 1, 0, Ordering::SeqCst, Ordering::SeqCst);
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
    /// session from then on (until the next repoint), not only for the first handshake. A
    /// consent to lose data (`Rebootstrap`, `Repair`) becomes `Refuse` once its repoint settles.
    pub(crate) repoint_divergence: Mutex<Option<super::repoint::RepointDivergence>>,
    /// A repoint cleared a halt in memory only: `node_state = ready` is persisted once the new
    /// upstream is accepted (so a restart before that reopens halted).
    pub halt_clear_pending: AtomicBool,
    /// A stream ended `DIVERGED` under a repoint's mode and the session was restarted: the
    /// next handshake must not answer RESUME (see `driver::contradicting_resume`).
    pub diverged_on_stream: AtomicBool,
    /// What a verify asked of the applier: hold, or snapshot at a seq.
    capture: Mutex<Capture>,
    /// Wakes an applier waiting on a hold.
    capture_changed: Notify,
    /// One verify run at a time.
    pub verify_lock: tokio::sync::Mutex<()>,
    last_verify: Mutex<Option<VerifyReport>>,
    /// `ts_ms` of the last applied entry; 0 = unknown (right after a bootstrap).
    // Read by the replication lag status (Plan 3 Task 7), which also keeps it current.
    #[allow(dead_code)]
    pub last_applied_ts: AtomicU64,
    /// Wakes the tail loop when a verify asks for a re-bootstrap.
    pub rebootstrap: Notify,
}

impl Inner {
    pub(crate) fn new(store: Store, cfg: SlaveConfig, meta: &MetaSnapshot) -> Result<Inner> {
        let node_state = NodeState::decode(meta.node_state.as_deref())?;
        let ident = match (meta.identity, node_state) {
            (_, NodeState::Bootstrapping) | (None, _) => None,
            (Some(_), _) if meta.epoch_history.is_empty() => {
                return Err(Error::corrupt(
                    "origin_identity without epoch_history in __meta",
                ));
            }
            (Some(id), _) => Some((id, meta.epoch_history.clone())),
        };
        let state = match node_state {
            NodeState::Halted(r) => SlaveState::Halted(r),
            NodeState::Ready | NodeState::Bootstrapping => SlaveState::Connecting,
        };
        let last_seq = store.position().last_seq;
        // The entry at last_seq, if still in the log, gives the lag's time base after a restart.
        let last_applied_ts = match last_seq {
            0 => 0,
            s => store
                .read_entries(s, 1, usize::MAX)
                .ok()
                .and_then(|v| v.first().map(|e| e.ts_ms))
                .unwrap_or(0),
        };
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
            diverged_on_stream: AtomicBool::new(false),
            capture: Mutex::new(Capture::Idle),
            capture_changed: Notify::new(),
            verify_lock: tokio::sync::Mutex::new(()),
            last_verify: Mutex::new(None),
            last_applied_ts: AtomicU64::new(last_applied_ts),
            rebootstrap: Notify::new(),
        })
    }

    /// The current upstream's address.
    pub(crate) fn upstream_uri(&self) -> String {
        self.upstream().uri()
    }

    pub(crate) fn upstream(&self) -> UpstreamConfig {
        self.upstream
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_upstream(&self, up: UpstreamConfig) {
        *self
            .upstream
            .write()
            .unwrap_or_else(PoisonError::into_inner) = up;
    }

    pub(crate) fn pending_repoint(&self) -> std::sync::MutexGuard<'_, Option<PendingRepoint>> {
        self.pending_repoint
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn policies(&self) -> Policies {
        Policies {
            identity: self.cfg.on_identity_change,
            divergence: self.cfg.on_divergence,
            gap: self.cfg.on_log_gap,
        }
    }

    pub(crate) fn ident(&self) -> Option<(DbIdentity, Vec<EpochStart>)> {
        self.ident
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn set_ident(&self, v: Option<(DbIdentity, Vec<EpochStart>)>) {
        *self.ident.write().unwrap_or_else(PoisonError::into_inner) = v;
    }

    /// This slave's newest epoch records, up to the epoch it follows (at most
    /// `KNOWN_EPOCHS`, oldest first; empty without data).
    pub(crate) fn known_epochs(&self) -> Vec<EpochStart> {
        self.ident()
            .map(|(id, history)| newest_records(&history, id.epoch).to_vec())
            .unwrap_or_default()
    }

    /// Readable and servable: the slave holds a consistent state at some seq.
    pub(crate) fn is_ready(&self) -> bool {
        self.ident
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
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
        self.upstream_chain
            .write()
            .unwrap_or_else(PoisonError::into_inner)
            .clear();
    }

    pub(crate) fn upstream_chain(&self) -> Vec<String> {
        self.upstream_chain
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    /// Records what the upstream said about itself in a handshake. Returns the upstream's
    /// `max_entry_bytes` if it changed, for the caller to persist off the async thread.
    pub(crate) fn note_upstream(&self, r: &crate::pb::HandshakeResponse) -> Option<u64> {
        let mut chain = vec![r.server_id.clone()];
        chain.extend(r.upstream_chain.iter().cloned());
        *self
            .upstream_chain
            .write()
            .unwrap_or_else(PoisonError::into_inner) = chain;
        self.note_positions(r.last_seq, r.origin_last_seq);
        self.note_contact();
        let changed = r.max_entry_bytes > 0
            && self
                .max_entry_bytes
                .swap(r.max_entry_bytes, Ordering::SeqCst)
                != r.max_entry_bytes;
        changed.then_some(r.max_entry_bytes)
    }

    pub(crate) fn note_positions(&self, upstream_last_seq: Seq, origin_last_seq: Seq) {
        self.upstream_last_seq
            .store(upstream_last_seq, Ordering::SeqCst);
        self.origin_last_seq
            .fetch_max(origin_last_seq, Ordering::SeqCst);
    }

    pub(crate) fn note_contact(&self) {
        *self
            .last_contact
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(SystemTime::now());
    }

    pub(crate) fn set_last_error(&self, e: Option<String>) {
        *self
            .last_error
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = e;
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
            origin_last_seq: self
                .origin_last_seq
                .load(Ordering::SeqCst)
                .max(pos.last_seq),
            upstream: self.upstream_uri(),
            upstream_chain: self.upstream_chain(),
            last_contact: *self
                .last_contact
                .lock()
                .unwrap_or_else(PoisonError::into_inner),
            last_error: self
                .last_error
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .clone(),
            listener_failed: self.listener_failed.load(Ordering::SeqCst),
            last_verify: self.last_verify(),
        }
    }
}

/// Why a verify waiting for seq `S` got no snapshot from the applier.
pub(crate) enum CaptureMiss {
    /// The applier committed this later seq without ever being at `S` while asked.
    Passed(Seq),
    /// Cancelled (a bootstrap began), or the snapshot could not be taken.
    Cancelled,
}

pub(crate) type CaptureReply = std::result::Result<ReadSnapshot, CaptureMiss>;

/// What a verify asked of the applier (DESIGN.md §12).
#[derive(Default)]
pub(crate) enum Capture {
    #[default]
    Idle,
    /// Commit nothing more until the upstream's snapshot seq is known.
    Hold,
    /// Take a snapshot right after committing `seq`.
    At {
        seq: Seq,
        tx: oneshot::Sender<CaptureReply>,
    },
    /// The state a held verify was waiting for is gone (a bootstrap began): its target is
    /// answered `Cancelled`, and the applier is not held.
    Cancelled,
}

/// A verify's hold on the applier. While it exists the applier commits nothing past the
/// target set with [`CaptureHold::target`]; dropping it releases the applier.
pub(crate) struct CaptureHold {
    inner: Arc<Inner>,
}

impl CaptureHold {
    /// Lets the applier run up to `seq`, snapshot right after committing it, and continue.
    ///
    /// Answered at once when the slave is already at or past `seq`: a commit that was under
    /// way when the hold was taken may have landed since. The applier is held, so a snapshot
    /// taken now is exactly the state at `seq`.
    pub(crate) fn target(&self, seq: Seq) -> oneshot::Receiver<CaptureReply> {
        let (tx, rx) = oneshot::channel();
        let mut slot = self.inner.capture();
        if matches!(*slot, Capture::Cancelled) {
            let _ = tx.send(Err(CaptureMiss::Cancelled));
            return rx;
        }
        let applied = self.inner.store.position().last_seq;
        if applied >= seq {
            let reply = if applied == seq {
                self.inner.store.read_snapshot(0).map_err(|e| {
                    tracing::warn!(node = %self.inner.cfg.identifier, seq, error = %e, "verify snapshot failed");
                    CaptureMiss::Cancelled
                })
            } else {
                Err(CaptureMiss::Passed(applied))
            };
            let _ = tx.send(reply);
            return rx;
        }
        *slot = Capture::At { seq, tx };
        drop(slot);
        self.inner.capture_changed.notify_waiters();
        rx
    }
}

impl Drop for CaptureHold {
    fn drop(&mut self) {
        *self.inner.capture() = Capture::Idle;
        self.inner.capture_changed.notify_waiters();
    }
}

impl Inner {
    fn capture(&self) -> std::sync::MutexGuard<'_, Capture> {
        self.capture.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Pauses the applier before its next commit, until the hold sets a target or is dropped.
    /// A verify takes it before asking the upstream for its snapshot, so this slave cannot pass
    /// the snapshot's seq meanwhile.
    pub(crate) fn hold_applier(self: &Arc<Self>) -> CaptureHold {
        *self.capture() = Capture::Hold;
        CaptureHold {
            inner: self.clone(),
        }
    }

    /// Called by the applier before each commit: waits while a verify holds it.
    pub(crate) async fn wait_while_held(&self) {
        loop {
            let notified = self.capture_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !matches!(*self.capture(), Capture::Hold) {
                return;
            }
            notified.await;
        }
    }

    /// Ends any capture (a bootstrap replaces the state it was waiting for): a target already
    /// set is answered `Cancelled`, and so is one a held verify sets later.
    pub(crate) fn cancel_capture(&self) {
        let mut slot = self.capture();
        match std::mem::take(&mut *slot) {
            Capture::At { tx, .. } => {
                let _ = tx.send(Err(CaptureMiss::Cancelled));
                *slot = Capture::Cancelled;
            }
            Capture::Hold | Capture::Cancelled => *slot = Capture::Cancelled,
            Capture::Idle => {}
        }
        drop(slot);
        self.capture_changed.notify_waiters();
    }

    /// Called by the applier on its blocking thread right after committing `seq`, before the
    /// next entry can commit: the snapshot it takes is exactly the state at `seq`.
    pub(crate) fn capture_after_commit(&self, seq: Seq) {
        let tx = {
            let mut slot = self.capture();
            match &*slot {
                Capture::At { seq: want, .. } if *want <= seq => match std::mem::take(&mut *slot) {
                    Capture::At { seq: want, tx } => Some((want, tx)),
                    _ => None,
                },
                _ => None,
            }
        };
        let Some((want, tx)) = tx else { return };
        let reply = if want == seq {
            self.store.read_snapshot(0).map_err(|e| {
                tracing::warn!(node = %self.cfg.identifier, seq, error = %e, "verify snapshot failed");
                CaptureMiss::Cancelled
            })
        } else {
            Err(CaptureMiss::Passed(seq))
        };
        let _ = tx.send(reply);
    }

    pub(crate) fn set_last_verify(&self, report: VerifyReport) {
        *self
            .last_verify
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(report);
    }

    pub(crate) fn last_verify(&self) -> Option<VerifyReport> {
        self.last_verify
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

#[cfg(test)]
mod tests {
    use super::ForcedBootstrap;

    #[test]
    fn an_older_request_never_overwrites_a_newer_one() {
        let f = ForcedBootstrap::default();
        f.request(2);
        f.request(1);
        assert!(f.is_requested_for(2) && !f.is_requested_for(1));
    }

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
        assert!(
            f.is_requested_for(1),
            "clearing another generation is a no-op"
        );
        f.clear(1);
        assert!(!f.is_requested_for(1));
    }
}
