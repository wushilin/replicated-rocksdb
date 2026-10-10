//! Shared state of one slave: the store plus everything the driver, the API and the
//! replication server (when serving) read.

use std::panic::{AssertUnwindSafe, catch_unwind};
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::SystemTime;

use tokio::sync::{Notify, oneshot, watch};

use super::config::{SlaveConfig, UpstreamConfig};
use super::policy::Policies;
use super::repoint::PendingRepoint;
use super::state::{HaltReason, NodeState, SlaveState, SlaveStatus, lag_ms};
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

/// A re-bootstrap asked for by a verify run (`MismatchPolicy::Rebootstrap`). Unlike
/// `ForcedBootstrap` (the operator's explicit consent to lose data, through `clear_halt`), it is
/// consent only to re-copy from the upstream that was verified: it applies to one repoint
/// generation and one database identity, and it never overrides a DIVERGED or IDENTITY_CHANGED
/// answer (`policy::after_handshake`). It ends when a bootstrap is installed.
#[derive(Default)]
pub(crate) struct VerifyRebootstrap(Mutex<Option<(u64, uuid::Uuid)>>);

impl VerifyRebootstrap {
    fn slot(&self) -> std::sync::MutexGuard<'_, Option<(u64, uuid::Uuid)>> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn request(&self, generation: u64, db_uuid: uuid::Uuid) {
        *self.slot() = Some((generation, db_uuid));
    }

    /// A request for this generation is waiting.
    pub(crate) fn pending_for(&self, generation: u64) -> bool {
        self.slot().is_some_and(|(g, _)| g == generation)
    }

    /// Whether the request covers a handshake in `generation` with an upstream of `db_uuid`
    /// (`None`: the answer carried no identity). A request for another database is dropped.
    pub(crate) fn applies(&self, generation: u64, db_uuid: Option<uuid::Uuid>) -> bool {
        let mut slot = self.slot();
        match *slot {
            Some((g, id)) if g == generation => {
                if db_uuid == Some(id) {
                    true
                } else {
                    if db_uuid.is_some() {
                        *slot = None;
                    }
                    false
                }
            }
            _ => false,
        }
    }

    pub(crate) fn clear(&self) {
        *self.slot() = None;
    }
}

/// Blocking work of verify runs that still references the database (scans on blocking threads
/// outlive a cancelled run for a moment). `Slave::shutdown` waits for it to drain.
#[derive(Default)]
pub(crate) struct VerifyWork(AtomicUsize);

pub(crate) struct WorkGuard(Arc<VerifyWork>);

impl Drop for WorkGuard {
    fn drop(&mut self) {
        self.0.0.fetch_sub(1, Ordering::SeqCst);
    }
}

impl VerifyWork {
    pub(crate) fn enter(self: &Arc<Self>) -> WorkGuard {
        self.0.fetch_add(1, Ordering::SeqCst);
        WorkGuard(self.clone())
    }

    /// True once no blocking work is left, false after `limit`.
    pub(crate) async fn idle(&self, limit: std::time::Duration) -> bool {
        let deadline = tokio::time::Instant::now() + limit;
        while self.0.load(Ordering::SeqCst) > 0 {
            if tokio::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        true
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
    capture: Mutex<CaptureSlot>,
    /// Hands out hold ids.
    capture_ids: AtomicU64,
    /// Wakes an applier waiting on a hold.
    capture_changed: Notify,
    /// One verify run at a time.
    pub verify_lock: tokio::sync::Mutex<()>,
    last_verify: Mutex<Option<VerifyReport>>,
    /// `ts_ms` of the last applied entry; 0 = unknown (right after a bootstrap).
    /// Kept current by the apply path and cleared by a bootstrap install; read by `status`.
    pub last_applied_ts: AtomicU64,
    /// `ts_ms` of the oldest entry received in the current tail batch and not yet applied;
    /// 0 = none. Advanced by the apply path, cleared when the batch or stream ends.
    pub pending_ts: AtomicU64,
    /// Wakes the tail loop when a verify asks for a re-bootstrap.
    pub rebootstrap: Notify,
    /// The re-bootstrap a verify asked for, if any (see [`VerifyRebootstrap`]).
    pub verify_rebootstrap: VerifyRebootstrap,
    /// Blocking work of verify runs; shutdown waits for it.
    pub verify_work: Arc<VerifyWork>,
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
            capture: Mutex::default(),
            capture_ids: AtomicU64::new(0),
            capture_changed: Notify::new(),
            verify_lock: tokio::sync::Mutex::new(()),
            last_verify: Mutex::new(None),
            last_applied_ts: AtomicU64::new(last_applied_ts),
            pending_ts: AtomicU64::new(0),
            rebootstrap: Notify::new(),
            verify_rebootstrap: VerifyRebootstrap::default(),
            verify_work: Arc::default(),
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
            // A halt means the operator decides: a stale automatic consent must not outlive it.
            if matches!(s, SlaveState::Halted(_)) {
                self.verify_rebootstrap.clear();
            }
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
        let origin_last_seq = self
            .origin_last_seq
            .load(Ordering::SeqCst)
            .max(pos.last_seq);
        SlaveStatus {
            state: self.state(),
            origin_identity: self.ident().map(|(id, _)| id),
            applied_seq: pos.last_seq,
            first_retained_seq: self.store.first_retained_seq(),
            upstream_last_seq: self.upstream_last_seq.load(Ordering::SeqCst),
            origin_last_seq,
            lag_ms: lag_ms(
                pos.last_seq,
                origin_last_seq,
                self.pending_ts.load(Ordering::SeqCst),
                self.last_applied_ts.load(Ordering::SeqCst),
                self.now_ms(),
            ),
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
    /// Identifies this hold in the slot: a hold that was replaced by a later one (its run was
    /// cancelled while its `target` call was still queued) must not touch the later one's state.
    id: u64,
}

impl CaptureHold {
    /// Lets the applier run up to `seq`, snapshot right after committing it, and continue.
    ///
    /// Answered at once when the slave is already at or past `seq`: a commit that was under
    /// way when the hold was taken may have landed since. The applier is held, so a snapshot
    /// taken now is exactly the state at `seq`. Blocking when it takes the snapshot: call it
    /// from a blocking thread.
    pub(crate) fn target(&self, seq: Seq) -> oneshot::Receiver<CaptureReply> {
        let (tx, rx) = oneshot::channel();
        let mut slot = self.inner.capture();
        if slot.owner != self.id || matches!(slot.state, Capture::Cancelled) {
            let _ = tx.send(Err(CaptureMiss::Cancelled));
            return rx;
        }
        let applied = self.inner.store.position().last_seq;
        if applied >= seq {
            let _ = tx.send(self.inner.snapshot_at(seq));
            return rx;
        }
        slot.state = Capture::At { seq, tx };
        drop(slot);
        self.inner.capture_changed.notify_waiters();
        rx
    }
}

impl Drop for CaptureHold {
    fn drop(&mut self) {
        let mut slot = self.inner.capture();
        if slot.owner == self.id {
            slot.state = Capture::Idle;
            drop(slot);
            self.inner.capture_changed.notify_waiters();
        }
    }
}

/// The capture slot: the state, and the hold that owns it.
#[derive(Default)]
pub(crate) struct CaptureSlot {
    owner: u64,
    state: Capture,
}

impl Inner {
    fn capture(&self) -> std::sync::MutexGuard<'_, CaptureSlot> {
        self.capture.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// A snapshot of exactly the state at `seq`. The snapshot's own `seq()` is checked: a
    /// commit that landed between the position read and the snapshot would make it later, and
    /// a run on it would report differences that are not there.
    fn snapshot_at(&self, seq: Seq) -> CaptureReply {
        match self.store.read_snapshot(0) {
            Ok(snap) if snap.seq() == seq => Ok(snap),
            Ok(snap) => Err(CaptureMiss::Passed(snap.seq())),
            Err(e) => {
                tracing::warn!(node = %self.cfg.identifier, seq, error = %e, "verify snapshot failed");
                Err(CaptureMiss::Cancelled)
            }
        }
    }

    /// Pauses the applier before its next commit, until the hold sets a target or is dropped.
    /// A verify takes it before asking the upstream for its snapshot, so this slave cannot pass
    /// the snapshot's seq meanwhile.
    pub(crate) fn hold_applier(self: &Arc<Self>) -> CaptureHold {
        let id = self.capture_ids.fetch_add(1, Ordering::SeqCst) + 1;
        let mut slot = self.capture();
        *slot = CaptureSlot {
            owner: id,
            state: Capture::Hold,
        };
        drop(slot);
        CaptureHold {
            inner: self.clone(),
            id,
        }
    }

    /// Called by the applier before each commit: waits while a verify holds it.
    pub(crate) async fn wait_while_held(&self) {
        loop {
            let notified = self.capture_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !matches!(self.capture().state, Capture::Hold) {
                return;
            }
            notified.await;
        }
    }

    /// Ends any capture (a bootstrap replaces the state it was waiting for): a target already
    /// set is answered `Cancelled`, and so is one a held verify sets later.
    pub(crate) fn cancel_capture(&self) {
        let mut slot = self.capture();
        match std::mem::take(&mut slot.state) {
            Capture::At { tx, .. } => {
                let _ = tx.send(Err(CaptureMiss::Cancelled));
                slot.state = Capture::Cancelled;
            }
            Capture::Hold | Capture::Cancelled => slot.state = Capture::Cancelled,
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
            match &slot.state {
                Capture::At { seq: want, .. } if *want <= seq => {
                    match std::mem::take(&mut slot.state) {
                        Capture::At { seq: want, tx } => Some((want, tx)),
                        _ => None,
                    }
                }
                _ => None,
            }
        };
        let Some((want, tx)) = tx else { return };
        let reply = if want == seq {
            self.snapshot_at(seq)
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
    use std::time::Duration;

    use super::*;
    use crate::codec::encode_ops;
    use crate::slave::config::UpstreamConfig;
    use crate::tuning::RocksTuning;
    use crate::types::{LogEntry, Op, Role};

    /// A slave `Inner` over a fresh store; `apply` commits the next entry like the applier.
    fn inner(dir: &std::path::Path) -> Arc<Inner> {
        let (store, meta) = Store::open(dir, Role::Slave, RocksTuning::new()).unwrap();
        let up = UpstreamConfig::new("127.0.0.1", 1);
        let cfg = SlaveConfig::new("t", dir, up);
        Arc::new(Inner::new(store, cfg, &meta).unwrap())
    }

    fn apply(i: &Inner) -> Seq {
        let seq = i.store.position().last_seq + 1;
        let ops = vec![Op::CreateCf {
            spec: crate::types::CfSpec::new(format!("c{seq}")),
        }];
        let e = LogEntry::new(seq, 1, 1_000 * seq, encode_ops(&ops));
        let prepared = i.store.prepare(&e, &ops).unwrap();
        i.store.commit(prepared, false).unwrap();
        i.capture_after_commit(seq);
        seq
    }

    async fn released(i: &Inner) -> bool {
        tokio::time::timeout(Duration::from_millis(500), i.wait_while_held())
            .await
            .is_ok()
    }

    #[tokio::test]
    async fn a_hold_stops_the_applier_until_it_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let i = inner(dir.path());
        assert!(released(&i).await, "nothing holds the applier");
        let hold = i.hold_applier();
        assert!(!released(&i).await, "held");
        drop(hold);
        assert!(released(&i).await, "dropping the hold releases it");
    }

    #[tokio::test]
    async fn a_waiting_applier_wakes_when_the_hold_sets_a_target_or_is_dropped() {
        let dir = tempfile::tempdir().unwrap();
        let i = inner(dir.path());
        for _ in 0..3 {
            apply(&i);
        }
        let hold = i.hold_applier();
        let waiter = tokio::spawn({
            let i = i.clone();
            async move { i.wait_while_held().await }
        });
        tokio::time::sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());
        let rx = hold.target(5);
        tokio::time::timeout(Duration::from_secs(2), waiter)
            .await
            .expect("a target releases the applier up to it")
            .unwrap();
        // The applier commits 4, then 5: the snapshot is taken right after 5.
        apply(&i);
        assert_eq!(apply(&i), 5);
        let snap = rx.await.unwrap().ok().expect("a snapshot");
        assert_eq!(snap.seq(), 5);
        drop(hold);
    }

    #[tokio::test]
    async fn a_target_at_or_behind_the_applied_seq_is_answered_at_once() {
        let dir = tempfile::tempdir().unwrap();
        let i = inner(dir.path());
        for _ in 0..3 {
            apply(&i);
        }
        let hold = i.hold_applier();
        let at = hold.target(3).await.unwrap();
        assert_eq!(at.ok().expect("snapshot").seq(), 3);
        let hold = i.hold_applier();
        match hold.target(2).await.unwrap() {
            Err(CaptureMiss::Passed(3)) => {}
            other => panic!("expected Passed(3), got ok={}", other.is_ok()),
        }
    }

    #[tokio::test]
    async fn cancel_answers_a_pending_target_and_refuses_later_ones() {
        let dir = tempfile::tempdir().unwrap();
        let i = inner(dir.path());
        apply(&i);
        // During a hold, before any target.
        let hold = i.hold_applier();
        i.cancel_capture();
        assert!(
            released(&i).await,
            "a cancelled hold does not hold the applier"
        );
        assert!(matches!(
            hold.target(5).await.unwrap(),
            Err(CaptureMiss::Cancelled)
        ));
        drop(hold);
        // During a pending target.
        let hold = i.hold_applier();
        let rx = hold.target(5);
        i.cancel_capture();
        assert!(matches!(rx.await.unwrap(), Err(CaptureMiss::Cancelled)));
        drop(hold);
        // Nothing to cancel: the next hold works.
        i.cancel_capture();
        let hold = i.hold_applier();
        assert!(!released(&i).await);
        drop(hold);
    }

    /// A hold replaced by a later run (its run was cancelled while its `target` call was
    /// queued) must not release or answer the later run's capture when it is dropped.
    #[tokio::test]
    async fn a_stale_hold_does_not_clobber_the_next_runs_slot() {
        let dir = tempfile::tempdir().unwrap();
        let i = inner(dir.path());
        apply(&i);
        let stale = i.hold_applier();
        let current = i.hold_applier();
        let rx = current.target(4);
        drop(stale);
        assert_eq!(apply(&i), 2);
        apply(&i);
        assert_eq!(apply(&i), 4);
        assert_eq!(rx.await.unwrap().ok().expect("snapshot").seq(), 4);
        let again = i.hold_applier();
        assert!(matches!(
            stale_target(&i).await,
            Err(CaptureMiss::Cancelled)
        ));
        drop((current, again));
    }

    async fn stale_target(i: &Arc<Inner>) -> CaptureReply {
        let old = i.hold_applier();
        let _newer = i.hold_applier();
        old.target(99).await.unwrap()
    }

    /// A halt means the operator decides: it ends a pending verify rebootstrap, so
    /// `clear_halt(Retry)` followed by RESUME just resumes.
    #[test]
    fn any_halt_expires_a_pending_verify_rebootstrap() {
        let dir = tempfile::tempdir().unwrap();
        let i = inner(dir.path());
        let id = uuid::Uuid::from_u128(1);
        i.verify_rebootstrap.request(0, id);
        i.set_state(SlaveState::Streaming);
        assert!(i.verify_rebootstrap.pending_for(0), "other states keep it");
        i.set_state(SlaveState::Halted(HaltReason::Diverged));
        assert!(!i.verify_rebootstrap.pending_for(0));
        assert!(!i.verify_rebootstrap.applies(0, Some(id)));
    }

    #[test]
    fn a_verify_rebootstrap_is_scoped_to_one_generation_and_one_database() {
        let (a, b) = (uuid::Uuid::from_u128(1), uuid::Uuid::from_u128(2));
        let r = VerifyRebootstrap::default();
        assert!(!r.applies(0, Some(a)) && !r.pending_for(0));
        r.request(3, a);
        assert!(r.pending_for(3) && !r.pending_for(4));
        assert!(!r.applies(4, Some(a)), "another repoint generation");
        assert!(
            r.pending_for(3),
            "an older generation's request just stays inert"
        );
        assert!(r.applies(3, Some(a)));
        assert!(
            !r.applies(3, None),
            "an answer without identity does not apply"
        );
        assert!(r.applies(3, Some(a)), "and does not spend it");
        assert!(!r.applies(3, Some(b)), "another database");
        assert!(!r.pending_for(3), "which expires it for good");
        assert!(!r.applies(3, Some(a)));
        r.request(3, a);
        r.clear();
        assert!(!r.applies(3, Some(a)));
    }

    #[tokio::test]
    async fn verify_work_drains() {
        let w = Arc::new(VerifyWork::default());
        assert!(w.idle(Duration::from_millis(10)).await);
        let g = w.enter();
        assert!(!w.idle(Duration::from_millis(50)).await);
        drop(g);
        assert!(w.idle(Duration::from_millis(500)).await);
    }

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
