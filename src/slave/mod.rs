//! A read-only replica (DESIGN.md §6, §10, §16).

mod apply;
mod bootstrap;
mod client;
pub(crate) use client::connect_upstream;
mod config;
mod driver;
mod inner;
mod maintenance;
mod policy;
mod promote;
mod rejoin;
mod repair;
mod repoint;
mod source;
mod state;
mod verifier;

use std::net::SocketAddr;
use std::sync::{Arc, PoisonError};
use std::time::Duration;

use tokio::sync::{oneshot, watch};
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::log::{EntryIter, LogApi};
use crate::read::{ReadApi, ReadSnapshot};
use crate::server::replicas::ReplicaStatus;
use crate::server::{ServerHandle, await_sole_owner};
use crate::store::Store;
use crate::store::read::DbIter;
use crate::types::{CfSpec, Direction, IterMode, LogEntry, Role, Seq, ValueMeta};
use crate::verify::{VerifyMode, VerifyReport};
use inner::Inner;
use state::NodeState;

pub use config::{
    Backoff, DivergencePolicy, FetchConfig, GapPolicy, IdentityPolicy, SlaveConfig, UpstreamConfig,
};
pub use promote::{PromoteRefused, PromotionCheck};
pub use rejoin::RejoinOptions;
pub use repoint::{AcceptDataLoss, RepointDivergence, RepointOptions, RepointOutcome};
pub use state::{HaltAction, HaltReason, SlaveState, SlaveStatus};

/// A read-only replica. It has no write methods: writing to a slave is a compile error.
///
/// It replicates from its upstream in the background, serves reads from local data
/// (except while bootstrapping, or while a repair replays up to its upstream's snapshot, when
/// reads return `Error::NotReady`), and keeps serving reads while halted.
pub struct Slave {
    inner: Arc<Inner>,
    driver: Option<JoinHandle<()>>,
    maintenance: Option<JoinHandle<()>>,
    verifier: Option<JoinHandle<()>>,
    server: Option<ServerHandle>,
}

impl Slave {
    /// Opens or creates a slave database and starts replicating. Must be called within a
    /// tokio runtime. Local data is readable at once unless a bootstrap was interrupted.
    pub async fn open(cfg: SlaveConfig) -> Result<Self> {
        Self::start(cfg, None).await.map(|(slave, _)| slave)
    }

    /// Opens the slave. With `settle`, the first handshake is armed as a repoint (generation 0)
    /// with that divergence mode before the driver starts, so it never follows
    /// `cfg.on_divergence`; the receiver gets that handshake's outcome, exactly as
    /// `Slave::repoint` returns it (`Slave::rejoin`).
    async fn start(
        cfg: SlaveConfig,
        settle: Option<RepointDivergence>,
    ) -> Result<(Self, Option<oneshot::Receiver<Result<RepointOutcome>>>)> {
        cfg.validate()?;
        let (store, meta) = Store::open(&cfg.path, Role::Slave, cfg.rocksdb.clone())?;
        let inner = Arc::new(Inner::new(store, cfg, &meta)?);
        let settled = settle.map(|mode| {
            let (reply, settled) = oneshot::channel();
            // Same lock order as `Slave::repoint`: `pending`, then the mode.
            let mut pending = inner.pending_repoint();
            *inner
                .repoint_divergence
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(mode);
            *pending = Some(repoint::PendingRepoint {
                generation: *inner.repoint_tx.borrow(),
                halt_error: None,
                reply,
            });
            settled
        });
        let server =
            crate::server::start_for(&inner, inner.cfg.serve.clone(), inner.replicas.clone())
                .await?;
        let driver = driver::spawn(&inner);
        let maintenance = maintenance::spawn(&inner);
        let verifier = verifier::spawn(&inner);
        Ok((
            Slave {
                inner,
                driver: Some(driver),
                maintenance: Some(maintenance),
                verifier,
                server,
            },
            settled,
        ))
    }

    /// Direct downstream replicas of this slave (see `Master::replicas`). Empty when it does
    /// not serve replication.
    pub fn replicas(&self) -> Vec<ReplicaStatus> {
        match &self.server {
            Some(s) => self.inner.replicas.status(
                &*self.inner,
                self.inner.now_ms(),
                s.replica_lag_time_max(),
            ),
            None => Vec::new(),
        }
    }

    /// The address this slave serves downstream replicas on, if `serve` is configured.
    pub fn serve_addr(&self) -> Option<SocketAddr> {
        self.server.as_ref().map(ServerHandle::addr)
    }

    pub fn status(&self) -> SlaveStatus {
        self.inner.status()
    }

    /// Last applied seq.
    pub fn applied_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }

    /// Last applied seq (the same as `LogApi::last_seq` and [`applied_seq`](Self::applied_seq),
    /// without the import).
    pub fn last_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }

    /// Waits until the slave is `Streaming` (connected and tailing its upstream).
    /// `Err(Halted(reason))` if it halts first, `Err(Timeout)` after `timeout`.
    pub async fn wait_streaming(&self, timeout: Duration) -> Result<()> {
        let mut states = self.inner.state_tx.subscribe();
        let wait = async {
            loop {
                match &*states.borrow_and_update() {
                    SlaveState::Streaming => return Ok(()),
                    SlaveState::Halted(r) => return Err(Error::Halted(*r)),
                    _ => {}
                }
                if states.changed().await.is_err() {
                    return Err(Error::NotReady);
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| Error::Timeout)?
    }

    /// Waits until `applied_seq() >= seq`. `Err(Halted(reason))` if the slave halts first,
    /// `Err(Timeout)` after `timeout`.
    pub async fn wait_for_seq(&self, seq: Seq, timeout: Duration) -> Result<()> {
        let mut seqs = self.inner.seq_tx.subscribe();
        let mut states = self.inner.state_tx.subscribe();
        let wait = async {
            loop {
                if *seqs.borrow_and_update() >= seq {
                    return Ok(());
                }
                if let SlaveState::Halted(r) = *states.borrow_and_update() {
                    return Err(Error::Halted(r));
                }
                tokio::select! {
                    r = seqs.changed() => if r.is_err() { return Err(Error::NotReady) },
                    r = states.changed() => if r.is_err() { return Err(Error::NotReady) },
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| Error::Timeout)?
    }

    /// Compares this slave's data with its upstream's now (DESIGN.md §12) and returns the
    /// report. The report is also stored in [`SlaveStatus::last_verify`] and passed to the
    /// listener's `on_verify_complete`; under `MismatchPolicy::Rebootstrap` a report with
    /// mismatches makes the slave bootstrap, under `MismatchPolicy::Repair` it takes the
    /// reported keys from the upstream (DESIGN.md §12.1). `cfg.verify` sets range sizes, the
    /// rate limit and the policy; `mode` overrides `cfg.verify.mode`. One run at a time: a
    /// concurrent call waits for the first.
    ///
    /// Errors: `NotReady` without data, or while a repair replays; `VerifyAborted(_)` when no
    /// report could be made
    /// (`Busy`, `SessionExpired`, `IdentityChanged`, `PassedSnapshot`, `CaptureTimeout`,
    /// `Upstream`); these leave `last_verify` unchanged.
    pub async fn verify_now(&self, mode: VerifyMode) -> Result<VerifyReport> {
        verifier::verify(&self.inner, mode).await
    }

    /// On a slave halted `Diverged`: its local entries past the fork point, for salvage
    /// (DESIGN.md §12.1). The fork point is recorded when a handshake answer halts the slave,
    /// from the epoch histories (or, for a slave ahead of its master, the master's
    /// `last_seq + 1`), and forgotten when the halt is cleared. `Err(Config)` if the slave is
    /// not halted `Diverged`, or the fork point is not known (neither shows it, or the halt
    /// came from a Tail stream that ended `DIVERGED`; `clear_halt(Retry)` handshakes again);
    /// `Err(Gap)` if local retention already removed part of the suffix.
    /// Reads the whole suffix into memory.
    pub fn diverged_entries(&self) -> Result<Vec<LogEntry>> {
        if self.inner.state() != SlaveState::Halted(HaltReason::Diverged) {
            return Err(Error::Config("the slave is not halted Diverged".into()));
        }
        let fork = self.inner.store.diverged_at()?.ok_or_else(|| {
            Error::Config("the fork point is not known: the epoch histories do not show it".into())
        })?;
        self.inner.store.read_entries(fork, usize::MAX, usize::MAX)
    }

    pub fn watch_state(&self) -> watch::Receiver<SlaveState> {
        self.inner.state_tx.subscribe()
    }

    /// Operator action: leaves a halt. `Retry` handshakes again with the current data (after
    /// fixing the cause, for example registering a missing comparator and reopening);
    /// `Rebootstrap { accept_loss: AcceptDataLoss }` discards local data and bootstraps — the
    /// only way out of `Diverged`, or of `IdentityChanged` under `IdentityPolicy::Refuse`, apart
    /// from `repoint`. It carries the consent to lose data in its signature. `Err(Config)` if
    /// not halted.
    pub async fn clear_halt(&self, action: HaltAction) -> Result<()> {
        // The repoint generation is read together with the halt check: a repoint landing during
        // the blocking write below leaves a forced bootstrap requested for the older generation
        // inert (DESIGN.md §13.1: only a `clear_halt` issued after the repoint applies).
        let generation = *self.inner.repoint_tx.borrow();
        let SlaveState::Halted(reason) = self.inner.state() else {
            return Err(Error::Config("the slave is not halted".into()));
        };
        let i = self.inner.clone();
        // The halt is gone, and with it the fork point it recorded (one synced batch): a later
        // `Diverged` halt records its own (or none), never this stale one.
        crate::background::blocking(move || i.store.clear_halt_meta(&NodeState::Ready.encode()))
            .await?;
        if matches!(action, HaltAction::Rebootstrap { .. }) {
            self.inner.force_bootstrap.request(generation);
        }
        tracing::info!(node = %self.inner.cfg.identifier, reason = reason.name(), ?action, "halt cleared");
        self.inner.set_last_error(None);
        self.inner.set_state(SlaveState::Connecting);
        self.inner.halt_cleared.notify_one();
        Ok(())
    }

    /// A consistent view at the current applied seq. `Err(NotReady)` while bootstrapping or
    /// while a repair replays.
    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        self.check_ready()?;
        // Test hook: between the readiness check and the snapshot.
        crate::failpoint::fire("slave-snapshot-after-check");
        let snap = self.inner.store.read_snapshot(self.inner.now_ms())?;
        // A repair marks the slave not ready before its rewind becomes visible: checked again
        // after the snapshot, a snapshot that could hold the rewind is never handed out.
        self.check_ready()?;
        Ok(snap)
    }

    /// A missing CF on a node that stopped being ready (a bootstrap began) is `NotReady`.
    fn live_cf(&self, cf: &str) -> Result<Arc<crate::store::cfs::PhysCf>> {
        match self.inner.store.live_cf(cf) {
            Err(Error::NoSuchCf(_)) if !self.inner.is_ready() => Err(Error::NotReady),
            r => r,
        }
    }

    fn check_ready(&self) -> Result<()> {
        if self.inner.is_ready() {
            Ok(())
        } else {
            Err(Error::NotReady)
        }
    }

    /// Stops replication and serving and waits until every task has released the slave
    /// (`Err(Timeout)` after 10 s). It does not wait for `ReadSnapshot`s: each keeps the
    /// database open until it is dropped.
    ///
    /// A bootstrap job or verify computation of this slave's server that cannot be stopped
    /// within 5 s makes this return `Err(Timeout)`; the rest of the shutdown still completes.
    pub async fn shutdown(mut self) -> Result<()> {
        // A server that could not stop its computations still lets the rest shut down, but
        // the result is an error: the database may still be referenced.
        let served = match self.server.take() {
            Some(server) => server.shutdown().await,
            None => Ok(()),
        };
        self.inner.shutdown_tx.send_replace(true);
        for task in [
            self.driver.take(),
            self.maintenance.take(),
            self.verifier.take(),
        ]
        .into_iter()
        .flatten()
        {
            let _ = task.await;
        }
        // A verify run's blocking scans stop at their next key (the run set their stop flag when
        // it was cancelled); each references the database, so wait for them before closing.
        if !self.inner.verify_work.idle(Duration::from_secs(5)).await {
            tracing::warn!(node = %self.inner.cfg.identifier, "verify scans did not stop in 5 s");
            return Err(Error::Timeout);
        }
        if await_sole_owner(&self.inner, Duration::from_secs(10)).await {
            served
        } else {
            Err(Error::Timeout)
        }
    }
}

#[cfg(feature = "failpoints")]
impl Slave {
    /// Test-only (feature `failpoints`): stops every task at once without any graceful step
    /// and waits until the database is closed, emulating a process crash.
    pub async fn kill(mut self) -> Result<()> {
        self.inner.shutdown_tx.send_replace(true);
        // Waits for the server's blocking work (bootstrap jobs, verify computations): each
        // references the database.
        let served = match self.server.take() {
            Some(server) => server.abort().await,
            None => Ok(()),
        };
        for task in [
            self.driver.take(),
            self.maintenance.take(),
            self.verifier.take(),
        ]
        .into_iter()
        .flatten()
        {
            task.abort();
            let _ = task.await;
        }
        if !self.inner.verify_work.idle(Duration::from_secs(5)).await {
            return Err(Error::Timeout);
        }
        if await_sole_owner(&self.inner, Duration::from_secs(10)).await {
            served
        } else {
            Err(Error::Timeout)
        }
    }
}

impl Drop for Slave {
    fn drop(&mut self) {
        self.inner.shutdown_tx.send_replace(true);
    }
}

impl ReadApi for Slave {
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        self.check_ready()?;
        let cf = self.live_cf(cf)?;
        let v = self
            .inner
            .store
            .live_ctx(self.inner.now_ms())
            .get(&cf, key)?;
        // Checked again after the read, as `snapshot` does (a repair's rewind may have landed).
        self.check_ready()?;
        Ok(v)
    }

    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>> {
        self.check_ready()?;
        let cf = self.live_cf(cf)?;
        let it = self
            .inner
            .store
            .live_ctx(self.inner.now_ms())
            .iter(cf, mode, None)?;
        // The iterator reads a fixed view taken now: checked again, as `snapshot` does.
        self.check_ready()?;
        Ok(it)
    }

    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>> {
        self.check_ready()?;
        let cf = self.live_cf(cf)?;
        let it = self.inner.store.live_ctx(self.inner.now_ms()).iter(
            cf,
            IterMode::From(prefix, Direction::Forward),
            Some(prefix),
        )?;
        self.check_ready()?;
        Ok(it)
    }

    /// Empty while bootstrapping.
    fn list_cfs(&self) -> Vec<CfSpec> {
        self.inner
            .store
            .cfs
            .live_entries()
            .into_iter()
            .map(|e| e.spec)
            .collect()
    }
}

impl LogApi for Slave {
    fn last_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }

    fn first_retained_seq(&self) -> Seq {
        self.inner.store.first_retained_seq()
    }

    fn entries_since(&self, seq: Seq) -> Result<EntryIter<'_>> {
        self.inner.store.entries_after(seq)
    }

    fn subscribe(&self) -> watch::Receiver<Seq> {
        self.inner.seq_tx.subscribe()
    }
}
