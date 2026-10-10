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
mod repoint;
mod source;
mod state;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::log::{EntryIter, LogApi};
use crate::read::{ReadApi, ReadSnapshot};
use crate::server::replicas::ReplicaStatus;
use crate::server::{ServerHandle, await_sole_owner};
use crate::store::Store;
use crate::store::meta::key;
use crate::store::read::DbIter;
use crate::types::{CfSpec, Direction, IterMode, Role, Seq, ValueMeta};
use inner::Inner;
use state::NodeState;

pub use config::{
    Backoff, DivergencePolicy, FetchConfig, GapPolicy, IdentityPolicy, SlaveConfig, UpstreamConfig,
};
pub use promote::{PromoteRefused, PromotionCheck};
pub use repoint::{AcceptDataLoss, RepointDivergence, RepointOptions, RepointOutcome};
pub use state::{HaltAction, HaltReason, SlaveState, SlaveStatus};

/// A read-only replica. It has no write methods: writing to a slave is a compile error.
///
/// It replicates from its upstream in the background, serves reads from local data
/// (except while bootstrapping, when reads return `Error::NotReady`), and keeps serving
/// reads while halted.
pub struct Slave {
    inner: Arc<Inner>,
    driver: Option<JoinHandle<()>>,
    maintenance: Option<JoinHandle<()>>,
    server: Option<ServerHandle>,
}

impl Slave {
    /// Opens or creates a slave database and starts replicating. Must be called within a
    /// tokio runtime. Local data is readable at once unless a bootstrap was interrupted.
    pub async fn open(cfg: SlaveConfig) -> Result<Self> {
        cfg.validate()?;
        let (store, meta) = Store::open(&cfg.path, Role::Slave, cfg.rocksdb.clone())?;
        let inner = Arc::new(Inner::new(store, cfg, &meta)?);
        let server =
            crate::server::start_for(&inner, inner.cfg.serve.clone(), inner.replicas.clone())
                .await?;
        let driver = driver::spawn(&inner);
        let maintenance = maintenance::spawn(&inner);
        Ok(Slave {
            inner,
            driver: Some(driver),
            maintenance: Some(maintenance),
            server,
        })
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
        crate::background::blocking(move || {
            i.store
                .write_meta(&[(key::NODE_STATE, &NodeState::Ready.encode())], true)
        })
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

    /// A consistent view at the current applied seq. `Err(NotReady)` while bootstrapping.
    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        self.check_ready()?;
        self.inner.store.read_snapshot(self.inner.now_ms())
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
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
        self.inner.shutdown_tx.send_replace(true);
        for task in [self.driver.take(), self.maintenance.take()]
            .into_iter()
            .flatten()
        {
            let _ = task.await;
        }
        if await_sole_owner(&self.inner, Duration::from_secs(10)).await {
            Ok(())
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
        if let Some(server) = self.server.take() {
            server.abort();
        }
        for task in [self.driver.take(), self.maintenance.take()]
            .into_iter()
            .flatten()
        {
            task.abort();
            let _ = task.await;
        }
        if await_sole_owner(&self.inner, Duration::from_secs(10)).await {
            Ok(())
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
        self.inner.store.live_ctx(self.inner.now_ms()).get(&cf, key)
    }

    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>> {
        self.check_ready()?;
        let cf = self.live_cf(cf)?;
        self.inner
            .store
            .live_ctx(self.inner.now_ms())
            .iter(cf, mode, None)
    }

    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>> {
        self.check_ready()?;
        let cf = self.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).iter(
            cf,
            IterMode::From(prefix, Direction::Forward),
            Some(prefix),
        )
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
