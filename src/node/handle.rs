//! The `Node` handle: either role behind one `RwLock`, with its state published on a watch
//! channel (DESIGN.md §13.2).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{RwLock, RwLockReadGuard, watch};
use tokio::task::JoinHandle;

use super::config::NodeConfig;
use super::fence;
use super::types::{FenceReason, NodeState};
use crate::background::blocking;
use crate::config::WriteConcern;
use crate::error::{Error, Result};
use crate::master::{Master, ReapStats};
use crate::read::{ReadApi, ReadSnapshot};
use crate::slave::{
    HaltAction, RepointOptions, RepointOutcome, Slave, SlaveConfig, SlaveState, SlaveStatus,
    UpstreamConfig,
};
use crate::store::offline::stored_meta;
use crate::types::{Batch, CfSpec, DbIdentity, LogEntry, Role, Seq, ValueMeta};
use crate::verify::{VerifyMode, VerifyReport};

/// `Error::NodeFailed`'s text after `Node::shutdown` (its Display adds "call Node::reopen").
pub(super) const STOPPED: &str = "the node was shut down";

/// What the node holds.
pub(super) enum Current {
    Master(Master),
    Slave(Slave),
    Fenced(FenceReason),
    Switching,
    Failed(String),
    Stopped,
}

impl Current {
    /// The error for a call this role cannot serve: a write on a slave is `NotMaster`, a
    /// slave-only call on a master is `NotSlave`.
    pub(super) fn unavailable(&self) -> Error {
        match self {
            Current::Master(_) => Error::NotSlave,
            Current::Slave(_) => Error::NotMaster,
            Current::Fenced(reason) => Error::Fenced {
                reason: reason.clone(),
            },
            Current::Switching => Error::Switching,
            Current::Failed(e) => Error::NodeFailed(e.clone()),
            Current::Stopped => Error::NodeFailed(STOPPED.into()),
        }
    }
}

/// The task forwarding a slave's states, and the generation it was started for: a forwarder
/// of an older generation never publishes (checked under this mutex).
#[derive(Default)]
struct Forward {
    generation: u64,
    task: Option<JoinHandle<()>>,
}

/// One database directory that runs as master or slave and changes role only through explicit
/// calls (DESIGN.md §13.2). Share it as `Arc<Node>`: every caller sees a role change on its
/// next call.
///
/// Sync calls never wait behind a role switch: while a switch holds or waits for the lock they
/// return `Error::Switching`. Async slave-admin calls (`repoint`, `clear_halt`, `verify_now`,
/// `wait_*`) hold the node across their await, and a switch waits for them. A stored master
/// opens **fenced** (no epoch, no port, no reads or writes) until a peer check clears it or
/// `NodeConfig::unchecked_master` is given.
///
/// # Read-modify-write
///
/// There is no `update`, compare-and-set or merge. **Between your `get` and your `put`, another
/// writer can change the key, and that change will be lost.** Hold your own lock around
/// get → compute → put (DESIGN.md §5.1), as with [`Master`].
pub struct Node {
    pub(super) cfg: NodeConfig,
    /// The slave role's upstream for later opens of that role: `cfg.slave.upstream` until a
    /// `repoint` (or `rejoin_as_slave`) replaces it.
    pub(super) upstream: Mutex<UpstreamConfig>,
    pub(super) role: RwLock<Current>,
    state_tx: watch::Sender<NodeState>,
    forward: Arc<Mutex<Forward>>,
}

const _: () = {
    fn assert<T: Send + Sync + 'static>() {}
    let _ = assert::<Node>;
};

/// Opens the stored role of `cfg`'s directory: a slave as a slave (with `upstream`); a master
/// directly under `unchecked_master`, else after the fence check (`Fenced` when it does not
/// clear). `Err(Config)` without a stored role. An `Err` of the fence check itself (the local
/// store cannot be read) is returned as is: it never clears the fence.
pub(super) async fn open_current(cfg: &NodeConfig, upstream: &UpstreamConfig) -> Result<Current> {
    let path = cfg.master.path.clone();
    let stored = blocking(move || stored_meta(&path)).await?;
    match stored.map(|m| m.role) {
        None => Err(Error::Config(format!(
            "{} holds no database; use Node::create_master or Node::create_slave",
            cfg.master.path.display()
        ))),
        Some(Role::Slave) => {
            let mut s = cfg.slave.clone();
            s.upstream = upstream.clone();
            Ok(Current::Slave(Slave::open(s).await?))
        }
        Some(Role::Master) if cfg.unchecked_master.is_some() => {
            tracing::warn!(node = %cfg.master.identifier, "opening a stored master without the fence check (AcceptSplitBrainRisk)");
            Ok(Current::Master(Master::open(cfg.master.clone()).await?))
        }
        Some(Role::Master) => {
            match fence::check(&cfg.master.path, &cfg.master.identifier, &cfg.peers, false).await? {
                None => Ok(Current::Master(Master::open(cfg.master.clone()).await?)),
                Some(reason) => {
                    tracing::warn!(node = %cfg.master.identifier, %reason, "stored master opens fenced");
                    Ok(Current::Fenced(reason))
                }
            }
        }
    }
}

impl Node {
    /// Creates a new master database. Only for a directory with no stored role (missing or
    /// empty); otherwise `Err(Config)`. No fence check.
    pub async fn create_master(cfg: NodeConfig) -> Result<Node> {
        cfg.validate()?;
        refuse_existing(&cfg).await?;
        let master = Master::open(cfg.master.clone()).await?;
        Ok(Node::with(cfg, Current::Master(master)))
    }

    /// Creates a new slave database that bootstraps from `cfg.slave.upstream`. Only for a
    /// directory with no stored role; otherwise `Err(Config)`.
    pub async fn create_slave(cfg: NodeConfig) -> Result<Node> {
        cfg.validate()?;
        refuse_existing(&cfg).await?;
        let slave = Slave::open(cfg.slave.clone()).await?;
        Ok(Node::with(cfg, Current::Slave(slave)))
    }

    /// Opens an existing database in its stored role. A stored slave opens as a slave. A
    /// stored master runs the fence check once (`cfg.peers`, DESIGN.md §13.2): when it clears,
    /// the master opens (starting its epoch); otherwise the node is returned **fenced**
    /// (`NodeState::Fenced { reason }`) and nothing re-checks automatically: call
    /// `Node::unfence` or `Node::rejoin_as_slave`. With `cfg.unchecked_master` the master
    /// opens without the check. `Err(Config)` when the directory holds no database (use
    /// `create_*`); `Err` too when the check cannot read the local store (nothing opens).
    pub async fn open(cfg: NodeConfig) -> Result<Node> {
        cfg.validate()?;
        let current = open_current(&cfg, &cfg.slave.upstream).await?;
        Ok(Node::with(cfg, current))
    }

    /// Wraps a running master. `Err(Config)` (the master is shut down) if `cfg` is invalid or
    /// names another directory.
    pub async fn from_master(master: Master, cfg: NodeConfig) -> Result<Node> {
        let checked = cfg.validate().and_then(|()| same_dir(master.path(), &cfg));
        if let Err(e) = checked {
            let _ = master.shutdown().await;
            return Err(e);
        }
        Ok(Node::with(cfg, Current::Master(master)))
    }

    /// Wraps a running slave; later reopens of the slave role use `cfg.slave.upstream`.
    /// `Err(Config)` (the slave is shut down) if `cfg` is invalid, names another directory,
    /// or names another upstream than the one the slave follows (`SlaveStatus::upstream`).
    pub async fn from_slave(slave: Slave, cfg: NodeConfig) -> Result<Node> {
        let checked = cfg
            .validate()
            .and_then(|()| same_dir(slave.path(), &cfg))
            .and_then(|()| {
                let follows = slave.status().upstream;
                if follows == cfg.slave.upstream.uri() {
                    Ok(())
                } else {
                    Err(Error::Config(format!(
                        "the slave follows {follows}, NodeConfig names {}",
                        cfg.slave.upstream.uri()
                    )))
                }
            });
        if let Err(e) = checked {
            let _ = slave.shutdown().await;
            return Err(e);
        }
        Ok(Node::with(cfg, Current::Slave(slave)))
    }

    fn with(cfg: NodeConfig, current: Current) -> Node {
        let node = Node {
            upstream: Mutex::new(cfg.slave.upstream.clone()),
            cfg,
            role: RwLock::new(Current::Switching),
            state_tx: watch::channel(NodeState::Switching).0,
            forward: Arc::default(),
        };
        node.publish(&current);
        *node.role.try_write().expect("not shared yet") = current;
        node
    }

    /// The exact current state.
    pub fn state(&self) -> NodeState {
        self.state_tx.borrow().clone()
    }

    /// Every state change, including the slave's own states (forwarded live).
    pub fn watch_state(&self) -> watch::Receiver<NodeState> {
        self.state_tx.subscribe()
    }

    /// Publishes the state of `cur`; for a slave, starts forwarding its states. Stops the
    /// previous forwarder first (under the same mutex, so none publishes after this).
    pub(super) fn publish(&self, cur: &Current) {
        let mut fwd = self.forward.lock().unwrap_or_else(PoisonError::into_inner);
        fwd.generation += 1;
        if let Some(task) = fwd.task.take() {
            task.abort();
        }
        let state = match cur {
            Current::Master(m) => NodeState::Master {
                identity: m.identity(),
            },
            Current::Slave(s) => {
                let mut rx = s.watch_state();
                let first = rx.borrow_and_update().clone();
                fwd.task = Some(forward_slave_states(
                    rx,
                    s.upstream_reader(),
                    self.state_tx.clone(),
                    self.forward.clone(),
                    fwd.generation,
                ));
                NodeState::Slave {
                    state: first,
                    upstream: s.status().upstream,
                }
            }
            Current::Fenced(reason) => NodeState::Fenced {
                reason: reason.clone(),
            },
            Current::Switching => NodeState::Switching,
            Current::Failed(error) => NodeState::Failed {
                error: error.clone(),
            },
            Current::Stopped => NodeState::Stopped,
        };
        self.state_tx.send_replace(state);
    }

    /// Re-sends the slave state (after its upstream changed).
    fn refresh_slave(&self, s: &Slave) {
        let _fwd = self.forward.lock().unwrap_or_else(PoisonError::into_inner);
        let status = s.status();
        self.state_tx.send_replace(NodeState::Slave {
            state: status.state,
            upstream: status.upstream,
        });
    }

    /// `cfg.slave` with the current upstream.
    pub(super) fn slave_config(&self) -> SlaveConfig {
        let mut s = self.cfg.slave.clone();
        s.upstream = self
            .upstream
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        s
    }

    fn current(&self) -> Result<RwLockReadGuard<'_, Current>> {
        self.role.try_read().map_err(|_| Error::Switching)
    }

    fn with_reader<T>(&self, f: impl FnOnce(&dyn ReadApi) -> Result<T>) -> Result<T> {
        let g = self.current()?;
        match &*g {
            Current::Master(m) => f(m),
            Current::Slave(s) => f(s),
            other => Err(other.unavailable()),
        }
    }

    fn with_master<T>(&self, f: impl FnOnce(&Master) -> Result<T>) -> Result<T> {
        let g = self.current()?;
        match &*g {
            Current::Master(m) => f(m),
            other => Err(other.unavailable()),
        }
    }

    fn with_slave<T>(&self, f: impl FnOnce(&Slave) -> Result<T>) -> Result<T> {
        let g = self.current()?;
        match &*g {
            Current::Slave(s) => f(s),
            other => Err(other.unavailable()),
        }
    }

    /// For async slave-admin calls: waits behind a running switch, then holds the node.
    async fn slave(&self) -> Result<RwLockReadGuard<'_, Slave>> {
        let g = self.role.read().await;
        RwLockReadGuard::try_map(g, |c| match c {
            Current::Slave(s) => Some(s),
            _ => None,
        })
        .map_err(|g| g.unavailable())
    }

    /// The replication address of the running role; `None` when fenced, failed, switching or
    /// not serving.
    pub fn serve_addr(&self) -> Option<SocketAddr> {
        match &*self.current().ok()? {
            Current::Master(m) => m.serve_addr(),
            Current::Slave(s) => s.serve_addr(),
            _ => None,
        }
    }

    /// The last committed (master) or applied (slave) seq.
    pub fn last_seq(&self) -> Result<Seq> {
        let g = self.current()?;
        match &*g {
            Current::Master(m) => Ok(m.last_seq()),
            Current::Slave(s) => Ok(s.last_seq()),
            other => Err(other.unavailable()),
        }
    }

    // ---- reads: master or slave ----

    /// Reads the current value of `key`, or `None` if absent or expired. The value can change
    /// as soon as this returns: to update a key from its current value, hold an application
    /// lock around get → compute → put (see the [type-level docs](Self#read-modify-write)).
    pub fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.with_reader(|r| r.get(cf, key))
    }

    /// [`ReadApi::get_with_meta`].
    pub fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        self.with_reader(|r| r.get_with_meta(cf, key))
    }

    /// [`ReadApi::multi_get`].
    pub fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        self.with_reader(|r| r.multi_get(cf, keys))
    }

    /// [`ReadApi::list_cfs`].
    pub fn list_cfs(&self) -> Result<Vec<CfSpec>> {
        self.with_reader(|r| Ok(r.list_cfs()))
    }

    /// A consistent view; iterate through it (`ReadApi::iter`). Drop it before a role switch:
    /// a live snapshot keeps the database open and makes the switch fail.
    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        let g = self.current()?;
        match &*g {
            Current::Master(m) => m.snapshot(),
            Current::Slave(s) => s.snapshot(),
            other => Err(other.unavailable()),
        }
    }

    // ---- writes: master only (a slave answers `NotMaster`) ----

    /// [`Master::put`]. Not safe to compute `value` from a previous `get` without an
    /// application lock around both calls (see the [type-level docs](Self#read-modify-write)).
    /// Blocks until fsynced; async callers use `spawn_blocking`.
    pub fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Seq> {
        self.with_master(|m| m.put(cf, key, value))
    }

    /// [`Master::put_with_ttl`].
    pub fn put_with_ttl(&self, cf: &str, key: &[u8], value: &[u8], ttl: Duration) -> Result<Seq> {
        self.with_master(|m| m.put_with_ttl(cf, key, value, ttl))
    }

    /// [`Master::put_with`].
    pub fn put_with(
        &self,
        cf: &str,
        key: &[u8],
        value: &[u8],
        concern: WriteConcern,
    ) -> Result<Seq> {
        self.with_master(|m| m.put_with(cf, key, value, concern))
    }

    /// [`Master::delete`].
    pub fn delete(&self, cf: &str, key: &[u8]) -> Result<Seq> {
        self.with_master(|m| m.delete(cf, key))
    }

    /// [`Master::delete_range`].
    pub fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Seq> {
        self.with_master(|m| m.delete_range(cf, start, end))
    }

    /// [`Master::write`]: one atomic entry. A batch is not a transaction over reads; values
    /// computed from earlier reads can be stale unless the application holds its own lock (see
    /// the [type-level docs](Self#read-modify-write)).
    pub fn write(&self, batch: Batch) -> Result<Seq> {
        self.with_master(|m| m.write(batch))
    }

    /// [`Master::write_with`].
    pub fn write_with(&self, batch: Batch, concern: WriteConcern) -> Result<Seq> {
        self.with_master(|m| m.write_with(batch, concern))
    }

    /// [`Master::create_cf`].
    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq> {
        self.with_master(|m| m.create_cf(spec))
    }

    /// [`Master::drop_cf`].
    pub fn drop_cf(&self, name: &str) -> Result<Seq> {
        self.with_master(|m| m.drop_cf(name))
    }

    /// [`Master::reap_expired`].
    pub fn reap_expired(&self) -> Result<ReapStats> {
        self.with_master(|m| m.reap_expired())
    }

    /// [`Master::bump_epoch`]; the new identity is published in `NodeState::Master`.
    pub fn bump_epoch(&self) -> Result<DbIdentity> {
        self.with_master(|m| {
            let id = m.bump_epoch()?;
            self.state_tx
                .send_replace(NodeState::Master { identity: id });
            Ok(id)
        })
    }

    // ---- slave admin: slave only (a master answers `NotSlave`) ----

    /// [`Slave::repoint`]. The new upstream is also the one later reopens of the slave role
    /// use, unless the address is refused with `Err(Config)` (the slave then still follows the
    /// old one).
    pub async fn repoint(
        &self,
        upstream: UpstreamConfig,
        opts: RepointOptions,
    ) -> Result<RepointOutcome> {
        let s = self.slave().await?;
        let result = s.repoint(upstream.clone(), opts).await;
        // `Slave::repoint` answers `Config` only before it adopts the new upstream.
        if !matches!(result, Err(Error::Config(_))) {
            *self.upstream.lock().unwrap_or_else(PoisonError::into_inner) = upstream;
            self.refresh_slave(&s);
        }
        result
    }

    /// [`Slave::clear_halt`].
    pub async fn clear_halt(&self, action: HaltAction) -> Result<()> {
        self.slave().await?.clear_halt(action).await
    }

    /// [`Slave::verify_now`].
    pub async fn verify_now(&self, mode: VerifyMode) -> Result<VerifyReport> {
        self.slave().await?.verify_now(mode).await
    }

    /// [`Slave::wait_streaming`]. A switch waits for it.
    pub async fn wait_streaming(&self, timeout: Duration) -> Result<()> {
        self.slave().await?.wait_streaming(timeout).await
    }

    /// [`Slave::wait_for_seq`]. A switch waits for it.
    pub async fn wait_for_seq(&self, seq: Seq, timeout: Duration) -> Result<()> {
        self.slave().await?.wait_for_seq(seq, timeout).await
    }

    /// [`Slave::diverged_entries`].
    pub fn diverged_entries(&self) -> Result<Vec<LogEntry>> {
        self.with_slave(|s| s.diverged_entries())
    }

    /// [`Slave::status`].
    pub fn status(&self) -> Result<SlaveStatus> {
        self.with_slave(|s| Ok(s.status()))
    }
}

async fn refuse_existing(cfg: &NodeConfig) -> Result<()> {
    let path = cfg.master.path.clone();
    if blocking(move || stored_meta(&path)).await?.is_some() {
        return Err(Error::Config(format!(
            "{} already holds a database; use Node::open",
            cfg.master.path.display()
        )));
    }
    Ok(())
}

fn same_dir(running: &std::path::Path, cfg: &NodeConfig) -> Result<()> {
    if running == cfg.master.path {
        Ok(())
    } else {
        Err(Error::Config(
            "the handle runs on another directory than NodeConfig names".into(),
        ))
    }
}

/// Forwards every state of a slave as `NodeState::Slave`, with the upstream the slave
/// follows at that moment. Ends when the slave is gone or a newer `publish` ran.
fn forward_slave_states(
    mut rx: watch::Receiver<SlaveState>,
    upstream: impl Fn() -> Option<String> + Send + 'static,
    tx: watch::Sender<NodeState>,
    forward: Arc<Mutex<Forward>>,
    generation: u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let state = rx.borrow_and_update().clone();
            let fwd = forward.lock().unwrap_or_else(PoisonError::into_inner);
            if fwd.generation != generation {
                return;
            }
            let Some(upstream) = upstream() else {
                return;
            };
            tx.send_replace(NodeState::Slave { state, upstream });
        }
    })
}
