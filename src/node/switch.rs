//! Role switches of a `Node` (DESIGN.md §13.2). Each takes the write lock, moves the running
//! handle out (the role reads `Switching`), runs the consuming operation, and stores what it
//! returned: the new role, the handed-back old one on a refusal, or `Failed` when the handle
//! was lost. A switch future dropped before it finishes leaves `Failed` (never `Switching`).
//!
//! After a cancelled switch, the dropped role releases the directory asynchronously (its
//! tasks wind down detached): [`Node::reopen`] may return a RocksDB lock error for a few
//! seconds. Retry it.

use std::sync::PoisonError;

use tokio::sync::RwLockWriteGuard;

use super::fence;
use super::handle::{Current, Node, open_current};
use super::types::UnfenceOptions;
use crate::background::blocking;
use crate::error::{Error, Result};
use crate::master::{DemoteRefused, Master};
use crate::slave::{PromoteRefused, RejoinOptions, RepointOutcome, Slave, UpstreamConfig};
use crate::store::offline::stored_meta;
use crate::types::Role;

/// `Error::NodeFailed`'s text after a dropped switch (its Display adds "call Node::reopen").
const CANCELLED: &str = "a role switch was cancelled before it finished";

/// A running switch: holds the write lock; dropped unfinished, it leaves the node `Failed`.
struct Switch<'a> {
    node: &'a Node,
    guard: RwLockWriteGuard<'a, Current>,
    done: bool,
}

impl Switch<'_> {
    /// Stores `new` as the node's role, publishes it, and returns `result`.
    fn finish<T>(mut self, new: Current, result: Result<T>) -> Result<T> {
        *self.guard = new;
        self.node.publish(&self.guard);
        self.done = true;
        result
    }
}

impl Drop for Switch<'_> {
    fn drop(&mut self) {
        if !self.done {
            tracing::error!(node = %self.node.cfg.master.identifier, "{CANCELLED}");
            *self.guard = Current::Failed(CANCELLED.into());
            self.node.publish(&self.guard);
        }
    }
}

/// `begin_switch` check: only a slave switches.
fn slave_only(c: &Current) -> Result<()> {
    match c {
        Current::Slave(_) => Ok(()),
        // A master answers `NotSlave`.
        other => Err(other.unavailable()),
    }
}

/// `begin_switch` check: only a running master switches.
fn master_only(c: &Current) -> Result<()> {
    match c {
        Current::Master(_) => Ok(()),
        // A slave answers `NotMaster`.
        other => Err(other.unavailable()),
    }
}

impl Node {
    /// Takes the write lock (waiting for running slave-admin calls), checks the role, and
    /// moves it out, publishing `Switching`. A refused check changes nothing. A slave moved out
    /// has its upstream recorded first.
    async fn begin_switch(
        &self,
        check: impl FnOnce(&Current) -> Result<()>,
    ) -> Result<(Switch<'_>, Current)> {
        let mut guard = self.role.write().await;
        check(&guard)?;
        let old = std::mem::replace(&mut *guard, Current::Switching);
        if let Current::Slave(s) = &old {
            // The slave leaves service: keep the upstream it actually follows (a dropped
            // `repoint` future may have changed it) for later opens of the slave role.
            self.record_upstream(s);
        }
        self.publish(&guard);
        Ok((
            Switch {
                node: self,
                guard,
                done: false,
            },
            old,
        ))
    }

    /// Opens the stored role again after `Failed` or [`shutdown`](Self::shutdown), exactly
    /// like [`Node::open`] (a stored master is fence-checked and may come back fenced). `Ok`
    /// does not say which role: see [`state`](Self::state). On `Err` the node stays as it
    /// was. `Err(Config)` in any other state.
    ///
    /// After a cancelled switch (a dropped switch future), the dropped role releases the
    /// directory asynchronously: `reopen` may return a RocksDB lock error for a few seconds
    /// (the node stays `Failed`). Retry it.
    pub async fn reopen(&self) -> Result<()> {
        let (sw, old) = self
            .begin_switch(|c| match c {
                Current::Failed(_) | Current::Stopped => Ok(()),
                _ => Err(Error::Config(
                    "only a failed or shut-down node can be reopened".into(),
                )),
            })
            .await?;
        let upstream = self.slave_config().upstream;
        match open_current(&self.cfg, &upstream).await {
            Ok(current) => sw.finish(current, Ok(())),
            Err(e) => sw.finish(old, Err(e)),
        }
    }

    /// Shuts the running role down (`Master::shutdown` / `Slave::shutdown`); the node is then
    /// `Stopped` (also when the shutdown reports an error, which is returned). Waits for
    /// running slave-admin calls (`wait_*`, `repoint`, ...) first.
    pub async fn shutdown(&self) -> Result<()> {
        let (sw, old) = self.begin_switch(|_| Ok(())).await?;
        let result = match old {
            Current::Master(m) => m.shutdown().await,
            Current::Slave(s) => s.shutdown().await,
            _ => Ok(()),
        };
        sw.finish(Current::Stopped, result)
    }

    /// [`Slave::promote_checked`] with `cfg.peers` and `cfg.master`. **Refused by default**
    /// (a later peer, a reachable master, a peer that would lose entries, an unverifiable
    /// peer, the checks of [`promote`](Self::promote)): the slave keeps running and the node
    /// stays a slave. Only a failure after the slave began shutting down (an I/O error, a live
    /// `ReadSnapshot` from [`snapshot`](Self::snapshot)) leaves the node `Failed`
    /// ([`reopen`](Self::reopen) opens the unchanged slave again). A refusal found on the
    /// stopped store (a repair that committed its rewind meanwhile) reopens the slave, still
    /// following the same upstream, and keeps it. `Err(NotSlave)` on a master; calls answer
    /// `Switching` while the check runs.
    pub async fn promote_checked(&self) -> Result<()> {
        let (sw, old) = self.begin_switch(slave_only).await?;
        let Current::Slave(slave) = old else {
            return sw.finish(old, Err(Error::NotSlave));
        };
        let result = slave
            .promote_checked(self.cfg.master.clone(), &self.cfg.peers)
            .await;
        self.after_promotion(sw, result)
    }

    /// Promotes without asking any peer, like [`Slave::promote`]: **the operator must stop or
    /// fence the old master first** ([`promote_checked`](Self::promote_checked) is the safe
    /// path). A refusal (bootstrapping, no data, a repair replaying, halted
    /// `Corrupt`/`SpecMismatch`) keeps the slave running; as for `promote_checked`, only a
    /// failure after the slave began shutting down leaves the node `Failed`. `Err(NotSlave)`
    /// on a master.
    pub async fn promote(&self) -> Result<()> {
        let (sw, old) = self.begin_switch(slave_only).await?;
        let Current::Slave(slave) = old else {
            return sw.finish(old, Err(Error::NotSlave));
        };
        let result = slave.promote_keeping(self.cfg.master.clone()).await;
        self.after_promotion(sw, result)
    }

    /// Stores the promoted master, the handed-back slave (recording the upstream it follows,
    /// the source of truth), or `Failed` when the slave handle was lost.
    fn after_promotion(
        &self,
        sw: Switch<'_>,
        result: std::result::Result<Master, PromoteRefused>,
    ) -> Result<()> {
        match result {
            Ok(master) => sw.finish(Current::Master(master), Ok(())),
            Err(PromoteRefused {
                error,
                slave: Some(slave),
            }) => {
                self.record_upstream(&slave);
                sw.finish(Current::Slave(*slave), Err(error))
            }
            Err(PromoteRefused { error, slave: None }) => {
                sw.finish(Current::Failed(error.to_string()), Err(error))
            }
        }
    }

    /// [`Master::demote`] with the node's slave config (and the upstream later opens of the
    /// slave role use): only undoes a promotion that has taken no writes (see
    /// [`Master::can_demote`]). **Refused by default**: the master keeps running and the node
    /// stays a master. Only a failure after the master began shutting down (an I/O error, a
    /// live `ReadSnapshot`) leaves the node `Failed`. `Err(NotMaster)` on a slave.
    pub async fn demote(&self) -> Result<()> {
        let (sw, old) = self.begin_switch(master_only).await?;
        let Current::Master(master) = old else {
            return sw.finish(old, Err(Error::NotMaster));
        };
        match master.demote(self.slave_config()).await {
            Ok(slave) => sw.finish(Current::Slave(slave), Ok(())),
            Err(DemoteRefused {
                error,
                master: Some(m),
            }) => sw.finish(Current::Master(*m), Err(error)),
            Err(DemoteRefused {
                error,
                master: None,
            }) => sw.finish(Current::Failed(error.to_string()), Err(error)),
        }
    }

    /// Turns this master (running or fenced) into a slave of `upstream` (`None`: the node's
    /// slave upstream) with [`Slave::rejoin`], **without wiping it**. Explicit operator
    /// action. Returns the first handshake's result: `Ok(EpochAdvanced)` (the usual answer
    /// after a failover), `Resumed`, ... when its log is a prefix of the upstream's;
    /// `Err(Diverged)` under the default `RepointDivergence::Refuse`, with the node then a
    /// slave halted `Diverged` (data intact, [`diverged_entries`](Self::diverged_entries);
    /// resolve with [`repoint`](Self::repoint) and `AcceptDataLoss`); `Ok(Retrying)` with
    /// `STALE_EPOCH` when this node holds a newer epoch than the upstream (it was promoted
    /// while the upstream's master ran). `opts.on_divergence` is the only consent it uses,
    /// single-use as for `repoint`: under `Repair` / `Rebootstrap` it resolves `DIVERGED` and
    /// also `STALE_EPOCH`, discarding this node's newer-epoch entries to follow the upstream
    /// (see [`RepointDivergence`](crate::RepointDivergence)).
    ///
    /// A running master is shut down first (a live `ReadSnapshot` fails the call and leaves
    /// the node `Failed`). A refusal before the directory is rewritten (it cannot be opened,
    /// ...) keeps a fenced node fenced; a master that was shut down, or any failure after the
    /// rewrite, leaves the node `Failed` ([`reopen`](Self::reopen) opens the stored role
    /// again). On a successful rejoin, and on a failure after the rewrite, `upstream` is also
    /// the one later opens of the slave role use. `Err(Config)` (nothing changes) for an
    /// invalid `upstream`; `Err(NotMaster)` on a slave.
    pub async fn rejoin_as_slave(
        &self,
        upstream: Option<UpstreamConfig>,
        opts: RejoinOptions,
    ) -> Result<RepointOutcome> {
        // Validated before the switch: a bad argument changes nothing and publishes nothing.
        let mut cfg = self.slave_config();
        if let Some(u) = upstream {
            cfg.upstream = u;
        }
        cfg.validate()?;
        let (sw, old) = self
            .begin_switch(|c| match c {
                Current::Master(_) | Current::Fenced(_) => Ok(()),
                other => Err(other.unavailable()),
            })
            .await?;
        let fenced = match old {
            Current::Master(master) => {
                if let Err(e) = master.shutdown_released().await {
                    return sw.finish(Current::Failed(e.to_string()), Err(e));
                }
                None
            }
            Current::Fenced(reason) => Some(reason),
            other => return sw.finish(other, Err(Error::NotMaster)),
        };
        let target = cfg.upstream.clone();
        match Slave::rejoin(cfg, opts).await {
            Ok((slave, outcome)) => {
                self.record_upstream(&slave);
                sw.finish(Current::Slave(slave), outcome)
            }
            Err(e) => {
                // Refused before the rewrite iff the directory is still a stored master.
                let path = self.cfg.master.path.clone();
                let still_master = blocking(move || stored_meta(&path))
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|m| m.role == Role::Master);
                if !still_master {
                    // The directory is a slave of `target` now: `reopen` must follow it, not
                    // the configured upstream.
                    *self.upstream.lock().unwrap_or_else(PoisonError::into_inner) = target;
                }
                match (fenced, still_master) {
                    (Some(reason), true) => sw.finish(Current::Fenced(reason), Err(e)),
                    _ => sw.finish(Current::Failed(e.to_string()), Err(e)),
                }
            }
        }
    }

    /// Runs the fence check again (DESIGN.md §13.2). When it clears, the master opens (its
    /// epoch starts) and this returns `Ok`. Otherwise `Err(Fenced { reason })` and the node
    /// stays fenced with that reason; an `Err` of the check itself (the local store cannot be
    /// read) is returned and the node stays fenced as it was. `opts.allow_unreachable`
    /// (explicit `AcceptSplitBrainRisk`) skips peers that do not answer; every other reason
    /// still blocks. `Err(Config)` if the node is not fenced (nothing changes). Calls answer
    /// `Switching` while the check runs.
    ///
    /// A master that clears the check but then fails to open (an I/O error, its serve port
    /// taken, ...) leaves the node `Failed`, not fenced: the open may already have started an
    /// epoch. [`reopen`](Self::reopen) then runs the fence check again **without**
    /// `AcceptSplitBrainRisk` (the consent given here is not reused), so the node may come
    /// back fenced and need another `unfence` with consent.
    pub async fn unfence(&self, opts: UnfenceOptions) -> Result<()> {
        let not_fenced = || Error::Config("the node is not fenced".into());
        let (sw, old) = self
            .begin_switch(|c| match c {
                Current::Fenced(_) => Ok(()),
                _ => Err(not_fenced()),
            })
            .await?;
        let Current::Fenced(reason) = old else {
            return sw.finish(old, Err(not_fenced()));
        };
        let c = &self.cfg;
        let checked = fence::check(
            &c.master.path,
            &c.master.identifier,
            &c.peers,
            opts.allow_unreachable.is_some(),
        )
        .await;
        match checked {
            Err(e) => sw.finish(Current::Fenced(reason), Err(e)),
            Ok(Some(reason)) => {
                tracing::warn!(node = %c.master.identifier, %reason, "unfence: the master stays fenced");
                sw.finish(
                    Current::Fenced(reason.clone()),
                    Err(Error::Fenced { reason }),
                )
            }
            Ok(None) => {
                if opts.allow_unreachable.is_some() {
                    tracing::warn!(node = %c.master.identifier, "unfenced with AcceptSplitBrainRisk: unreachable peers were not checked");
                }
                match Master::open(c.master.clone()).await {
                    Ok(master) => sw.finish(Current::Master(master), Ok(())),
                    Err(e) => sw.finish(Current::Failed(e.to_string()), Err(e)),
                }
            }
        }
    }
}
