//! Role switches of a `Node` (DESIGN.md §13.2). Each takes the write lock, moves the running
//! handle out (the role reads `Switching`), runs the consuming operation, and stores what it
//! returned: the new role, the handed-back old one on a refusal, or `Failed` when the handle
//! was lost. A switch future dropped before it finishes leaves `Failed` (never `Switching`).

use tokio::sync::RwLockWriteGuard;

use super::handle::{Current, Node, open_current};
use crate::error::{Error, Result};

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

impl Node {
    /// Takes the write lock (waiting for running slave-admin calls), checks the role, and
    /// moves it out, publishing `Switching`. A refused check changes nothing.
    async fn begin_switch(
        &self,
        check: impl FnOnce(&Current) -> Result<()>,
    ) -> Result<(Switch<'_>, Current)> {
        let mut guard = self.role.write().await;
        check(&guard)?;
        let old = std::mem::replace(&mut *guard, Current::Switching);
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
}
