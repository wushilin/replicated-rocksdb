//! Promotion of a slave to master (DESIGN.md §13.1).

use std::sync::Arc;
use std::time::Duration;

use super::state::{HaltReason, SlaveState};
use super::{Slave, UpstreamConfig};
use crate::admin::{self, compare, Comparison, NodeInfo, Side};
use crate::config::MasterConfig;
use crate::error::{Error, Result};
use crate::master::Master;
use crate::server::status::node_info;
use crate::server::ServerHandle;
use crate::store::Store;
use crate::types::{Role, Seq};

impl Slave {
    /// Turns this slave into the master. **Operator action: stop or fence the old master
    /// first** — the crate cannot detect two live masters, only the divergence they cause.
    ///
    /// Refused with `Error::Config` while bootstrapping, without data, when halted with
    /// `Corrupt` or `SpecMismatch`, or if `cfg.path` is not this slave's path. Every
    /// `ReadSnapshot` taken from this slave must be dropped first: one that is alive keeps
    /// the database open and the promotion fails. In every failure case the slave has been
    /// shut down and its directory is unchanged, so it can be reopened as a slave (after the
    /// last snapshot is dropped).
    ///
    /// Downstream streams end with `UPSTREAM_RESET`. The new master starts epoch
    /// `1 + max(epoch history)` at `applied_seq() + 1` and rebuilds its TTL index, so values
    /// written with a TTL before the promotion are reaped by it.
    pub async fn promote(self, cfg: MasterConfig) -> Result<Master> {
        if let Some(msg) = self.promote_refusal(&cfg) {
            self.shutdown().await?;
            return Err(Error::Config(msg));
        }
        self.promote_now(cfg).await
    }

    /// Why `promote(cfg)` would be refused before it changes anything, if it would.
    fn promote_refusal(&self, cfg: &MasterConfig) -> Option<String> {
        match self.inner.state() {
            SlaveState::Bootstrapping { .. } => Some("cannot promote while bootstrapping".to_string()),
            SlaveState::Halted(r @ (HaltReason::Corrupt | HaltReason::SpecMismatch)) => {
                Some(format!("cannot promote a slave halted with {r:?}"))
            }
            _ if !self.inner.is_ready() => Some("this slave has no consistent data to promote".to_string()),
            _ if cfg.path != self.inner.cfg.path => Some("MasterConfig.path must be the slave's path".to_string()),
            _ => cfg.validate().err().map(|e| e.to_string()),
        }
    }

    /// The irreversible part of `promote`: from here on a failure loses the slave handle.
    async fn promote_now(self, cfg: MasterConfig) -> Result<Master> {
        let path = cfg.path.clone();
        let tuning = cfg.rocksdb.clone();
        // Snapshots share the database handle; one that outlives the slave keeps the DB open.
        let db = Arc::downgrade(self.inner.store.db());
        self.inner.reset_downstream();
        self.shutdown().await?;
        if db.strong_count() > 0 {
            return Err(Error::Config(
                "a ReadSnapshot of this slave is still open and keeps the database open; drop it and reopen the slave"
                    .into(),
            ));
        }
        crate::background::blocking(move || {
            let (store, _) = Store::open(&path, Role::Slave, tuning)?;
            let indexed = store.rebuild_ttl_index()?;
            let identity = store.promote_to_master()?;
            tracing::info!(epoch = identity.epoch, ttl_entries = indexed, "slave promoted to master");
            Ok(())
        })
        .await?;
        Master::open(cfg).await
    }
}

/// What `check_promotion` found among the peers (none of them is later than this slave).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PromotionCheck {
    /// Peers whose `Status` could not be read (address and error). They do not block.
    pub unreachable: Vec<(String, String)>,
    /// Peers that hold entries this slave does not (`node_id`, fork seq).
    pub diverged: Vec<(String, Seq)>,
    /// Peers of another database.
    pub foreign: Vec<String>,
    /// Peers behind this slave whose history cannot be verified against it (`node_id`,
    /// `last_seq`): following it may discard entries they hold.
    pub unverifiable: Vec<(String, Seq)>,
}

impl Slave {
    /// What this slave reports about itself; the same fields as the `Status` RPC.
    pub fn promotion_info(&self) -> NodeInfo {
        let lag_max = self.server.as_ref().map_or(Duration::from_secs(10), ServerHandle::replica_lag_time_max);
        node_info(&*self.inner, &self.inner.replicas, lag_max, &[])
    }

    /// Asks every reachable peer for its `Status` and **refuses by default** (addendum §4):
    /// `Error::NotMostAdvanced` if a peer is proven to hold more of the log, and
    /// `Error::PromotionUnverifiable` if a peer is at least as far by position but its history
    /// cannot be verified (the entry that would prove it is trimmed or before its bootstrap).
    /// Each peer is probed at this slave's `last_seq`, and this slave's entry at the peer's
    /// `last_seq` is read locally, so [`admin::compare`] can check the shorter node's last
    /// entry. Peers proven diverged are reported (in the result, and in a refusal's error),
    /// not refused; unreachable peers (no answer within `admin::STATUS_TIMEOUT`), foreign
    /// peers and unverifiable peers that are behind are reported too: the operator decides.
    /// A peer that is reachable but answered `Status` with an error (permission denied, another
    /// protocol, ...) blocks as `PromotionUnverifiable` (`seq` 0, the error in `peer`): its state
    /// is unknown. A peer that reports `role == Master` blocks with `Error::MasterReachable`:
    /// fence it, or `Master::demote` it if it was promoted by mistake.
    ///
    /// This slave's `last_seq` is sampled while it may still be replicating; the failover
    /// runbook requires the old master to be fenced first, so it is no longer advancing.
    pub async fn check_promotion(&self, peers: &[UpstreamConfig]) -> Result<PromotionCheck> {
        let mut me = self.promotion_info();
        let mut check = PromotionCheck::default();
        let (mut later, mut unverified_ahead) = (Vec::new(), Vec::new());
        for peer in peers {
            let probe = Some(me.last_seq).filter(|&s| s > 0);
            let info = match admin::status_classified(peer, &me.node_id, probe).await {
                Ok(info) => info,
                Err(admin::StatusFailure::Unreachable(e)) => {
                    tracing::warn!(peer = %peer.uri(), error = %e, "promotion check: peer unreachable");
                    check.unreachable.push((peer.uri(), e));
                    continue;
                }
                // The peer is alive but its state is unknown (refused the call, spoke another
                // protocol, ...): it may be ahead, so it blocks like an unverifiable peer.
                Err(admin::StatusFailure::Answered(e)) => {
                    tracing::warn!(peer = %peer.uri(), error = %e, "promotion check: peer answered with an error");
                    unverified_ahead.push((format!("{} ({e})", peer.uri()), 0));
                    continue;
                }
            };
            if info.role == Role::Master {
                return Err(Error::MasterReachable { peer: info.node_id });
            }
            if info.last_seq > 0 && info.last_seq < me.last_seq {
                let crc = self.inner.store.entry_crc(info.last_seq)?;
                me.probes.insert(info.last_seq, crc);
            }
            match compare(&me, &info) {
                Comparison::Later(Side::B) => later.push((info.node_id, info.last_seq)),
                Comparison::Unverifiable { later: Some(Side::B) | None } => unverified_ahead.push((info.node_id, info.last_seq)),
                Comparison::Unverifiable { later: Some(Side::A) } => check.unverifiable.push((info.node_id, info.last_seq)),
                Comparison::Diverged { fork } => check.diverged.push((info.node_id, fork)),
                Comparison::DifferentDatabase => check.foreign.push(info.node_id),
                Comparison::Later(Side::A) | Comparison::Equal => {}
            }
        }
        if let Some((leader, seq)) = later.into_iter().next() {
            return Err(Error::NotMostAdvanced { leader, seq, diverged: check.diverged });
        }
        if let Some((peer, seq)) = unverified_ahead.into_iter().next() {
            return Err(Error::PromotionUnverifiable { peer, seq, diverged: check.diverged });
        }
        Ok(check)
    }

    /// [`check_promotion`](Self::check_promotion), then [`promote`](Self::promote), refusing
    /// by default. **A refusal hands the slave back untouched and still running**
    /// ([`PromoteRefused::into_slave`]): the checks of `promote` (bootstrapping, no data,
    /// halted `Corrupt`/`SpecMismatch`, wrong `cfg.path`, invalid `cfg`), a later reachable peer
    /// (`Error::NotMostAdvanced`), a peer at least as far whose history cannot be verified
    /// (`Error::PromotionUnverifiable`), or a peer check that could not run.
    ///
    /// Only a failure after the slave has started shutting down for the promotion loses the
    /// handle (`PromoteRefused::slave` is `None`): an I/O error during shutdown, a
    /// `ReadSnapshot` that still holds the database (`Error::Config`; drop it and reopen the
    /// directory as a slave, which is unchanged), or a failure while rewriting it.
    pub async fn promote_checked(self, cfg: MasterConfig, peers: &[UpstreamConfig]) -> Result<Master, PromoteRefused> {
        if let Some(msg) = self.promote_refusal(&cfg) {
            return Err(PromoteRefused { error: Error::Config(msg), slave: Some(Box::new(self)) });
        }
        match self.check_promotion(peers).await {
            Ok(check) => {
                if !check.unreachable.is_empty() || !check.diverged.is_empty() || !check.foreign.is_empty() || !check.unverifiable.is_empty() {
                    tracing::warn!(?check, "promoting despite unreachable, diverged or foreign peers");
                }
                self.promote_now(cfg).await.map_err(|error| PromoteRefused { error, slave: None })
            }
            Err(error) => Err(PromoteRefused { error, slave: Some(Box::new(self)) }),
        }
    }
}

/// A refused or failed [`Slave::promote_checked`]. `slave` is the untouched, still-running
/// slave for every refusal; it is `None` only when the promotion failed after the slave had
/// started shutting down.
pub struct PromoteRefused {
    pub error: Error,
    /// Boxed to keep the `Err` variant small.
    pub slave: Option<Box<Slave>>,
}

impl PromoteRefused {
    /// The slave, still running, if the promotion was refused before it changed anything.
    pub fn into_slave(self) -> Option<Slave> {
        self.slave.map(|s| *s)
    }
}

impl std::fmt::Debug for PromoteRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PromoteRefused").field("error", &self.error).field("slave_returned", &self.slave.is_some()).finish()
    }
}

impl std::fmt::Display for PromoteRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "promotion refused: {}", self.error)
    }
}

impl std::error::Error for PromoteRefused {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
