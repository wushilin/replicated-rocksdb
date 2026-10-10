//! Promotion of a slave to master (DESIGN.md §13.1).

use std::sync::Arc;
use std::time::Duration;

use super::state::{HaltReason, SlaveState};
use super::{Slave, UpstreamConfig};
use crate::admin::{self, Comparison, NodeInfo, Side, compare};
use crate::config::MasterConfig;
use crate::error::{Error, Result};
use crate::master::Master;
use crate::server::ServerHandle;
use crate::server::status::node_info;
use crate::store::Store;
use crate::types::{Role, Seq};

impl Slave {
    /// Turns this slave into the master. **Operator action: stop or fence the old master
    /// first** — the crate cannot detect two live masters, only the divergence they cause.
    ///
    /// Refused with `Error::Config` while bootstrapping, while a repair replays (DESIGN.md
    /// §12.1), without data, when halted with `Corrupt` or `SpecMismatch`, or if `cfg.path` is
    /// not this slave's path. Every
    /// `ReadSnapshot` taken from this slave must be dropped first: one that is alive keeps
    /// the database open and the promotion fails. In every failure case the slave has been
    /// shut down and its directory is unchanged, so it can be reopened as a slave (after the
    /// last snapshot is dropped).
    ///
    /// Downstream streams end with `UPSTREAM_RESET`. The new master starts epoch
    /// `1 + max(epoch history, max_burned_epoch)` at `applied_seq() + 1` and rebuilds its TTL
    /// index, so values written with a TTL before the promotion are reaped by it.
    ///
    /// **Epoch numbers.** Plain `promote` asks no peer, so it only knows the epochs this node
    /// has seen itself (its history, and any epoch it gave up with `Master::demote`). If another
    /// node was promoted by mistake, demoted again, and replicas followed it in between, they
    /// may hold the same epoch number this promotion picks. The handshake still tells the two
    /// epochs apart by their nonce (PROTOCOL.md §6.3, `known_epochs`), so no history
    /// is mixed, but [`promote_checked`](Self::promote_checked), which chooses an epoch above
    /// every epoch its reachable peers report, is the safe path.
    pub async fn promote(self, cfg: MasterConfig) -> Result<Master> {
        if let Some(msg) = self.promote_refusal(&cfg) {
            self.shutdown().await?;
            return Err(Error::Config(msg));
        }
        self.promote_now(cfg, 0, false)
            .await
            .map_err(|refused| refused.error)
    }

    /// Why `promote(cfg)` would be refused before it changes anything, if it would.
    fn promote_refusal(&self, cfg: &MasterConfig) -> Option<String> {
        match self.inner.state() {
            SlaveState::Bootstrapping { .. } => {
                Some("cannot promote while bootstrapping".to_string())
            }
            SlaveState::Halted(r @ (HaltReason::Corrupt | HaltReason::SpecMismatch)) => {
                Some(format!("cannot promote a slave halted with {r:?}"))
            }
            // Keys at the upstream's snapshot next to keys at the fork (DESIGN.md §12.1).
            _ if self.inner.repairing().is_some() => Some(format!(
                "cannot promote while a repair replays up to seq {}: the data is not a consistent state yet",
                self.inner.repairing().unwrap_or_default()
            )),
            _ if !self.inner.is_ready() => {
                Some("this slave has no consistent data to promote".to_string())
            }
            _ if cfg.path != self.inner.cfg.path => {
                Some("MasterConfig.path must be the slave's path".to_string())
            }
            _ => cfg.validate().err().map(|e| e.to_string()),
        }
    }

    /// The irreversible part of `promote`: from here on a failure loses the slave handle,
    /// except for the final check on the stopped store (an unfinished repair, DESIGN.md
    /// §12.1), which leaves the directory unchanged; with `reopen` the slave is then reopened
    /// and handed back. The new epoch is above `epoch_floor` (the highest epoch the peers
    /// reported).
    async fn promote_now(
        self,
        cfg: MasterConfig,
        epoch_floor: u64,
        reopen: bool,
    ) -> std::result::Result<Master, PromoteRefused> {
        let lost = |error| PromoteRefused { error, slave: None };
        // Test hook: the promotion passed its checks; the slave is still running.
        crate::failpoint::pause("slave-promote-before-shutdown").await;
        let path = cfg.path.clone();
        let tuning = cfg.rocksdb.clone();
        let slave_cfg = self.inner.cfg.clone();
        // Snapshots share the database handle; one that outlives the slave keeps the DB open.
        let db = Arc::downgrade(self.inner.store.db());
        self.inner.reset_downstream();
        self.shutdown().await.map_err(lost)?;
        if db.strong_count() > 0 {
            return Err(lost(Error::Config(
                "a ReadSnapshot of this slave is still open and keeps the database open; drop it and reopen the slave"
                    .into(),
            )));
        }
        // The checks above ran while the slave was replicating; a repair may have committed
        // its rewind since. The stopped store decides.
        let promoted = crate::background::blocking(move || {
            let (store, _) = Store::open(&path, Role::Slave, tuning)?;
            if let Some(why) = store.promotion_blocker()? {
                return Ok(Err(why));
            }
            let indexed = store.rebuild_ttl_index()?;
            let identity = store.promote_to_master(epoch_floor)?;
            tracing::info!(
                epoch = identity.epoch,
                ttl_entries = indexed,
                "slave promoted to master"
            );
            Ok(Ok(()))
        })
        .await
        .map_err(lost)?;
        if let Err(why) = promoted {
            tracing::warn!(error = %why, "promotion refused on the stopped store");
            let error = Error::Config(why);
            let slave = match reopen {
                true => match Slave::open(slave_cfg).await {
                    Ok(s) => Some(Box::new(s)),
                    Err(e) => {
                        tracing::error!(error = %e, "could not reopen the slave after the refused promotion");
                        None
                    }
                },
                false => None,
            };
            return Err(PromoteRefused { error, slave });
        }
        Master::open_promoted(cfg).await.map_err(lost)
    }
}

/// What `check_promotion` found among the peers (none of them is later than this slave).
/// Fields may be added, hence `non_exhaustive` (`Default` builds an empty one).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct PromotionCheck {
    /// Peers whose `Status` could not be read (address and error). They do not block.
    pub unreachable: Vec<(String, String)>,
    /// Peers that hold entries this slave does not (`node_id`, fork seq).
    pub diverged: Vec<(String, Seq)>,
    /// Peers of another database.
    pub foreign: Vec<String>,
    /// The highest epoch number any reachable peer of this database reported (its identity,
    /// every record of its history, and the epoch it burned by demoting). `promote_checked`
    /// starts an epoch above it, so an epoch number is never reused.
    pub max_peer_epoch: u64,
}

impl Slave {
    /// What this slave reports about itself; the same fields as the `Status` RPC.
    pub fn promotion_info(&self) -> NodeInfo {
        let lag_max = self
            .server
            .as_ref()
            .map_or(Duration::from_secs(10), ServerHandle::replica_lag_time_max);
        node_info(&*self.inner, &self.inner.replicas, lag_max, &[])
    }

    /// Asks every reachable peer for its `Status` and **refuses by default** (addendum §4, §7):
    /// `Error::NotMostAdvanced` if a peer holds more of a compatible history,
    /// `Error::MasterReachable` if a peer reports `role == Master`, and
    /// `Error::PromotionWouldDiscard` (below). Compatibility is decided by the epoch histories,
    /// nonces included ([`admin::compare`]), so a peer whose entries were trimmed is never
    /// incomparable; each peer is also probed at this slave's `last_seq`, and this slave's
    /// entry at the peer's `last_seq` is read locally, so a CRC mismatch is caught as a second
    /// line of defence. A peer whose history forks from this slave's at `fork` and that holds
    /// entries at or past it blocks with `Error::PromotionWouldDiscard` (promoting this slave
    /// would discard them; promote that node instead, or use plain `promote` to accept the
    /// loss). A diverged peer with nothing at or past the fork is only reported (in the
    /// result, and in a refusal's error); unreachable peers (no answer within
    /// `admin::STATUS_TIMEOUT`) and foreign peers are reported too: the operator decides.
    /// A peer that is reachable but answered `Status` with an error (permission denied, another
    /// protocol, ...) blocks as `PromotionUnverifiable` (`seq` 0, the error in `peer`): its state
    /// is unknown. So does a peer whose TLS handshake is rejected (its certificate does not
    /// verify) and a peer entry that cannot be used at all (an invalid address or TLS
    /// configuration): these are misconfigurations, not unreachable peers. A peer that reports `role == Master` blocks with `Error::MasterReachable`:
    /// fence it, or `Master::demote` it if it was promoted by mistake.
    ///
    /// A later peer that is replaying a repair (`node_state = repairing:<seq>`) still blocks
    /// with `NotMostAdvanced` naming it, although it refuses promotion itself until its replay
    /// reaches `seq` (which needs an upstream). [`admin::choose_promotion_candidate`] lists such
    /// nodes in `Choice::not_promotable`; when that replay can never finish, promoting this
    /// slave with plain `promote` is the operator's explicit decision.
    ///
    /// This slave's `last_seq` is sampled while it may still be replicating; the failover
    /// runbook requires the old master to be fenced first, so it is no longer advancing.
    pub async fn check_promotion(&self, peers: &[UpstreamConfig]) -> Result<PromotionCheck> {
        let mut me = self.promotion_info();
        let mut check = PromotionCheck::default();
        let (mut later, mut unanswered, mut would_discard) = (Vec::new(), Vec::new(), Vec::new());
        for peer in peers {
            let probe = Some(me.last_seq).filter(|&s| s > 0);
            let info = match admin::status_classified(peer, &me.node_id, probe).await {
                Ok(info) => info,
                Err(admin::StatusFailure::Unreachable { reason, source }) => {
                    tracing::warn!(peer = %peer.uri(), reason = %reason, error = ?source, "promotion check: peer unreachable");
                    check.unreachable.push((peer.uri(), reason));
                    continue;
                }
                // The peer is alive but its state is unknown (refused the call, spoke another
                // protocol, ...): it may be ahead, so it blocks as unverifiable.
                Err(admin::StatusFailure::Answered(e)) => {
                    tracing::warn!(peer = %peer.uri(), error = %e, "promotion check: peer answered with an error");
                    unanswered.push((format!("{} ({e})", peer.uri()), 0));
                    continue;
                }
            };
            if info.role == Role::Master {
                return Err(Error::MasterReachable { peer: info.node_id });
            }
            let same_db = info
                .identity
                .is_some_and(|id| me.identity.is_some_and(|mine| mine.db_uuid == id.db_uuid));
            if same_db || info.identity.is_none() {
                check.max_peer_epoch = check.max_peer_epoch.max(info.max_epoch_seen());
            }
            if info.last_seq > 0 && info.last_seq < me.last_seq {
                // A log read: on a blocking thread.
                let (i, seq) = (self.inner.clone(), info.last_seq);
                let crc = crate::background::blocking(move || i.store.entry_crc(seq)).await?;
                me.probes.insert(info.last_seq, crc);
            }
            match compare(&me, &info) {
                Comparison::Later(Side::B) => {
                    if info.node_state.starts_with("repairing:") {
                        // Still blocks: it holds entries this slave does not.
                        tracing::warn!(peer = %info.node_id, node_state = %info.node_state, "promotion check: a later peer is replaying a repair and cannot be promoted until it finishes");
                    }
                    later.push((info.node_id, info.last_seq))
                }
                // The peer holds entries this slave would discard: refused by default.
                Comparison::Diverged { fork } if info.last_seq >= fork => {
                    would_discard.push((info.node_id, fork, info.last_seq))
                }
                Comparison::Diverged { fork } => check.diverged.push((info.node_id, fork)),
                Comparison::DifferentDatabase => check.foreign.push(info.node_id),
                Comparison::Later(Side::A) | Comparison::Equal => {}
            }
        }
        if let Some((leader, seq)) = later.into_iter().next() {
            return Err(Error::NotMostAdvanced {
                leader,
                seq,
                diverged: check.diverged,
            });
        }
        if let Some((peer, seq)) = unanswered.into_iter().next() {
            return Err(Error::PromotionUnverifiable {
                peer,
                seq,
                diverged: check.diverged,
            });
        }
        if let Some((peer, fork, peer_last_seq)) = would_discard.into_iter().next() {
            return Err(Error::PromotionWouldDiscard {
                peer,
                fork,
                peer_last_seq,
            });
        }
        Ok(check)
    }

    /// [`check_promotion`](Self::check_promotion), then [`promote`](Self::promote), refusing
    /// by default. The new epoch is above every epoch a reachable peer reports
    /// ([`PromotionCheck::max_peer_epoch`]), so it never reuses the number of an undone
    /// promotion that some replica followed. **A refusal hands the slave back untouched and still running**
    /// ([`PromoteRefused::into_slave`]): the checks of `promote` (bootstrapping, no data,
    /// halted `Corrupt`/`SpecMismatch`, wrong `cfg.path`, invalid `cfg`), a later reachable peer
    /// (`Error::NotMostAdvanced`), a peer that answered `Status` with an error
    /// (`Error::PromotionUnverifiable`), a peer that reports itself a master
    /// (`Error::MasterReachable`), a peer holding entries at or past a fork with this slave
    /// (`Error::PromotionWouldDiscard`), or a peer check that could not run.
    ///
    /// Only a failure after the slave has started shutting down for the promotion loses the
    /// handle (`PromoteRefused::slave` is `None`): an I/O error during shutdown, a
    /// `ReadSnapshot` that still holds the database (`Error::Config`; drop it and reopen the
    /// directory as a slave, which is unchanged), or a failure while rewriting it. One final
    /// check runs on the stopped store: a repair that committed its rewind meanwhile (it is
    /// replaying, DESIGN.md §12.1) or a bootstrap that began meanwhile (no data to promote)
    /// refuses with `Error::Config`, the directory unchanged, and the slave is reopened and
    /// handed back. It is reopened with the configured `cfg.upstream`: an upstream set by
    /// `repoint` is not kept (repoint it again). If that reopen fails, the handle is lost
    /// (`slave` is `None`) and the application reopens the directory.
    pub async fn promote_checked(
        self,
        cfg: MasterConfig,
        peers: &[UpstreamConfig],
    ) -> Result<Master, PromoteRefused> {
        if let Some(msg) = self.promote_refusal(&cfg) {
            return Err(PromoteRefused {
                error: Error::Config(msg),
                slave: Some(Box::new(self)),
            });
        }
        match self.check_promotion(peers).await {
            Ok(check) => {
                if !check.unreachable.is_empty()
                    || !check.diverged.is_empty()
                    || !check.foreign.is_empty()
                {
                    tracing::warn!(
                        ?check,
                        "promoting despite unreachable, diverged or foreign peers"
                    );
                }
                self.promote_now(cfg, check.max_peer_epoch, true).await
            }
            Err(error) => Err(PromoteRefused {
                error,
                slave: Some(Box::new(self)),
            }),
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

    /// The error and the slave (if it was handed back), to keep the slave while still
    /// propagating the error with `?`:
    ///
    /// ```no_run
    /// # use replicated_rocksdb::{Master, MasterConfig, Result, Slave, UpstreamConfig};
    /// # fn keep_replicating(_: Option<Slave>) {}
    /// # async fn f(slave: Slave, cfg: MasterConfig, peers: Vec<UpstreamConfig>) -> Result<Master> {
    /// let master = match slave.promote_checked(cfg, &peers).await {
    ///     Ok(master) => master,
    ///     Err(refused) => {
    ///         let (error, slave) = refused.into_parts();
    ///         keep_replicating(slave);
    ///         return Err(error);
    ///     }
    /// };
    /// # Ok(master) }
    /// ```
    pub fn into_parts(self) -> (Error, Option<Slave>) {
        (self.error, self.slave.map(|s| *s))
    }
}

impl std::fmt::Debug for PromoteRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PromoteRefused")
            .field("error", &self.error)
            .field("slave_returned", &self.slave.is_some())
            .finish()
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
