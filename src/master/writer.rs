//! The gated writer: one entry at a time, always fsynced (DESIGN.md §5).

use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};

use rocksdb::WriteBatch;
use tokio::sync::watch;

use crate::codec::encode_ops;
use crate::config::MasterConfig;
use crate::error::{Error, Result};
use crate::store::Store;
use crate::store::meta::{self, CF_META, key};
use crate::types::{DbIdentity, EpochStart, LogEntry, Op, Seq};

pub(crate) struct WriterState {
    pub last_ts: u64,
    /// Set after a failed write: durability of that batch is unknown, so no more writes.
    pub fenced: bool,
}

pub(crate) struct Inner {
    pub store: Store,
    pub cfg: MasterConfig,
    writer: Mutex<WriterState>,
    // Identity and epoch history change together, under one lock.
    identity: RwLock<(DbIdentity, Vec<EpochStart>)>,
    pub seq_tx: watch::Sender<Seq>,
    pub shutdown_tx: watch::Sender<bool>,
    /// Bumped by `bump_epoch`: every served replication stream ends with `UPSTREAM_RESET`.
    pub reset_tx: watch::Sender<u64>,
    /// Downstream replicas served by this master (acks, in-sync state).
    pub(crate) replicas: Arc<crate::server::replicas::ReplicaRegistry>,
}

impl Inner {
    pub(crate) fn new(
        store: Store,
        cfg: MasterConfig,
        last_ts: u64,
        identity: DbIdentity,
        history: Vec<EpochStart>,
    ) -> Self {
        let last_seq = store.position().last_seq;
        Inner {
            store,
            cfg,
            writer: Mutex::new(WriterState {
                last_ts,
                fenced: false,
            }),
            identity: RwLock::new((identity, history)),
            seq_tx: watch::channel(last_seq).0,
            shutdown_tx: watch::channel(false).0,
            reset_tx: watch::channel(0).0,
            replicas: Arc::default(),
        }
    }

    pub(crate) fn trim_changelog(&self) -> Result<u64> {
        self.store
            .trim_changelog(self.now_ms(), &self.cfg.retention)
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.cfg.clock.now_ms()
    }

    pub(crate) fn identity(&self) -> DbIdentity {
        self.identity
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .0
    }

    pub(crate) fn epoch_history(&self) -> Vec<EpochStart> {
        self.identity
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .1
            .clone()
    }

    /// Identity and epoch history, read under one lock.
    pub(crate) fn identity_and_history(&self) -> (DbIdentity, Vec<EpochStart>) {
        self.identity
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn lock_writer(&self) -> Result<MutexGuard<'_, WriterState>> {
        let w = match self.writer.lock() {
            Ok(w) => w,
            Err(poisoned) => {
                // A panic while holding the lock leaves durability unknown: fence.
                let mut w = poisoned.into_inner();
                w.fenced = true;
                tracing::error!("writer mutex poisoned; writer fenced until reopen");
                return Err(Error::WriterFenced);
            }
        };
        if w.fenced {
            Err(Error::WriterFenced)
        } else {
            Ok(w)
        }
    }

    pub(crate) fn commit_ops(&self, build: impl FnOnce(u64) -> Result<Vec<Op>>) -> Result<Seq> {
        let mut w = self.lock_writer()?;
        self.commit_locked(&mut w, build, |_, _| Ok(()))
    }

    /// Builds, validates and durably commits one entry. `build` gets the entry's `ts_ms`;
    /// `extra` may add master-local writes (TTL index) to the same batch.
    pub(crate) fn commit_locked(
        &self,
        w: &mut WriterState,
        build: impl FnOnce(u64) -> Result<Vec<Op>>,
        extra: impl FnOnce(&mut WriteBatch, &Store) -> Result<()>,
    ) -> Result<Seq> {
        if w.fenced {
            return Err(Error::WriterFenced);
        }
        let ts = self.now_ms().max(w.last_ts);
        let ops = build(ts)?;
        let seq = self.store.position().last_seq + 1;
        let entry = LogEntry::new(seq, self.identity().epoch, ts, encode_ops(&ops));
        let size = entry.encoded_len() as u64;
        if size > self.cfg.max_entry_bytes {
            return Err(Error::EntryTooLarge {
                size,
                max: self.cfg.max_entry_bytes,
            });
        }
        let mut prepared = self.store.prepare(&entry, &ops)?;
        self.store.add_ttl_index(&mut prepared.batch, &ops)?;
        extra(&mut prepared.batch, &self.store)?;
        prepared
            .batch
            .put_cf(&self.store.cf(CF_META), key::LAST_TS, ts.to_be_bytes());
        if let Err(e) = self.store.commit(prepared, true) {
            w.fenced = true;
            tracing::error!(seq, error = %e, "master write failed; writer fenced until reopen");
            return Err(e);
        }
        w.last_ts = ts;
        self.seq_tx.send_replace(seq);
        Ok(seq)
    }

    /// New epoch, same db_uuid; seq numbering continues (DESIGN.md §13).
    /// Identity and history are written in one synced batch.
    pub(crate) fn bump_epoch(&self) -> Result<DbIdentity> {
        let mut w = self.lock_writer()?;
        let mut guard = self
            .identity
            .write()
            .unwrap_or_else(PoisonError::into_inner);
        let (mut id, mut history) = guard.clone();
        // `max_burned_epoch` needs no check here: a number is burned only by `demote` or
        // `Slave::rejoin`, which turn the node into a slave, and a slave becomes a master again
        // only through a promotion, which does honour it (`Store::promote_to_master`).
        id.epoch = history
            .iter()
            .map(|e| e.epoch)
            .max()
            .unwrap_or(id.epoch)
            .max(id.epoch)
            + 1;
        let start = self.store.position().last_seq + 1;
        history.push(EpochStart::fresh(id.epoch, start));
        let written = crate::failpoint::io_error("master-bump-epoch-write").and_then(|()| {
            self.store.write_meta(
                &[
                    (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                    (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
                ],
                true,
            )
        });
        if let Err(e) = written {
            w.fenced = true;
            tracing::error!(error = %e, "epoch bump failed; writer fenced until reopen");
            return Err(e);
        }
        *guard = (id, history);
        drop(guard);
        self.reset_tx.send_modify(|n| *n += 1);
        tracing::info!(epoch = id.epoch, start_seq = start, "epoch bumped");
        Ok(id)
    }
}
