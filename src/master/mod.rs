//! The writable master node.

mod maintenance;
mod reaper;
mod source;
pub(crate) mod writer;

pub use reaper::ReapStats;

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::codec::validate_cf_name;
use crate::config::MasterConfig;
use crate::error::Result;
use crate::log::{EntryIter, LogApi};
use crate::read::{ReadApi, ReadSnapshot};
use crate::server::ServerHandle;
use crate::store::meta::{self, key};
use crate::store::read::DbIter;
use crate::store::Store;
use crate::types::{expire_at, Batch, CfSpec, DbIdentity, Direction, EpochStart, IterMode, Op, Role, Seq, ValueMeta};
use writer::Inner;

/// The single writable node. Every write is one fsynced log entry.
///
/// # Read-modify-write
///
/// There is no `update`, compare-and-set or merge. Each write method serialises one call
/// only: **between your `get` and your `put`, another writer can change the key, and that
/// change will be lost.** If a new value is derived from an old one, hold your own lock
/// around the whole get → compute → put sequence, and make every writer of those keys take
/// the same lock. See DESIGN.md §5.1.
///
/// # Closing
///
/// Dropping the `Master` (or calling [`shutdown`](Self::shutdown)) releases the database once
/// no maintenance tick is running. Any [`ReadSnapshot`] still alive keeps the database open,
/// and its directory LOCK held, until that snapshot is dropped.
pub struct Master {
    inner: Arc<Inner>,
    task: Option<JoinHandle<()>>,
    server: Option<ServerHandle>,
}

impl Master {
    /// Opens or creates a master database. Must be called within a tokio runtime.
    pub async fn open(cfg: MasterConfig) -> Result<Self> {
        cfg.validate()?;
        let (store, meta) = Store::open(&cfg.path, Role::Master, cfg.rocksdb.clone())?;
        let mut fresh = false;
        let (identity, history) = match meta.identity {
            Some(id) if !meta.epoch_history.is_empty() => (id, meta.epoch_history),
            Some(_) => return Err(crate::Error::corrupt("origin_identity without epoch_history in __meta")),
            None => {
                let id = DbIdentity { db_uuid: uuid::Uuid::new_v4(), epoch: 1 };
                let history = vec![EpochStart { epoch: 1, start_seq: 1 }];
                store.write_meta(
                    &[
                        (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                        (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
                    ],
                    true,
                )?;
                fresh = true;
                (id, history)
            }
        };
        let bump = cfg.bump_epoch_on_open;
        let inner = Arc::new(Inner::new(store, cfg, meta.last_ts, identity, history));
        if bump && !fresh {
            inner.bump_epoch()?;
        }
        // The epoch bump above happens before the replication port opens (DESIGN.md §13).
        let server = crate::server::start_for(&inner, inner.cfg.serve.clone()).await?;
        let task = maintenance::spawn(&inner);
        Ok(Master { inner, task: Some(task), server })
    }

    /// The origin identity (`db_uuid` and current epoch) stamped on every entry.
    pub fn identity(&self) -> DbIdentity {
        self.inner.identity()
    }

    /// One `{epoch, start_seq}` record per epoch, oldest first (PROTOCOL.md §5.5).
    pub fn epoch_history(&self) -> Vec<EpochStart> {
        self.inner.epoch_history()
    }

    /// Writes `value` under `key`, replacing any value and clearing any TTL.
    ///
    /// This is an absolute write. It is **not** safe to compute `value` from a previous
    /// [`get`](Self::get) without holding an application lock around both calls; see the
    /// [type-level docs](Self#read-modify-write).
    ///
    /// Blocks until the entry is fsynced. Async callers should use `spawn_blocking`.
    pub fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Seq> {
        let op = Op::Put { cf: cf.into(), key: key.into(), value: value.into(), expire_at_ms: None };
        self.inner.commit_ops(|_| Ok(vec![op]))
    }

    /// Like [`put`](Self::put), with an expiry of commit time + `ttl`. Expired values are
    /// hidden from reads at once and deleted by the reaper later.
    ///
    /// Blocks until the entry is fsynced. Async callers should use `spawn_blocking`.
    pub fn put_with_ttl(&self, cf: &str, key: &[u8], value: &[u8], ttl: Duration) -> Result<Seq> {
        let (cf, key, value) = (cf.to_string(), key.to_vec(), value.to_vec());
        self.inner.commit_ops(|ts| Ok(vec![Op::Put { cf, key, value, expire_at_ms: Some(expire_at(ts, ttl)) }]))
    }

    /// Deletes `key`. A no-op delete is still logged as an entry.
    ///
    /// Blocks until the entry is fsynced. Async callers should use `spawn_blocking`.
    pub fn delete(&self, cf: &str, key: &[u8]) -> Result<Seq> {
        let op = Op::Delete { cf: cf.into(), key: key.into() };
        self.inner.commit_ops(|_| Ok(vec![op]))
    }

    /// Deletes keys in `[start, end)` under the CF's comparator. `start` must sort before `end`.
    ///
    /// Blocks until the entry is fsynced. Async callers should use `spawn_blocking`.
    pub fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Seq> {
        let op = Op::DeleteRange { cf: cf.into(), start: start.into(), end: end.into() };
        self.inner.commit_ops(|_| Ok(vec![op]))
    }

    /// Commits all operations in `batch` atomically, as one log entry.
    ///
    /// A batch is not a transaction over reads: values computed from earlier reads can be
    /// stale by the time the batch commits unless the application holds its own lock; see
    /// the [type-level docs](Self#read-modify-write).
    ///
    /// Blocks until the entry is fsynced. Async callers should use `spawn_blocking`.
    pub fn write(&self, batch: Batch) -> Result<Seq> {
        self.inner.commit_ops(|ts| Ok(batch.into_ops(ts)))
    }

    /// Runs one reaper round now: logs `Delete`s for expired keys, in entries of at most
    /// `reaper.max_deletes_per_entry`, up to `reaper.max_deletes_per_round` deletes.
    pub fn reap_expired(&self) -> Result<ReapStats> {
        self.inner.reap_round()
    }

    /// Creates a column family. Fails with `AlreadyExists`, `InvalidCfName` or
    /// `UnknownComparator`. Logged as its own entry.
    ///
    /// Blocks until the entry is fsynced. Async callers should use `spawn_blocking`.
    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq> {
        validate_cf_name(&spec.name)?;
        self.inner.commit_ops(|_| Ok(vec![Op::CreateCf { spec }]))
    }

    /// Logically drops a column family. The physical drop is deferred while any snapshot or
    /// iterator still references it. Logged as its own entry.
    ///
    /// Blocks until the entry is fsynced. Async callers should use `spawn_blocking`.
    pub fn drop_cf(&self, name: &str) -> Result<Seq> {
        validate_cf_name(name)?;
        let op = Op::DropCf { name: name.into() };
        self.inner.commit_ops(|_| Ok(vec![op]))
    }

    /// Reads the current value of `key`, or `None` if absent or expired.
    ///
    /// The value can change as soon as this returns. To update a key based on its current
    /// value, hold an application lock around get → compute → put; see the
    /// [type-level docs](Self#read-modify-write).
    pub fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        ReadApi::get(self, cf, key)
    }

    /// A consistent point-in-time view; "now" for TTL is fixed when it is taken. Holding it
    /// keeps dropped column families on disk and the database open (see [`Master`] docs).
    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        self.inner.store.read_snapshot(self.inner.now_ms())
    }

    /// Starts a new epoch (same db_uuid; seq continues). Use after restoring from a backup.
    pub fn bump_epoch(&self) -> Result<DbIdentity> {
        self.inner.bump_epoch()
    }

    /// The address the replication server listens on, if this master serves replication.
    pub fn serve_addr(&self) -> Option<SocketAddr> {
        self.server.as_ref().map(ServerHandle::addr)
    }

    /// Ends every replication stream with `SHUTTING_DOWN`, stops background work, and waits
    /// until the database is closed, so the directory can be reopened at once.
    /// `Err(Timeout)` if something still holds the database after 10 s (for example an
    /// outstanding [`ReadSnapshot`], which keeps the database open until dropped).
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
        self.inner.shutdown_tx.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        if crate::server::await_sole_owner(&self.inner, Duration::from_secs(10)).await {
            Ok(())
        } else {
            Err(crate::Error::Timeout)
        }
    }
}

impl Drop for Master {
    fn drop(&mut self) {
        self.inner.shutdown_tx.send_replace(true);
    }
}

impl ReadApi for Master {
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).get(&cf, key)
    }

    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        // One CF lookup and one clock read for the whole call.
        let cf = self.inner.store.live_cf(cf)?;
        let ctx = self.inner.store.live_ctx(self.inner.now_ms());
        keys.iter().map(|k| Ok(ctx.get(&cf, k)?.map(|(v, _)| v))).collect()
    }

    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>> {
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).iter(cf, mode, None)
    }

    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>> {
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).iter(cf, IterMode::From(prefix, Direction::Forward), Some(prefix))
    }

    fn list_cfs(&self) -> Vec<CfSpec> {
        self.inner.store.cfs.live_entries().into_iter().map(|e| e.spec).collect()
    }
}

impl LogApi for Master {
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

const _: () = {
    fn assert<T: Send + Sync + 'static>() {}
    let _ = assert::<Master>;
    let _ = assert::<ReadSnapshot>;
};
