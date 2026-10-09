//! A RocksDB snapshot that owns its database handle, so it can outlive any borrow.

use std::sync::Arc;

use rocksdb::{AsColumnFamilyRef, DBIteratorWithThreadMode, IteratorMode, ReadOptions, SnapshotWithThreadMode, DB};

use super::DbCore;
use crate::error::Result;

pub(crate) struct OwnedSnapshot {
    // Field order matters: `snap` borrows the DB behind `db` and must be dropped first.
    snap: SnapshotWithThreadMode<'static, DB>,
    db: Arc<DbCore>,
}

impl OwnedSnapshot {
    pub(crate) fn new(db: Arc<DbCore>) -> Self {
        let snap = db.snapshot();
        // SAFETY: `snap` borrows the `DB` inside the `DbCore` behind `db`. The `Arc` keeps that
        // `DbCore`, and therefore the `DB`, alive and at a stable address for as long as `Self`
        // exists, and `snap` is declared first, so it is dropped before `db`. The `'static`
        // snapshot never leaves this struct: it is only used by `read_opts`, which is private,
        // and every `ReadOptions` or iterator built from it is created inside a method whose
        // result is tied to `&self` (`get_cf`, `iterator_cf`), so none can outlive `Self`.
        let snap = unsafe {
            std::mem::transmute::<SnapshotWithThreadMode<'_, DB>, SnapshotWithThreadMode<'static, DB>>(snap)
        };
        OwnedSnapshot { snap, db }
    }

    pub(crate) fn db(&self) -> &DB {
        &self.db
    }

    fn read_opts(&self) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_snapshot(&self.snap);
        opts
    }

    /// Point read as of the snapshot.
    pub(crate) fn get_cf(&self, cf: &impl AsColumnFamilyRef, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf_opt(cf, key, &self.read_opts())?)
    }

    /// Iterator as of the snapshot; it cannot outlive `self`.
    pub(crate) fn iterator_cf<'a>(
        &'a self,
        cf: &impl AsColumnFamilyRef,
        mode: IteratorMode<'_>,
    ) -> DBIteratorWithThreadMode<'a, DB> {
        self.db.iterator_cf_opt(cf, self.read_opts(), mode)
    }
}
