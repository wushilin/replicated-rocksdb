//! A RocksDB snapshot that owns its database handle, so it can outlive any borrow.

use std::sync::Arc;

use rocksdb::{ReadOptions, SnapshotWithThreadMode, DB};

use super::DbCore;

pub(crate) struct OwnedSnapshot {
    // Field order matters: `snap` borrows the DB behind `db` and must be dropped first.
    snap: SnapshotWithThreadMode<'static, DB>,
    db: Arc<DbCore>,
}

impl OwnedSnapshot {
    pub(crate) fn new(db: Arc<DbCore>) -> Self {
        let snap = db.snapshot();
        // SAFETY: `snap` borrows the `DB` inside the `DbCore` behind `db`. The `Arc` keeps that
        // `DbCore`, and therefore the `DB`, alive and at a stable address for as long as `Self` exists, and `snap` is declared first, so
        // it is dropped before `db`. The `'static` lifetime never escapes this struct:
        // `read_opts` only lends it out behind `&self`.
        let snap = unsafe {
            std::mem::transmute::<SnapshotWithThreadMode<'_, DB>, SnapshotWithThreadMode<'static, DB>>(snap)
        };
        OwnedSnapshot { snap, db }
    }

    pub(crate) fn db(&self) -> &DB {
        &self.db
    }

    pub(crate) fn read_opts(&self) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_snapshot(&self.snap);
        opts
    }
}
