//! Public read API, shared by `Master`, `Slave` and `ReadSnapshot`.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::store::cfs::PhysCf;
use crate::store::read::{DbIter, ReadCtx};
use crate::store::snapshot::OwnedSnapshot;
use crate::types::{CfSpec, Direction, IterMode, Seq, ValueMeta};

/// Read access to replicated data. Expired values are never returned.
pub trait ReadApi {
    /// The value of `key`, or `None` if it is absent or expired.
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.get_with_meta(cf, key)?.map(|(v, _)| v))
    }

    /// Like [`get`](Self::get), plus the value's `write_seq` and expiry.
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>>;

    /// Reads several keys of one CF with one CF lookup and one clock read. The result has one
    /// element per key, in order.
    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|k| self.get(cf, k)).collect()
    }

    /// Iterates a CF from `mode`, in the CF's comparator order, skipping expired values.
    /// The iterator borrows `self`; a live iterator keeps a dropped CF on disk.
    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>>;

    /// Keys starting with `prefix`, in order. Meaningful for bytewise-ordered CFs.
    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>>;

    /// Live user CFs, sorted by name.
    fn list_cfs(&self) -> Vec<CfSpec>;
}

/// A consistent, point-in-time view at one seq. "Now" for TTL purposes is fixed when the
/// snapshot is taken, so repeated reads always agree. Holding one keeps dropped CFs alive.
pub struct ReadSnapshot {
    pub(crate) snap: OwnedSnapshot,
    pub(crate) cfs: BTreeMap<String, Arc<PhysCf>>,
    pub(crate) seq: Seq,
    pub(crate) now_ms: u64,
}

impl ReadSnapshot {
    /// The seq this snapshot reflects.
    pub fn seq(&self) -> Seq {
        self.seq
    }

    fn ctx(&self) -> ReadCtx<'_> {
        ReadCtx {
            db: self.snap.db(),
            snap: Some(&self.snap),
            now_ms: self.now_ms,
        }
    }

    fn cf(&self, name: &str) -> Result<Arc<PhysCf>> {
        self.cfs
            .get(name)
            .cloned()
            .ok_or_else(|| Error::NoSuchCf(name.to_string()))
    }
}

impl ReadApi for ReadSnapshot {
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        self.ctx().get(&*self.cf(cf)?, key)
    }

    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>> {
        self.ctx().iter(self.cf(cf)?, mode, None)
    }

    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>> {
        self.ctx().iter(
            self.cf(cf)?,
            IterMode::From(prefix, Direction::Forward),
            Some(prefix),
        )
    }

    fn list_cfs(&self) -> Vec<CfSpec> {
        self.cfs.values().map(|p| p.entry.spec.clone()).collect()
    }
}
