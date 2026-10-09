//! Helpers for store unit tests.

use std::path::Path;

use super::Store;
use crate::codec::encode_ops;
use crate::error::Result;
use crate::tuning::RocksTuning;
use crate::types::{LogEntry, Op, Role, Seq};

pub(crate) fn open_store(dir: &Path) -> Store {
    open_store_with(dir, RocksTuning::new())
}

pub(crate) fn open_store_with(dir: &Path, tuning: RocksTuning) -> Store {
    Store::open(dir, Role::Master, tuning).unwrap().0
}

/// Applies `ops` as the next entry (epoch 1, ts = 1000 × seq), without fsync.
pub(crate) fn apply(store: &Store, ops: Vec<Op>) -> Result<Seq> {
    let seq = store.position().last_seq + 1;
    let entry = LogEntry::new(seq, 1, 1_000 * seq, encode_ops(&ops));
    let prepared = store.prepare(&entry, &ops)?;
    store.commit(prepared, false)?;
    Ok(seq)
}
