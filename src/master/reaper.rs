//! TTL reaper (master only, DESIGN.md §8).

use rocksdb::{WriteBatch, WriteOptions};

use super::writer::Inner;
use crate::error::Result;
use crate::types::Op;

/// What one reaper round did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReapStats {
    /// Keys deleted through logged `Delete` ops.
    pub deleted: usize,
    /// Index entries removed because the key was overwritten, deleted or its CF dropped.
    pub stale: usize,
    /// Log entries written.
    pub entries: usize,
}

impl Inner {
    pub(crate) fn reap_round(&self) -> Result<ReapStats> {
        let cfg = &self.cfg.reaper;
        let mut stats = ReapStats::default();
        let mut resume: Option<Vec<u8>> = None;
        while stats.deleted < cfg.max_deletes_per_round {
            let limit = cfg.max_deletes_per_entry.min(cfg.max_deletes_per_round - stats.deleted);
            // The writer lock is held per entry and released between entries, so the reaper
            // never blocks application writes for long.
            let mut w = self.lock_writer()?;
            let due = self.store.ttl_due(self.now_ms(), limit, resume.as_deref())?;
            if due.is_empty() {
                break;
            }
            resume = due.last().map(|e| e.raw_key.clone());
            let mut ops = Vec::new();
            for e in &due {
                if self.store.ttl_entry_is_live(e)? {
                    ops.push(Op::Delete { cf: e.cf_name.clone(), key: e.key.clone() });
                }
            }
            stats.stale += due.len() - ops.len();
            if ops.is_empty() {
                // Master-local cleanup; __ttl_index is derived and never replicated.
                let mut batch = WriteBatch::default();
                self.store.remove_ttl_index(&mut batch, &due);
                self.store.db().write_opt(batch, &WriteOptions::default())?;
            } else {
                let n = ops.len();
                self.commit_locked(&mut w, |_| Ok(ops), |batch, store| {
                    store.remove_ttl_index(batch, &due);
                    Ok(())
                })?;
                stats.deleted += n;
                stats.entries += 1;
            }
        }
        Ok(stats)
    }
}
