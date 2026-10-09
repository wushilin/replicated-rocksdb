//! Changelog retention (DESIGN.md §9). Runs on every node.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use rocksdb::{IteratorMode, WriteBatch, WriteOptions};

use super::meta::{self, CF_CHANGELOG};
use super::Store;
use crate::config::Retention;
use crate::error::Result;
use crate::types::{LogEntry, Seq};

#[derive(Debug, Default)]
pub(crate) struct GuardSet {
    next_id: u64,
    held: BTreeMap<u64, Seq>,
}

/// While held, retention keeps every entry with seq greater than the guarded seq.
#[derive(Debug)]
pub(crate) struct RetentionGuard {
    set: Arc<Mutex<GuardSet>>,
    id: u64,
}

impl Drop for RetentionGuard {
    fn drop(&mut self) {
        self.set.lock().unwrap_or_else(PoisonError::into_inner).held.remove(&self.id);
    }
}

impl Store {
    /// Keeps entries with seq greater than `seq` while the returned guard is held.
    ///
    /// A guard only protects entries that are still present: callers must check
    /// `first_retained_seq()` AFTER acquiring the guard (`trim_changelog` holds the same
    /// lock for its whole critical section, so taking a guard serialises against a trim).
    pub(crate) fn guard(&self, seq: Seq) -> RetentionGuard {
        let mut set = self.guards.lock().unwrap_or_else(PoisonError::into_inner);
        let id = set.next_id;
        set.next_id += 1;
        set.held.insert(id, seq);
        RetentionGuard { set: self.guards.clone(), id }
    }

    /// Deletes entries from the front of the changelog that are older than `max_age`
    /// (by the master's `ts_ms`) or beyond `max_entries`, never past a guard.
    /// `last_seq` is untouched. Returns how many entries were removed.
    ///
    /// Intended to be called only by the maintenance task. The `guards` lock is held for
    /// the whole decide-delete-publish section so a concurrent `guard()` cannot slip in
    /// between the limit computation and the delete. `first_retained` is published after
    /// the delete; a momentarily stale (too low) value is harmless because
    /// `entries_after` checks continuity and yields `Err(Gap)` rather than skipping entries.
    pub(crate) fn trim_changelog(&self, now_ms: u64, r: &Retention) -> Result<u64> {
        let first = self.first_retained_seq();
        let last = self.position().last_seq;
        if first > last {
            return Ok(0);
        }
        // Entries in [first, cut) are deleted.
        let guards = self.guards.lock().unwrap_or_else(PoisonError::into_inner);
        let guard_limit = guards
            .held
            .values()
            .min()
            .map_or(last + 1, |s| s.saturating_add(1));
        let by_count = r.max_entries.map_or(first, |m| (last + 1).saturating_sub(m));
        let mut cut = by_count.max(first);
        if let Some(age) = r.max_age {
            let age_ms = u64::try_from(age.as_millis()).unwrap_or(u64::MAX);
            let start = cut.to_be_bytes();
            let cf = self.cf(CF_CHANGELOG);
            let iter = self.db().iterator_cf(&cf, IteratorMode::From(&start, rocksdb::Direction::Forward));
            for item in iter {
                let (k, v) = item?;
                let seq = meta::be64(&k)?;
                if seq >= guard_limit || LogEntry::decode(&v)?.ts_ms.saturating_add(age_ms) > now_ms {
                    break;
                }
                cut = seq + 1;
            }
        }
        let cut = cut.min(guard_limit).min(last + 1);
        if cut <= first {
            return Ok(0);
        }
        let cf = self.cf(CF_CHANGELOG);
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&cf, first.to_be_bytes(), cut.to_be_bytes());
        self.db().write_opt(batch, &WriteOptions::default())?;
        self.set_first_retained_seq(cut);
        drop(guards);
        self.db().compact_range_cf(&cf, None::<&[u8]>, Some(cut.to_be_bytes()));
        Ok(cut - first)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::Retention;
    use crate::error::Error;
    use crate::store::test_support::{apply, open_store};
    use crate::types::{CfSpec, Op};

    const HOUR: u64 = 3_600_000;

    fn retention(max_age_ms: Option<u64>, max_entries: Option<u64>) -> Retention {
        Retention { max_age: max_age_ms.map(Duration::from_millis), max_entries, check_interval: Duration::from_secs(1) }
    }

    fn put(i: u8) -> Vec<Op> {
        vec![Op::Put { cf: "c".into(), key: vec![i], value: vec![], expire_at_ms: None }]
    }

    // test_support::apply stamps ts = 1000 × seq.
    fn store_with(n: u8) -> (tempfile::TempDir, crate::store::Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        for i in 0..n {
            apply(&store, put(i)).unwrap();
        }
        (dir, store)
    }

    #[test]
    fn trims_by_age_from_the_front() {
        let (_d, store) = store_with(4); // seqs 1..=5, ts 1000..=5000
        assert_eq!(store.trim_changelog(3_500 + HOUR, &retention(Some(HOUR), None)).unwrap(), 3);
        assert_eq!(store.first_retained_seq(), 4);
        assert!(matches!(store.entries_after(2), Err(Error::Gap { requested: 3, first_retained: 4 })));
        assert_eq!(store.entries_after(3).unwrap().count(), 2);
    }

    #[test]
    fn trims_by_count() {
        let (_d, store) = store_with(4);
        assert_eq!(store.trim_changelog(0, &retention(None, Some(2))).unwrap(), 3);
        assert_eq!(store.first_retained_seq(), 4);
    }

    #[test]
    fn quiet_store_can_trim_everything_but_keeps_position() {
        let (_d, store) = store_with(2);
        assert_eq!(store.trim_changelog(u64::MAX, &retention(Some(1), None)).unwrap(), 3);
        assert_eq!(store.position().last_seq, 3);
        assert_eq!(store.first_retained_seq(), 4);
        assert_eq!(apply(&store, put(9)).unwrap(), 4);
        assert_eq!(store.first_retained_seq(), 4);
        assert_eq!(store.entries_after(3).unwrap().next().unwrap().unwrap().seq, 4);
    }

    #[test]
    fn guard_keeps_entries_after_its_seq() {
        let (_d, store) = store_with(4);
        let g = store.guard(2);
        assert_eq!(store.trim_changelog(u64::MAX, &retention(Some(1), None)).unwrap(), 2);
        assert_eq!(store.first_retained_seq(), 3);
        drop(g);
        assert_eq!(store.trim_changelog(u64::MAX, &retention(Some(1), None)).unwrap(), 3);
    }

    #[test]
    fn max_entries_zero_trims_everything_and_writes_continue() {
        let (_d, store) = store_with(3);
        assert_eq!(store.trim_changelog(0, &retention(None, Some(0))).unwrap(), 4);
        assert_eq!(store.position().last_seq, 4);
        assert_eq!(store.first_retained_seq(), 5);
        assert_eq!(apply(&store, put(9)).unwrap(), 5);
        assert_eq!(store.entries_after(4).unwrap().next().unwrap().unwrap().seq, 5);
    }

    #[test]
    fn larger_of_age_and_count_cut_wins() {
        // seqs 1..=5, ts 1000..=5000. Age cut (now 3500+HOUR) removes 1..=3; count keeps 4.
        let (_d, store) = store_with(4);
        assert_eq!(store.trim_changelog(3_500 + HOUR, &retention(Some(HOUR), Some(4))).unwrap(), 3);
        assert_eq!(store.first_retained_seq(), 4);
        // Count keeps only 1, age (nothing old at now=0) would remove none: count wins.
        assert_eq!(store.trim_changelog(0, &retention(Some(HOUR), Some(1))).unwrap(), 1);
        assert_eq!(store.first_retained_seq(), 5);
    }

    #[test]
    fn first_retained_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
            apply(&store, put(1)).unwrap();
            store.trim_changelog(u64::MAX, &retention(None, Some(1))).unwrap();
        }
        let store = open_store(dir.path());
        assert_eq!(store.first_retained_seq(), 2);
    }
}
