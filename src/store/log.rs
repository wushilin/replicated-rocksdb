//! Reading the changelog.

use rocksdb::{DBIteratorWithThreadMode, Direction, IteratorMode, DB};

use super::meta::{self, CF_CHANGELOG};
use super::Store;
use crate::error::{Error, Result};
use crate::types::{LogEntry, Seq};

/// Entries with seq greater than a cursor, in order. Yields `Err(Gap)` and stops if the
/// next expected entry has been trimmed by retention.
pub struct EntryIter<'a> {
    inner: DBIteratorWithThreadMode<'a, DB>,
    next: Seq,
    done: bool,
}

impl Iterator for EntryIter<'_> {
    type Item = Result<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let (k, v) = match self.inner.next()? {
            Ok(kv) => kv,
            Err(e) => {
                self.done = true;
                return Some(Err(e.into()));
            }
        };
        let result = meta::be64(&k).and_then(|seq| {
            if seq != self.next {
                return Err(Error::Gap { requested: self.next, first_retained: seq });
            }
            LogEntry::decode(&v)
        });
        match &result {
            Ok(_) => self.next += 1,
            Err(_) => self.done = true,
        }
        Some(result)
    }
}

impl Store {
    pub(crate) fn entries_after(&self, seq: Seq) -> Result<EntryIter<'_>> {
        let from = seq.saturating_add(1);
        let first = self.first_retained_seq();
        if from < first {
            return Err(Error::Gap { requested: from, first_retained: first });
        }
        let key = from.to_be_bytes();
        let cf = self.cf(CF_CHANGELOG);
        let inner = self.db().iterator_cf(&cf, IteratorMode::From(&key, Direction::Forward));
        Ok(EntryIter { inner, next: from, done: false })
    }
}
