//! Reading the changelog.

use rocksdb::{DB, DBIteratorWithThreadMode, Direction, IteratorMode};

use super::Store;
use super::meta::{self, CF_CHANGELOG};
use crate::error::{Error, Result};
use crate::types::{LogEntry, Seq};

/// Entries with seq greater than a cursor, in order. Yields `Err(Gap)` and stops if the
/// next expected entry has been trimmed by retention. For a mid-stream hole, the error's
/// `first_retained` is the next seq actually present.
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
                return Err(Error::Gap {
                    requested: self.next,
                    first_retained: seq,
                });
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
            return Err(Error::Gap {
                requested: from,
                first_retained: first,
            });
        }
        let key = from.to_be_bytes();
        let cf = self.cf(CF_CHANGELOG);
        let inner = self
            .db()
            .iterator_cf(&cf, IteratorMode::From(&key, Direction::Forward));
        Ok(EntryIter {
            inner,
            next: from,
            done: false,
        })
    }
}

#[cfg(test)]
mod tests {
    use crate::error::Error;
    use crate::store::meta::CF_CHANGELOG;
    use crate::store::test_support::{apply, open_store};
    use crate::types::{CfSpec, Op};

    #[test]
    fn mid_stream_hole_yields_gap_then_stops() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap();
        for i in 0..4u8 {
            apply(
                &store,
                vec![Op::Put {
                    cf: "c".into(),
                    key: vec![i],
                    value: vec![i],
                    expire_at_ms: None,
                }],
            )
            .unwrap();
        }
        let last = store.position().last_seq;
        assert_eq!(last, 5);
        store
            .db()
            .delete_cf(&store.cf(CF_CHANGELOG), 3u64.to_be_bytes())
            .unwrap();

        let mut it = store.entries_after(0).unwrap();
        assert_eq!(it.next().unwrap().unwrap().seq, 1);
        assert_eq!(it.next().unwrap().unwrap().seq, 2);
        assert!(matches!(
            it.next(),
            Some(Err(Error::Gap {
                requested: 3,
                first_retained: 4
            }))
        ));
        assert!(it.next().is_none(), "iteration stops after the gap");

        assert_eq!(store.entries_after(last + 5).unwrap().count(), 0);
    }
}
