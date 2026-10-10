//! Store helpers for the replication server: entry lookups, batched log reads, and raw reads
//! through a snapshot (bootstrap and verify read envelopes, never stripped values).

use std::sync::Arc;

use rocksdb::{DB, DBIteratorWithThreadMode, IteratorMode};

use super::Store;
use super::cfs::PhysCf;
use super::meta::{CF_CHANGELOG, CF_META, MetaSnapshot};
use super::read::ReadCtx;
use crate::error::{Error, Result};
use crate::read::ReadSnapshot;
use crate::types::{LogEntry, Seq};

impl Store {
    /// The `crc32c` of entry `seq`, if known: from the changelog while it is retained, and
    /// for `seq == last_seq` also from `last_entry_crc` (known after a bootstrap or after
    /// retention emptied the log).
    pub(crate) fn entry_crc(&self, seq: Seq) -> Result<Option<u32>> {
        let pos = self.position();
        if seq == 0 || seq > pos.last_seq {
            return Ok(None);
        }
        if seq >= self.first_retained_seq() {
            let cf = self.cf(CF_CHANGELOG);
            if let Some(v) = self.db().get_cf(&cf, seq.to_be_bytes())? {
                return Ok(Some(LogEntry::decode(&v)?.crc32c));
            }
        }
        Ok(if seq == pos.last_seq {
            pos.last_entry_crc
        } else {
            None
        })
    }

    /// Up to `max` consecutive entries starting at `from`, stopping early once their
    /// encoded size reaches `max_bytes` (so at least one entry is returned if there is any).
    /// Fewer if the log ends sooner; `Err(Gap)` if `from` (or a later entry in the range)
    /// has been trimmed.
    pub(crate) fn read_entries(
        &self,
        from: Seq,
        max: usize,
        max_bytes: usize,
    ) -> Result<Vec<LogEntry>> {
        let mut out = Vec::new();
        let mut bytes = 0usize;
        for entry in self.entries_after(from.saturating_sub(1))?.take(max) {
            let entry = entry?;
            bytes = bytes.saturating_add(entry.encoded_len());
            out.push(entry);
            if bytes >= max_bytes {
                break;
            }
        }
        Ok(out)
    }
}

impl ReadSnapshot {
    /// `__meta` as of this snapshot: `last_seq`, identity, epoch history and `cf_specs` are
    /// always consistent with the data read through the same snapshot.
    pub(crate) fn meta(&self) -> Result<MetaSnapshot> {
        let cf = self
            .snap
            .db()
            .cf_handle(CF_META)
            .ok_or_else(|| Error::corrupt("__meta is not open"))?;
        MetaSnapshot::load(|k| self.snap.get_cf(&cf, k))?
            .ok_or_else(|| Error::corrupt("__meta is empty in snapshot"))
    }

    /// The physical CF this snapshot holds for logical CF `name`.
    pub(crate) fn phys(&self, name: &str) -> Option<Arc<PhysCf>> {
        self.cfs.get(name).cloned()
    }

    pub(crate) fn raw_ctx(&self) -> ReadCtx<'_> {
        ReadCtx {
            db: self.snap.db(),
            snap: Some(&self.snap),
            now_ms: self.now_ms,
        }
    }
}

/// `(key, envelope)` pairs of one CF in comparator order, expired values included.
pub(crate) struct RawIter<'a> {
    inner: DBIteratorWithThreadMode<'a, DB>,
    _cf: Arc<PhysCf>,
}

impl Iterator for RawIter<'_> {
    type Item = Result<(Box<[u8]>, Box<[u8]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| r.map_err(Error::from))
    }
}

impl<'a> ReadCtx<'a> {
    /// Through the snapshot when there is one; the iterator cannot outlive it.
    pub(crate) fn iter_raw(&self, cf: Arc<PhysCf>) -> Result<RawIter<'a>> {
        let handle = self.db.cf_handle(&cf.physical).ok_or_else(|| {
            Error::corrupt(format!("physical column family {} is missing", cf.physical))
        })?;
        let inner = match self.snap {
            Some(s) => s.iterator_cf(&handle, IteratorMode::Start),
            None => self.db.iterator_cf(&handle, IteratorMode::Start),
        };
        Ok(RawIter { inner, _cf: cf })
    }
}

impl<'a> ReadCtx<'a> {
    /// Like [`iter_raw`](Self::iter_raw), starting at the first key `>= from` under the CF's
    /// comparator (`None`: the first key).
    pub(crate) fn iter_raw_from(
        &self,
        cf: Arc<PhysCf>,
        from: Option<&[u8]>,
    ) -> Result<RawIter<'a>> {
        let handle = self.db.cf_handle(&cf.physical).ok_or_else(|| {
            Error::corrupt(format!("physical column family {} is missing", cf.physical))
        })?;
        let mode = match from {
            Some(k) => IteratorMode::From(k, rocksdb::Direction::Forward),
            None => IteratorMode::Start,
        };
        let inner = match self.snap {
            Some(s) => s.iterator_cf(&handle, mode),
            None => self.db.iterator_cf(&handle, mode),
        };
        Ok(RawIter { inner, _cf: cf })
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::codec::envelope;
    use crate::config::Retention;
    use crate::error::Error;
    use crate::store::Store;
    use crate::store::meta::{self, key};
    use crate::store::test_support::{apply, open_store};
    use crate::types::{CfEntry, CfSpec, Op, Role};

    fn put(k: &[u8], v: &[u8], at: Option<u64>) -> Op {
        Op::Put {
            cf: "c".into(),
            key: k.to_vec(),
            value: v.to_vec(),
            expire_at_ms: at,
        }
    }

    #[test]
    fn entry_crc_and_read_entries() {
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
            apply(&store, vec![put(&[i], b"v", None)]).unwrap();
        }
        let all = store.read_entries(1, 100, usize::MAX).unwrap();
        assert_eq!(
            all.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![1, 2, 3, 4, 5]
        );
        assert_eq!(
            store
                .read_entries(2, 2, usize::MAX)
                .unwrap()
                .iter()
                .map(|e| e.seq)
                .collect::<Vec<_>>(),
            vec![2, 3]
        );
        assert!(store.read_entries(6, 10, usize::MAX).unwrap().is_empty());
        // The byte budget stops a read after the entry that reaches it; one entry is always read.
        let one = all[0].encoded_len();
        assert_eq!(store.read_entries(1, 100, one + 1).unwrap().len(), 2);
        assert_eq!(store.read_entries(1, 100, 1).unwrap().len(), 1);
        assert_eq!(store.entry_crc(3).unwrap(), Some(all[2].crc32c));
        assert_eq!(store.entry_crc(0).unwrap(), None);
        assert_eq!(store.entry_crc(6).unwrap(), None);

        let r = Retention {
            max_age: None,
            max_entries: Some(2),
            check_interval: Duration::from_secs(1),
        };
        store.trim_changelog(0, &r).unwrap();
        assert_eq!(store.entry_crc(3).unwrap(), None, "trimmed");
        assert_eq!(store.entry_crc(5).unwrap(), Some(all[4].crc32c));
        assert!(matches!(
            store.read_entries(3, 10, usize::MAX),
            Err(Error::Gap {
                requested: 3,
                first_retained: 4
            })
        ));

        // With the whole log trimmed, the CRC of the last entry is still known.
        let none = Retention {
            max_age: Some(Duration::from_millis(1)),
            max_entries: None,
            check_interval: Duration::from_secs(1),
        };
        store.trim_changelog(u64::MAX, &none).unwrap();
        assert_eq!(store.first_retained_seq(), 6);
        assert_eq!(store.entry_crc(5).unwrap(), Some(all[4].crc32c));
        assert_eq!(store.entry_crc(4).unwrap(), None);
    }

    #[test]
    fn snapshot_meta_and_raw_iteration_are_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap();
        apply(
            &store,
            vec![
                put(b"b", b"2", Some(5)),
                put(b"", b"", None),
                put(b"a", b"1", None),
            ],
        )
        .unwrap();
        let snap = store.read_snapshot(u64::MAX).unwrap();
        apply(&store, vec![put(b"z", b"later", None)]).unwrap();
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("d"),
            }],
        )
        .unwrap();

        let m = snap.meta().unwrap();
        assert_eq!(m.last_seq, 2);
        assert_eq!(
            m.cf_specs,
            vec![CfEntry {
                spec: CfSpec::new("c"),
                create_seq: 1
            }]
        );
        assert_eq!(
            m.last_entry_crc,
            store
                .read_entries(2, 1, usize::MAX)
                .unwrap()
                .first()
                .map(|e| e.crc32c)
        );

        let phys = snap.phys("c").unwrap();
        assert!(snap.phys("d").is_none());
        let raw: Vec<(Vec<u8>, Vec<u8>)> = snap
            .raw_ctx()
            .iter_raw(phys)
            .unwrap()
            .map(|r| r.map(|(k, v)| (k.into_vec(), v.into_vec())).unwrap())
            .collect();
        let keys: Vec<&[u8]> = raw.iter().map(|(k, _)| &k[..]).collect();
        assert_eq!(
            keys,
            vec![&b""[..], b"a", b"b"],
            "expired b is included; z is not"
        );
        assert_eq!(&raw[2].1[..], envelope::encode(2, Some(5), b"2").as_slice());
    }

    #[test]
    fn replication_meta_keys_load_and_bootstrap_cfs_are_known_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let leftover = CfEntry {
            spec: CfSpec::new("x"),
            create_seq: 7,
        };
        {
            let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
            assert_eq!((m.node_state, m.max_entry_bytes), (None, None));
            assert!(m.bootstrap_cfs.is_empty());
            store
                .write_meta(
                    &[
                        (key::NODE_STATE, b"bootstrapping"),
                        (key::MAX_ENTRY_BYTES, &(16u64 << 20).to_be_bytes()),
                        (
                            key::BOOTSTRAP_CFS,
                            &meta::encode_cf_list(std::slice::from_ref(&leftover)),
                        ),
                    ],
                    true,
                )
                .unwrap();
            // A crash right after an in-progress bootstrap created its physical CF.
            store
                .db()
                .create_cf(leftover.physical_name(), &rocksdb::Options::default())
                .unwrap();
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(m.node_state.as_deref(), Some(&b"bootstrapping"[..]));
        assert_eq!(m.max_entry_bytes, Some(16 << 20));
        assert_eq!(m.bootstrap_cfs, vec![leftover.clone()]);
        assert!(store.db().cf_handle(&leftover.physical_name()).is_some());
    }
}
