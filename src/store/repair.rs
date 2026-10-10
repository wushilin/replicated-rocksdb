//! Store side of repair (DESIGN.md §12.1): rewinding a slave to a common seq in one synced
//! batch, with the affected keys set to the upstream's state; the persisted repair target
//! (`__meta.repair_until`); and the recorded fork point of a divergence.

use std::sync::{Arc, PoisonError};

use rocksdb::{WriteBatch, WriteOptions};

use super::bootstrap::NODE_READY;
use super::cfs::PhysCf;
use super::meta::{self, CF_CHANGELOG, CF_META, key};
use super::{Position, Store};
use crate::error::{Error, Result};
use crate::types::{DbIdentity, EpochStart, Seq};
use crate::verify::range::{Bounds, KeyOrder};

/// A key to set to the upstream's envelope, or to delete (`None`).
pub(crate) type KeyWrite = (Arc<PhysCf>, Vec<u8>, Option<Vec<u8>>);

/// Everything one repair commits atomically.
pub(crate) struct Rewind {
    /// The new `last_seq`: the last entry both sides share.
    pub keep_through: Seq,
    /// `crc32c` of entry `keep_through` (`None` when it is 0 or not known).
    pub keep_crc: Option<u32>,
    /// The current `last_seq`: local log entries `keep_through + 1 ..= drop_through` are
    /// removed. The rewind is refused if the position moved since it was prepared.
    pub drop_through: Seq,
    pub identity: DbIdentity,
    pub epoch_history: Vec<EpochStart>,
    /// Reads answer `NotReady` until `last_seq` reaches this seq again (`None`: ready now).
    /// Persisted as `__meta.repair_until`, apart from `node_state`, so a halt during the
    /// replay (which rewrites `node_state`) cannot make the node ready too early.
    pub repair_until: Option<Seq>,
    /// Every local key in these ranges is deleted first.
    pub clear: Vec<(Arc<PhysCf>, Bounds)>,
    /// Then these keys are set to the upstream's envelope, or deleted (`None`).
    pub writes: Vec<KeyWrite>,
}

impl Store {
    /// Commits a repair in one synced batch: the affected keys, the removal of the local log
    /// suffix, `last_seq` / `last_entry_crc`, identity and epoch history, `node_state = ready`
    /// and `repair_until`. The caller must have stopped applying entries. All or nothing: on an
    /// error nothing was written and the in-memory position is unchanged.
    pub(crate) fn rewind(&self, r: &Rewind) -> Result<()> {
        // Serialised with retention, which publishes `first_retained` under the same lock.
        let _trim = self.guards.lock().unwrap_or_else(PoisonError::into_inner);
        let last = self.position().last_seq;
        if r.drop_through != last || r.keep_through > r.drop_through {
            return Err(Error::Io(std::io::Error::other(format!(
                "repair prepared for last_seq {} (keeping {}), but the slave is at {last}",
                r.drop_through, r.keep_through
            ))));
        }
        let mut batch = WriteBatch::default();
        for (cf, bounds) in &r.clear {
            let order = KeyOrder::new(
                &self.tuning().comparators,
                cf.entry.spec.comparator.as_deref(),
            )?;
            let handle = self.handle(cf)?;
            for item in self
                .live_ctx(0)
                .iter_raw_from(cf.clone(), bounds.lower.as_deref())?
            {
                let (k, _) = item?;
                if !bounds.below_upper(&order, &k) {
                    break;
                }
                batch.delete_cf(&handle, &k);
            }
        }
        for (cf, k, envelope) in &r.writes {
            let handle = self.handle(cf)?;
            match envelope {
                Some(v) => batch.put_cf(&handle, k, v),
                None => batch.delete_cf(&handle, k),
            }
        }
        if r.drop_through > r.keep_through {
            let from = (r.keep_through + 1).to_be_bytes();
            let to = (r.drop_through + 1).to_be_bytes();
            batch.delete_range_cf(&self.cf(CF_CHANGELOG), &from[..], &to[..]);
        }
        let meta_cf = self.cf(CF_META);
        batch.put_cf(&meta_cf, key::LAST_SEQ, r.keep_through.to_be_bytes());
        match r.keep_crc {
            Some(crc) => batch.put_cf(&meta_cf, key::LAST_ENTRY_CRC, crc.to_be_bytes()),
            None => batch.delete_cf(&meta_cf, key::LAST_ENTRY_CRC),
        }
        batch.put_cf(
            &meta_cf,
            key::ORIGIN_IDENTITY,
            meta::encode_identity(&r.identity),
        );
        batch.put_cf(
            &meta_cf,
            key::EPOCH_HISTORY,
            meta::encode_epoch_history(&r.epoch_history),
        );
        // The divergence is resolved: a persisted `Diverged` halt goes too.
        batch.put_cf(&meta_cf, key::NODE_STATE, NODE_READY);
        match r.repair_until {
            Some(seq) => batch.put_cf(&meta_cf, key::REPAIR_UNTIL, seq.to_be_bytes()),
            None => batch.delete_cf(&meta_cf, key::REPAIR_UNTIL),
        }
        batch.delete_cf(&meta_cf, key::DIVERGED_AT);
        drop(meta_cf);
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        self.set_position(Position {
            last_seq: r.keep_through,
            last_entry_crc: r.keep_crc,
        });
        let first = self.first_changelog_seq()?.unwrap_or(r.keep_through + 1);
        self.set_first_retained_seq(first);
        Ok(())
    }

    /// The bytes of local keys (keys and envelopes) in these ranges, or `None` once more than
    /// `limit` (a repair that would delete that much falls back to a bootstrap).
    pub(crate) fn range_bytes(
        &self,
        ranges: &[(Arc<PhysCf>, Bounds)],
        limit: u64,
    ) -> Result<Option<u64>> {
        let mut bytes = 0u64;
        for (cf, bounds) in ranges {
            let order = KeyOrder::new(
                &self.tuning().comparators,
                cf.entry.spec.comparator.as_deref(),
            )?;
            for item in self
                .live_ctx(0)
                .iter_raw_from(cf.clone(), bounds.lower.as_deref())?
            {
                let (k, v) = item?;
                if !bounds.below_upper(&order, &k) {
                    break;
                }
                bytes += (k.len() + v.len()) as u64;
                if bytes > limit {
                    return Ok(None);
                }
            }
        }
        Ok(Some(bytes))
    }

    /// `__meta.repair_until`: the seq replay must reach before the node is ready again.
    pub(crate) fn repair_until(&self) -> Result<Option<Seq>> {
        let raw = self.db().get_cf(&self.cf(CF_META), key::REPAIR_UNTIL)?;
        raw.map(|v| meta::be64(&v)).transpose()
    }

    /// Forgets `__meta.repair_until` (replay reached it). Synced.
    pub(crate) fn clear_repair_until(&self) -> Result<()> {
        self.delete_meta_synced(key::REPAIR_UNTIL)
    }

    fn delete_meta_synced(&self, k: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete_cf(&self.cf(CF_META), k);
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        Ok(self.db().write_opt(batch, &wo)?)
    }

    /// Records (or, with `None`, forgets) where the local divergent suffix starts. Synced.
    pub(crate) fn set_diverged_at(&self, fork: Option<Seq>) -> Result<()> {
        match fork {
            Some(seq) => self.write_meta(&[(key::DIVERGED_AT, &seq.to_be_bytes())], true),
            None => self.delete_meta_synced(key::DIVERGED_AT),
        }
    }

    pub(crate) fn diverged_at(&self) -> Result<Option<Seq>> {
        let raw = self.db().get_cf(&self.cf(CF_META), key::DIVERGED_AT)?;
        raw.map(|v| meta::be64(&v)).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope;
    use crate::read::ReadApi;
    use crate::store::test_support::{apply, open_store};
    use crate::types::{CfSpec, Op};

    fn put(k: &[u8], v: &[u8]) -> Op {
        Op::Put {
            cf: "c".into(),
            key: k.to_vec(),
            value: v.to_vec(),
            expire_at_ms: None,
        }
    }

    fn rewind_to(store: &Store, keep_through: Seq, drop_through: Seq) -> Rewind {
        Rewind {
            keep_through,
            keep_crc: store.entry_crc(keep_through).unwrap(),
            drop_through,
            identity: DbIdentity {
                db_uuid: uuid::Uuid::new_v4(),
                epoch: 1,
            },
            epoch_history: vec![EpochStart {
                epoch: 1,
                start_seq: 1,
                nonce: 0,
            }],
            repair_until: None,
            clear: Vec::new(),
            writes: Vec::new(),
        }
    }

    #[test]
    fn rewind_sets_keys_drops_the_suffix_and_moves_the_position() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap(); // 1
        apply(
            &store,
            vec![
                put(b"a", b"1"),
                put(b"b", b"1"),
                put(b"r1", b"1"),
                put(b"r2", b"1"),
            ],
        )
        .unwrap(); // 2
        let crc2 = store.entry_crc(2).unwrap();
        apply(&store, vec![put(b"a", b"diverged")]).unwrap(); // 3
        apply(
            &store,
            vec![Op::Delete {
                cf: "c".into(),
                key: b"b".to_vec(),
            }],
        )
        .unwrap(); // 4
        store.set_diverged_at(Some(3)).unwrap();
        let cf = store.live_cf("c").unwrap();
        let identity = DbIdentity {
            db_uuid: uuid::Uuid::new_v4(),
            epoch: 2,
        };
        let history = vec![
            EpochStart {
                epoch: 1,
                start_seq: 1,
                nonce: 0,
            },
            EpochStart {
                epoch: 2,
                start_seq: 3,
                nonce: 0,
            },
        ];
        let bounds = Bounds {
            lower: Some(b"r".to_vec()),
            upper: Some(b"s".to_vec()),
        };
        store
            .rewind(&Rewind {
                keep_through: 2,
                keep_crc: crc2,
                drop_through: 4,
                identity,
                epoch_history: history.clone(),
                repair_until: Some(9),
                clear: vec![(cf.clone(), bounds)],
                writes: vec![
                    (
                        cf.clone(),
                        b"a".to_vec(),
                        Some(envelope::encode(5, None, b"upstream")),
                    ),
                    (
                        cf.clone(),
                        b"b".to_vec(),
                        Some(envelope::encode(2, None, b"1")),
                    ),
                    (
                        cf.clone(),
                        b"r2".to_vec(),
                        Some(envelope::encode(6, None, b"kept")),
                    ),
                ],
            })
            .unwrap();
        assert_eq!(
            store.position(),
            Position {
                last_seq: 2,
                last_entry_crc: crc2
            }
        );
        assert_eq!(store.first_retained_seq(), 1);
        assert!(
            store.read_entries(3, 10, usize::MAX).unwrap().is_empty(),
            "the suffix is gone"
        );
        let snap = store.read_snapshot(0).unwrap();
        assert_eq!(snap.get("c", b"a").unwrap(), Some(b"upstream".to_vec()));
        assert_eq!(snap.get("c", b"b").unwrap(), Some(b"1".to_vec()));
        assert_eq!(snap.get("c", b"r1").unwrap(), None, "cleared range");
        assert_eq!(
            snap.get_with_meta("c", b"r2").unwrap().unwrap().1.write_seq,
            6
        );
        drop(snap);
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!((m.identity, m.epoch_history), (Some(identity), history));
        assert_eq!(m.last_seq, 2);
        assert_eq!(m.last_entry_crc, crc2);
        assert_eq!(m.node_state.as_deref(), Some(NODE_READY));
        assert_eq!(store.repair_until().unwrap(), Some(9));
        assert_eq!(store.diverged_at().unwrap(), None, "the fork is resolved");
        // a: 1 + 10 + 8, b: 1 + 10 + 1, r2: 2 + 10 + 4 (key, envelope header, value).
        assert_eq!(
            store
                .range_bytes(&[(cf.clone(), Bounds::default())], u64::MAX)
                .unwrap(),
            Some(47)
        );
        assert_eq!(
            store.range_bytes(&[(cf, Bounds::default())], 3).unwrap(),
            None
        );
        store.clear_repair_until().unwrap();
        assert_eq!(store.repair_until().unwrap(), None);
    }

    /// A rewind prepared for another position (the applier moved since) writes nothing.
    #[test]
    fn a_rewind_prepared_for_another_position_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        for i in 0..3 {
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new(format!("c{i}")),
                }],
            )
            .unwrap();
        }
        let before = store.position();
        assert!(store.rewind(&rewind_to(&store, 1, 2)).is_err(), "at 3");
        assert!(store.rewind(&rewind_to(&store, 3, 2)).is_err());
        assert_eq!(store.position(), before);
        assert_eq!(store.load_meta().unwrap().unwrap().last_seq, 3);
        assert_eq!(store.read_entries(1, 10, usize::MAX).unwrap().len(), 3);
        // The same rewind for the current position goes through.
        store.rewind(&rewind_to(&store, 1, 3)).unwrap();
        assert_eq!(store.position().last_seq, 1);
        assert_eq!(store.read_entries(1, 10, usize::MAX).unwrap().len(), 1);
    }

    #[test]
    fn the_fork_point_is_recorded_and_forgotten() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        assert_eq!(store.diverged_at().unwrap(), None);
        store.set_diverged_at(Some(42)).unwrap();
        assert_eq!(store.diverged_at().unwrap(), Some(42));
        store.set_diverged_at(None).unwrap();
        assert_eq!(store.diverged_at().unwrap(), None);
    }
}
