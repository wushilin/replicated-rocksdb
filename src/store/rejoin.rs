//! Store side of `Slave::rejoin` (DESIGN.md §13.2): one synced batch turns a stopped master
//! into a ready slave without touching its entries.

use rocksdb::{IteratorMode, WriteBatch, WriteOptions};

use super::Store;
use super::meta::{self, CF_META, CF_TTL_INDEX, key};
use crate::error::{Error, Result};
use crate::types::{DbIdentity, EpochStart, Role, Seq};

/// What the rewrite did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Rejoined {
    /// The identity the slave now follows (`None`: the master never had one, no data).
    pub identity: Option<DbIdentity>,
    /// The highest epoch number dropped from the history, if any (burned).
    pub burned: Option<u64>,
}

/// Drops the trailing epochs that hold no entry (`start_seq > last_seq`: per-open epochs of
/// restarts without writes, an empty promotion epoch) and returns the highest dropped number.
/// At least one record stays: the one covering `last_seq` (with `last_seq == 0`, the first).
/// Records are sorted by epoch with non-decreasing `start_seq`, so the empty ones are a suffix.
pub(crate) fn trim_trailing_empty(
    mut history: Vec<EpochStart>,
    last_seq: Seq,
) -> (Vec<EpochStart>, Option<u64>) {
    let keep = history
        .iter()
        .rposition(|e| e.start_seq <= last_seq)
        .map_or(1, |i| i + 1)
        .min(history.len());
    if keep >= history.len() {
        return (history, None);
    }
    let burned = history[keep..].iter().map(|e| e.epoch).max();
    history.truncate(keep);
    (history, burned)
}

impl Store {
    /// One synced batch: role = slave; the trailing empty epochs dropped from the history and
    /// their highest number burned into `max_burned_epoch` (max with the stored value), like
    /// `demote_to_slave`; `origin_identity = (db_uuid, newest remaining epoch)` with the
    /// history; master-only keys removed (`__ttl_index`, `promoted_from_len`, `last_ts`); and
    /// the slave-state keys removed so the slave starts ready (`node_state`, `repair_until`,
    /// `diverged_at`, `max_entry_bytes`, `bootstrap_cfs`). Entries and user data are untouched.
    /// `Err(WrongRole)` unless the stored role is `master`; nothing is written then.
    pub(crate) fn rejoin_as_slave(&self) -> Result<Rejoined> {
        let m = self
            .load_meta()?
            .ok_or_else(|| Error::corrupt("__meta is empty"))?;
        if m.role != Role::Master {
            return Err(Error::WrongRole {
                expected: Role::Master,
                found: m.role,
            });
        }
        if m.identity.is_some() && m.epoch_history.is_empty() {
            return Err(Error::corrupt(
                "origin_identity without epoch_history in __meta",
            ));
        }
        let (history, burned) = trim_trailing_empty(m.epoch_history, m.last_seq);
        let identity = m.identity.map(|id| DbIdentity {
            db_uuid: id.db_uuid,
            epoch: history.last().map_or(id.epoch, |e| e.epoch),
        });
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ROLE, Role::Slave.as_bytes());
        if let Some(id) = &identity {
            // Identity and history in the same batch (CLAUDE.md invariant).
            batch.put_cf(&meta_cf, key::ORIGIN_IDENTITY, meta::encode_identity(id));
            batch.put_cf(
                &meta_cf,
                key::EPOCH_HISTORY,
                meta::encode_epoch_history(&history),
            );
        }
        if let Some(b) = burned {
            batch.put_cf(
                &meta_cf,
                key::MAX_BURNED_EPOCH,
                m.max_burned_epoch.max(b).to_be_bytes(),
            );
        }
        for k in [
            key::PROMOTED_FROM_LEN,
            key::LAST_TS,
            key::NODE_STATE,
            key::REPAIR_UNTIL,
            key::DIVERGED_AT,
            key::MAX_ENTRY_BYTES,
            key::BOOTSTRAP_CFS,
        ] {
            batch.delete_cf(&meta_cf, k);
        }
        // Every `__ttl_index` key sorts below `last ‖ 0x00`.
        let ttl = self.cf(CF_TTL_INDEX);
        let last = self
            .db()
            .iterator_cf(&ttl, IteratorMode::End)
            .next()
            .transpose()?;
        if let Some((last, _)) = last {
            let mut end = last.to_vec();
            end.push(0);
            batch.delete_range_cf(&ttl, Vec::new(), end);
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        Ok(Rejoined { identity, burned })
    }
}

#[cfg(test)]
mod tests {
    use rocksdb::IteratorMode;

    use super::*;
    use crate::store::meta::{CF_TTL_INDEX, key};
    use crate::store::test_support::{apply, open_store};
    use crate::store::ttl::index_key;
    use crate::types::{CfEntry, CfSpec, Op, Role};

    fn rec(epoch: u64, start_seq: Seq) -> EpochStart {
        EpochStart {
            epoch,
            start_seq,
            nonce: epoch * 100,
        }
    }

    #[test]
    fn trailing_empty_epochs_are_trimmed_and_burned() {
        // Empty epochs after the last entry are dropped; the highest is burned.
        let h = vec![rec(1, 1), rec(2, 3), rec(3, 4), rec(4, 4)];
        assert_eq!(
            trim_trailing_empty(h, 3),
            (vec![rec(1, 1), rec(2, 3)], Some(4))
        );
        // An empty epoch in the middle stays (a later one holds entries).
        let h = vec![rec(1, 1), rec(2, 5), rec(3, 5)];
        assert_eq!(trim_trailing_empty(h.clone(), 7), (h, None));
        // last_seq 0 keeps the first record.
        assert_eq!(
            trim_trailing_empty(vec![rec(1, 1), rec(2, 1), rec(3, 1)], 0),
            (vec![rec(1, 1)], Some(3))
        );
        // Nothing to trim.
        assert_eq!(
            trim_trailing_empty(vec![rec(1, 1)], 9),
            (vec![rec(1, 1)], None)
        );
        assert_eq!(trim_trailing_empty(vec![], 0), (vec![], None));
    }

    /// A master store with 3 entries in epochs 1..2, two empty per-open epochs after them, a
    /// TTL index, a promotion record and `last_ts`.
    fn master_with_trailing_epochs(dir: &std::path::Path, burned_before: u64) -> uuid::Uuid {
        let store = open_store(dir);
        let db_uuid = uuid::Uuid::from_u128(7);
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap();
        for k in [b"a", b"b"] {
            let put = Op::Put {
                cf: "c".into(),
                key: k.to_vec(),
                value: vec![1],
                expire_at_ms: Some(50),
            };
            apply(&store, vec![put]).unwrap();
        }
        let history = vec![rec(1, 1), rec(2, 3), rec(3, 4), rec(4, 4)];
        let id = DbIdentity { db_uuid, epoch: 4 };
        store
            .write_meta(
                &[
                    (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                    (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
                    (key::MAX_BURNED_EPOCH, &burned_before.to_be_bytes()),
                    (key::PROMOTED_FROM_LEN, &1u64.to_be_bytes()),
                    (key::LAST_TS, &3_000u64.to_be_bytes()),
                ],
                true,
            )
            .unwrap();
        let c = CfEntry {
            spec: CfSpec::new("c"),
            create_seq: 1,
        };
        let ttl = store.cf(CF_TTL_INDEX);
        for k in [b"a", b"b"] {
            store.db().put_cf(&ttl, index_key(50, &c, k), []).unwrap();
        }
        db_uuid
    }

    #[test]
    fn rejoin_rewrites_a_master_into_a_ready_slave_in_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db_uuid = master_with_trailing_epochs(dir.path(), 0);
        {
            let (store, _) = Store::open(dir.path(), Role::Master, Default::default()).unwrap();
            let r = store.rejoin_as_slave().unwrap();
            assert_eq!(r.identity, Some(DbIdentity { db_uuid, epoch: 2 }));
            assert_eq!(r.burned, Some(4));
        }
        assert!(
            matches!(
                Store::open(dir.path(), Role::Master, Default::default()),
                Err(Error::WrongRole { .. })
            ),
            "the role is persisted"
        );
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(m.role, Role::Slave);
        assert_eq!(m.identity, Some(DbIdentity { db_uuid, epoch: 2 }));
        assert_eq!(m.epoch_history, vec![rec(1, 1), rec(2, 3)]);
        assert_eq!(m.max_burned_epoch, 4);
        assert_eq!(m.last_seq, 3, "no entry is touched");
        assert_eq!(
            (m.promoted_from_len, m.last_ts, m.node_state),
            (None, 0, None)
        );
        assert!(
            store
                .db()
                .iterator_cf(&store.cf(CF_TTL_INDEX), IteratorMode::Start)
                .next()
                .is_none(),
            "slaves keep no TTL index"
        );
        assert_eq!(store.repair_until().unwrap(), None);
    }

    #[test]
    fn an_older_burn_is_kept_when_higher() {
        let dir = tempfile::tempdir().unwrap();
        master_with_trailing_epochs(dir.path(), 9);
        let (store, _) = Store::open(dir.path(), Role::Master, Default::default()).unwrap();
        store.rejoin_as_slave().unwrap();
        assert_eq!(store.load_meta().unwrap().unwrap().max_burned_epoch, 9);
    }

    /// Review focus: a master that never took a write (restarted a few times) keeps only its
    /// first record and becomes a slave without data loss.
    #[test]
    fn a_master_without_entries_keeps_its_first_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let history = vec![rec(1, 1), rec(2, 1), rec(3, 1)];
        let id = DbIdentity {
            db_uuid: uuid::Uuid::from_u128(8),
            epoch: 3,
        };
        store
            .write_meta(
                &[
                    (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                    (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
                ],
                true,
            )
            .unwrap();
        let r = store.rejoin_as_slave().unwrap();
        assert_eq!((r.identity.unwrap().epoch, r.burned), (1, Some(3)));
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!(m.epoch_history, vec![rec(1, 1)]);
    }

    #[test]
    fn stored_meta_reads_without_opening_for_write() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(
            crate::store::offline::stored_meta(dir.path()).unwrap(),
            None
        );
        assert_eq!(
            crate::store::offline::stored_meta(&dir.path().join("missing")).unwrap(),
            None
        );
        let store = open_store(dir.path());
        // Works while the store is open read-write in this process (no LOCK needed).
        let m = crate::store::offline::stored_meta(dir.path())
            .unwrap()
            .unwrap();
        assert_eq!(m.role, Role::Master);
        drop(store);
    }
}
