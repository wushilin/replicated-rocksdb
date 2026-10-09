//! Store side of installing a bootstrap on a slave (DESIGN.md §10, receiving side), and
//! adopting a new epoch.
//!
//! Crash safety: `reset_for_bootstrap` persists `node_state = bootstrapping` and removes the
//! identity in the same synced batch, so a crash at any later point restarts the bootstrap.
//! Every physical CF a bootstrap creates is first recorded in `__meta.bootstrap_cfs`
//! (synced), so the next open knows its comparator and the next reset drops it.

use std::sync::PoisonError;

use rocksdb::{AsColumnFamilyRef, IteratorMode, WriteBatch, WriteOptions};

use super::cfs::PhysCf;
use super::meta::{self, key, CF_CHANGELOG, CF_META, CF_TTL_INDEX};
use super::{Position, Store};
use crate::error::{Error, Result};
use crate::tuning::CfKind;
use crate::types::{CfEntry, DbIdentity, EpochStart, Seq};

pub(crate) const NODE_READY: &[u8] = b"ready";
pub(crate) const NODE_BOOTSTRAPPING: &[u8] = b"bootstrapping";

/// Keys deleted per batch when clearing a CF.
const CLEAR_BATCH: usize = 1_000;

/// Everything the final bootstrap batch commits.
#[derive(Clone, Debug)]
pub(crate) struct BootstrapFinish {
    pub identity: DbIdentity,
    pub epoch_history: Vec<EpochStart>,
    pub cf_specs: Vec<CfEntry>,
    pub snapshot_seq: Seq,
    pub snapshot_crc: Option<u32>,
    pub max_entry_bytes: u64,
}

impl Store {
    fn sync_write(&self, batch: WriteBatch) -> Result<()> {
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        Ok(self.db().write_opt(batch, &wo)?)
    }

    fn stored_bootstrap_cfs(&self) -> Result<Vec<CfEntry>> {
        Ok(self.load_meta()?.map(|m| m.bootstrap_cfs).unwrap_or_default())
    }

    /// Discards all replicated state: live CFs and leftovers of an interrupted bootstrap
    /// become pending drops (physically dropped once no snapshot references them), the
    /// changelog and TTL index are cleared, and identity, history and position are removed.
    /// Persists `node_state = bootstrapping` in the same synced batch.
    pub(crate) fn reset_for_bootstrap(&self) -> Result<()> {
        let leftovers = self.stored_bootstrap_cfs()?;
        {
            let mut live = self.cfs.write();
            let mut pending = self.cfs.pending.lock().unwrap_or_else(PoisonError::into_inner);
            for e in leftovers {
                if !pending.iter().any(|p| p.entry == e) && !live.values().any(|p| p.entry == e) {
                    pending.push(PhysCf::new(e));
                }
            }
            pending.extend(std::mem::take(&mut *live).into_values());
            let pending_list: Vec<CfEntry> = pending.iter().map(|p| p.entry.clone()).collect();
            let meta_cf = self.cf(CF_META);
            let mut batch = WriteBatch::default();
            batch.put_cf(&meta_cf, key::NODE_STATE, NODE_BOOTSTRAPPING);
            batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(&[]));
            batch.put_cf(&meta_cf, key::PENDING_CF_DROPS, meta::encode_cf_list(&pending_list));
            batch.put_cf(&meta_cf, key::LAST_SEQ, 0u64.to_be_bytes());
            for k in [key::BOOTSTRAP_CFS, key::LAST_ENTRY_CRC, key::ORIGIN_IDENTITY, key::EPOCH_HISTORY, key::MAX_ENTRY_BYTES] {
                batch.delete_cf(&meta_cf, k);
            }
            // Changelog keys are 8 bytes; [0xFF; 9] sorts after all of them.
            batch.delete_range_cf(&self.cf(CF_CHANGELOG), &[][..], &[0xFFu8; 9][..]);
            self.sync_write(batch)?;
            self.delete_all_keys(&self.cf(CF_TTL_INDEX))?;
            self.set_position(Position::default());
            self.set_first_retained_seq(1);
        }
        self.drop_unreferenced_cfs()?;
        Ok(())
    }

    /// Prepares the physical CF for one bootstrap CF. It is recorded in `bootstrap_cfs`
    /// before it is created. If a pending CF with the same physical name is still referenced
    /// by a reader's snapshot, it is taken over and emptied (snapshots keep their view).
    pub(crate) fn begin_bootstrap_cf(&self, entry: &CfEntry) -> Result<()> {
        let physical = entry.physical_name();
        // Fails early with UnknownComparator, before anything is recorded.
        let options = self.tuning().cf_options(CfKind::User(&entry.spec))?;
        self.drop_unreferenced_cfs()?;
        let mut recorded = self.stored_bootstrap_cfs()?;
        recorded.push(entry.clone());
        let reused = {
            let mut pending = self.cfs.pending.lock().unwrap_or_else(PoisonError::into_inner);
            let taken = match pending.iter().position(|p| p.physical == physical) {
                Some(i) if pending[i].entry != *entry => {
                    return Err(Error::Config(format!(
                        "column family {physical} is still referenced by an open snapshot with a different spec; \
                         release it and the bootstrap will be retried"
                    )));
                }
                Some(i) => Some(pending.swap_remove(i)),
                None => None,
            };
            let pending_list: Vec<CfEntry> = pending.iter().map(|p| p.entry.clone()).collect();
            let meta_cf = self.cf(CF_META);
            let mut batch = WriteBatch::default();
            batch.put_cf(&meta_cf, key::BOOTSTRAP_CFS, meta::encode_cf_list(&recorded));
            batch.put_cf(&meta_cf, key::PENDING_CF_DROPS, meta::encode_cf_list(&pending_list));
            self.sync_write(batch)?;
            taken
        };
        if reused.is_some() || self.db().cf_handle(&physical).is_some() {
            self.clear_physical_cf(&physical)
        } else {
            self.db().create_cf(&physical, &options)?;
            // rust-rocksdb does not keep a created CF's comparator alive; the store does.
            self.retain_cf_options(options);
            Ok(())
        }
    }

    fn clear_physical_cf(&self, physical: &str) -> Result<()> {
        let handle = self
            .db()
            .cf_handle(physical)
            .ok_or_else(|| Error::corrupt(format!("physical column family {physical} is missing")))?;
        self.delete_all_keys(&handle)
    }

    /// Deletes every key of a CF, in unsynced batches of `CLEAR_BATCH` keys.
    pub(crate) fn delete_all_keys(&self, cf: &impl AsColumnFamilyRef) -> Result<()> {
        let mut batch = WriteBatch::default();
        for item in self.db().iterator_cf(cf, IteratorMode::Start) {
            batch.delete_cf(cf, item?.0);
            if batch.len() >= CLEAR_BATCH {
                self.db().write(std::mem::take(&mut batch))?;
            }
        }
        if !batch.is_empty() {
            self.db().write(batch)?;
        }
        Ok(())
    }

    /// Writes one bootstrap records message in one batch, without fsync (the final
    /// bootstrap batch is synced, which makes everything before it durable).
    pub(crate) fn write_bootstrap_records(&self, entry: &CfEntry, records: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let physical = entry.physical_name();
        let handle = self
            .db()
            .cf_handle(&physical)
            .ok_or_else(|| Error::corrupt(format!("bootstrap column family {physical} is missing")))?;
        let mut batch = WriteBatch::default();
        for (k, v) in records {
            batch.put_cf(&handle, k, v);
        }
        Ok(self.db().write(batch)?)
    }

    /// Commits a completed bootstrap in one synced batch and makes it live.
    pub(crate) fn finish_bootstrap(&self, f: &BootstrapFinish) -> Result<()> {
        let mut live = self.cfs.write();
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ORIGIN_IDENTITY, meta::encode_identity(&f.identity));
        batch.put_cf(&meta_cf, key::EPOCH_HISTORY, meta::encode_epoch_history(&f.epoch_history));
        batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(&f.cf_specs));
        batch.put_cf(&meta_cf, key::LAST_SEQ, f.snapshot_seq.to_be_bytes());
        match f.snapshot_crc {
            Some(crc) => batch.put_cf(&meta_cf, key::LAST_ENTRY_CRC, crc.to_be_bytes()),
            None => batch.delete_cf(&meta_cf, key::LAST_ENTRY_CRC),
        }
        batch.put_cf(&meta_cf, key::MAX_ENTRY_BYTES, f.max_entry_bytes.to_be_bytes());
        batch.put_cf(&meta_cf, key::NODE_STATE, NODE_READY);
        batch.delete_cf(&meta_cf, key::BOOTSTRAP_CFS);
        self.sync_write(batch)?;
        *live = f.cf_specs.iter().map(|e| (e.spec.name.clone(), PhysCf::new(e.clone()))).collect();
        drop(live);
        self.set_position(Position { last_seq: f.snapshot_seq, last_entry_crc: f.snapshot_crc });
        self.set_first_retained_seq(f.snapshot_seq + 1);
        Ok(())
    }

    /// Persists a new identity and epoch history together (EPOCH_ADVANCED), synced.
    pub(crate) fn adopt_identity(&self, identity: &DbIdentity, history: &[EpochStart]) -> Result<()> {
        self.write_meta(
            &[
                (key::ORIGIN_IDENTITY, &meta::encode_identity(identity)),
                (key::EPOCH_HISTORY, &meta::encode_epoch_history(history)),
            ],
            true,
        )
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope;
    use crate::read::ReadApi;
    use crate::store::test_support::apply;
    use crate::types::{CfSpec, Op, Role};

    fn slave_store(dir: &std::path::Path) -> Store {
        Store::open(dir, Role::Slave, Default::default()).unwrap().0
    }

    fn put(cf: &str, k: &[u8], v: &[u8]) -> Op {
        Op::Put { cf: cf.into(), key: k.to_vec(), value: v.to_vec(), expire_at_ms: None }
    }

    fn finish(cfs: Vec<CfEntry>, seq: Seq) -> BootstrapFinish {
        BootstrapFinish {
            identity: DbIdentity { db_uuid: uuid::Uuid::from_u128(7), epoch: 2 },
            epoch_history: vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 3 }],
            cf_specs: cfs,
            snapshot_seq: seq,
            snapshot_crc: Some(0xABCD),
            max_entry_bytes: 1 << 20,
        }
    }

    fn rec(k: &[u8], seq: Seq, v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (k.to_vec(), envelope::encode(seq, None, v))
    }

    #[test]
    fn reset_discards_everything_and_persists_bootstrapping() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put("c", b"k", b"v")]).unwrap();
        store.reset_for_bootstrap().unwrap();
        assert_eq!(store.position(), Position::default());
        assert_eq!(store.first_retained_seq(), 1);
        assert!(store.cfs.live_entries().is_empty());
        assert!(store.db().cf_handle("u:c:1").is_none(), "unreferenced CF dropped at once");
        assert!(store.entries_after(0).unwrap().next().is_none(), "changelog cleared");
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!(m.node_state.as_deref(), Some(NODE_BOOTSTRAPPING));
        assert_eq!((m.identity, m.last_seq, m.last_entry_crc), (None, 0, None));
        assert!(m.cf_specs.is_empty() && m.pending_drops.is_empty());
    }

    #[test]
    fn full_install_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let c = CfEntry { spec: CfSpec::new("c"), create_seq: 2 };
        let empty = CfEntry { spec: CfSpec::new("e"), create_seq: 1 };
        {
            let store = slave_store(dir.path());
            store.reset_for_bootstrap().unwrap();
            store.begin_bootstrap_cf(&c).unwrap();
            store.begin_bootstrap_cf(&empty).unwrap();
            store.write_bootstrap_records(&c, &[rec(b"", 3, b""), rec(b"\0", 4, b"\0")]).unwrap();
            store.write_bootstrap_records(&c, &[rec(b"k", 5, b"v")]).unwrap();
            store.finish_bootstrap(&finish(vec![c.clone(), empty.clone()], 9)).unwrap();
            assert_eq!(store.position(), Position { last_seq: 9, last_entry_crc: Some(0xABCD) });
            assert_eq!(store.first_retained_seq(), 10);
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(m.node_state.as_deref(), Some(NODE_READY));
        assert!(m.bootstrap_cfs.is_empty());
        assert_eq!(m.cf_specs, vec![c, empty]);
        assert_eq!((m.last_seq, m.last_entry_crc, m.max_entry_bytes), (9, Some(0xABCD), Some(1 << 20)));
        assert_eq!(m.identity.unwrap().epoch, 2);
        assert_eq!(m.epoch_history.len(), 2);
        let snap = store.read_snapshot(0).unwrap();
        assert_eq!(snap.get("c", b"").unwrap(), Some(vec![]));
        assert_eq!(snap.get("c", b"\0").unwrap(), Some(vec![0]));
        assert_eq!(snap.get_with_meta("c", b"k").unwrap().unwrap().1.write_seq, 5);
    }

    #[test]
    fn crash_mid_bootstrap_reopens_and_the_next_reset_drops_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = crate::tuning::RocksTuning::new();
        tuning.comparators.register("rev", |a: &[u8], b: &[u8]| b.cmp(a));
        let c = CfEntry { spec: CfSpec::new("c").with_comparator("rev"), create_seq: 4 };
        {
            let store = Store::open(dir.path(), Role::Slave, tuning.clone()).unwrap().0;
            store.reset_for_bootstrap().unwrap();
            store.begin_bootstrap_cf(&c).unwrap();
            store.write_bootstrap_records(&c, &[rec(b"z", 1, b"v")]).unwrap();
            // crash: no finish
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, tuning).unwrap();
        assert_eq!(m.node_state.as_deref(), Some(NODE_BOOTSTRAPPING));
        assert_eq!(m.bootstrap_cfs, vec![c.clone()]);
        assert!(store.db().cf_handle("u:c:4").is_some());
        store.reset_for_bootstrap().unwrap();
        assert!(store.db().cf_handle("u:c:4").is_none());
        assert!(store.load_meta().unwrap().unwrap().bootstrap_cfs.is_empty());
    }

    #[test]
    fn a_cf_held_by_a_snapshot_is_taken_over_without_disturbing_the_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put("c", b"old", b"1")]).unwrap();
        let snap = store.read_snapshot(0).unwrap();
        store.reset_for_bootstrap().unwrap();
        assert!(store.db().cf_handle("u:c:1").is_some(), "the snapshot still references it");

        let c = CfEntry { spec: CfSpec::new("c"), create_seq: 1 };
        store.begin_bootstrap_cf(&c).unwrap();
        store.write_bootstrap_records(&c, &[rec(b"new", 7, b"2")]).unwrap();
        store.finish_bootstrap(&finish(vec![c], 7)).unwrap();

        assert_eq!(snap.get("c", b"old").unwrap(), Some(b"1".to_vec()));
        assert_eq!(snap.get("c", b"new").unwrap(), None);
        let now = store.read_snapshot(0).unwrap();
        assert_eq!(now.get("c", b"old").unwrap(), None);
        assert_eq!(now.get("c", b"new").unwrap(), Some(b"2".to_vec()));
        drop(snap);
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0, "the live CF is no longer pending");
        assert!(store.load_meta().unwrap().unwrap().pending_drops.is_empty());
    }

    #[test]
    fn adopt_identity_persists_identity_and_history_together() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        let id = DbIdentity { db_uuid: uuid::Uuid::from_u128(1), epoch: 3 };
        let h = vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 3, start_seq: 40 }];
        store.adopt_identity(&id, &h).unwrap();
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!((m.identity, m.epoch_history), (Some(id), h));
    }
}
