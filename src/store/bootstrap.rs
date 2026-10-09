//! Store side of installing a bootstrap on a slave (DESIGN.md §10, receiving side), and
//! adopting a new epoch.
//!
//! Crash safety: `reset_for_bootstrap` persists `node_state = bootstrapping` and removes the
//! identity in the same synced batch, so a crash at any later point restarts the bootstrap.
//! Every physical CF a bootstrap creates is first recorded in `__meta.bootstrap_cfs`
//! (synced), so the next open knows its comparator and the next reset drops it.

use std::sync::{Arc, PoisonError};

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
    /// Persists `node_state = bootstrapping` in the same synced batch. Disk is written first;
    /// memory changes only after the write succeeded.
    ///
    /// A physical CF never gets a second `Arc<PhysCf>`: CFs taken over by a bootstrap go back
    /// to `pending` with their original `Arc`, so reader references stay counted.
    pub(crate) fn reset_for_bootstrap(&self) -> Result<()> {
        let leftovers = self.stored_bootstrap_cfs()?;
        {
            let mut live = self.cfs.write();
            let mut pending = self.cfs.pending.lock().unwrap_or_else(PoisonError::into_inner);
            let mut held = self.cfs.held.lock().unwrap_or_else(PoisonError::into_inner);
            let known = |p: &Arc<PhysCf>, e: &CfEntry| p.physical == e.physical_name();
            let mut new_pending: Vec<Arc<PhysCf>> = pending.clone();
            new_pending.extend(held.iter().cloned());
            for e in leftovers {
                // Recorded but never created: nothing to drop.
                let exists = self.db().cf_handle(&e.physical_name()).is_some();
                let tracked = new_pending.iter().any(|p| known(p, &e)) || live.values().any(|p| known(p, &e));
                if exists && !tracked {
                    new_pending.push(PhysCf::new(e));
                }
            }
            new_pending.extend(live.values().cloned());
            let pending_list: Vec<CfEntry> = new_pending.iter().map(|p| p.entry.clone()).collect();
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
            live.clear();
            held.clear();
            *pending = new_pending;
            self.delete_all_keys(&self.cf(CF_TTL_INDEX))?;
            self.set_position(Position::default());
            self.set_first_retained_seq(1);
        }
        self.drop_unreferenced_cfs()?;
        Ok(())
    }

    /// Prepares the physical CF for one bootstrap CF. It is recorded in `bootstrap_cfs`
    /// before it is created. If a pending CF with the same physical name is still referenced
    /// by a reader's snapshot, it is taken over (keeping its reference count) and emptied;
    /// snapshots keep their view. If the spec differs, fails with the retriable
    /// [`Error::CfInUse`]; the slave driver retries it with backoff and reports it in
    /// `SlaveStatus::last_error`.
    pub(crate) fn begin_bootstrap_cf(&self, entry: &CfEntry) -> Result<()> {
        let physical = entry.physical_name();
        // Fails early with UnknownComparator, before anything is recorded.
        let options = self.tuning().cf_options(CfKind::User(&entry.spec))?;
        self.drop_unreferenced_cfs()?;
        let mut recorded = self.stored_bootstrap_cfs()?;
        if !recorded.contains(entry) {
            recorded.push(entry.clone());
        }
        let reused = {
            let mut pending = self.cfs.pending.lock().unwrap_or_else(PoisonError::into_inner);
            let mut held = self.cfs.held.lock().unwrap_or_else(PoisonError::into_inner);
            let index = pending.iter().position(|p| p.physical == physical);
            if let Some(i) = index {
                if pending[i].entry != *entry {
                    return Err(Error::CfInUse { physical });
                }
            }
            if held.iter().any(|p| p.physical == physical && p.entry != *entry) {
                return Err(Error::CfInUse { physical });
            }
            let pending_list: Vec<CfEntry> = pending
                .iter()
                .enumerate()
                .filter(|(i, _)| Some(*i) != index)
                .map(|(_, p)| p.entry.clone())
                .collect();
            let meta_cf = self.cf(CF_META);
            let mut batch = WriteBatch::default();
            batch.put_cf(&meta_cf, key::BOOTSTRAP_CFS, meta::encode_cf_list(&recorded));
            batch.put_cf(&meta_cf, key::PENDING_CF_DROPS, meta::encode_cf_list(&pending_list));
            self.sync_write(batch)?;
            let taken = index.map(|i| pending.swap_remove(i));
            let reused = taken.is_some() || held.iter().any(|p| p.physical == physical);
            if let Some(cf) = taken {
                held.push(cf);
            }
            reused
        };
        if reused || self.db().cf_handle(&physical).is_some() {
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

    /// Commits a completed bootstrap in one synced batch and makes it live. CFs taken over
    /// from a snapshot-held pending drop keep their original `Arc`.
    pub(crate) fn finish_bootstrap(&self, f: &BootstrapFinish) -> Result<()> {
        let created = self.stored_bootstrap_cfs()?;
        let mut live = self.cfs.write();
        let mut pending = self.cfs.pending.lock().unwrap_or_else(PoisonError::into_inner);
        let mut held = self.cfs.held.lock().unwrap_or_else(PoisonError::into_inner);
        let mut new_live = std::collections::BTreeMap::new();
        let mut unused = held.clone();
        for e in &f.cf_specs {
            let cf = match unused.iter().position(|p| p.entry == *e) {
                Some(i) => unused.swap_remove(i),
                None => PhysCf::new(e.clone()),
            };
            new_live.insert(e.spec.name.clone(), cf);
        }
        // Taken-over CFs the final spec does not use go back to the pending drops.
        let mut new_pending = pending.clone();
        new_pending.extend(unused);
        // CFs this bootstrap created that the final spec does not use are dropped later too.
        for e in created {
            let physical = e.physical_name();
            let used = f.cf_specs.contains(&e);
            let tracked = new_pending.iter().any(|p| p.physical == physical) || held.iter().any(|p| p.physical == physical);
            if !used && !tracked && self.db().cf_handle(&physical).is_some() {
                new_pending.push(PhysCf::new(e));
            }
        }
        let pending_list: Vec<CfEntry> = new_pending.iter().map(|p| p.entry.clone()).collect();
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ORIGIN_IDENTITY, meta::encode_identity(&f.identity));
        batch.put_cf(&meta_cf, key::EPOCH_HISTORY, meta::encode_epoch_history(&f.epoch_history));
        batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(&f.cf_specs));
        batch.put_cf(&meta_cf, key::PENDING_CF_DROPS, meta::encode_cf_list(&pending_list));
        batch.put_cf(&meta_cf, key::LAST_SEQ, f.snapshot_seq.to_be_bytes());
        match f.snapshot_crc {
            Some(crc) => batch.put_cf(&meta_cf, key::LAST_ENTRY_CRC, crc.to_be_bytes()),
            None => batch.delete_cf(&meta_cf, key::LAST_ENTRY_CRC),
        }
        batch.put_cf(&meta_cf, key::MAX_ENTRY_BYTES, f.max_entry_bytes.to_be_bytes());
        batch.put_cf(&meta_cf, key::NODE_STATE, NODE_READY);
        batch.delete_cf(&meta_cf, key::BOOTSTRAP_CFS);
        self.sync_write(batch)?;
        *live = new_live;
        *pending = new_pending;
        held.clear();
        drop((live, pending, held));
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
    use crate::store::cfs::PhysCf;
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
    fn a_recorded_but_never_created_cf_does_not_block_resets() {
        let dir = tempfile::tempdir().unwrap();
        let ghost = CfEntry { spec: CfSpec::new("g"), create_seq: 3 };
        {
            let store = slave_store(dir.path());
            store.reset_for_bootstrap().unwrap();
            store.write_meta(&[(crate::store::meta::key::BOOTSTRAP_CFS, &crate::store::meta::encode_cf_list(std::slice::from_ref(&ghost)))], true).unwrap();
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(m.bootstrap_cfs, vec![ghost]);
        store.reset_for_bootstrap().unwrap();
        let m = store.load_meta().unwrap().unwrap();
        assert!(m.bootstrap_cfs.is_empty() && m.pending_drops.is_empty());
        // and a pending ghost (already in pending_drops) is dropped as a no-op
        store.cfs.push_pending(PhysCf::new(CfEntry { spec: CfSpec::new("h"), create_seq: 5 }));
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
    }

    #[test]
    fn a_taken_over_cf_keeps_protecting_the_snapshot_after_finish_and_drop() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put("c", b"old", b"1")]).unwrap();
        let snap = store.read_snapshot(0).unwrap();
        store.reset_for_bootstrap().unwrap();
        let c = CfEntry { spec: CfSpec::new("c"), create_seq: 1 };
        store.begin_bootstrap_cf(&c).unwrap();
        store.finish_bootstrap(&finish(vec![c], 1)).unwrap();
        apply(&store, vec![Op::DropCf { name: "c".into() }]).unwrap();
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0, "the snapshot still references it");
        assert_eq!(snap.get("c", b"old").unwrap(), Some(b"1".to_vec()));
        drop(snap);
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
    }

    #[test]
    fn a_failed_bootstrap_then_reset_keeps_a_held_cf_protected() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put("c", b"old", b"1")]).unwrap();
        let snap = store.read_snapshot(0).unwrap();
        store.reset_for_bootstrap().unwrap();
        let c = CfEntry { spec: CfSpec::new("c"), create_seq: 1 };
        store.begin_bootstrap_cf(&c).unwrap();
        store.reset_for_bootstrap().unwrap();
        assert!(store.db().cf_handle("u:c:1").is_some());
        assert_eq!(snap.get("c", b"old").unwrap(), Some(b"1".to_vec()));
        drop(snap);
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
        assert!(store.db().cf_handle("u:c:1").is_none());
    }

    #[test]
    fn a_snapshot_held_cf_with_another_spec_is_cf_in_use() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = crate::tuning::RocksTuning::new();
        tuning.comparators.register("rev", |a: &[u8], b: &[u8]| b.cmp(a));
        let store = Store::open(dir.path(), Role::Slave, tuning).unwrap().0;
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        let _snap = store.read_snapshot(0).unwrap();
        store.reset_for_bootstrap().unwrap();
        let other = CfEntry { spec: CfSpec::new("c").with_comparator("rev"), create_seq: 1 };
        assert!(matches!(store.begin_bootstrap_cf(&other), Err(Error::CfInUse { .. })));
    }

    #[test]
    fn create_cf_reuses_the_snapshot_held_arc_after_a_reset() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put("c", b"old", b"1")]).unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let original = snap.cfs["c"].clone();
        store.reset_for_bootstrap().unwrap();
        // The same entry (name, create_seq 1) arrives again through the log.
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        let live = store.live_cf("c").unwrap();
        assert!(Arc::ptr_eq(&live, &original), "no second Arc<PhysCf> for one physical CF");
        assert!(store.cfs.pending.lock().unwrap().is_empty());
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0);
        assert!(store.db().cf_handle("u:c:1").is_some(), "the live CF is never dropped");
        apply(&store, vec![put("c", b"new", b"2")]).unwrap();
        assert_eq!(snap.get("c", b"old").unwrap(), Some(b"1".to_vec()), "the snapshot keeps its view");
        assert_eq!(snap.get("c", b"new").unwrap(), None);
        assert_eq!(store.live_ctx(0).get(&live, b"old").unwrap(), None, "reused CF starts empty");
        assert_eq!(store.live_ctx(0).get(&live, b"new").unwrap().map(|v| v.0), Some(b"2".to_vec()));
        drop((snap, original, live));
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0);
        assert_eq!(store.load_meta().unwrap().unwrap().pending_drops.len(), 0);
    }

    #[test]
    fn create_cf_with_another_spec_than_a_held_cf_is_cf_in_use_and_writes_nothing() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = crate::tuning::RocksTuning::new();
        tuning.comparators.register("rev", |a: &[u8], b: &[u8]| b.cmp(a));
        let store = Store::open(dir.path(), Role::Slave, tuning).unwrap().0;
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        let _snap = store.read_snapshot(0).unwrap();
        store.reset_for_bootstrap().unwrap();
        let err = apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c").with_comparator("rev") }]).unwrap_err();
        assert!(matches!(err, Error::CfInUse { .. }), "{err}");
        assert_eq!(store.position().last_seq, 0);
        assert!(store.cfs.live_entries().is_empty());
        assert_eq!(store.load_meta().unwrap().unwrap().pending_drops.len(), 1);
    }

    #[test]
    fn begin_bootstrap_cf_refuses_a_different_entry_for_a_held_physical_name() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = crate::tuning::RocksTuning::new();
        tuning.comparators.register("rev", |a: &[u8], b: &[u8]| b.cmp(a));
        let store = Store::open(dir.path(), Role::Slave, tuning).unwrap().0;
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        let _snap = store.read_snapshot(0).unwrap();
        store.reset_for_bootstrap().unwrap();
        let c = CfEntry { spec: CfSpec::new("c"), create_seq: 1 };
        store.begin_bootstrap_cf(&c).unwrap();
        let other = CfEntry { spec: CfSpec::new("c").with_comparator("rev"), create_seq: 1 };
        assert!(matches!(store.begin_bootstrap_cf(&other), Err(Error::CfInUse { .. })));
    }

    #[test]
    fn finish_queues_bootstrap_cfs_missing_from_the_final_specs() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        store.reset_for_bootstrap().unwrap();
        let kept = CfEntry { spec: CfSpec::new("kept"), create_seq: 1 };
        let gone = CfEntry { spec: CfSpec::new("gone"), create_seq: 2 };
        store.begin_bootstrap_cf(&kept).unwrap();
        store.begin_bootstrap_cf(&gone).unwrap();
        store.finish_bootstrap(&finish(vec![kept], 4)).unwrap();
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!(m.pending_drops, vec![gone], "persisted in the final batch");
        assert!(m.bootstrap_cfs.is_empty());
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
        assert!(store.db().cf_handle("u:gone:2").is_none());
        assert!(store.db().cf_handle("u:kept:1").is_some());
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
