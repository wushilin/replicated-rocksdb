//! Turning one `LogEntry` into one atomic `WriteBatch` (DESIGN.md §5, §6, §7).

use std::cmp::Ordering;
use std::sync::{Arc, PoisonError};

use rocksdb::{WriteBatch, WriteOptions};

use super::cfs::PhysCf;
use super::meta::{self, CF_CHANGELOG, CF_META, key};
use super::{Position, Store};
use crate::codec::{check_structure, envelope};
use crate::error::{Error, Result};
use crate::tuning::CfKind;
use crate::types::{CfEntry, LogEntry, Op, Seq};

pub(crate) enum CfChange {
    Create(CfEntry),
    Drop(Arc<PhysCf>),
}

/// A validated entry ready to commit. Building one has no side effects.
pub(crate) struct Prepared {
    pub batch: WriteBatch,
    pub seq: Seq,
    pub crc: u32,
    cf_change: Option<CfChange>,
}

impl Prepared {
    /// The position the store has once this entry is committed.
    pub(crate) fn position(&self) -> Position {
        Position {
            last_seq: self.seq,
            last_entry_crc: Some(self.crc),
        }
    }
}

/// Consecutive entries without CF ops, prepared into one `WriteBatch` (slave group commit,
/// DESIGN.md §6). Every entry is whole in it: its ops, `__changelog[seq]` and the position keys
/// (later entries overwrite `__meta.last_seq` / `last_entry_crc` inside the batch, so the batch
/// ends on the last entry's). Building one has no side effects.
pub(crate) struct PreparedGroup {
    batch: WriteBatch,
    /// The position the group starts after.
    base: Position,
    /// The position once the group is committed.
    last: Position,
    len: usize,
}

impl PreparedGroup {
    /// An empty group that starts right after `base`.
    pub(crate) fn after(base: Position) -> Self {
        PreparedGroup {
            batch: WriteBatch::default(),
            base,
            last: base,
            len: 0,
        }
    }

    /// Number of entries in the group.
    pub(crate) fn len(&self) -> usize {
        self.len
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len == 0
    }

    /// The position the store has once the group is committed.
    pub(crate) fn position(&self) -> Position {
        self.last
    }
}

/// True when `ops` change the CF registry (such entries never share a batch).
pub(crate) fn is_cf_entry(ops: &[Op]) -> bool {
    ops.iter()
        .any(|op| matches!(op, Op::CreateCf { .. } | Op::DropCf { .. }))
}

impl Store {
    /// Prepares `entry` as the next entry after the stored position.
    pub(crate) fn prepare(&self, entry: &LogEntry, ops: &[Op]) -> Result<Prepared> {
        self.prepare_after(self.position(), entry, ops)
    }

    /// Prepares `entry` on top of `prev` (the previous entry of the same group) instead of the
    /// stored position. Everything else it reads is the CF registry and the comparators, which
    /// only a CF entry changes, and CF entries are never grouped.
    pub(crate) fn prepare_after(
        &self,
        prev: Position,
        entry: &LogEntry,
        ops: &[Op],
    ) -> Result<Prepared> {
        let mut batch = WriteBatch::default();
        let cf_change = self.prepare_into(prev.last_seq, &mut batch, entry, ops)?;
        Ok(Prepared {
            batch,
            seq: entry.seq,
            crc: entry.crc32c,
            cf_change,
        })
    }

    /// Adds `entry` to `group` as its next entry. On error the group is unchanged (nothing of
    /// `entry` is in it), so the entries before it can still be committed. A CF entry is
    /// refused (`Corrupt`): it commits alone, holding the registry lock.
    pub(crate) fn prepare_in_group(
        &self,
        group: &mut PreparedGroup,
        entry: &LogEntry,
        ops: &[Op],
    ) -> Result<()> {
        if is_cf_entry(ops) {
            return Err(Error::corrupt(format!(
                "entry {} changes a column family and cannot join a group",
                entry.seq
            )));
        }
        let cf_change = self.prepare_into(group.last.last_seq, &mut group.batch, entry, ops)?;
        debug_assert!(cf_change.is_none());
        group.last = Position {
            last_seq: entry.seq,
            last_entry_crc: Some(entry.crc32c),
        };
        group.len += 1;
        Ok(())
    }

    /// Validates `entry` as the entry after `prev_seq`, then writes it into `batch`: its ops,
    /// `__changelog[seq]`, `__meta.last_seq` and `last_entry_crc`. All checks run before the
    /// first write, so an error leaves `batch` untouched.
    fn prepare_into(
        &self,
        prev_seq: Seq,
        batch: &mut WriteBatch,
        entry: &LogEntry,
        ops: &[Op],
    ) -> Result<Option<CfChange>> {
        if entry.seq != prev_seq + 1 {
            return Err(Error::corrupt(format!(
                "entry {} does not follow last_seq {prev_seq}",
                entry.seq
            )));
        }
        check_structure(ops)?;

        let meta_cf = self.cf(CF_META);
        let live = self.cfs.read();
        let lookup = |name: &str| {
            live.get(name)
                .cloned()
                .ok_or_else(|| Error::NoSuchCf(name.to_string()))
        };

        // Validate every op and resolve its column family; nothing is written yet.
        let mut handles = Vec::with_capacity(ops.len());
        let mut cf_change = None;
        let mut specs = None;
        for op in ops {
            match op {
                Op::Put { cf, .. } | Op::Delete { cf, .. } => {
                    let phys = lookup(cf)?;
                    handles.push(Some(self.handle(&phys)?));
                }
                Op::DeleteRange { cf, start, end } => {
                    let phys = lookup(cf)?;
                    let cmp = self.tuning().comparators.compare(
                        phys.entry.spec.comparator.as_deref(),
                        start,
                        end,
                    );
                    if cmp != Ordering::Less {
                        return Err(Error::InvalidRange);
                    }
                    handles.push(Some(self.handle(&phys)?));
                }
                Op::CreateCf { spec } => {
                    if live.contains_key(&spec.name) {
                        return Err(Error::AlreadyExists(spec.name.clone()));
                    }
                    if let Some(c) = &spec.comparator {
                        if !self.tuning().comparators.contains(c) {
                            return Err(Error::UnknownComparator(c.clone()));
                        }
                    }
                    let new = CfEntry {
                        spec: spec.clone(),
                        create_seq: entry.seq,
                    };
                    let mut list: Vec<CfEntry> = live.values().map(|p| p.entry.clone()).collect();
                    list.push(new.clone());
                    list.sort_by(|a, b| a.spec.name.cmp(&b.spec.name));
                    specs = Some(list);
                    cf_change = Some(CfChange::Create(new));
                    handles.push(None);
                }
                Op::DropCf { name } => {
                    let phys = lookup(name)?;
                    let list: Vec<CfEntry> = live
                        .values()
                        .filter(|p| p.entry.spec.name != *name)
                        .map(|p| p.entry.clone())
                        .collect();
                    specs = Some(list);
                    cf_change = Some(CfChange::Drop(phys));
                    handles.push(None);
                }
            }
        }
        drop(live);

        for (op, handle) in ops.iter().zip(&handles) {
            match (op, handle) {
                (
                    Op::Put {
                        key,
                        value,
                        expire_at_ms,
                        ..
                    },
                    Some(h),
                ) => batch.put_cf(h, key, envelope::encode(entry.seq, *expire_at_ms, value)),
                (Op::Delete { key, .. }, Some(h)) => batch.delete_cf(h, key),
                (Op::DeleteRange { start, end, .. }, Some(h)) => {
                    batch.delete_range_cf(h, start, end)
                }
                (Op::CreateCf { .. }, None) => {
                    let list = specs.as_deref().unwrap_or_default();
                    batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(list));
                    batch.delete_cf(&meta_cf, key::CF_CREATE_INTENT);
                }
                (Op::DropCf { .. }, None) => {
                    let list = specs.as_deref().unwrap_or_default();
                    batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(list));
                }
                _ => unreachable!("every data op was resolved to a handle above"),
            }
        }

        batch.put_cf(
            &self.cf(CF_CHANGELOG),
            entry.seq.to_be_bytes(),
            entry.encode_to_vec(),
        );
        batch.put_cf(&meta_cf, key::LAST_SEQ, entry.seq.to_be_bytes());
        batch.put_cf(&meta_cf, key::LAST_ENTRY_CRC, entry.crc32c.to_be_bytes());
        Ok(cf_change)
    }

    /// Commits a group of consecutive non-CF entries in one `WriteBatch` (one fsync with
    /// `sync`). All or nothing: on error nothing of the group is committed and the position
    /// does not move. An empty group does nothing.
    pub(crate) fn commit_group(&self, group: PreparedGroup, sync: bool) -> Result<()> {
        if group.is_empty() {
            return Ok(());
        }
        let last = self.position();
        if group.base.last_seq != last.last_seq {
            return Err(Error::corrupt(format!(
                "commit of a group after {} does not follow last_seq {}",
                group.base.last_seq, last.last_seq
            )));
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        let pos = group.position();
        self.db().write_opt(group.batch, &wo)?;
        self.set_position(pos);
        Ok(())
    }

    /// Writes a prepared entry. CF changes hold the registry write lock across the write,
    /// so snapshots always see a CF list that matches their data.
    pub(crate) fn commit(&self, p: Prepared, sync: bool) -> Result<()> {
        let last = self.position().last_seq;
        if p.seq != last + 1 {
            return Err(Error::corrupt(format!(
                "commit of entry {} does not follow last_seq {last}",
                p.seq
            )));
        }
        let pos = p.position();
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        let mut p = p;
        match p.cf_change.take() {
            Some(CfChange::Create(entry)) => {
                // The registry write lock is held across the fsynced intent, create_cf and the
                // batch, so readers stall briefly during a CF create. Acceptable: creates are rare.
                // Lock order: registry write lock, then pending.
                let mut live = self.cfs.write();
                let mut pending = self
                    .cfs
                    .pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let physical = entry.physical_name();
                // A pending (snapshot-held) CF with this physical name keeps its one `Arc`:
                // it is emptied and made live again, never given a second `Arc<PhysCf>`.
                let reuse = pending.iter().position(|c| c.physical == physical);
                if let Some(i) = reuse {
                    if pending[i].entry != entry {
                        return Err(Error::CfInUse { physical });
                    }
                    let list: Vec<CfEntry> = pending
                        .iter()
                        .enumerate()
                        .filter(|(j, _)| *j != i)
                        .map(|(_, c)| c.entry.clone())
                        .collect();
                    p.batch.put_cf(
                        &self.cf(CF_META),
                        key::PENDING_CF_DROPS,
                        meta::encode_cf_list(&list),
                    );
                }
                // Note: `delete_all_keys` below runs under the registry write lock; reuse is rare
                // (a snapshot-held CF re-created at the same seq) and reads stall meanwhile.
                // A leftover intent (a commit that failed or crashed after create_cf) for this
                // physical name but another spec: the existing physical CF was made for that
                // spec (its comparator may differ) and must not be reused for this entry.
                let leftover = self.db().get_cf(&self.cf(CF_META), key::CF_CREATE_INTENT)?;
                if let Some(prev) = leftover.map(|v| meta::decode_cf_entry(&v)).transpose()? {
                    let orphan = prev.physical_name();
                    if orphan == physical
                        && prev != entry
                        && reuse.is_none()
                        && self.db().cf_handle(&physical).is_some()
                    {
                        return Err(Error::corrupt(format!(
                            "physical CF {physical} was created for {:?} by an unfinished commit; entry {} creates {:?}",
                            prev.spec, p.seq, entry.spec
                        )));
                    }
                    // A leftover of another physical name: the intent below overwrites the only
                    // record of it, so drop it now, or the next open would find a physical CF
                    // that `__meta` does not know.
                    if orphan != physical
                        && !live.values().any(|c| c.physical == orphan)
                        && !pending.iter().any(|c| c.physical == orphan)
                        && self.db().cf_handle(&orphan).is_some()
                    {
                        tracing::warn!(cf = %orphan, "dropping a column family left by an unfinished create");
                        self.db().drop_cf(&orphan)?;
                    }
                }
                // Record the intent durably first: a crash after create_cf leaves a CF whose
                // comparator would otherwise be unknown at the next open.
                self.write_meta(
                    &[(key::CF_CREATE_INTENT, &meta::encode_cf_entry(&entry))],
                    true,
                )?;
                match self.db().cf_handle(&physical) {
                    None => {
                        let opts = self.tuning().cf_options(CfKind::User(&entry.spec))?;
                        self.db().create_cf(&physical, &opts)?;
                        // create_cf does not keep the custom comparator alive; the CF would dangle.
                        self.retain_cf_options(opts);
                    }
                    // Snapshots keep their view; only the live data is emptied.
                    Some(handle) if reuse.is_some() => self.delete_all_keys(&handle)?,
                    Some(_) => {}
                }
                self.db().write_opt(p.batch, &wo)?;
                let cf = match reuse {
                    Some(i) => pending.swap_remove(i),
                    None => PhysCf::new(entry.clone()),
                };
                live.insert(entry.spec.name.clone(), cf);
            }
            Some(CfChange::Drop(phys)) => {
                // Lock order: registry write lock, then pending. The persisted pending list is
                // computed under the lock so a concurrent physical drop cannot lose this entry.
                let mut live = self.cfs.write();
                let mut pending = self
                    .cfs
                    .pending
                    .lock()
                    .unwrap_or_else(PoisonError::into_inner);
                let mut list: Vec<CfEntry> = pending.iter().map(|c| c.entry.clone()).collect();
                list.push(phys.entry.clone());
                p.batch.put_cf(
                    &self.cf(CF_META),
                    key::PENDING_CF_DROPS,
                    meta::encode_cf_list(&list),
                );
                self.db().write_opt(p.batch, &wo)?;
                live.remove(&phys.entry.spec.name);
                pending.push(phys);
            }
            None => self.db().write_opt(p.batch, &wo)?,
        }
        self.set_position(pos);
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use crate::codec::envelope;
    use crate::error::Error;
    use crate::store::meta::{CF_CHANGELOG, key};
    use crate::store::test_support::{apply, open_store, open_store_with};
    use crate::tuning::RocksTuning;
    use crate::types::{CfSpec, LogEntry, Op};

    fn put(cf: &str, k: &[u8], v: &[u8]) -> Op {
        Op::Put {
            cf: cf.into(),
            key: k.to_vec(),
            value: v.to_vec(),
            expire_at_ms: None,
        }
    }
    fn create(name: &str) -> Op {
        Op::CreateCf {
            spec: CfSpec::new(name),
        }
    }
    fn raw(store: &crate::store::Store, cf: &str, k: &[u8]) -> Option<Vec<u8>> {
        let phys = store.live_cf(cf).unwrap();
        store.db().get_cf(&store.handle(&phys).unwrap(), k).unwrap()
    }

    #[test]
    fn put_writes_envelope_changelog_and_position_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        assert_eq!(apply(&store, vec![create("users")]).unwrap(), 1);
        assert_eq!(apply(&store, vec![put("users", b"k", b"v")]).unwrap(), 2);

        let env = raw(&store, "users", b"k").unwrap();
        assert_eq!(envelope::decode(&env).unwrap().meta.write_seq, 2);
        let cl = store
            .db()
            .get_cf(&store.cf(CF_CHANGELOG), 2u64.to_be_bytes())
            .unwrap()
            .unwrap();
        let entry = LogEntry::decode(&cl).unwrap();
        assert_eq!(entry.seq, 2);
        assert_eq!(store.position().last_seq, 2);
        assert_eq!(store.position().last_entry_crc, Some(entry.crc32c));
        let meta = store.load_meta().unwrap().unwrap();
        assert_eq!(meta.last_seq, 2);
        assert_eq!(meta.last_entry_crc, Some(entry.crc32c));
        assert_eq!(meta.cf_specs.len(), 1);
    }

    #[test]
    fn empty_keys_and_values_work() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        apply(&store, vec![put("c", b"", b"")]).unwrap();
        let env = raw(&store, "c", b"").unwrap();
        assert_eq!(envelope::decode(&env).unwrap().value, b"");
        apply(
            &store,
            vec![Op::DeleteRange {
                cf: "c".into(),
                start: b"".to_vec(),
                end: b"a".to_vec(),
            }],
        )
        .unwrap();
        assert_eq!(raw(&store, "c", b""), None);
    }

    #[test]
    fn nul_empty_and_large_keys_and_values_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let big_key = vec![0xABu8; 1 << 20];
        let big_val: Vec<u8> = (0..8usize << 20).map(|i| (i % 251) as u8).collect();
        let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"".to_vec(), b"".to_vec()),
            (b"\0".to_vec(), b"\0".to_vec()),
            (b"\0\0k\0".to_vec(), b"v\0\0".to_vec()),
            (vec![0u8; 64], vec![0u8; 64]),
            (big_key, b"small".to_vec()),
            (b"big-value".to_vec(), big_val),
        ];
        {
            let store = open_store(dir.path());
            apply(&store, vec![create("c")]).unwrap();
            for (k, v) in &cases {
                apply(&store, vec![put("c", k, v)]).unwrap();
            }
        }
        let store = open_store(dir.path());
        for (k, v) in &cases {
            let env =
                raw(&store, "c", k).unwrap_or_else(|| panic!("missing key of length {}", k.len()));
            assert_eq!(
                envelope::decode(&env).unwrap().value,
                v.as_slice(),
                "key of length {}",
                k.len()
            );
        }
        // b"" and b"\0" are distinct keys.
        apply(
            &store,
            vec![Op::Delete {
                cf: "c".into(),
                key: b"".to_vec(),
            }],
        )
        .unwrap();
        assert!(raw(&store, "c", b"").is_none());
        assert!(raw(&store, "c", b"\0").is_some());
    }

    #[test]
    fn validation_errors_leave_no_trace() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        #[allow(clippy::type_complexity)]
        let cases: Vec<(Vec<Op>, fn(&Error) -> bool)> = vec![
            (vec![], |e| matches!(e, Error::EmptyBatch)),
            (vec![put("missing", b"k", b"v")], |e| {
                matches!(e, Error::NoSuchCf(_))
            }),
            (vec![create("c")], |e| matches!(e, Error::AlreadyExists(_))),
            (vec![create("__x")], |e| {
                matches!(e, Error::InvalidCfName(_))
            }),
            (
                vec![Op::CreateCf {
                    spec: CfSpec::new("d").with_comparator("nope"),
                }],
                |e| matches!(e, Error::UnknownComparator(_)),
            ),
            (
                vec![Op::DeleteRange {
                    cf: "c".into(),
                    start: b"b".to_vec(),
                    end: b"a".to_vec(),
                }],
                |e| matches!(e, Error::InvalidRange),
            ),
            (
                vec![Op::DeleteRange {
                    cf: "c".into(),
                    start: b"a".to_vec(),
                    end: b"a".to_vec(),
                }],
                |e| matches!(e, Error::InvalidRange),
            ),
            (
                vec![Op::DropCf {
                    name: "missing".into(),
                }],
                |e| matches!(e, Error::NoSuchCf(_)),
            ),
            (vec![create("e"), put("c", b"k", b"v")], |e| {
                matches!(e, Error::Corrupt(_))
            }),
        ];
        for (ops, expected) in cases {
            let err = apply(&store, ops.clone()).unwrap_err();
            assert!(expected(&err), "{ops:?} gave {err:?}");
        }
        assert_eq!(store.position().last_seq, 1);
    }

    #[test]
    fn out_of_order_entry_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ops = vec![create("c")];
        let entry = LogEntry::new(5, 1, 0, crate::codec::encode_ops(&ops));
        assert!(matches!(
            store.prepare(&entry, &ops),
            Err(Error::Corrupt(_))
        ));
    }

    #[test]
    fn drop_is_deferred_while_referenced_then_physical() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        apply(&store, vec![put("c", b"k", b"v")]).unwrap();
        let held = store.live_cf("c").unwrap();
        apply(&store, vec![Op::DropCf { name: "c".into() }]).unwrap();

        assert!(store.live_cf("c").is_err());
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0);
        assert!(store.db().cf_handle("u:c:1").is_some(), "still referenced");
        assert_eq!(store.load_meta().unwrap().unwrap().pending_drops.len(), 1);

        drop(held);
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
        assert!(store.db().cf_handle("u:c:1").is_none());
        assert!(store.load_meta().unwrap().unwrap().pending_drops.is_empty());
    }

    #[test]
    fn recreate_after_drop_uses_new_physical_cf() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        apply(&store, vec![put("c", b"k", b"old")]).unwrap();
        apply(&store, vec![Op::DropCf { name: "c".into() }]).unwrap();
        assert_eq!(apply(&store, vec![create("c")]).unwrap(), 4);
        assert_eq!(store.live_cf("c").unwrap().physical, "u:c:4");
        assert_eq!(raw(&store, "c", b"k"), None);
    }

    #[test]
    fn odd_cf_names_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            apply(&store, vec![create("a:b:1")]).unwrap();
            apply(&store, vec![create("ünï")]).unwrap();
            apply(&store, vec![put("a:b:1", b"k", b"v")]).unwrap();
        }
        let store = open_store(dir.path());
        assert!(raw(&store, "a:b:1", b"k").is_some());
        assert_eq!(store.live_cf("ünï").unwrap().physical, "u:ünï:2");
        apply(
            &store,
            vec![Op::DropCf {
                name: "a:b:1".into(),
            }],
        )
        .unwrap();
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
    }

    #[test]
    fn custom_comparator_orders_ranges_and_is_required_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = RocksTuning::new();
        tuning
            .comparators
            .register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        {
            let store = open_store_with(dir.path(), tuning.clone());
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new("r").with_comparator("reverse"),
                }],
            )
            .unwrap();
            // Under "reverse", b"b" sorts before b"a", so [b, a) is a valid range.
            apply(
                &store,
                vec![Op::DeleteRange {
                    cf: "r".into(),
                    start: b"b".to_vec(),
                    end: b"a".to_vec(),
                }],
            )
            .unwrap();
        }
        let err =
            crate::store::Store::open(dir.path(), crate::types::Role::Master, RocksTuning::new())
                .err();
        assert!(matches!(err, Some(Error::UnknownComparator(n)) if n == "reverse"));
        let store = open_store_with(dir.path(), tuning);
        assert!(store.live_cf("r").is_ok());
    }

    #[test]
    fn interrupted_create_is_cleaned_up_on_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            // Simulate a crash after the intent and the physical CF, before the commit batch.
            let intent = crate::types::CfEntry {
                spec: CfSpec::new("x"),
                create_seq: 1,
            };
            store
                .write_meta(
                    &[(
                        key::CF_CREATE_INTENT,
                        &crate::store::meta::encode_cf_entry(&intent),
                    )],
                    true,
                )
                .unwrap();
            store
                .db()
                .create_cf("u:x:1", &rocksdb::Options::default())
                .unwrap();
        }
        let store = open_store(dir.path());
        assert!(store.db().cf_handle("u:x:1").is_none());
        assert!(
            store
                .load_meta()
                .unwrap()
                .unwrap()
                .cf_create_intent
                .is_none()
        );
        assert_eq!(apply(&store, vec![create("x")]).unwrap(), 1);
    }

    #[test]
    fn interrupted_create_with_custom_comparator_is_cleaned_up_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = RocksTuning::new();
        tuning
            .comparators
            .register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        {
            let store = open_store_with(dir.path(), tuning.clone());
            let spec = CfSpec::new("x").with_comparator("reverse");
            let intent = crate::types::CfEntry {
                spec: spec.clone(),
                create_seq: 1,
            };
            store
                .write_meta(
                    &[(
                        key::CF_CREATE_INTENT,
                        &crate::store::meta::encode_cf_entry(&intent),
                    )],
                    true,
                )
                .unwrap();
            let opts = tuning
                .cf_options(crate::tuning::CfKind::User(&spec))
                .unwrap();
            store.db().create_cf("u:x:1", &opts).unwrap();
            store.retain_cf_options(opts);
        }
        let store = open_store_with(dir.path(), tuning);
        assert!(store.db().cf_handle("u:x:1").is_none());
        assert!(
            store
                .load_meta()
                .unwrap()
                .unwrap()
                .cf_create_intent
                .is_none()
        );
        assert_eq!(
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new("x").with_comparator("reverse")
                }]
            )
            .unwrap(),
            1
        );
    }

    #[test]
    fn db_clone_outlives_store_with_custom_comparator_cf() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = RocksTuning::new();
        tuning
            .comparators
            .register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let store = open_store_with(dir.path(), tuning);
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("r").with_comparator("reverse"),
            }],
        )
        .unwrap();
        apply(&store, vec![put("r", b"a", b"1")]).unwrap();
        apply(&store, vec![put("r", b"b", b"2")]).unwrap();
        let phys = store.live_cf("r").unwrap();
        let db = store.db().clone();
        drop(store);
        let cf = db.cf_handle(&phys.physical).unwrap();
        let keys: Vec<Vec<u8>> = db
            .iterator_cf(&cf, rocksdb::IteratorMode::Start)
            .map(|i| i.unwrap().0.to_vec())
            .collect();
        assert_eq!(keys, vec![b"b".to_vec(), b"a".to_vec()]);
        assert!(db.get_cf(&cf, b"a").unwrap().is_some());
        drop(cf);
        drop(db);
    }

    #[test]
    fn commit_rechecks_sequence() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ops = vec![create("c")];
        let entry = LogEntry::new(1, 1, 0, crate::codec::encode_ops(&ops));
        let a = store.prepare(&entry, &ops).unwrap();
        let b = store.prepare(&entry, &ops).unwrap();
        store.commit(a, false).unwrap();
        assert!(matches!(store.commit(b, false), Err(Error::Corrupt(_))));
        assert_eq!(store.position().last_seq, 1);
    }

    fn entry(seq: u64, ops: &[Op]) -> LogEntry {
        LogEntry::new(seq, 1, 1_000 * seq, crate::codec::encode_ops(ops))
    }

    /// Group commit (DESIGN.md §6): three entries in one batch end on the third's position,
    /// with every changelog row and every value.
    #[test]
    fn a_group_commits_several_entries_in_one_batch() {
        use crate::store::apply::PreparedGroup;
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        let mut group = PreparedGroup::after(store.position());
        let entries: Vec<LogEntry> = (2..=4u64)
            .map(|seq| entry(seq, &[put("c", &[seq as u8], b"v")]))
            .collect();
        for e in &entries {
            let ops = e.decode_ops().unwrap();
            store.prepare_in_group(&mut group, e, &ops).unwrap();
        }
        assert_eq!(group.len(), 3);
        assert_eq!(store.position().last_seq, 1, "nothing written before the commit");
        store.commit_group(group, true).unwrap();

        let last = entries.last().unwrap();
        assert_eq!(store.position().last_seq, 4);
        assert_eq!(store.position().last_entry_crc, Some(last.crc32c));
        let meta = store.load_meta().unwrap().unwrap();
        assert_eq!(meta.last_seq, 4);
        assert_eq!(meta.last_entry_crc, Some(last.crc32c));
        for e in &entries {
            let row = store
                .db()
                .get_cf(&store.cf(CF_CHANGELOG), e.seq.to_be_bytes())
                .unwrap()
                .unwrap();
            assert_eq!(LogEntry::decode(&row).unwrap(), *e);
            let env = raw(&store, "c", &[e.seq as u8]).unwrap();
            let decoded = envelope::decode(&env).unwrap();
            assert_eq!(decoded.meta.write_seq, e.seq);
            assert_eq!(decoded.value, b"v");
        }
    }

    /// CF entries commit alone; an entry that fails to prepare leaves the group as it was, so
    /// the entries before it still commit.
    #[test]
    fn a_group_refuses_cf_entries_and_keeps_earlier_entries_on_error() {
        use crate::store::apply::PreparedGroup;
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        let mut group = PreparedGroup::after(store.position());
        let e2 = entry(2, &[put("c", b"a", b"1")]);
        store
            .prepare_in_group(&mut group, &e2, &e2.decode_ops().unwrap())
            .unwrap();
        let cf = entry(3, &[create("d")]);
        assert!(matches!(
            store.prepare_in_group(&mut group, &cf, &cf.decode_ops().unwrap()),
            Err(Error::Corrupt(_))
        ));
        let bad = entry(3, &[put("c", b"b", b"2"), put("missing", b"x", b"y")]);
        assert!(matches!(
            store.prepare_in_group(&mut group, &bad, &bad.decode_ops().unwrap()),
            Err(Error::NoSuchCf(_))
        ));
        assert_eq!(group.len(), 1);
        assert_eq!(group.position().last_seq, 2);
        store.commit_group(group, false).unwrap();
        assert_eq!(store.position().last_seq, 2);
        assert!(raw(&store, "c", b"a").is_some());
        assert!(raw(&store, "c", b"b").is_none(), "nothing of the failed entry");
        assert!(
            store
                .db()
                .get_cf(&store.cf(CF_CHANGELOG), 3u64.to_be_bytes())
                .unwrap()
                .is_none()
        );
    }

    #[test]
    fn prepare_after_needs_the_next_seq() {
        use crate::store::Position;
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        let prev = Position {
            last_seq: 7,
            last_entry_crc: None,
        };
        let e = entry(9, &[put("c", b"k", b"v")]);
        assert!(matches!(
            store.prepare_after(prev, &e, &e.decode_ops().unwrap()),
            Err(Error::Corrupt(_))
        ));
        let e = entry(8, &[put("c", b"k", b"v")]);
        let p = store
            .prepare_after(prev, &e, &e.decode_ops().unwrap())
            .unwrap();
        assert_eq!(p.position().last_seq, 8);
        assert_eq!(p.position().last_entry_crc, Some(e.crc32c));
    }

    /// A group prepared on a position that moved since is refused whole.
    #[test]
    fn commit_group_rechecks_the_position() {
        use crate::store::apply::PreparedGroup;
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        let mut group = PreparedGroup::after(store.position());
        let e = entry(2, &[put("c", b"a", b"1")]);
        store
            .prepare_in_group(&mut group, &e, &e.decode_ops().unwrap())
            .unwrap();
        apply(&store, vec![put("c", b"z", b"1")]).unwrap();
        assert!(matches!(
            store.commit_group(group, false),
            Err(Error::Corrupt(_))
        ));
        assert_eq!(store.position().last_seq, 2);
        assert!(raw(&store, "c", b"a").is_none());
    }

    /// A failed create left `u:a:1` and its intent; the next create at the same seq is for
    /// another name. The orphan is dropped before its intent is overwritten, or the next open
    /// would find a physical CF that `__meta` does not know.
    #[test]
    fn an_orphan_of_another_name_is_dropped_before_its_intent_is_replaced() {
        use crate::store::{Store, meta};
        use crate::types::{CfEntry, Role};
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            let orphan = CfEntry {
                spec: CfSpec::new("a"),
                create_seq: 1,
            };
            store
                .db()
                .create_cf(orphan.physical_name(), &rocksdb::Options::default())
                .unwrap();
            store
                .write_meta(
                    &[(key::CF_CREATE_INTENT, &meta::encode_cf_entry(&orphan))],
                    true,
                )
                .unwrap();
            assert_eq!(apply(&store, vec![create("b")]).unwrap(), 1);
            assert!(
                store.db().cf_handle("u:a:1").is_none(),
                "the orphan is gone"
            );
        }
        let (_store, m) = Store::open(dir.path(), Role::Master, RocksTuning::new())
            .expect("reopens: no physical CF unknown to __meta");
        let names: Vec<&str> = m.cf_specs.iter().map(|e| e.spec.name.as_str()).collect();
        assert_eq!(names, vec!["b"]);
    }

    /// M3: a physical CF left by a commit that failed after `create_cf` is reused only by the
    /// same create (a retry of that entry), never by an entry with another spec.
    #[test]
    fn a_leftover_physical_cf_is_reused_only_by_the_same_create() {
        use crate::store::meta;
        use crate::types::CfEntry;
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        // Seq 1 creates "c"; a failed commit of another spec left `u:c:1` and its intent.
        let other = CfEntry {
            spec: CfSpec {
                name: "c".into(),
                comparator: Some("reverse".into()),
            },
            create_seq: 1,
        };
        store
            .db()
            .create_cf(other.physical_name(), &rocksdb::Options::default())
            .unwrap();
        store
            .write_meta(
                &[(key::CF_CREATE_INTENT, &meta::encode_cf_entry(&other))],
                true,
            )
            .unwrap();
        assert!(matches!(
            apply(&store, vec![create("c")]),
            Err(Error::Corrupt(_))
        ));
        assert_eq!(store.position().last_seq, 0, "nothing committed");
        // The same spec (a retry of the failed entry) reuses it.
        let same = CfEntry {
            spec: CfSpec::new("c"),
            create_seq: 1,
        };
        store
            .write_meta(
                &[(key::CF_CREATE_INTENT, &meta::encode_cf_entry(&same))],
                true,
            )
            .unwrap();
        assert_eq!(apply(&store, vec![create("c")]).unwrap(), 1);
        assert_eq!(apply(&store, vec![put("c", b"k", b"v")]).unwrap(), 2);
        assert!(raw(&store, "c", b"k").is_some());
    }
}
