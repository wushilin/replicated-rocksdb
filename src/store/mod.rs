//! Crate-private RocksDB wrapper shared by master and slave.

pub(crate) mod cfs;
pub(crate) mod meta;


use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, WriteOptions, DB};

use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::error::{Error, Result};
use crate::tuning::{CfKind, RocksTuning};
use crate::types::{CfEntry, Role, Seq};
use cfs::{CfRegistry, PhysCf};
use meta::{key, MetaSnapshot, CF_CHANGELOG, CF_META, CF_TTL_INDEX};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Position {
    pub last_seq: Seq,
    pub last_entry_crc: Option<u32>,
}

pub(crate) struct Store {
    db: Arc<DB>,
    role: Role,
    tuning: RocksTuning,
    pub(crate) cfs: CfRegistry,
    pos: RwLock<Position>,
    first_retained: AtomicU64,
}

impl Store {
    /// Opens or creates a store. Two-phase: `__meta` is read through a read-only open first,
    /// so every user CF can be reopened with its registered comparator.
    pub(crate) fn open(path: &Path, role: Role, tuning: RocksTuning) -> Result<(Store, MetaSnapshot)> {
        let db_opts = tuning.db_options();
        let existing: Vec<String> =
            if path.join("CURRENT").exists() { DB::list_cf(&Options::default(), path)? } else { Vec::new() };
        let prior = if existing.iter().any(|n| n == CF_META) { read_meta_read_only(path)? } else { None };
        if let Some(m) = &prior {
            check_compatible(m, role)?;
        }

        let mut descriptors = vec![
            ColumnFamilyDescriptor::new(CF_META, tuning.cf_options(CfKind::Meta)?),
            ColumnFamilyDescriptor::new(CF_CHANGELOG, tuning.cf_options(CfKind::Changelog)?),
            ColumnFamilyDescriptor::new(CF_TTL_INDEX, tuning.cf_options(CfKind::TtlIndex)?),
        ];
        let known: HashMap<String, CfEntry> = prior
            .iter()
            .flat_map(|m| m.cf_specs.iter().chain(&m.pending_drops).chain(&m.cf_create_intent))
            .map(|e| (e.physical_name(), e.clone()))
            .collect();
        for name in existing.iter().filter(|n| n.starts_with("u:")) {
            let entry = known
                .get(name)
                .ok_or_else(|| Error::corrupt(format!("physical column family {name} is not in __meta")))?;
            descriptors.push(ColumnFamilyDescriptor::new(name, tuning.cf_options(CfKind::User(&entry.spec))?));
        }
        let db = Arc::new(DB::open_cf_descriptors(&db_opts, path, descriptors)?);

        let mut store = Store {
            db,
            role,
            tuning,
            cfs: CfRegistry::default(),
            pos: RwLock::new(Position::default()),
            first_retained: AtomicU64::new(1),
        };
        let meta = match prior {
            Some(m) => m,
            None => store.initialise_meta()?,
        };
        let meta = store.recover_cf_state(meta, &existing)?;
        store.cfs = CfRegistry::load(&meta.cf_specs, &meta.pending_drops);
        store.set_position(Position { last_seq: meta.last_seq, last_entry_crc: meta.last_entry_crc });
        let first = store.first_changelog_seq()?.unwrap_or(meta.last_seq + 1);
        store.set_first_retained_seq(first);
        Ok((store, meta))
    }

    fn initialise_meta(&self) -> Result<MetaSnapshot> {
        self.write_meta(
            &[
                (key::ROLE, self.role.as_bytes()),
                (key::PROTOCOL_VERSION, &u64::from(PROTOCOL_VERSION).to_be_bytes()),
                (key::ENVELOPE_VERSION, &u64::from(ENVELOPE_VERSION).to_be_bytes()),
                (key::LAST_SEQ, &0u64.to_be_bytes()),
                (key::CF_SPECS, &meta::encode_cf_list(&[])),
                (key::PENDING_CF_DROPS, &meta::encode_cf_list(&[])),
            ],
            true,
        )?;
        self.load_meta()?.ok_or_else(|| Error::corrupt("__meta missing right after initialisation"))
    }

    /// Finishes CF operations interrupted by a crash (DESIGN.md §7). Extended in Task 6.
    fn recover_cf_state(&self, meta: MetaSnapshot, _existing: &[String]) -> Result<MetaSnapshot> {
        Ok(meta)
    }

    pub(crate) fn load_meta(&self) -> Result<Option<MetaSnapshot>> {
        let cf = self.cf(CF_META);
        MetaSnapshot::load(|k| Ok(self.db.get_cf(&cf, k)?))
    }

    pub(crate) fn role(&self) -> Role {
        self.role
    }

    pub(crate) fn tuning(&self) -> &RocksTuning {
        &self.tuning
    }

    pub(crate) fn db(&self) -> &Arc<DB> {
        &self.db
    }

    pub(crate) fn position(&self) -> Position {
        *self.pos.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn set_position(&self, p: Position) {
        *self.pos.write().unwrap_or_else(PoisonError::into_inner) = p;
    }

    pub(crate) fn first_retained_seq(&self) -> Seq {
        self.first_retained.load(Ordering::SeqCst)
    }

    pub(crate) fn set_first_retained_seq(&self, seq: Seq) {
        self.first_retained.store(seq, Ordering::SeqCst);
    }

    /// Handle of a reserved CF. These are always open, so a miss is a programming error.
    pub(crate) fn cf(&self, name: &'static str) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(name).unwrap_or_else(|| panic!("reserved column family {name} is not open"))
    }

    pub(crate) fn handle(&self, cf: &PhysCf) -> Result<Arc<BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(&cf.physical)
            .ok_or_else(|| Error::corrupt(format!("physical column family {} is missing", cf.physical)))
    }

    pub(crate) fn live_cf(&self, name: &str) -> Result<Arc<PhysCf>> {
        self.cfs.get(name).ok_or_else(|| Error::NoSuchCf(name.to_string()))
    }

    pub(crate) fn write_meta(&self, kvs: &[(&[u8], &[u8])], sync: bool) -> Result<()> {
        let cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        for (k, v) in kvs {
            batch.put_cf(&cf, k, v);
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        Ok(self.db.write_opt(batch, &wo)?)
    }

    fn first_changelog_seq(&self) -> Result<Option<Seq>> {
        let cf = self.cf(CF_CHANGELOG);
        match self.db.iterator_cf(&cf, IteratorMode::Start).next() {
            Some(item) => Ok(Some(meta::be64(&item?.0)?)),
            None => Ok(None),
        }
    }
}

fn read_meta_read_only(path: &Path) -> Result<Option<MetaSnapshot>> {
    let db = DB::open_cf_descriptors_read_only(
        &Options::default(),
        path,
        [ColumnFamilyDescriptor::new(CF_META, Options::default())],
        false,
    )?;
    let cf = db.cf_handle(CF_META).ok_or_else(|| Error::corrupt("__meta missing in read-only open"))?;
    MetaSnapshot::load(|k| Ok(db.get_cf(&cf, k)?))
}

fn check_compatible(m: &MetaSnapshot, role: Role) -> Result<()> {
    if m.role != role {
        return Err(Error::WrongRole { expected: role, found: m.role });
    }
    if m.protocol_version != u64::from(PROTOCOL_VERSION) {
        return Err(Error::VersionMismatch {
            what: "protocol_version",
            found: m.protocol_version,
            expected: PROTOCOL_VERSION.into(),
        });
    }
    if m.envelope_version != u64::from(ENVELOPE_VERSION) {
        return Err(Error::VersionMismatch {
            what: "envelope_version",
            found: m.envelope_version,
            expected: ENVELOPE_VERSION.into(),
        });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuning::RocksTuning;
    use crate::types::{CfEntry, CfSpec, DbIdentity, EpochStart, Role};
    use crate::error::Error;

    #[test]
    fn fresh_store_initialises_meta() {
        let dir = tempfile::tempdir().unwrap();
        let (store, meta) = Store::open(dir.path(), Role::Master, RocksTuning::new()).unwrap();
        assert_eq!(meta.role, Role::Master);
        assert_eq!(meta.last_seq, 0);
        assert_eq!(meta.identity, None);
        assert!(meta.epoch_history.is_empty());
        assert!(meta.cf_specs.is_empty());
        assert_eq!(store.position(), Position { last_seq: 0, last_entry_crc: None });
        assert_eq!(store.first_retained_seq(), 1);
    }

    #[test]
    fn reopen_keeps_meta_and_rejects_wrong_role() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = Store::open(dir.path(), Role::Master, RocksTuning::new()).unwrap();
            store.write_meta(&[(meta::key::LAST_SEQ, &5u64.to_be_bytes())], true).unwrap();
        }
        let (store, meta) = Store::open(dir.path(), Role::Master, RocksTuning::new()).unwrap();
        assert_eq!(meta.last_seq, 5);
        assert_eq!(store.position().last_seq, 5);
        assert_eq!(store.first_retained_seq(), 6);
        drop(store);
        let err = Store::open(dir.path(), Role::Slave, RocksTuning::new()).err().unwrap();
        assert!(matches!(err, Error::WrongRole { expected: Role::Slave, found: Role::Master }));
    }

    #[test]
    fn version_mismatch_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = Store::open(dir.path(), Role::Master, RocksTuning::new()).unwrap();
            store.write_meta(&[(meta::key::PROTOCOL_VERSION, &99u64.to_be_bytes())], true).unwrap();
        }
        let err = Store::open(dir.path(), Role::Master, RocksTuning::new()).err().unwrap();
        assert!(matches!(err, Error::VersionMismatch { what: "protocol_version", found: 99, expected: 1 }));
    }

    #[test]
    fn meta_encodings_round_trip() {
        let id = DbIdentity { db_uuid: uuid::Uuid::new_v4(), epoch: 3 };
        assert_eq!(meta::decode_identity(&meta::encode_identity(&id)).unwrap(), id);
        let list = vec![
            CfEntry { spec: CfSpec::new("a"), create_seq: 1 },
            CfEntry { spec: CfSpec::new("b").with_comparator("rev"), create_seq: 9 },
        ];
        assert_eq!(meta::decode_cf_list(&meta::encode_cf_list(&list)).unwrap(), list);
        assert_eq!(meta::decode_cf_entry(&meta::encode_cf_entry(&list[1])).unwrap(), list[1]);
        let h = vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 50 }];
        assert_eq!(meta::decode_epoch_history(&meta::encode_epoch_history(&h)).unwrap(), h);
        assert!(meta::be64(b"short").is_err());
    }
}
