//! Reading a closed store without opening it for writing (DESIGN.md §13.2): the stored role
//! of a directory, and a fenced master's own position.

use std::path::Path;

use rocksdb::{ColumnFamilyDescriptor, DB, Options};

use super::meta::{CF_CHANGELOG, CF_META, MetaSnapshot};
use crate::admin::NodeInfo;
use crate::error::{Error, Result};
use crate::types::{LogEntry, Seq};

/// The stored `__meta` of the database at `path`, read through a read-only open (no LOCK, no
/// data write; RocksDB may still touch its info LOG). `None` when there is no database or it never stored a role (missing directory,
/// empty directory, or a crash during first creation).
pub(crate) fn stored_meta(path: &Path) -> Result<Option<MetaSnapshot>> {
    if !path.join("CURRENT").exists() {
        return Ok(None);
    }
    if !DB::list_cf(&Options::default(), path)?
        .iter()
        .any(|n| n == CF_META)
    {
        return Ok(None);
    }
    super::read_meta_read_only(path)
}

/// A read-only view of a closed store: its `__meta` and `__changelog`. Opening it takes no
/// LOCK and writes nothing, so a fenced master's directory stays exactly as it was. Drop it
/// before opening the directory for writing.
pub(crate) struct OfflineLog {
    db: DB,
    meta: MetaSnapshot,
}

impl OfflineLog {
    /// `None` when `path` holds no stored role (see [`stored_meta`]).
    pub(crate) fn open(path: &Path) -> Result<Option<OfflineLog>> {
        let Some(meta) = stored_meta(path)? else {
            return Ok(None);
        };
        let db = DB::open_cf_descriptors_read_only(
            &Options::default(),
            path,
            [
                ColumnFamilyDescriptor::new(CF_META, Options::default()),
                ColumnFamilyDescriptor::new(CF_CHANGELOG, Options::default()),
            ],
            false,
        )?;
        Ok(Some(OfflineLog { db, meta }))
    }

    /// What this node would report in `Status` (no replicas, no probes, no upstream), with
    /// `node_state = "fenced"`.
    pub(crate) fn node_info(&self, node_id: &str) -> NodeInfo {
        let m = &self.meta;
        let mut info = NodeInfo::new(node_id, m.role);
        info.identity = m.identity;
        info.epoch_history = m.epoch_history.clone();
        info.last_seq = m.last_seq;
        info.last_entry_crc = m.last_entry_crc;
        info.node_state = "fenced".into();
        info.max_burned_epoch = m.max_burned_epoch;
        info
    }

    /// The CRC of the entry at `seq` (as `Store::entry_crc`): from the changelog, or
    /// `last_entry_crc` at `last_seq`; `None` when not held.
    pub(crate) fn entry_crc(&self, seq: Seq) -> Result<Option<u32>> {
        if seq == 0 || seq > self.meta.last_seq {
            return Ok(None);
        }
        let cf = self
            .db
            .cf_handle(CF_CHANGELOG)
            .ok_or_else(|| Error::corrupt("__changelog missing in read-only open"))?;
        if let Some(v) = self.db.get_cf(&cf, seq.to_be_bytes())? {
            return Ok(Some(LogEntry::decode(&v)?.crc32c));
        }
        Ok((seq == self.meta.last_seq)
            .then_some(self.meta.last_entry_crc)
            .flatten())
    }
}
