//! `__ttl_index` (master only): `BE64(expire_at) ‖ BE64(create_seq) ‖ BE16(len(name)) ‖ name ‖ key`.
//! Including `create_seq` keeps a dropped-and-recreated CF from matching old entries.

use rocksdb::{IteratorMode, WriteBatch};

use super::Store;
use super::meta::CF_TTL_INDEX;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::types::{CfEntry, Op, Seq};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub raw_key: Vec<u8>,
    pub expire_at: u64,
    pub cf_name: String,
    pub create_seq: Seq,
    pub key: Vec<u8>,
}

pub(crate) fn index_key(expire_at: u64, cf: &CfEntry, key: &[u8]) -> Vec<u8> {
    let name = cf.spec.name.as_bytes();
    let mut k = Vec::with_capacity(18 + name.len() + key.len());
    k.extend_from_slice(&expire_at.to_be_bytes());
    k.extend_from_slice(&cf.create_seq.to_be_bytes());
    k.extend_from_slice(&(name.len() as u16).to_be_bytes());
    k.extend_from_slice(name);
    k.extend_from_slice(key);
    k
}

pub(crate) fn parse_index_key(raw: &[u8]) -> Result<IndexEntry> {
    let bad = || Error::corrupt("malformed __ttl_index key");
    if raw.len() < 18 {
        return Err(bad());
    }
    let expire_at = u64::from_be_bytes(raw[0..8].try_into().map_err(|_| bad())?);
    let create_seq = u64::from_be_bytes(raw[8..16].try_into().map_err(|_| bad())?);
    let name_len = u16::from_be_bytes(raw[16..18].try_into().map_err(|_| bad())?) as usize;
    let name_end = 18 + name_len;
    if raw.len() < name_end {
        return Err(bad());
    }
    let cf_name = String::from_utf8(raw[18..name_end].to_vec()).map_err(|_| bad())?;
    Ok(IndexEntry {
        raw_key: raw.to_vec(),
        expire_at,
        cf_name,
        create_seq,
        key: raw[name_end..].to_vec(),
    })
}

impl Store {
    pub(crate) fn add_ttl_index(&self, batch: &mut WriteBatch, ops: &[Op]) -> Result<()> {
        let cf = self.cf(CF_TTL_INDEX);
        for op in ops {
            if let Op::Put {
                cf: name,
                key,
                expire_at_ms: Some(at),
                ..
            } = op
            {
                let phys = self.live_cf(name)?;
                batch.put_cf(&cf, index_key(*at, &phys.entry, key), []);
            }
        }
        Ok(())
    }

    /// Up to `limit` index entries with `expire_at <= now_ms`, oldest first. With `after`, the
    /// scan starts strictly after that raw index key, so a caller that has already processed
    /// earlier entries does not rescan them (or their tombstones).
    pub(crate) fn ttl_due(
        &self,
        now_ms: u64,
        limit: usize,
        after: Option<&[u8]>,
    ) -> Result<Vec<IndexEntry>> {
        let cf = self.cf(CF_TTL_INDEX);
        let mode = match after {
            Some(k) => IteratorMode::From(k, rocksdb::Direction::Forward),
            None => IteratorMode::Start,
        };
        let mut due = Vec::new();
        for item in self.db().iterator_cf(&cf, mode) {
            if due.len() >= limit {
                break;
            }
            let (raw, _) = item?;
            if after == Some(&*raw) {
                continue;
            }
            let entry = parse_index_key(&raw)?;
            if entry.expire_at > now_ms {
                break;
            }
            due.push(entry);
        }
        Ok(due)
    }

    /// True if the key still holds the value this index entry was written for.
    pub(crate) fn ttl_entry_is_live(&self, e: &IndexEntry) -> Result<bool> {
        let Some(phys) = self.cfs.get(&e.cf_name) else {
            return Ok(false);
        };
        if phys.entry.create_seq != e.create_seq {
            return Ok(false);
        }
        let Some(raw) = self.live_ctx(0).get_raw(&phys, &e.key)? else {
            return Ok(false);
        };
        Ok(envelope::decode(&raw)?.meta.expire_at_ms == Some(e.expire_at))
    }

    pub(crate) fn remove_ttl_index(&self, batch: &mut WriteBatch, entries: &[IndexEntry]) {
        let cf = self.cf(CF_TTL_INDEX);
        for e in entries {
            batch.delete_cf(&cf, &e.raw_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CfSpec;

    #[test]
    fn index_key_round_trips_and_sorts_by_expiry() {
        let cf = CfEntry {
            spec: CfSpec::new("a:b"),
            create_seq: 7,
        };
        let k1 = index_key(5, &cf, b"");
        let k2 = index_key(6, &cf, b"\x00");
        assert!(k1 < k2);
        let e = parse_index_key(&k2).unwrap();
        assert_eq!(
            (
                e.expire_at,
                e.create_seq,
                e.cf_name.as_str(),
                e.key.as_slice()
            ),
            (6, 7, "a:b", &b"\x00"[..])
        );
        assert!(parse_index_key(&k2[..17]).is_err());
    }
}
