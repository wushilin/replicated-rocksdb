//! `__meta` keys and value encodings (DESIGN.md §3.2).

use prost::Message;

use crate::error::{Error, Result};
use crate::pb;
use crate::proto::local::v1 as local;
use crate::types::{CfEntry, CfSpec, DbIdentity, EpochStart, Role, Seq};

pub(crate) const CF_META: &str = "__meta";
pub(crate) const CF_CHANGELOG: &str = "__changelog";
pub(crate) const CF_TTL_INDEX: &str = "__ttl_index";

pub(crate) mod key {
    pub const ROLE: &[u8] = b"role";
    pub const PROTOCOL_VERSION: &[u8] = b"protocol_version";
    pub const ENVELOPE_VERSION: &[u8] = b"envelope_version";
    pub const ORIGIN_IDENTITY: &[u8] = b"origin_identity";
    pub const LAST_SEQ: &[u8] = b"last_seq";
    pub const LAST_ENTRY_CRC: &[u8] = b"last_entry_crc";
    pub const LAST_TS: &[u8] = b"last_ts";
    pub const EPOCH_HISTORY: &[u8] = b"epoch_history";
    pub const CF_SPECS: &[u8] = b"cf_specs";
    pub const PENDING_CF_DROPS: &[u8] = b"pending_cf_drops";
    pub const CF_CREATE_INTENT: &[u8] = b"cf_create_intent";
    pub const NODE_STATE: &[u8] = b"node_state";
    pub const MAX_ENTRY_BYTES: &[u8] = b"max_entry_bytes";
    pub const BOOTSTRAP_CFS: &[u8] = b"bootstrap_cfs";
    /// The highest epoch number this node ever started and then gave up (`Master::demote`, or
    /// the empty trailing epochs `Slave::rejoin` drops). A later promotion of this node never
    /// reuses it.
    pub const MAX_BURNED_EPOCH: &[u8] = b"max_burned_epoch";
    /// Master that was promoted: how many epoch records its history held before the promotion
    /// (so `demote` can remove every record added since, including per-open epochs).
    pub const PROMOTED_FROM_LEN: &[u8] = b"promoted_from_len";
    /// The first seq of the local divergent suffix, recorded when a slave halts `Diverged`
    /// (`Slave::diverged_entries`). Cleared by a repair or a bootstrap.
    pub const DIVERGED_AT: &[u8] = b"diverged_at";
    /// While a repair replays (DESIGN.md §12.1): reads answer `NotReady` until `last_seq`
    /// reaches this seq. Kept apart from `node_state`, which a halt rewrites.
    pub const REPAIR_UNTIL: &[u8] = b"repair_until";
}

pub(crate) fn be64(b: &[u8]) -> Result<u64> {
    let a: [u8; 8] = b
        .try_into()
        .map_err(|_| Error::corrupt("expected an 8-byte integer in __meta"))?;
    Ok(u64::from_be_bytes(a))
}

pub(crate) fn be32(b: &[u8]) -> Result<u32> {
    let a: [u8; 4] = b
        .try_into()
        .map_err(|_| Error::corrupt("expected a 4-byte integer in __meta"))?;
    Ok(u32::from_be_bytes(a))
}

pub(crate) fn encode_identity(id: &DbIdentity) -> Vec<u8> {
    pb::Identity {
        db_uuid: id.db_uuid.as_bytes().to_vec(),
        epoch: id.epoch,
    }
    .encode_to_vec()
}

pub(crate) fn decode_identity(b: &[u8]) -> Result<DbIdentity> {
    let p = pb::Identity::decode(b)
        .map_err(|e| Error::corrupt(format!("bad identity in __meta: {e}")))?;
    let db_uuid = uuid::Uuid::from_slice(&p.db_uuid)
        .map_err(|e| Error::corrupt(format!("bad db_uuid: {e}")))?;
    Ok(DbIdentity {
        db_uuid,
        epoch: p.epoch,
    })
}

pub(crate) fn encode_epoch_history(h: &[EpochStart]) -> Vec<u8> {
    let epochs = h
        .iter()
        .map(|e| pb::EpochStart {
            epoch: e.epoch,
            start_seq: e.start_seq,
            nonce: e.nonce,
        })
        .collect();
    local::EpochHistory { epochs }.encode_to_vec()
}

pub(crate) fn decode_epoch_history(b: &[u8]) -> Result<Vec<EpochStart>> {
    let p = local::EpochHistory::decode(b)
        .map_err(|e| Error::corrupt(format!("bad epoch history in __meta: {e}")))?;
    Ok(p.epochs
        .into_iter()
        .map(|e| EpochStart {
            epoch: e.epoch,
            start_seq: e.start_seq,
            nonce: e.nonce,
        })
        .collect())
}

fn entry_to_pb(e: &CfEntry) -> pb::CfSpec {
    pb::CfSpec {
        name: e.spec.name.clone(),
        comparator: e.spec.comparator.clone(),
        create_seq: e.create_seq,
    }
}

fn entry_from_pb(p: pb::CfSpec) -> CfEntry {
    CfEntry {
        spec: CfSpec {
            name: p.name,
            comparator: p.comparator,
        },
        create_seq: p.create_seq,
    }
}

pub(crate) fn encode_cf_list(list: &[CfEntry]) -> Vec<u8> {
    local::CfEntryList {
        cfs: list.iter().map(entry_to_pb).collect(),
    }
    .encode_to_vec()
}

pub(crate) fn decode_cf_list(b: &[u8]) -> Result<Vec<CfEntry>> {
    let p = local::CfEntryList::decode(b)
        .map_err(|e| Error::corrupt(format!("bad CF list in __meta: {e}")))?;
    Ok(p.cfs.into_iter().map(entry_from_pb).collect())
}

pub(crate) fn encode_cf_entry(e: &CfEntry) -> Vec<u8> {
    entry_to_pb(e).encode_to_vec()
}

pub(crate) fn decode_cf_entry(b: &[u8]) -> Result<CfEntry> {
    let p = pb::CfSpec::decode(b)
        .map_err(|e| Error::corrupt(format!("bad CF entry in __meta: {e}")))?;
    Ok(entry_from_pb(p))
}

/// Everything in `__meta`, read in one go (live, read-only, or through a snapshot).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetaSnapshot {
    pub role: Role,
    pub protocol_version: u64,
    pub envelope_version: u64,
    pub identity: Option<DbIdentity>,
    /// Empty until an identity is assigned (fresh master, or slave after bootstrap).
    pub epoch_history: Vec<EpochStart>,
    pub last_seq: Seq,
    pub last_entry_crc: Option<u32>,
    pub last_ts: u64,
    pub cf_specs: Vec<CfEntry>,
    pub pending_drops: Vec<CfEntry>,
    pub cf_create_intent: Option<CfEntry>,
    /// Slave only: `ready`, `bootstrapping` or `halted:<Reason>`. `None` means ready.
    pub node_state: Option<Vec<u8>>,
    /// Slave only: the master's `max_entry_bytes` as learnt from upstream.
    pub max_entry_bytes: Option<u64>,
    /// Slave only: physical CFs created by a bootstrap that has not finished.
    pub bootstrap_cfs: Vec<CfEntry>,
    /// The highest epoch number this node started and gave up by demoting; 0 if none.
    pub max_burned_epoch: u64,
    /// Master only: the length of the epoch history before the promotion that made this node a
    /// master; `None` if it was never promoted (or demoted since).
    pub promoted_from_len: Option<usize>,
}

impl MetaSnapshot {
    /// `Ok(None)` if the store has never been initialised (no `role` key).
    pub(crate) fn load(
        get: impl Fn(&[u8]) -> Result<Option<Vec<u8>>>,
    ) -> Result<Option<MetaSnapshot>> {
        let Some(role) = get(key::ROLE)? else {
            return Ok(None);
        };
        let role =
            Role::from_bytes(&role).ok_or_else(|| Error::corrupt("unknown role in __meta"))?;
        let u64_or = |k: &[u8], default: u64| -> Result<u64> {
            get(k)?
                .map(|v| be64(&v))
                .transpose()
                .map(|v| v.unwrap_or(default))
        };
        Ok(Some(MetaSnapshot {
            role,
            protocol_version: u64_or(key::PROTOCOL_VERSION, 0)?,
            envelope_version: u64_or(key::ENVELOPE_VERSION, 0)?,
            identity: get(key::ORIGIN_IDENTITY)?
                .map(|v| decode_identity(&v))
                .transpose()?,
            epoch_history: get(key::EPOCH_HISTORY)?
                .map(|v| decode_epoch_history(&v))
                .transpose()?
                .unwrap_or_default(),
            last_seq: u64_or(key::LAST_SEQ, 0)?,
            last_entry_crc: get(key::LAST_ENTRY_CRC)?.map(|v| be32(&v)).transpose()?,
            last_ts: u64_or(key::LAST_TS, 0)?,
            cf_specs: get(key::CF_SPECS)?
                .map(|v| decode_cf_list(&v))
                .transpose()?
                .unwrap_or_default(),
            pending_drops: get(key::PENDING_CF_DROPS)?
                .map(|v| decode_cf_list(&v))
                .transpose()?
                .unwrap_or_default(),
            cf_create_intent: get(key::CF_CREATE_INTENT)?
                .map(|v| decode_cf_entry(&v))
                .transpose()?,
            node_state: get(key::NODE_STATE)?,
            max_entry_bytes: get(key::MAX_ENTRY_BYTES)?.map(|v| be64(&v)).transpose()?,
            bootstrap_cfs: get(key::BOOTSTRAP_CFS)?
                .map(|v| decode_cf_list(&v))
                .transpose()?
                .unwrap_or_default(),
            max_burned_epoch: u64_or(key::MAX_BURNED_EPOCH, 0)?,
            promoted_from_len: get(key::PROMOTED_FROM_LEN)?
                .map(|v| be64(&v).map(|n| n as usize))
                .transpose()?,
        }))
    }
}
