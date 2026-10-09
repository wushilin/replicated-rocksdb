//! `LogEntry` and `Op` encoding (PROTOCOL.md §5.1–5.2).

use bytes::Bytes;
use prost::Message;

use super::MAX_CF_NAME_BYTES;
use crate::error::{Error, Result};
use crate::pb;
use crate::types::{CfSpec, LogEntry, Op, Seq};

/// CRC-32C over `BE64(seq) ‖ BE64(epoch) ‖ BE64(ts_ms) ‖ ops`.
pub fn entry_crc(seq: Seq, epoch: u64, ts_ms: u64, ops: &[u8]) -> u32 {
    let crc = crc32c::crc32c(&seq.to_be_bytes());
    let crc = crc32c::crc32c_append(crc, &epoch.to_be_bytes());
    let crc = crc32c::crc32c_append(crc, &ts_ms.to_be_bytes());
    crc32c::crc32c_append(crc, ops)
}

impl LogEntry {
    /// Builds an entry and computes its CRC. Only the master creates entries.
    pub fn new(seq: Seq, epoch: u64, ts_ms: u64, ops: Bytes) -> Self {
        let crc32c = entry_crc(seq, epoch, ts_ms, &ops);
        LogEntry { seq, epoch, ts_ms, ops, crc32c }
    }

    pub fn verify_crc(&self) -> Result<()> {
        if entry_crc(self.seq, self.epoch, self.ts_ms, &self.ops) == self.crc32c {
            Ok(())
        } else {
            Err(Error::corrupt(format!("log entry {} failed its CRC check", self.seq)))
        }
    }

    /// Decodes and structurally validates `ops`. Call `verify_crc` first.
    pub fn decode_ops(&self) -> Result<Vec<Op>> {
        decode_ops(&self.ops)
    }

    fn to_pb(&self) -> pb::LogEntry {
        pb::LogEntry {
            seq: self.seq,
            epoch: self.epoch,
            ts_ms: self.ts_ms,
            ops: self.ops.clone(),
            crc32c: self.crc32c,
        }
    }

    pub(crate) fn encode_to_vec(&self) -> Vec<u8> {
        self.to_pb().encode_to_vec()
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.to_pb().encoded_len()
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<LogEntry> {
        let p = pb::LogEntry::decode(bytes)
            .map_err(|e| Error::corrupt(format!("undecodable log entry: {e}")))?;
        Ok(LogEntry { seq: p.seq, epoch: p.epoch, ts_ms: p.ts_ms, ops: p.ops, crc32c: p.crc32c })
    }
}

pub(crate) fn encode_ops(ops: &[Op]) -> Bytes {
    let list = pb::OpList { ops: ops.iter().map(op_to_pb).collect() };
    Bytes::from(list.encode_to_vec())
}

/// Decodes an `OpList`. Any violation of PROTOCOL.md §5.2 rules 1–3 is `Corrupt`.
pub(crate) fn decode_ops(bytes: &[u8]) -> Result<Vec<Op>> {
    let list = pb::OpList::decode(bytes)
        .map_err(|e| Error::corrupt(format!("undecodable operation list: {e}")))?;
    let ops = list.ops.into_iter().map(op_from_pb).collect::<Result<Vec<_>>>()?;
    check_structure(&ops).map_err(|e| match e {
        Error::Corrupt(m) => Error::Corrupt(m),
        other => Error::Corrupt(other.to_string()),
    })?;
    Ok(ops)
}

/// PROTOCOL.md §5.2 rules 1–3: non-empty, CF ops alone, valid CF names.
pub(crate) fn check_structure(ops: &[Op]) -> Result<()> {
    if ops.is_empty() {
        return Err(Error::EmptyBatch);
    }
    let cf_ops = ops.iter().filter(|o| matches!(o, Op::CreateCf { .. } | Op::DropCf { .. })).count();
    if cf_ops > 0 && ops.len() > 1 {
        return Err(Error::corrupt("a column family operation must be the only operation in its entry"));
    }
    ops.iter().try_for_each(|op| validate_cf_name(op.cf_name()))
}

pub fn validate_cf_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_CF_NAME_BYTES || name.starts_with("__") {
        Err(Error::InvalidCfName(name.to_string()))
    } else {
        Ok(())
    }
}

fn op_to_pb(op: &Op) -> pb::Op {
    use pb::op::Kind;
    let kind = match op {
        Op::Put { cf, key, value, expire_at_ms } => Kind::Put(pb::Put {
            cf: cf.clone(),
            key: key.clone(),
            value: value.clone(),
            expire_at_ms: *expire_at_ms,
        }),
        Op::Delete { cf, key } => Kind::Delete(pb::Delete { cf: cf.clone(), key: key.clone() }),
        Op::DeleteRange { cf, start, end } => Kind::DeleteRange(pb::DeleteRange {
            cf: cf.clone(),
            start: start.clone(),
            end: end.clone(),
        }),
        Op::CreateCf { spec } => Kind::CreateCf(pb::CreateCf {
            name: spec.name.clone(),
            comparator: spec.comparator.clone(),
        }),
        Op::DropCf { name } => Kind::DropCf(pb::DropCf { name: name.clone() }),
    };
    pb::Op { kind: Some(kind) }
}

fn op_from_pb(op: pb::Op) -> Result<Op> {
    use pb::op::Kind;
    Ok(match op.kind.ok_or_else(|| Error::corrupt("operation with no kind"))? {
        Kind::Put(p) => Op::Put { cf: p.cf, key: p.key, value: p.value, expire_at_ms: p.expire_at_ms },
        Kind::Delete(d) => Op::Delete { cf: d.cf, key: d.key },
        Kind::DeleteRange(r) => Op::DeleteRange { cf: r.cf, start: r.start, end: r.end },
        Kind::CreateCf(c) => Op::CreateCf { spec: CfSpec { name: c.name, comparator: c.comparator } },
        Kind::DropCf(d) => Op::DropCf { name: d.name },
    })
}

/// Encoded size of the entry the master would write for `ops`. Used for `max_entry_bytes`.
pub fn encoded_entry_len(seq: Seq, epoch: u64, ts_ms: u64, ops: &[Op]) -> usize {
    LogEntry::new(seq, epoch, ts_ms, encode_ops(ops)).encoded_len()
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::types::CfSpec;

    pub(crate) fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace().map(|b| u8::from_str_radix(b, 16).unwrap()).collect()
    }

    fn v1_ops() -> Vec<Op> {
        vec![Op::Put { cf: "users".into(), key: b"k1".to_vec(), value: b"v1".to_vec(), expire_at_ms: None }]
    }

    #[test]
    fn crc32c_sanity() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn vector_v1_ops_bytes_and_crc() {
        let ops = encode_ops(&v1_ops());
        assert_eq!(ops.as_ref(), hex("0a 11 0a 0f 0a 05 75 73 65 72 73 12 02 6b 31 1a 02 76 31").as_slice());
        let e = LogEntry::new(1, 1, 1_700_000_000_000, ops);
        assert_eq!(e.crc32c, 0x7371_7118);
        e.verify_crc().unwrap();
    }

    #[test]
    fn entry_round_trips_through_storage_encoding() {
        let e = LogEntry::new(7, 2, 99, encode_ops(&v1_ops()));
        let back = LogEntry::decode(&e.encode_to_vec()).unwrap();
        assert_eq!(back, e);
        assert_eq!(back.decode_ops().unwrap(), v1_ops());
        assert_eq!(e.encoded_len(), e.encode_to_vec().len());
    }

    #[test]
    fn crc_covers_header_fields() {
        let mut e = LogEntry::new(1, 1, 5, encode_ops(&v1_ops()));
        e.seq = 2;
        assert!(matches!(e.verify_crc(), Err(Error::Corrupt(_))));
    }

    #[test]
    fn decode_rejects_structural_violations() {
        let empty = pb::OpList { ops: vec![] }.encode_to_vec();
        assert!(matches!(decode_ops(&empty), Err(Error::Corrupt(_))));

        let no_kind = pb::OpList { ops: vec![pb::Op { kind: None }] }.encode_to_vec();
        assert!(matches!(decode_ops(&no_kind), Err(Error::Corrupt(_))));

        let mixed = encode_ops(&[
            Op::CreateCf { spec: CfSpec::new("a") },
            Op::Delete { cf: "a".into(), key: vec![] },
        ]);
        assert!(matches!(decode_ops(&mixed), Err(Error::Corrupt(_))));

        let reserved = encode_ops(&[Op::Delete { cf: "__meta".into(), key: vec![] }]);
        assert!(matches!(decode_ops(&reserved), Err(Error::Corrupt(_))));

        assert!(matches!(decode_ops(b"\xff\xff"), Err(Error::Corrupt(_))));
    }

    #[test]
    fn decode_rejects_more_structural_violations() {
        let two_cf_ops = encode_ops(&[
            Op::CreateCf { spec: CfSpec::new("a") },
            Op::CreateCf { spec: CfSpec::new("b") },
        ]);
        assert!(matches!(decode_ops(&two_cf_ops), Err(Error::Corrupt(_))));

        let drop_mixed = encode_ops(&[
            Op::DropCf { name: "a".into() },
            Op::Put { cf: "a".into(), key: vec![], value: vec![], expire_at_ms: None },
        ]);
        assert!(matches!(decode_ops(&drop_mixed), Err(Error::Corrupt(_))));

        let reserved_create = encode_ops(&[Op::CreateCf { spec: CfSpec::new("__meta") }]);
        assert!(matches!(decode_ops(&reserved_create), Err(Error::Corrupt(_))));

        let reserved_drop = encode_ops(&[Op::DropCf { name: "__changelog".into() }]);
        assert!(matches!(decode_ops(&reserved_drop), Err(Error::Corrupt(_))));
    }

    #[test]
    fn cf_name_rules() {
        validate_cf_name("users").unwrap();
        validate_cf_name("a:b:ü").unwrap();
        validate_cf_name(&"x".repeat(255)).unwrap();
        for bad in ["", "__x", &"x".repeat(256)] {
            assert!(matches!(validate_cf_name(bad), Err(Error::InvalidCfName(_))), "{bad:?}");
        }
    }
}
