//! Conversions between crate types and their protobuf messages.

use crate::error::{Error, Result};
use crate::pb;
use crate::types::{CfEntry, CfSpec, DbIdentity, EpochStart, LogEntry};

pub(crate) fn identity_to_pb(id: &DbIdentity) -> pb::Identity {
    pb::Identity { db_uuid: id.db_uuid.as_bytes().to_vec(), epoch: id.epoch }
}

/// `Corrupt` if `db_uuid` is not 16 bytes.
pub(crate) fn identity_from_pb(p: &pb::Identity) -> Result<DbIdentity> {
    let db_uuid = uuid::Uuid::from_slice(&p.db_uuid)
        .map_err(|_| Error::corrupt(format!("identity has a {}-byte db_uuid", p.db_uuid.len())))?;
    Ok(DbIdentity { db_uuid, epoch: p.epoch })
}

pub(crate) fn cf_to_pb(e: &CfEntry) -> pb::CfSpec {
    pb::CfSpec { name: e.spec.name.clone(), comparator: e.spec.comparator.clone(), create_seq: e.create_seq }
}

pub(crate) fn cf_from_pb(p: &pb::CfSpec) -> CfEntry {
    CfEntry { spec: CfSpec { name: p.name.clone(), comparator: p.comparator.clone() }, create_seq: p.create_seq }
}

pub(crate) fn epochs_to_pb(h: &[EpochStart]) -> Vec<pb::EpochStart> {
    h.iter().map(|e| pb::EpochStart { epoch: e.epoch, start_seq: e.start_seq }).collect()
}

pub(crate) fn epochs_from_pb(h: &[pb::EpochStart]) -> Vec<EpochStart> {
    h.iter().map(|e| EpochStart { epoch: e.epoch, start_seq: e.start_seq }).collect()
}

/// Validates an epoch history received from upstream (PROTOCOL.md §5.5) for `identity`:
/// non-empty, starting with `{1, 1}`, epochs strictly increasing, start seqs non-decreasing,
/// and ending with the identity's epoch. Anything else is `Corrupt`.
pub(crate) fn checked_history(identity: &DbIdentity, h: &[pb::EpochStart]) -> Result<Vec<EpochStart>> {
    let history = epochs_from_pb(h);
    let bad = |why: &str| Err(Error::corrupt(format!("invalid epoch history {history:?}: {why}")));
    match history.first() {
        None => return bad("empty"),
        Some(first) if (first.epoch, first.start_seq) != (1, 1) => return bad("must start with {1, 1}"),
        Some(_) => {}
    }
    if !history.windows(2).all(|w| w[0].epoch < w[1].epoch && w[0].start_seq <= w[1].start_seq) {
        return bad("epochs must increase and start seqs must not decrease");
    }
    if history.last().map(|e| e.epoch) != Some(identity.epoch) {
        return bad("the last epoch must be the identity's epoch");
    }
    Ok(history)
}

/// `ops` is moved, never re-encoded.
pub(crate) fn entry_to_pb(e: LogEntry) -> pb::LogEntry {
    pb::LogEntry { seq: e.seq, epoch: e.epoch, ts_ms: e.ts_ms, ops: e.ops, crc32c: e.crc32c }
}

pub(crate) fn entry_from_pb(p: pb::LogEntry) -> LogEntry {
    LogEntry { seq: p.seq, epoch: p.epoch, ts_ms: p.ts_ms, ops: p.ops, crc32c: p.crc32c }
}

#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::codec::encode_ops;
    use crate::types::Op;

    #[test]
    fn identity_round_trips_and_rejects_short_uuid() {
        let id = DbIdentity { db_uuid: uuid::Uuid::new_v4(), epoch: 7 };
        assert_eq!(identity_from_pb(&identity_to_pb(&id)).unwrap(), id);
        let bad = pb::Identity { db_uuid: vec![1, 2, 3], epoch: 1 };
        assert!(matches!(identity_from_pb(&bad), Err(Error::Corrupt(_))));
    }

    #[test]
    fn cf_specs_and_epochs_round_trip() {
        let e = CfEntry { spec: CfSpec::new("a:b").with_comparator("rev"), create_seq: 9 };
        assert_eq!(cf_from_pb(&cf_to_pb(&e)), e);
        let h = vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 3, start_seq: 70 }];
        assert_eq!(epochs_from_pb(&epochs_to_pb(&h)), h);
    }

    #[test]
    fn epoch_histories_are_validated() {
        let id = |epoch| DbIdentity { db_uuid: uuid::Uuid::nil(), epoch };
        let h = |v: &[(u64, u64)]| v.iter().map(|&(epoch, start_seq)| pb::EpochStart { epoch, start_seq }).collect::<Vec<_>>();
        assert_eq!(checked_history(&id(1), &h(&[(1, 1)])).unwrap(), vec![EpochStart { epoch: 1, start_seq: 1 }]);
        checked_history(&id(3), &h(&[(1, 1), (2, 50), (3, 50)])).unwrap();
        for (identity, bad) in [
            (1, h(&[])),
            (1, h(&[(1, 2)])),
            (2, h(&[(2, 1)])),
            (2, h(&[(1, 1), (2, 10), (2, 20)])),
            (3, h(&[(1, 1), (3, 10), (2, 20)])),
            (2, h(&[(1, 1), (2, 10), (3, 5)])),
            (3, h(&[(1, 1), (2, 10)])),
        ] {
            assert!(matches!(checked_history(&id(identity), &bad), Err(Error::Corrupt(_))), "{bad:?}");
        }
    }

    #[test]
    fn entry_bytes_survive_the_wire_unchanged() {
        let ops = vec![Op::Put { cf: "c".into(), key: vec![], value: vec![0; 3], expire_at_ms: None }];
        let e = LogEntry::new(5, 2, 1_000, encode_ops(&ops));
        let wire = entry_to_pb(e.clone()).encode_to_vec();
        let back = entry_from_pb(pb::LogEntry::decode(wire.as_slice()).unwrap());
        assert_eq!(back, e);
        back.verify_crc().unwrap();
        assert_eq!(back.decode_ops().unwrap(), ops);
    }

    #[test]
    fn empty_and_absent_bytes_both_decode_to_empty() {
        // proto3 does not distinguish an empty `bytes` field from an absent one.
        let rec = pb::Record { key: vec![], envelope: vec![] };
        let back = pb::Record::decode(rec.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back.key, Vec::<u8>::new());
        // `KeyRange.lower` is `optional bytes`: an empty key is distinct from "unbounded".
        let r = pb::KeyRange { cf: "c".into(), lower: Some(vec![]), upper: None };
        let back = pb::KeyRange::decode(r.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back.lower, Some(vec![]));
        assert_eq!(back.upper, None);
    }
}
