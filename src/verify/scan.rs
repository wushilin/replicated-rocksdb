//! Range reads through a pinned snapshot, under the CF's comparator. Used by the server for
//! `HashRange` and `ListRange`, and by the verifier for its own side.

use std::sync::{Arc, Mutex, PoisonError};

use super::VerifyMode;
use super::range::{
    Bounds, Chunker, KeyOrder, Lister, LocalRange, LocalRecord, RangeHasher, Throttle,
};
use crate::error::Result;
use crate::pb;
use crate::read::ReadSnapshot;
use crate::store::cfs::PhysCf;

/// Calls `f(key, envelope)` for each key of `cf` in `bounds`, in order, until `f` returns
/// `false`. Expired values are included: verify compares what is stored.
pub(crate) fn scan(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mut f: impl FnMut(&[u8], &[u8]) -> Result<bool>,
) -> Result<()> {
    let ctx = snap.raw_ctx();
    for item in ctx.iter_raw_from(cf, bounds.lower.as_deref())? {
        let (key, raw) = item?;
        if !bounds.below_upper(order, &key) || !f(&key, &raw)? {
            break;
        }
    }
    Ok(())
}

/// `(count, hash)` of a range (PROTOCOL.md §5.4, §9.3).
pub(crate) fn hash_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mode: VerifyMode,
) -> Result<(u64, [u8; 32])> {
    let mut h = RangeHasher::new(mode);
    scan(snap, cf, order, bounds, |k, v| h.add(k, v).map(|()| true))?;
    Ok(h.finish())
}

/// Up to `limit` keys of a range with their `write_seq` and payload hash; `true` if there are more.
pub(crate) fn list_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mode: VerifyMode,
    limit: usize,
    soft_bytes: usize,
) -> Result<(Vec<pb::KeyDigest>, bool)> {
    let mut l = Lister::new(mode, limit, soft_bytes);
    scan(snap, cf, order, bounds, |k, v| l.offer(k, v))?;
    Ok((l.keys, l.truncated))
}

/// Sleeps as long as `throttle` asks for after reading `n` bytes.
pub(crate) fn pace(throttle: &Mutex<Throttle>, n: u64) {
    let pause = throttle
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .consume(n);
    if !pause.is_zero() {
        std::thread::sleep(pause);
    }
}

/// The local records of a range, for narrowing.
pub(crate) fn load_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mode: VerifyMode,
    throttle: &Mutex<Throttle>,
) -> Result<Vec<LocalRecord>> {
    let mut out = Vec::new();
    scan(snap, cf, order, bounds, |k, v| {
        pace(throttle, (k.len() + v.len()) as u64);
        out.push(LocalRecord::new(k, v, mode)?);
        Ok(true)
    })?;
    Ok(out)
}

/// Cuts a whole CF into contiguous ranges and hands each to `emit` as soon as it is closed.
/// Stops early (without error) when `emit` returns `false`. Returns the number of keys read.
pub(crate) fn chunk_cf(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    mode: VerifyMode,
    chunk_keys: usize,
    chunk_bytes: usize,
    throttle: &Mutex<Throttle>,
    mut emit: impl FnMut(LocalRange) -> bool,
) -> Result<u64> {
    let mut chunker = Chunker::new(mode, chunk_keys, chunk_bytes);
    let mut keys = 0u64;
    let mut open = true;
    scan(
        snap,
        cf,
        &KeyOrder::bytewise(),
        &Bounds::default(),
        |k, v| {
            pace(throttle, (k.len() + v.len()) as u64);
            keys += 1;
            if let Some(r) = chunker.push(k, v)? {
                open = emit(r);
            }
            Ok(open)
        },
    )?;
    if open {
        emit(chunker.finish());
    }
    Ok(keys)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope;
    use crate::store::test_support::{apply, open_store, open_store_with};
    use crate::tuning::RocksTuning;
    use crate::types::{CfSpec, Op};

    fn hex32(s: &str) -> [u8; 32] {
        let v: Vec<u8> = (0..32)
            .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    fn put(cf: &str, k: &[u8], v: &[u8]) -> Op {
        Op::Put {
            cf: cf.into(),
            key: k.to_vec(),
            value: v.to_vec(),
            expire_at_ms: None,
        }
    }

    fn all() -> Bounds {
        Bounds::default()
    }

    fn b(lower: Option<&[u8]>, upper: Option<&[u8]>) -> Bounds {
        Bounds {
            lower: lower.map(<[u8]>::to_vec),
            upper: upper.map(<[u8]>::to_vec),
        }
    }

    #[test]
    fn a_snapshot_range_hashes_to_the_v3_vectors() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("users"),
            }],
        )
        .unwrap();
        // V3 stores k1 with the envelope written at seq 1; write it raw into the CF.
        let phys = store.live_cf("users").unwrap();
        store
            .db()
            .put_cf(
                &store.handle(&phys).unwrap(),
                b"k1",
                envelope::encode(1, None, b"v1"),
            )
            .unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let o = KeyOrder::bytewise();
        let cf = snap.phys("users").unwrap();
        assert_eq!(
            hash_range(&snap, cf.clone(), &o, &all(), VerifyMode::Quick).unwrap(),
            (
                1,
                hex32("2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3")
            )
        );
        assert_eq!(
            hash_range(&snap, cf.clone(), &o, &all(), VerifyMode::Full).unwrap(),
            (
                1,
                hex32("14e556f28a8301390e9be56ecb4ec7ff3ceea3cd48316ab7f7f0488117e53ecd")
            )
        );
        assert_eq!(
            hash_range(&snap, cf, &o, &b(None, Some(b"k1")), VerifyMode::Full).unwrap(),
            (
                0,
                hex32("af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            )
        );
    }

    #[test]
    fn ranges_are_half_open_and_the_empty_key_is_a_bound() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap();
        apply(
            &store,
            vec![
                put("c", b"", b"e"),
                put("c", b"\0", b"n"),
                put("c", b"\0\0", b"nn"),
                put("c", b"a", b"a"),
            ],
        )
        .unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let o = KeyOrder::bytewise();
        let cf = snap.phys("c").unwrap();
        let count = |bounds: Bounds| {
            hash_range(&snap, cf.clone(), &o, &bounds, VerifyMode::Quick)
                .unwrap()
                .0
        };
        assert_eq!(count(all()), 4);
        assert_eq!(
            count(b(None, Some(b""))),
            0,
            "an empty upper bound excludes everything"
        );
        assert_eq!(
            count(b(Some(b""), None)),
            4,
            "an empty lower bound includes the empty key"
        );
        assert_eq!(count(b(Some(b""), Some(b"\0"))), 1);
        assert_eq!(count(b(Some(b"\0"), Some(b"a"))), 2);
        assert_eq!(count(b(Some(b"a"), Some(b"a"))), 0);
        // Contiguous ranges partition the CF.
        let parts = [
            b(None, Some(b"\0")),
            b(Some(b"\0"), Some(b"\0\0")),
            b(Some(b"\0\0"), None),
        ];
        assert_eq!(parts.into_iter().map(count).sum::<u64>(), 4);
    }

    #[test]
    fn listings_and_loads_follow_a_custom_comparator() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = RocksTuning::new();
        tuning
            .comparators
            .register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let store = open_store_with(dir.path(), tuning.clone());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("r").with_comparator("reverse"),
            }],
        )
        .unwrap();
        apply(
            &store,
            (b'a'..=b'f').map(|k| put("r", &[k], &[k])).collect(),
        )
        .unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let o = KeyOrder::new(&tuning.comparators, Some("reverse"));
        let cf = snap.phys("r").unwrap();
        // [e, b) under reverse order is e, d, c.
        let (keys, more) = list_range(
            &snap,
            cf.clone(),
            &o,
            &b(Some(b"e"), Some(b"b")),
            VerifyMode::Quick,
            1024,
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            keys.iter().map(|k| k.key.clone()).collect::<Vec<_>>(),
            vec![b"e".to_vec(), b"d".to_vec(), b"c".to_vec()]
        );
        assert!(!more);
        assert!(keys.iter().all(|k| k.write_seq == 2));
        let (keys, more) = list_range(
            &snap,
            cf.clone(),
            &o,
            &all(),
            VerifyMode::Quick,
            2,
            usize::MAX,
        )
        .unwrap();
        assert_eq!((keys.len(), more), (2, true));
        let t = Mutex::new(Throttle::new(None));
        let recs = load_range(
            &snap,
            cf.clone(),
            &o,
            &b(None, Some(b"d")),
            VerifyMode::Full,
            &t,
        )
        .unwrap();
        assert_eq!(
            recs.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
            vec![b"f".to_vec(), b"e".to_vec()]
        );
        let mut ranges = Vec::new();
        let n = chunk_cf(&snap, cf, VerifyMode::Quick, 4, usize::MAX, &t, |r| {
            ranges.push(r);
            true
        })
        .unwrap();
        assert_eq!(n, 6);
        assert_eq!(ranges.len(), 2);
        assert_eq!(
            ranges[0].bounds.upper.as_deref(),
            Some(&b"b"[..]),
            "boundaries follow the CF order"
        );
    }

    #[test]
    fn the_snapshot_pins_what_is_hashed() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap();
        apply(&store, vec![put("c", b"a", b"1")]).unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let before = hash_range(
            &snap,
            snap.phys("c").unwrap(),
            &KeyOrder::bytewise(),
            &all(),
            VerifyMode::Full,
        )
        .unwrap();
        apply(&store, vec![put("c", b"a", b"2"), put("c", b"b", b"3")]).unwrap();
        let after = hash_range(
            &snap,
            snap.phys("c").unwrap(),
            &KeyOrder::bytewise(),
            &all(),
            VerifyMode::Full,
        )
        .unwrap();
        assert_eq!(before, after);
        assert_eq!(after.0, 1);
    }
}
