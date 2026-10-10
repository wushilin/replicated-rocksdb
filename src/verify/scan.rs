//! Range reads through a pinned snapshot, under the CF's comparator. Used by the server for
//! `HashRange` and `ListRange`, and by the verifier for its own side.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError};

use super::VerifyMode;
use super::range::{Bounds, Chunker, KeyOrder, LocalRange, LocalRecord, Throttle};
#[cfg(test)]
use super::range::{Lister, RangeHasher};
use crate::error::Result;
use crate::pb;
use crate::read::ReadSnapshot;
use crate::store::cfs::PhysCf;

/// Calls `f(key, envelope)` for each key of `cf` in `bounds`, in order, until `f` returns
/// `false`. Expired values are included: verify compares what is stored.
#[cfg(test)]
pub(crate) fn scan(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    f: impl FnMut(&[u8], &[u8]) -> Result<bool>,
) -> Result<()> {
    scan_until(snap, cf, order, bounds, None, f).map(|_| ())
}

/// Like [`scan`], but checks `cancel` before every key. Returns `false` if it was set (the
/// scan then stopped early and its result must be discarded).
pub(crate) fn scan_until(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    cancel: Option<&AtomicBool>,
    mut f: impl FnMut(&[u8], &[u8]) -> Result<bool>,
) -> Result<bool> {
    let ctx = snap.raw_ctx();
    for item in ctx.iter_raw_from(cf, bounds.lower.as_deref())? {
        if cancel.is_some_and(|c| c.load(Ordering::Relaxed)) {
            return Ok(false);
        }
        let (key, raw) = item?;
        if !bounds.below_upper(order, &key) || !f(&key, &raw)? {
            break;
        }
    }
    Ok(true)
}

/// Up to `limit` keys of a range with their whole envelopes, for repair; `true` if there are
/// more. Like a listing, the answer is cut at about `soft_bytes`, but never below two keys.
/// Checks `cancel` before every key; `None` if it was set (discard the result).
pub(crate) fn fetch_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    limit: usize,
    soft_bytes: usize,
    cancel: &AtomicBool,
) -> Result<Option<(Vec<pb::FetchedRecord>, bool)>> {
    let (mut records, mut bytes, mut truncated) = (Vec::new(), 0usize, false);
    let done = scan_until(snap, cf, order, bounds, Some(cancel), |k, v| {
        if records.len() >= limit || (records.len() >= 2 && bytes >= soft_bytes) {
            truncated = true;
            return Ok(false);
        }
        bytes += k.len() + v.len();
        records.push(pb::FetchedRecord {
            key: k.to_vec(),
            envelope: Some(v.to_vec()),
        });
        Ok(true)
    })?;
    Ok(done.then_some((records, truncated)))
}

/// `(count, hash)` of a range (PROTOCOL.md §5.4, §9.3). The server hashes through `scan_until`
/// (cancellable); this is the plain reference its tests compare against.
#[cfg(test)]
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
/// Test reference for the server's cancellable listing.
#[cfg(test)]
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

/// Blocking: sleeps the OS thread (`std::thread::sleep`); async callers must run it inside
/// `spawn_blocking` / `background::blocking`. Sleeps as long as `throttle` asks for after
/// reading `n` bytes, in slices of at most [`PACE_SLICE`], and returns early once `stop` is set.
pub(crate) fn pace(throttle: &Mutex<Throttle>, n: u64, stop: &AtomicBool) {
    let mut pause = throttle
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .consume(n);
    while !pause.is_zero() && !stop.load(Ordering::Relaxed) {
        let slice = pause.min(PACE_SLICE);
        std::thread::sleep(slice);
        pause -= slice;
    }
}

/// The longest [`pace`] sleeps without looking at its stop flag.
pub(crate) const PACE_SLICE: std::time::Duration = std::time::Duration::from_millis(20);

/// The error of a local scan that `stop` ended: the run is gone, nobody reads the result.
fn stopped() -> crate::error::Error {
    crate::error::Error::Io(std::io::Error::new(
        std::io::ErrorKind::Interrupted,
        "the verify run was stopped",
    ))
}

/// The local records of a range, for narrowing. Blocking (RocksDB reads plus [`pace`]): call
/// only from `spawn_blocking` / `background::blocking`. `Err(Interrupted)` once `stop` is set.
pub(crate) fn load_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mode: VerifyMode,
    throttle: &Mutex<Throttle>,
    stop: &AtomicBool,
) -> Result<Vec<LocalRecord>> {
    let mut out = Vec::new();
    let finished = scan_until(snap, cf, order, bounds, Some(stop), |k, v| {
        pace(throttle, (k.len() + v.len()) as u64, stop);
        out.push(LocalRecord::new(k, v, mode)?);
        Ok(true)
    })?;
    if !finished || stop.load(Ordering::Relaxed) {
        return Err(stopped());
    }
    Ok(out)
}

/// Cuts a whole CF into contiguous ranges and hands each to `emit` as soon as it is closed.
/// Blocking (RocksDB reads plus [`pace`]): call only from `spawn_blocking` /
/// `background::blocking`.
///
/// Returns `Some(keys read)` when the whole CF was cut and emitted. Returns `None` when
/// `emit` returned `false` or `stop` was set and the scan stopped early: ranges are then
/// missing, so callers must treat it as an abort, never as a complete scan.
#[allow(clippy::too_many_arguments)]
pub(crate) fn chunk_cf(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    mode: VerifyMode,
    chunk_keys: usize,
    chunk_bytes: usize,
    throttle: &Mutex<Throttle>,
    stop: &AtomicBool,
    mut emit: impl FnMut(LocalRange) -> bool,
) -> Result<Option<u64>> {
    let mut chunker = Chunker::new(mode, chunk_keys, chunk_bytes);
    let mut keys = 0u64;
    let mut open = true;
    let finished = scan_until(
        snap,
        cf,
        &KeyOrder::bytewise(),
        &Bounds::default(),
        Some(stop),
        |k, v| {
            pace(throttle, (k.len() + v.len()) as u64, stop);
            keys += 1;
            if let Some(r) = chunker.push(k, v)? {
                open = emit(r);
            }
            Ok(open && !stop.load(Ordering::Relaxed))
        },
    )?;
    if !finished || !open || stop.load(Ordering::Relaxed) {
        return Ok(None);
    }
    if !emit(chunker.finish()) {
        return Ok(None);
    }
    Ok(Some(keys))
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
        let o = KeyOrder::new(&tuning.comparators, Some("reverse")).unwrap();
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
            &AtomicBool::new(false),
        )
        .unwrap();
        assert_eq!(
            recs.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
            vec![b"f".to_vec(), b"e".to_vec()]
        );
        let mut ranges = Vec::new();
        let n = chunk_cf(
            &snap,
            cf,
            VerifyMode::Quick,
            4,
            usize::MAX,
            &t,
            &AtomicBool::new(false),
            |r| {
                ranges.push(r);
                true
            },
        )
        .unwrap();
        assert_eq!(n, Some(6));
        assert_eq!(ranges.len(), 2);
        assert_eq!(
            ranges[0].bounds.upper.as_deref(),
            Some(&b"b"[..]),
            "boundaries follow the CF order"
        );
    }

    /// A stop flag ends local scans at once, even throttled to a crawl: they must not keep the
    /// database open after the run is gone.
    #[test]
    fn a_stopped_scan_ends_at_once_even_when_throttled() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap();
        let ops = (0..50u8).map(|i| put("c", &[i], &[i; 100])).collect();
        apply(&store, ops).unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let cf = snap.phys("c").unwrap();
        // 100 bytes per second: the unstopped scan would take over a minute.
        let slow = Mutex::new(Throttle::new(Some(100)));
        let stop = Arc::new(AtomicBool::new(false));
        let flag = stop.clone();
        let setter = std::thread::spawn(move || {
            std::thread::sleep(std::time::Duration::from_millis(100));
            flag.store(true, Ordering::SeqCst);
        });
        let started = std::time::Instant::now();
        let n = chunk_cf(
            &snap,
            cf.clone(),
            VerifyMode::Full,
            10,
            usize::MAX,
            &slow,
            &stop,
            |_| true,
        )
        .unwrap();
        assert_eq!(n, None, "an early stop is never a complete scan");
        assert!(started.elapsed() < std::time::Duration::from_secs(5));
        setter.join().unwrap();
        // Already stopped: load_range refuses at once.
        let e = load_range(
            &snap,
            cf,
            &KeyOrder::bytewise(),
            &all(),
            VerifyMode::Full,
            &slow,
            &stop,
        )
        .unwrap_err();
        assert!(
            matches!(&e, crate::error::Error::Io(io) if io.kind() == std::io::ErrorKind::Interrupted),
            "{e:?}"
        );
    }

    #[test]
    fn fetch_range_returns_whole_envelopes_in_pages() {
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
            (b'a'..=b'e').map(|k| put("c", &[k], &[k; 3])).collect(),
        )
        .unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let cf = snap.phys("c").unwrap();
        let o = KeyOrder::bytewise();
        let live = AtomicBool::new(false);
        let (all_in, more) = fetch_range(
            &snap,
            cf.clone(),
            &o,
            &b(Some(b"b"), Some(b"e")),
            1024,
            usize::MAX,
            &live,
        )
        .unwrap()
        .unwrap();
        assert!(!more);
        assert_eq!(
            all_in.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
        assert_eq!(
            all_in[0].envelope.as_deref(),
            Some(envelope::encode(2, None, b"bbb").as_slice())
        );
        let (page, more) = fetch_range(&snap, cf.clone(), &o, &all(), 1024, 1, &live)
            .unwrap()
            .unwrap();
        assert_eq!(
            (page.len(), more),
            (2, true),
            "two records even past the soft size"
        );
        let (page, more) = fetch_range(&snap, cf.clone(), &o, &all(), 3, usize::MAX, &live)
            .unwrap()
            .unwrap();
        assert_eq!((page.len(), more), (3, true), "cut at the limit");
        let cancelled = AtomicBool::new(true);
        assert!(
            fetch_range(&snap, cf, &o, &all(), 1024, usize::MAX, &cancelled)
                .unwrap()
                .is_none(),
            "a set cancel flag stops the scan"
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
