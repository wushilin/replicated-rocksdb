//! Range arithmetic both verify sides share: key order, half-open bounds, per-mode payloads,
//! range hashes, key listings, the chunker that cuts a CF into contiguous ranges, and the
//! read-rate throttle.

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{MAX_BOUNDARY_KEY_BYTES, VerifyMode};
use crate::codec::digest::Digest;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::pb;
use crate::tuning::{ComparatorRegistry, CompareFn};
use crate::types::Seq;

/// A CF's key order: its registered comparator, or bytewise.
#[derive(Clone, Default)]
pub(crate) struct KeyOrder(Option<Arc<CompareFn>>);

impl KeyOrder {
    pub(crate) fn bytewise() -> Self {
        KeyOrder(None)
    }

    /// `None` is bytewise. A name that is not registered is an error: guessing bytewise would
    /// make ranges disagree with the CF's real order.
    pub(crate) fn new(reg: &ComparatorRegistry, name: Option<&str>) -> Result<Self> {
        match name {
            None => Ok(KeyOrder(None)),
            Some(n) => reg
                .get(n)
                .map(|f| KeyOrder(Some(f)))
                .ok_or_else(|| Error::Config(format!("comparator {n:?} is not registered"))),
        }
    }

    pub(crate) fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        match &self.0 {
            Some(f) => f(a, b),
            None => a.cmp(b),
        }
    }
}

/// A half-open key range `[lower, upper)` under a CF's order. `None` is unbounded; an empty
/// key is a real bound (PROTOCOL.md §9.2).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Bounds {
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
}

impl Bounds {
    /// True if `key` sorts before `upper` (always, without an upper bound).
    pub(crate) fn below_upper(&self, order: &KeyOrder, key: &[u8]) -> bool {
        self.upper
            .as_deref()
            .is_none_or(|u| order.cmp(key, u) == Ordering::Less)
    }

    #[cfg(test)] // the tests' in-memory upstream; the scans only need `below_upper`
    pub(crate) fn contains(&self, order: &KeyOrder, key: &[u8]) -> bool {
        self.lower
            .as_deref()
            .is_none_or(|l| order.cmp(key, l) != Ordering::Less)
            && self.below_upper(order, key)
    }

    pub(crate) fn to_pb(&self, cf: &str) -> pb::KeyRange {
        pb::KeyRange {
            cf: cf.to_string(),
            lower: self.lower.clone(),
            upper: self.upper.clone(),
        }
    }

    pub(crate) fn from_pb(r: &pb::KeyRange) -> Bounds {
        Bounds {
            lower: r.lower.clone(),
            upper: r.upper.clone(),
        }
    }
}

/// The bytes hashed for one record (PROTOCOL.md §9.3): the envelope header in `Quick`, the
/// whole envelope in `Full`. `Corrupt` if the envelope does not decode.
pub(crate) fn payload(raw: &[u8], mode: VerifyMode) -> Result<&[u8]> {
    let env = envelope::decode(raw)?;
    Ok(match mode {
        VerifyMode::Quick => &raw[..env.header_len],
        VerifyMode::Full => raw,
    })
}

/// Range hash (PROTOCOL.md §5.4) of records offered in key order.
pub(crate) struct RangeHasher {
    mode: VerifyMode,
    digest: Digest,
}

impl RangeHasher {
    pub(crate) fn new(mode: VerifyMode) -> Self {
        RangeHasher {
            mode,
            digest: Digest::new(),
        }
    }

    pub(crate) fn add(&mut self, key: &[u8], raw: &[u8]) -> Result<()> {
        self.digest.record(key, payload(raw, self.mode)?);
        Ok(())
    }

    /// `(count, hash)`.
    pub(crate) fn finish(self) -> (u64, [u8; 32]) {
        self.digest.finish_range()
    }
}

/// A `ListRange` answer being built from records offered in key order.
pub(crate) struct Lister {
    mode: VerifyMode,
    limit: usize,
    soft_bytes: usize,
    bytes: usize,
    pub keys: Vec<pb::KeyDigest>,
    pub truncated: bool,
}

impl Lister {
    pub(crate) fn new(mode: VerifyMode, limit: usize, soft_bytes: usize) -> Self {
        Lister {
            mode,
            limit,
            soft_bytes,
            bytes: 0,
            keys: Vec::new(),
            truncated: false,
        }
    }

    /// Offers the next key. Returns `false` once the listing is full; that key is not added
    /// and `truncated` is set. The listing is full at `limit` keys, or at `soft_bytes` once it
    /// holds two keys (so a client that continues from the last key always makes progress).
    pub(crate) fn offer(&mut self, key: &[u8], raw: &[u8]) -> Result<bool> {
        let full = self.keys.len() >= self.limit
            || (self.keys.len() >= 2 && self.bytes >= self.soft_bytes);
        if full {
            self.truncated = true;
            return Ok(false);
        }
        let write_seq = envelope::decode(raw)?.meta.write_seq;
        let hash = blake3::hash(payload(raw, self.mode)?);
        self.bytes += key.len() + 48;
        self.keys.push(pb::KeyDigest {
            key: key.to_vec(),
            write_seq,
            payload_hash: hash.as_bytes().to_vec(),
        });
        Ok(true)
    }
}

/// One local range with its count and hash, as sent in a `HashRange`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalRange {
    pub bounds: Bounds,
    pub count: u64,
    pub hash: [u8; 32],
}

/// One local record of a range being narrowed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalRecord {
    pub key: Vec<u8>,
    pub write_seq: Seq,
    /// The mode's payload (header or whole envelope).
    pub payload: Vec<u8>,
}

impl LocalRecord {
    pub(crate) fn new(key: &[u8], raw: &[u8], mode: VerifyMode) -> Result<Self> {
        let write_seq = envelope::decode(raw)?.meta.write_seq;
        Ok(LocalRecord {
            key: key.to_vec(),
            write_seq,
            payload: payload(raw, mode)?.to_vec(),
        })
    }
}

/// `LocalRange` of `records` (already in key order) within `bounds`.
pub(crate) fn local_range(bounds: Bounds, records: &[LocalRecord]) -> LocalRange {
    let mut d = Digest::new();
    for r in records {
        d.record(&r.key, &r.payload);
    }
    let (count, hash) = d.finish_range();
    LocalRange {
        bounds,
        count,
        hash,
    }
}

/// Cuts one CF, read in key order, into contiguous ranges (PROTOCOL.md §9.2): the first
/// has no lower bound, each upper bound is the next lower bound, the last has no upper
/// bound. A range is closed when it holds `chunk_keys` keys or `chunk_bytes` bytes and the
/// next key can serve as a boundary (at most [`MAX_BOUNDARY_KEY_BYTES`]).
pub(crate) struct Chunker {
    mode: VerifyMode,
    chunk_keys: u64,
    chunk_bytes: u64,
    lower: Option<Vec<u8>>,
    digest: Digest,
    bytes: u64,
}

impl Chunker {
    pub(crate) fn new(mode: VerifyMode, chunk_keys: usize, chunk_bytes: usize) -> Self {
        Chunker {
            mode,
            chunk_keys: chunk_keys.max(1) as u64,
            chunk_bytes: chunk_bytes.max(1) as u64,
            lower: None,
            digest: Digest::new(),
            bytes: 0,
        }
    }

    /// Adds the next key. Returns the range this key closes, if any.
    pub(crate) fn push(&mut self, key: &[u8], raw: &[u8]) -> Result<Option<LocalRange>> {
        let payload = payload(raw, self.mode)?;
        let full = self.digest.records() >= self.chunk_keys || self.bytes >= self.chunk_bytes;
        let mut closed = None;
        if full && self.digest.records() > 0 && key.len() <= MAX_BOUNDARY_KEY_BYTES {
            let (count, hash) = std::mem::take(&mut self.digest).finish_range();
            let lower = self.lower.replace(key.to_vec());
            closed = Some(LocalRange {
                bounds: Bounds {
                    lower,
                    upper: Some(key.to_vec()),
                },
                count,
                hash,
            });
            self.bytes = 0;
        }
        self.digest.record(key, payload);
        self.bytes += (key.len() + raw.len()) as u64;
        Ok(closed)
    }

    /// The last range, unbounded above. An empty CF is one range `(-∞, +∞)` with no keys.
    pub(crate) fn finish(self) -> LocalRange {
        let (count, hash) = self.digest.finish_range();
        LocalRange {
            bounds: Bounds {
                lower: self.lower,
                upper: None,
            },
            count,
            hash,
        }
    }
}

/// Keeps a reader at or below `rate` bytes per second.
pub(crate) struct Throttle {
    rate: Option<u64>,
    start: Instant,
    bytes: u64,
}

impl Throttle {
    pub(crate) fn new(rate: Option<u64>) -> Self {
        Throttle {
            rate,
            start: Instant::now(),
            bytes: 0,
        }
    }

    /// Accounts for `n` more bytes read; returns how long to pause.
    pub(crate) fn consume(&mut self, n: u64) -> Duration {
        let elapsed = self.start.elapsed();
        self.consume_at(n, elapsed)
    }

    fn consume_at(&mut self, n: u64, elapsed: Duration) -> Duration {
        let Some(rate) = self.rate else {
            return Duration::ZERO;
        };
        self.bytes = self.bytes.saturating_add(n);
        Duration::from_secs_f64(self.bytes as f64 / rate as f64).saturating_sub(elapsed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope::encode;

    fn hex32(s: &str) -> [u8; 32] {
        let v: Vec<u8> = (0..32)
            .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    #[test]
    fn range_hashes_match_the_v3_vectors() {
        let env = encode(1, None, b"v1");
        let mut quick = RangeHasher::new(VerifyMode::Quick);
        quick.add(b"k1", &env).unwrap();
        assert_eq!(
            quick.finish(),
            (
                1,
                hex32("2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3")
            )
        );
        let mut full = RangeHasher::new(VerifyMode::Full);
        full.add(b"k1", &env).unwrap();
        assert_eq!(
            full.finish(),
            (
                1,
                hex32("14e556f28a8301390e9be56ecb4ec7ff3ceea3cd48316ab7f7f0488117e53ecd")
            )
        );
        assert_eq!(
            RangeHasher::new(VerifyMode::Full).finish(),
            (
                0,
                hex32("af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            )
        );
        // The local side hashes the same records to the same value.
        let rec = LocalRecord::new(b"k1", &env, VerifyMode::Quick).unwrap();
        assert_eq!(
            local_range(Bounds::default(), &[rec]).hash,
            hex32("2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3")
        );
    }

    #[test]
    fn quick_payload_is_the_header_with_or_without_ttl() {
        let plain = encode(7, None, b"value");
        let ttl = encode(7, Some(99), b"value");
        assert_eq!(payload(&plain, VerifyMode::Quick).unwrap(), &plain[..10]);
        assert_eq!(payload(&ttl, VerifyMode::Quick).unwrap(), &ttl[..18]);
        assert_eq!(payload(&ttl, VerifyMode::Full).unwrap(), &ttl[..]);
        assert!(payload(b"short", VerifyMode::Quick).is_err());
    }

    #[test]
    fn empty_key_bound_is_not_unbounded() {
        let o = KeyOrder::bytewise();
        let below_empty = Bounds {
            lower: None,
            upper: Some(vec![]),
        };
        assert!(
            !below_empty.contains(&o, b""),
            "nothing sorts before the empty key"
        );
        let from_empty = Bounds {
            lower: Some(vec![]),
            upper: None,
        };
        assert!(from_empty.contains(&o, b"") && from_empty.contains(&o, b"\0"));
        let only_empty = Bounds {
            lower: Some(vec![]),
            upper: Some(vec![0]),
        };
        assert!(only_empty.contains(&o, b"") && !only_empty.contains(&o, b"\0"));
        let back = Bounds::from_pb(&only_empty.to_pb("c"));
        assert_eq!(back, only_empty);
    }

    #[test]
    fn an_unregistered_comparator_is_an_error() {
        let reg = ComparatorRegistry::default();
        assert!(KeyOrder::new(&reg, Some("nope")).is_err());
        assert!(KeyOrder::new(&reg, None).is_ok());
    }

    #[test]
    fn custom_order_decides_containment() {
        let mut reg = ComparatorRegistry::default();
        reg.register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let o = KeyOrder::new(&reg, Some("reverse")).unwrap();
        let b = Bounds {
            lower: Some(b"m".to_vec()),
            upper: Some(b"c".to_vec()),
        };
        assert!(b.contains(&o, b"m") && b.contains(&o, b"d"));
        assert!(!b.contains(&o, b"c") && !b.contains(&o, b"z"));
    }

    fn keys(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                (
                    format!("k{i:03}").into_bytes(),
                    encode(i as u64 + 1, None, &[i as u8; 10]),
                )
            })
            .collect()
    }

    fn chunk(
        records: &[(Vec<u8>, Vec<u8>)],
        chunk_keys: usize,
        chunk_bytes: usize,
    ) -> Vec<LocalRange> {
        let mut c = Chunker::new(VerifyMode::Full, chunk_keys, chunk_bytes);
        let mut out = Vec::new();
        for (k, v) in records {
            out.extend(c.push(k, v).unwrap());
        }
        out.push(c.finish());
        out
    }

    #[test]
    fn chunks_are_contiguous_and_cover_everything() {
        let recs = keys(25);
        let ranges = chunk(&recs, 10, usize::MAX);
        assert_eq!(
            ranges.iter().map(|r| r.count).collect::<Vec<_>>(),
            vec![10, 10, 5]
        );
        assert_eq!(ranges[0].bounds.lower, None);
        assert_eq!(ranges[2].bounds.upper, None);
        for w in ranges.windows(2) {
            assert_eq!(
                w[0].bounds.upper, w[1].bounds.lower,
                "no gap between ranges"
            );
        }
        assert_eq!(ranges[1].bounds.lower.as_deref(), Some(&b"k010"[..]));
        // Each range hashes exactly its own records.
        let rec: Vec<LocalRecord> = recs
            .iter()
            .map(|(k, v)| LocalRecord::new(k, v, VerifyMode::Full).unwrap())
            .collect();
        assert_eq!(
            local_range(ranges[1].bounds.clone(), &rec[10..20]),
            ranges[1]
        );
    }

    #[test]
    fn chunk_bytes_closes_ranges_and_an_empty_cf_is_one_range() {
        let recs = keys(6); // 24 bytes per record: a 4-byte key and a 20-byte envelope
        let ranges = chunk(&recs, usize::MAX, 48);
        assert_eq!(
            ranges.iter().map(|r| r.count).collect::<Vec<_>>(),
            vec![2, 2, 2]
        );
        let empty = Chunker::new(VerifyMode::Quick, 10, 10).finish();
        assert_eq!(
            empty,
            LocalRange {
                bounds: Bounds::default(),
                count: 0,
                hash: *blake3::hash(b"").as_bytes()
            }
        );
    }

    #[test]
    fn oversized_keys_never_become_boundaries() {
        let big = vec![b'b'; MAX_BOUNDARY_KEY_BYTES + 1];
        let recs = vec![
            (b"a".to_vec(), encode(1, None, b"")),
            (big.clone(), encode(2, None, b"")),
            (b"c".to_vec(), encode(3, None, b"")),
        ];
        let ranges = chunk(&recs, 1, usize::MAX);
        assert_eq!(ranges.len(), 2);
        assert_eq!(
            ranges[0].bounds.upper.as_deref(),
            Some(&b"c"[..]),
            "the long key was skipped as a boundary"
        );
        assert_eq!(ranges[0].count, 2);
        let exactly = vec![b'b'; MAX_BOUNDARY_KEY_BYTES];
        let recs = vec![
            (b"a".to_vec(), encode(1, None, b"")),
            (exactly.clone(), encode(2, None, b"")),
        ];
        assert_eq!(
            chunk(&recs, 1, usize::MAX)[0].bounds.upper,
            Some(exactly),
            "1 MiB keys are fine"
        );
    }

    #[test]
    fn listings_respect_limit_and_soft_size_but_keep_two_keys() {
        let mut l = Lister::new(VerifyMode::Quick, 3, usize::MAX);
        for (k, v) in keys(5) {
            if !l.offer(&k, &v).unwrap() {
                break;
            }
        }
        assert_eq!((l.keys.len(), l.truncated), (3, true));
        assert_eq!(l.keys[1].write_seq, 2);
        assert_eq!(
            l.keys[1].payload_hash,
            blake3::hash(&encode(2, None, &[1; 10])[..10])
                .as_bytes()
                .to_vec()
        );

        let mut l = Lister::new(VerifyMode::Full, 1024, 1);
        for (k, v) in keys(5) {
            if !l.offer(&k, &v).unwrap() {
                break;
            }
        }
        assert_eq!(
            (l.keys.len(), l.truncated),
            (2, true),
            "two keys even past the soft size"
        );

        let mut l = Lister::new(VerifyMode::Full, 1024, usize::MAX);
        for (k, v) in keys(2) {
            assert!(l.offer(&k, &v).unwrap());
        }
        assert!(!l.truncated);
    }

    #[test]
    fn throttle_paces_reads() {
        let mut t = Throttle::new(Some(1_000));
        assert_eq!(
            t.consume_at(500, Duration::ZERO),
            Duration::from_millis(500)
        );
        assert_eq!(
            t.consume_at(500, Duration::from_millis(600)),
            Duration::from_millis(400)
        );
        assert_eq!(t.consume_at(0, Duration::from_secs(5)), Duration::ZERO);
        assert_eq!(
            Throttle::new(None).consume_at(u64::MAX, Duration::ZERO),
            Duration::ZERO
        );
    }
}
