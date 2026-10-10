//! The comparison a slave runs for each CF (PROTOCOL.md §9.2, §9.4): hash every local range
//! upstream, `VERIFY_WINDOW` requests at a time; bisect each mismatched range at its local
//! median key down to `min_chunk_keys`; then list the upstream keys of that range (paging
//! through truncated answers) and diff them key by key against the local records.
//!
//! Generic over the upstream ([`Oracle`]) and the local data ([`LocalSide`]), so the logic is
//! unit-tested without a network.

use std::cmp::Ordering;

use tokio::sync::mpsc;

use super::range::{Bounds, KeyOrder, LocalRange, LocalRecord, local_range};
use super::{
    CfVersion, MAX_BOUNDARY_KEY_BYTES, MAX_LIST_LIMIT, MAX_RESUME_KEY_BYTES, Mismatch,
    VERIFY_WINDOW,
};
use crate::error::{Error, Result};
use crate::pb;
use crate::types::CfEntry;

/// The upstream side of one session.
pub(crate) trait Oracle {
    /// Sends one `HashRange` per range (all before reading any answer) and returns `matched`
    /// for each, in order.
    async fn hash(&mut self, cf: &str, ranges: &[LocalRange]) -> Result<Vec<bool>>;
    /// One `ListRange`: the keys of `bounds` (up to `limit`) and whether there are more.
    async fn list(
        &mut self,
        cf: &str,
        bounds: &Bounds,
        limit: u32,
    ) -> Result<(Vec<pb::KeyDigest>, bool)>;
}

/// The local side: the records of a range, read from the same snapshot as the ranges.
/// Implementations over `scan::load_range` block the thread: run them in `spawn_blocking`.
pub(crate) trait LocalSide {
    async fn load(&self, bounds: &Bounds) -> Result<Vec<LocalRecord>>;
}

/// How many more [`Mismatch::Key`] a run may report.
#[derive(Debug)]
pub(crate) struct Budget(usize);

impl Budget {
    pub(crate) fn new(max_keys: usize) -> Self {
        Budget(max_keys)
    }

    fn take(&mut self) -> bool {
        if self.0 == 0 {
            return false;
        }
        self.0 -= 1;
        true
    }
}

/// One CF being compared.
pub(crate) struct CfRun<'a> {
    pub cf: &'a str,
    pub order: KeyOrder,
    pub min_chunk_keys: usize,
    pub budget: &'a mut Budget,
    pub out: &'a mut Vec<Mismatch>,
}

impl CfRun<'_> {
    /// Reports a key difference; `false` once the budget is spent.
    fn key(&mut self, key: &[u8], local: Option<u64>, upstream: Option<u64>) -> bool {
        if !self.budget.take() {
            return false;
        }
        self.out.push(Mismatch::Key {
            cf: self.cf.to_string(),
            key: key.to_vec(),
            local,
            upstream,
        });
        true
    }

    fn range(&mut self, lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) {
        self.out.push(Mismatch::Range {
            cf: self.cf.to_string(),
            lower,
            upper,
        });
    }
}

/// Compares one CF. `ranges` yields the local ranges in order (contiguous, covering the CF).
pub(crate) async fn compare_cf<O: Oracle, L: LocalSide>(
    oracle: &mut O,
    local: &L,
    run: &mut CfRun<'_>,
    ranges: &mut mpsc::Receiver<Result<LocalRange>>,
) -> Result<()> {
    let mut mismatched = Vec::new();
    let mut batch = Vec::with_capacity(VERIFY_WINDOW);
    loop {
        let done = match ranges.recv().await {
            Some(r) => {
                batch.push(r?);
                false
            }
            None => true,
        };
        if batch.len() == VERIFY_WINDOW || (done && !batch.is_empty()) {
            let matched = oracle.hash(run.cf, &batch).await?;
            if matched.len() != batch.len() {
                return Err(Error::corrupt(format!(
                    "verify upstream answered {} of {} hash requests",
                    matched.len(),
                    batch.len()
                )));
            }
            for (r, ok) in batch.drain(..).zip(matched) {
                if !ok {
                    mismatched.push(r.bounds);
                }
            }
        }
        if done {
            break;
        }
    }
    for bounds in mismatched {
        let records = local.load(&bounds).await?;
        let before = run.out.len();
        let (lower, upper) = (bounds.lower.clone(), bounds.upper.clone());
        narrow(oracle, run, bounds, &records).await?;
        if run.out.len() == before {
            // The hash said "differs" but the key diff found nothing: never end clean.
            run.range(lower, upper);
        }
    }
    Ok(())
}

/// The index to split `records` at: the key nearest the median that can be a boundary.
/// `None` when the range is small enough to list.
fn split_point(records: &[LocalRecord], min_chunk_keys: usize) -> Option<usize> {
    if records.len() <= min_chunk_keys.max(1) {
        return None;
    }
    let mid = records.len() / 2;
    (0..records.len())
        .flat_map(|d| [mid.checked_sub(d), mid.checked_add(d)])
        .flatten()
        .filter(|&i| i >= 1 && i < records.len())
        .find(|&i| records[i].key.len() <= MAX_BOUNDARY_KEY_BYTES)
}

/// Bisects a mismatched range at local median keys, hashing both halves, until the
/// mismatched parts are small enough to list.
async fn narrow<O: Oracle>(
    oracle: &mut O,
    run: &mut CfRun<'_>,
    bounds: Bounds,
    records: &[LocalRecord],
) -> Result<()> {
    let mut stack = vec![(bounds, 0..records.len())];
    while let Some((bounds, idx)) = stack.pop() {
        let recs = &records[idx.clone()];
        let Some(i) = split_point(recs, run.min_chunk_keys) else {
            list_and_diff(oracle, run, &bounds, recs).await?;
            continue;
        };
        let at = recs[i].key.clone();
        let left = local_range(
            Bounds {
                lower: bounds.lower.clone(),
                upper: Some(at.clone()),
            },
            &recs[..i],
        );
        let right = local_range(
            Bounds {
                lower: Some(at),
                upper: bounds.upper.clone(),
            },
            &recs[i..],
        );
        let matched = oracle.hash(run.cf, &[left.clone(), right.clone()]).await?;
        if matched.len() != 2 {
            return Err(Error::corrupt(format!(
                "verify upstream answered {} of 2 hash requests",
                matched.len()
            )));
        }
        if matched[0] && matched[1] {
            // The node mismatched, yet both halves match: nothing to narrow, never clean.
            run.range(bounds.lower, bounds.upper);
            continue;
        }
        // Right first, so the left half is narrowed (and reported) first.
        if !matched[1] {
            stack.push((right.bounds, idx.start + i..idx.end));
        }
        if !matched[0] {
            stack.push((left.bounds, idx.start..idx.start + i));
        }
    }
    Ok(())
}

/// Lists the upstream keys of `bounds` and diffs them against `local` (the local records of
/// the same range, in order). A truncated listing is continued with `lower` = its last key,
/// whose repeat at the start of the next answer is skipped; this works under any comparator.
async fn list_and_diff<O: Oracle>(
    oracle: &mut O,
    run: &mut CfRun<'_>,
    bounds: &Bounds,
    local: &[LocalRecord],
) -> Result<()> {
    let before = run.out.len();
    let mut i = 0;
    let mut page = bounds.clone();
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let (keys, truncated) = oracle.list(run.cf, &page, MAX_LIST_LIMIT).await?;
        let mut last: Option<Vec<u8>> = None;
        for u in keys {
            if resume.as_deref() == Some(u.key.as_slice()) {
                continue;
            }
            // Local keys before `u` exist only locally.
            // Keys are paired by their bytes; the comparator only drives the merge order, so two
            // keys it ranks equal but whose bytes differ are one local-only and one upstream-only.
            while i < local.len() && precedes(&run.order, &local[i].key, &u.key) {
                if !run.key(&local[i].key, Some(local[i].write_seq), None) {
                    run.range(Some(local[i].key.clone()), bounds.upper.clone());
                    return Ok(());
                }
                i += 1;
            }
            let reported = if i < local.len() && local[i].key == u.key {
                let l = &local[i];
                i += 1;
                let same = l.write_seq == u.write_seq
                    && blake3::hash(&l.payload).as_bytes()[..] == u.payload_hash[..];
                same || run.key(&u.key, Some(l.write_seq), Some(u.write_seq))
            } else {
                run.key(&u.key, None, Some(u.write_seq))
            };
            if !reported {
                run.range(Some(u.key.clone()), bounds.upper.clone());
                return Ok(());
            }
            last = Some(u.key);
        }
        if !truncated {
            break;
        }
        // Continue after the last key, unless that cannot make progress or the key is too
        // long to send back; then the rest of the range is reported as a whole.
        match last {
            Some(k) if k.len() <= MAX_RESUME_KEY_BYTES => {
                page.lower = Some(k.clone());
                resume = Some(k);
            }
            other => {
                run.range(other.or_else(|| page.lower.clone()), bounds.upper.clone());
                return Ok(());
            }
        }
    }
    while i < local.len() {
        if !run.key(&local[i].key, Some(local[i].write_seq), None) {
            run.range(Some(local[i].key.clone()), bounds.upper.clone());
            return Ok(());
        }
        i += 1;
    }
    if run.out.len() == before {
        // The leaf's hash differed, yet its listing shows no difference: never clean.
        run.range(bounds.lower.clone(), bounds.upper.clone());
    }
    Ok(())
}

/// True when local key `l` must be emitted before upstream key `u`: it sorts first, or the
/// order ranks them equal although their bytes differ.
fn precedes(order: &KeyOrder, l: &[u8], u: &[u8]) -> bool {
    match order.cmp(l, u) {
        Ordering::Less => true,
        Ordering::Equal => l != u,
        Ordering::Greater => false,
    }
}

fn version(e: &CfEntry) -> CfVersion {
    CfVersion {
        spec: e.spec.clone(),
        create_seq: e.create_seq,
    }
}

/// Compares the CF lists (both sorted by name). Returns the differences and the CFs whose
/// data can be compared: present on both sides with the same comparator.
pub(crate) fn compare_specs(
    local: &[CfEntry],
    upstream: &[CfEntry],
) -> (Vec<Mismatch>, Vec<CfEntry>) {
    let mut names: Vec<&str> = local
        .iter()
        .chain(upstream)
        .map(|e| e.spec.name.as_str())
        .collect();
    names.sort_unstable();
    names.dedup();
    let mut mismatches = Vec::new();
    let mut common = Vec::new();
    for name in names {
        let l = local.iter().find(|e| e.spec.name == name);
        let u = upstream.iter().find(|e| e.spec.name == name);
        if l != u {
            mismatches.push(Mismatch::CfSpec {
                name: name.to_string(),
                local: l.map(version),
                upstream: u.map(version),
            });
        }
        if let (Some(l), Some(u)) = (l, u) {
            if l.spec.comparator == u.spec.comparator {
                common.push(l.clone());
            }
        }
    }
    (mismatches, common)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope::encode;
    use crate::types::CfSpec;
    use crate::verify::VerifyMode;
    use crate::verify::range::{Chunker, Lister, RangeHasher};

    type Data = Vec<(Vec<u8>, Vec<u8>)>;

    /// An upstream over in-memory records (sorted by `order`), answering like the server.
    struct Fake {
        order: KeyOrder,
        mode: VerifyMode,
        data: Data,
        soft_bytes: usize,
        hash_batches: Vec<usize>,
        lists: Vec<Bounds>,
        lie: bool,
        /// Hashes of ranges that hold this key and at least this many keys "mismatch".
        lie_about: Option<(Vec<u8>, u64)>,
        short_from: Option<usize>,
    }

    impl Fake {
        fn new(data: Data, mode: VerifyMode) -> Self {
            Fake {
                order: KeyOrder::bytewise(),
                mode,
                data,
                soft_bytes: usize::MAX,
                hash_batches: Vec::new(),
                lists: Vec::new(),
                lie: false,
                lie_about: None,
                short_from: None,
            }
        }

        fn in_range<'a>(
            &'a self,
            b: &'a Bounds,
        ) -> impl Iterator<Item = &'a (Vec<u8>, Vec<u8>)> + 'a {
            self.data
                .iter()
                .filter(move |(k, _)| b.contains(&self.order, k))
        }
    }

    impl Oracle for Fake {
        async fn hash(&mut self, _cf: &str, ranges: &[LocalRange]) -> Result<Vec<bool>> {
            self.hash_batches.push(ranges.len());
            let call = self.hash_batches.len() - 1;
            let mut v: Vec<bool> = ranges
                .iter()
                .map(|r| {
                    let mut h = RangeHasher::new(self.mode);
                    for (k, v) in self.in_range(&r.bounds) {
                        h.add(k, v).unwrap();
                    }
                    let lies = self.lie
                        || self.lie_about.as_ref().is_some_and(|(k, n)| {
                            r.count >= *n && r.bounds.contains(&self.order, k)
                        });
                    !lies && h.finish() == (r.count, r.hash)
                })
                .collect();
            if self.short_from.is_some_and(|n| call >= n) {
                v.pop();
            }
            Ok(v)
        }

        async fn list(
            &mut self,
            _cf: &str,
            b: &Bounds,
            limit: u32,
        ) -> Result<(Vec<pb::KeyDigest>, bool)> {
            self.lists.push(b.clone());
            let mut l = Lister::new(self.mode, limit as usize, self.soft_bytes);
            for (k, v) in self.in_range(b) {
                if !l.offer(k, v).unwrap() {
                    break;
                }
            }
            Ok((l.keys, l.truncated))
        }
    }

    struct Local {
        order: KeyOrder,
        mode: VerifyMode,
        data: Data,
    }

    impl LocalSide for Local {
        async fn load(&self, b: &Bounds) -> Result<Vec<LocalRecord>> {
            Ok(self
                .data
                .iter()
                .filter(|(k, _)| b.contains(&self.order, k))
                .map(|(k, v)| LocalRecord::new(k, v, self.mode).unwrap())
                .collect())
        }
    }

    fn data(n: usize) -> Data {
        (0..n)
            .map(|i| {
                (
                    format!("k{i:04}").into_bytes(),
                    encode(i as u64 + 1, None, format!("v{i}").as_bytes()),
                )
            })
            .collect()
    }

    /// Runs the comparison of one CF with `chunk_keys`, `min_chunk_keys` and a key budget.
    async fn run(
        local: Data,
        upstream: &mut Fake,
        chunk_keys: usize,
        min_chunk_keys: usize,
        budget: usize,
    ) -> Vec<Mismatch> {
        try_run(local, upstream, chunk_keys, min_chunk_keys, budget)
            .await
            .unwrap()
    }

    async fn try_run(
        local: Data,
        upstream: &mut Fake,
        chunk_keys: usize,
        min_chunk_keys: usize,
        budget: usize,
    ) -> Result<Vec<Mismatch>> {
        let mode = upstream.mode;
        let (tx, mut rx) = mpsc::channel(4);
        let ranges = {
            let mut c = Chunker::new(mode, chunk_keys, usize::MAX);
            let mut out = Vec::new();
            for (k, v) in &local {
                out.extend(c.push(k, v).unwrap());
            }
            out.push(c.finish());
            out
        };
        tokio::spawn(async move {
            for r in ranges {
                tx.send(Ok(r)).await.unwrap();
            }
        });
        let side = Local {
            order: upstream.order.clone(),
            mode,
            data: local,
        };
        let mut budget = Budget::new(budget);
        let mut out = Vec::new();
        let mut cf = CfRun {
            cf: "c",
            order: upstream.order.clone(),
            min_chunk_keys,
            budget: &mut budget,
            out: &mut out,
        };
        compare_cf(upstream, &side, &mut cf, &mut rx).await?;
        Ok(out)
    }

    fn key(k: &str, local: Option<u64>, upstream: Option<u64>) -> Mismatch {
        Mismatch::Key {
            cf: "c".into(),
            key: k.as_bytes().to_vec(),
            local,
            upstream,
        }
    }

    #[tokio::test]
    async fn identical_data_needs_only_the_pipelined_hashes() {
        let mut up = Fake::new(data(1000), VerifyMode::Full);
        let out = run(data(1000), &mut up, 10, 4, 100).await;
        assert!(out.is_empty(), "{out:?}");
        assert_eq!(
            up.hash_batches,
            vec![16; 6].into_iter().chain([4]).collect::<Vec<_>>(),
            "100 ranges, 16 per window"
        );
        assert!(up.lists.is_empty());
    }

    #[tokio::test]
    async fn a_key_only_upstream_between_boundaries_is_found_by_bisection() {
        let mut upstream = data(1000);
        upstream.insert(455, (b"k0454a".to_vec(), encode(7, None, b"x")));
        let mut up = Fake::new(upstream, VerifyMode::Quick);
        let out = run(data(1000), &mut up, 100, 4, 100).await;
        assert_eq!(out, vec![key("k0454a", None, Some(7))]);
        assert_eq!(up.lists.len(), 1, "only the narrowed leaf is listed");
        let leaf = &up.lists[0];
        let local_in_leaf = data(1000)
            .iter()
            .filter(|(k, _)| leaf.contains(&KeyOrder::bytewise(), k))
            .count();
        assert!(
            local_in_leaf <= 4,
            "the listed range holds {local_in_leaf} local keys"
        );
        assert!(leaf.contains(&KeyOrder::bytewise(), b"k0454a"));
    }

    #[tokio::test]
    async fn missing_extra_and_changed_keys_including_the_open_ended_ranges() {
        let local = data(300);
        let mut upstream = data(300);
        upstream.remove(299); // last key: extra locally (range without upper bound)
        upstream.remove(150); // middle key: extra locally
        upstream.remove(0); // first key: extra locally (range without lower bound)
        upstream.push((b"k9999".to_vec(), encode(5, None, b"new"))); // only upstream, past every local key
        upstream.insert(0, (b"a".to_vec(), encode(6, None, b"new"))); // only upstream, before every local key
        upstream[100].1 = encode(100, None, b"stale"); // k0100: an older write_seq upstream
        let mut full = Fake::new(upstream.clone(), VerifyMode::Full);
        let out = run(local.clone(), &mut full, 32, 4, 100).await;
        assert_eq!(
            out,
            vec![
                key("a", None, Some(6)),
                key("k0000", Some(1), None),
                key("k0100", Some(101), Some(100)),
                key("k0150", Some(151), None),
                key("k0299", Some(300), None),
                key("k9999", None, Some(5)),
            ]
        );
        // Quick does not see the value change, only the write_seq.
        let mut quick = Fake::new(upstream, VerifyMode::Quick);
        let out = run(local, &mut quick, 32, 4, 100).await;
        assert_eq!(out.len(), 6, "{out:?}");
    }

    #[tokio::test]
    async fn equal_write_seq_with_another_value_is_only_seen_in_full_mode() {
        let mut upstream = data(50);
        upstream[20].1 = encode(21, None, b"tampered");
        let mut quick = Fake::new(upstream.clone(), VerifyMode::Quick);
        assert!(run(data(50), &mut quick, 8, 2, 100).await.is_empty());
        let mut full = Fake::new(upstream, VerifyMode::Full);
        assert_eq!(
            run(data(50), &mut full, 8, 2, 100).await,
            vec![key("k0020", Some(21), Some(21))]
        );
    }

    #[tokio::test]
    async fn listings_are_paged_without_repeats_and_the_budget_ends_in_a_range() {
        // Locally empty: one range (-inf, +inf), 3000 keys only upstream, listed in pages of 1024.
        let mut up = Fake::new(data(3000), VerifyMode::Quick);
        let out = run(Vec::new(), &mut up, 10, 4, 10_000).await;
        assert_eq!(out.len(), 3000);
        assert!(
            out.iter()
                .enumerate()
                .all(|(i, m)| *m == key(&format!("k{i:04}"), None, Some(i as u64 + 1)))
        );
        assert_eq!(up.lists.len(), 3);
        assert_eq!(
            up.lists[1].lower.as_deref(),
            Some(&b"k1023"[..]),
            "the next page starts at the last key"
        );

        let mut up = Fake::new(data(3000), VerifyMode::Quick);
        let out = run(Vec::new(), &mut up, 10, 4, 100).await;
        assert_eq!(out.len(), 101);
        assert_eq!(
            out[100],
            Mismatch::Range {
                cf: "c".into(),
                lower: Some(b"k0100".to_vec()),
                upper: None
            }
        );
    }

    #[tokio::test]
    async fn a_listing_that_cannot_progress_is_reported_as_a_range() {
        // Each page is cut after 2 keys by size; a page that only repeats the resume key
        // cannot happen with a conforming server, so paging always advances.
        let mut up = Fake::new(data(7), VerifyMode::Full);
        up.soft_bytes = 1;
        let out = run(Vec::new(), &mut up, 10, 4, 100).await;
        assert_eq!(out.len(), 7);
        assert_eq!(
            up.lists.len(),
            6,
            "2 new keys on the first page, 1 on each later page"
        );

        // A key too long to send back as a lower bound ends the listing with a range.
        let big = vec![b'k'; MAX_RESUME_KEY_BYTES + 1];
        let rows = vec![
            (b"a".to_vec(), encode(1, None, b"")),
            (big.clone(), encode(2, None, b"")),
            (b"z".to_vec(), encode(3, None, b"")),
        ];
        let mut up = Fake::new(rows, VerifyMode::Quick);
        up.soft_bytes = 1;
        let out = run(Vec::new(), &mut up, 10, 4, 100).await;
        assert_eq!(
            out,
            vec![
                key("a", None, Some(1)),
                Mismatch::Key {
                    cf: "c".into(),
                    key: big.clone(),
                    local: None,
                    upstream: Some(2)
                },
                Mismatch::Range {
                    cf: "c".into(),
                    lower: Some(big),
                    upper: None
                },
            ]
        );
    }

    fn ci() -> KeyOrder {
        let mut reg = crate::tuning::ComparatorRegistry::default();
        reg.register("ci", |a: &[u8], b: &[u8]| {
            a.to_ascii_lowercase().cmp(&b.to_ascii_lowercase())
        });
        KeyOrder::new(&reg, Some("ci")).unwrap()
    }

    #[tokio::test]
    async fn keys_the_comparator_ranks_equal_but_with_other_bytes_are_reported() {
        let rec = |k: &str| (k.as_bytes().to_vec(), encode(5, None, b"v"));
        let mut up = Fake::new(vec![rec("user:a")], VerifyMode::Full);
        up.order = ci();
        let out = run(vec![rec("user:A")], &mut up, 10, 4, 100).await;
        assert_eq!(
            out,
            vec![key("user:A", Some(5), None), key("user:a", None, Some(5))]
        );
    }

    #[tokio::test]
    async fn a_hash_mismatch_that_the_diff_cannot_explain_is_a_range() {
        let mut up = Fake::new(data(40), VerifyMode::Full);
        up.lie = true; // every hash "mismatches", yet the listings are identical
        let out = run(data(40), &mut up, 100, 4, 100).await;
        // Every leaf is reported as a range (T3-N1): together they cover the whole CF.
        let bounds: Vec<_> = out
            .iter()
            .map(|m| match m {
                Mismatch::Range { lower, upper, .. } => (lower.clone(), upper.clone()),
                other => panic!("{other:?}"),
            })
            .collect();
        assert!(bounds.len() > 1, "{out:?}");
        assert_eq!(bounds.first().unwrap().0, None);
        assert_eq!(bounds.last().unwrap().1, None);
        assert!(
            bounds
                .windows(2)
                .all(|w| w[0].1.is_some() && w[0].1 == w[1].0),
            "contiguous: {out:?}"
        );
    }

    /// Final review T3-N1: an unexplained mismatch is reported where it was seen, per node,
    /// not only as the whole top-level range: a leaf whose listing shows no difference, and a
    /// node whose two halves both hash equal although it did not.
    #[tokio::test]
    async fn an_unexplained_mismatch_is_reported_as_the_node_that_showed_it() {
        let bound = |k: &str| Some(k.as_bytes().to_vec());
        // A leaf: every range holding k0005 mismatches, down to the listed leaf.
        let mut up = Fake::new(data(40), VerifyMode::Full);
        up.lie_about = Some((b"k0005".to_vec(), 1));
        let out = run(data(40), &mut up, 100, 4, 100).await;
        assert_eq!(out.len(), 1, "{out:?}");
        let Mismatch::Range { lower, upper, .. } = &out[0] else {
            panic!("{out:?}");
        };
        assert!(
            (lower.is_some() || upper.is_some()) && upper.as_deref() <= Some(&b"k0010"[..]),
            "a sub-range around k0005, not the whole CF: {out:?}"
        );
        // A node: ranges of 20+ keys holding k0005 mismatch; the two halves of the left half
        // (10 keys each) hash equal.
        let mut up = Fake::new(data(40), VerifyMode::Full);
        up.lie_about = Some((b"k0005".to_vec(), 20));
        let out = run(data(40), &mut up, 100, 4, 100).await;
        assert_eq!(
            out,
            vec![Mismatch::Range {
                cf: "c".into(),
                lower: None,
                upper: bound("k0020"),
            }]
        );
    }

    #[tokio::test]
    async fn a_short_oracle_answer_is_an_error_not_a_match() {
        let mut up = Fake::new(data(40), VerifyMode::Full);
        up.short_from = Some(0);
        assert!(try_run(data(40), &mut up, 10, 4, 100).await.is_err());
        // Short only while narrowing (the first window is answered in full).
        let mut upstream = data(40);
        upstream[5].1 = encode(99, None, b"x");
        let mut up = Fake::new(upstream, VerifyMode::Full);
        up.short_from = Some(1);
        assert!(try_run(data(40), &mut up, 10, 2, 100).await.is_err());
    }

    #[test]
    fn split_points_avoid_long_keys_and_small_ranges() {
        let rec = |k: Vec<u8>| LocalRecord {
            key: k,
            write_seq: 1,
            payload: vec![],
        };
        let small: Vec<LocalRecord> = (0..4u8).map(|i| rec(vec![i])).collect();
        assert_eq!(split_point(&small, 4), None);
        assert_eq!(split_point(&small, 3), Some(2));
        let mut long = small.clone();
        long[2].key = vec![2; MAX_BOUNDARY_KEY_BYTES + 1];
        assert_eq!(
            split_point(&long, 1),
            Some(1),
            "nearest boundary-capable key to the median"
        );
        let all_long: Vec<LocalRecord> = (0..3u8)
            .map(|i| rec(vec![i; MAX_BOUNDARY_KEY_BYTES + 1]))
            .collect();
        assert_eq!(split_point(&all_long, 1), None);
    }

    #[test]
    fn cf_lists_are_compared_by_name_comparator_and_create_seq() {
        let e = |n: &str, cmp: Option<&str>, seq| CfEntry {
            spec: CfSpec {
                name: n.into(),
                comparator: cmp.map(str::to_string),
            },
            create_seq: seq,
        };
        let local = vec![
            e("a", None, 1),
            e("b", None, 2),
            e("c", Some("rev"), 3),
            e("only-local", None, 4),
        ];
        let upstream = vec![
            e("a", None, 1),
            e("b", None, 9),
            e("c", None, 3),
            e("only-up", None, 5),
        ];
        let (m, common) = compare_specs(&local, &upstream);
        let names: Vec<&str> = m
            .iter()
            .map(|m| match m {
                Mismatch::CfSpec { name, .. } => name.as_str(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(names, vec!["b", "c", "only-local", "only-up"]);
        assert_eq!(
            common
                .iter()
                .map(|e| e.spec.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(
            m[2],
            Mismatch::CfSpec {
                name: "only-local".into(),
                local: Some(CfVersion {
                    spec: CfSpec::new("only-local"),
                    create_seq: 4
                }),
                upstream: None
            }
        );
    }
}
