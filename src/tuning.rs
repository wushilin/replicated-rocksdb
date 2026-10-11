//! RocksDB tuning. Callers get a restricted [`Tunable`] instead of raw `rocksdb::Options`,
//! so merge operators, compaction filters and native TTL cannot be configured at all.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rocksdb::{BlockBasedOptions, DBCompactionStyle, DBCompressionType, Options};

use crate::error::{Error, Result};
use crate::types::CfSpec;

pub type CompareFn = dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync;

/// Named key orderings. A name must map to the same ordering on every node, forever.
#[derive(Clone, Default)]
pub struct ComparatorRegistry {
    map: HashMap<String, Arc<CompareFn>>,
}

impl fmt::Debug for ComparatorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.map.keys()).finish()
    }
}

impl ComparatorRegistry {
    /// Registers `f` under `name`. Re-registering a name **replaces** the earlier ordering; a
    /// name must map to the same ordering forever, or existing data becomes unreadable.
    pub fn register(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
    ) -> &mut Self {
        self.map.insert(name.into(), Arc::new(f));
        self
    }

    pub fn contains(&self, name: &str) -> bool {
        self.map.contains_key(name)
    }

    pub(crate) fn get(&self, name: &str) -> Option<Arc<CompareFn>> {
        self.map.get(name).cloned()
    }

    /// Orders two keys of a CF. `None`, or a name that is not registered, means bytewise;
    /// callers validate names before data reaches this point.
    pub(crate) fn compare(&self, name: Option<&str>, a: &[u8], b: &[u8]) -> Ordering {
        match name.and_then(|n| self.map.get(n)) {
            Some(f) => f(a, b),
            None => a.cmp(b),
        }
    }
}

/// Which RocksDB options a tuning callback is being asked about.
#[derive(Clone, Copy, Debug)]
pub enum CfKind<'a> {
    Db,
    Meta,
    Changelog,
    TtlIndex,
    User(&'a CfSpec),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

/// RocksDB compaction style. Performance only; the log and the data are the same either way.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum CompactionStyle {
    /// Leveled compaction (the RocksDB default).
    Level,
    /// Universal (size-tiered) compaction.
    Universal,
}

/// The tuning knobs this crate allows. They affect performance only, never semantics.
/// There is deliberately no way to set a merge operator, compaction filter, native TTL or
/// prefix extractor:
///
/// - Merge operators are relative ops, which the log forbids.
/// - Compaction filters and native TTL would drop data on one node without a logged
///   `Delete`, so replicas would diverge silently. TTL is master-driven here.
/// - A prefix extractor lets RocksDB iterators skip keys outside the seek prefix unless
///   `total_order_seek` is on. Bootstrap, verify and `prefix_iter` assume total-order scans
///   and would silently skip keys.
///
/// ```compile_fail
/// fn f(t: &mut replicated_rocksdb::tuning::Tunable<'_>) {
///     t.set_merge_operator_associative("m", |_, _, _| None);
/// }
/// ```
///
/// ```compile_fail
/// fn f(t: &mut replicated_rocksdb::tuning::Tunable<'_>) {
///     t.set_compaction_filter("f", |_, _, _| rocksdb::compaction_filter::Decision::Keep);
/// }
/// ```
///
/// ```compile_fail
/// fn f(t: &mut replicated_rocksdb::tuning::Tunable<'_>) {
///     t.set_prefix_extractor(rocksdb::SliceTransform::create_fixed_prefix(3));
/// }
/// ```
///
/// ```compile_fail
/// fn f(t: &mut replicated_rocksdb::tuning::Tunable<'_>) {
///     t.set_ttl(60);
/// }
/// ```
///
/// ```compile_fail
/// fn f(t: &mut replicated_rocksdb::tuning::Tunable<'_>) {
///     t.set_periodic_compaction_seconds(60);
/// }
/// ```
pub struct Tunable<'a> {
    opts: &'a mut Options,
    /// Created on the first table setter, so untouched CFs keep RocksDB's default table.
    table: Option<BlockBasedOptions>,
}

impl Tunable<'_> {
    fn table(&mut self) -> &mut BlockBasedOptions {
        self.table.get_or_insert_with(BlockBasedOptions::default)
    }

    /// Block-based table: data block size in bytes.
    pub fn set_block_size(&mut self, bytes: usize) {
        self.table().set_block_size(bytes);
    }
    /// Block-based table: bloom filter with `bits_per_key` bits per key.
    pub fn set_bloom_filter(&mut self, bits_per_key: f64) {
        self.table().set_bloom_filter(bits_per_key, false);
    }
    /// Block-based table: keep index and filter blocks in the block cache.
    pub fn set_cache_index_and_filter_blocks(&mut self, v: bool) {
        self.table().set_cache_index_and_filter_blocks(v);
    }
    /// Compaction style.
    pub fn set_compaction_style(&mut self, style: CompactionStyle) {
        self.opts.set_compaction_style(match style {
            CompactionStyle::Level => DBCompactionStyle::Level,
            CompactionStyle::Universal => DBCompactionStyle::Universal,
        });
    }
    /// Number of L0 files that triggers a compaction.
    pub fn set_level_zero_file_num_compaction_trigger(&mut self, n: i32) {
        self.opts.set_level_zero_file_num_compaction_trigger(n);
    }
    /// Target size of the base level, in bytes.
    pub fn set_max_bytes_for_level_base(&mut self, bytes: u64) {
        self.opts.set_max_bytes_for_level_base(bytes);
    }
    /// Size levels dynamically from the last level.
    pub fn set_level_compaction_dynamic_level_bytes(&mut self, v: bool) {
        self.opts.set_level_compaction_dynamic_level_bytes(v);
    }
    /// Compression of the bottommost level.
    pub fn set_bottommost_compression(&mut self, c: Compression) {
        self.opts
            .set_bottommost_compression_type(compression_type(c));
    }
    /// Incrementally sync SST files every `bytes` written (0 disables).
    pub fn set_bytes_per_sync(&mut self, bytes: u64) {
        self.opts.set_bytes_per_sync(bytes);
    }
    /// Incrementally sync WAL files every `bytes` written (0 disables).
    pub fn set_wal_bytes_per_sync(&mut self, bytes: u64) {
        self.opts.set_wal_bytes_per_sync(bytes);
    }
    /// Total WAL size that forces memtable flushes (0 lets RocksDB choose).
    pub fn set_max_total_wal_size(&mut self, bytes: u64) {
        self.opts.set_max_total_wal_size(bytes);
    }
    pub fn set_write_buffer_size(&mut self, bytes: usize) {
        self.opts.set_write_buffer_size(bytes);
    }
    pub fn set_max_write_buffer_number(&mut self, n: i32) {
        self.opts.set_max_write_buffer_number(n);
    }
    pub fn set_target_file_size_base(&mut self, bytes: u64) {
        self.opts.set_target_file_size_base(bytes);
    }
    pub fn set_compression(&mut self, c: Compression) {
        self.opts.set_compression_type(compression_type(c));
    }
    pub fn set_max_background_jobs(&mut self, n: i32) {
        self.opts.set_max_background_jobs(n);
    }
    pub fn set_max_open_files(&mut self, n: i32) {
        self.opts.set_max_open_files(n);
    }
    pub fn increase_parallelism(&mut self, threads: i32) {
        self.opts.increase_parallelism(threads);
    }
}

fn compression_type(c: Compression) -> DBCompressionType {
    match c {
        Compression::None => DBCompressionType::None,
        Compression::Lz4 => DBCompressionType::Lz4,
        Compression::Zstd => DBCompressionType::Zstd,
    }
}

type OptionsFn = dyn Fn(CfKind<'_>, &mut Tunable<'_>) + Send + Sync;

/// RocksDB tuning for one node: named comparators plus an optional options callback.
#[derive(Clone, Default)]
pub struct RocksTuning {
    pub comparators: ComparatorRegistry,
    options: Option<Arc<OptionsFn>>,
}

impl fmt::Debug for RocksTuning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksTuning")
            .field("comparators", &self.comparators)
            .field("options", &self.options.is_some())
            .finish()
    }
}

impl RocksTuning {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_options(
        mut self,
        f: impl Fn(CfKind<'_>, &mut Tunable<'_>) + Send + Sync + 'static,
    ) -> Self {
        self.options = Some(Arc::new(f));
        self
    }

    pub(crate) fn db_options(&self) -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        if let Some(cb) = &self.options {
            cb(
                CfKind::Db,
                &mut Tunable {
                    opts: &mut opts,
                    table: None,
                },
            );
        }
        opts
    }

    pub(crate) fn cf_options(&self, kind: CfKind<'_>) -> Result<Options> {
        let mut opts = Options::default();
        if let CfKind::User(spec) = kind {
            if let Some(name) = &spec.comparator {
                let f = self
                    .comparators
                    .get(name)
                    .ok_or_else(|| Error::UnknownComparator(name.clone()))?;
                opts.set_comparator(name.as_str(), Box::new(move |a: &[u8], b: &[u8]| f(a, b)));
            }
        }
        let mut table = None;
        if let Some(cb) = &self.options {
            let mut t = Tunable {
                opts: &mut opts,
                table: None,
            };
            cb(kind, &mut t);
            table = t.table;
        }
        if let Some(t) = &table {
            opts.set_block_based_table_factory(t);
        }
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

    fn every_knob(t: &mut Tunable<'_>, style: CompactionStyle) {
        t.set_block_size(8 << 10);
        t.set_bloom_filter(10.0);
        t.set_cache_index_and_filter_blocks(true);
        t.set_compaction_style(style);
        t.set_level_zero_file_num_compaction_trigger(8);
        t.set_max_bytes_for_level_base(64 << 20);
        t.set_level_compaction_dynamic_level_bytes(true);
        t.set_bottommost_compression(Compression::Zstd);
        t.set_bytes_per_sync(1 << 20);
        t.set_wal_bytes_per_sync(1 << 20);
        t.set_max_total_wal_size(256 << 20);
    }

    #[test]
    fn every_knob_opens_and_round_trips() {
        use crate::read::ReadApi;
        use crate::store::test_support::{apply, open_store_with};
        use crate::types::Op;
        for style in [CompactionStyle::Level, CompactionStyle::Universal] {
            let t = RocksTuning::new().with_options(move |kind, t| {
                // Universal only on user CFs and the DB level; both kinds see every knob.
                let style = if matches!(kind, CfKind::User(_) | CfKind::Db) {
                    style
                } else {
                    CompactionStyle::Level
                };
                every_knob(t, style);
            });
            let dir = tempfile::tempdir().unwrap();
            let store = open_store_with(dir.path(), t);
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new("c"),
                }],
            )
            .unwrap();
            apply(
                &store,
                vec![Op::Put {
                    cf: "c".into(),
                    key: b"k".to_vec(),
                    value: b"v".to_vec(),
                    expire_at_ms: None,
                }],
            )
            .unwrap();
            let snap = store.read_snapshot(0).unwrap();
            assert_eq!(snap.get("c", b"k").unwrap().unwrap(), b"v");
        }
    }

    #[test]
    fn compare_uses_registered_comparator_or_bytewise() {
        let mut reg = ComparatorRegistry::default();
        reg.register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        assert!(reg.contains("reverse"));
        assert!(!reg.contains("other"));
        assert_eq!(reg.compare(None, b"a", b"b"), Ordering::Less);
        assert_eq!(reg.compare(Some("reverse"), b"a", b"b"), Ordering::Greater);
    }

    #[test]
    fn unknown_comparator_is_rejected() {
        let t = RocksTuning::new();
        let spec = CfSpec::new("c").with_comparator("missing");
        assert!(
            matches!(t.cf_options(CfKind::User(&spec)), Err(Error::UnknownComparator(n)) if n == "missing")
        );
        t.cf_options(CfKind::User(&CfSpec::new("plain"))).unwrap();
    }

    #[test]
    fn options_callback_sees_every_kind() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let t = RocksTuning::new().with_options(move |_kind, opts| {
            c.fetch_add(1, AtomicOrdering::SeqCst);
            opts.set_write_buffer_size(8 << 20);
            opts.set_compression(Compression::Lz4);
        });
        let _ = t.db_options();
        for kind in [
            CfKind::Meta,
            CfKind::Changelog,
            CfKind::TtlIndex,
            CfKind::User(&CfSpec::new("u")),
        ] {
            t.cf_options(kind).unwrap();
        }
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 5);
    }
}
