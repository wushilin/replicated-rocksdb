//! RocksDB tuning. Callers get a restricted [`Tunable`] instead of raw `rocksdb::Options`,
//! so merge operators, compaction filters and native TTL cannot be configured at all.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rocksdb::{DBCompressionType, Options};

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

/// The tuning knobs this crate allows. There is deliberately no way to set a merge
/// operator, compaction filter or TTL:
///
/// ```compile_fail
/// fn f(t: &mut replicated_rocksdb::tuning::Tunable<'_>) {
///     t.set_merge_operator_associative("m", |_, _, _| None);
/// }
/// ```
pub struct Tunable<'a> {
    opts: &'a mut Options,
}

impl Tunable<'_> {
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
        self.opts.set_compression_type(match c {
            Compression::None => DBCompressionType::None,
            Compression::Lz4 => DBCompressionType::Lz4,
            Compression::Zstd => DBCompressionType::Zstd,
        });
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
            cb(CfKind::Db, &mut Tunable { opts: &mut opts });
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
        if let Some(cb) = &self.options {
            cb(kind, &mut Tunable { opts: &mut opts });
        }
        Ok(opts)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};

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
