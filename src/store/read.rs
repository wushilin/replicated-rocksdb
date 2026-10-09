//! Low-level reads shared by live reads and snapshots. Values are unwrapped from their
//! envelope, and expired values are hidden (DESIGN.md §8).

use std::sync::Arc;

use rocksdb::{BoundColumnFamily, DBIteratorWithThreadMode, ReadOptions, DB};

use super::cfs::PhysCf;
use super::snapshot::OwnedSnapshot;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::types::{Direction, IterMode, ValueMeta};

pub(crate) struct ReadCtx<'a> {
    pub db: &'a DB,
    pub snap: Option<&'a OwnedSnapshot>,
    /// Fixed for the whole read: once per call, iterator or snapshot.
    pub now_ms: u64,
}

fn handle<'d>(db: &'d DB, cf: &PhysCf) -> Result<Arc<BoundColumnFamily<'d>>> {
    db.cf_handle(&cf.physical)
        .ok_or_else(|| Error::corrupt(format!("physical column family {} is missing", cf.physical)))
}

impl<'a> ReadCtx<'a> {
    fn opts(&self) -> ReadOptions {
        self.snap.map(OwnedSnapshot::read_opts).unwrap_or_default()
    }

    /// The raw envelope, expired or not.
    pub(crate) fn get_raw(&self, cf: &PhysCf, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf_opt(&handle(self.db, cf)?, key, &self.opts())?)
    }

    pub(crate) fn get(&self, cf: &PhysCf, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        let Some(raw) = self.get_raw(cf, key)? else { return Ok(None) };
        let env = envelope::decode(&raw)?;
        if env.is_expired(self.now_ms) {
            return Ok(None);
        }
        Ok(Some((env.value.to_vec(), env.meta)))
    }

    pub(crate) fn iter(&self, cf: Arc<PhysCf>, mode: IterMode<'_>, prefix: Option<&[u8]>) -> Result<DbIter<'a>> {
        let mode = match mode {
            IterMode::Start => rocksdb::IteratorMode::Start,
            IterMode::End => rocksdb::IteratorMode::End,
            IterMode::From(k, Direction::Forward) => rocksdb::IteratorMode::From(k, rocksdb::Direction::Forward),
            IterMode::From(k, Direction::Reverse) => rocksdb::IteratorMode::From(k, rocksdb::Direction::Reverse),
        };
        let inner = self.db.iterator_cf_opt(&handle(self.db, &cf)?, self.opts(), mode);
        Ok(DbIter { inner, _cf: cf, now_ms: self.now_ms, prefix: prefix.map(<[u8]>::to_vec), done: false })
    }
}

/// Iterator over `(key, value)` pairs of a CF, with envelopes stripped and expired values skipped.
pub struct DbIter<'a> {
    inner: DBIteratorWithThreadMode<'a, DB>,
    // Keeps the physical CF from being dropped while iterating.
    _cf: Arc<PhysCf>,
    now_ms: u64,
    prefix: Option<Vec<u8>>,
    done: bool,
}

impl Iterator for DbIter<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            let (k, v) = match self.inner.next()? {
                Ok(kv) => kv,
                Err(e) => return Some(Err(e.into())),
            };
            if self.prefix.as_deref().is_some_and(|p| !k.starts_with(p)) {
                self.done = true;
                return None;
            }
            match envelope::decode(&v) {
                Err(e) => return Some(Err(e)),
                Ok(env) if env.is_expired(self.now_ms) => continue,
                Ok(env) => return Some(Ok((k.into_vec(), env.value.to_vec()))),
            }
        }
        None
    }
}

#[cfg(test)]
mod tests {
    use crate::read::ReadApi;
    use crate::store::test_support::{apply, open_store};
    use crate::types::{CfSpec, Direction, IterMode, Op};

    fn put(k: &[u8], v: &[u8], ttl_at: Option<u64>) -> Op {
        Op::Put { cf: "c".into(), key: k.to_vec(), value: v.to_vec(), expire_at_ms: ttl_at }
    }

    fn setup() -> (tempfile::TempDir, crate::store::Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put(b"a", b"1", None), put(b"b", b"2", Some(100)), put(b"ba", b"3", None)]).unwrap();
        apply(&store, vec![put(b"c", b"4", None), put(b"", b"empty", None)]).unwrap();
        (dir, store)
    }

    fn keys(it: crate::store::read::DbIter<'_>) -> Vec<Vec<u8>> {
        it.map(|r| r.unwrap().0).collect()
    }

    #[test]
    fn get_strips_envelope_and_hides_expired() {
        let (_d, store) = setup();
        let cf = store.live_cf("c").unwrap();
        let (v, meta) = store.live_ctx(99).get(&cf, b"b").unwrap().unwrap();
        assert_eq!(v, b"2");
        assert_eq!(meta.write_seq, 2);
        assert_eq!(meta.expire_at_ms, Some(100));
        assert_eq!(store.live_ctx(100).get(&cf, b"b").unwrap(), None);
        assert_eq!(store.live_ctx(100).get(&cf, b"").unwrap().unwrap().0, b"empty");
        assert_eq!(store.live_ctx(0).get(&cf, b"zz").unwrap(), None);
    }

    #[test]
    fn iterators_respect_mode_prefix_and_expiry() {
        let (_d, store) = setup();
        let cf = store.live_cf("c").unwrap();
        let ctx = store.live_ctx(100);
        assert_eq!(keys(ctx.iter(cf.clone(), IterMode::Start, None).unwrap()), vec![b"".to_vec(), b"a".to_vec(), b"ba".to_vec(), b"c".to_vec()]);
        assert_eq!(keys(ctx.iter(cf.clone(), IterMode::End, None).unwrap()), vec![b"c".to_vec(), b"ba".to_vec(), b"a".to_vec(), b"".to_vec()]);
        assert_eq!(
            keys(ctx.iter(cf.clone(), IterMode::From(b"b", Direction::Forward), None).unwrap()),
            vec![b"ba".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            keys(ctx.iter(cf.clone(), IterMode::From(b"b", Direction::Forward), Some(b"b")).unwrap()),
            vec![b"ba".to_vec()]
        );
        let at_99 = store.live_ctx(99);
        assert_eq!(keys(at_99.iter(cf, IterMode::From(b"b", Direction::Forward), Some(b"b")).unwrap()).len(), 2);
    }

    #[test]
    fn snapshot_is_isolated_and_fixes_now() {
        let (_d, store) = setup();
        let snap = store.read_snapshot(99).unwrap();
        assert_eq!(snap.seq(), 3);
        apply(&store, vec![put(b"a", b"changed", None)]).unwrap();
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("later") }]).unwrap();
        assert_eq!(snap.get("c", b"a").unwrap().unwrap(), b"1");
        assert_eq!(snap.get("c", b"b").unwrap().unwrap(), b"2", "now is fixed at 99");
        assert_eq!(snap.list_cfs(), vec![CfSpec::new("c")]);
        assert_eq!(
            snap.multi_get("c", &[b"a", b"zz"]).unwrap(),
            vec![Some(b"1".to_vec()), None]
        );
        assert_eq!(keys(snap.prefix_iter("c", b"b").unwrap()).len(), 2);
        assert!(matches!(snap.get("later", b"k"), Err(crate::Error::NoSuchCf(_))));
    }

    #[test]
    fn nul_and_empty_keys_order_and_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("n") }]).unwrap();
        let p = |k: &[u8]| Op::Put { cf: "n".into(), key: k.to_vec(), value: k.to_vec(), expire_at_ms: None };
        apply(&store, vec![p(b"a"), p(b"\0\0"), p(b""), p(b"\0")]).unwrap();
        let cf = store.live_cf("n").unwrap();
        let ctx = store.live_ctx(0);
        let all: Vec<(Vec<u8>, Vec<u8>)> =
            ctx.iter(cf.clone(), IterMode::Start, None).unwrap().map(Result::unwrap).collect();
        assert_eq!(all.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>(), vec![&b""[..], b"\0", b"\0\0", b"a"]);
        assert!(all.iter().all(|(k, v)| k == v), "values equal keys, including empty ones");
        assert_eq!(keys(ctx.iter(cf.clone(), IterMode::From(b"", Direction::Forward), Some(b"")).unwrap()).len(), 4);
        assert_eq!(
            keys(ctx.iter(cf, IterMode::From(b"\0", Direction::Forward), Some(b"\0")).unwrap()),
            vec![b"\0".to_vec(), b"\0\0".to_vec()]
        );
    }

    #[test]
    fn snapshot_keeps_dropped_cf_readable_until_released() {
        let (_d, store) = setup();
        let snap = store.read_snapshot(0).unwrap();
        apply(&store, vec![Op::DropCf { name: "c".into() }]).unwrap();
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0);
        assert_eq!(snap.get("c", b"a").unwrap().unwrap(), b"1");
        drop(snap);
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
    }
}
