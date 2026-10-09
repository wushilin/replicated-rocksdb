mod common;

use std::time::Duration;

use common::{clock, config, log};
use replicated_rocksdb::{Batch, CfSpec, LogApi, Master, Op, ReadApi, ReapStats};

async fn master(dir: &std::path::Path, c: &std::sync::Arc<replicated_rocksdb::ManualClock>) -> Master {
    let m = Master::open(config(dir, c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    m
}

#[tokio::test]
async fn expired_values_are_hidden_then_reaped_by_a_logged_delete() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.put_with_ttl("c", b"k", b"v", Duration::from_secs(10)).unwrap();
    assert_eq!(m.get("c", b"k").unwrap().unwrap(), b"v");
    assert_eq!(m.reap_expired().unwrap(), ReapStats::default(), "nothing due yet");

    c.advance(Duration::from_secs(10));
    assert_eq!(m.get("c", b"k").unwrap(), None, "hidden before reaping");
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 1, stale: 0, entries: 1 });
    let last = log(&m).pop().unwrap();
    assert_eq!(last.decode_ops().unwrap(), vec![Op::Delete { cf: "c".into(), key: b"k".to_vec() }]);
    assert_eq!(m.reap_expired().unwrap(), ReapStats::default(), "index entry removed");
}

#[tokio::test]
async fn overwrite_clears_ttl_and_leaves_a_stale_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.put_with_ttl("c", b"k", b"v", Duration::from_secs(1)).unwrap();
    m.put("c", b"k", b"forever").unwrap();
    let seq = m.last_seq();
    c.advance(Duration::from_secs(5));
    assert_eq!(m.get("c", b"k").unwrap().unwrap(), b"forever");
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 0, stale: 1, entries: 0 });
    assert_eq!(m.last_seq(), seq, "stale cleanup is not logged");
    assert_eq!(m.get("c", b"k").unwrap().unwrap(), b"forever");
}

#[tokio::test]
async fn range_deleted_and_dropped_cf_entries_are_stale() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.create_cf(CfSpec::new("d")).unwrap();
    m.put_with_ttl("c", b"a", b"v", Duration::from_secs(1)).unwrap();
    m.put_with_ttl("d", b"a", b"v", Duration::from_secs(1)).unwrap();
    m.delete_range("c", b"a", b"b").unwrap();
    m.drop_cf("d").unwrap();
    m.create_cf(CfSpec::new("d")).unwrap();
    m.put("d", b"a", b"new").unwrap();
    c.advance(Duration::from_secs(2));
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 0, stale: 2, entries: 0 });
    assert_eq!(m.get("d", b"a").unwrap().unwrap(), b"new");
}

#[tokio::test]
async fn large_rounds_split_into_bounded_entries() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.reaper.max_deletes_per_entry = 3;
    cfg.reaper.max_deletes_per_round = 7;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut b = Batch::new();
    for i in 0..10u8 {
        b.put_with_ttl("c", &[i], b"v", Duration::from_secs(1));
    }
    m.write(b).unwrap();
    c.advance(Duration::from_secs(1));
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 7, stale: 0, entries: 3 });
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 3, stale: 0, entries: 1 });
    let sizes: Vec<usize> = log(&m).iter().skip(2).map(|e| e.decode_ops().unwrap().len()).collect();
    assert_eq!(sizes, vec![3, 3, 1, 3]);
}

#[tokio::test]
async fn ttl_on_empty_and_nul_keys_is_reaped() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.put_with_ttl("c", b"", b"", Duration::from_secs(1)).unwrap();
    m.put_with_ttl("c", b"\0", b"\0", Duration::from_secs(1)).unwrap();
    m.put("c", b"\0\0", b"keep").unwrap();
    c.advance(Duration::from_secs(1));
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 2, stale: 0, entries: 1 });
    assert_eq!(m.get("c", b"\0\0").unwrap().unwrap(), b"keep");
}

#[tokio::test]
async fn extreme_ttls() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.put_with_ttl("c", b"zero", b"v", Duration::ZERO).unwrap();
    m.put_with_ttl("c", b"max", b"v", Duration::MAX).unwrap();
    assert_eq!(m.get("c", b"zero").unwrap(), None, "expire_at == now is expired");
    let (_, meta) = m.get_with_meta("c", b"max").unwrap().unwrap();
    assert_eq!(meta.expire_at_ms, Some(u64::MAX));
    c.set(u64::MAX - 1);
    assert!(m.get("c", b"max").unwrap().is_some());
    assert_eq!(m.reap_expired().unwrap().deleted, 1);
}
