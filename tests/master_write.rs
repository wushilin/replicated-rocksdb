mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{clock, config, log, T0};
use replicated_rocksdb::{Batch, CfSpec, EpochStart, Error, IterMode, LogApi, Master, Op, ReadApi};

#[tokio::test]
async fn first_open_creates_identity_and_reopen_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    let id = m.identity();
    assert_eq!(id.epoch, 1);
    assert_eq!(m.epoch_history(), vec![EpochStart { epoch: 1, start_seq: 1 }]);
    m.create_cf(CfSpec::new("users")).unwrap();
    m.put("users", b"k", b"v").unwrap();
    m.shutdown().await.unwrap();

    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(m.identity(), id);
    assert_eq!(m.last_seq(), 2);
    assert_eq!(m.get("users", b"k").unwrap().unwrap(), b"v");
    assert_eq!(m.put("users", b"k2", b"v").unwrap(), 3);
}

#[tokio::test]
async fn every_write_is_one_logged_entry() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(m.create_cf(CfSpec::new("c")).unwrap(), 1);
    assert_eq!(m.put("c", b"a", b"1").unwrap(), 2);
    assert_eq!(m.put_with_ttl("c", b"b", b"2", Duration::from_secs(1)).unwrap(), 3);
    assert_eq!(m.delete("c", b"a").unwrap(), 4);
    assert_eq!(m.delete_range("c", b"a", b"z").unwrap(), 5);
    let mut b = Batch::new();
    b.put("c", b"x", b"1").put("c", b"y", b"2");
    assert_eq!(m.write(b).unwrap(), 6);
    assert_eq!(m.drop_cf("c").unwrap(), 7);

    let entries = log(&m);
    assert_eq!(entries.iter().map(|e| e.seq).collect::<Vec<_>>(), (1..=7).collect::<Vec<_>>());
    for e in &entries {
        e.verify_crc().unwrap();
        assert_eq!(e.epoch, 1);
        assert_eq!(e.ts_ms, T0);
    }
    assert_eq!(
        entries[2].decode_ops().unwrap(),
        vec![Op::Put { cf: "c".into(), key: b"b".to_vec(), value: b"2".to_vec(), expire_at_ms: Some(T0 + 1_000) }]
    );
    assert_eq!(entries[5].decode_ops().unwrap().len(), 2);
}

#[tokio::test]
async fn failed_validation_consumes_no_seq() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    assert!(matches!(m.put("nope", b"k", b"v"), Err(Error::NoSuchCf(_))));
    assert!(matches!(m.write(Batch::new()), Err(Error::EmptyBatch)));
    assert!(matches!(m.create_cf(CfSpec::new("c")), Err(Error::AlreadyExists(_))));
    assert!(matches!(m.create_cf(CfSpec::new("__x")), Err(Error::InvalidCfName(_))));
    assert!(matches!(m.delete_range("c", b"b", b"a"), Err(Error::InvalidRange)));
    assert_eq!(m.put("c", b"k", b"v").unwrap(), 2);
}

#[tokio::test]
async fn max_entry_bytes_is_enforced_at_the_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.max_entry_bytes = 200;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    // Find the value size whose encoded entry is exactly 200 bytes.
    let size_for = |n: usize| {
        let ops = vec![Op::Put { cf: "c".into(), key: b"k".to_vec(), value: vec![0; n], expire_at_ms: None }];
        replicated_rocksdb::codec::entry::encoded_entry_len(2, 1, T0, &ops)
    };
    let n = (0..200).find(|&n| size_for(n) == 200).expect("some value size encodes to exactly 200 bytes");
    let err = m.put("c", b"k", &vec![0; n + 1]).unwrap_err();
    assert!(matches!(err, Error::EntryTooLarge { max: 200, .. }), "{err:?}");
    assert_eq!(m.put("c", b"k", &vec![0; n]).unwrap(), 2);
}

#[tokio::test]
async fn timestamps_never_go_backwards() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    c.set(T0 - 60_000);
    m.put("c", b"k", b"v").unwrap();
    let entries = log(&m);
    assert_eq!(entries[1].ts_ms, T0);
}

#[tokio::test]
async fn concurrent_writers_get_unique_gap_free_seqs() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Arc::new(Master::open(config(dir.path(), &c)).await.unwrap());
    m.create_cf(CfSpec::new("c")).unwrap();
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let m = m.clone();
            std::thread::spawn(move || {
                (0..50).map(|i| m.put("c", format!("{t}-{i}").as_bytes(), b"v").unwrap()).collect::<Vec<_>>()
            })
        })
        .collect();
    let mut seqs: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    seqs.sort_unstable();
    assert_eq!(seqs, (2..=401).collect::<Vec<_>>());
    let ts: Vec<u64> = log(&*m).iter().map(|e| e.ts_ms).collect();
    assert!(ts.windows(2).all(|w| w[0] <= w[1]));
}

#[tokio::test]
async fn bump_epoch_continues_seq_and_tags_new_entries() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let before = m.identity();
    let after = m.bump_epoch().unwrap();
    assert_eq!(after.db_uuid, before.db_uuid);
    assert_eq!(after.epoch, 2);
    assert_eq!(m.put("c", b"k", b"v").unwrap(), 2);
    let entries = log(&m);
    assert_eq!((entries[0].epoch, entries[1].epoch), (1, 2));
    assert_eq!(
        m.epoch_history(),
        vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 2 }]
    );
    m.shutdown().await.unwrap();

    let mut cfg = config(dir.path(), &c);
    cfg.bump_epoch_on_open = true;
    let m = Master::open(cfg).await.unwrap();
    assert_eq!(m.identity().epoch, 3);
    assert_eq!(m.epoch_history().last(), Some(&EpochStart { epoch: 3, start_seq: 3 }));
}

#[tokio::test]
async fn edge_case_keys_and_values_through_the_master() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("e")).unwrap();
    let big_key = vec![0x5Au8; 1 << 20];
    let near_limit = vec![0xC3u8; (16 << 20) - 4096];
    let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (vec![], vec![]),
        (vec![0], vec![0]),
        (b"\0k\0".to_vec(), b"\0\0\0".to_vec()),
        (big_key.clone(), vec![]),
        (b"big".to_vec(), near_limit),
    ];
    for (k, v) in &cases {
        m.put("e", k, v).unwrap();
    }
    assert!(matches!(m.put("e", b"too-big", &vec![0; 16 << 20]), Err(Error::EntryTooLarge { .. })));
    m.shutdown().await.unwrap();

    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    for (k, v) in &cases {
        assert_eq!(m.get("e", k).unwrap().as_deref(), Some(v.as_slice()), "key of length {}", k.len());
    }
    // An empty value is a value, not an absence.
    assert_eq!(m.get("e", b"").unwrap(), Some(vec![]));
    assert_eq!(m.iter("e", IterMode::Start).unwrap().count(), cases.len());
    // [b"", b"\x01") covers exactly the empty key and the keys starting with NUL.
    m.delete_range("e", b"", b"\x01").unwrap();
    assert_eq!(m.get("e", b"").unwrap(), None);
    assert_eq!(m.get("e", b"\0").unwrap(), None);
    assert_eq!(m.get("e", b"\0k\0").unwrap(), None);
    assert!(m.get("e", &big_key).unwrap().is_some());
    m.put_with_ttl("e", b"", b"", Duration::from_secs(1)).unwrap();
    assert_eq!(m.get("e", b"").unwrap(), Some(vec![]));
    c.advance(Duration::from_secs(1));
    assert_eq!(m.get("e", b"").unwrap(), None);
}

#[tokio::test]
async fn read_api_and_snapshot_on_master() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"a", b"1").unwrap();
    let snap = m.snapshot().unwrap();
    m.put("c", b"a", b"2").unwrap();
    assert_eq!(snap.seq(), 2);
    assert_eq!(snap.get("c", b"a").unwrap().unwrap(), b"1");
    assert_eq!(m.get("c", b"a").unwrap().unwrap(), b"2");
    assert_eq!(m.list_cfs(), vec![CfSpec::new("c")]);
    let (_, meta) = m.get_with_meta("c", b"a").unwrap().unwrap();
    assert_eq!(meta.write_seq, 3);
}

#[tokio::test]
async fn subscribe_sees_each_commit() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    let mut rx = m.subscribe();
    m.create_cf(CfSpec::new("c")).unwrap();
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow(), 1);
}

#[tokio::test]
async fn bump_on_open_is_skipped_for_a_fresh_database() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.bump_epoch_on_open = true;
    let m = Master::open(cfg).await.unwrap();
    assert_eq!(m.identity().epoch, 1);
    assert_eq!(m.epoch_history(), vec![EpochStart { epoch: 1, start_seq: 1 }]);
}

#[tokio::test]
async fn zero_heartbeat_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.heartbeat = Duration::ZERO;
    assert!(matches!(Master::open(cfg).await, Err(Error::Config(_))));
}

#[tokio::test]
async fn invalid_identifier_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.identifier = "bad id!".into();
    assert!(matches!(Master::open(cfg).await, Err(Error::Config(_))));
}
