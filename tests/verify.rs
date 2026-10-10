//! Verify end to end: a slave compares itself with its upstream over real gRPC.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, Error, HaltReason, Master, ReadApi, Slave, SlaveState, VerifyAbort, VerifyMode,
};
use support::{Cluster, assert_converged, wait_state, wait_until};

/// Values expired but not reaped, a DeleteRange, a CF dropped and re-created under the same
/// name, and an empty CF: all identical on disk, so both modes are clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_verifies_clean_in_both_modes() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("data")).unwrap();
    for i in 0..300u32 {
        m.put(
            "data",
            format!("k{i:03}").as_bytes(),
            format!("value-{i}").as_bytes(),
        )
        .unwrap();
    }
    m.put_with_ttl("data", b"ttl", b"soon gone", Duration::from_secs(5))
        .unwrap();
    m.delete_range("data", b"k100", b"k150").unwrap();
    m.create_cf(CfSpec::new("again")).unwrap();
    m.put("again", b"old", b"1").unwrap();
    m.drop_cf("again").unwrap();
    m.create_cf(CfSpec::new("again")).unwrap();
    m.put("again", b"new", b"2").unwrap();
    m.create_cf(CfSpec::new("empty")).unwrap();
    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.verify.chunk_keys = 16;
    cfg.verify.min_chunk_keys = 2;
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    c.clock.advance(Duration::from_secs(10));
    assert_eq!(
        b.get("data", b"ttl").unwrap(),
        None,
        "expired on reads, not reaped"
    );

    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let r = b.verify_now(mode).await.unwrap();
        assert!(r.is_clean(), "{mode:?}: {:?}", r.mismatches);
        assert_eq!((r.seq, r.mode, r.cfs_checked), (m.last_seq(), mode, 3));
        assert_eq!(
            r.keys_checked,
            250 + 1 + 1,
            "raw keys, the expired one included"
        );
    }
    assert_eq!(
        b.status().last_verify.map(|r| r.mode),
        Some(VerifyMode::Full)
    );
    assert_eq!(listener.verifies().len(), 2);
}

/// A→B convergence under concurrent writes, with Full verifies while the writes go on: each
/// run compares both sides at the upstream's snapshot seq, so it is clean however far the
/// slave has moved on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_pins_the_snapshot_while_concurrent_writes_continue() {
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("data")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_state(&b, "bootstrapped and streaming", |s| {
        *s == SlaveState::Streaming
    })
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4u32)
        .map(|w| {
            let (m, stop) = (m.clone(), stop.clone());
            tokio::task::spawn_blocking(move || {
                let mut n = 0u32;
                while !stop.load(Ordering::SeqCst) {
                    // Overwrites too: write_seqs change under the verifier's feet.
                    m.put(
                        "data",
                        format!("w{w}-{:03}", n % 300).as_bytes(),
                        &n.to_be_bytes(),
                    )
                    .unwrap();
                    n += 1;
                }
                n
            })
        })
        .collect();
    let mut last = 0;
    for _ in 0..5 {
        let r = b.verify_now(VerifyMode::Full).await.unwrap();
        assert!(r.is_clean(), "{:?}", r.mismatches);
        assert!(r.seq >= last, "snapshot seqs never go back");
        last = r.seq;
    }
    stop.store(true, Ordering::SeqCst);
    for w in writers {
        assert!(w.await.unwrap() > 0);
    }
    assert!(
        m.last_seq() > last,
        "the writes went on during the verifies"
    );
    assert_converged(&*m, &[("b", &b)]).await;
    let r = b.verify_now(VerifyMode::Full).await.unwrap();
    assert!(r.is_clean() && r.seq == m.last_seq());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_needs_data_and_the_same_identity() {
    let c = Cluster::new();
    let nowhere = std::net::SocketAddr::from(([127, 0, 0, 1], support::reserved_port()));
    let (lonely, _) = c.slave("lonely", nowhere).await;
    assert!(matches!(
        lonely.verify_now(VerifyMode::Quick).await,
        Err(Error::NotReady)
    ));

    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let port = m.serve_addr().unwrap().port();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    // Another database takes over the upstream's address.
    m.shutdown().await.unwrap();
    let mut other = c.master_config("other");
    other.serve.as_mut().unwrap().port = port;
    let _other = Master::open(other).await.unwrap();
    wait_state(&b, "IdentityChanged halt", |s| {
        *s == SlaveState::Halted(HaltReason::IdentityChanged)
    })
    .await;
    let e = b.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::IdentityChanged)),
        "{e:?}"
    );
    assert!(
        b.status().last_verify.is_none(),
        "an abandoned run leaves no report"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_verifies_run_on_their_interval() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        m.put("c", &[i], &[i]).unwrap();
    }
    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.verify.interval = Some(Duration::from_millis(200));
    cfg.verify.mode = VerifyMode::Full;
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    wait_until("two scheduled verifies", || listener.verifies().len() >= 2).await;
    assert!(
        listener
            .verifies()
            .iter()
            .all(|r| r.is_clean() && r.mode == VerifyMode::Full)
    );
    assert!(b.status().last_verify.unwrap().is_clean());
    // The scheduler holds the slave only during a run: shutdown still closes the database.
    b.shutdown().await.unwrap();
}

/// A key deleted behind the slave's back (a stopped slave's directory opened with plain
/// RocksDB) is found; `Rebootstrap` then repairs it by bootstrapping again, `Report` does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_mismatch_is_reported_and_rebootstrap_repairs_it() {
    use replicated_rocksdb::{Mismatch, MismatchPolicy};
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("data")).unwrap();
    for i in 0..40u8 {
        m.put("data", &[i], &[i]).unwrap();
    }
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    b.shutdown().await.unwrap();
    {
        let path = c.path("b");
        let opts = rocksdb::Options::default();
        let names = rocksdb::DB::list_cf(&opts, &path).unwrap();
        let mut o = rocksdb::Options::default();
        o.create_if_missing(false);
        let db = rocksdb::DB::open_cf(&o, &path, &names).unwrap();
        let cf = db
            .cf_handle(names.iter().find(|n| n.starts_with("u:data:")).unwrap())
            .unwrap();
        db.delete_cf(&cf, [7u8]).unwrap();
    }

    for policy in [MismatchPolicy::Report, MismatchPolicy::Rebootstrap] {
        let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
        cfg.verify.on_mismatch = policy;
        let b = Slave::open(cfg).await.unwrap();
        b.wait_streaming(Duration::from_secs(20)).await.unwrap();
        let r = b.verify_now(VerifyMode::Quick).await.unwrap();
        assert!(
            matches!(&r.mismatches[..], [Mismatch::Key { key, local: None, upstream: Some(_), .. }] if key == &[7u8]),
            "{:?}",
            r.mismatches
        );
        assert_eq!(listener.verifies().len(), 1);
        if policy == MismatchPolicy::Report {
            tokio::time::sleep(Duration::from_millis(300)).await;
            assert_eq!(listener.bootstraps(), 0);
            assert_eq!(b.get("data", &[7]).unwrap(), None, "reported, not repaired");
            b.shutdown().await.unwrap();
        } else {
            wait_until("re-bootstrapped", || listener.bootstraps() == 1).await;
            b.wait_streaming(Duration::from_secs(20)).await.unwrap();
            assert_eq!(b.get("data", &[7]).unwrap(), Some(vec![7]));
            assert!(b.verify_now(VerifyMode::Full).await.unwrap().is_clean());
        }
    }
}
