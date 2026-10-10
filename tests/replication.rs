//! Core replication, end to end: a master and slaves in one process over real gRPC.

mod support;

use std::time::Duration;

use replicated_rocksdb::{
    Batch, CfSpec, Direction, Error, FetchConfig, IterMode, LogApi, Op, ReadApi, Slave, SlaveState,
};
use support::{
    Cluster, WAIT, assert_converged, check_edge_cases, dump, wait_state, wait_until,
    write_edge_cases,
};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slave_bootstraps_then_tails_every_kind_of_write() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("users")).unwrap();
    m.put("users", b"alice", b"1").unwrap();
    m.put("users", b"bob", b"2").unwrap();

    let (b, lb) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(lb.bootstraps(), 1);
    assert!(
        lb.entries().is_empty(),
        "no per-record events during bootstrap"
    );
    let boot_seq = b.last_seq();

    m.create_cf(CfSpec::new("orders")).unwrap();
    let mut batch = Batch::new();
    batch
        .put("users", b"carol", b"3")
        .put("orders", b"o1", b"x")
        .delete("users", b"alice");
    m.write(batch).unwrap();
    m.delete_range("users", b"a", b"c").unwrap();
    m.create_cf(CfSpec::new("tmp")).unwrap();
    m.put("tmp", b"k", b"v").unwrap();
    m.drop_cf("tmp").unwrap();
    m.create_cf(CfSpec::new("tmp")).unwrap();
    assert_converged(&m, &[("b", &b)]).await;

    assert_eq!(b.list_cfs(), m.list_cfs());
    assert_eq!(b.get("users", b"bob").unwrap(), None, "range-deleted");
    assert_eq!(b.get("users", b"carol").unwrap(), Some(b"3".to_vec()));
    assert_eq!(
        b.get("tmp", b"k").unwrap(),
        None,
        "re-created CF starts empty"
    );
    assert_eq!(
        b.get_with_meta("orders", b"o1").unwrap(),
        m.get_with_meta("orders", b"o1").unwrap()
    );
    assert_eq!(
        lb.entries(),
        ((boot_seq + 1)..=m.last_seq()).collect::<Vec<_>>()
    );
    lb.assert_no_violations();

    // The slave's log holds the master's entries byte for byte.
    let ours: Vec<_> = b
        .entries_since(boot_seq)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    let theirs: Vec<_> = m
        .entries_since(boot_seq)
        .unwrap()
        .map(Result::unwrap)
        .collect();
    assert_eq!(ours, theirs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_api_on_a_slave() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for k in [&b"a"[..], b"ab", b"abc", b"b"] {
        m.put("c", k, k).unwrap();
    }
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;

    assert_eq!(
        b.multi_get("c", &[b"a", b"zz"]).unwrap(),
        vec![Some(b"a".to_vec()), None]
    );
    let keys = |it: replicated_rocksdb::DbIter<'_>| it.map(|r| r.unwrap().0).collect::<Vec<_>>();
    assert_eq!(
        keys(b.prefix_iter("c", b"ab").unwrap()),
        vec![b"ab".to_vec(), b"abc".to_vec()]
    );
    assert_eq!(
        keys(
            b.iter("c", IterMode::From(b"ab", Direction::Reverse))
                .unwrap()
        ),
        vec![b"ab".to_vec(), b"a".to_vec()]
    );
    assert!(matches!(b.get("nope", b"k"), Err(Error::NoSuchCf(_))));

    // A snapshot keeps a dropped CF readable on the slave too.
    let snap = b.snapshot().unwrap();
    m.drop_cf("c").unwrap();
    b.wait_for_seq(m.last_seq(), WAIT).await.unwrap();
    assert!(b.list_cfs().is_empty());
    assert_eq!(snap.get("c", b"abc").unwrap(), Some(b"abc".to_vec()));
    assert_eq!(snap.seq(), m.last_seq() - 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_values_replicate_and_the_reapers_delete_replicates() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    m.put_with_ttl("c", b"session", b"s1", Duration::from_secs(10))
        .unwrap();
    m.put_with_ttl("c", b"", b"", Duration::from_secs(10))
        .unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let (_, meta) = b.get_with_meta("c", b"session").unwrap().unwrap();
    assert_eq!(meta.expire_at_ms, Some(support::T0 + 10_000));

    c.clock.advance(Duration::from_secs(10));
    assert_eq!(
        b.get("c", b"session").unwrap(),
        None,
        "expired values are hidden on slaves before reaping"
    );
    let before = m.last_seq();
    assert_eq!(m.reap_expired().unwrap().deleted, 2);
    assert_converged(&m, &[("b", &b)]).await;
    let reap: Vec<Op> = b
        .entries_since(before)
        .unwrap()
        .flat_map(|e| e.unwrap().decode_ops().unwrap())
        .collect();
    assert_eq!(reap.len(), 2);
    assert!(reap.iter().all(|op| matches!(op, Op::Delete { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_case_data_through_tail_and_bootstrap() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("seed")).unwrap();
    let (tailing, lt) = c.slave("tailing", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("tailing", &tailing)]).await;

    write_edge_cases(&m, "edge");
    assert_converged(&m, &[("tailing", &tailing)]).await;
    assert_eq!(lt.bootstraps(), 1, "the edge cases arrived through Tail");
    check_edge_cases(&tailing, "edge");

    let (booted, lb) = c.slave("booted", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("booted", &booted)]).await;
    assert_eq!(lb.bootstraps(), 1);
    assert!(
        lb.entries().is_empty(),
        "the edge cases arrived through Bootstrap"
    );
    check_edge_cases(&booted, "edge");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resumes_without_bootstrap() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let seq = b.last_seq();
    b.shutdown().await.unwrap();

    for i in 0..10u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let (cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_eq!(
        b.get("c", &[0]).unwrap(),
        None,
        "local data is readable at once"
    );
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 0);
    assert_eq!(
        listener.entries(),
        ((seq + 1)..=m.last_seq()).collect::<Vec<_>>()
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_and_wait_for_seq() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    assert_eq!(*b.watch_state().borrow(), SlaveState::Streaming);
    m.put("c", b"k", b"v").unwrap();
    b.wait_for_seq(2, WAIT).await.unwrap();
    wait_until("heartbeat positions", || b.status().upstream_last_seq == 2).await;
    let s = b.status();
    assert_eq!(s.state, SlaveState::Streaming);
    assert_eq!(s.origin_identity, Some(m.identity()));
    assert_eq!((s.applied_seq, s.origin_last_seq), (2, 2));
    assert_eq!(s.upstream_chain, vec!["a".to_string()]);
    assert!(s.last_contact.is_some());
    assert_eq!(s.last_error, None);
    assert!(!s.listener_failed);
    assert!(matches!(
        b.wait_for_seq(3, Duration::from_millis(100)).await,
        Err(Error::Timeout)
    ));
    // The master sees B's acknowledgements (addendum §1).
    wait_until("b acknowledged 2", || {
        m.replicas().iter().any(|r| r.applied_seq == 2 && r.in_sync)
    })
    .await;
    let r = m.replicas();
    assert_eq!(r.len(), 1);
    assert_eq!(
        (
            r[0].node_id.as_str(),
            r[0].sent_seq,
            r[0].lag_entries,
            r[0].lag_ms
        ),
        ("b", 2, 0, Some(0))
    );
    assert_eq!((r[0].epoch, r[0].peer_verified), (1, false));
    assert!(r[0].remote_addr.ip().is_loopback());
    let states = listener.states();
    assert!(
        states.contains(&SlaveState::Bootstrapping {
            target_seq: 1,
            records: 0
        }),
        "{states:?}"
    );
    assert_eq!(states.last(), Some(&SlaveState::Streaming));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slave_without_data_is_not_ready_and_reports_the_connection_error() {
    let c = Cluster::new();
    let nobody = std::net::SocketAddr::from(([127, 0, 0, 1], support::reserved_port())); // nothing binds it
    let (b, _) = c.slave("b", nobody).await;
    assert!(matches!(b.get("c", b"k"), Err(Error::NotReady)));
    assert!(matches!(b.snapshot(), Err(Error::NotReady)));
    assert!(matches!(
        b.wait_for_seq(1, Duration::from_millis(200)).await,
        Err(Error::Timeout)
    ));
    wait_until("a connection error", || b.status().last_error.is_some()).await;
    assert_eq!(b.status().state, SlaveState::Connecting);
    assert!(dump(&b).is_empty());
}

/// With fetch batching on, entries arrive in batches but are still applied one entry per
/// atomic write, each delivered to the listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batching_slave_applies_every_entry_on_its_own() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.fetch = FetchConfig {
        min_batch_bytes: 64 << 10,
        max_wait: Duration::from_millis(150),
        max_batch_bytes: 16 << 10,
    };
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let first = m.last_seq() + 1;
    for i in 0..200u32 {
        m.put("c", &i.to_be_bytes(), &[3; 300]).unwrap();
    }
    write_edge_cases(&m, "edge");
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(
        listener.entries(),
        (first..=m.last_seq()).collect::<Vec<_>>()
    );
    check_edge_cases(&b, "edge");
    listener.assert_no_violations();
    let fetch = m.replicas()[0].fetch;
    assert_eq!(
        (fetch.min_batch_bytes, fetch.max_wait, fetch.max_batch_bytes),
        (64 << 10, Duration::from_millis(150), 16 << 10)
    );
}

/// `sync_writes = true` fsyncs every applied entry; replication and resume are unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sync_writes_slave_replicates_and_resumes() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.sync_writes = true;
    let b = Slave::open(cfg.clone()).await.unwrap();
    for i in 0..20u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    assert_converged(&m, &[("b", &b)]).await;
    b.shutdown().await.unwrap();
    m.put("c", b"later", b"v").unwrap();
    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.sync_writes = true;
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_listener_is_disabled_and_replication_goes_on() {
    struct Panics;
    impl replicated_rocksdb::ReplicationListener for Panics {
        fn on_entry(&self, _: &replicated_rocksdb::LogEntry) {
            panic!("listener bug");
        }
    }
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.listener = Some(std::sync::Arc::new(Panics));
    let b = Slave::open(cfg).await.unwrap();
    b.wait_for_seq(1, WAIT).await.unwrap();
    m.put("c", b"k1", b"v").unwrap();
    m.put("c", b"k2", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert!(b.status().listener_failed);
}

/// A configured token that is not an RFC 6750 b64token is refused before it is sent: the
/// slave halts `AuthRejected`, which is not persisted, so a fixed configuration resumes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_invalid_token_halts_auth_rejected_without_persisting() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.upstream.credentials =
        std::sync::Arc::new(replicated_rocksdb::TokenCredentials::new("not a token"));
    let b = Slave::open(cfg).await.unwrap();
    wait_state(&b, "AuthRejected", |s| {
        *s == SlaveState::Halted(replicated_rocksdb::HaltReason::AuthRejected)
    })
    .await;
    b.shutdown().await.unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
}

/// Ruling (d), through the driver: a re-bootstrap needs a CF that a reader's snapshot still
/// holds with another spec. The slave backs off, reports it in `last_error`, never halts, and
/// converges once the snapshot is released. (Triggered with a replacement master of another
/// identity whose "c" has the same name and create_seq but a different comparator.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cf_held_by_a_snapshot_is_retried_not_halted() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"old", b"1").unwrap();
    let port = m.serve_addr().unwrap().port();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.on_identity_change = replicated_rocksdb::IdentityPolicy::Rebootstrap;
    cfg.rocksdb
        .comparators
        .register("rev", |a: &[u8], b: &[u8]| b.cmp(a));
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let snap = b.snapshot().unwrap();
    m.shutdown().await.unwrap();

    let mut mcfg = c.master_config("a2");
    mcfg.serve.as_mut().unwrap().port = port;
    mcfg.rocksdb
        .comparators
        .register("rev", |a: &[u8], b: &[u8]| b.cmp(a));
    let m2 = replicated_rocksdb::Master::open(mcfg).await.unwrap();
    m2.create_cf(CfSpec::new("c").with_comparator("rev"))
        .unwrap();
    m2.put("c", b"new", b"2").unwrap();

    wait_until("a CfInUse error", || {
        b.status()
            .last_error
            .is_some_and(|e| e.contains("still referenced by a snapshot"))
    })
    .await;
    assert!(!matches!(b.status().state, SlaveState::Halted(_)));
    assert_eq!(
        snap.get("c", b"old").unwrap(),
        Some(b"1".to_vec()),
        "the snapshot keeps its view"
    );
    drop(snap);
    assert_converged(&m2, &[("b", &b)]).await;
    assert_eq!(b.get("c", b"new").unwrap(), Some(b"2".to_vec()));
    assert_eq!(b.get("c", b"old").unwrap(), None);
}
