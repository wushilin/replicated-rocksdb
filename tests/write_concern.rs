//! Write concern end to end (addendum §2, §6): real slaves acknowledge over gRPC.

mod support;

use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{CfSpec, Error, LogApi, Master, SlaveState, UpstreamConfig, WriteConcern};
use support::{assert_converged, wait_state, wait_until, Cluster};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_returns_after_a_real_slave_acknowledged_it() {
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_until("b in sync", || m.replicas().iter().any(|r| r.node_id == "b" && r.in_sync)).await;
    for i in 0..20u8 {
        let m2 = m.clone();
        let seq = tokio::task::spawn_blocking(move || m2.put_with("c", &[i], b"v", WriteConcern::in_sync(1)))
            .await
            .unwrap()
            .unwrap();
        assert!(b.last_seq() >= seq, "entry {seq} is committed on the slave when the write returns");
    }
    let r = m.replicas();
    assert_eq!((r.len(), r[0].applied_seq), (1, m.last_seq()));
}

/// The master is lost right after a write acknowledged by both in-sync replicas;
/// `promote_checked` promotes one of them and the write is there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acknowledged_write_survives_losing_the_master() {
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    let a_addr = m.serve_addr().unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    for (name, s) in [("b", &b), ("c", &cn)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    wait_until("both in sync", || m.replicas().iter().filter(|r| r.in_sync).count() == 2).await;
    let m2 = m.clone();
    let seq = tokio::task::spawn_blocking(move || m2.put_with("c", b"acked", b"v", WriteConcern::in_sync(2)))
        .await
        .unwrap()
        .unwrap();
    let m = Arc::try_unwrap(m).ok().expect("the writer task is done");
    m.shutdown().await.unwrap();

    let peers = [UpstreamConfig::from_addr(cn.serve_addr().unwrap()), UpstreamConfig::from_addr(a_addr)];
    let new_master = b.promote_checked(c.master_config("b"), &peers).await.unwrap();
    assert!(new_master.last_seq() >= seq);
    assert_eq!(new_master.get("c", b"acked").unwrap(), Some(b"v".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_configured_write_concern_applies_to_every_write() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    cfg.write_concern = WriteConcern { min_insync_replicas: 1, ack_timeout: Duration::from_secs(10) };
    let m = Master::open(cfg).await.unwrap();
    let r = m.create_cf(CfSpec::new("c"));
    assert!(matches!(r, Err(Error::NotEnoughReplicas { required: 1, in_sync: 0 })), "{r:?}");
    assert_eq!(m.last_seq(), 0);
    // An explicit asynchronous write needs no replica.
    assert_eq!(m.write_with(replicated_rocksdb::Batch::new(), WriteConcern::default()).err().map(|e| e.to_string()), Some("batch is empty".into()));
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    // The slave bootstraps the empty database (S = 0) and tails; then it is in sync.
    wait_until("b in sync", || m.replicas().iter().any(|r| r.in_sync)).await;
    let m = Arc::new(m);
    let m2 = m.clone();
    tokio::task::spawn_blocking(move || m2.create_cf(CfSpec::new("c"))).await.unwrap().unwrap();
    assert_converged(&*m, &[("b", &b)]).await;
}

/// Kafka-style in-sync (PROTOCOL.md §7.6): under a steady stream of writes the master is
/// almost always ahead of the replica's latest ack, yet the replica stays in sync, because an
/// ack of a batch that drained the master's log counts as caught up as of that send. Two
/// concurrent writers with N = 1 run for several in-sync windows and never get
/// `NotEnoughReplicas`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_writers_keep_the_replica_in_sync() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    cfg.serve.as_mut().unwrap().limits.replica_lag_time_max = Duration::from_millis(200);
    cfg.heartbeat = Duration::from_millis(100); // at most half the window (validated)
    let m = Arc::new(Master::open(cfg).await.unwrap());
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_until("b in sync", || m.replicas().iter().any(|r| r.in_sync)).await;
    let until = std::time::Instant::now() + Duration::from_millis(1_000);
    let writers: Vec<_> = (0..2u8)
        .map(|w| {
            let m = m.clone();
            tokio::task::spawn_blocking(move || {
                let mut n = 0u32;
                while std::time::Instant::now() < until {
                    m.put_with("c", &[w, (n % 256) as u8], b"v", WriteConcern::in_sync(1)).unwrap();
                    n += 1;
                }
                n
            })
        })
        .collect();
    let mut total = 0;
    for w in writers {
        total += w.await.unwrap();
    }
    assert!(total > 10, "{total} writes");
    assert_converged(&*m, &[("b", &b)]).await;
}
