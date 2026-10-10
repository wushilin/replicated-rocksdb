//! Write concern end to end (addendum §2, §6): real slaves acknowledge over gRPC.

mod support;

use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, Error, Master, ReadApi, SlaveState, UpstreamConfig, WriteConcern,
};
use support::{Cluster, assert_converged, wait_state, wait_until};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_returns_after_a_real_slave_acknowledged_it() {
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_until("b in sync", || {
        m.replicas().iter().any(|r| r.node_id == "b" && r.in_sync)
    })
    .await;
    for i in 0..20u8 {
        let m2 = m.clone();
        let seq = tokio::task::spawn_blocking(move || {
            m2.put_with("c", &[i], b"v", WriteConcern::in_sync(1))
        })
        .await
        .unwrap()
        .unwrap();
        assert!(
            b.last_seq() >= seq,
            "entry {seq} is committed on the slave when the write returns"
        );
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
    wait_until("both in sync", || {
        m.replicas().iter().filter(|r| r.in_sync).count() == 2
    })
    .await;
    let m2 = m.clone();
    let seq = tokio::task::spawn_blocking(move || {
        m2.put_with("c", b"acked", b"v", WriteConcern::in_sync(2))
    })
    .await
    .unwrap()
    .unwrap();
    let m = Arc::try_unwrap(m).ok().expect("the writer task is done");
    m.shutdown().await.unwrap();

    let peers = [
        UpstreamConfig::from_addr(cn.serve_addr().unwrap()),
        UpstreamConfig::from_addr(a_addr),
    ];
    let new_master = b
        .promote_checked(c.master_config("b"), &peers)
        .await
        .unwrap();
    assert!(new_master.last_seq() >= seq);
    assert_eq!(new_master.get("c", b"acked").unwrap(), Some(b"v".to_vec()));
}

/// The configured write concern applies to every data write; CF operations are exempt
/// (addendum §8): a fresh master with `min_insync_replicas = 1` creates its CF with no replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_configured_write_concern_applies_to_every_write() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    cfg.write_concern = WriteConcern {
        min_insync_replicas: 1,
        ack_timeout: Duration::from_secs(10),
    };
    let m = Master::open(cfg).await.unwrap();
    assert_eq!(
        m.create_cf(CfSpec::new("c")).unwrap(),
        1,
        "no replica needed"
    );
    let r = m.put("c", b"k", b"v");
    assert!(
        matches!(
            r,
            Err(Error::NotEnoughReplicas {
                required: 1,
                in_sync: 0
            })
        ),
        "{r:?}"
    );
    assert_eq!(m.last_seq(), 1, "the refused write consumed no seq");
    // An explicit asynchronous write needs no replica.
    assert_eq!(
        m.write_with(replicated_rocksdb::Batch::new(), WriteConcern::default())
            .err()
            .map(|e| e.to_string()),
        Some("batch is empty".into())
    );
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    m.wait_in_sync(1, support::WAIT).await.unwrap();
    let m = Arc::new(m);
    let m2 = m.clone();
    tokio::task::spawn_blocking(move || m2.put("c", b"k", b"v"))
        .await
        .unwrap()
        .unwrap();
    assert_converged(&*m, &[("b", &b)]).await;
    m.drop_cf("c").unwrap();
    assert!(matches!(
        m.wait_in_sync(2, Duration::from_millis(100)).await,
        Err(Error::Timeout)
    ));
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
                    m.put_with("c", &[w, (n % 256) as u8], b"v", WriteConcern::in_sync(1))
                        .unwrap();
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

/// Final review M2.1: CF operations are exempt from the configured write concern (addendum
/// §8): with `min_insync_replicas = 1` and no replica at all, `create_cf` and `drop_cf` both
/// succeed, while a put is refused.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn cf_operations_need_no_replica_under_a_write_concern() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    cfg.write_concern = WriteConcern::in_sync(1);
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c"))
        .expect("create_cf needs no replica");
    assert!(matches!(
        m.put("c", b"k", b"v"),
        Err(Error::NotEnoughReplicas { .. })
    ));
    m.drop_cf("c").expect("drop_cf needs no replica");
    assert!(m.list_cfs().iter().all(|s| s.name != "c"));
    assert_eq!(m.last_seq(), 2, "both CF operations were logged");
}
