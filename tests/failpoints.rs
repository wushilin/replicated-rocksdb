//! Fault injection, end to end. Needs `--features failpoints`. The tests share the global
//! failpoint registry, so they run one at a time (a file-level mutex plus `FailScenario`).

mod support;

use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use replicated_rocksdb::{
    CfSpec, Error, LogApi, Master, ReadApi, RepointOptions, RepointOutcome, Slave, SlaveState,
    UpstreamConfig, WriteConcern,
};
use support::{Cluster, assert_converged, wait_until};

static SERIAL: Mutex<()> = Mutex::const_new(());

async fn serial() -> (MutexGuard<'static, ()>, fail::FailScenario<'static>) {
    let guard = SERIAL.lock().await;
    (guard, fail::FailScenario::setup())
}

/// A slave killed in the middle of a bootstrap restarts it from scratch on reopen and
/// converges; the server's guard of the abandoned bootstrap is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_bootstrap_restarts_the_bootstrap() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 64;
    mcfg.retention.max_entries = Some(1);
    mcfg.retention.check_interval = Duration::from_millis(20);
    // The server may finish streaming before the slave is killed; its lease then holds the
    // guard until it expires.
    mcfg.serve.as_mut().unwrap().limits.bootstrap_lease_ttl = Duration::from_millis(500);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..100u8 {
        m.put("c", &[i], &[i; 100]).unwrap();
    }
    fail::cfg("slave-bootstrap-after-chunk", "1*return").unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    wait_until(
        "frozen after the first chunk",
        || matches!(b.status().state, SlaveState::Bootstrapping { records, .. } if records > 0),
    )
    .await;
    b.kill().await.unwrap();
    fail::remove("slave-bootstrap-after-chunk");
    // The abandoned bootstrap's guard is released (the job ends when the client is gone, or
    // its unclaimed lease expires): retention runs past its S again.
    for i in 0..3u8 {
        m.put("c", &[b'x', i], b"v").unwrap();
    }
    wait_until("the abandoned guard is released", || {
        m.first_retained_seq() == m.last_seq()
    })
    .await;

    let (cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    // The interrupted bootstrap left no readable state: reads are not ready until it is redone.
    match b.get("c", &[0]) {
        Err(replicated_rocksdb::Error::NotReady) => {}
        Ok(v) => assert_eq!(
            v,
            Some(vec![0; 100]),
            "only a completed bootstrap is readable"
        ),
        Err(e) => panic!("unexpected error {e}"),
    }
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1);
    scenario.teardown();
}

/// A crash between commit and listener: the entry is durable and is not delivered again
/// (at-most-once); replication resumes after it with no gap or duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_apply_resumes_without_gap_or_duplicate() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (cfg, first) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    m.put("c", b"1", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;

    fail::cfg("slave-after-apply", "1*return").unwrap();
    let crashed = m.put("c", b"2", b"v").unwrap();
    wait_until("entry committed", || b.last_seq() == crashed).await;
    b.kill().await.unwrap();
    fail::remove("slave-after-apply");
    assert!(
        !first.entries().contains(&crashed),
        "the listener never saw the crashed entry"
    );

    for k in [&b"3"[..], b"4"] {
        m.put("c", k, b"v").unwrap();
    }
    let (cfg, second) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_eq!(
        b.get("c", b"2").unwrap(),
        Some(b"v".to_vec()),
        "durable before the crash"
    );
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(second.entries(), vec![crashed + 1, crashed + 2]);
    assert_eq!(second.bootstraps(), 0);
    scenario.teardown();
}

/// The connection drops mid-tail: the slave reconnects and resumes, no bootstrap, no
/// duplicate or missing entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_drop_mid_tail_resumes() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    let start = b.last_seq();
    fail::cfg("server-tail-after-entry", "3*return").unwrap();
    for i in 0..20u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    assert_converged(&m, &[("b", &b)]).await;
    fail::remove("server-tail-after-entry");
    assert_eq!(
        listener.entries(),
        ((start + 1)..=m.last_seq()).collect::<Vec<_>>()
    );
    assert_eq!(listener.bootstraps(), 1, "only the initial bootstrap");
    let reconnects = listener
        .states()
        .iter()
        .filter(|s| **s == SlaveState::Streaming)
        .count();
    assert!(
        reconnects >= 4,
        "three drops, three reconnects: {:?}",
        listener.states()
    );
    scenario.teardown();
}

/// Aggressive retention (`max_entries = 1`) while a bootstrap is slow: the lease keeps entry
/// S + 1, so the slave tails from S + 1 without a second bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_lease_survives_aggressive_retention() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_entries = Some(1);
    mcfg.retention.check_interval = Duration::from_millis(10);
    mcfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 64;
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        m.put("c", &[i], &[i; 100]).unwrap();
    }
    let s = m.last_seq();
    // The slave pauses after its first chunk (until the point is removed) while the master
    // writes on and retention runs.
    fail::cfg("slave-bootstrap-after-chunk", "pause").unwrap();
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_until(
        "bootstrap paused",
        || matches!(b.status().state, SlaveState::Bootstrapping { records, .. } if records > 0),
    )
    .await;
    for i in 0..10u8 {
        m.put("c", &[b'x', i], b"v").unwrap();
    }
    wait_until("retention trims up to the bootstrap guard", || {
        m.first_retained_seq() == s + 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await; // ten more retention rounds
    assert_eq!(
        m.first_retained_seq(),
        s + 1,
        "the bootstrap guard keeps S + 1"
    );
    fail::remove("slave-bootstrap-after-chunk"); // unpauses the slave
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(
        listener.bootstraps(),
        1,
        "tailed from S + 1 with the lease: no second bootstrap"
    );
    wait_until("retention resumes after the lease", || {
        m.first_retained_seq() == m.last_seq()
    })
    .await;
    scenario.teardown();
}

/// A failed epoch bump fences the master's writer until reopen (the durability of the bump
/// is unknown); the reopened master starts the next epoch and accepts writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_epoch_bump_fences_the_master_writer() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    fail::cfg("master-bump-epoch-write", "1*return").unwrap();
    assert!(m.bump_epoch().is_err());
    fail::remove("master-bump-epoch-write");
    assert!(matches!(
        m.put("c", b"k", b"v"),
        Err(replicated_rocksdb::Error::WriterFenced)
    ));
    // A write concern does not hide the fence behind NotEnoughReplicas (no replica is in sync).
    assert!(matches!(
        m.put_with("c", b"k", b"v", WriteConcern::in_sync(1)),
        Err(replicated_rocksdb::Error::WriterFenced)
    ));
    assert_eq!(m.identity().epoch, 1);
    m.shutdown().await.unwrap();
    // The failed bump persisted nothing: the reopen starts epoch 2 (every open starts one).
    let m = Master::open(c.master_config("a")).await.unwrap();
    assert_eq!(m.identity().epoch, 2);
    assert_eq!(m.epoch_history().len(), 2);
    assert_eq!(m.put("c", b"k", b"v").unwrap(), 2);
    scenario.teardown();
}

/// A repoint in the middle of a bootstrap abandons it; the bootstrap starts from scratch
/// against the new upstream and converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repoint_during_bootstrap_restarts_it_against_the_new_upstream() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 64;
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..100u8 {
        m.put("c", &[i], &[i; 100]).unwrap();
    }
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;

    fail::cfg("slave-bootstrap-after-chunk", "1*return").unwrap();
    let (cn, listener) = c.slave("c", m.serve_addr().unwrap()).await;
    wait_until(
        "frozen after the first chunk",
        || matches!(cn.status().state, SlaveState::Bootstrapping { records, .. } if records > 0),
    )
    .await;
    fail::remove("slave-bootstrap-after-chunk");
    let repoint = cn.repoint(
        UpstreamConfig::from_addr(b.serve_addr().unwrap()),
        RepointOptions::default(),
    );
    let outcome = tokio::time::timeout(support::WAIT, repoint)
        .await
        .expect("repoint settles")
        .unwrap();
    assert_eq!(outcome, RepointOutcome::Bootstrapped);
    m.put("c", b"after", b"v").unwrap();
    assert_converged(&m, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(listener.bootstraps(), 1, "one completed bootstrap");
    assert_eq!(
        cn.status().upstream_chain,
        vec!["b".to_string(), "a".to_string()]
    );
    scenario.teardown();
}

/// The only in-sync replica commits an entry but never acknowledges it: the write is
/// committed on the master and returns `AckTimeout`. After the replica comes back, the entry
/// is replicated and acknowledged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_that_stops_acking_times_the_write_out() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = std::sync::Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_until("b in sync", || {
        m.replicas().iter().any(|r| r.node_id == "b" && r.in_sync)
    })
    .await;
    let wc = WriteConcern {
        min_insync_replicas: 1,
        ack_timeout: Duration::from_millis(300),
    };
    let m2 = m.clone();
    tokio::task::spawn_blocking(move || m2.put_with("c", b"acked", b"v", wc))
        .await
        .unwrap()
        .unwrap();

    fail::cfg("slave-after-apply", "1*return").unwrap();
    let m2 = m.clone();
    let err = tokio::task::spawn_blocking(move || m2.put_with("c", b"never acked", b"v", wc))
        .await
        .unwrap()
        .unwrap_err();
    let seq = m.last_seq();
    assert!(
        matches!(err, Error::AckTimeout { seq: s, acked: 0, required: 1 } if s == seq),
        "{err}"
    );
    assert_eq!(
        m.get("c", b"never acked").unwrap(),
        Some(b"v".to_vec()),
        "committed on the master"
    );
    wait_until("committed on b, not acknowledged", || b.last_seq() == seq).await;
    assert!(m.replicas().iter().all(|r| r.applied_seq < seq));
    b.kill().await.unwrap();
    fail::remove("slave-after-apply");

    // The replica returns: the entry is there and the master sees it acknowledged.
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&*m, &[("b", &b)]).await;
    wait_until("the ack lands", || {
        m.replicas()
            .iter()
            .any(|r| r.node_id == "b" && r.applied_seq == seq)
    })
    .await;
    scenario.teardown();
}

/// M3: a commit that fails with an I/O error is retried with backoff (reported, never a
/// persisted `Corrupt` halt); the entry is applied once the storage recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_io_error_on_commit_is_retried_not_a_corrupt_halt() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    support::wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    fail::cfg("slave-commit-io", "2*return").unwrap();
    m.put("c", b"k", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    fail::remove("slave-commit-io");
    let states = listener.states();
    assert!(
        states.iter().all(|s| !matches!(s, SlaveState::Halted(_))),
        "never halted: {states:?}"
    );
    assert!(
        states
            .iter()
            .filter(|s| **s == SlaveState::Streaming)
            .count()
            >= 2,
        "the session was retried: {states:?}"
    );
    // Nothing was persisted as halted: a restart resumes.
    let b = c.restart_slave(b, b_cfg).await;
    assert!(!matches!(b.status().state, SlaveState::Halted(_)));
    m.put("c", b"k2", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    scenario.teardown();
}
