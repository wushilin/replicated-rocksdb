//! Fault injection, end to end. Needs `--features failpoints`. The tests share the global
//! failpoint registry, so they run one at a time (a file-level mutex plus `FailScenario`).

mod support;

use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use replicated_rocksdb::{
    CfSpec, Error, HaltReason, LogApi, Master, MismatchPolicy, ReadApi, RepointOptions,
    RepointOutcome, Slave, SlaveState, UpstreamConfig, VerifyAbort, VerifyMode, WriteConcern,
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

/// The upstream does not answer `VerifyBegin`: the applier is held, but only for
/// `min(10 s, capture_timeout)`. Replication continues once the run gave up, and also when the
/// caller drops the `verify_now` future while the applier is held.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_applier_is_released_when_verify_begin_stalls_or_the_run_is_dropped() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.verify.capture_timeout = Duration::from_millis(600);
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    b.wait_streaming(support::WAIT).await.unwrap();

    fail::cfg("server-verify-begin", "pause").unwrap();
    // 1. The run gives up after the bound; meanwhile the slave applies nothing.
    let (r, ()) = tokio::join!(b.verify_now(VerifyMode::Quick), async {
        tokio::time::sleep(Duration::from_millis(100)).await;
        m.put("c", b"held", b"1").unwrap();
        tokio::time::sleep(Duration::from_millis(250)).await;
        assert!(b.last_seq() < m.last_seq(), "the applier is held");
    });
    assert!(
        matches!(r, Err(Error::VerifyAborted(VerifyAbort::Upstream(_)))),
        "{r:?}"
    );
    wait_until("replication continues", || b.last_seq() == m.last_seq()).await;

    // 2. The future is dropped while the applier is held.
    let r = tokio::time::timeout(Duration::from_millis(200), async {
        tokio::join!(b.verify_now(VerifyMode::Quick), async {
            tokio::time::sleep(Duration::from_millis(50)).await;
            m.put("c", b"held2", b"1").unwrap();
            std::future::pending::<()>().await
        })
    })
    .await;
    assert!(r.is_err(), "dropped by the timeout");
    wait_until("replication continues after a cancel", || {
        b.last_seq() == m.last_seq()
    })
    .await;
    fail::remove("server-verify-begin");
    scenario.teardown();
}

/// The slave's applier is frozen behind the upstream's snapshot: the run gives up with
/// `CaptureTimeout` after the configured time and releases its hold.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slave_that_cannot_reach_the_snapshot_times_out() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.verify.capture_timeout = Duration::from_millis(500);
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    b.wait_streaming(support::WAIT).await.unwrap();
    fail::cfg("slave-after-apply", "1*return").unwrap();
    m.put("c", b"a", b"1").unwrap();
    wait_until("frozen after the entry", || b.last_seq() == m.last_seq()).await;
    m.put("c", b"b", b"2").unwrap();
    m.put("c", b"c", b"3").unwrap();
    let started = std::time::Instant::now();
    let e = b.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::CaptureTimeout { snapshot_seq }) if snapshot_seq == m.last_seq()),
        "{e:?}"
    );
    assert!(started.elapsed() >= Duration::from_millis(450));
    fail::remove("slave-after-apply");
    b.kill().await.unwrap();
    scenario.teardown();
}

/// `MismatchPolicy::Rebootstrap` is consent to re-copy from the upstream that was verified. If
/// the operator repoints the slave to another database while the run is still in flight, the
/// mismatch found against the old upstream must not make the slave discard its data for the
/// new one: the repoint's own refusal (`Refuse` on a foreign identity) stands.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repoint_during_a_run_makes_its_rebootstrap_inert() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        a.put("c", &[i], &[i]).unwrap();
    }
    let (mut cfg, listener) = c.slave_config("b", a.serve_addr().unwrap());
    cfg.verify.on_mismatch = MismatchPolicy::Rebootstrap;
    let b = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    b.wait_streaming(support::WAIT).await.unwrap();
    b.shutdown().await.unwrap();
    c.delete_behind_its_back("b", "c", &[7]);

    let other = c.master("other").await;
    other.create_cf(CfSpec::new("c")).unwrap();
    other.put("c", b"x", b"1").unwrap();
    let b = std::sync::Arc::new(Slave::open(cfg).await.unwrap());
    b.wait_streaming(support::WAIT).await.unwrap();
    assert_eq!(listener.bootstraps(), 1, "the first copy");

    fail::cfg("verify-before-finish", "pause").unwrap();
    let run = tokio::spawn({
        let b = b.clone();
        async move { b.verify_now(VerifyMode::Quick).await }
    });
    tokio::time::sleep(Duration::from_millis(500)).await;
    let repoint = tokio::spawn({
        let b = b.clone();
        let to = UpstreamConfig::from_addr(other.serve_addr().unwrap());
        async move { b.repoint(to, RepointOptions::default()).await }
    });
    support::wait_state(&b, "refused the foreign database", |s| {
        *s == SlaveState::Halted(HaltReason::IdentityChanged)
    })
    .await;
    fail::remove("verify-before-finish");
    let report = run.await.unwrap().unwrap();
    assert!(!report.is_clean(), "the run did find the missing key");
    let _ = repoint.await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(
        b.status().state,
        SlaveState::Halted(HaltReason::IdentityChanged)
    );
    assert_eq!(
        listener.bootstraps(),
        1,
        "no bootstrap from the new upstream"
    );
    assert!(
        b.get("c", &[1]).unwrap().is_some(),
        "the data is still there"
    );
    scenario.teardown();
}

/// Kafka-style lag: while an entry is received but not yet applied, `lag_ms` is its age, not
/// the age of the last applied entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lag_ms_is_the_age_of_the_oldest_received_unapplied_entry() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    b.wait_streaming(support::WAIT).await.unwrap();
    m.put("c", b"a", b"1").unwrap(); // T0, applied
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().lag_ms, Some(0));

    c.clock.advance(Duration::from_secs(30));
    fail::cfg("slave-before-apply", "pause").unwrap();
    m.put("c", b"b", b"2").unwrap(); // T0 + 30 s, received, held before its commit
    c.clock.advance(Duration::from_secs(5));
    wait_until("received, not applied", || {
        let s = b.status();
        s.origin_last_seq > s.applied_seq
    })
    .await;
    assert_eq!(
        b.status().lag_ms,
        Some(5_000),
        "age of the held entry, not 35 s"
    );
    fail::remove("slave-before-apply");
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().lag_ms, Some(0));
    scenario.teardown();
}

/// A failpoint callback (`fail::cfg_callback`) that lets the first `skip` evaluations pass,
/// then records that it was reached and blocks the evaluating thread until [`Gate::open`];
/// every evaluation after that passes.
#[derive(Clone, Default)]
struct Gate {
    skip: usize,
    state: std::sync::Arc<(std::sync::Mutex<GateState>, std::sync::Condvar)>,
}

/// (evaluations, reached, open)
type GateState = (usize, bool, bool);

impl Gate {
    fn after(skip: usize) -> Gate {
        Gate {
            skip,
            ..Gate::default()
        }
    }

    fn install(&self, name: &str) {
        let gate = self.clone();
        fail::cfg_callback(name, move || gate.block()).unwrap();
    }

    fn block(&self) {
        let (m, cv) = &*self.state;
        let mut s = m.lock().unwrap();
        s.0 += 1;
        if s.0 <= self.skip || s.2 {
            return;
        }
        s.1 = true;
        cv.notify_all();
        while !s.2 {
            s = cv.wait(s).unwrap();
        }
    }

    fn reached(&self) -> bool {
        self.state.0.lock().unwrap().1
    }

    fn open(&self) {
        let (m, cv) = &*self.state;
        m.lock().unwrap().2 = true;
        cv.notify_all();
    }

    /// Opens the gate when dropped, so a failing assertion fails the test instead of leaving a
    /// worker blocked in the gate (and the runtime's drop waiting for it).
    fn open_on_drop(&self) -> OpenOnDrop {
        OpenOnDrop(self.clone())
    }
}

struct OpenOnDrop(Gate);

impl Drop for OpenOnDrop {
    fn drop(&mut self) {
        self.0.open();
    }
}

/// While a repair replays (`repair_until` above `last_seq`): reads answer `NotReady`, `Status`
/// reports `repairing:<s>`, and a promotion is refused (the slave is handed back running).
async fn assert_repairing(d: Slave, c: &Cluster, s: u64) -> Slave {
    assert!(
        matches!(d.get("c", &[0]), Err(Error::NotReady)),
        "mixed state is never served"
    );
    assert!(matches!(d.snapshot(), Err(Error::NotReady)));
    assert_eq!(d.promotion_info().node_state, format!("repairing:{s}"));
    assert!(matches!(
        d.verify_now(VerifyMode::Quick).await,
        Err(Error::NotReady)
    ));
    let Err(refused) = d.promote_checked(c.master_config("d"), &[]).await else {
        panic!("a repairing slave is not promotable");
    };
    assert!(
        matches!(refused.error, Error::Config(ref m) if m.contains("repair")),
        "{:?}",
        refused.error
    );
    refused.into_slave().expect("handed back running")
}

/// A slave killed right after its repair committed holds keys at the upstream's snapshot `S`
/// next to keys at the fork: it stays `NotReady` until replay passes `S` — after a restart with
/// no upstream (`node_state` `repairing:<S>`), and after a halt that rewrote `node_state`
/// during the replay — then converges, without any bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_repair_then_converge() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, first) = c.slave_config("d", a.serve_addr().unwrap());
    cfg.on_divergence = replicated_rocksdb::DivergencePolicy::Repair;
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    for i in 0..20u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    assert_converged(&a, &[("d", &d)]).await;

    // D receives entries the restored master will not have.
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    let fork = a.last_seq() + 1;
    a.put("c", &[1], b"lost").unwrap();
    a.delete("c", &[2]).unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let a = Master::open(mcfg).await.unwrap();
    for i in 0..5u8 {
        a.put("c", &[100 + i], b"new").unwrap(); // so replay must run past the rewind point
    }
    let s = a.last_seq();

    fail::cfg("slave-repair-after-rewind", "1*return").unwrap();
    let listener = std::sync::Arc::new(support::RecordingListener::default());
    let mut frozen_cfg = cfg.clone();
    frozen_cfg.listener = Some(listener.clone());
    let d = Slave::open(frozen_cfg).await.unwrap();
    wait_until("frozen right after the rewind", || listener.rewinds() == 1).await;
    assert!(
        matches!(d.get("c", &[0]), Err(Error::NotReady)),
        "mixed state is never served"
    );
    assert_eq!(
        d.promotion_info().node_state,
        format!("repairing:{s}"),
        "before the crash"
    );
    d.kill().await.unwrap();
    fail::remove("slave-repair-after-rewind");
    assert_eq!(listener.bootstraps(), 0);
    assert_eq!(
        support::RawDb::open(&cfg.path).last_seq(),
        fork - 1,
        "rewound to the fork"
    );

    // Restarted with no reachable upstream: still not ready, and Status says why.
    let dead = std::net::SocketAddr::from(([127, 0, 0, 1], support::reserved_port()));
    let (mut lonely, _) = c.slave_config("d", dead);
    lonely.listener = None;
    let d = Slave::open(lonely).await.unwrap();
    assert!(matches!(d.get("c", &[0]), Err(Error::NotReady)));
    assert_eq!(
        d.promotion_info().node_state,
        format!("repairing:{s}"),
        "after the restart"
    );
    d.shutdown().await.unwrap();

    // A halt that persists (another database: IdentityChanged) rewrites node_state; the
    // repair target survives it, also across a restart.
    let x = c.master("x").await;
    let (mut foreign, _) = c.slave_config("d", x.serve_addr().unwrap());
    foreign.listener = None;
    let d = Slave::open(foreign).await.unwrap();
    d.wait_streaming(support::WAIT).await.unwrap_err();
    assert!(matches!(d.get("c", &[0]), Err(Error::NotReady)));
    d.shutdown().await.unwrap();
    let d = Slave::open(cfg).await.unwrap();
    assert_eq!(
        d.status().state,
        SlaveState::Halted(replicated_rocksdb::HaltReason::IdentityChanged)
    );
    assert!(
        matches!(d.get("c", &[0]), Err(Error::NotReady)),
        "a halted slave mid-replay is not ready either"
    );
    d.clear_halt(replicated_rocksdb::HaltAction::Retry)
        .await
        .unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(d.get("c", &[1]).unwrap(), Some(b"base".to_vec()));
    assert_eq!(first.bootstraps(), 1, "only the initial bootstrap, ever");
    assert!(
        d.verify_now(replicated_rocksdb::VerifyMode::Full)
            .await
            .unwrap()
            .is_clean()
    );
    drop(x);
    scenario.teardown();
}

/// A slave `d` (repairing on divergence, one entry per Tail batch) that received entries the
/// restored master `a` will not have, at seqs `fork..`; `a` is restored from its backup with
/// its own new history. Returns the master, `d`'s config, `fork` and the master's config.
async fn diverged_slave_and_restored_master(
    c: &Cluster,
) -> (
    Master,
    replicated_rocksdb::SlaveConfig,
    std::sync::Arc<support::RecordingListener>,
    u64,
    replicated_rocksdb::MasterConfig,
) {
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    let (mut cfg, ld) = c.slave_config("d", a.serve_addr().unwrap());
    cfg.on_divergence = replicated_rocksdb::DivergencePolicy::Repair;
    // One entry per Tail batch, so the replay can be stopped between entries.
    cfg.fetch.max_batch_bytes = 1;
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    let fork = a.last_seq() + 1;
    a.put("c", &[1], b"lost").unwrap();
    a.delete_range("c", &[5], &[9]).unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let a = Master::open(mcfg.clone()).await.unwrap();
    (a, cfg, ld, fork, mcfg)
}

/// User-requested (b): the leader becomes unavailable in the middle of a repair's replay.
/// The slave keeps its repair target: reads answer `NotReady`, `Status` reports
/// `repairing:<S>`, it cannot be promoted, it serves no downstream handshake, and here the
/// master does not count it as in sync. When the leader is back, it resumes the replay and
/// converges without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_leader_lost_mid_replay_keeps_the_repair_target_and_resumes() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (a, cfg, ld, fork, mcfg) = diverged_slave_and_restored_master(&c).await;
    for i in 0..6u8 {
        a.put("c", &[100 + i], b"new").unwrap();
    }
    a.put("c", &[6], b"new").unwrap(); // inside the lost range delete
    let s = a.last_seq();

    fail::cfg("slave-repair-after-rewind", "pause").unwrap();
    let d = Slave::open(cfg.clone()).await.unwrap();
    wait_until("rewound", || ld.rewinds() == 1).await;
    assert_eq!(d.last_seq(), fork - 1, "rewound to the fork");
    let d = assert_repairing(d, &c, s).await;
    // A downstream replica of `d` is refused while `d` repairs.
    let (e_cfg, le) = c.slave_config("e", d.serve_addr().unwrap());
    let e = Slave::open(e_cfg).await.unwrap();
    wait_until("e refused by d", || {
        e.status()
            .last_error
            .is_some_and(|m| m.contains("NOT_READY"))
    })
    .await;

    // The leader sends two entries of the replay, then its Tail producer is held before it
    // sends the third: the slave cannot have received more.
    let held = Gate::after(2);
    held.install("server-tail-after-entry");
    let _held = held.open_on_drop();
    fail::remove("slave-repair-after-rewind");
    wait_until("the leader holds the third entry", || held.reached()).await;
    wait_until("two entries replayed", || d.last_seq() == fork + 1).await;
    // Illustration only, not a proof: here its acks of replayed entries have not made it an
    // in-sync replica, so a write concern of 1 is refused. The guarantee itself is structural
    // (DESIGN.md §12.1: a replica in sync acknowledged everything the master had, and a
    // replaying slave is behind the master's `S`); this test does not exercise every timing.
    let mine = a.replicas().into_iter().find(|r| r.node_id == "d");
    assert!(
        mine.is_some_and(|r| !r.in_sync && r.applied_seq < s),
        "{:?}",
        a.replicas()
    );
    let concern = WriteConcern {
        min_insync_replicas: 1,
        ack_timeout: Duration::from_secs(1),
    };
    assert!(matches!(
        a.put_with("c", b"needs-an-in-sync-replica", b"v", concern),
        Err(Error::NotEnoughReplicas { .. })
    ));
    assert_eq!(a.last_seq(), s, "nothing was written");
    // The leader goes away with the rest of the replay unsent.
    let stopping = tokio::spawn(a.shutdown());
    tokio::time::sleep(Duration::from_millis(100)).await;
    held.open();
    fail::remove("server-tail-after-entry");
    stopping.await.unwrap().unwrap();
    wait_until("the stream to the lost leader ended", || {
        d.status().state == SlaveState::Connecting && d.status().last_error.is_some()
    })
    .await;
    let mid = d.last_seq();
    assert!(
        (fork + 1..=fork + 2).contains(&mid) && mid < s,
        "mid-replay: {mid}, fork {fork}, S {s}"
    );
    let d = assert_repairing(d, &c, s).await;
    tokio::time::sleep(Duration::from_millis(300)).await; // a few failed reconnects
    assert_eq!(d.last_seq(), mid);
    let d = assert_repairing(d, &c, s).await;
    assert!(
        e.status()
            .last_error
            .is_some_and(|m| m.contains("NOT_READY")),
        "{:?}",
        e.status().last_error
    );
    assert_eq!(e.status().origin_identity, None, "e got nothing from d");

    // The leader is back: the replay resumes where it stopped.
    let a = Master::open(mcfg).await.unwrap();
    assert_converged(&a, &[("d", &d), ("e", &e)]).await;
    assert_eq!(d.get("c", &[1]).unwrap(), Some(b"base".to_vec()));
    assert_eq!(d.get("c", &[6]).unwrap(), Some(b"new".to_vec()));
    assert_eq!(d.get("c", &[7]).unwrap(), Some(b"base".to_vec()));
    assert_eq!(ld.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(ld.rewinds(), 1);
    assert_eq!(le.bootstraps(), 1, "e bootstraps from d once it is ready");
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    ld.assert_no_violations();
    scenario.teardown();
}

/// User-requested (a), deterministic: the repair's session has pinned the leader's snapshot
/// `S`, and the leader writes to the affected keys (puts, deletes, a range delete) before the
/// slave fetches. The slave takes the keys as of `S` (it reports `repairing:<S>`, not a later
/// seq), replays the writes after `S`, and matches the leader exactly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn leader_writes_between_the_snapshot_and_the_fetch_are_replayed() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (a, cfg, ld, fork, _) = diverged_slave_and_restored_master(&c).await;
    a.put("c", &[100], b"new").unwrap();
    let gate = Gate::after(0);
    gate.install("slave-repair-before-fetch");
    let _gate = gate.open_on_drop();
    fail::cfg("slave-repair-after-rewind", "pause").unwrap();
    let d = Slave::open(cfg).await.unwrap();
    wait_until("the session is open, nothing fetched yet", || {
        gate.reached()
    })
    .await;
    let s = a.last_seq();
    a.put("c", &[1], b"after-s").unwrap();
    a.delete("c", &[3]).unwrap();
    a.delete_range("c", &[5], &[8]).unwrap();
    a.put("c", &[6], b"after-s").unwrap();
    a.put("c", &[5], b"before-the-range-delete-was-undone")
        .unwrap();
    a.delete("c", &[5]).unwrap();
    gate.open();
    fail::remove("slave-repair-before-fetch");
    wait_until("rewound", || ld.rewinds() == 1).await;
    assert_eq!(d.last_seq(), fork - 1);
    let d = assert_repairing(d, &c, s).await;
    // The leader sends the replay up to `S` (here one entry, `fork`), then is held: the slave
    // becomes ready at exactly `S` and must hold the leader's state at `S`, not a later one
    // (a fetch that read live state would show the writes made after `S`).
    let at_s = Gate::after(usize::try_from(s - (fork - 1)).unwrap());
    at_s.install("server-tail-after-entry");
    let _at_s = at_s.open_on_drop();
    fail::remove("slave-repair-after-rewind");
    wait_until("ready at S, the rest held", || {
        at_s.reached() && d.last_seq() == s && d.get("c", &[0]).is_ok()
    })
    .await;
    assert_eq!(d.promotion_info().node_state, "ready");
    for k in [1u8, 3, 5, 6, 7, 8] {
        assert_eq!(
            d.get("c", &[k]).unwrap(),
            Some(b"base".to_vec()),
            "key {k} as of S"
        );
    }
    assert_eq!(d.get("c", &[100]).unwrap(), Some(b"new".to_vec()));
    at_s.open();
    fail::remove("server-tail-after-entry");
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(d.get("c", &[1]).unwrap(), Some(b"after-s".to_vec()));
    assert_eq!(d.get("c", &[5]).unwrap(), None);
    assert_eq!(d.get("c", &[7]).unwrap(), None);
    assert_eq!(d.get("c", &[8]).unwrap(), Some(b"base".to_vec()));
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1);
    scenario.teardown();
}

/// The upstream's history changes below the repair's `S` while the slave replays (the master
/// is restored from an older backup): the keys taken at `S` hold writes the new history never
/// had, so replaying it can never produce that state. The slave treats it as a divergence
/// (here: `Repair`, which cannot repair it and bootstraps) instead of becoming ready on a
/// mixed state.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repair_whose_snapshot_history_is_lost_upstream_never_becomes_ready() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (a, cfg, ld, fork, _) = diverged_slave_and_restored_master(&c).await;
    a.put("c", &[50], b"h2").unwrap(); // seq fork, kept below
    // A backup holding only that entry of the new history.
    let a = c.restart_master_after_backup(a, "a", "a-backup2").await;
    // Lost again below. Key 1 is one the slave's divergent suffix touched, so the repair
    // takes it as of `S`; nothing after `fork` that survives writes it again.
    a.put("c", &[1], b"h2-lost").unwrap();
    a.put("c", &[51], b"h2-lost").unwrap();
    let s = a.last_seq();

    fail::cfg("slave-repair-after-rewind", "pause").unwrap();
    let d = Slave::open(cfg).await.unwrap();
    wait_until("rewound", || ld.rewinds() == 1).await;
    assert_eq!(d.last_seq(), fork - 1);
    // `d` holds key 1 as of `S` ("h2-lost").
    let d = assert_repairing(d, &c, s).await;

    // The master is restored from the second backup: everything after `fork` is lost and a
    // new epoch starts there. Key 1 is never written again.
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    support::restore_dir(&c.path("a-backup2"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let a = Master::open(mcfg).await.unwrap();
    for i in 0..3u8 {
        a.put("c", &[60 + i], b"h3").unwrap();
    }
    assert!(a.last_seq() >= s, "replay would pass S on the new history");
    fail::remove("slave-repair-after-rewind");

    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(d.get("c", &[1]).unwrap(), Some(b"base".to_vec()));
    assert_eq!(d.get("c", &[50]).unwrap(), Some(b"h2".to_vec()));
    assert_eq!(d.get("c", &[51]).unwrap(), None);
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    scenario.teardown();
}

/// Fix round 1, A-I1 = B-I1: a promotion checked while the slave was still ready (its repair
/// in the network phase) must not promote the mixed state the rewind leaves behind. The final
/// check is on the store itself, after the slave stopped: the promotion is refused and the
/// slave is handed back, reopened, and finishes its replay.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rewind_during_a_checked_promotion_refuses_the_promotion() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (a, cfg, ld, fork, _) = diverged_slave_and_restored_master(&c).await;
    for i in 0..3u8 {
        a.put("c", &[100 + i], b"new").unwrap();
    }
    let s = a.last_seq();
    let fetch = Gate::after(0);
    fetch.install("slave-repair-before-fetch");
    let _fetch = fetch.open_on_drop();
    let promoting = Gate::after(0);
    promoting.install("slave-promote-before-shutdown");
    let _promoting = promoting.open_on_drop();
    let d = Slave::open(cfg).await.unwrap();
    wait_until("the repair's network phase", || fetch.reached()).await;
    assert!(d.get("c", &[0]).is_ok(), "still ready while fetching");
    let promotion = tokio::spawn(d.promote_checked(c.master_config("d"), &[]));
    wait_until("the promotion passed its checks", || promoting.reached()).await;
    // The rewind commits now; the replay is frozen right after it.
    fail::cfg("slave-repair-after-rewind", "1*return").unwrap();
    fetch.open();
    fail::remove("slave-repair-before-fetch");
    wait_until("rewound", || ld.rewinds() == 1).await;
    promoting.open();
    fail::remove("slave-promote-before-shutdown");
    let refused = match promotion.await.unwrap() {
        Ok(_) => panic!("a mixed state was promoted"),
        Err(r) => r,
    };
    assert!(
        matches!(refused.error, Error::Config(ref m) if m.contains("repair")),
        "{:?}",
        refused.error
    );
    fail::remove("slave-repair-after-rewind");
    // Still a slave (its directory was not promoted), at the fork or replaying towards `S`.
    let d = refused.into_slave().expect("reopened and handed back");
    assert!(d.last_seq() >= fork - 1 && d.last_seq() <= s);
    assert_converged(&a, &[("d", &d)]).await;
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1);
    scenario.teardown();
}

/// Fix round 1, B-I2: a repair that falls back bootstraps only from the database its consent
/// covered. Here the upstream's address serves another database by the time the fallback
/// bootstrap starts: the identity policy (`Refuse` by default) halts, nothing is installed and
/// the local data stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_fallback_bootstrap_from_another_database_follows_the_identity_policy() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    let (mut cfg, ld) = c.slave_config("d", a.serve_addr().unwrap());
    cfg.on_divergence = replicated_rocksdb::DivergencePolicy::Repair;
    cfg.repair_max_bytes = 4096;
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    a.put("c", &[1], b"lost").unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let a = Master::open(mcfg).await.unwrap();
    // Key 1's value at `S` is larger than `repair_max_bytes`: the fetch falls back.
    a.put("c", &[1], &[9u8; 8192]).unwrap();

    let gate = Gate::after(0);
    gate.install("slave-repair-before-fallback");
    let _gate = gate.open_on_drop();
    let d = Slave::open(cfg).await.unwrap();
    wait_until("the repair fell back", || gate.reached()).await;
    // The address now serves another database.
    a.shutdown().await.unwrap();
    let mut xcfg = c.master_config("x");
    xcfg.serve.as_mut().unwrap().port = port;
    let x = Master::open(xcfg).await.unwrap();
    x.create_cf(CfSpec::new("c")).unwrap();
    x.put("c", b"foreign", b"y").unwrap();
    gate.open();
    fail::remove("slave-repair-before-fallback");

    support::wait_state(&d, "IdentityChanged halt", |s| {
        *s == SlaveState::Halted(HaltReason::IdentityChanged)
    })
    .await;
    assert_eq!(
        ld.bootstraps(),
        1,
        "nothing installed from the other database"
    );
    assert_eq!(
        d.get("c", &[1]).unwrap(),
        Some(b"lost".to_vec()),
        "local data intact"
    );
    assert_eq!(d.get("c", b"foreign").unwrap(), None);
    drop(x);
    scenario.teardown();
}

/// Fix round 1, A-I2 (i): a crash after part of the replay (`T < last_seq < S`). On restart
/// the persisted target keeps the slave not ready (`repairing:<S>`) at its durable position,
/// and it resumes the replay from there and converges, without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_mid_replay_resumes_after_a_restart() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (a, cfg, ld, fork, _) = diverged_slave_and_restored_master(&c).await;
    for i in 0..6u8 {
        a.put("c", &[100 + i], b"new").unwrap();
    }
    let s = a.last_seq();
    let held = Gate::after(2);
    held.install("server-tail-after-entry");
    let _held = held.open_on_drop();
    let d = Slave::open(cfg.clone()).await.unwrap();
    wait_until("two entries replayed, the third held", || {
        held.reached() && d.last_seq() == fork + 1
    })
    .await;
    assert_eq!(ld.rewinds(), 1);
    d.kill().await.unwrap();
    // Every later Tail of the leader is held too, until the restarted slave was checked.
    let again = Gate::after(0);
    again.install("server-tail-after-entry");
    let _again = again.open_on_drop();
    held.open();
    {
        let raw = support::RawDb::open(&cfg.path);
        assert_eq!(raw.last_seq(), fork + 1, "two replayed entries are durable");
        assert_eq!(raw.meta(b"repair_until"), Some(s.to_be_bytes().to_vec()));
    }
    let d = Slave::open(cfg).await.unwrap();
    assert!(matches!(d.get("c", &[0]), Err(Error::NotReady)));
    assert_eq!(d.promotion_info().node_state, format!("repairing:{s}"));
    wait_until("the restarted slave reached the leader", || again.reached()).await;
    assert_eq!(d.last_seq(), fork + 1);
    let d = assert_repairing(d, &c, s).await;
    again.open();
    fail::remove("server-tail-after-entry");
    assert_converged(&a, &[("d", &d)]).await;
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(ld.rewinds(), 1);
    scenario.teardown();
}

/// Fix round 1, A-I2 (ii): a crash after the commit of `S` but before the repair target is
/// removed. On restart the target is at `last_seq`, so it is reached: the slave is ready at
/// once (no stuck NotReady), its state is the leader's at `S`, and the stale target is gone.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_between_reaching_s_and_clearing_the_target_is_ready_after_a_restart() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (a, cfg, ld, _fork, _) = diverged_slave_and_restored_master(&c).await;
    for i in 0..3u8 {
        a.put("c", &[100 + i], b"new").unwrap();
    }
    let s = a.last_seq();
    fail::cfg("slave-repair-before-finish", "1*return").unwrap();
    let d = Slave::open(cfg.clone()).await.unwrap();
    wait_until("S committed", || d.last_seq() == s).await;
    assert!(
        matches!(d.get("c", &[0]), Err(Error::NotReady)),
        "frozen before the target is removed"
    );
    d.kill().await.unwrap();
    fail::remove("slave-repair-before-finish");
    {
        let raw = support::RawDb::open(&cfg.path);
        assert_eq!(raw.last_seq(), s);
        assert_eq!(raw.meta(b"repair_until"), Some(s.to_be_bytes().to_vec()));
    }
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_eq!(
        d.get("c", &[1]).unwrap(),
        Some(b"base".to_vec()),
        "ready at once"
    );
    assert_eq!(d.promotion_info().node_state, "ready");
    assert_converged(&a, &[("d", &d)]).await;
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1);
    d.shutdown().await.unwrap();
    assert_eq!(
        support::RawDb::open(&cfg.path).meta(b"repair_until"),
        None,
        "the reached target was removed on open"
    );
    scenario.teardown();
}

/// Fix round 1, B-M1: `Slave::snapshot` checked readiness while the slave was still ready
/// (its repair in the network phase), then the rewind landed before the snapshot was taken.
/// The snapshot would hold the mixed state: it is refused with `NotReady` instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_snapshot_racing_a_rewind_is_refused() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (a, cfg, ld, _fork, _) = diverged_slave_and_restored_master(&c).await;
    for i in 0..3u8 {
        a.put("c", &[100 + i], b"new").unwrap();
    }
    let fetch = Gate::after(0);
    fetch.install("slave-repair-before-fetch");
    let _fetch = fetch.open_on_drop();
    let d = std::sync::Arc::new(Slave::open(cfg).await.unwrap());
    wait_until("the repair's network phase", || fetch.reached()).await;
    let checked = Gate::after(0);
    checked.install("slave-snapshot-after-check");
    let _checked = checked.open_on_drop();
    let snapping = {
        let d = d.clone();
        tokio::task::spawn_blocking(move || d.snapshot().map(|s| s.seq()))
    };
    wait_until("the snapshot passed its readiness check", || {
        checked.reached()
    })
    .await;
    fail::cfg("slave-repair-after-rewind", "pause").unwrap();
    fetch.open();
    fail::remove("slave-repair-before-fetch");
    wait_until("rewound", || ld.rewinds() == 1).await;
    checked.open();
    fail::remove("slave-snapshot-after-check");
    let got = snapping.await.unwrap();
    assert!(matches!(got, Err(Error::NotReady)), "{got:?}");
    fail::remove("slave-repair-after-rewind");
    assert_converged(&a, &[("d", &*d)]).await;
    assert!(d.snapshot().is_ok());
    scenario.teardown();
}

/// Fix round 2 (N-I1): a verify's consent never covers a divergence. The verify repair falls
/// back, and by then its upstream (same database) is a failover to a node whose history
/// leaves this slave's below its `last_seq`. The bootstrap is refused (`Retry`), nothing is
/// installed, and the next handshake halts `Diverged` under the default policy, with the
/// divergent entries kept for salvage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_verify_repair_fallback_never_discards_a_divergent_suffix() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    let (mut cfg, ld) = c.slave_config("d", a.serve_addr().unwrap());
    cfg.verify.on_mismatch = MismatchPolicy::Repair;
    cfg.repair_max_bytes = 4096;
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    let fork = a.last_seq() + 1;
    // Entries the failover target will not have; key 7 is larger than the repair budget.
    a.put("c", &[7], &[7u8; 8192]).unwrap();
    a.put("c", b"only-before-the-failover", b"x").unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    let last = d.last_seq();
    d.shutdown().await.unwrap();
    support::RawDb::open(&cfg.path).corrupt_value("c", &[7]);

    let gate = Gate::after(0);
    gate.install("slave-repair-before-fallback");
    let _gate = gate.open_on_drop();
    let d = Slave::open(cfg).await.unwrap();
    d.wait_streaming(support::WAIT).await.unwrap();
    assert!(!d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    wait_until("the verify repair fell back", || gate.reached()).await;
    // Failover: the address now serves the same database on a history without those entries.
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let a = Master::open(mcfg).await.unwrap();
    gate.open();
    fail::remove("slave-repair-before-fallback");

    support::wait_state(&d, "Diverged halt", |s| {
        *s == SlaveState::Halted(HaltReason::Diverged)
    })
    .await;
    assert_eq!(ld.bootstraps(), 1, "nothing installed");
    assert_eq!(d.last_seq(), last, "the divergent entries are kept");
    assert_eq!(
        d.diverged_entries()
            .unwrap()
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        (fork..=last).collect::<Vec<_>>()
    );
    assert_eq!(
        d.get("c", b"only-before-the-failover").unwrap(),
        Some(b"x".to_vec())
    );
    drop(a);
    scenario.teardown();
}

/// Fix round 2: a divergence repair's consent covers the suffix from the fork its handshake
/// showed. If the fallback bootstrap is offered a history that forks deeper (the upstream was
/// restored further back meanwhile), it is refused (`Retry`, nothing installed); the next
/// handshake reports the deeper divergence, which the policy (`Repair`) then handles itself.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_divergence_repair_fallback_refuses_a_deeper_fork() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    let (mut cfg, ld) = c.slave_config("d", a.serve_addr().unwrap());
    cfg.on_divergence = replicated_rocksdb::DivergencePolicy::Repair;
    cfg.repair_max_bytes = 4096;
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    let a = c.restart_master_after_backup(a, "a", "a-old").await;
    a.put("c", &[2], b"between").unwrap();
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    a.put("c", &[1], b"lost").unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    let d_epoch = a.identity().epoch;
    let port = a.serve_addr().unwrap().port();
    // Restores a backup into "a" on the same port, opened until its epoch is above the
    // slave's (an older epoch number would answer STALE_EPOCH, not DIVERGED); every open
    // starts an epoch at the same seq.
    let restore = |a: Master, backup: &'static str| {
        let c = &c;
        async move {
            a.shutdown().await.unwrap();
            support::restore_dir(&c.path(backup), &c.path("a"));
            let mut mcfg = c.master_config("a");
            mcfg.serve.as_mut().unwrap().port = port;
            let mut a = Master::open(mcfg.clone()).await.unwrap();
            while a.identity().epoch <= d_epoch {
                a = c.restart_master(a, mcfg.clone()).await;
            }
            // Larger than the repair budget: every repair against it falls back.
            a.put("c", &[1], &[9u8; 8192]).unwrap();
            a
        }
    };
    let a = restore(a, "a-backup").await;

    let first = Gate::after(0);
    first.install("slave-repair-before-fallback");
    let _first = first.open_on_drop();
    let d = Slave::open(cfg).await.unwrap();
    wait_until("the repair fell back", || first.reached()).await;
    // Restored further back: the fork is now below the one the repair was consented for.
    let a = restore(a, "a-old").await;
    let second = Gate::after(0);
    second.install("slave-repair-before-fallback");
    let _second = second.open_on_drop();
    first.open();
    wait_until("the next repair's fallback", || second.reached()).await;
    assert_eq!(ld.bootstraps(), 1, "the deeper fork was not installed");
    assert_eq!(d.get("c", &[1]).unwrap(), Some(b"lost".to_vec()));
    assert!(
        d.status().last_error.is_some_and(|m| m.contains("deeper")),
        "{:?}",
        d.status().last_error
    );
    second.open();
    fail::remove("slave-repair-before-fallback");
    wait_until("bootstrapped at the deeper fork", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    scenario.teardown();
}

/// DESIGN.md §21: the slave is killed at random points while the master keeps writing, eight
/// times. Each process resumes from the durable seq of its predecessor without a bootstrap;
/// its listener sees consecutive seqs starting right after that seq (no gap, no duplicate,
/// in order); and in the end the slave's log holds every entry exactly once.
///
/// The random points come from a seeded generator: set `RRDB_KILL_SEED` to replay a failure
/// (the seed is in every assertion message and printed at the start).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_kills_while_writing_never_lose_or_repeat_an_entry() {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicBool, Ordering};

    const ROUNDS: usize = 8;
    let seed: u64 = std::env::var("RRDB_KILL_SEED")
        .ok()
        .and_then(|s| s.parse().ok())
        .unwrap_or_else(|| {
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos() as u64
        });
    eprintln!("random_kills seed = {seed} (RRDB_KILL_SEED={seed} replays it)");
    // splitmix64
    let mut state = seed;
    let mut next = move || {
        state = state.wrapping_add(0x9E37_79B9_7F4A_7C15);
        let mut z = state;
        z = (z ^ (z >> 30)).wrapping_mul(0xBF58_476D_1CE4_E5B9);
        z = (z ^ (z >> 27)).wrapping_mul(0x94D0_49BB_133E_11B1);
        z ^ (z >> 31)
    };

    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("c")).unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (m, stop) = (m.clone(), stop.clone());
        tokio::task::spawn_blocking(move || {
            let mut n = 0u32;
            while !stop.load(Ordering::SeqCst) {
                m.put("c", &n.to_be_bytes(), &n.to_le_bytes()).unwrap();
                n += 1;
            }
            n
        })
    };

    let (cfg, first) = c.slave_config("b", m.serve_addr().unwrap());
    let mut b = Slave::open(cfg.clone()).await.unwrap();
    wait_until("the initial bootstrap", || first.bootstraps() == 1).await;
    // (listener of that process, the durable seq its process started from)
    let mut processes = vec![(first, None::<u64>)];
    for round in 0..ROUNDS {
        // A random point: 1-60 entries further on, plus up to 15 ms.
        let target = b.last_seq() + 1 + next() % 60;
        wait_until("progress before the kill", || b.last_seq() >= target).await;
        tokio::time::sleep(Duration::from_millis(next() % 16)).await;
        b.kill().await.unwrap();
        let durable = support::RawDb::open(&cfg.path).last_seq();
        let (next_cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
        b = Slave::open(next_cfg).await.unwrap();
        wait_until("delivery resumed", || !listener.entries().is_empty()).await;
        assert_eq!(
            listener.entries()[0],
            durable + 1,
            "seed {seed}, round {round}: no gap and no repeat at the restart"
        );
        assert_eq!(
            listener.bootstraps(),
            0,
            "seed {seed}, round {round}: resumed without a bootstrap"
        );
        processes.push((listener, Some(durable)));
    }
    stop.store(true, Ordering::SeqCst);
    assert!(writer.await.unwrap() > 0);
    assert_converged(&*m, &[("b", &b)]).await;

    // Listener order: within each process the seqs are consecutive and in order, and a
    // process starts right after the durable seq its predecessor left.
    let mut previous_last = 0u64;
    for (i, (listener, durable)) in processes.iter().enumerate() {
        listener.assert_no_violations();
        let seen = listener.entries();
        assert!(!seen.is_empty(), "seed {seed}: process {i} saw nothing");
        for w in seen.windows(2) {
            assert_eq!(
                w[1],
                w[0] + 1,
                "seed {seed}: process {i} has a gap or duplicate"
            );
        }
        if let Some(d) = durable {
            assert_eq!(seen[0], d + 1, "seed {seed}: process {i} start");
            // Delivery follows the commit: everything the predecessor heard of is durable
            // (the one committed-but-undelivered entry at a kill is the only one not heard of).
            assert!(
                *d >= previous_last,
                "seed {seed}: process {i}: durable {d} vs last delivered {previous_last}"
            );
        }
        previous_last = *seen.last().unwrap();
    }

    // The slave's log: every entry from the first retained one exactly once.
    let first_retained = b.first_retained_seq();
    let seqs: Vec<u64> = b
        .entries_since(first_retained - 1)
        .unwrap()
        .map(|e| e.unwrap().seq)
        .collect();
    assert_eq!(
        seqs,
        (first_retained..=b.last_seq()).collect::<Vec<_>>(),
        "seed {seed}: the slave's log has a gap or duplicate"
    );
    assert_eq!(b.last_seq(), m.last_seq());
    scenario.teardown();
}

/// Final fix wave, I1: a queued verify repair that cannot run (every later `VerifyBegin`
/// answers `BUSY`, as when other replicas hold the upstream's verify sessions) never stops
/// replication: the slave keeps applying and acknowledging new entries (write concern 1 with
/// it as the only replica), retries the repair with backoff, and repairs once a session is
/// free.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_queued_verify_repair_that_keeps_failing_does_not_stop_replication() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.write_concern = WriteConcern::in_sync(1);
    let a = Master::open(mcfg).await.unwrap();
    let (mut cfg, ld) = c.slave_config("d", a.serve_addr().unwrap());
    cfg.verify.on_mismatch = MismatchPolicy::Repair;
    let d = Slave::open(cfg.clone()).await.unwrap();
    a.wait_in_sync(1, support::WAIT).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    support::RawDb::open(&cfg.path).corrupt_value("c", &[5]);
    let d = Slave::open(cfg).await.unwrap();
    a.wait_in_sync(1, support::WAIT).await.unwrap();

    let begins = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let n = begins.clone();
    fail::cfg_callback("server-verify-begin", move || {
        n.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    })
    .unwrap();
    // The verify's own `VerifyBegin` is served; every later one (the repair's) is BUSY.
    fail::cfg("server-verify-begin-busy", "1*off->return").unwrap();
    let report = d.verify_now(VerifyMode::Full).await.unwrap();
    assert_eq!(report.mismatches.len(), 1, "{:?}", report.mismatches);
    let attempts = || begins.load(std::sync::atomic::Ordering::SeqCst) - 1;
    wait_until("the repair was attempted", || attempts() >= 1).await;
    // Each write waits for the slave's acknowledgement.
    for i in 0..20u8 {
        a.put("c", &[100 + i], b"new")
            .expect("the replica stays in sync");
    }
    d.wait_for_seq(a.last_seq(), Duration::from_secs(5))
        .await
        .expect("the slave keeps applying new entries");
    wait_until("the repair is retried", || attempts() >= 3).await;
    a.put("c", b"after-retries", b"x").expect("still in sync");
    assert_eq!(ld.rewinds(), 0, "not repaired while BUSY");
    assert_eq!(d.status().state, SlaveState::Streaming);

    fail::remove("server-verify-begin-busy");
    wait_until("repaired once a session is free", || ld.rewinds() == 1).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1, "no bootstrap");
    fail::remove("server-verify-begin");
    scenario.teardown();
}

/// Final fix wave, I2: a slave `d` (master `a`, backed up at "a-backup" first) applies
/// entries `a` will lose, then sees a gap mid-stream (`server-tail-skip-entry`) and requests a
/// bootstrap under the default `GapPolicy::Rebootstrap`. It is held in `slave-after-gap` (the
/// returned gate, reached) before its next handshake, while `a` is stopped. With `repoint`, `d`
/// was first repointed to the same upstream with the default (`Refuse`), which settled.
/// Returns `d`, its listener, the stopped master's port and the first and last entry `d`
/// holds that the backup does not.
async fn gap_then_upstream_lost(
    c: &Cluster,
    tweak: impl FnOnce(&mut replicated_rocksdb::SlaveConfig),
    repoint: bool,
) -> (
    Slave,
    std::sync::Arc<support::RecordingListener>,
    Gate,
    u16,
    u64,
    u64,
) {
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..10u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    let (mut cfg, ld) = c.slave_config("d", a.serve_addr().unwrap());
    tweak(&mut cfg);
    let d = Slave::open(cfg).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    d.wait_streaming(support::WAIT).await.unwrap();
    if repoint {
        let up = UpstreamConfig::from_addr(a.serve_addr().unwrap());
        let outcome = d.repoint(up, RepointOptions::default()).await.unwrap();
        assert_eq!(outcome, RepointOutcome::Resumed);
    }
    let fork = a.last_seq() + 1;
    a.put("c", b"only-before-the-failover", b"x").unwrap();
    a.put("c", &[1], b"lost").unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    let last = d.last_seq();

    let gate = Gate::after(0);
    gate.install("slave-after-gap");
    fail::cfg("server-tail-skip-entry", "1*return").unwrap();
    a.put("c", b"skipped", b"x").unwrap();
    a.put("c", b"after-the-gap", b"x").unwrap();
    wait_until("the slave saw the gap", || gate.reached()).await;
    fail::remove("server-tail-skip-entry");
    assert_eq!(d.last_seq(), last, "nothing past the gap applied");
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    (d, ld, gate, port, fork, last)
}

/// The backup of "a" restored with an epoch bump on `port`: the same database on a history
/// without the slave's last entries.
async fn restored_master(c: &Cluster, port: u16) -> Master {
    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    mcfg.bump_epoch_on_open = true;
    let a = Master::open(mcfg).await.unwrap();
    a.put("c", b"new-history", b"y").unwrap();
    a
}

/// Final fix wave, I2: the bootstrap a mid-stream gap requested (`GapPolicy::Rebootstrap`) is
/// consent to re-copy the same history, not the operator's consent to lose data. When the
/// next handshake answers DIVERGED (the upstream failed over to a history without the slave's
/// last entries), the configured `DivergencePolicy::Halt` decides: the slave halts with its
/// divergent entries intact. `clear_halt(Retry)` does not bring the request back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gap_request_does_not_override_a_later_divergence() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (d, ld, gate, port, fork, last) = gap_then_upstream_lost(&c, |_| {}, false).await;
    let _gate = gate.open_on_drop();
    let a = restored_master(&c, port).await;
    gate.open();
    fail::remove("slave-after-gap");

    support::wait_state(&d, "Diverged halt", |s| {
        *s == SlaveState::Halted(HaltReason::Diverged)
    })
    .await;
    assert_eq!(ld.bootstraps(), 1, "nothing installed");
    assert_eq!(d.last_seq(), last, "the divergent entries are kept");
    let suffix: Vec<u64> = d
        .diverged_entries()
        .unwrap()
        .iter()
        .map(|e| e.seq)
        .collect();
    assert_eq!(suffix, (fork..=last).collect::<Vec<_>>());

    // Retry is no consent to lose data: the next DIVERGED halts again.
    d.clear_halt(replicated_rocksdb::HaltAction::Retry)
        .await
        .unwrap();
    support::wait_state(&d, "Diverged halt again", |s| {
        *s == SlaveState::Halted(HaltReason::Diverged)
    })
    .await;
    assert_eq!(ld.bootstraps(), 1);
    assert_eq!(
        d.get("c", b"only-before-the-failover").unwrap(),
        Some(b"x".to_vec())
    );
    drop(a);
    scenario.teardown();
}

/// Final fix wave, I2: the gap request never covers another database. When the address now
/// serves a foreign database (BOOTSTRAP / IDENTITY_CHANGED), the default
/// `IdentityPolicy::Refuse` halts the slave with its data.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gap_request_does_not_bootstrap_a_foreign_database() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (d, ld, gate, port, _, last) = gap_then_upstream_lost(&c, |_| {}, false).await;
    let _gate = gate.open_on_drop();
    let mut zcfg = c.master_config("z");
    zcfg.serve.as_mut().unwrap().port = port;
    let z = Master::open(zcfg).await.unwrap();
    z.create_cf(CfSpec::new("c")).unwrap();
    gate.open();
    fail::remove("slave-after-gap");

    support::wait_state(&d, "IdentityChanged halt", |s| {
        *s == SlaveState::Halted(HaltReason::IdentityChanged)
    })
    .await;
    assert_eq!(ld.bootstraps(), 1, "the foreign database was not installed");
    assert_eq!(d.last_seq(), last);
    assert_eq!(
        d.get("c", b"only-before-the-failover").unwrap(),
        Some(b"x".to_vec())
    );
    drop(z);
    scenario.teardown();
}

/// Final fix wave, I2: after a repoint settled, its mode is `Refuse` (consent is single-use).
/// A later gap request does not skip it: a DIVERGED answer halts the slave, even though the
/// configured `DivergencePolicy::Rebootstrap` would have bootstrapped without the repoint.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_gap_request_does_not_override_a_settled_repoints_refuse() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (d, ld, gate, port, _, last) = gap_then_upstream_lost(
        &c,
        |cfg| cfg.on_divergence = replicated_rocksdb::DivergencePolicy::Rebootstrap,
        true,
    )
    .await;
    let _gate = gate.open_on_drop();
    let a = restored_master(&c, port).await;
    gate.open();
    fail::remove("slave-after-gap");

    support::wait_state(&d, "Diverged halt", |s| {
        *s == SlaveState::Halted(HaltReason::Diverged)
    })
    .await;
    assert_eq!(ld.bootstraps(), 1, "nothing installed");
    assert_eq!(d.last_seq(), last);
    drop(a);
    scenario.teardown();
}

/// Final residual m3: a bootstrap that follows a handshake is pinned to the database that
/// handshake answered for, on automatic paths too. The gap request converts the RESUME of
/// the original master into a bootstrap; before the Bootstrap RPC starts, the address serves
/// another database. The default `IdentityPolicy::Refuse` halts, nothing is installed and the
/// local data stays.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_automatic_bootstrap_is_pinned_to_the_database_the_handshake_answered_for() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let (d, ld, gate, port, _, last) = gap_then_upstream_lost(&c, |_| {}, false).await;
    let _gate = gate.open_on_drop();
    // The same database again: the next handshake answers RESUME, which the gap request
    // converts into a bootstrap.
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let a = Master::open(mcfg).await.unwrap();
    let before_rpc = Gate::after(0);
    before_rpc.install("slave-before-bootstrap-rpc");
    let _before_rpc = before_rpc.open_on_drop();
    gate.open();
    fail::remove("slave-after-gap");
    wait_until("the bootstrap was decided", || before_rpc.reached()).await;
    // The address now serves another database.
    a.shutdown().await.unwrap();
    let mut zcfg = c.master_config("z");
    zcfg.serve.as_mut().unwrap().port = port;
    let z = Master::open(zcfg).await.unwrap();
    z.create_cf(CfSpec::new("c")).unwrap();
    z.put("c", b"foreign", b"y").unwrap();
    before_rpc.open();
    fail::remove("slave-before-bootstrap-rpc");

    support::wait_state(&d, "IdentityChanged halt", |s| {
        *s == SlaveState::Halted(HaltReason::IdentityChanged)
    })
    .await;
    assert_eq!(ld.bootstraps(), 1, "the foreign database was not installed");
    assert_eq!(d.last_seq(), last);
    assert_eq!(d.get("c", b"foreign").unwrap(), None);
    assert_eq!(
        d.get("c", b"only-before-the-failover").unwrap(),
        Some(b"x".to_vec())
    );
    drop(z);
    scenario.teardown();
}
