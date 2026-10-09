//! Switching upstream at runtime (DESIGN.md §13.1, addendum §3).

mod support;

use replicated_rocksdb::admin;
use replicated_rocksdb::{
    AcceptDataLoss, CfSpec, Error, HaltAction, HaltReason, LogApi, Master, ReadApi, RepointDivergence, RepointOptions, RepointOutcome,
    Slave, SlaveState, UpstreamConfig,
};
use support::{assert_converged, wait_state, wait_until, Cluster};

fn up(addr: std::net::SocketAddr) -> UpstreamConfig {
    UpstreamConfig::from_addr(addr)
}

/// `Slave::repoint`, bounded by the harness wait.
async fn repoint(s: &Slave, upstream: UpstreamConfig, opts: RepointOptions) -> Result<RepointOutcome, Error> {
    tokio::time::timeout(support::WAIT, s.repoint(upstream, opts)).await.expect("repoint settles")
}

/// The failover runbook (DESIGN.md §13.1): the master is lost; the operator compares the
/// replicas, promotes the most advanced one with `promote_checked`, and repoints the others
/// at it without reopening them or bootstrapping.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_runbook_end_to_end() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    let (cn, lc) = c.slave("c", a.serve_addr().unwrap()).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    wait_state(&cn, "c streaming", |s| *s == SlaveState::Streaming).await;
    for i in 0..5u8 {
        a.put("c", &[i], &[i]).unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    a.shutdown().await.unwrap(); // the master is lost

    // 1. Compare. 2. Promote the candidate, checking its peers once more.
    let choice = admin::choose_promotion_candidate(&[b.promotion_info(), cn.promotion_info()]).unwrap();
    assert_eq!(choice.candidate.node_id, "b", "equal positions: lowest node id");
    assert_eq!(choice.compatible, vec!["c".to_string()]);
    assert!(choice.diverged.is_empty());
    let b = b.promote_checked(c.master_config("b"), &[up(cn.serve_addr().unwrap())]).await.unwrap();
    // 3. Repoint the others. Reads keep working throughout.
    let outcome = repoint(&cn, up(b.serve_addr().unwrap()), RepointOptions::default()).await.unwrap();
    assert_eq!(outcome, RepointOutcome::EpochAdvanced);
    assert_eq!(cn.get("c", &[4]).unwrap(), Some(vec![4]));
    b.put("c", b"after failover", b"1").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(cn.status().upstream_chain, vec!["b".to_string()]);
    wait_until("c in sync on b", || b.replicas().iter().any(|r| r.node_id == "c" && r.in_sync)).await;
    // Repointing to the upstream it already follows resumes.
    assert_eq!(repoint(&cn, up(b.serve_addr().unwrap()), RepointOptions::default()).await.unwrap(), RepointOutcome::Resumed);
}

/// D holds entries the new master never saw. Repoint refuses by default and keeps D's data
/// readable; `Repair` is not available yet; `Rebootstrap` with `AcceptDataLoss` converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn divergence_is_refused_unless_data_loss_is_accepted() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    let (d, ld) = c.slave("d", a.serve_addr().unwrap()).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d)]).await;
    let b_cfg = c.slave_config("b", a.serve_addr().unwrap()).0;
    b.shutdown().await.unwrap();
    a.put("c", b"only on d", b"2").unwrap();
    a.put("c", b"also only on d", b"3").unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    a.shutdown().await.unwrap();
    let b = Slave::open(b_cfg).await.unwrap();
    let fork = b.last_seq() + 1;
    let b = b.promote(c.master_config("b")).await.unwrap();
    let b_up = up(b.serve_addr().unwrap());

    let err = repoint(&d, b_up.clone(), RepointOptions::default()).await.unwrap_err();
    match err {
        Error::Diverged { fork_seq, local_last_seq, divergent_entries } => {
            assert_eq!((fork_seq, local_last_seq, divergent_entries), (fork, d.last_seq(), 2));
        }
        e => panic!("expected Diverged, got {e}"),
    }
    assert_eq!(d.status().state, SlaveState::Halted(HaltReason::Diverged));
    assert_eq!(d.get("c", b"only on d").unwrap(), Some(b"2".to_vec()), "data intact and readable");

    let repair = RepointOptions { on_divergence: RepointDivergence::Repair { accept_loss: AcceptDataLoss } };
    assert!(matches!(repoint(&d, b_up.clone(), repair).await, Err(Error::Unsupported(_))));
    assert_eq!(d.status().state, SlaveState::Halted(HaltReason::Diverged));
    assert_eq!(d.get("c", b"also only on d").unwrap(), Some(b"3".to_vec()));

    let rebootstrap =
        RepointOptions { on_divergence: RepointDivergence::Rebootstrap { accept_loss: AcceptDataLoss } };
    assert_eq!(repoint(&d, b_up, rebootstrap).await.unwrap(), RepointOutcome::Bootstrapped);
    b.put("c", b"after", b"4").unwrap();
    assert_converged(&b, &[("d", &d)]).await;
    assert_eq!(d.get("c", b"only on d").unwrap(), None, "the divergent entries were discarded");
    assert_eq!(ld.bootstraps(), 2);
}

/// Repointing at itself or at its own downstream halts `LoopDetected` (data intact); a
/// further repoint to a valid upstream recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repoint_refuses_loops() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, lb) = c.slave("b", a.serve_addr().unwrap()).await;
    let (cn, _) = c.slave("c", b.serve_addr().unwrap()).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let to_self = repoint(&b, up(b.serve_addr().unwrap()), RepointOptions::default()).await;
    assert!(matches!(to_self, Err(Error::Halted(HaltReason::LoopDetected))), "{to_self:?}");
    let to_downstream = repoint(&b, up(cn.serve_addr().unwrap()), RepointOptions::default()).await;
    assert!(matches!(to_downstream, Err(Error::Halted(HaltReason::LoopDetected))), "{to_downstream:?}");
    assert_eq!(b.get("c", b"k").unwrap(), Some(b"v".to_vec()));

    assert_eq!(repoint(&b, up(a.serve_addr().unwrap()), RepointOptions::default()).await.unwrap(), RepointOutcome::Resumed);
    a.put("c", b"k2", b"v2").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(lb.bootstraps(), 1);
}

/// Repointing at a restarted old master reports `Retrying` with STALE_EPOCH (not a halt);
/// repointing back resumes without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repoint_to_a_stale_master_keeps_retrying() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, lc) = c.slave("c", a_addr).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    wait_state(&cn, "c streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let a_cfg = c.master_config("a");
    a.shutdown().await.unwrap();
    let b = b.promote(c.master_config("b")).await.unwrap();
    let b_up = up(b.serve_addr().unwrap());
    assert_eq!(repoint(&cn, b_up.clone(), RepointOptions::default()).await.unwrap(), RepointOutcome::EpochAdvanced);
    b.put("c", b"k2", b"v2").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;

    let mut a_cfg = a_cfg;
    a_cfg.serve.as_mut().unwrap().port = a_addr.port();
    let old = Master::open(a_cfg).await.unwrap();
    old.put("c", b"split brain", b"x").unwrap();
    match repoint(&cn, up(a_addr), RepointOptions::default()).await.unwrap() {
        RepointOutcome::Retrying { last_error } => assert!(last_error.contains("STALE_EPOCH"), "{last_error}"),
        o => panic!("expected Retrying, got {o:?}"),
    }
    assert!(!matches!(cn.status().state, SlaveState::Halted(_)));
    assert_eq!(cn.get("c", b"split brain").unwrap(), None);

    assert_eq!(repoint(&cn, b_up, RepointOptions::default()).await.unwrap(), RepointOutcome::Resumed);
    b.put("c", b"k3", b"v3").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 1);
}

/// The repoint's mode lasts beyond its first handshake: D is configured with
/// `on_divergence: Rebootstrap`, is repointed (Refuse) at an address where nothing listens
/// yet (`Retrying`), and only then does a diverged new master start there. D halts
/// `Diverged` with its data intact instead of re-bootstrapping.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refusing_repoint_still_refuses_after_retrying() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    let (mut d_cfg, ld) = c.slave_config("d", a.serve_addr().unwrap());
    d_cfg.on_divergence = replicated_rocksdb::DivergencePolicy::Rebootstrap;
    let d = Slave::open(d_cfg).await.unwrap();
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d)]).await;
    let b_cfg = c.slave_config("b", a.serve_addr().unwrap()).0;
    b.shutdown().await.unwrap();
    a.put("c", b"only on d", b"2").unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    a.shutdown().await.unwrap();

    // A free port: nothing listens there until B is promoted onto it.
    let port = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port();
    let target = UpstreamConfig::new("127.0.0.1", port);
    assert!(matches!(repoint(&d, target, RepointOptions::default()).await.unwrap(), RepointOutcome::Retrying { .. }));

    let b = Slave::open(b_cfg).await.unwrap();
    let mut mcfg = c.master_config("b");
    mcfg.serve.as_mut().unwrap().port = port;
    let b = b.promote(mcfg).await.unwrap();
    wait_state(&d, "Diverged halt", |s| *s == SlaveState::Halted(HaltReason::Diverged)).await;
    assert_eq!(d.get("c", b"only on d").unwrap(), Some(b"2".to_vec()), "data intact");
    assert_eq!(ld.bootstraps(), 1, "the configured Rebootstrap policy was not applied");
    drop(b);
}

/// D holds an entry the (to be promoted) B never saw. Returns B's config, D, D's config and
/// D's listener; A is gone and B is not running.
async fn diverged_pair(
    c: &Cluster,
) -> (replicated_rocksdb::SlaveConfig, Slave, replicated_rocksdb::SlaveConfig, std::sync::Arc<support::RecordingListener>) {
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    let (d_cfg, ld) = c.slave_config("d", a.serve_addr().unwrap());
    let d = Slave::open(d_cfg.clone()).await.unwrap();
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d)]).await;
    let b_cfg = c.slave_config("b", a.serve_addr().unwrap()).0;
    b.shutdown().await.unwrap();
    a.put("c", b"only on d", b"2").unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    a.shutdown().await.unwrap();
    (b_cfg, d, d_cfg, ld)
}

fn free_port() -> u16 {
    std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap().port()
}

/// A forced bootstrap left over from `clear_halt(Rebootstrap)` (or a gap) is not consent to
/// discard data on a later repoint: the repoint's own mode decides.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_leftover_forced_bootstrap_does_not_override_a_refusing_repoint() {
    let c = Cluster::new();
    let (b_cfg, d, _, ld) = diverged_pair(&c).await;
    let (p1, p2) = (free_port(), free_port());
    assert!(matches!(
        repoint(&d, UpstreamConfig::new("127.0.0.1", p1), RepointOptions::default()).await.unwrap(),
        RepointOutcome::Retrying { .. }
    ));
    let b = Slave::open(b_cfg).await.unwrap();
    let mut mcfg = c.master_config("b");
    mcfg.serve.as_mut().unwrap().port = p1;
    let b = b.promote(mcfg.clone()).await.unwrap();
    wait_state(&d, "Diverged halt", |s| *s == SlaveState::Halted(HaltReason::Diverged)).await;
    b.shutdown().await.unwrap();

    // The operator asks for a rebootstrap, but the upstream is gone: the flag stays set.
    d.clear_halt(HaltAction::Rebootstrap { accept_loss: AcceptDataLoss }).await.unwrap();
    assert!(matches!(
        repoint(&d, UpstreamConfig::new("127.0.0.1", p2), RepointOptions::default()).await.unwrap(),
        RepointOutcome::Retrying { .. }
    ));
    mcfg.serve.as_mut().unwrap().port = p2;
    let b = Master::open(mcfg).await.unwrap();
    wait_state(&d, "Diverged halt", |s| *s == SlaveState::Halted(HaltReason::Diverged)).await;
    assert_eq!(d.get("c", b"only on d").unwrap(), Some(b"2".to_vec()), "data intact");
    assert_eq!(ld.bootstraps(), 1, "the leftover flag bootstrapped the slave");
    drop(b);
}

/// A restart before the new upstream is accepted does not bypass the refusal: the halt is
/// cleared in memory only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restart_before_the_repoint_settles_reopens_halted() {
    let c = Cluster::new();
    let (b_cfg, d, d_cfg, _) = diverged_pair(&c).await;
    let port = free_port();
    repoint(&d, UpstreamConfig::new("127.0.0.1", port), RepointOptions::default()).await.unwrap();
    let b = Slave::open(b_cfg).await.unwrap();
    let mut mcfg = c.master_config("b");
    mcfg.serve.as_mut().unwrap().port = port;
    let b = b.promote(mcfg).await.unwrap();
    wait_state(&d, "Diverged halt", |s| *s == SlaveState::Halted(HaltReason::Diverged)).await;
    // Repoint again at a dead address and restart before anything settles successfully.
    let r = repoint(&d, UpstreamConfig::new("127.0.0.1", free_port()), RepointOptions::default()).await.unwrap();
    assert!(matches!(r, RepointOutcome::Retrying { .. }));
    assert!(!matches!(d.status().state, SlaveState::Halted(_)), "cleared in memory");
    d.shutdown().await.unwrap();
    let d = Slave::open(d_cfg).await.unwrap();
    assert_eq!(d.status().state, SlaveState::Halted(HaltReason::Diverged), "reopens halted");
    assert_eq!(d.get("c", b"only on d").unwrap(), Some(b"2".to_vec()));
    drop(b);
}

/// The status names the current upstream and forgets the old chain until the new one answers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_reports_the_current_upstream() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    assert_eq!(b.status().upstream, format!("http://{}", a.serve_addr().unwrap()));
    let port = free_port();
    let target = UpstreamConfig::new("127.0.0.1", port);
    let uri = format!("http://127.0.0.1:{port}");
    assert!(matches!(repoint(&b, target, RepointOptions::default()).await.unwrap(), RepointOutcome::Retrying { .. }));
    assert_eq!(b.status().upstream, uri);
    assert!(b.status().upstream_chain.is_empty());
}

/// I1: `Rebootstrap { AcceptDataLoss }` is consent for its own repoint only. After that repoint
/// settles (`Bootstrapped`), the upstream is restored from an old backup without an epoch bump
/// (D is now ahead of it: `DIVERGED`). D halts with its data intact instead of discarding it on
/// the strength of the old consent.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn consent_to_lose_data_ends_when_its_repoint_settles() {
    let c = Cluster::new();
    let (b_cfg, d, _, ld) = diverged_pair(&c).await;
    let b = Slave::open(b_cfg).await.unwrap();
    let b = b.promote(c.master_config("b")).await.unwrap();
    let rebootstrap = RepointOptions { on_divergence: RepointDivergence::Rebootstrap { accept_loss: AcceptDataLoss } };
    assert_eq!(repoint(&d, up(b.serve_addr().unwrap()), rebootstrap).await.unwrap(), RepointOutcome::Bootstrapped);
    assert_converged(&b, &[("d", &d)]).await;
    assert_eq!(ld.bootstraps(), 2);

    // Back up B, write past the backup (D follows), then restore B without an epoch bump.
    let b = c.restart_master_after_backup(b, "b", "b-backup").await;
    let port = b.serve_addr().unwrap().port();
    b.put("c", b"after the backup", b"1").unwrap();
    b.put("c", b"after the backup 2", b"2").unwrap();
    assert_converged(&b, &[("d", &d)]).await;
    b.shutdown().await.unwrap();
    support::restore_dir(&c.path("b-backup"), &c.path("b"));
    let mut cfg = c.master_config("b");
    cfg.serve.as_mut().unwrap().port = port;
    let b = Master::open(cfg).await.unwrap();

    wait_state(&d, "Diverged halt", |s| *s == SlaveState::Halted(HaltReason::Diverged)).await;
    assert_eq!(d.get("c", b"after the backup 2").unwrap(), Some(b"2".to_vec()), "data intact");
    assert_eq!(ld.bootstraps(), 2, "the settled repoint's consent was not reused");
    drop(b);
}
