//! Policies and re-bootstrap, end to end: retention gaps, identity changes, divergence,
//! restores from backup and epoch bumps.

mod support;

use std::time::Duration;

use replicated_rocksdb::{
    AcceptDataLoss, CfSpec, DivergencePolicy, Error, HaltAction, HaltReason, IdentityPolicy, LogApi, Master, ReadApi, RocksTuning,
    Slave, SlaveState,
};
use support::{assert_converged, restore_dir, wait_state, wait_until, Cluster, WAIT};

fn halted(r: HaltReason) -> impl Fn(&SlaveState) -> bool {
    move |s| *s == SlaveState::Halted(r)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_gap_triggers_a_rebootstrap_that_converges() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_entries = Some(2);
    mcfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    // Wait for the Tail that presents the bootstrap lease; until then the lease holds retention.
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    let behind = b.last_seq();
    b.shutdown().await.unwrap();

    for i in 0..20u8 {
        m.put("c", &[i], &[i]).unwrap();
    }
    wait_until("master retention", || m.first_retained_seq() > behind + 1).await;
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1, "LOG_GAP with GapPolicy::Rebootstrap");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_change_is_refused_persistently_until_clear_halt_rebootstraps() {
    let c = Cluster::new();
    let m1 = c.master("a1").await;
    let port = m1.serve_addr().unwrap().port();
    m1.create_cf(CfSpec::new("old")).unwrap();
    let (cfg, _) = c.slave_config("b", m1.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m1, &[("b", &b)]).await;
    m1.shutdown().await.unwrap();

    // A different database appears at the same address.
    let mut cfg2 = c.master_config("a2");
    cfg2.serve.as_mut().unwrap().port = port;
    let m2 = Master::open(cfg2).await.unwrap();
    m2.create_cf(CfSpec::new("new")).unwrap();
    for i in 0..3u8 {
        m2.put("new", &[i], b"v").unwrap();
    }
    wait_state(&b, "IdentityChanged halt", halted(HaltReason::IdentityChanged)).await;
    assert!(matches!(b.wait_for_seq(99, WAIT).await, Err(Error::Halted(HaltReason::IdentityChanged))));
    assert_eq!(b.get("old", b"k").unwrap(), None, "reads keep working while halted");
    assert_eq!(b.list_cfs(), vec![CfSpec::new("old")]);

    // The halt survives a restart: the reopened slave does not contact its upstream.
    let (cfg, listener) = c.slave_config("b", m2.serve_addr().unwrap());
    b.shutdown().await.unwrap();
    let b = Slave::open(cfg).await.unwrap();
    assert_eq!(b.status().state, SlaveState::Halted(HaltReason::IdentityChanged));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(b.status().state, SlaveState::Halted(HaltReason::IdentityChanged));
    assert_eq!(listener.bootstraps(), 0);

    b.clear_halt(HaltAction::Rebootstrap { accept_loss: AcceptDataLoss }).await.unwrap();
    // Seqs of different databases can coincide: wait for the bootstrap before comparing.
    wait_until("the bootstrap from the new database", || listener.bootstraps() == 1).await;
    assert_converged(&m2, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1);
    assert_eq!(b.status().origin_identity, Some(m2.identity()));
    assert!(matches!(b.clear_halt(HaltAction::Retry).await, Err(Error::Config(_))), "not halted any more");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_change_with_rebootstrap_policy_converges_directly() {
    let c = Cluster::new();
    let m1 = c.master("a1").await;
    let port = m1.serve_addr().unwrap().port();
    m1.create_cf(CfSpec::new("old")).unwrap();
    let (mut cfg, listener) = c.slave_config("b", m1.serve_addr().unwrap());
    cfg.on_identity_change = IdentityPolicy::Rebootstrap;
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m1, &[("b", &b)]).await;
    m1.shutdown().await.unwrap();

    let mut cfg2 = c.master_config("a2");
    cfg2.serve.as_mut().unwrap().port = port;
    let m2 = Master::open(cfg2).await.unwrap();
    m2.create_cf(CfSpec::new("new")).unwrap();
    m2.put("new", b"k", b"v").unwrap();
    wait_until("the bootstrap from the new database", || listener.bootstraps() == 2).await;
    assert_converged(&m2, &[("b", &b)]).await;
    assert!(b.list_cfs().iter().all(|s| s.name != "old"));
}

/// A master restored from an older backup without an epoch bump: a slave that is ahead is
/// DIVERGED (rule 11) and halts by default, or re-bootstraps with `DivergencePolicy::Rebootstrap`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn master_restored_without_epoch_bump_makes_ahead_slaves_diverge() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"1").unwrap();
    let m = c.restart_master_after_backup(m, "a", "a-backup").await;
    for i in 0..3u8 {
        m.put("c", &[i], b"after backup").unwrap();
    }
    let (halting, _) = c.slave("halting", m.serve_addr().unwrap()).await;
    let (mut cfg, rebooted_listener) = c.slave_config("rebooting", m.serve_addr().unwrap());
    cfg.on_divergence = DivergencePolicy::Rebootstrap;
    let rebooting = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("halting", &halting), ("rebooting", &rebooting)]).await;
    let ahead = m.last_seq();

    let port = m.serve_addr().unwrap().port();
    m.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let m = Master::open(mcfg).await.unwrap();
    assert!(m.last_seq() < ahead);
    m.put("c", b"new history", b"x").unwrap();

    wait_state(&halting, "Diverged halt", halted(HaltReason::Diverged)).await;
    assert_eq!(halting.last_seq(), ahead, "a halted slave keeps its data");
    wait_until("the re-bootstrap after DIVERGED", || rebooted_listener.bootstraps() == 2).await;
    assert_converged(&m, &[("rebooting", &rebooting)]).await;
}

/// Restoring a backup with `bump_epoch_on_open`: a slave at or behind the restore point with
/// matching entries continues without a bootstrap (EPOCH_ADVANCED); one past it is DIVERGED.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restored_master_with_epoch_bump_keeps_behind_slaves_and_rejects_ahead_ones() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"1").unwrap();
    let (behind_cfg, _) = c.slave_config("behind", m.serve_addr().unwrap());
    let behind = Slave::open(behind_cfg).await.unwrap();
    assert_converged(&m, &[("behind", &behind)]).await;
    behind.shutdown().await.unwrap();

    let m = c.restart_master_after_backup(m, "a", "a-backup").await;
    m.put("c", b"lost", b"x").unwrap();
    let (ahead, _) = c.slave("ahead", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("ahead", &ahead)]).await;

    let port = m.serve_addr().unwrap().port();
    m.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    mcfg.bump_epoch_on_open = true;
    let m = Master::open(mcfg).await.unwrap();
    assert_eq!(m.identity().epoch, 2);
    m.put("c", b"after restore", b"y").unwrap();

    let (cfg, listener) = c.slave_config("behind", m.serve_addr().unwrap());
    let behind = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("behind", &behind)]).await;
    assert_eq!(listener.bootstraps(), 0, "EPOCH_ADVANCED: no bootstrap");
    assert_eq!(behind.status().origin_identity.unwrap().epoch, 2);
    wait_state(&ahead, "Diverged halt", halted(HaltReason::Diverged)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_epoch_bump_resets_streams_and_slaves_continue() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    let id = m.bump_epoch().unwrap();
    m.put("c", b"k", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().origin_identity, Some(id));
    assert_eq!(listener.bootstraps(), 1, "only the initial bootstrap");
    let states = listener.states();
    let streaming = states.iter().filter(|s| **s == SlaveState::Streaming).count();
    assert!(streaming >= 2, "the stream was reset and re-established: {states:?}");
}

/// A slave without a CF's comparator halts with SpecMismatch (persisted); after the
/// comparator is registered and the halt cleared with Retry, it applies the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_comparator_halts_until_registered_and_cleared() {
    let reverse = |t: &mut RocksTuning| {
        t.comparators.register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
    };
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    reverse(&mut mcfg.rocksdb);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("plain")).unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    m.create_cf(CfSpec::new("rev").with_comparator("reverse")).unwrap();
    m.put("rev", b"a", b"1").unwrap();
    m.put("rev", b"b", b"2").unwrap();
    wait_state(&b, "SpecMismatch halt", halted(HaltReason::SpecMismatch)).await;

    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    reverse(&mut cfg.rocksdb);
    b.shutdown().await.unwrap();
    let b = Slave::open(cfg).await.unwrap();
    assert_eq!(b.status().state, SlaveState::Halted(HaltReason::SpecMismatch), "persisted");
    b.clear_halt(HaltAction::Retry).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 0);
    let keys: Vec<Vec<u8>> = b.iter("rev", replicated_rocksdb::IterMode::Start).unwrap().map(|r| r.unwrap().0).collect();
    assert_eq!(keys, vec![b"b".to_vec(), b"a".to_vec()], "reverse order on the slave too");
}
