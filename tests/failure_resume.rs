//! Failure and resume without fault injection: restarts anywhere in a chain, a downstream
//! that is ahead of its upstream, and a persisted LogGap halt.

mod support;

use std::time::Duration;

use replicated_rocksdb::{
    AcceptDataLoss, CfSpec, GapPolicy, HaltAction, HaltReason, LogApi, Master, ReadApi, Slave,
    SlaveState,
};
use support::{Cluster, assert_converged, copy_dir, restore_dir, wait_state, wait_until};

/// A → B → C with writes between every restart: master, mid-chain and leaf restarts all
/// resume from the durable position, with no bootstrap, gap or duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarts_of_master_mid_chain_and_leaf_resume_without_bootstrap() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, lb) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    let (c_cfg, lc) = c.slave_config("c", b.serve_addr().unwrap());
    let cn = Slave::open(c_cfg.clone()).await.unwrap();
    let mut n = 0u32;
    let mut write = |m: &Master, count: u32| {
        for _ in 0..count {
            n += 1;
            m.put("c", &n.to_be_bytes(), &n.to_le_bytes()).unwrap();
        }
    };
    write(&a, 10);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let a = c.restart_master(a, c.master_config("a")).await;
    write(&a, 10);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let b = c.restart_slave(b, b_cfg).await;
    write(&a, 10);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let cn = c.restart_slave(cn, c_cfg.clone()).await;
    write(&a, 10);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    assert_eq!(
        (lb.bootstraps(), lc.bootstraps()),
        (1, 1),
        "only the initial bootstraps"
    );
    lb.assert_no_violations();
    lc.assert_no_violations();
}

/// B crashes after forwarding entries it had not flushed, so C is ahead of B. B answers
/// RESUME; C's Tail waits with heartbeats until B has caught up from A, then continues.
/// (A restored copy of B's directory stands in for the unflushed tail of B's log.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_downstream_ahead_of_its_upstream_waits_and_converges() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    let b_port = b.serve_addr().unwrap().port();
    b.shutdown().await.unwrap();
    copy_dir(&c.path("b"), &c.path("b-old"));
    let b = Slave::open({
        let mut cfg = b_cfg.clone();
        cfg.serve.as_mut().unwrap().port = b_port;
        cfg
    })
    .await
    .unwrap();

    let (c_cfg, lc) = c.slave_config("c", b.serve_addr().unwrap());
    let cn = Slave::open(c_cfg).await.unwrap();
    for i in 0..10u8 {
        a.put("c", &[i], b"v").unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let c_seq = cn.last_seq();

    // B loses everything after the old copy; A is stopped so B cannot catch up yet.
    let a_port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    restore_dir(&c.path("b-old"), &c.path("b"));
    let mut cfg = b_cfg.clone();
    cfg.serve.as_mut().unwrap().port = b_port;
    let b = Slave::open(cfg).await.unwrap();
    assert!(b.last_seq() < c_seq);

    // C resumes against B without a bootstrap and waits (heartbeats only).
    wait_state(&cn, "C streaming from B", |s| *s == SlaveState::Streaming).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(cn.last_seq(), c_seq);
    assert_eq!(cn.status().state, SlaveState::Streaming);

    // A comes back; B catches up and passes C; C continues.
    let mut a_cfg = c.master_config("a");
    a_cfg.serve.as_mut().unwrap().port = a_port;
    let a = Master::open(a_cfg).await.unwrap();
    a.put("c", b"after", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 1, "only C's initial bootstrap");
    lc.assert_no_violations();
}

/// GapPolicy::Halt: a slave that fell behind retention halts with LogGap, the halt survives
/// a restart, and clear_halt(Rebootstrap) recovers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_log_gap_halt_persists_across_restart_until_cleared() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_entries = Some(2);
    mcfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.on_log_gap = GapPolicy::Halt;
    let b = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    // Wait for the Tail that presents the bootstrap lease; until then the lease holds retention.
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    let behind = b.last_seq();
    b.shutdown().await.unwrap();

    for i in 0..20u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    wait_until("master retention", || m.first_retained_seq() > behind + 1).await;
    let b = Slave::open(cfg.clone()).await.unwrap();
    wait_state(&b, "LogGap halt", |s| {
        *s == SlaveState::Halted(HaltReason::LogGap)
    })
    .await;
    assert_eq!(b.last_seq(), behind);
    assert_eq!(b.list_cfs(), vec![CfSpec::new("c")], "reads keep working");

    b.shutdown().await.unwrap();
    let (mut cfg2, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg2.on_log_gap = GapPolicy::Halt;
    let b = Slave::open(cfg2).await.unwrap();
    assert_eq!(
        b.status().state,
        SlaveState::Halted(HaltReason::LogGap),
        "persisted across restart"
    );
    b.clear_halt(HaltAction::Rebootstrap {
        accept_loss: AcceptDataLoss,
    })
    .await
    .unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1);
}

/// Addendum §7: every master open starts a new epoch, and its followers move to it with
/// `EPOCH_ADVANCED`, without a bootstrap, even when the master no longer holds a follower's
/// last entry (the history proves continuity; no CRC is needed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_master_restart_moves_followers_to_the_new_epoch_without_a_bootstrap() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_entries = Some(3);
    mcfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(mcfg.clone()).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    m.put("c", b"k", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let b_last = b.last_seq();
    b.shutdown().await.unwrap();
    for i in 0..3u8 {
        m.put("c", &[i], b"w").unwrap();
    }
    wait_until("the master keeps only the entries after b's", || {
        m.first_retained_seq() == b_last + 1
    })
    .await;
    let m = c.restart_master(m, mcfg).await;
    assert_eq!(m.identity().epoch, 2);

    let b = Slave::open(b_cfg).await.unwrap();
    wait_until("b adopts the new epoch", || {
        b.status().origin_identity == Some(m.identity())
    })
    .await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    m.put("c", b"after the restart", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().origin_identity, Some(m.identity()));
    assert_eq!(b.promotion_info().epoch_history, m.epoch_history());
    assert_eq!(listener.bootstraps(), 1, "only the initial bootstrap");
    listener.assert_no_violations();
}
