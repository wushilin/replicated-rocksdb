//! Promotion of a slave to master, end to end (DESIGN.md §13.1, PROTOCOL.md §6.4).

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use replicated_rocksdb::admin::{self, Comparison};
use replicated_rocksdb::{
    AllowAll, AllowMethods, Method, CfSpec, DivergencePolicy, EpochStart, Error, HaltReason, LogApi, Master, Op, ReadApi, Role, Slave, SlaveState, UpstreamConfig,
};
use support::{assert_converged, check_edge_cases, wait_state, wait_until, write_edge_cases, Cluster};

/// Reopens slave `name` (shut down by the caller) pointed at `upstream`.
async fn reopen(c: &Cluster, name: &str, upstream: SocketAddr, tweak: impl FnOnce(&mut replicated_rocksdb::SlaveConfig)) -> (Slave, std::sync::Arc<support::RecordingListener>) {
    let (mut cfg, listener) = c.slave_config(name, upstream);
    tweak(&mut cfg);
    (Slave::open(cfg).await.unwrap(), listener)
}

/// A → {B, C, D, E}. C stops early (behind B), D and E keep following A past B.
/// A stops; B is promoted. C continues without a bootstrap (EPOCH_ADVANCED); D (default
/// policy) halts Diverged; E (Rebootstrap) re-bootstraps and converges. Writes, TTL reaping
/// and the edge-case dataset all replicate from the new master, through tail and bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_end_to_end() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    // The replicas bootstrap now, so every later entry reaches them through Tail and stays in
    // their logs: a promoted node proves continuity with the CRC of an entry it holds.
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    let (d, _) = c.slave("d", a_addr).await;
    let (e, _) = c.slave("e", a_addr).await;
    for (name, s) in [("b", &b), ("c", &cn), ("d", &d), ("e", &e)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    a.put_with_ttl("c", b"session", b"s", Duration::from_secs(60)).unwrap();
    for i in 0..5u8 {
        a.put("c", &[i], &[i]).unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn), ("d", &d), ("e", &e)]).await;
    cn.shutdown().await.unwrap(); // C stays behind
    a.put("c", b"seen by b", b"1").unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d), ("e", &e)]).await;
    b.shutdown().await.unwrap(); // B stops at the fork point
    a.put("c", b"never seen by b", b"2").unwrap();
    assert_converged(&a, &[("d", &d), ("e", &e)]).await;
    let ahead = a.last_seq();
    a.shutdown().await.unwrap(); // the operator stops the old master

    // Promote B. It must be a running slave: reopen it (its upstream is gone) and promote.
    let (b, _) = reopen(&c, "b", a_addr, |_| {}).await;
    let fork = b.last_seq() + 1;
    let mut mcfg = c.master_config("b");
    mcfg.identifier = "b".into();
    let b = b.promote(mcfg).await.unwrap();
    assert_eq!(b.identity().epoch, 2);
    assert_eq!(b.epoch_history(), vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: fork }]);
    let b_addr = b.serve_addr().unwrap();

    // C (behind the fork) continues without a bootstrap.
    let (cn, lc) = reopen(&c, "c", b_addr, |_| {}).await;
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 0);
    assert_eq!(cn.status().origin_identity, Some(b.identity()));

    // D and E (past the fork) are DIVERGED; D halts, E re-bootstraps.
    d.shutdown().await.unwrap();
    e.shutdown().await.unwrap();
    let (d, _) = reopen(&c, "d", b_addr, |_| {}).await;
    let (e, le) = reopen(&c, "e", b_addr, |cfg| cfg.on_divergence = DivergencePolicy::Rebootstrap).await;
    wait_state(&d, "Diverged halt", |s| *s == SlaveState::Halted(HaltReason::Diverged)).await;
    assert_eq!(d.last_seq(), ahead, "D keeps its data");
    wait_until("E re-bootstraps after DIVERGED", || le.bootstraps() == 1).await;
    assert_converged(&b, &[("c", &cn), ("e", &e)]).await;

    // Writes on the new master replicate; the TTL written on A before promotion is reaped by B.
    b.put("c", b"after promotion", b"3").unwrap();
    c.clock.advance(Duration::from_secs(60));
    let before_reap = b.last_seq();
    assert_eq!(b.reap_expired().unwrap().deleted, 1);
    write_edge_cases(&b, "edge");
    assert_converged(&b, &[("c", &cn), ("e", &e)]).await;
    // The new master's reaper Delete itself reached C through the log.
    let reaped: Vec<Op> = cn.entries_since(before_reap).unwrap().flat_map(|e| e.unwrap().decode_ops().unwrap()).collect();
    assert!(reaped.contains(&Op::Delete { cf: "c".into(), key: b"session".to_vec() }), "{reaped:?}");
    check_edge_cases(&cn, "edge");
    check_edge_cases(&e, "edge");

    // A fresh replica of the new master gets the edge cases through Bootstrap.
    let (f, lf) = c.slave("f", b_addr).await;
    assert_converged(&b, &[("f", &f)]).await;
    assert_eq!(lf.bootstraps(), 1);
    check_edge_cases(&f, "edge");
}

/// The old master, restarted, is refused by new-epoch replicas with STALE_EPOCH; they keep
/// retrying (not halted) and follow the new master again once re-pointed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_old_master_is_refused_with_stale_epoch_and_the_client_retries() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    // Bootstrapped and tailing before the write, so entry 2 reaches both through Tail.
    wait_state(&b, "b streaming", |st| *st == SlaveState::Streaming).await;
    wait_state(&cn, "c streaming", |st| *st == SlaveState::Streaming).await;
    a.put("c", b"through tail", b"0").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let a_cfg = c.master_config("a");
    a.shutdown().await.unwrap();

    let b = b.promote(c.master_config("b")).await.unwrap();
    cn.shutdown().await.unwrap();
    let (cn, _) = reopen(&c, "c", b.serve_addr().unwrap(), |_| {}).await;
    b.put("c", b"k", b"v").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(cn.status().origin_identity.unwrap().epoch, 2);

    // The old master comes back at its old address; C is (wrongly) pointed at it.
    let mut a_cfg = a_cfg;
    a_cfg.serve.as_mut().unwrap().port = a_addr.port();
    let old = Master::open(a_cfg).await.unwrap();
    old.put("c", b"split brain", b"x").unwrap();
    cn.shutdown().await.unwrap();
    let (cn, _) = reopen(&c, "c", a_addr, |_| {}).await;
    wait_until("STALE_EPOCH reported", || cn.status().last_error.as_deref().is_some_and(|e| e.contains("STALE_EPOCH"))).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let s = cn.status();
    assert_eq!(s.state, SlaveState::Connecting, "retrying, not halted");
    assert!(s.last_error.unwrap().contains("STALE_EPOCH"));
    assert_eq!(cn.get("c", b"split brain").unwrap(), None);

    // Re-pointed at the new master, it continues.
    cn.shutdown().await.unwrap();
    let (cn, lc) = reopen(&c, "c", b.serve_addr().unwrap(), |_| {}).await;
    b.put("c", b"k2", b"v2").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_is_refused_without_data_and_needs_released_snapshots() {
    let c = Cluster::new();
    let nobody = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let (empty, _) = c.slave("empty", nobody).await;
    assert!(matches!(empty.promote(c.master_config("empty")).await, Err(Error::Config(_))));

    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b)]).await;
    assert!(matches!(b.promote(c.master_config("elsewhere")).await, Err(Error::Config(_))), "path must match");

    let (b, _) = reopen(&c, "b", a.serve_addr().unwrap(), |_| {}).await;
    b.wait_for_seq(1, support::WAIT).await.unwrap();
    let snap = b.snapshot().unwrap();
    let err = b.promote(c.master_config("b")).await.err().expect("an open snapshot keeps the database open");
    assert!(matches!(&err, Error::Config(m) if m.contains("ReadSnapshot")), "{err}");
    drop(snap);
    // Nothing was changed: the directory is still a slave that resumes.
    let (b, lb) = reopen(&c, "b", a.serve_addr().unwrap(), |_| {}).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    assert_eq!(lb.bootstraps(), 0);
}

// ---------------- Promotion tooling (addendum §4) ----------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_status_rpc_and_promotion_info_report_positions() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    wait_until("b in sync on a", || a.replicas().iter().any(|r| r.node_id == "b" && r.in_sync)).await;

    let of_a = admin::node_status(&UpstreamConfig::from_addr(a.serve_addr().unwrap()), "operator").await.unwrap();
    assert_eq!((of_a.node_id.as_str(), of_a.role, of_a.last_seq), ("a", Role::Master, 2));
    assert_eq!(of_a.identity, Some(a.identity()));
    assert_eq!(of_a.upstream, None);
    assert_eq!(of_a.replicas.iter().map(|r| (r.node_id.as_str(), r.applied_seq, r.in_sync)).collect::<Vec<_>>(), vec![("b", 2, true)]);
    assert!(of_a.last_ts_ms > 0);

    let of_b = admin::node_status(&UpstreamConfig::from_addr(b.serve_addr().unwrap()), "operator").await.unwrap();
    assert_eq!(of_b, b.promotion_info(), "the RPC and the in-process call agree");
    assert_eq!((of_b.role, of_b.last_seq, of_b.node_state.as_str()), (Role::Slave, 2, "ready"));
    assert_eq!(of_b.last_entry_crc, of_a.last_entry_crc);
    assert_eq!(of_b.upstream, Some(format!("http://{}", a.serve_addr().unwrap())));
    assert_eq!(admin::compare(&of_a, &of_b), Comparison::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_checked_refuses_when_a_reachable_peer_is_later() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    // B and C tail every entry, so C still holds B's last entry for the CRC probe. D joins
    // later: it is bootstrapped past entry 1 and cannot prove it holds B's history.
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    for (name, s) in [("b", &b), ("c", &cn)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    a.create_cf(CfSpec::new("c")).unwrap();
    let (d, _) = c.slave("d", a_addr).await;
    assert_converged(&a, &[("b", &b), ("c", &cn), ("d", &d)]).await;
    b.shutdown().await.unwrap(); // b falls behind
    a.put("c", b"only c has it", b"v").unwrap();
    assert_converged(&a, &[("c", &cn), ("d", &d)]).await;
    a.shutdown().await.unwrap();

    let (b, _) = reopen(&c, "b", a_addr, |_| {}).await;
    let c_addr = UpstreamConfig::from_addr(cn.serve_addr().unwrap());
    let refused = b.promote_checked(c.master_config("b"), std::slice::from_ref(&c_addr)).await.err().expect("c is later");
    assert!(matches!(&refused.error, Error::NotMostAdvanced { leader, seq: 2, .. } if leader == "c"), "{refused}");
    // Refused by default: b comes back untouched and still running.
    let b = refused.into_slave().expect("a refusal returns the slave");
    // D is ahead too, but its history cannot be verified (bootstrapped past B's last entry):
    // refused as well, not treated as diverged and ignored.
    let d_addr = UpstreamConfig::from_addr(d.serve_addr().unwrap());
    let refused = b.promote_checked(c.master_config("b"), std::slice::from_ref(&d_addr)).await.err().expect("d is unverifiable");
    assert!(matches!(&refused.error, Error::PromotionUnverifiable { peer, seq: 2, .. } if peer == "d"), "{refused}");
    let b = refused.into_slave().expect("a refusal returns the slave");
    assert_eq!(b.get("c", b"only c has it").unwrap(), None, "b still serves its own data");
    assert_eq!(b.last_seq(), 1);
    // A refusal from promote's own checks returns it too.
    let refused = b.promote_checked(c.master_config("elsewhere"), &[]).await.err().expect("wrong path");
    assert!(matches!(refused.error, Error::Config(_)));
    let b = refused.into_slave().expect("a refusal returns the slave");

    // The most advanced node passes the check; the unreachable old master does not block it.
    let check = cn.check_promotion(&[UpstreamConfig::from_addr(b.serve_addr().unwrap()), UpstreamConfig::from_addr(a_addr)]).await.unwrap();
    assert_eq!(check.unreachable.len(), 1);
    assert!(check.diverged.is_empty() && check.foreign.is_empty() && check.unverifiable.is_empty());
    let new_master = cn.promote_checked(c.master_config("c"), &[UpstreamConfig::from_addr(a_addr)]).await.unwrap();
    assert_eq!(new_master.get("c", b"only c has it").unwrap(), Some(b"v".to_vec()));
    // The returned slave is fully alive: it still serves a downstream replica.
    let (e, _) = c.slave("e", b.serve_addr().unwrap()).await;
    assert_converged(&b, &[("e", &e)]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demote_undoes_a_wrong_promotion_with_zero_loss() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    for (name, s) in [("b", &b), ("c", &cn)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    for i in 0..5u8 {
        a.put("c", &[i], &[i]).unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let before = support::dump(&b);
    a.shutdown().await.unwrap();

    // The operator promotes b by mistake, then undoes it before any write.
    let b_master = b.promote(c.master_config("b")).await.unwrap();
    b_master.can_demote().unwrap();
    let (b_cfg, _) = c.slave_config("b", a_addr);
    let b = b_master.demote(b_cfg).await.unwrap();
    assert_eq!(b.status().origin_identity.unwrap().epoch, 1, "the old identity is back");
    support::assert_same("b after demote", &before, &support::dump(&b));

    // c is promoted instead; b follows it without a bootstrap: nothing was lost.
    let c_master = cn.promote(c.master_config("c")).await.unwrap();
    b.shutdown().await.unwrap();
    let (b, lb) = reopen(&c, "b", c_master.serve_addr().unwrap(), |_| {}).await;
    c_master.put("c", b"after", b"v").unwrap();
    assert_converged(&c_master, &[("b", &b)]).await;
    assert_eq!(lb.bootstraps(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demote_is_refused_once_the_new_epoch_has_entries() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    assert_converged(&a, &[("b", &b)]).await;
    a.shutdown().await.unwrap();
    let b_master = b.promote(c.master_config("b")).await.unwrap();
    let seq = b_master.put("c", b"new epoch", b"v").unwrap();
    assert!(matches!(b_master.can_demote(), Err(Error::Config(_))));
    let (b_cfg, _) = c.slave_config("b", a_addr);
    let refused = b_master.demote(b_cfg).await.err().expect("the new epoch has entries");
    assert!(matches!(refused.error, Error::Config(_)), "{refused}");
    // Refused by default: the master comes back untouched and still running.
    let b_master = refused.into_master().expect("a refusal returns the master");
    assert_eq!((b_master.last_seq(), b_master.get("c", b"new epoch").unwrap()), (seq, Some(b"v".to_vec())));
    let (d, _) = c.slave("d", b_master.serve_addr().unwrap()).await;
    b_master.put("c", b"still writable", b"v").unwrap();
    assert_converged(&b_master, &[("d", &d)]).await;
}

/// Same-epoch divergence (a master restored from a backup without an epoch bump): epoch
/// histories agree, so only the CRC probe shows that B's entry 4 is not C's entry 4.
/// `gather` + the chooser flag B as diverged, and B's checked promotion is refused: C holds entries 4..=5 that
/// promoting B would discard.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn same_epoch_divergence_is_found_by_the_crc_probe() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    a.put("c", b"1", b"v").unwrap();
    a.put("c", b"2", b"v").unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let a = c.restart_master_after_backup(a, "a", "a-backup").await; // backup at seq 3
    let b_cfg = c.slave_config("b", a_addr).0;
    b.shutdown().await.unwrap();
    a.put("c", b"original 4", b"v").unwrap();
    a.put("c", b"original 5", b"v").unwrap();
    assert_converged(&a, &[("c", &cn)]).await;
    // The operator restores A from the backup and forgets the epoch bump.
    a.shutdown().await.unwrap();
    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut a_cfg = c.master_config("a");
    a_cfg.serve.as_mut().unwrap().port = a_addr.port();
    let a = Master::open(a_cfg).await.unwrap();
    a.put("c", b"different 4", b"v").unwrap();
    let b = Slave::open(b_cfg).await.unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    assert_eq!((b.last_seq(), cn.last_seq()), (4, 5));
    a.shutdown().await.unwrap();

    let peers = [UpstreamConfig::from_addr(b.serve_addr().unwrap()), UpstreamConfig::from_addr(cn.serve_addr().unwrap())];
    let gathered = admin::gather(&peers, "operator").await;
    assert!(gathered.unreachable.is_empty());
    let choice = admin::choose_promotion_candidate(&gathered.nodes).unwrap();
    assert_eq!(choice.candidate.node_id, "c");
    assert_eq!(choice.diverged, vec![("b".to_string(), 4)], "same epoch, different entry 4");
    assert!(choice.compatible.is_empty());
    // C holds entries 4..=5 that promoting B would discard: refused by default.
    let err = b.check_promotion(&peers[1..]).await.expect_err("c holds entries b would discard");
    assert!(matches!(err, Error::PromotionWouldDiscard { ref peer, fork: 4, peer_last_seq: 5 } if peer == "c"), "{err}");
}

/// A reachable peer that is itself a master (promoted by mistake) blocks `promote_checked`
/// with `MasterReachable`, whatever its position; the slave comes back running. After the
/// wrong master is demoted, the same check passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_checked_refuses_while_another_master_is_reachable() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    a.shutdown().await.unwrap();

    // b is promoted by mistake: epoch 2 starts after its last_seq and is empty.
    let b_master = b.promote(c.master_config("b")).await.unwrap();
    let b_peer = UpstreamConfig::from_addr(b_master.serve_addr().unwrap());
    let refused = cn.promote_checked(c.master_config("c"), std::slice::from_ref(&b_peer)).await.err().expect("b is a master");
    assert!(matches!(&refused.error, Error::MasterReachable { peer } if peer == "b"), "{refused}");
    let cn = refused.into_slave().expect("a refusal returns the slave");
    assert_eq!(cn.get("c", b"nothing").unwrap(), None, "c still serves reads");

    // Demote b (nothing was written), then c may be promoted with b as a replica peer.
    let (b_cfg, _) = c.slave_config("b", a_addr);
    let b = b_master.demote(b_cfg).await.unwrap();
    let b_peer = UpstreamConfig::from_addr(b.serve_addr().unwrap());
    let c_master = cn.promote_checked(c.master_config("c"), std::slice::from_ref(&b_peer)).await.unwrap();
    // b burned epoch 2 by demoting (a replica may have followed it): c starts epoch 3.
    assert_eq!(b.promotion_info().max_burned_epoch, 2);
    assert_eq!(c_master.identity().epoch, 3);
}

/// A peer that is reachable but refuses `Status` (here: its authenticator excludes it) has an
/// unknown state, so it blocks as `PromotionUnverifiable` and is not treated as unreachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_that_answers_with_an_error_blocks_promote_checked() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (mut d_cfg, _) = c.slave_config("d", a_addr);
    d_cfg.serve.as_mut().unwrap().authenticator =
        std::sync::Arc::new(AllowMethods::new(std::sync::Arc::new(AllowAll), &[Method::Handshake, Method::Tail, Method::Bootstrap, Method::Verify]));
    let d = Slave::open(d_cfg).await.unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d)]).await;
    a.shutdown().await.unwrap();

    let d_peer = UpstreamConfig::from_addr(d.serve_addr().unwrap());
    let refused = b.promote_checked(c.master_config("b"), std::slice::from_ref(&d_peer)).await.err().expect("d refuses Status");
    assert!(matches!(&refused.error, Error::PromotionUnverifiable { peer, .. } if peer.contains("PermissionDenied") || peer.contains("permission")), "{refused}");
    let b = refused.into_slave().expect("a refusal returns the slave");
    let check = b.check_promotion(&[UpstreamConfig::from_addr(a_addr)]).await.unwrap();
    assert_eq!(check.unreachable.len(), 1, "a refused connection is unreachable, not blocking");
}

/// A refusal returns a slave that keeps replicating from its live upstream.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_slave_keeps_replicating_from_its_live_upstream() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b)]).await;
    let refused = b.promote_checked(c.master_config("elsewhere"), &[]).await.err().expect("wrong path");
    let b = refused.into_slave().expect("a refusal returns the slave");
    a.put("c", b"after refusal", b"v").unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    assert_eq!(b.get("c", b"after refusal").unwrap(), Some(b"v".to_vec()));
}

/// A slave whose newest epoch is empty (it follows a promotion that took no writes) against a
/// slave of the old epoch that holds entries past that epoch's start: the empty one must not be
/// promoted (it would discard those entries); the other one may be (the empty one has nothing
/// at or past the fork), reporting it as diverged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_that_would_discard_entries_is_refused() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    wait_state(&b, "b", |s| *s == SlaveState::Streaming).await;
    wait_state(&cn, "c", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"1", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    b.shutdown().await.unwrap();
    a.put("c", b"2", b"v").unwrap();
    a.put("c", b"3", b"v").unwrap();
    assert_converged(&a, &[("c", &cn)]).await;
    a.shutdown().await.unwrap();

    // B is promoted (epoch 2, empty); D follows it and has the empty epoch too.
    let (b, _) = reopen(&c, "b", a_addr, |_| {}).await;
    let fork = b.last_seq() + 1;
    let b_master = b.promote(c.master_config("b")).await.unwrap();
    let (d, _) = c.slave("d", b_master.serve_addr().unwrap()).await;
    wait_state(&d, "d streaming", |s| *s == SlaveState::Streaming).await;
    assert_converged(&b_master, &[("d", &d)]).await;
    let c_peer = UpstreamConfig::from_addr(cn.serve_addr().unwrap());

    let refused = d.promote_checked(c.master_config("d"), std::slice::from_ref(&c_peer)).await.err().expect("c holds entries");
    assert!(
        matches!(&refused.error, Error::PromotionWouldDiscard { peer, fork: f, peer_last_seq } if peer == "c" && *f == fork && *peer_last_seq == cn.last_seq()),
        "{refused}"
    );
    let d = refused.into_slave().expect("the slave is returned");
    let d_peer = UpstreamConfig::from_addr(d.serve_addr().unwrap());

    // The other way round nothing is discarded: reported, not blocking.
    let check = cn.check_promotion(std::slice::from_ref(&d_peer)).await.unwrap();
    assert_eq!(check.diverged, vec![("d".to_string(), fork)]);
}

// ---------------- Epoch numbers are never reused (C1) ----------------

/// The setup of the epoch-reuse failure: A → {W, X, R}. W and X stop at `s_w`; R follows A to
/// `s_r > s_w`; A is lost. W is promoted by mistake (epoch 2 from `s_w + 1`), X follows it
/// (adopting that epoch), and W is demoted again: epoch 2 is burned, but X still holds it.
/// Returns W (a slave again), X, R (a slave of the dead A), X's listener and `s_w`.
async fn wrong_promotion_followed_then_undone(c: &Cluster) -> (Slave, Slave, Slave, std::sync::Arc<support::RecordingListener>, u64) {
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (w, _) = c.slave("w", a_addr).await;
    let (x, _) = c.slave("x", a_addr).await;
    let (r, _) = c.slave("r", a_addr).await;
    for (name, s) in [("w", &w), ("x", &x), ("r", &r)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    for i in 0..3u8 {
        a.put("c", &[i], &[i]).unwrap();
    }
    assert_converged(&a, &[("w", &w), ("x", &x), ("r", &r)]).await;
    let s_w = a.last_seq();
    w.shutdown().await.unwrap();
    x.shutdown().await.unwrap();
    a.put("c", b"only r", b"1").unwrap();
    a.put("c", b"only r too", b"2").unwrap();
    assert_converged(&a, &[("r", &r)]).await;
    a.shutdown().await.unwrap();

    let (w, _) = reopen(c, "w", a_addr, |_| {}).await;
    let w_master = w.promote(c.master_config("w")).await.unwrap();
    assert_eq!(w_master.epoch_history().last(), Some(&EpochStart { epoch: 2, start_seq: s_w + 1 }));
    let (x, lx) = reopen(c, "x", w_master.serve_addr().unwrap(), |_| {}).await;
    wait_until("x adopts w's epoch", || x.status().origin_identity.is_some_and(|id| id.epoch == 2)).await;
    let w = w_master.demote(c.slave_config("w", a_addr).0).await.unwrap();
    assert_eq!(w.promotion_info().max_burned_epoch, 2, "the undone epoch is burned");
    assert_eq!(x.promotion_info().epoch_history.last(), Some(&EpochStart { epoch: 2, start_seq: s_w + 1 }));
    (w, x, r, lx, s_w)
}

/// `promote_checked` never reuses an epoch number a peer reports (here: W's burned epoch and
/// X's history), so the sibling X that followed the undone promotion is told apart from the new
/// master's epoch, continues without a loop, and loses nothing: it never held an entry of W's
/// epoch, so continuity is proven (EPOCH_ADVANCED) and no bootstrap is needed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_checked_never_reuses_a_burned_epoch_and_the_sibling_converges() {
    let c = Cluster::new();
    let (w, x, r, lx, _) = wrong_promotion_followed_then_undone(&c).await;
    let peers = [UpstreamConfig::from_addr(w.serve_addr().unwrap()), UpstreamConfig::from_addr(x.serve_addr().unwrap())];
    let r = r.promote_checked(c.master_config("r"), &peers).await.unwrap();
    assert!(r.identity().epoch > 2, "the new epoch {} reuses the burned epoch 2", r.identity().epoch);
    assert_eq!(r.identity().epoch, 3);

    let outcome = tokio::time::timeout(
        support::WAIT,
        x.repoint(UpstreamConfig::from_addr(r.serve_addr().unwrap()), replicated_rocksdb::RepointOptions::default()),
    )
    .await
    .expect("repoint settles")
    .unwrap();
    assert_eq!(outcome, replicated_rocksdb::RepointOutcome::EpochAdvanced);
    r.put("c", b"after", b"v").unwrap();
    assert_converged(&r, &[("x", &x)]).await;
    assert_eq!(x.promotion_info().epoch_history, r.epoch_history(), "x follows r's history");
    assert_eq!(lx.bootstraps(), 0, "no bootstrap since x was reopened");
    assert_eq!(x.status().last_error, None);
    lx.assert_no_violations();
    drop(w);
}

/// Plain `promote` asks no peer and picks epoch 2 again (from another seq). The handshake's
/// `known_epoch_start` tells the two epochs 2 apart: X is not resumed into a history that
/// contradicts its own (which looped every 50 ms before), it advances to R's epoch 2 and
/// converges, or it would be refused DIVERGED had it held entries of W's epoch.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_reused_epoch_number_is_detected_by_its_start_seq() {
    let c = Cluster::new();
    let (w, x, r, lx, s_w) = wrong_promotion_followed_then_undone(&c).await;
    let s_r = r.last_seq();
    let r = r.promote(c.master_config("r")).await.unwrap();
    assert_eq!(r.epoch_history().last(), Some(&EpochStart { epoch: 2, start_seq: s_r + 1 }), "same number, other start");
    assert_ne!(s_r, s_w);

    let outcome = tokio::time::timeout(
        support::WAIT,
        x.repoint(UpstreamConfig::from_addr(r.serve_addr().unwrap()), replicated_rocksdb::RepointOptions::default()),
    )
    .await
    .expect("repoint settles")
    .unwrap();
    assert_eq!(outcome, replicated_rocksdb::RepointOutcome::EpochAdvanced);
    r.put("c", b"after", b"v").unwrap();
    assert_converged(&r, &[("x", &x)]).await;
    assert_eq!(x.promotion_info().epoch_history, r.epoch_history());
    assert_eq!(x.get("c", b"only r").unwrap(), Some(b"1".to_vec()));
    assert_eq!(lx.bootstraps(), 0, "no bootstrap since x was reopened: nothing lost");
    assert_eq!(x.status().last_error, None);
    lx.assert_no_violations();
    drop(w);
}
