//! The `Node` handle (DESIGN.md §13.2), with real nodes over 127.0.0.1.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use replicated_rocksdb::{
    AcceptDataLoss, AcceptSplitBrainRisk, CfSpec, Error, FenceReason, HaltReason, Master, Node,
    NodeState, RejoinOptions, RepointDivergence, RepointOptions, RepointOutcome, Slave, SlaveState,
    UnfenceOptions, UpstreamConfig,
};
use support::{Cluster, RawDb, WAIT, node_cfg, reserved_port, wait_until};

fn up(a: SocketAddr) -> UpstreamConfig {
    UpstreamConfig::from_addr(a)
}

/// An address nothing listens on.
fn dead_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], reserved_port()))
}

async fn wait_node(n: &Node, what: &str, f: impl Fn(&NodeState) -> bool) {
    wait_until(what, || f(&n.state())).await;
}

fn streaming(s: &NodeState) -> bool {
    matches!(
        s,
        NodeState::Slave {
            state: SlaveState::Streaming,
            ..
        }
    )
}

fn is_master(s: &NodeState) -> bool {
    matches!(s, NodeState::Master { .. })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_open_each_role() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    assert!(is_master(&a.state()));
    a.create_cf(CfSpec::new("c")).unwrap();
    a.put("c", b"k", b"v").unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[]))
        .await
        .unwrap();
    wait_node(&b, "b streaming", streaming).await;
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    assert_eq!(b.get("c", b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(b.list_cfs().unwrap().len(), 1);
    assert!(matches!(b.put("c", b"k", b"x"), Err(Error::NotMaster)));
    assert!(matches!(b.bump_epoch(), Err(Error::NotMaster)));
    assert!(matches!(a.status(), Err(Error::NotSlave)));
    assert!(matches!(a.diverged_entries(), Err(Error::NotSlave)));

    b.shutdown().await.unwrap();
    assert_eq!(b.state(), NodeState::Stopped);
    assert!(matches!(b.get("c", b"k"), Err(Error::NodeFailed(_))));
    let b = Node::open(node_cfg(&c, "b", a_addr, &[])).await.unwrap();
    wait_node(&b, "b streaming after open", streaming).await;

    let NodeState::Master { identity } = a.state() else {
        panic!()
    };
    a.shutdown().await.unwrap();
    let mut cfg = node_cfg(&c, "a", dead_addr(), &[]);
    cfg.unchecked_master = Some(AcceptSplitBrainRisk);
    let a = Node::open(cfg).await.unwrap();
    match a.state() {
        NodeState::Master { identity: now } => assert_eq!(now.epoch, identity.epoch + 1),
        s => panic!("expected Master, got {s:?}"),
    }
    let id = a.bump_epoch().unwrap();
    assert_eq!(a.state(), NodeState::Master { identity: id });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_needs_an_empty_directory_and_open_a_stored_role() {
    let c = Cluster::new();
    assert!(matches!(
        Node::open(node_cfg(&c, "a", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.shutdown().await.unwrap();
    assert!(matches!(
        Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        Node::create_slave(node_cfg(&c, "a", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    let mut cfg = node_cfg(&c, "x", dead_addr(), &[]);
    cfg.slave.path = c.path("y");
    assert!(matches!(
        Node::create_master(cfg).await,
        Err(Error::Config(_))
    ));
    let mut cfg = node_cfg(&c, "x", dead_addr(), &[]);
    cfg.slave.identifier = "y".into();
    assert!(matches!(
        Node::create_slave(cfg).await,
        Err(Error::Config(_))
    ));
}

/// A stored master opened without peers stays fenced: no epoch, no port, no reads or writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stored_master_without_peers_opens_fenced() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    a.put("c", b"k", b"v").unwrap();
    let NodeState::Master { identity } = a.state() else {
        panic!()
    };
    a.shutdown().await.unwrap();

    let a = Node::open(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    assert_eq!(
        a.state(),
        NodeState::Fenced {
            reason: FenceReason::NoPeers
        }
    );
    assert!(matches!(
        a.get("c", b"k"),
        Err(Error::Fenced {
            reason: FenceReason::NoPeers
        })
    ));
    assert!(matches!(a.put("c", b"k", b"x"), Err(Error::Fenced { .. })));
    assert!(matches!(a.snapshot(), Err(Error::Fenced { .. })));
    assert!(matches!(a.last_seq(), Err(Error::Fenced { .. })));
    assert_eq!(a.serve_addr(), None, "nothing is served");
    a.shutdown().await.unwrap();

    let mut cfg = node_cfg(&c, "a", dead_addr(), &[]);
    cfg.unchecked_master = Some(AcceptSplitBrainRisk);
    let a = Node::open(cfg).await.unwrap();
    match a.state() {
        NodeState::Master { identity: now } => {
            assert_eq!(
                now.epoch,
                identity.epoch + 1,
                "the fenced open started no epoch"
            )
        }
        s => panic!("expected Master, got {s:?}"),
    }
    assert_eq!(a.get("c", b"k").unwrap(), Some(b"v".to_vec()));
}

/// A → B. A dies, B is promoted; A restarts through `Node` and is fenced by B.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_old_master_restarts_fenced_by_the_new_one() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    a.put("c", b"k", b"v").unwrap();
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    // Streaming, not only caught up: a slave caught up by its bootstrap still reads
    // `Bootstrapping` until its next handshake, and refuses promotion.
    wait_until("b streaming", || b.status().state == SlaveState::Streaming).await;
    a.shutdown().await.unwrap();
    let b = b.promote(c.master_config("b")).await.unwrap();
    let b_addr = b.serve_addr().unwrap();

    let a = Node::open(node_cfg(&c, "a", b_addr, &[b_addr]))
        .await
        .unwrap();
    assert_eq!(
        a.state(),
        NodeState::Fenced {
            reason: FenceReason::PeerIsMaster { peer: "b".into() }
        }
    );
    assert!(matches!(a.get("c", b"k"), Err(Error::Fenced { .. })));
}

/// Wrapping checks the directory, and for a slave also the upstream it follows; a refused
/// handle is shut down.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_handles_can_be_wrapped() {
    let c = Cluster::new();
    let m = Master::open(c.master_config("a")).await.unwrap();
    let a = Node::from_master(m, node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let s = Slave::open(c.slave_config("b", a_addr).0).await.unwrap();
    let b = Node::from_slave(s, node_cfg(&c, "b", a_addr, &[]))
        .await
        .unwrap();
    wait_node(&b, "b streaming", streaming).await;

    let other = Master::open(c.master_config("z")).await.unwrap();
    assert!(matches!(
        Node::from_master(other, node_cfg(&c, "y", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    // The refused handle was shut down: its directory opens again.
    Master::open(c.master_config("z"))
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();

    // A slave following another upstream than `cfg.slave.upstream` is refused and shut down.
    let s = Slave::open(c.slave_config("w", a_addr).0).await.unwrap();
    assert!(matches!(
        Node::from_slave(s, node_cfg(&c, "w", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    Slave::open(c.slave_config("w", a_addr).0)
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
}

/// Slave sub-states are forwarded live; `repoint` updates the upstream used by later reopens;
/// review focus: a refused repoint (invalid address) does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_state_forwards_slave_states_and_repoint_updates_the_upstream() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", dead_addr(), &[]))
        .await
        .unwrap();
    let mut rx = b.watch_state();
    assert!(matches!(b.state(), NodeState::Slave { .. }));
    assert_eq!(
        b.repoint(up(a_addr), RepointOptions::default())
            .await
            .unwrap(),
        RepointOutcome::Bootstrapped
    );
    tokio::time::timeout(WAIT, rx.wait_for(streaming))
        .await
        .unwrap()
        .unwrap();
    match b.state() {
        NodeState::Slave { upstream, .. } => assert_eq!(upstream, format!("http://{a_addr}")),
        s => panic!("expected Slave, got {s:?}"),
    }
    assert!(matches!(
        b.repoint(
            UpstreamConfig::new("127.0.0.1", 0),
            RepointOptions::default()
        )
        .await,
        Err(Error::Config(_))
    ));
    b.shutdown().await.unwrap();
    b.reopen().await.unwrap();
    wait_node(&b, "b streams from a after reopen", streaming).await;
}

/// `reopen` only acts on a failed or shut-down node; `shutdown` of a fenced node stops it, and
/// `reopen` then runs the fence check again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn reopen_needs_a_stopped_node_and_checks_the_fence_again() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    assert!(matches!(a.reopen().await, Err(Error::Config(_))));
    assert!(is_master(&a.state()), "a refused reopen changes nothing");
    a.shutdown().await.unwrap();
    a.reopen().await.unwrap();
    assert_eq!(
        a.state(),
        NodeState::Fenced {
            reason: FenceReason::NoPeers
        }
    );
    assert!(matches!(a.reopen().await, Err(Error::Config(_))));
    a.shutdown().await.unwrap();
    assert_eq!(a.state(), NodeState::Stopped);
    assert!(matches!(a.put("c", b"k", b"v"), Err(Error::NodeFailed(_))));
}

/// Review focus: a `repoint` future dropped before its handshake settles (the documented way
/// to bound the wait) still sets the upstream later reopens use.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_dropped_repoint_still_sets_the_upstream_for_reopen() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", dead_addr(), &[]))
        .await
        .unwrap();
    // A zero timeout polls the repoint once: it adopts the new upstream, then is dropped
    // while it waits for the handshake.
    assert!(
        tokio::time::timeout(
            std::time::Duration::ZERO,
            b.repoint(up(a_addr), RepointOptions::default())
        )
        .await
        .is_err(),
        "the repoint future was dropped before it settled"
    );
    b.shutdown().await.unwrap();
    b.reopen().await.unwrap();
    wait_node(&b, "b streams from a after reopen", streaming).await;
    match b.state() {
        NodeState::Slave { upstream, .. } => assert_eq!(upstream, format!("http://{a_addr}")),
        s => panic!("expected Slave, got {s:?}"),
    }
}

/// A fence check that cannot read the local store (here: a corrupt `__changelog` entry at the
/// peer's position) fails the open; nothing opens, and `reopen` leaves the node `Stopped`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_failed_fence_check_fails_the_open_and_keeps_the_node_stopped() {
    let c = Cluster::new();
    let s_addr = SocketAddr::from(([127, 0, 0, 1], reserved_port()));
    let a_cfg = node_cfg(&c, "a", dead_addr(), &[s_addr]);
    let a = Node::create_master(a_cfg.clone()).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (mut s_cfg, _) = c.slave_config("s", a.serve_addr().unwrap());
    s_cfg.serve.as_mut().unwrap().port = s_addr.port();
    let s = Slave::open(s_cfg.clone()).await.unwrap();
    let shared = a.put("c", b"k", b"1").unwrap();
    s.wait_for_seq(shared, WAIT).await.unwrap();
    s.shutdown().await.unwrap();
    a.put("c", b"k2", b"2").unwrap();
    a.shutdown().await.unwrap();
    // The peer answers `Status` at `shared`, behind `a`: the check reads `a`'s entry there.
    let s = Slave::open(s_cfg).await.unwrap();
    assert_eq!(s.last_seq(), shared);
    {
        let path = c.path("a");
        let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), &path).unwrap();
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), &path, &names).unwrap();
        let log = db.cf_handle("__changelog").unwrap();
        db.put_cf(&log, shared.to_be_bytes(), b"not an entry")
            .unwrap();
    }

    let err = a.reopen().await.unwrap_err();
    assert!(matches!(err, Error::Corrupt(_)), "{err}");
    assert_eq!(a.state(), NodeState::Stopped);
    assert!(
        matches!(Node::open(a_cfg).await, Err(Error::Corrupt(_))),
        "nothing opens"
    );
    s.shutdown().await.unwrap();
}

/// Writers hammer `put` while the node is promoted: each put succeeds as master or fails with
/// `NotMaster`/`Switching`, nothing else; after the promotion, writes succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_under_concurrent_writers() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Arc::new(
        Node::create_slave(node_cfg(&c, "b", a_addr, &[]))
            .await
            .unwrap(),
    );
    wait_node(&b, "b streaming", streaming).await;
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    a.shutdown().await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (b, stop) = (b.clone(), stop.clone());
            tokio::task::spawn_blocking(move || {
                let (mut ok, mut refused, mut i) = (0u64, 0u64, 0u64);
                while !stop.load(Ordering::SeqCst) {
                    match b.put("c", format!("{w}-{i}").as_bytes(), b"v") {
                        Ok(_) => ok += 1,
                        Err(Error::NotMaster | Error::Switching) => {
                            refused += 1;
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(e) => panic!("writer {w}: unexpected error {e}"),
                    }
                    i += 1;
                }
                (ok, refused)
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(50)).await;
    b.promote().await.unwrap();
    assert!(is_master(&b.state()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, Ordering::SeqCst);
    let (mut ok, mut refused) = (0, 0);
    for w in writers {
        let (o, r) = w.await.unwrap();
        ok += o;
        refused += r;
    }
    assert!(refused > 0, "writes before the promotion were refused");
    assert!(ok > 0, "writes after the promotion succeeded");
    b.put("c", b"after", b"1").unwrap();
}

/// A refused checked promotion (a master is reachable) leaves the slave running and following.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_promotion_keeps_the_slave_running() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[a_addr]))
        .await
        .unwrap();
    wait_node(&b, "b streaming", streaming).await;
    assert!(matches!(
        b.promote_checked().await,
        Err(Error::MasterReachable { .. })
    ));
    assert!(streaming(&b.state()), "still a streaming slave");
    let seq = a.put("c", b"k", b"v").unwrap();
    b.wait_for_seq(seq, WAIT).await.unwrap();
    assert!(matches!(a.promote().await, Err(Error::NotSlave)));
    assert!(matches!(a.promote_checked().await, Err(Error::NotSlave)));
    assert!(is_master(&a.state()), "a refused switch changes nothing");
    // Plain promote refuses a slave without data and keeps it too.
    let e = Node::create_slave(node_cfg(&c, "e", dead_addr(), &[]))
        .await
        .unwrap();
    assert!(matches!(e.promote().await, Err(Error::Config(_))));
    assert!(matches!(e.state(), NodeState::Slave { .. }));
    assert!(e.status().is_ok(), "the slave still runs");
}

/// Demote undoes an unused promotion; once written, it is refused and the master keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demote_through_the_node() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[]))
        .await
        .unwrap();
    wait_node(&b, "b streaming", streaming).await;
    assert!(matches!(b.demote().await, Err(Error::NotMaster)));
    b.promote().await.unwrap();
    b.demote().await.unwrap();
    assert!(matches!(b.state(), NodeState::Slave { .. }));
    b.promote().await.unwrap();
    b.put("c", b"k", b"v").unwrap();
    assert!(matches!(b.demote().await, Err(Error::Config(_))));
    assert!(is_master(&b.state()));
    b.put("c", b"k2", b"v").expect("still the master");
}

/// The full runbook through `Node`: A dies, B is promoted, A restarts fenced by B and rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_old_master_rejoins_through_the_node() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[a_addr]))
        .await
        .unwrap();
    a.put("c", b"k", b"1").unwrap();
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    // A slave caught up by its bootstrap reads `Bootstrapping` until its next handshake.
    wait_node(&b, "b streaming", streaming).await;
    a.shutdown().await.unwrap();
    b.promote_checked().await.unwrap();
    b.put("c", b"on b", b"2").unwrap();
    let b_addr = b.serve_addr().unwrap();

    let a = Node::open(node_cfg(&c, "a", b_addr, &[b_addr]))
        .await
        .unwrap();
    assert_eq!(
        a.state(),
        NodeState::Fenced {
            reason: FenceReason::PeerIsMaster { peer: "b".into() }
        }
    );
    assert_eq!(
        a.rejoin_as_slave(None, RejoinOptions::default())
            .await
            .unwrap(),
        RepointOutcome::EpochAdvanced
    );
    a.wait_for_seq(b.last_seq().unwrap(), WAIT).await.unwrap();
    assert_eq!(a.get("c", b"on b").unwrap(), Some(b"2".to_vec()));
    assert!(matches!(
        a.rejoin_as_slave(None, RejoinOptions::default()).await,
        Err(Error::NotMaster)
    ));
}

/// A running master is shut down and rejoins another master's history (here: a stale master
/// restarted without the fence check, its restart epoch empty); the given upstream is the one
/// the slave follows and later reopens use.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_running_master_rejoins_a_given_upstream() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[]))
        .await
        .unwrap();
    a.put("c", b"k", b"1").unwrap();
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    // A slave caught up by its bootstrap reads `Bootstrapping` until its next handshake.
    wait_node(&b, "b streaming", streaming).await;
    a.shutdown().await.unwrap();
    b.promote().await.unwrap();
    b.put("c", b"on b", b"2").unwrap();
    let b_addr = b.serve_addr().unwrap();

    let mut cfg = node_cfg(&c, "a", dead_addr(), &[]);
    cfg.unchecked_master = Some(AcceptSplitBrainRisk);
    let a = Node::open(cfg).await.unwrap();
    assert!(is_master(&a.state()));
    assert_eq!(
        a.rejoin_as_slave(Some(up(b_addr)), RejoinOptions::default())
            .await
            .unwrap(),
        RepointOutcome::EpochAdvanced
    );
    a.wait_for_seq(b.last_seq().unwrap(), WAIT).await.unwrap();
    assert_eq!(a.get("c", b"on b").unwrap(), Some(b"2".to_vec()));
    a.shutdown().await.unwrap();
    a.reopen().await.unwrap();
    wait_node(&a, "a streams from b after reopen", streaming).await;
    match a.state() {
        NodeState::Slave { upstream, .. } => assert_eq!(upstream, format!("http://{b_addr}")),
        s => panic!("expected Slave, got {s:?}"),
    }
}

/// A rejoin refused before the rewrite (the directory cannot be opened) keeps a fenced node
/// fenced; from a running master (shut down first) it leaves the node `Failed`, and `reopen`
/// opens the unchanged master again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_rejoin_refused_before_the_rewrite_keeps_the_stored_master() {
    let c = Cluster::new();
    let a_cfg = node_cfg(&c, "a", dead_addr(), &[]);
    let a = Node::create_master(a_cfg.clone()).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    a.shutdown().await.unwrap();

    let a = Node::open(a_cfg.clone()).await.unwrap();
    let fenced = NodeState::Fenced {
        reason: FenceReason::NoPeers,
    };
    assert_eq!(a.state(), fenced);
    let raw = RawDb::open(&a_cfg.master.path);
    let err = a
        .rejoin_as_slave(None, RejoinOptions::default())
        .await
        .unwrap_err();
    assert!(err.to_string().to_lowercase().contains("lock"), "{err}");
    assert_eq!(a.state(), fenced, "still fenced");
    drop(raw);
    a.shutdown().await.unwrap();

    let mut cfg = a_cfg;
    cfg.unchecked_master = Some(AcceptSplitBrainRisk);
    let a = Node::open(cfg).await.unwrap();
    assert!(is_master(&a.state()));
    let snap = a.snapshot().unwrap();
    let err = a
        .rejoin_as_slave(None, RejoinOptions::default())
        .await
        .unwrap_err();
    assert!(err.to_string().contains("ReadSnapshot"), "{err}");
    assert!(matches!(a.state(), NodeState::Failed { .. }));
    drop(snap);
    a.reopen().await.unwrap();
    assert!(is_master(&a.state()), "the directory is still a master");
}

/// A diverged rejoin through the node halts, keeps the suffix readable, and repairs only with
/// `AcceptDataLoss`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_diverged_rejoin_halts_then_repairs_with_consent() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[a_addr]))
        .await
        .unwrap();
    a.put("c", b"k", b"1").unwrap();
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    b.shutdown().await.unwrap();
    a.put("c", b"only on a", b"x").unwrap();
    a.shutdown().await.unwrap();
    b.reopen().await.unwrap();
    b.promote_checked().await.unwrap();
    let b_addr = b.serve_addr().unwrap();

    let a = Node::open(node_cfg(&c, "a", b_addr, &[b_addr]))
        .await
        .unwrap();
    assert!(matches!(
        a.rejoin_as_slave(None, RejoinOptions::default()).await,
        Err(Error::Diverged {
            divergent_entries: 1,
            ..
        })
    ));
    assert!(matches!(
        a.state(),
        NodeState::Slave {
            state: SlaveState::Halted(HaltReason::Diverged),
            ..
        }
    ));
    assert_eq!(a.diverged_entries().unwrap().len(), 1);
    assert_eq!(a.get("c", b"only on a").unwrap(), Some(b"x".to_vec()));
    let repair = RepointOptions {
        on_divergence: RepointDivergence::Repair {
            accept_loss: AcceptDataLoss,
        },
    };
    assert!(matches!(
        a.repoint(up(b_addr), repair).await.unwrap(),
        RepointOutcome::Repaired { .. }
    ));
    a.wait_for_seq(b.last_seq().unwrap(), WAIT).await.unwrap();
    assert_eq!(a.get("c", b"only on a").unwrap(), None);
}

/// Unreachable peers keep the node fenced unless the operator accepts the risk; peers that are
/// slaves behind this node clear it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unfence_clears_with_behind_slaves_or_explicit_consent() {
    let c = Cluster::new();
    let a_cfg = node_cfg(&c, "a", dead_addr(), &[]);
    let a = Node::create_master(a_cfg.clone()).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let (s, _) = c.slave("s", a_addr).await;
    a.put("c", b"k", b"1").unwrap();
    s.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    a.shutdown().await.unwrap();
    let s_addr = s.serve_addr().unwrap();
    let dead = dead_addr();

    let mut cfg = a_cfg.clone();
    cfg.peers = vec![up(s_addr), up(dead)];
    let a = Node::open(cfg).await.unwrap();
    let fenced = NodeState::Fenced {
        reason: FenceReason::PeerUnreachable {
            peer: format!("http://{dead}"),
        },
    };
    assert_eq!(a.state(), fenced);
    assert!(matches!(
        a.unfence(UnfenceOptions::default()).await,
        Err(Error::Fenced { .. })
    ));
    assert_eq!(a.state(), fenced, "still fenced");
    a.unfence(UnfenceOptions {
        allow_unreachable: Some(AcceptSplitBrainRisk),
    })
    .await
    .unwrap();
    assert!(is_master(&a.state()));
    assert!(matches!(
        a.unfence(UnfenceOptions::default()).await,
        Err(Error::Config(_))
    ));
    assert!(is_master(&a.state()), "a refused unfence changes nothing");
    a.shutdown().await.unwrap();

    // Only the behind slave as peer: the open itself clears.
    let mut cfg = a_cfg;
    cfg.peers = vec![up(s_addr)];
    let a = Node::open(cfg).await.unwrap();
    assert!(is_master(&a.state()), "{:?}", a.state());
}

/// Review focus (F9b): the default `unfence` clears once every peer answers as a slave behind
/// this node: here the only peer was down at the open and is back.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unfence_by_default_clears_once_the_behind_slave_answers() {
    let c = Cluster::new();
    let a_cfg = node_cfg(&c, "a", dead_addr(), &[]);
    let a = Node::create_master(a_cfg.clone()).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let (s_cfg, _) = c.slave_config("s", a_addr);
    let s = Slave::open(s_cfg.clone()).await.unwrap();
    a.put("c", b"k", b"1").unwrap();
    s.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    let s_addr = s.serve_addr().unwrap();
    s.shutdown().await.unwrap();
    a.put("c", b"k", b"2").unwrap();
    a.shutdown().await.unwrap();

    let mut cfg = a_cfg;
    cfg.peers = vec![up(s_addr)];
    let a = Node::open(cfg).await.unwrap();
    assert_eq!(
        a.state(),
        NodeState::Fenced {
            reason: FenceReason::PeerUnreachable {
                peer: format!("http://{s_addr}")
            }
        }
    );
    let mut s_cfg = s_cfg;
    s_cfg.serve.as_mut().unwrap().port = s_addr.port();
    let _s = Slave::open(s_cfg).await.unwrap();
    a.unfence(UnfenceOptions::default()).await.unwrap();
    assert!(is_master(&a.state()), "{:?}", a.state());
    a.put("c", b"k", b"3").unwrap();
}

/// Review focus: a live `ReadSnapshot` makes the promotion lose the handle; the node is
/// `Failed`, and after dropping the snapshot `reopen` brings back the (unchanged) slave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_handle_fails_the_node_and_reopen_recovers() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[]))
        .await
        .unwrap();
    wait_node(&b, "b streaming", streaming).await;
    let snap = b.snapshot().unwrap();
    let err = b.promote().await.unwrap_err();
    assert!(err.to_string().contains("ReadSnapshot"), "{err}");
    assert!(matches!(b.state(), NodeState::Failed { .. }));
    assert!(matches!(b.get("c", b"k"), Err(Error::NodeFailed(_))));
    drop(snap);
    b.reopen().await.unwrap();
    wait_node(&b, "b streaming again", streaming).await;
}
