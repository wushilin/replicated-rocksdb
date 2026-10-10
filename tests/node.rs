//! The `Node` handle (DESIGN.md §13.2), with real nodes over 127.0.0.1.

mod support;

use std::net::SocketAddr;

use replicated_rocksdb::{
    AcceptSplitBrainRisk, CfSpec, Error, FenceReason, Master, Node, NodeState, RepointOptions,
    RepointOutcome, Slave, SlaveState, UpstreamConfig,
};
use support::{Cluster, WAIT, node_cfg, reserved_port, wait_until};

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
