//! Chained replication, end to end: A (master) → B (slave serving) → C.

mod support;

use std::time::Duration;

use replicated_rocksdb::{CfSpec, HaltReason, IdentityPolicy, LogApi, Master, ReadApi, Slave, SlaveState};
use support::{assert_converged, check_edge_cases, wait_state, wait_until, write_edge_cases, Cluster};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_replicates_everything_and_bootstraps_from_the_middle() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..50u8 {
        a.put("c", &[i], &[i; 100]).unwrap();
    }
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b)]).await;

    // C bootstraps from B while A keeps writing.
    let writer = {
        let a = &a;
        async move {
            for i in 0..50u8 {
                a.put("c", &[b'w', i], b"during").unwrap();
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    };
    let ((cn, lc), ()) = tokio::join!(c.slave("c", b.serve_addr().unwrap()), writer);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 1);

    a.create_cf(CfSpec::new("tmp")).unwrap();
    a.put_with_ttl("tmp", b"t", b"v", Duration::from_secs(5)).unwrap();
    a.drop_cf("c").unwrap();
    write_edge_cases(&a, "edge");
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    check_edge_cases(&cn, "edge");
    lc.assert_no_violations();

    let s = cn.status();
    assert_eq!(s.upstream_chain, vec!["b".to_string(), "a".to_string()]);
    assert_eq!(s.origin_identity, Some(a.identity()));
    wait_until("origin position reaches the leaf", || cn.status().origin_last_seq == a.last_seq()).await;
    // C's log is A's log, byte for byte.
    let from = cn.first_retained_seq() - 1;
    let ours: Vec<_> = cn.entries_since(from).unwrap().map(Result::unwrap).collect();
    let origin: Vec<_> = a.entries_since(from).unwrap().map(Result::unwrap).collect();
    assert_eq!(ours, origin);
    // Each node reports only its direct replicas.
    wait_until("c acknowledged on b", || b.replicas().iter().any(|r| r.node_id == "c" && r.applied_seq == a.last_seq())).await;
    assert_eq!(a.replicas().iter().map(|r| r.node_id.clone()).collect::<Vec<_>>(), vec!["b".to_string()]);
    assert_eq!(b.replicas().len(), 1);
    assert!(cn.replicas().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loop_is_detected_and_the_halt_is_not_persisted() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    let (cn, _) = c.slave("c", b.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    // Point B at C, which is downstream of B.
    let mut looped = b_cfg.clone();
    looped.upstream = replicated_rocksdb::UpstreamConfig::from_addr(cn.serve_addr().unwrap());
    let b = c.restart_slave(b, looped).await;
    wait_state(&b, "LoopDetected halt", |s| *s == SlaveState::Halted(HaltReason::LoopDetected)).await;

    // Configuration halts are re-evaluated on restart: pointed back at A, B resumes.
    let b = c.restart_slave(b, b_cfg).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
}

/// Rule 3 also catches a slave pointed at its own address (`client_id == server_id`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slave_pointed_at_itself_halts_loop_detected() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    let mut own = b_cfg;
    own.upstream = replicated_rocksdb::UpstreamConfig::from_addr(b.serve_addr().unwrap());
    let b = c.restart_slave(b, own).await;
    wait_state(&b, "LoopDetected halt", |s| *s == SlaveState::Halted(HaltReason::LoopDetected)).await;
    assert_eq!(b.get("c", b"k").unwrap(), None, "still readable while halted");
}

/// When B re-bootstraps, it ends C's streams with UPSTREAM_RESET; C reconnects and follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upstream_rebootstrap_resets_downstream_streams() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let port = a.serve_addr().unwrap().port();
    a.create_cf(CfSpec::new("old")).unwrap();
    let (mut b_cfg, lb) = c.slave_config("b", a.serve_addr().unwrap());
    b_cfg.on_identity_change = IdentityPolicy::Rebootstrap;
    let b = Slave::open(b_cfg).await.unwrap();
    let (mut c_cfg, lc) = c.slave_config("c", b.serve_addr().unwrap());
    c_cfg.on_identity_change = IdentityPolicy::Rebootstrap;
    let cn = Slave::open(c_cfg).await.unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    wait_state(&cn, "C streaming", |s| *s == SlaveState::Streaming).await;
    let streaming = || lc.states().iter().filter(|s| **s == SlaveState::Streaming).count();
    let streams_before = streaming();

    // B's upstream is replaced by a different database: B re-bootstraps while serving C.
    a.shutdown().await.unwrap();
    let mut cfg2 = c.master_config("a2");
    cfg2.serve.as_mut().unwrap().port = port;
    let a2 = Master::open(cfg2).await.unwrap();
    a2.create_cf(CfSpec::new("new")).unwrap();
    a2.put("new", b"k", b"v").unwrap();

    assert_converged(&a2, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(lb.bootstraps(), 2);
    assert_eq!(lc.bootstraps(), 2, "C followed B's new identity");
    wait_until("C's stream reset and re-established", || streaming() > streams_before).await;
}
