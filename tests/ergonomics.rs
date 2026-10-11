//! The small helpers of addendum §8, end to end.

mod support;

use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, Error, HaltReason, IdentityPolicy, MasterConfig, ReadApi, Slave, SlaveState,
};
use support::{Cluster, WAIT, assert_converged};

/// `Slave::wait_streaming` resolves once the slave tails, and fails at once with the halt
/// reason when it halts instead; `last_seq` is inherent on both node types.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_streaming_resolves_or_reports_the_halt() {
    let c = Cluster::new();
    // The second master reuses this master's address, so it is picked up front from the
    // reserved band (a released ephemeral port could be taken by someone else meanwhile).
    let port = support::reserved_port();
    let mut m_cfg = c.master_config("a");
    m_cfg.serve.as_mut().unwrap().port = port;
    let m = replicated_rocksdb::Master::open(m_cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    b.wait_streaming(WAIT).await.unwrap();
    assert_eq!(b.status().state, SlaveState::Streaming);
    m.put("c", b"k", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.last_seq(), m.last_seq());

    // Another database at the same address, refused by policy: the wait reports the halt.
    m.shutdown().await.unwrap();
    let mut other = c.master_config("other");
    other.serve.as_mut().unwrap().port = port;
    let other = replicated_rocksdb::Master::open(other).await.unwrap();
    b.shutdown().await.unwrap();
    let (mut cfg, _) = c.slave_config("b", other.serve_addr().unwrap());
    cfg.on_identity_change = IdentityPolicy::Refuse;
    let b = Slave::open(cfg).await.unwrap();
    assert!(matches!(
        b.wait_streaming(WAIT).await,
        Err(Error::Halted(HaltReason::IdentityChanged))
    ));
    drop(other);
    // A short wait on a slave that cannot get there times out.
    let unreachable = std::net::SocketAddr::from(([127, 0, 0, 1], support::reserved_port()));
    let (lonely, _) = c.slave("lonely", unreachable).await;
    assert!(matches!(
        lonely.wait_streaming(Duration::from_millis(100)).await,
        Err(Error::WaitTimeout(p)) if p.target_seq.is_none() && !p.progressed
    ));
}

/// `MasterConfig::from_slave_config` keeps what a promotion must keep, and a refused
/// promotion's `into_parts` hands back the error and the still-running slave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_config_and_refusal_parts() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg.clone()).await.unwrap();
    b.wait_streaming(WAIT).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;

    let mcfg = MasterConfig::from_slave_config(&cfg, Some(c.serve()));
    assert_eq!(
        (mcfg.identifier.as_str(), &mcfg.path, mcfg.heartbeat),
        ("b", &cfg.path, cfg.heartbeat)
    );
    assert!(mcfg.serve.is_some());

    // The old master is still reachable: refused, and the slave comes back running.
    let master_up = replicated_rocksdb::UpstreamConfig::from_addr(m.serve_addr().unwrap());
    let refused = b
        .promote_checked(mcfg.clone(), std::slice::from_ref(&master_up))
        .await
        .err()
        .expect("the old master is reachable");
    let (error, b) = refused.into_parts();
    assert!(matches!(error, Error::MasterReachable { .. }), "{error}");
    let b = b.expect("a refusal returns the slave");
    assert_eq!(b.get("c", b"k").unwrap(), Some(b"v".to_vec()));

    m.shutdown().await.unwrap();
    let promoted = b.promote(mcfg).await.unwrap();
    assert_eq!(promoted.get("c", b"k").unwrap(), Some(b"v".to_vec()));
    promoted.put("c", b"after", b"w").unwrap();
}
