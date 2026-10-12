//! Read-only RocksDB properties on `Master`, `Slave` and `Node`, and the shared block cache.

mod support;

use replicated_rocksdb::{CfSpec, Error, Node, NodeState, RocksTuning};
use std::net::SocketAddr;
use support::{Cluster, WAIT, node_cfg, reserved_port};

fn dead_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], reserved_port()))
}

fn check_properties(
    get: impl Fn(Option<&str>, &str) -> Result<Option<String>, Error>,
    get_int: impl Fn(Option<&str>, &str) -> Result<Option<u64>, Error>,
) {
    assert!(get(None, "rocksdb.estimate-num-keys").unwrap().is_some());
    assert!(
        get_int(None, "rocksdb.estimate-num-keys")
            .unwrap()
            .is_some()
    );
    assert!(
        get_int(Some("c"), "rocksdb.estimate-num-keys")
            .unwrap()
            .is_some()
    );
    assert!(get(Some("c"), "rocksdb.stats").unwrap().is_some());
    assert_eq!(get(None, "rocksdb.no-such-property").unwrap(), None);
    assert_eq!(
        get_int(Some("c"), "rocksdb.no-such-property").unwrap(),
        None
    );
    for cf in ["__meta", "__changelog", "nope"] {
        assert!(matches!(
            get(Some(cf), "rocksdb.stats"),
            Err(Error::NoSuchCf(_))
        ));
        assert!(matches!(
            get_int(Some(cf), "rocksdb.estimate-num-keys"),
            Err(Error::NoSuchCf(_))
        ));
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn properties_on_master_slave_and_dropped_cf() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..10u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    check_properties(|cf, n| m.property(cf, n), |cf, n| m.property_int(cf, n));
    let (s, _) = c.slave("b", m.serve_addr().unwrap()).await;
    s.wait_for_seq(m.last_seq(), WAIT).await.unwrap();
    check_properties(|cf, n| s.property(cf, n), |cf, n| s.property_int(cf, n));

    m.create_cf(CfSpec::new("gone")).unwrap();
    assert!(m.property(Some("gone"), "rocksdb.stats").unwrap().is_some());
    m.drop_cf("gone").unwrap();
    assert!(matches!(
        m.property(Some("gone"), "rocksdb.stats"),
        Err(Error::NoSuchCf(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn properties_through_node() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[]))
        .await
        .unwrap();
    assert!(matches!(a.state(), NodeState::Master { .. }));
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..10u8 {
        a.put("c", &[i], b"v").unwrap();
    }
    check_properties(|cf, n| a.property(cf, n), |cf, n| a.property_int(cf, n));
    let b = Node::create_slave(node_cfg(&c, "b", a.serve_addr().unwrap(), &[]))
        .await
        .unwrap();
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    check_properties(|cf, n| b.property(cf, n), |cf, n| b.property_int(cf, n));
    b.shutdown().await.unwrap();
    assert!(matches!(
        b.property(None, "rocksdb.estimate-num-keys"),
        Err(Error::NodeFailed(_))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn block_cache_is_shared_by_every_cf() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    cfg.rocksdb = RocksTuning::new().with_block_cache(8 << 20);
    let m = replicated_rocksdb::Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("x")).unwrap();
    m.create_cf(CfSpec::new("y")).unwrap();
    for cf in ["x", "y"] {
        m.put(cf, b"k", b"v").unwrap();
        assert_eq!(m.get(cf, b"k").unwrap(), Some(b"v".to_vec()));
    }
    assert_eq!(
        m.property_int(None, "rocksdb.block-cache-capacity")
            .unwrap(),
        Some(8 << 20)
    );
    for cf in ["x", "y"] {
        assert_eq!(
            m.property_int(Some(cf), "rocksdb.block-cache-capacity")
                .unwrap(),
            Some(8 << 20)
        );
    }
}
