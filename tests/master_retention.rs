mod common;

use std::time::Duration;

use common::{clock, config};
use replicated_rocksdb::{CfSpec, LogApi, Master};

/// The maintenance task trims the log and physically drops released CFs on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_runs_in_the_background() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.retention.max_entries = Some(1);
    cfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while m.first_retained_seq() != 6 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "retention did not run"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(m.last_seq(), 6);
    m.shutdown().await.unwrap();

    // Shutdown released the DB: it can be reopened at once.
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(m.first_retained_seq(), 6);
}

/// The maintenance task physically drops a logically dropped CF on its own (every 5 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_drops_released_cfs() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("gone")).unwrap();
    let physical = |path: &std::path::Path| {
        rocksdb::DB::list_cf(&rocksdb::Options::default(), path)
            .unwrap()
            .into_iter()
            .filter(|n| n.starts_with("u:gone:"))
            .count()
    };
    assert_eq!(physical(dir.path()), 1);
    m.drop_cf("gone").unwrap();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    while physical(dir.path()) != 0 {
        assert!(
            tokio::time::Instant::now() < deadline,
            "physical CF was not dropped"
        );
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
    m.shutdown().await.unwrap();
}
