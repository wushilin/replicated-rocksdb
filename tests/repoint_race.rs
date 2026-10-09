//! A repoint racing a session that ends in a halt (feature `failpoints`).
#![cfg(feature = "failpoints")]

mod support;

use std::sync::{Arc, mpsc};
use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, Error, RepointOptions, RepointOutcome, SlaveState, UpstreamConfig,
};
use support::{Cluster, wait_state};

/// Session of repoint 1 ends in a halt (`LoopDetected`: B repointed at itself). Repoint 2 arrives
/// after the session ended but before the halt is persisted. The halt must not swallow it:
/// repoint 2 settles, repoint 1 is superseded.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repoint_racing_a_halt_is_not_lost() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    let b = Arc::new(b);

    // The halt blocks (on a blocking thread) until released.
    let (reached_tx, reached) = mpsc::channel::<()>();
    let (release_tx, release) = mpsc::channel::<()>();
    let release = std::sync::Mutex::new(release);
    fail::cfg_callback("slave-before-halt", move || {
        let _ = reached_tx.send(());
        let _ = release
            .lock()
            .unwrap()
            .recv_timeout(Duration::from_secs(30));
    })
    .unwrap();

    let own = UpstreamConfig::from_addr(b.serve_addr().unwrap());
    let first = tokio::spawn({
        let b = b.clone();
        async move { b.repoint(own, RepointOptions::default()).await }
    });
    tokio::task::spawn_blocking(move || {
        reached
            .recv_timeout(Duration::from_secs(30))
            .expect("the halt was reached")
    })
    .await
    .unwrap();

    let a_up = UpstreamConfig::from_addr(a.serve_addr().unwrap());
    let a_uri = format!("http://{}", a.serve_addr().unwrap());
    let second = tokio::spawn({
        let b = b.clone();
        async move { b.repoint(a_up, RepointOptions::default()).await }
    });
    support::wait_until("second repoint registered", || b.status().upstream == a_uri).await;
    release_tx.send(()).unwrap();
    fail::remove("slave-before-halt");

    let second = tokio::time::timeout(support::WAIT, second)
        .await
        .expect("the second repoint settles")
        .unwrap();
    assert_eq!(second.unwrap(), RepointOutcome::Resumed);
    let first = tokio::time::timeout(support::WAIT, first)
        .await
        .expect("the first repoint settles")
        .unwrap();
    assert!(matches!(first, Err(Error::RepointSuperseded)), "{first:?}");
}
