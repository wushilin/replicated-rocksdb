//! Observability, end to end: replication lag.

mod support;

use std::time::Duration;

use replicated_rocksdb::{CfSpec, HaltAction, HaltReason, Master, Slave, SlaveState};
use support::{Cluster, assert_converged, wait_state};

fn reverse(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    b.cmp(a)
}

/// `lag_ms` is 0 when caught up and, while behind, the age of the last applied entry. B is
/// stopped behind by an entry whose comparator it lacks (it halts `SpecMismatch` there).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lag_ms_is_the_age_of_the_last_applied_entry_while_behind() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.rocksdb.comparators.register("reverse", reverse);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    // Tailed, not bootstrapped: B keeps these entries in its log, with their ts_ms.
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap(); // ts_ms = T0: the clock has not moved
    }
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().lag_ms, Some(0), "caught up");
    b.shutdown().await.unwrap();

    let behind = m.last_seq();
    c.clock.advance(Duration::from_secs(1));
    m.create_cf(CfSpec::new("rev").with_comparator("reverse"))
        .unwrap();
    m.put("c", b"later", b"v").unwrap();
    let b = Slave::open(cfg.clone()).await.unwrap();
    wait_state(&b, "SpecMismatch halt", |s| {
        *s == SlaveState::Halted(HaltReason::SpecMismatch)
    })
    .await;
    c.clock.advance(Duration::from_secs(7));
    let st = b.status();
    assert_eq!((st.applied_seq, st.origin_last_seq), (behind, m.last_seq()));
    assert_eq!(
        st.lag_ms,
        Some(8_000),
        "now − ts_ms of entry {behind}, which was written at T0"
    );

    b.shutdown().await.unwrap();
    let mut fixed = cfg;
    fixed.rocksdb.comparators.register("reverse", reverse);
    let b = Slave::open(fixed).await.unwrap();
    b.clear_halt(HaltAction::Retry).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().lag_ms, Some(0));
}
