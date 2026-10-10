//! Integrity scans, end to end: damage is injected into a stopped slave's files with plain
//! RocksDB, then the slave verifies against its upstream over real gRPC.

mod support;

use std::cmp::Ordering;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use replicated_rocksdb::codec::envelope;
use replicated_rocksdb::{
    CfSpec, Error, HaltReason, Master, Mismatch, MismatchPolicy, ReadApi, Slave, SlaveConfig,
    SlaveState, VerifyAbort, VerifyMode,
};
use support::{
    Cluster, RawDb, RecordingListener, assert_converged, copy_dir, edge_cases, restore_dir,
    wait_state, wait_until, write_edge_cases,
};

const KEYS: u32 = 200;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:03}").into_bytes()
}

fn fill(m: &Master) {
    m.create_cf(CfSpec::new("data")).unwrap();
    for i in 0..KEYS {
        m.put("data", &key(i), format!("value-{i}").as_bytes())
            .unwrap();
    }
}

async fn primary(c: &Cluster) -> Master {
    let m = c.master("a").await;
    fill(&m);
    m
}

/// A replica with small ranges (16 keys, listed at 2), so a mismatch is narrowed through
/// several bisection levels.
async fn replica(
    c: &Cluster,
    name: &str,
    upstream: SocketAddr,
    tweak: impl FnOnce(&mut SlaveConfig),
) -> (Slave, SlaveConfig, Arc<RecordingListener>) {
    let (mut cfg, listener) = c.slave_config(name, upstream);
    cfg.verify.chunk_keys = 16;
    cfg.verify.min_chunk_keys = 2;
    tweak(&mut cfg);
    (Slave::open(cfg.clone()).await.unwrap(), cfg, listener)
}

/// Stops `s`, changes its files with plain RocksDB, and opens it again.
async fn tamper(s: Slave, cfg: &SlaveConfig, f: impl FnOnce(&RawDb)) -> Slave {
    tamper_with(s, cfg, &[], f).await
}

async fn tamper_with(
    s: Slave,
    cfg: &SlaveConfig,
    comparators: &[(&str, &str, support::KeyCmp)],
    f: impl FnOnce(&RawDb),
) -> Slave {
    s.shutdown().await.unwrap();
    f(&RawDb::open_with(&cfg.path, comparators));
    Slave::open(cfg.clone()).await.unwrap()
}

fn ws(m: &Master, cf: &str, k: &[u8]) -> u64 {
    m.get_with_meta(cf, k).unwrap().unwrap().1.write_seq
}

fn mismatch(cf: &str, k: &[u8], local: Option<u64>, upstream: Option<u64>) -> Mismatch {
    Mismatch::Key {
        cf: cf.into(),
        key: k.to_vec(),
        local,
        upstream,
    }
}

fn reverse(a: &[u8], b: &[u8]) -> Ordering {
    b.cmp(a)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupted_value_is_found_by_full_verify_only() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b)]).await;
    let b = tamper(b, &cfg, |raw| raw.corrupt_value("data", &key(100))).await;

    let quick = b.verify_now(VerifyMode::Quick).await.unwrap();
    assert!(
        quick.is_clean(),
        "Quick hashes envelope headers only: {:?}",
        quick.mismatches
    );
    let full = b.verify_now(VerifyMode::Full).await.unwrap();
    let w = ws(&m, "data", &key(100));
    assert_eq!(
        full.mismatches,
        vec![mismatch("data", &key(100), Some(w), Some(w))]
    );
    assert_eq!(full.keys_checked, u64::from(KEYS));
    assert_eq!(b.status().last_verify, Some(full));
}

/// The first and the last key sit in the two ranges without a lower / upper bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_deleted_on_disk_are_reported_in_both_modes() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b)]).await;
    let b = tamper(b, &cfg, |raw| {
        raw.delete("data", &key(0));
        raw.delete("data", &key(KEYS - 1));
    })
    .await;
    let expected = vec![
        mismatch("data", &key(0), None, Some(ws(&m, "data", &key(0)))),
        mismatch(
            "data",
            &key(KEYS - 1),
            None,
            Some(ws(&m, "data", &key(KEYS - 1))),
        ),
    ];
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let report = b.verify_now(mode).await.unwrap();
        assert_eq!(report.mismatches, expected, "{mode:?}");
        assert_eq!(report.keys_checked, u64::from(KEYS) - 2, "{mode:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extra_keys_on_disk_are_reported_with_no_upstream_value() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b)]).await;
    let b = tamper(b, &cfg, |raw| {
        raw.put("data", b"k050x", &envelope::encode(7, None, b"stray"));
        raw.put("data", b"zzz", &envelope::encode(8, Some(1), b"stray"));
    })
    .await;
    let expected = vec![
        mismatch("data", b"k050x", Some(7), None),
        mismatch("data", b"zzz", Some(8), None),
    ];
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let report = b.verify_now(mode).await.unwrap();
        assert_eq!(report.mismatches, expected, "{mode:?}");
        assert_eq!(report.keys_checked, u64::from(KEYS) + 2, "{mode:?}");
    }
}

/// The slave cuts ranges at its own keys (every 16th: k016, k032, k048, …). A key that exists
/// only upstream, strictly between two boundaries, is still inside a range, and bisection
/// plus listing finds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_only_upstream_between_two_chunk_boundaries_is_found() {
    let c = Cluster::new();
    let m = primary(&c).await;
    m.put("data", b"k040a", b"only upstream").unwrap();
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b)]).await;
    let b = tamper(b, &cfg, |raw| raw.delete("data", b"k040a")).await;
    let w = ws(&m, "data", b"k040a");
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let r = b.verify_now(mode).await.unwrap();
        assert_eq!(
            r.mismatches,
            vec![mismatch("data", b"k040a", None, Some(w))],
            "{mode:?}"
        );
        assert_eq!(r.keys_checked, u64::from(KEYS), "k040a is not a local key");
    }
}

/// Empty and NUL keys, an all-NUL key, a 1 MiB key (as a range boundary: chunks of one key)
/// and a ~16 MiB value verify clean in both modes; then damage to the 1 MiB key and to the
/// big value is found.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_case_data_verifies_and_its_corruption_is_found() {
    let c = Cluster::new();
    let m = c.master("a").await;
    write_edge_cases(&m, "edge");
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        cfg.verify.chunk_keys = 1;
        cfg.verify.min_chunk_keys = 1;
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let r = b.verify_now(mode).await.unwrap();
        assert!(r.is_clean(), "{mode:?}: {} mismatches", r.mismatches.len());
        assert_eq!(r.keys_checked, edge_cases().len() as u64);
    }

    let long_key = vec![0x5A; 1 << 20];
    let zeros = vec![0u8; 64];
    let w_empty = ws(&m, "edge", b"");
    let b = tamper(b, &cfg, |raw| {
        // Gone on disk: seen by both modes.
        raw.delete("edge", &long_key);
        raw.delete("edge", b"\0");
        // Same header, different bytes: only Full sees them.
        raw.corrupt_value("edge", b"big");
        raw.corrupt_value("edge", b"\0k\0");
        raw.corrupt_value("edge", &zeros);
        // The empty key's empty value became a one-byte value.
        raw.put("edge", b"", &envelope::encode(w_empty, None, b"x"));
    })
    .await;
    let gone = |k: &[u8]| mismatch("edge", k, None, Some(ws(&m, "edge", k)));
    let changed = |k: &[u8]| {
        let w = ws(&m, "edge", k);
        mismatch("edge", k, Some(w), Some(w))
    };
    // Bytewise key order: "", "\0", 64 zeros, "\0k\0", the 1 MiB key (0x5A), "big".
    let quick_expected = vec![gone(b"\0"), gone(&long_key)];
    let full_expected = vec![
        changed(b""),
        gone(b"\0"),
        changed(&zeros),
        changed(b"\0k\0"),
        gone(&long_key),
        changed(b"big"),
    ];
    let quick = b.verify_now(VerifyMode::Quick).await.unwrap();
    assert!(
        quick.mismatches == quick_expected,
        "Quick: {} mismatches",
        quick.mismatches.len()
    );
    let full = b.verify_now(VerifyMode::Full).await.unwrap();
    assert!(
        full.mismatches == full_expected,
        "Full: {} mismatches",
        full.mismatches.len()
    );
}

/// Ranges, bisection and listings follow the CF's comparator: under reverse order k050
/// comes before k020, and that is the order mismatches are reported in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_custom_comparator_cf_verifies_and_localises_damage() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.rocksdb.comparators.register("reverse", reverse);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("rev").with_comparator("reverse"))
        .unwrap();
    for i in 0..100 {
        m.put("rev", &key(i), format!("value-{i}").as_bytes())
            .unwrap();
    }
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        cfg.rocksdb.comparators.register("reverse", reverse);
        cfg.verify.chunk_keys = 7;
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        assert!(b.verify_now(mode).await.unwrap().is_clean(), "{mode:?}");
    }
    let b = tamper_with(b, &cfg, &[("rev", "reverse", reverse)], |raw| {
        raw.delete("rev", &key(20));
        raw.corrupt_value("rev", &key(50));
    })
    .await;
    let (w20, w50) = (ws(&m, "rev", &key(20)), ws(&m, "rev", &key(50)));
    let quick = b.verify_now(VerifyMode::Quick).await.unwrap();
    assert_eq!(
        quick.mismatches,
        vec![mismatch("rev", &key(20), None, Some(w20))]
    );
    let full = b.verify_now(VerifyMode::Full).await.unwrap();
    assert_eq!(
        full.mismatches,
        vec![
            mismatch("rev", &key(50), Some(w50), Some(w50)),
            mismatch("rev", &key(20), None, Some(w20))
        ]
    );
}

/// A→B→C: each slave verifies against its own upstream; damage on C shows on C's link only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_link_of_a_chain_verifies_against_its_own_upstream() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, _, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    let (cn, c_cfg, _) = replica(&c, "c", b.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b), ("c", &cn)]).await;
    for (name, s) in [("b", &b), ("c", &cn)] {
        let r = s.verify_now(VerifyMode::Full).await.unwrap();
        assert!(r.is_clean(), "{name}: {:?}", r.mismatches);
        assert_eq!(r.seq, m.last_seq(), "{name}");
    }
    let cn = tamper(cn, &c_cfg, |raw| raw.corrupt_value("data", &key(7))).await;
    let w = ws(&m, "data", &key(7));
    assert_eq!(
        cn.verify_now(VerifyMode::Full).await.unwrap().mismatches,
        vec![mismatch("data", &key(7), Some(w), Some(w))]
    );
    assert!(
        b.verify_now(VerifyMode::Full).await.unwrap().is_clean(),
        "B's link is unaffected"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mismatch_policy_rebootstrap_repairs_and_a_reverify_is_clean() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, cfg, listener) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        cfg.verify.on_mismatch = MismatchPolicy::Rebootstrap
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    assert_eq!(listener.bootstraps(), 1);
    let b = tamper(b, &cfg, |raw| {
        raw.delete("data", &key(3));
        raw.corrupt_value("data", &key(150));
    })
    .await;
    let report = b.verify_now(VerifyMode::Full).await.unwrap();
    assert_eq!(report.mismatches.len(), 2, "{:?}", report.mismatches);
    wait_until("the policy re-bootstraps the slave", || {
        listener.bootstraps() == 2
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.get("data", &key(3)).unwrap(), Some(b"value-3".to_vec()));
    assert!(b.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(listener.verifies().len(), 2);
    listener.assert_no_violations();
}

/// A session idle for longer than the upstream's `verify_session_timeout` is gone: the run is
/// abandoned with `SessionExpired`. (The slave reads its single range at 2 000 bytes/s, so its
/// first request comes ~2 s after `VerifyBegin`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_session_expires_and_the_run_is_abandoned() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().limits.verify_session_timeout = Duration::from_millis(300);
    let m = Master::open(mcfg).await.unwrap();
    fill(&m);
    let (b, _, _) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        cfg.verify.chunk_keys = usize::MAX;
        cfg.verify.chunk_bytes = usize::MAX;
        cfg.verify.max_bytes_per_sec = Some(2_000);
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    let e = b.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::SessionExpired)),
        "{e:?}"
    );
}

/// With `max_verify_sessions = 1`, two slaves verifying at once cannot both get a session:
/// one run is refused with `Busy`, and once the slot is free both verify clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_upstream_limits_concurrent_verify_sessions() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().limits.max_verify_sessions = 1;
    let m = Master::open(mcfg).await.unwrap();
    fill(&m);
    let (b, _, _) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        // A slow run (~2 s) that holds the only session.
        cfg.verify.chunk_keys = usize::MAX;
        cfg.verify.chunk_bytes = usize::MAX;
        cfg.verify.max_bytes_per_sec = Some(2_000);
    })
    .await;
    let (cn, _, _) = replica(&c, "c", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b), ("c", &cn)]).await;

    let b = Arc::new(b);
    let slow = {
        let b = b.clone();
        tokio::spawn(async move { b.verify_now(VerifyMode::Quick).await })
    };
    let mut refused = 0;
    while !slow.is_finished() {
        match cn.verify_now(VerifyMode::Quick).await {
            Err(Error::VerifyAborted(VerifyAbort::Busy)) => refused += 1,
            Ok(r) => assert!(r.is_clean()),
            Err(e) => panic!("{e:?}"),
        }
        tokio::task::yield_now().await;
    }
    let slow = slow.await.unwrap();
    let slow_refused = matches!(slow, Err(Error::VerifyAborted(VerifyAbort::Busy)));
    assert!(
        refused > 0 || slow_refused,
        "one of two overlapping sessions must have been refused"
    );
    if !slow_refused {
        assert!(slow.unwrap().is_clean());
    }
    assert!(
        cn.verify_now(VerifyMode::Quick).await.unwrap().is_clean(),
        "the slot is free again"
    );
}

/// C is ahead of B (B lost entries it had not flushed, DESIGN.md §6). C cannot capture B's
/// snapshot seq and abandons the run instead of reporting phantom differences; once B has
/// caught up, C verifies clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slave_ahead_of_the_upstream_snapshot_abandons_the_run() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    let b_port = b.serve_addr().unwrap().port();
    let at_port = |mut cfg: SlaveConfig| {
        cfg.serve.as_mut().unwrap().port = b_port;
        cfg
    };
    b.shutdown().await.unwrap();
    copy_dir(&c.path("b"), &c.path("b-old"));
    let b = Slave::open(at_port(b_cfg.clone())).await.unwrap();
    let (c_cfg, _) = c.slave_config("c", b.serve_addr().unwrap());
    let cn = Slave::open(c_cfg).await.unwrap();
    for i in 0..10u8 {
        a.put("c", &[i], b"v").unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let c_seq = cn.last_seq();

    // B loses its newer entries; A is stopped, so B cannot catch up yet.
    let a_port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    restore_dir(&c.path("b-old"), &c.path("b"));
    let b = Slave::open(at_port(b_cfg.clone())).await.unwrap();
    let b_seq = b.last_seq();
    assert!(b_seq < c_seq);
    let e = cn.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::PassedSnapshot { snapshot_seq, applied_seq })
            if snapshot_seq == b_seq && applied_seq == c_seq),
        "{e:?}"
    );

    let mut a_cfg = c.master_config("a");
    a_cfg.serve.as_mut().unwrap().port = a_port;
    let a = Master::open(a_cfg).await.unwrap();
    a.put("c", b"after", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    assert!(cn.verify_now(VerifyMode::Quick).await.unwrap().is_clean());
}

/// A slave halted behind the upstream's snapshot can never reach it: the run gives up at once
/// instead of waiting `capture_timeout` (60 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_halted_slave_behind_the_snapshot_gives_up_at_once() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.rocksdb.comparators.register("reverse", reverse);
    let m = Master::open(mcfg).await.unwrap();
    fill(&m);
    let (b, _, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await; // no "reverse" here
    assert_converged(&m, &[("b", &b)]).await;
    m.create_cf(CfSpec::new("rev").with_comparator("reverse"))
        .unwrap();
    wait_state(&b, "SpecMismatch halt", |s| {
        *s == SlaveState::Halted(HaltReason::SpecMismatch)
    })
    .await;
    let started = Instant::now();
    let e = b.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::CaptureTimeout { snapshot_seq }) if snapshot_seq == m.last_seq()),
        "{e:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "gave up without waiting for capture_timeout"
    );
}
