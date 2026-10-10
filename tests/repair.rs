//! Repair, end to end (DESIGN.md §12.1): a diverged slave, or one a verify found damaged,
//! takes only the affected keys from its upstream instead of bootstrapping.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::time::Duration;

use replicated_rocksdb::codec::envelope;
use replicated_rocksdb::{
    CfSpec, DivergencePolicy, Error, HaltReason, LogApi, Master, MismatchPolicy, ReadApi, Slave,
    SlaveConfig, SlaveState, VerifyMode,
};
use support::{
    Cluster, RawDb, RecordingListener, assert_converged, restore_dir, wait_state, wait_until,
};

fn key(i: u32) -> Vec<u8> {
    format!("k{i:03}").into_bytes()
}

/// A master with 50 keys and a converged slave `d`, bootstrapped after them: its log holds no
/// entry before a later fork point, and the epoch histories locate the fork anyway.
async fn setup(
    c: &Cluster,
    tweak: impl FnOnce(&mut SlaveConfig),
) -> (Master, Slave, SlaveConfig, Arc<RecordingListener>) {
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..50u32 {
        a.put("c", &key(i), b"base").unwrap();
    }
    let (mut cfg, listener) = c.slave_config("d", a.serve_addr().unwrap());
    tweak(&mut cfg);
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    wait_state(&d, "streaming", |s| *s == SlaveState::Streaming).await;
    (a, d, cfg, listener)
}

/// Backs the master up, lets `write_lost` write entries the slave receives, then restores the
/// backup with an epoch bump (DESIGN.md §13): those entries are the slave's divergent suffix.
/// `write_new` writes the restored master's own history.
async fn diverge(
    c: &Cluster,
    a: Master,
    d: &Slave,
    write_lost: impl FnOnce(&Master),
    write_new: impl FnOnce(&Master),
) -> Master {
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    write_lost(&a);
    assert_converged(&a, &[("d", d)]).await;
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut cfg = c.master_config("a");
    cfg.serve.as_mut().unwrap().port = port;
    cfg.bump_epoch_on_open = true;
    let a = Master::open(cfg).await.unwrap();
    write_new(&a);
    a
}

fn lost(a: &Master) {
    a.put("c", &key(1), b"lost").unwrap();
    a.put("c", b"only-in-the-lost-history", b"x").unwrap();
    a.delete("c", &key(2)).unwrap();
    a.delete_range("c", &key(10), &key(20)).unwrap();
}

fn new_history(a: &Master) {
    a.put("c", &key(3), b"new").unwrap();
    a.delete("c", &key(15)).unwrap();
    a.put("c", b"only-in-the-new-history", b"y").unwrap();
}

/// Puts, a delete and a range delete in the divergent suffix; the slave rewrites only those
/// keys, drops the suffix, replays the new history and matches the master exactly, with no
/// second bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_diverged_slave_repairs_without_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, _, ld) = setup(&c, |cfg| cfg.on_divergence = DivergencePolicy::Repair).await;
    let a = diverge(&c, a, &d, lost, new_history).await;
    wait_until("the divergence is repaired", || ld.rewinds() == 1).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(d.get("c", b"only-in-the-lost-history").unwrap(), None);
    assert_eq!(
        d.get("c", &key(12)).unwrap(),
        Some(b"base".to_vec()),
        "the lost range delete is undone"
    );
    assert_eq!(d.get("c", &key(1)).unwrap(), Some(b"base".to_vec()));
    assert_eq!(d.status().origin_identity, Some(a.identity()));
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    ld.assert_no_violations();
}

/// Under the default `Halt` policy the data stays as it was, readable, and the divergent
/// suffix can be read back for salvage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_halted_diverged_slave_lists_its_divergent_entries() {
    let c = Cluster::new();
    let (a, d, _, _) = setup(&c, |_| {}).await;
    assert!(
        matches!(d.diverged_entries(), Err(Error::Config(_))),
        "not halted"
    );
    let fork = a.last_seq() + 1;
    let a = diverge(&c, a, &d, lost, new_history).await;
    wait_state(&d, "Diverged halt", |s| {
        *s == SlaveState::Halted(HaltReason::Diverged)
    })
    .await;
    let suffix = d.diverged_entries().unwrap();
    assert_eq!(
        suffix.iter().map(|e| e.seq).collect::<Vec<_>>(),
        (fork..fork + 4).collect::<Vec<_>>()
    );
    assert_eq!(
        d.get("c", b"only-in-the-lost-history").unwrap(),
        Some(b"x".to_vec()),
        "data intact and readable"
    );
    // Clearing the halt forgets the fork point; the next refusal records it again.
    d.clear_halt(replicated_rocksdb::HaltAction::Retry)
        .await
        .unwrap();
    wait_state(&d, "Diverged again", |s| {
        *s == SlaveState::Halted(HaltReason::Diverged)
    })
    .await;
    assert_eq!(d.diverged_entries().unwrap().len(), 4);
    drop(a);
}

/// A CF created in the divergent suffix cannot be undone key by key: the slave bootstraps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cf_operation_in_the_suffix_falls_back_to_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, _, ld) = setup(&c, |cfg| cfg.on_divergence = DivergencePolicy::Repair).await;
    let a = diverge(
        &c,
        a,
        &d,
        |a| {
            a.create_cf(CfSpec::new("extra")).unwrap();
            a.put("extra", b"k", b"v").unwrap();
        },
        new_history,
    )
    .await;
    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.rewinds(), 0);
}

/// The slave's own retention removed its divergent suffix: it cannot know what to repair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trimmed_local_suffix_falls_back_to_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, _, ld) = setup(&c, |cfg| {
        cfg.on_divergence = DivergencePolicy::Repair;
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    let a = diverge(&c, a, &d, lost, new_history).await;
    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.rewinds(), 0);
}

/// The upstream no longer holds the fork point: it cannot be proven where the histories
/// split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upstream_without_the_fork_point_falls_back_to_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, d_cfg, ld) = setup(&c, |cfg| cfg.on_divergence = DivergencePolicy::Repair).await;
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    lost(&a);
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut cfg = c.master_config("a");
    cfg.serve.as_mut().unwrap().port = port;
    cfg.bump_epoch_on_open = true;
    cfg.retention.max_entries = Some(1);
    cfg.retention.check_interval = Duration::from_millis(20);
    let a = Master::open(cfg).await.unwrap();
    new_history(&a);
    let before = a.last_seq();
    wait_until("the restored master trims its log", || {
        a.first_retained_seq() == before
    })
    .await;
    let d = Slave::open(d_cfg).await.unwrap();
    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.rewinds(), 0);
}

/// `MismatchPolicy::Repair`: the keys a verify reports — corrupted, missing, extra — are
/// taken from the upstream; no bootstrap, and the next verify is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_mismatches_are_repaired_from_the_upstream() {
    let c = Cluster::new();
    let (a, d, cfg, ld) = setup(&c, |cfg| {
        cfg.verify.on_mismatch = MismatchPolicy::Repair;
        cfg.verify.chunk_keys = 8;
        cfg.verify.min_chunk_keys = 2;
    })
    .await;
    d.shutdown().await.unwrap();
    {
        let raw = RawDb::open(&cfg.path);
        raw.corrupt_value("c", &key(40));
        raw.delete("c", &key(3));
        raw.put("c", b"k020x", &envelope::encode(7, None, b"stray"));
    }
    let d = Slave::open(cfg).await.unwrap();
    let report = d.verify_now(VerifyMode::Full).await.unwrap();
    assert_eq!(report.mismatches.len(), 3, "{:?}", report.mismatches);
    wait_until("the reported keys are repaired", || ld.rewinds() == 1).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(d.get("c", &key(3)).unwrap(), Some(b"base".to_vec()));
    assert_eq!(d.get("c", b"k020x").unwrap(), None);
}

/// `Range` mismatches (here: everything past `max_reported_keys`) are repaired as ranges:
/// every local key in them is deleted and the upstream's keys are written, so a stray local
/// key that no `Key` mismatch names goes too.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_range_mismatches_are_cleared_and_refetched() {
    let c = Cluster::new();
    let (a, d, cfg, ld) = setup(&c, |cfg| {
        cfg.verify.on_mismatch = MismatchPolicy::Repair;
        cfg.verify.chunk_keys = 8;
        cfg.verify.min_chunk_keys = 2;
        cfg.verify.max_reported_keys = 1;
    })
    .await;
    d.shutdown().await.unwrap();
    {
        let raw = RawDb::open(&cfg.path);
        raw.delete("c", &key(3));
        raw.put("c", b"k020x", &envelope::encode(7, None, b"stray"));
        raw.corrupt_value("c", &key(40));
    }
    let d = Slave::open(cfg).await.unwrap();
    let report = d.verify_now(VerifyMode::Full).await.unwrap();
    let ranges = report
        .mismatches
        .iter()
        .filter(|m| matches!(m, replicated_rocksdb::Mismatch::Range { .. }))
        .count();
    assert!(ranges >= 1, "{:?}", report.mismatches);
    wait_until("the reported ranges are repaired", || ld.rewinds() == 1).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(d.get("c", b"k020x").unwrap(), None, "cleared");
    assert_eq!(d.get("c", &key(40)).unwrap(), Some(b"base".to_vec()));
}

/// A divergent suffix larger than `repair_max_bytes` is not read into memory: bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn more_than_repair_max_bytes_falls_back_to_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, _, ld) = setup(&c, |cfg| {
        cfg.on_divergence = DivergencePolicy::Repair;
        cfg.repair_max_bytes = 4 << 10;
    })
    .await;
    let a = diverge(
        &c,
        a,
        &d,
        |a| {
            for i in 0..8u32 {
                a.put("c", &key(i), &[7u8; 1024]).unwrap();
            }
        },
        new_history,
    )
    .await;
    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.rewinds(), 0);
}

/// One round of the leader's writes to keys the divergent suffix touched: a put, a delete
/// or put of another affected key, and a range delete over part of the affected range with a
/// key written back into it.
fn write_affected(a: &Master, i: u64) {
    a.put("c", &key(1), format!("w{i}").as_bytes()).unwrap();
    if i % 2 == 0 {
        a.delete("c", &key(2)).unwrap();
        a.put("c", b"only-in-the-lost-history", format!("w{i}").as_bytes())
            .unwrap();
    } else {
        a.put("c", &key(2), format!("w{i}").as_bytes()).unwrap();
        a.delete("c", b"only-in-the-lost-history").unwrap();
    }
    if i % 3 == 0 {
        a.delete_range("c", &key(11), &key(16)).unwrap();
    } else {
        a.put("c", &key(12 + (i % 3) as u32), format!("w{i}").as_bytes())
            .unwrap();
    }
}

/// User-requested (a): the new leader keeps writing to the affected keys — puts, deletes and
/// a range delete covering some of them — while the slave opens its repair session and
/// fetches. The fetch reads the leader's snapshot `S`, replay applies everything after the
/// fork, and the result matches the leader exactly and verifies clean, without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_leader_writes_to_the_affected_keys_during_the_repair() {
    let c = Cluster::new();
    let (a, d, d_cfg, ld) = setup(&c, |cfg| cfg.on_divergence = DivergencePolicy::Repair).await;
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    lost(&a);
    assert_converged(&a, &[("d", &d)]).await;
    // The slave reconnects only once the leader is writing, so the writes overlap its
    // handshake, its verify session and every fetch.
    d.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut cfg = c.master_config("a");
    cfg.serve.as_mut().unwrap().port = port;
    let a = Arc::new(Master::open(cfg).await.unwrap());
    new_history(&a);

    let stop = Arc::new(AtomicBool::new(false));
    let rounds = Arc::new(AtomicU64::new(0));
    let writer = {
        let (a, stop, rounds) = (a.clone(), stop.clone(), rounds.clone());
        std::thread::spawn(move || {
            while !stop.load(Ordering::SeqCst) {
                let i = rounds.load(Ordering::SeqCst);
                write_affected(&a, i);
                rounds.store(i + 1, Ordering::SeqCst);
            }
        })
    };
    wait_until("the leader is writing", || {
        rounds.load(Ordering::SeqCst) >= 5
    })
    .await;
    let at_open = rounds.load(Ordering::SeqCst);
    let d = Slave::open(d_cfg).await.unwrap();
    wait_until("the divergence is repaired", || ld.rewinds() == 1).await;
    let at_rewind = rounds.load(Ordering::SeqCst);
    assert!(
        at_rewind > at_open,
        "the leader wrote while the slave handshook, opened its session and fetched"
    );
    // And keeps writing while the slave replays up to `S` and past it.
    wait_until("more writes after the rewind", || {
        rounds.load(Ordering::SeqCst) >= at_rewind + 10
    })
    .await;
    stop.store(true, Ordering::SeqCst);
    writer.join().unwrap();

    assert_converged(&*a, &[("d", &d)]).await;
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1, "repaired without a bootstrap");
    assert_eq!(ld.rewinds(), 1);
    assert_eq!(
        d.get("c", b"only-in-the-new-history").unwrap(),
        Some(b"y".to_vec())
    );
    ld.assert_no_violations();
}
