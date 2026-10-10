//! Repair: fix a replica by rewriting only the keys that differ, instead of re-copying all.
//!
//! Two situations, both from DESIGN.md section 12.1:
//!
//! 1. **Divergence.** A replica holds entries its new upstream's history does not have (here:
//!    the operator promoted a replica that was behind). `repoint` refuses by default; with
//!    `RepointDivergence::Repair { accept_loss: AcceptDataLoss }` the replica drops only its
//!    divergent entries, takes the keys they touched from the new upstream, and replays.
//!    `Slave::diverged_entries` lets you salvage the dropped entries first.
//! 2. **Damage found by verify.** With `MismatchPolicy::Repair`, the keys a verify reports are
//!    taken from the upstream at once. Verify cannot tell which side is damaged: this policy
//!    trusts the upstream, so use it on replicas of the master.
//!
//! Run with: `cargo run --example repair`

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    AcceptDataLoss, AllowAll, CfSpec, Error as RrError, Master, MasterConfig, MismatchPolicy,
    ReadApi, RepointDivergence, RepointOptions, RepointOutcome, ServeConfig, Slave, SlaveConfig,
    UpstreamConfig, VerifyMode,
};

const WAIT: Duration = Duration::from_secs(10);

fn serve() -> ServeConfig {
    ServeConfig::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, Arc::new(AllowAll))
}

fn replica_cfg(dir: &Path, id: &str, upstream: SocketAddr) -> SlaveConfig {
    let mut cfg = SlaveConfig::new(id, dir.join(id), UpstreamConfig::from_addr(upstream));
    cfg.serve = Some(serve());
    cfg
}

fn text(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|v| String::from_utf8_lossy(&v).into_owned())
}

async fn divergence(dir: &Path) -> Result<(), Box<dyn Error>> {
    println!("=== 1. Repairing a divergence ===");
    let mut acfg = MasterConfig::new("A", dir.join("A"));
    acfg.serve = Some(serve());
    let a = Master::open(acfg).await?;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("kv"))?;
    let b_cfg = replica_cfg(dir, "B", a_addr);
    let b = Slave::open(b_cfg.clone()).await?;
    let c = Slave::open(replica_cfg(dir, "C", a_addr)).await?;
    let seq = a.put("kv", b"shared", b"everyone has this")?;
    b.wait_for_seq(seq, WAIT).await?;
    c.wait_for_seq(seq, WAIT).await?;
    b.wait_streaming(WAIT).await?;
    c.wait_streaming(WAIT).await?;

    // B stops; C receives two more entries; A dies.
    b.shutdown().await?;
    a.put("kv", b"only-c", b"B never saw this")?;
    let seq = a.delete("kv", b"shared")?;
    c.wait_for_seq(seq, WAIT).await?;
    a.shutdown().await?;

    // The operator promotes the stale B (plain `promote` checks no peers; `promote_checked`
    // would have refused).
    let b = Slave::open(b_cfg.clone()).await?;
    let b = b
        .promote(MasterConfig::from_slave_config(&b_cfg, Some(serve())))
        .await?;
    let b_up = UpstreamConfig::from_addr(b.serve_addr().unwrap());

    // The default repoint refuses: C would lose entries. Its data stays readable.
    match c.repoint(b_up.clone(), RepointOptions::default()).await {
        Err(RrError::Diverged {
            fork_seq,
            divergent_entries,
            ..
        }) => println!(
            "default repoint refused: diverged at seq {fork_seq}, {divergent_entries} local entries"
        ),
        other => panic!("expected a refusal, got {other:?}"),
    }
    // Salvage first: the divergent entries are still in C's log.
    for e in c.diverged_entries()? {
        println!(
            "  salvageable: entry {} ({} bytes of ops)",
            e.seq,
            e.ops.len()
        );
    }

    // Accept the loss of exactly those entries: only the keys they touched are rewritten.
    let repair = RepointOptions {
        on_divergence: RepointDivergence::Repair {
            accept_loss: AcceptDataLoss,
        },
    };
    match c.repoint(b_up, repair).await? {
        RepointOutcome::Repaired { discarded } => {
            println!("C repaired, no bootstrap: Repaired {{ discarded: {discarded:?} }}")
        }
        other => println!("C: {other:?} (repair fell back)"),
    }
    let seq = b.put("kv", b"after", b"written on the new master")?;
    c.wait_for_seq(seq, WAIT).await?;
    println!(
        "C now has shared = {:?}, only-c = {:?}, after = {:?}",
        text(c.get("kv", b"shared")?),
        text(c.get("kv", b"only-c")?),
        text(c.get("kv", b"after")?),
    );
    assert!(c.verify_now(VerifyMode::Full).await?.is_clean());
    println!("C verifies clean against B");
    c.shutdown().await?;
    b.shutdown().await?;
    Ok(())
}

async fn verify_repair(dir: &Path) -> Result<(), Box<dyn Error>> {
    println!("=== 2. Repairing what a verify found ===");
    let mut mcfg = MasterConfig::new("M", dir.join("M"));
    mcfg.serve = Some(serve());
    let m = Master::open(mcfg).await?;
    m.create_cf(CfSpec::new("kv"))?;
    for i in 0..100u32 {
        m.put("kv", format!("k{i:03}").as_bytes(), b"v")?;
    }
    let mut cfg = replica_cfg(dir, "R", m.serve_addr().unwrap());
    cfg.verify.on_mismatch = MismatchPolicy::Repair;
    // `cfg.verify.interval = Some(..)` would also run verifies on a schedule.
    let r = Slave::open(cfg.clone()).await?;
    r.wait_for_seq(m.last_seq(), WAIT).await?;

    // Lose a key behind the crate's back (a stopped node opened with plain RocksDB).
    r.shutdown().await?;
    {
        let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), &cfg.path)?;
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), &cfg.path, &names)?;
        let cf = names
            .iter()
            .find(|n| n.starts_with("u:kv:"))
            .ok_or("no CF on disk")?;
        db.delete_cf(&db.cf_handle(cf).ok_or("CF not open")?, b"k042")?;
    }
    let r = Slave::open(cfg).await?;
    r.wait_streaming(WAIT).await?;
    println!(
        "k042 on the damaged replica: {:?}",
        text(r.get("kv", b"k042")?)
    );

    let report = r.verify_now(VerifyMode::Quick).await?;
    println!(
        "verify found {} mismatch(es): {:?}",
        report.mismatches.len(),
        report.mismatches
    );
    assert_eq!(report.mismatches.len(), 1);
    // The repair runs in the background (reads answer NotReady while it replays); wait for
    // the node to serve again, then the next verify is clean.
    r.wait_streaming(WAIT).await?;
    let deadline = std::time::Instant::now() + WAIT;
    while !matches!(r.get("kv", b"k042"), Ok(Some(_))) && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    println!("k042 is back: {:?}", text(r.get("kv", b"k042")?));
    assert!(r.verify_now(VerifyMode::Full).await?.is_clean());
    println!("the next Full verify is clean");
    r.shutdown().await?;
    m.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let tmp = tempfile::tempdir()?;
    divergence(&tmp.path().join("divergence")).await?;
    verify_repair(&tmp.path().join("verify")).await?;
    println!("done");
    Ok(())
}
