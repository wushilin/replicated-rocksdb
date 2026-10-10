//! Integrity verification: compare a replica's data with its upstream's, at the same seq.
//!
//! Shows `Slave::verify_now` in both modes and what a `VerifyReport` says: `Quick` compares
//! envelope headers (missing, extra or stale writes), `Full` compares whole values (it also
//! finds a value corrupted on disk). The damage here is injected the way it happens in real
//! life: behind the crate's back, by opening the stopped replica's directory with plain
//! RocksDB. Scheduled verification (`VerifyConfig::interval`) and the reaction to a mismatch
//! (`MismatchPolicy`) are configured on the slave; see the `repair` example for `Repair`.
//!
//! Run with: `cargo run --example verify`

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    AllowAll, CfSpec, Master, MasterConfig, ServeConfig, Slave, SlaveConfig, UpstreamConfig,
    VerifyMode,
};

const WAIT: Duration = Duration::from_secs(10);

/// Flips the last byte of the value stored under `key` in CF `cf` of a STOPPED node. The
/// envelope header (and its `write_seq`) stays intact: only a `Full` verify can see this.
fn corrupt_on_disk(dir: &Path, cf: &str, key: &[u8]) -> Result<(), Box<dyn Error>> {
    let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir)?;
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir, &names)?;
    // User CFs are stored as `u:{name}:{create_seq}`.
    let physical = names
        .iter()
        .find(|n| n.starts_with(&format!("u:{cf}:")))
        .ok_or("no such CF on disk")?;
    let handle = db.cf_handle(physical).ok_or("CF not open")?;
    let mut envelope = db.get_cf(&handle, key)?.ok_or("no such key on disk")?;
    let last = envelope.len() - 1;
    envelope[last] ^= 0xFF;
    db.put_cf(&handle, key, envelope)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let mut mcfg = MasterConfig::new("master", dir.path().join("master"));
    mcfg.serve = Some(ServeConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        0,
        Arc::new(AllowAll),
    ));
    let master = Master::open(mcfg).await?;
    master.create_cf(CfSpec::new("kv"))?;
    for i in 0..1_000u32 {
        master.put("kv", format!("key-{i:04}").as_bytes(), b"some value")?;
    }

    let scfg = SlaveConfig::new(
        "replica",
        dir.path().join("replica"),
        UpstreamConfig::from_addr(master.serve_addr().unwrap()),
    );
    let replica = Slave::open(scfg.clone()).await?;
    replica.wait_for_seq(master.last_seq(), WAIT).await?;

    // ---- A clean run. Both sides are compared at exactly the same seq S, even while the
    // master keeps writing: the replica pauses applying until it has a snapshot at S.
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let report = replica.verify_now(mode).await?;
        println!(
            "{mode:?} verify at seq {}: {} CFs, {} keys, clean = {}",
            report.seq,
            report.cfs_checked,
            report.keys_checked,
            report.is_clean(),
        );
        assert!(report.is_clean());
    }

    // ---- Damage the replica's copy of one value on disk, then look again.
    replica.shutdown().await?;
    corrupt_on_disk(&dir.path().join("replica"), "kv", b"key-0500")?;
    let replica = Slave::open(scfg).await?;
    replica.wait_streaming(WAIT).await?;

    let quick = replica.verify_now(VerifyMode::Quick).await?;
    println!(
        "Quick after the damage: clean = {} (the headers still match)",
        quick.is_clean()
    );
    assert!(quick.is_clean());
    let full = replica.verify_now(VerifyMode::Full).await?;
    println!(
        "Full after the damage: {} mismatch(es)",
        full.mismatches.len()
    );
    for m in &full.mismatches {
        // `Mismatch::Key { cf, key, local, upstream }`: the write_seq on each side; equal
        // write_seqs mean the stored values differ.
        println!("  {m:?}");
    }
    assert_eq!(full.mismatches.len(), 1);

    replica.shutdown().await?;
    master.shutdown().await?;
    println!("done");
    Ok(())
}
