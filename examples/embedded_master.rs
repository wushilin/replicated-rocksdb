//! A `Master` used as a plain embedded key-value store (no replication).
//!
//! Shows column families, put/get/delete/delete_range, atomic batches, TTL, snapshots,
//! iteration, and - most importantly - the read-modify-write pattern. This crate
//! deliberately has NO `update`, compare-and-set or merge: if a new value depends on the old
//! one, YOU hold a lock around get -> compute -> put.
//!
//! Run with: `cargo run --example embedded_master`

use std::error::Error;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use replicated_rocksdb::{Batch, CfSpec, Direction, IterMode, Master, MasterConfig, ReadApi};

/// The read-modify-write pattern (DESIGN.md section 5.1).
///
/// The master's internal writer lock only serialises individual writes. Between our `get`
/// and our `put`, another thread could write the same key and its update would be lost. So
/// every writer of these keys takes the same application-level lock for the whole sequence.
/// (One master process => an in-process lock is enough. Use a per-key or striped lock if a
/// single global one becomes a bottleneck.)
fn increment(master: &Master, lock: &Mutex<()>, key: &[u8]) -> Result<u64, Box<dyn Error>> {
    let _guard = lock.lock().unwrap(); // held across get -> compute -> put
    let old = master.get("counters", key)?;
    let new = old.map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap())) + 1;
    master.put("counters", key, &new.to_be_bytes())?; // logged as an absolute Put
    Ok(new)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    // No `serve` config => no replication server; the master is just a durable local store.
    let master =
        Arc::new(Master::open(MasterConfig::new("embedded", dir.path().join("db"))).await?);

    // Column families are created through the master (and logged), not via RocksDB options.
    master.create_cf(CfSpec::new("users"))?;
    master.create_cf(CfSpec::new("counters"))?;
    println!("column families: {:?}", cf_names(&*master));

    // Basic operations. Every write is fsynced and returns its sequence number.
    let seq = master.put("users", b"alice", b"Alice A.")?;
    master.put("users", b"bob", b"Bob B.")?;
    println!("put alice at seq {seq}");
    println!("get alice -> {:?}", show(master.get("users", b"alice")?));
    master.delete("users", b"bob")?;
    println!(
        "get bob after delete -> {:?}",
        show(master.get("users", b"bob")?)
    );

    // A Batch is committed atomically as ONE log entry and may span column families.
    let mut batch = Batch::new();
    batch
        .put("users", b"carol", b"Carol C.")
        .put("users", b"dave", b"Dave D.")
        .put("counters", b"signups", &2u64.to_be_bytes());
    let seq = master.write(batch)?;
    println!("batch of 3 ops committed atomically at seq {seq}");

    // delete_range removes the half-open key range [start, end).
    for i in 0..10u8 {
        master.put("counters", format!("tmp:{i}").as_bytes(), &[i])?;
    }
    master.delete_range("counters", b"tmp:3", b"tmp:7")?; // removes tmp:3..tmp:6
    let left: Vec<String> = keys(master.prefix_iter("counters", b"tmp:")?)?;
    println!("after delete_range [tmp:3, tmp:7): {left:?}");

    // TTL: the expiry is computed on the master and reads hide expired values at once.
    master.put_with_ttl(
        "users",
        b"session",
        b"token-123",
        Duration::from_millis(300),
    )?;
    println!(
        "session before expiry -> {:?}",
        show(master.get("users", b"session")?)
    );
    tokio::time::sleep(Duration::from_millis(400)).await;
    println!(
        "session after expiry  -> {:?}",
        show(master.get("users", b"session")?)
    );

    // A snapshot is a consistent point-in-time view (it also implements ReadApi).
    let snap = master.snapshot()?;
    master.put("users", b"erin", b"written after the snapshot")?;
    println!(
        "snapshot at seq {}: erin -> {:?}, live: erin -> {:?}",
        snap.seq(),
        show(snap.get("users", b"erin")?),
        show(master.get("users", b"erin")?)
    );
    drop(snap); // snapshots pin the database; drop them before shutting down

    // Iteration: forward from the start, reverse from the end, or from a key.
    let forward: Vec<String> = keys(master.iter("users", IterMode::Start)?)?;
    println!("users forward: {forward:?}");
    let last_two: Vec<String> = keys(master.iter("users", IterMode::End)?.take(2))?;
    println!("users reverse (2): {last_two:?}");
    let from_carol: Vec<String> =
        keys(master.iter("users", IterMode::From(b"carol", Direction::Forward))?)?;
    println!("users from \"carol\": {from_carol:?}");

    // ---- Read-modify-write, the safe way -------------------------------------------------
    // 8 threads each bump the same counter 25 times. Without the shared lock, increments
    // would be lost; with it the result is exactly 200.
    let lock = Arc::new(Mutex::new(()));
    let threads: Vec<_> = (0..8)
        .map(|_| {
            let (m, l) = (master.clone(), lock.clone());
            std::thread::spawn(move || {
                for _ in 0..25 {
                    increment(&m, &l, b"hits").unwrap();
                }
            })
        })
        .collect();
    for t in threads {
        t.join().unwrap();
    }
    let hits = u64::from_be_bytes(
        master
            .get("counters", b"hits")?
            .unwrap()
            .try_into()
            .unwrap(),
    );
    println!("counter after 8 threads x 25 increments (locked): {hits}");
    assert_eq!(hits, 200);

    // Clean shutdown (needs sole ownership of the Arc; all threads are joined).
    let master = Arc::try_unwrap(master).ok().expect("no other owners");
    master.shutdown().await?;
    println!("done");
    Ok(())
}

/// Iterators yield `Result<(key, value)>`; collect just the keys as strings.
fn keys(
    it: impl Iterator<Item = replicated_rocksdb::Result<(Vec<u8>, Vec<u8>)>>,
) -> Result<Vec<String>, Box<dyn Error>> {
    Ok(it
        .map(|r| r.map(|(k, _)| String::from_utf8_lossy(&k).into_owned()))
        .collect::<Result<_, _>>()?)
}

fn cf_names(db: &impl ReadApi) -> Vec<String> {
    db.list_cfs().into_iter().map(|s| s.name).collect()
}

fn show(v: Option<Vec<u8>>) -> Option<String> {
    v.map(|v| String::from_utf8_lossy(&v).into_owned())
}
