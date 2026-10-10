//! Write concern: make a write wait until N replicas have it (Kafka's `min.insync.replicas`).
//!
//! Shows `WriteConcern { min_insync_replicas, ack_timeout }` set in `MasterConfig` and per
//! call (`put_with` / `write_with`), and the two different failures:
//!
//! * `Error::NotEnoughReplicas`: refused up front; NOTHING was written. Safe to retry.
//! * `Error::AckTimeout`: the entry IS committed and keeps replicating; only the
//!   acknowledgements did not arrive in time. Do NOT blindly retry, or the write happens
//!   twice.
//!
//! Run with: `cargo run --example write_concern`

use std::error::Error as StdError;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    AllowAll, CfSpec, Error, FetchConfig, Master, MasterConfig, ReadApi, Seq, ServeConfig, Slave,
    SlaveConfig, UpstreamConfig, WriteConcern,
};

/// Writes block (fsync, then waiting for acks), so from async code run them on the blocking
/// pool. Wrapping this once keeps the examples below readable.
async fn put_with(
    master: &Arc<Master>,
    key: &'static [u8],
    value: &'static [u8],
    wc: WriteConcern,
) -> Result<Seq, Error> {
    let master = master.clone();
    tokio::task::spawn_blocking(move || master.put_with("kv", key, value, wc))
        .await
        .expect("blocking task panicked")
}

async fn open_slave(
    dir: &std::path::Path,
    name: &str,
    addr: std::net::SocketAddr,
    fetch: FetchConfig,
) -> Result<Slave, Box<dyn StdError>> {
    let mut cfg = SlaveConfig::new(name, dir.join(name), UpstreamConfig::from_addr(addr));
    cfg.fetch = fetch;
    let slave = Slave::open(cfg).await?;
    slave.wait_streaming(Duration::from_secs(10)).await?;
    Ok(slave)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn StdError>> {
    let dir = tempfile::tempdir()?;

    // ---- Concern in the config: applies to EVERY data write on this master (put, write,
    // delete, ...) unless a call overrides it with the `*_with` variants. CF operations are
    // exempt: they carry no user data.
    let mut cfg = MasterConfig::new("master", dir.path().join("master"));
    cfg.serve = Some(ServeConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        0,
        Arc::new(AllowAll),
    ));
    cfg.write_concern = WriteConcern {
        min_insync_replicas: 1,
        ack_timeout: Duration::from_secs(5),
    };
    let master = Arc::new(Master::open(cfg).await?);
    let addr = master.serve_addr().unwrap();

    // 1. No replica yet. Creating a CF needs none (CF operations ignore the write concern),
    // but a data write is refused, and nothing is written (no seq used).
    master.create_cf(CfSpec::new("kv"))?;
    let m = master.clone();
    match tokio::task::spawn_blocking(move || m.put("kv", b"early", b"x")).await? {
        Err(Error::NotEnoughReplicas { required, in_sync }) => {
            println!("put refused: {in_sync} in sync, {required} required (nothing written)")
        }
        other => panic!("unexpected: {other:?}"),
    }
    // So attach a replica first: one that is connected and caught up counts as in sync.
    let fast = open_slave(dir.path(), "fast", addr, FetchConfig::default()).await?;
    master.wait_in_sync(1, Duration::from_secs(10)).await?;

    // 2. Success: the call returns only after `fast` acknowledged the entry.
    let seq = put_with(&master, b"a", b"1", WriteConcern::in_sync(1)).await?;
    println!(
        "put acked by 1 replica at seq {seq}; replica already has it: {}",
        fast.get("kv", b"a")?.is_some()
    );

    // Per-call override: `WriteConcern::default()` (0 replicas) means "don't wait for anyone",
    // whatever the config says. Handy for bulk loads or low-value data.
    let seq = put_with(&master, b"fire-and-forget", b"x", WriteConcern::default()).await?;
    println!("put with concern 0 at seq {seq} did not wait for any ack");

    // 3. NotEnoughReplicas: ask for 2 acks while only 1 replica is in sync. Nothing written.
    let before = master.replicas().len();
    match put_with(&master, b"b", b"2", WriteConcern::in_sync(2)).await {
        Err(Error::NotEnoughReplicas { required, in_sync }) => println!(
            "refused: required {required}, in sync {in_sync} ({before} connected). \
             Nothing was written, so retrying later is safe."
        ),
        other => panic!("unexpected: {other:?}"),
    }
    assert_eq!(master.get("kv", b"b")?, None);

    // 4. AckTimeout. Add a second replica that fetches in big, slow batches (it holds
    // entries back for up to 2 s), so it IS in sync but cannot acknowledge quickly.
    let slow_fetch = FetchConfig {
        min_batch_bytes: 1 << 20,
        max_wait: Duration::from_secs(2),
        max_batch_bytes: 1 << 20,
    };
    let slow = open_slave(dir.path(), "slow", addr, slow_fetch).await?;
    master.wait_in_sync(2, Duration::from_secs(10)).await?;
    let tight = WriteConcern {
        min_insync_replicas: 2,
        ack_timeout: Duration::from_millis(200),
    };
    match put_with(&master, b"c", b"3", tight).await {
        Err(Error::AckTimeout {
            seq,
            acked,
            required,
        }) => {
            println!(
                "ack timeout: seq {seq} is COMMITTED but only {acked}/{required} acked in time"
            );
            // The entry is on the master right now and keeps replicating:
            assert_eq!(master.get("kv", b"c")?, Some(b"3".to_vec()));
            slow.wait_for_seq(seq, Duration::from_secs(10)).await?;
            println!("...and the slow replica did receive it a moment later");
            // WRONG: calling put again here would append a second entry for the same logical
            // write. Instead: treat the write as done-but-unconfirmed. If you must be sure,
            // wait/poll (e.g. replica `wait_for_seq(seq)`), or make the operation idempotent
            // (an absolute Put of the same value is, but a counter increment is not).
        }
        other => panic!("unexpected: {other:?}"),
    }

    slow.shutdown().await?;
    fast.shutdown().await?;
    Arc::try_unwrap(master)
        .ok()
        .expect("no writers left")
        .shutdown()
        .await?;
    println!("done");
    Ok(())
}
