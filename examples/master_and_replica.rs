//! One master, one read-only replica (slave) following it.
//!
//! Shows: serving replication, writes on the master, read-your-writes on the slave with
//! `wait_for_seq`, per-replica monitoring (`Master::replicas`, `Slave::status`), and the
//! two fetch modes (real-time vs batched).
//!
//! Run with: `cargo run --example master_and_replica`

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    AllowAll, CfSpec, FetchConfig, Master, MasterConfig, ReadApi, ServeConfig, Slave, SlaveConfig,
    SlaveState, UpstreamConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let localhost = IpAddr::V4(Ipv4Addr::LOCALHOST);

    // ---- Master: add a `ServeConfig` to accept replicas. Port 0 = pick a free port. -------
    // `AllowAll` means no authentication (fine for a demo on loopback; see the
    // `secure_replication` example for TLS + tokens).
    let mut mcfg = MasterConfig::new("master-1", dir.path().join("master"));
    mcfg.serve = Some(ServeConfig::new(localhost, 0, Arc::new(AllowAll)));
    let master = Master::open(mcfg).await?;
    let addr = master.serve_addr().expect("master is serving");
    println!("master serving on {addr}");

    master.create_cf(CfSpec::new("users"))?;
    master.put("users", b"alice", b"Alice A.")?;

    // ---- Slave: just point it at the master. It bootstraps a snapshot, then tails the log.
    // Fetch mode: the DEFAULT is real-time (min_batch_bytes = 0): the master sends each
    // entry as soon as it is committed. That is what you want for low replication lag.
    let scfg = SlaveConfig::new(
        "replica-1",
        dir.path().join("replica"),
        UpstreamConfig::from_addr(addr),
    );
    let slave = Slave::open(scfg).await?;

    // Replication is asynchronous, so a read right after a master write may not see it yet.
    // `wait_for_seq(seq)` gives read-your-writes: block until the slave applied `seq`.
    let seq = master.put("users", b"bob", b"Bob B.")?;
    slave.wait_for_seq(seq, Duration::from_secs(10)).await?;
    println!(
        "replica read bob at seq {seq}: {:?}",
        slave
            .get("users", b"bob")?
            .map(|v| String::from_utf8_lossy(&v).into_owned())
    );
    println!(
        "replica has alice (from bootstrap): {}",
        slave.get("users", b"alice")?.is_some()
    );
    // The slave is read-only: the type has no write methods at all.

    // ---- Monitoring ------------------------------------------------------------------------
    // Wait until the master considers the replica "in sync" (fully caught up recently).
    // This is the signal `WriteConcern` uses (see the `write_concern` example).
    master.wait_in_sync(1, Duration::from_secs(10)).await?;
    for r in master.replicas() {
        println!(
            "master sees replica {:?}: applied_seq={} lag_entries={} lag_ms={:?} in_sync={}",
            r.node_id, r.applied_seq, r.lag_entries, r.lag_ms, r.in_sync
        );
    }
    let st = slave.status();
    println!(
        "slave status: state={:?} applied_seq={} upstream_last_seq={} upstream={}",
        st.state, st.applied_seq, st.upstream_last_seq, st.upstream
    );
    assert_eq!(st.state, SlaveState::Streaming);
    assert_eq!(slave.last_seq(), master.last_seq());

    // ---- Real-time vs batched fetching ------------------------------------------------------
    // A Kafka-style trade-off, chosen per slave (`SlaveConfig::fetch`):
    //
    //   real-time (default): min_batch_bytes = 0. Every committed entry is pushed
    //     immediately. Lowest lag, most small network messages.
    //
    //   batched: the master holds a fetch until it has at least `min_batch_bytes` of
    //     entries OR `max_wait` has passed, and never sends more than `max_batch_bytes` at
    //     once. Fewer, larger messages (cheaper for busy or far-away replicas) at the cost of
    //     up to `max_wait` extra replication lag - which also delays write-concern acks.
    let realtime = FetchConfig::default();
    let batched = FetchConfig {
        min_batch_bytes: 64 * 1024,           // wait for 64 KiB of entries...
        max_wait: Duration::from_millis(500), // ...but never longer than 0.5 s
        max_batch_bytes: 1024 * 1024,         // ...and cap one message at 1 MiB
    };
    println!("default fetch (real-time): {realtime:?}");

    let mut bcfg = SlaveConfig::new(
        "replica-batched",
        dir.path().join("replica-batched"),
        UpstreamConfig::from_addr(addr),
    );
    bcfg.fetch = batched;
    let batched_slave = Slave::open(bcfg).await?;
    // Let it finish its bootstrap and attach, so the next write travels through the log.
    batched_slave
        .wait_streaming(Duration::from_secs(10))
        .await?;
    master.wait_in_sync(2, Duration::from_secs(10)).await?;
    let seq = master.put("users", b"carol", b"Carol C.")?;
    let t = std::time::Instant::now();
    slave.wait_for_seq(seq, Duration::from_secs(10)).await?;
    let realtime_ms = t.elapsed().as_millis();
    batched_slave
        .wait_for_seq(seq, Duration::from_secs(10))
        .await?;
    let batched_ms = t.elapsed().as_millis();
    println!("real-time replica applied seq {seq} after {realtime_ms} ms");
    println!("batched replica applied seq {seq} after {batched_ms} ms (held back by max_wait)");
    for r in master.replicas() {
        println!(
            "  {:<16} fetch: min_batch_bytes={} max_wait={:?} max_batch_bytes={}",
            r.node_id, r.fetch.min_batch_bytes, r.fetch.max_wait, r.fetch.max_batch_bytes
        );
    }

    // Shut down replicas first, then the master.
    batched_slave.shutdown().await?;
    slave.shutdown().await?;
    master.shutdown().await?;
    println!("done");
    Ok(())
}
