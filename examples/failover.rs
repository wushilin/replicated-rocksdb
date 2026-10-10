//! The failover runbook (DESIGN.md section 13.1), end to end in one process.
//!
//!   1. Fence/stop the old master A.
//!   2. `admin::gather` every node's status and `choose_promotion_candidate`.
//!   3. `Slave::promote_checked` the most advanced replica (it REFUSES unless safe).
//!   4. `repoint` the other replicas at the new master (it REFUSES to discard data).
//!
//! Scenario 1 is the happy path. Scenario 2 shows what happens if you ignore the advice and
//! promote a stale replica: the ahead replica gets `Error::Diverged`, and only an explicit
//! `AcceptDataLoss` can make it throw its extra entries away.
//!
//! Run with: `cargo run --example failover`

use std::error::Error as StdError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::admin::{choose_promotion_candidate, gather};
use replicated_rocksdb::{
    AcceptDataLoss, AllowAll, CfSpec, Error, Master, MasterConfig, ReadApi, RepointDivergence,
    RepointOptions, ServeConfig, Slave, SlaveConfig, UpstreamConfig,
};

type Res<T> = Result<T, Box<dyn StdError>>;
const WAIT: Duration = Duration::from_secs(10);

fn serve() -> ServeConfig {
    ServeConfig::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, Arc::new(AllowAll))
}

fn val(db: &impl ReadApi, key: &[u8]) -> Option<String> {
    db.get("kv", key)
        .unwrap()
        .map(|v| String::from_utf8_lossy(&v).into_owned())
}

/// Replicas are slaves that also `serve`, so peers (and the admin tools) can query them and
/// they can later become masters.
fn replica_cfg(dir: &Path, id: &str, upstream: SocketAddr) -> SlaveConfig {
    let mut cfg = SlaveConfig::new(id, dir.join(id), UpstreamConfig::from_addr(upstream));
    cfg.serve = Some(serve());
    cfg
}

/// The config to promote replica `id` with: same identifier, directory, retention, tuning
/// and clock as its `SlaveConfig`, now serving as a master.
fn master_cfg(dir: &Path, id: &str) -> MasterConfig {
    let slave_cfg = replica_cfg(dir, id, ([127, 0, 0, 1], 0).into());
    MasterConfig::from_slave_config(&slave_cfg, Some(serve()))
}

/// Master A with replicas B and C. B stops early (so it is BEHIND), A writes two more
/// entries which only C receives, then A dies. Returns B (reopened, its upstream gone), C,
/// and A's old address.
async fn crash_scenario(dir: &Path) -> Res<(Slave, Slave, SocketAddr)> {
    let mut acfg = MasterConfig::new("A", dir.join("A"));
    acfg.serve = Some(serve());
    let a = Master::open(acfg).await?;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("kv"))?;

    let b_cfg = replica_cfg(dir, "B", a_addr);
    let b = Slave::open(b_cfg.clone()).await?;
    let c = Slave::open(replica_cfg(dir, "C", a_addr)).await?;
    b.wait_streaming(WAIT).await?;
    c.wait_streaming(WAIT).await?;

    let seq = a.put("kv", b"k1", b"written while everyone was up")?;
    b.wait_for_seq(seq, WAIT).await?;
    c.wait_for_seq(seq, WAIT).await?;

    let b_seq = b.last_seq();
    b.shutdown().await?; // B drops out and falls behind
    a.put("kv", b"k2", b"B never saw this")?;
    let seq = a.put("kv", b"k3", b"nor this")?;
    c.wait_for_seq(seq, WAIT).await?;

    // Step 1 of the runbook: the old master must be stopped/fenced BEFORE promoting anyone.
    a.shutdown().await?;
    println!(
        "master A is down; C is at seq {}, B is behind at seq {b_seq}",
        c.last_seq()
    );

    // Bring B back up; its configured upstream (A) is gone, so it just sits there.
    let b = Slave::open(b_cfg).await?;
    Ok((b, c, a_addr))
}

#[tokio::main]
async fn main() -> Res<()> {
    // =========================== Scenario 1: the safe runbook ==========================
    println!("=== Scenario 1: promote the most advanced replica ===");
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    let (b, c, a_addr) = crash_scenario(dir).await?;
    let (b_addr, c_addr) = (b.serve_addr().unwrap(), c.serve_addr().unwrap());

    // Step 2: ask every node (including the dead one) for its status. Unreachable nodes are
    // reported, not fatal. `client_id` just names the caller in the nodes' logs.
    let nodes = [a_addr, b_addr, c_addr].map(UpstreamConfig::from_addr);
    let gathered = gather(&nodes, "operator").await;
    for n in &gathered.nodes {
        println!("  {} role={:?} last_seq={}", n.node_id, n.role, n.last_seq);
    }
    for (addr, why) in &gathered.unreachable {
        println!("  unreachable: {addr} ({why})");
    }
    let choice = choose_promotion_candidate(&gathered.nodes)?;
    println!(
        "candidate: {} (compatible: {:?})",
        choice.candidate.node_id, choice.compatible
    );
    assert_eq!(choice.candidate.node_id, "C");

    // Step 3: `promote_checked` re-verifies against live peers and refuses if unsafe. Say the
    // operator picks the WRONG node (B): it refuses with NotMostAdvanced and hands the slave
    // back, untouched and still running, inside the error.
    let peers = [
        UpstreamConfig::from_addr(c_addr),
        UpstreamConfig::from_addr(a_addr),
    ];
    let b = match b.promote_checked(master_cfg(dir, "B"), &peers).await {
        Ok(_) => unreachable!("B is behind C"),
        Err(refused) => {
            println!("promoting B refused: {}", refused.error);
            assert!(matches!(refused.error, Error::NotMostAdvanced { .. }));
            refused
                .into_slave()
                .expect("a refusal gives the slave back")
        }
    };
    // Promote the right node. The dead master A is merely reported as unreachable.
    let peers = [
        UpstreamConfig::from_addr(b_addr),
        UpstreamConfig::from_addr(a_addr),
    ];
    // `into_parts` keeps the slave of a refusal while `?` propagates the error.
    let new_master = match c.promote_checked(master_cfg(dir, "C"), &peers).await {
        Ok(master) => master,
        Err(refused) => {
            let (error, _slave) = refused.into_parts();
            return Err(error.into());
        }
    };
    let new_addr = new_master.serve_addr().unwrap();
    println!(
        "C is now master, epoch {}, serving on {new_addr}",
        new_master.identity().epoch
    );

    // Step 4: repoint the remaining replica. The default options REFUSE to discard data;
    // B is only behind, so it simply resumes.
    let outcome = b
        .repoint(
            UpstreamConfig::from_addr(new_addr),
            RepointOptions::default(),
        )
        .await?;
    println!("B repointed to C: {outcome:?}");

    // The new master accepts writes and they reach B.
    let seq = new_master.put("kv", b"k4", b"written after failover")?;
    b.wait_for_seq(seq, WAIT).await?;
    println!("B sees k3 (from old epoch): {:?}", val(&b, b"k3"));
    println!("B sees k4 (from new master): {:?}", val(&b, b"k4"));
    b.shutdown().await?;
    new_master.shutdown().await?;
    drop(tmp);

    // ============== Scenario 2: promoting a stale replica loses data ===================
    println!("\n=== Scenario 2: what a wrong promotion costs ===");
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    let (b, c, _a_addr) = crash_scenario(dir).await?;

    // The operator ignores the runbook and uses plain `promote` on the stale B. Unlike
    // `promote_checked` it does no peer checks: C's two extra entries are about to be orphaned.
    let new_master = b.promote(master_cfg(dir, "B")).await?;
    let new_addr = new_master.serve_addr().unwrap();

    // The default repoint refuses to throw C's data away: the error says exactly what.
    match c
        .repoint(
            UpstreamConfig::from_addr(new_addr),
            RepointOptions::default(),
        )
        .await
    {
        Err(Error::Diverged {
            fork_seq,
            local_last_seq,
            divergent_entries,
        }) => println!(
            "C refused to follow B: diverged at seq {fork_seq}; {divergent_entries} local entries \
             (up to seq {local_last_seq}) are not on B"
        ),
        other => panic!("expected Diverged, got {other:?}"),
    }
    // C is halted but its data is intact and still readable, so the entries can be salvaged.
    println!("C is halted but still readable: k3 = {:?}", val(&c, b"k3"));

    // The explicit opt-in: you must literally write `AcceptDataLoss`. Nothing in the crate
    // does this for you. C discards its local data and re-bootstraps from the new master.
    let opts = RepointOptions {
        on_divergence: RepointDivergence::Rebootstrap {
            accept_loss: AcceptDataLoss,
        },
    };
    let outcome = c.repoint(UpstreamConfig::from_addr(new_addr), opts).await?;
    println!("C repointed with AcceptDataLoss: {outcome:?}");
    println!(
        "C after re-bootstrap: k3 = {:?} (lost for good)",
        val(&c, b"k3")
    );

    c.shutdown().await?;
    new_master.shutdown().await?;
    println!("done");
    Ok(())
}
