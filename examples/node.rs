//! The failover runbook with the `Node` handle (DESIGN.md section 13.2), in one process.
//!
//!   1. A is the master, B its replica, each behind one `Node`.
//!   2. B drops out; A takes one write B never sees; then A dies.
//!   3. B comes back and is promoted through the same handle (`promote_checked`).
//!   4. A restarts: its stored role is master, so it opens FENCED (B is a master).
//!   5. A rejoins as a slave of B: refused (diverged), its extra entry is shown.
//!   6. The operator accepts the loss explicitly: `Repair { accept_loss: AcceptDataLoss }`.
//!
//! Run with: `cargo run --example node`

use std::error::Error as StdError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    AcceptDataLoss, AllowAll, CfSpec, Error, MasterConfig, Node, NodeConfig, NodeState,
    RejoinOptions, RepointDivergence, RepointOptions, ServeConfig, SlaveConfig, UpstreamConfig,
};

type Res<T> = Result<T, Box<dyn StdError>>;
const WAIT: Duration = Duration::from_secs(10);

fn serve() -> ServeConfig {
    ServeConfig::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, Arc::new(AllowAll))
}

/// One node: the same directory and identifier in both roles. `upstream` is the default
/// upstream of the slave role, `peers` the nodes the fence check and `promote_checked` ask.
fn node_cfg(dir: &Path, id: &str, upstream: SocketAddr, peers: &[SocketAddr]) -> NodeConfig {
    let mut slave = SlaveConfig::new(id, dir.join(id), UpstreamConfig::from_addr(upstream));
    slave.serve = Some(serve());
    let master = MasterConfig::from_slave_config(&slave, Some(serve()));
    let mut cfg = NodeConfig::new(master, slave);
    cfg.peers = peers
        .iter()
        .copied()
        .map(UpstreamConfig::from_addr)
        .collect();
    cfg
}

fn show(node: &Node, key: &[u8]) -> String {
    match node.get("kv", key) {
        Ok(Some(v)) => String::from_utf8_lossy(&v).into_owned(),
        Ok(None) => "(absent)".into(),
        Err(e) => format!("(error: {e})"),
    }
}

#[tokio::main]
async fn main() -> Res<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    let nowhere: SocketAddr = ([127, 0, 0, 1], 1).into(); // A starts as master: no upstream yet

    println!("1. A is the master, B its replica");
    let a = Node::create_master(node_cfg(dir, "A", nowhere, &[])).await?;
    a.create_cf(CfSpec::new("kv"))?;
    let a_addr = a.serve_addr().expect("A serves");
    let b = Arc::new(Node::create_slave(node_cfg(dir, "B", a_addr, &[a_addr])).await?);
    let seq = a.put("kv", b"k1", b"everyone has this")?;
    b.wait_for_seq(seq, WAIT).await?;
    b.wait_streaming(WAIT).await?;
    // `state()` is exact: it reads the running slave, so this prints `Streaming`.
    println!("   B state: {:?}", b.state());

    println!("2. B drops out, A writes k2 alone, then A dies");
    b.shutdown().await?;
    a.put("kv", b"k2", b"only A has this")?;
    a.shutdown().await?;

    println!("3. B comes back and is promoted (A is unreachable: reported, not blocking)");
    b.reopen().await?;
    b.promote_checked().await?;
    b.put("kv", b"k3", b"written on B")?;
    let b_addr = b.serve_addr().expect("B serves");
    println!("   B state: {:?}", b.state());

    println!("4. A restarts: a stored master opens fenced until a peer check clears it");
    let a = Node::open(node_cfg(dir, "A", b_addr, &[b_addr])).await?;
    match a.state() {
        NodeState::Fenced { reason } => println!("   A is fenced: {reason}"),
        other => return Err(format!("expected A fenced, got {other:?}").into()),
    }
    match a.get("kv", b"k1") {
        Err(Error::Fenced { .. }) => println!("   reads on A are refused while fenced"),
        other => return Err(format!("expected Fenced, got {other:?}").into()),
    }

    println!("5. A rejoins as a slave of B (no wipe); divergence is refused by default");
    match a.rejoin_as_slave(None, RejoinOptions::default()).await {
        Err(Error::Diverged {
            fork_seq,
            divergent_entries,
            ..
        }) => {
            println!(
                "   refused: {divergent_entries} entr(y/ies) from seq {fork_seq} exist only on A"
            );
            for e in a.diverged_entries()? {
                println!("   salvageable entry: seq {} (epoch {})", e.seq, e.epoch);
            }
            println!("   A still reads its own data: k2 = {}", show(&a, b"k2"));
        }
        Ok(outcome) => println!("   A follows B: {outcome:?}"),
        Err(e) => return Err(e.into()),
    }

    println!("6. The operator accepts the loss of A's extra entry, explicitly");
    let repair = RepointOptions {
        on_divergence: RepointDivergence::Repair {
            accept_loss: AcceptDataLoss,
        },
    };
    let outcome = a.repoint(UpstreamConfig::from_addr(b_addr), repair).await?;
    println!("   {outcome:?}");
    a.wait_for_seq(b.last_seq()?, WAIT).await?;
    println!(
        "   A: k1 = {}, k2 = {}, k3 = {}",
        show(&a, b"k1"),
        show(&a, b"k2"),
        show(&a, b"k3")
    );
    assert_eq!(a.get("kv", b"k2")?, None);
    assert_eq!(a.get("kv", b"k3")?, Some(b"written on B".to_vec()));

    a.shutdown().await?;
    b.shutdown().await?;
    println!("done");
    Ok(())
}
