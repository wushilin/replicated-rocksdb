//! An interactive web playground: four `Node`s in one process, replicating as a daisy chain
//! A → B → C → D. Write on the master, read any key on every node side by side, and promote,
//! demote, repoint, rejoin, unfence, stop and restart nodes from the browser. Every node's
//! state, position, lag and last error are pushed live over a WebSocket, fed by each slave's
//! `ReplicationListener` (`on_entry`, `on_state_change`, ...) and each `Node::watch_state`.
//!
//! Run with: `cargo run --example playground`, then open <http://127.0.0.1:8080>.
//!
//! Options: `--http ADDR` (default 127.0.0.1:8080), `--port-base N` (replication ports N..N+3,
//! default 7101), `--dir PATH` (keep the data there; default a temporary directory, deleted
//! on exit). A `--dir` that already holds the nodes reopens them: stored masters come back
//! fenced until a peer check clears them, exactly as in production.

use std::collections::VecDeque;
use std::error::Error as StdError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::{Query, State};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use replicated_rocksdb::{
    AcceptDataLoss, AcceptSplitBrainRisk, AllowAll, Backoff, CfSpec, HaltAction, IterMode,
    LogEntry, MasterConfig, Node, NodeConfig, NodeState, Op, ReadApi, RejoinOptions,
    ReplicationListener, RepointDivergence, RepointOptions, Seq, ServeConfig, SlaveConfig,
    SlaveState, UnfenceOptions, UpstreamConfig, VerifyMode, VerifyReport, WriteConcern,
};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::sync::{Notify, broadcast};

const CF: &str = "kv";
const IDS: [&str; 4] = ["A", "B", "C", "D"];
const LOG_KEEP: usize = 300;

type Res<T> = Result<T, Box<dyn StdError + Send + Sync>>;

// ---------------------------------------------------------------------------------------
// The event bus: listeners and handlers publish here; WebSocket clients subscribe.

struct Bus {
    tx: broadcast::Sender<String>,
    /// Set whenever something changed; the snapshot pusher sends a fresh view.
    dirty: Notify,
    log: Mutex<VecDeque<Value>>,
}

impl Bus {
    fn emit(&self, node: &str, kind: &str, text: impl Into<String>) {
        let ev = json!({
            "type": "event",
            "t": now_ms(),
            "node": node,
            "kind": kind,
            "text": text.into(),
        });
        {
            let mut log = self.log.lock().unwrap();
            if log.len() == LOG_KEEP {
                log.pop_front();
            }
            log.push_back(ev.clone());
        }
        let _ = self.tx.send(ev.to_string());
        self.dirty.notify_one();
    }
}

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_millis() as u64)
}

fn show(bytes: &[u8]) -> String {
    let s = String::from_utf8_lossy(bytes);
    if s.chars().count() > 40 {
        format!("{}…", s.chars().take(40).collect::<String>())
    } else {
        s.into_owned()
    }
}

fn describe_ops(entry: &LogEntry) -> String {
    let ops = match entry.decode_ops() {
        Ok(ops) => ops,
        Err(e) => return format!("(undecodable: {e})"),
    };
    let first = match ops.first() {
        Some(Op::Put { cf, key, value, .. }) => {
            format!("put {cf}/{} = {}", show(key), show(value))
        }
        Some(Op::Delete { cf, key }) => format!("delete {cf}/{}", show(key)),
        Some(other) => format!("{other:?}"),
        None => "empty".into(),
    };
    if ops.len() > 1 {
        format!("{first} (+{} ops)", ops.len() - 1)
    } else {
        first
    }
}

/// The notifier: each slave's listener forwards what it applies and every state change.
struct UiListener {
    node: &'static str,
    bus: Arc<Bus>,
}

impl ReplicationListener for UiListener {
    fn on_entry(&self, entry: &LogEntry) {
        self.bus.emit(
            self.node,
            "entry",
            format!(
                "applied #{} (epoch {}): {}",
                entry.seq,
                entry.epoch,
                describe_ops(entry)
            ),
        );
    }
    fn on_bootstrap_begin(&self, snapshot_seq: Seq) {
        self.bus.emit(
            self.node,
            "bootstrap",
            format!("bootstrap started at snapshot seq {snapshot_seq}"),
        );
    }
    fn on_bootstrap_complete(&self, snapshot_seq: Seq) {
        self.bus.emit(
            self.node,
            "bootstrap",
            format!("bootstrap complete at seq {snapshot_seq}"),
        );
    }
    fn on_state_change(&self, state: &SlaveState) {
        self.bus
            .emit(self.node, "state", format!("slave state → {state:?}"));
    }
    fn on_verify_complete(&self, report: &VerifyReport) {
        self.bus.emit(self.node, "verify", verify_text(report));
    }
    fn on_rewind(&self, last_seq: Seq) {
        self.bus.emit(
            self.node,
            "repair",
            format!("repair rewound the log to seq {last_seq}"),
        );
    }
}

fn verify_text(r: &VerifyReport) -> String {
    format!(
        "verify {:?} at seq {}: {} keys, {} mismatches",
        r.mode,
        r.seq,
        r.keys_checked,
        r.mismatches.len()
    )
}

// ---------------------------------------------------------------------------------------
// The nodes.

struct Member {
    id: &'static str,
    addr: SocketAddr,
    node: Node,
}

struct App {
    members: Vec<Member>,
    bus: Arc<Bus>,
}

impl App {
    fn member(&self, id: &str) -> Result<&Member, String> {
        self.members
            .iter()
            .find(|m| m.id == id)
            .ok_or_else(|| format!("no node {id:?}"))
    }

    /// The node id serving at `uri` (`http://127.0.0.1:7101`), if it is one of ours.
    fn id_of(&self, uri: &str) -> Option<&'static str> {
        self.members
            .iter()
            .find(|m| uri.ends_with(&format!(":{}", m.addr.port())))
            .map(|m| m.id)
    }

    fn snapshot(&self) -> Value {
        let nodes: Vec<Value> = self.members.iter().map(|m| self.node_view(m)).collect();
        json!({ "type": "snapshot", "t": now_ms(), "nodes": nodes })
    }

    fn node_view(&self, m: &Member) -> Value {
        let state = m.node.state();
        let (kind, detail) = match &state {
            NodeState::Master { identity } => ("master", format!("epoch {}", identity.epoch)),
            NodeState::Fenced { reason } => ("fenced", reason.to_string()),
            NodeState::Slave { state, .. } => ("slave", format!("{state:?}")),
            NodeState::Switching => ("switching", String::new()),
            NodeState::Failed { error } => ("failed", error.clone()),
            NodeState::Stopped => ("stopped", String::new()),
            other => ("unknown", format!("{other:?}")),
        };
        let slave = m.node.status().ok().map(|s| {
            json!({
                "state": format!("{:?}", s.state),
                "applied_seq": s.applied_seq,
                "upstream": s.upstream,
                "upstream_id": self.id_of(&s.upstream),
                "upstream_chain": s.upstream_chain,
                "upstream_last_seq": s.upstream_last_seq,
                "origin_last_seq": s.origin_last_seq,
                "lag_ms": s.lag_ms,
                "epoch": s.origin_identity.map(|i| i.epoch),
                "last_contact_ago_ms": s.last_contact.and_then(|t| t.elapsed().ok()).map(|d| d.as_millis() as u64),
                "last_error": s.last_error,
                "last_error_kind": s.last_error_kind.map(|k| format!("{k:?}")),
                "listener_failed": s.listener_failed,
                "last_verify": s.last_verify.as_ref().map(verify_text),
            })
        });
        // Direct downstream replicas: what a write concern counts on a master.
        let replicas: Vec<Value> = m
            .node
            .replicas()
            .unwrap_or_default()
            .iter()
            .map(|r| {
                json!({
                    "node_id": r.node_id,
                    "applied_seq": r.applied_seq,
                    "lag_entries": r.lag_entries,
                    "lag_ms": r.lag_ms,
                    "in_sync": r.in_sync,
                    "epoch": r.epoch,
                })
            })
            .collect();
        json!({
            "id": m.id,
            "addr": m.addr.to_string(),
            "replicas": replicas,
            "kind": kind,
            "detail": detail,
            "last_seq": m.node.last_seq().ok(),
            "slave": slave,
        })
    }
}

fn serve_config(port: u16) -> ServeConfig {
    ServeConfig::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port, Arc::new(AllowAll))
}

/// One node: same directory, identifier and replication port in both roles, so it keeps its
/// address through promotions and demotions. Every other node is a peer (fence check,
/// `promote_checked`).
fn node_config(dir: &Path, i: usize, addrs: &[SocketAddr], bus: &Arc<Bus>) -> NodeConfig {
    let id = IDS[i];
    let upstream = if i == 0 { addrs[0] } else { addrs[i - 1] };
    let mut slave = SlaveConfig::new(id, dir.join(id), UpstreamConfig::from_addr(upstream));
    slave.serve = Some(serve_config(addrs[i].port()));
    slave.reconnect = Backoff {
        initial: Duration::from_millis(100),
        max: Duration::from_secs(2),
        factor: 2,
    };
    slave.listener = Some(Arc::new(UiListener {
        node: id,
        bus: bus.clone(),
    }));
    let master = MasterConfig::from_slave_config(&slave, Some(serve_config(addrs[i].port())));
    let mut cfg = NodeConfig::new(master, slave);
    cfg.peers = addrs
        .iter()
        .enumerate()
        .filter(|(j, _)| *j != i)
        .map(|(_, a)| UpstreamConfig::from_addr(*a))
        .collect();
    cfg
}

async fn start_nodes(dir: &Path, port_base: u16, bus: &Arc<Bus>) -> Res<Vec<Member>> {
    let addrs: Vec<SocketAddr> = (0..IDS.len())
        .map(|i| SocketAddr::from(([127, 0, 0, 1], port_base + i as u16)))
        .collect();
    let existing = dir.join(IDS[0]).exists();
    let mut members = Vec::new();
    if existing {
        // Reopen everything at once, so the stored master's fence check finds its peers.
        let opens = (0..IDS.len()).map(|i| Node::open(node_config(dir, i, &addrs, bus)));
        for (i, node) in futures_join_all(opens).await.into_iter().enumerate() {
            members.push(Member {
                id: IDS[i],
                addr: addrs[i],
                node: node?,
            });
        }
    } else {
        for i in 0..IDS.len() {
            let cfg = node_config(dir, i, &addrs, bus);
            let node = if i == 0 {
                let n = Node::create_master(cfg).await?;
                n.create_cf(CfSpec::new(CF))?;
                n
            } else {
                Node::create_slave(cfg).await?
            };
            members.push(Member {
                id: IDS[i],
                addr: addrs[i],
                node,
            });
        }
    }
    Ok(members)
}

/// `join_all` without the `futures` crate: spawn each open and await them in order.
async fn futures_join_all<F, T>(futs: impl Iterator<Item = F>) -> Vec<T>
where
    F: std::future::Future<Output = T> + Send + 'static,
    T: Send + 'static,
{
    let handles: Vec<_> = futs.map(tokio::spawn).collect();
    let mut out = Vec::new();
    for h in handles {
        out.push(h.await.expect("open task panicked"));
    }
    out
}

// ---------------------------------------------------------------------------------------
// HTTP and WebSocket.

type AppState = State<Arc<App>>;

fn reply(ok: bool, message: impl Into<String>) -> Json<Value> {
    Json(json!({ "ok": ok, "message": message.into() }))
}

async fn index() -> Html<&'static str> {
    Html(include_str!("index.html"))
}

async fn ws(upgrade: WebSocketUpgrade, State(app): AppState) -> impl IntoResponse {
    upgrade.on_upgrade(move |socket| client(socket, app))
}

async fn client(mut socket: WebSocket, app: Arc<App>) {
    let mut rx = app.bus.tx.subscribe();
    let history: Vec<Value> = app.bus.log.lock().unwrap().iter().cloned().collect();
    let hello = json!({ "type": "history", "events": history });
    if socket
        .send(Message::Text(hello.to_string().into()))
        .await
        .is_err()
        || socket
            .send(Message::Text(app.snapshot().to_string().into()))
            .await
            .is_err()
    {
        return;
    }
    loop {
        tokio::select! {
            msg = rx.recv() => match msg {
                Ok(text) => {
                    if socket.send(Message::Text(text.into())).await.is_err() {
                        return;
                    }
                }
                // A slow browser skipped some events; the next snapshot catches it up.
                Err(broadcast::error::RecvError::Lagged(_)) => {}
                Err(broadcast::error::RecvError::Closed) => return,
            },
            incoming = socket.recv() => match incoming {
                Some(Ok(Message::Close(_))) | None | Some(Err(_)) => return,
                Some(Ok(_)) => {}
            },
        }
    }
}

/// Sends a fresh snapshot after every change (at most every 150 ms), and at least once a
/// second so lag and "last contact" stay current.
async fn snapshot_pusher(app: Arc<App>) {
    loop {
        let _ = tokio::time::timeout(Duration::from_secs(1), app.bus.dirty.notified()).await;
        tokio::time::sleep(Duration::from_millis(150)).await;
        let _ = app.bus.tx.send(app.snapshot().to_string());
    }
}

#[derive(Deserialize)]
struct KeyQuery {
    key: String,
}

/// The value of one key on every node, side by side.
async fn read_key(State(app): AppState, Query(q): Query<KeyQuery>) -> Json<Value> {
    let values: Vec<Value> = app
        .members
        .iter()
        .map(|m| match m.node.get_with_meta(CF, q.key.as_bytes()) {
            Ok(Some((v, meta))) => json!({
                "node": m.id,
                "value": String::from_utf8_lossy(&v),
                "write_seq": meta.write_seq,
            }),
            Ok(None) => json!({ "node": m.id, "absent": true }),
            Err(e) => json!({ "node": m.id, "error": e.to_string() }),
        })
        .collect();
    Json(json!({ "key": q.key, "values": values }))
}

#[derive(Deserialize)]
struct ScanQuery {
    node: String,
}

/// Up to 200 keys of one node. The snapshot is dropped at once: a rejoin refuses to run
/// while a `ReadSnapshot` is alive.
async fn scan(State(app): AppState, Query(q): Query<ScanQuery>) -> Json<Value> {
    let m = match app.member(&q.node) {
        Ok(m) => m,
        Err(e) => return reply(false, e),
    };
    let rows = m.node.snapshot().and_then(|snap| {
        snap.iter(CF, IterMode::Start)?
            .take(200)
            .map(|r| r.map(|(k, v)| json!([show(&k), show(&v)])))
            .collect::<replicated_rocksdb::Result<Vec<Value>>>()
    });
    match rows {
        Ok(rows) => Json(json!({ "ok": true, "node": m.id, "rows": rows })),
        Err(e) => reply(false, e.to_string()),
    }
}

#[derive(Deserialize)]
struct WriteReq {
    node: String,
    op: String,
    key: String,
    #[serde(default)]
    value: String,
    /// > 1: write `key-0` .. `key-{count-1}`.
    #[serde(default)]
    count: Option<u32>,
    /// Write concern: wait for this many in-sync replicas (0 = asynchronous).
    #[serde(default)]
    min_insync: usize,
}

async fn write(State(app): AppState, Json(req): Json<WriteReq>) -> Json<Value> {
    let app2 = app.clone();
    let res = tokio::task::spawn_blocking(move || -> Result<String, String> {
        let m = app2.member(&req.node)?;
        let concern = WriteConcern {
            min_insync_replicas: req.min_insync,
            ack_timeout: Duration::from_secs(3),
        };
        let count = req.count.unwrap_or(1).clamp(1, 5000);
        let mut last = 0;
        for i in 0..count {
            let key = if count == 1 {
                req.key.clone()
            } else {
                format!("{}-{i}", req.key)
            };
            let r = match req.op.as_str() {
                "put" => m
                    .node
                    .put_with(CF, key.as_bytes(), req.value.as_bytes(), concern),
                "delete" => m.node.delete(CF, key.as_bytes()),
                other => return Err(format!("unknown op {other:?}")),
            };
            last = r.map_err(|e| {
                let msg = format!("{} {key} on {} failed: {e}", req.op, m.id);
                app2.bus.emit(m.id, "error", msg.clone());
                msg
            })?;
        }
        let what = if count == 1 {
            format!("{} {}", req.op, req.key)
        } else {
            format!("{} {}-0..{}", req.op, req.key, count - 1)
        };
        let msg = format!("{what} committed at seq {last}");
        app2.bus.emit(m.id, "write", msg.clone());
        Ok(msg)
    })
    .await
    .unwrap_or_else(|e| Err(format!("write task failed: {e}")));
    match res {
        Ok(m) => reply(true, m),
        Err(e) => reply(false, e),
    }
}

#[derive(Deserialize)]
struct ActionReq {
    node: String,
    action: String,
    #[serde(default)]
    target: Option<String>,
    /// Consent for `repoint` / `rejoin` / `rebootstrap` to discard divergent entries.
    #[serde(default)]
    accept_loss: bool,
    /// Consent for `unfence` to ignore peers that do not answer.
    #[serde(default)]
    accept_split_brain: bool,
}

async fn action(State(app): AppState, Json(req): Json<ActionReq>) -> Json<Value> {
    let m = match app.member(&req.node) {
        Ok(m) => m,
        Err(e) => return reply(false, e),
    };
    let target = || -> Result<UpstreamConfig, String> {
        let t = req.target.as_deref().ok_or("choose a target node")?;
        if t == m.id {
            return Err("a node cannot replicate from itself".into());
        }
        Ok(UpstreamConfig::from_addr(app.member(t)?.addr))
    };
    let divergence = if req.accept_loss {
        RepointDivergence::Repair {
            accept_loss: AcceptDataLoss,
        }
    } else {
        RepointDivergence::Refuse
    };
    app.bus
        .emit(m.id, "action", format!("{} requested", req.action));
    let n = &m.node;
    let result: Result<String, String> = async {
        let e = |e: replicated_rocksdb::Error| e.to_string();
        Ok(match req.action.as_str() {
            "promote_checked" => n
                .promote_checked()
                .await
                .map(|()| "promoted".into())
                .map_err(e)?,
            "promote" => n
                .promote()
                .await
                .map(|()| "promoted WITHOUT checking peers".into())
                .map_err(e)?,
            "demote" => n.demote().await.map(|()| "demoted".into()).map_err(e)?,
            "repoint" => {
                let up = target()?;
                let out = n
                    .repoint(
                        up,
                        RepointOptions {
                            on_divergence: divergence,
                        },
                    )
                    .await
                    .map_err(e)?;
                format!("repointed: {out:?}")
            }
            "rejoin" => {
                let up = target()?;
                let out = n
                    .rejoin_as_slave(
                        Some(up),
                        RejoinOptions {
                            on_divergence: divergence,
                        },
                    )
                    .await
                    .map_err(e)?;
                format!("rejoined: {out:?}")
            }
            "unfence" => {
                let opts = UnfenceOptions {
                    allow_unreachable: req.accept_split_brain.then_some(AcceptSplitBrainRisk),
                };
                n.unfence(opts).await.map_err(e)?;
                format!("fence check done: {:?}", n.state())
            }
            "stop" => n.shutdown().await.map(|()| "stopped".into()).map_err(e)?,
            "start" => {
                n.reopen().await.map_err(e)?;
                format!("reopened: {:?}", n.state())
            }
            "clear_halt" => n
                .clear_halt(HaltAction::Retry)
                .await
                .map(|()| "halt cleared, retrying".into())
                .map_err(e)?,
            "rebootstrap" => {
                if !req.accept_loss {
                    return Err("rebootstrap discards local data: tick \"accept data loss\"".into());
                }
                n.clear_halt(HaltAction::Rebootstrap {
                    accept_loss: AcceptDataLoss,
                })
                .await
                .map(|()| "rebootstrapping".into())
                .map_err(e)?
            }
            "verify" => verify_text(&n.verify_now(VerifyMode::Full).await.map_err(e)?),
            "diverged" => {
                let entries = n.diverged_entries().map_err(e)?;
                let list: Vec<String> = entries
                    .iter()
                    .map(|en| format!("#{} {}", en.seq, describe_ops(en)))
                    .collect();
                format!("{} divergent entries: {}", list.len(), list.join("; "))
            }
            other => return Err(format!("unknown action {other:?}")),
        })
    }
    .await;
    match result {
        Ok(msg) => {
            app.bus.emit(m.id, "ok", format!("{}: {msg}", req.action));
            reply(true, msg)
        }
        Err(err) => {
            app.bus
                .emit(m.id, "error", format!("{} refused: {err}", req.action));
            reply(false, err)
        }
    }
}

// ---------------------------------------------------------------------------------------

struct Args {
    http: SocketAddr,
    port_base: u16,
    dir: Option<PathBuf>,
}

fn args() -> Res<Args> {
    let mut a = Args {
        http: "127.0.0.1:8080".parse()?,
        port_base: 7101,
        dir: None,
    };
    let mut it = std::env::args().skip(1);
    while let Some(flag) = it.next() {
        let val = it.next().ok_or(format!("{flag} needs a value"))?;
        match flag.as_str() {
            "--http" => a.http = val.parse()?,
            "--port-base" => a.port_base = val.parse()?,
            "--dir" => a.dir = Some(PathBuf::from(val)),
            _ => return Err(format!("unknown option {flag}").into()),
        }
    }
    Ok(a)
}

#[tokio::main]
async fn main() -> Res<()> {
    let args = args()?;
    let tmp = tempfile::tempdir()?;
    let dir = args.dir.clone().unwrap_or_else(|| tmp.path().to_path_buf());
    std::fs::create_dir_all(&dir)?;
    let bus = Arc::new(Bus {
        tx: broadcast::channel(4096).0,
        dirty: Notify::new(),
        log: Mutex::new(VecDeque::new()),
    });
    let members = start_nodes(&dir, args.port_base, &bus).await?;
    let app = Arc::new(App { members, bus });
    for m in &app.members {
        app.bus.emit(m.id, "role", format!("{:?}", m.node.state()));
    }
    // Role changes of every node (master ↔ slave, fenced, switching, stopped ...).
    for (i, m) in app.members.iter().enumerate() {
        let mut states = m.node.watch_state();
        let app = app.clone();
        tokio::spawn(async move {
            let id = app.members[i].id;
            let mut last = String::new();
            while states.changed().await.is_ok() {
                let s = states.borrow_and_update().clone();
                // Slave sub-states come from the listener already; report role changes here.
                let role = match &s {
                    NodeState::Slave { upstream, .. } => {
                        format!("slave of {}", app.id_of(upstream).unwrap_or(upstream))
                    }
                    other => format!("{other:?}"),
                };
                if role != last {
                    app.bus.emit(id, "role", role.clone());
                    last = role;
                }
            }
        });
    }
    tokio::spawn(snapshot_pusher(app.clone()));

    let router = Router::new()
        .route("/", get(index))
        .route("/ws", get(ws))
        .route("/api/key", get(read_key))
        .route("/api/scan", get(scan))
        .route("/api/write", post(write))
        .route("/api/action", post(action))
        .with_state(app.clone());
    let listener = tokio::net::TcpListener::bind(args.http).await?;
    println!("data in {}", dir.display());
    println!(
        "nodes A..D replicate on 127.0.0.1:{}..{} (A → B → C → D)",
        args.port_base,
        args.port_base + 3
    );
    println!("open http://{}", listener.local_addr()?);
    axum::serve(listener, router)
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    for m in &app.members {
        let _ = m.node.shutdown().await;
    }
    Ok(())
}
