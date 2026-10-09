//! Multi-node test harness: real nodes in one process, talking gRPC over 127.0.0.1.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use replicated_rocksdb::{
    AllowAll, Backoff, CfSpec, IterMode, LogApi, LogEntry, ManualClock, Master, MasterConfig,
    ReadApi, ReplicationListener, Seq, ServeConfig, Slave, SlaveConfig, SlaveState, UpstreamConfig,
    ValueMeta,
};

pub const T0: u64 = 1_700_000_000_000;
pub const HEARTBEAT: Duration = Duration::from_millis(500);
pub const WAIT: Duration = Duration::from_secs(30);

pub fn localhost() -> IpAddr {
    IpAddr::from([127, 0, 0, 1])
}

/// Nodes of one test: a shared manual clock and a temp directory per node name.
pub struct Cluster {
    pub clock: Arc<ManualClock>,
    dir: tempfile::TempDir,
}

impl Cluster {
    pub fn new() -> Self {
        Cluster {
            clock: Arc::new(ManualClock::new(T0)),
            dir: tempfile::tempdir().unwrap(),
        }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// Plaintext, `AllowAll`, port 0.
    pub fn serve(&self) -> ServeConfig {
        ServeConfig::new(localhost(), 0, Arc::new(AllowAll))
    }

    /// Serving master with a manual clock and background maintenance effectively off.
    pub fn master_config(&self, name: &str) -> MasterConfig {
        let mut cfg = MasterConfig::new(name, self.path(name));
        cfg.clock = self.clock.clone();
        cfg.heartbeat = HEARTBEAT;
        cfg.retention.check_interval = Duration::from_secs(3600);
        cfg.reaper.interval = Duration::from_secs(3600);
        cfg.serve = Some(self.serve());
        cfg
    }

    pub async fn master(&self, name: &str) -> Master {
        Master::open(self.master_config(name)).await.unwrap()
    }

    /// Reopens a master on the port it served before, so its slaves find it again.
    pub async fn restart_master(&self, m: Master, cfg: MasterConfig) -> Master {
        let port = m.serve_addr().unwrap().port();
        m.shutdown().await.unwrap();
        let mut cfg = cfg;
        cfg.serve.as_mut().unwrap().port = port;
        Master::open(cfg).await.unwrap()
    }

    /// Stops `m`, copies its directory to `backup` (a consistent backup of a stopped node),
    /// and reopens it on the same port.
    pub async fn restart_master_after_backup(&self, m: Master, name: &str, backup: &str) -> Master {
        let port = m.serve_addr().unwrap().port();
        m.shutdown().await.unwrap();
        copy_dir(&self.path(name), &self.path(backup));
        let mut cfg = self.master_config(name);
        cfg.serve.as_mut().unwrap().port = port;
        Master::open(cfg).await.unwrap()
    }

    /// A serving slave with fast reconnects and a recording listener.
    pub fn slave_config(
        &self,
        name: &str,
        upstream: SocketAddr,
    ) -> (SlaveConfig, Arc<RecordingListener>) {
        let listener = Arc::new(RecordingListener::default());
        let mut cfg = SlaveConfig::new(name, self.path(name), UpstreamConfig::from_addr(upstream));
        cfg.clock = self.clock.clone();
        cfg.heartbeat = HEARTBEAT;
        cfg.retention.check_interval = Duration::from_secs(3600);
        cfg.reconnect = Backoff {
            initial: Duration::from_millis(20),
            max: Duration::from_millis(200),
            factor: 2,
        };
        cfg.serve = Some(self.serve());
        cfg.listener = Some(listener.clone());
        (cfg, listener)
    }

    pub async fn slave(&self, name: &str, upstream: SocketAddr) -> (Slave, Arc<RecordingListener>) {
        let (cfg, listener) = self.slave_config(name, upstream);
        (Slave::open(cfg).await.unwrap(), listener)
    }

    /// Reopens a slave on the port it served before, so its downstream finds it again.
    pub async fn restart_slave(&self, s: Slave, cfg: SlaveConfig) -> Slave {
        let port = s.serve_addr().map(|a| a.port());
        s.shutdown().await.unwrap();
        let mut cfg = cfg;
        if let (Some(port), Some(serve)) = (port, cfg.serve.as_mut()) {
            serve.port = port;
        }
        Slave::open(cfg).await.unwrap()
    }
}

/// Polls `f` every 20 ms; panics after 30 s.
pub async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {what}"
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn wait_state(s: &Slave, what: &str, f: impl Fn(&SlaveState) -> bool) {
    wait_until(what, || f(&s.status().state)).await;
}

/// Read and log access, as master and slaves both offer.
pub trait Node: ReadApi + LogApi {}
impl<T: ReadApi + LogApi> Node for T {}

/// Every live CF with every visible `(key, value, meta)`, read through the public API.
pub type Dump = BTreeMap<String, Vec<(Vec<u8>, Vec<u8>, ValueMeta)>>;

pub fn dump(n: &dyn Node) -> Dump {
    let mut out = Dump::new();
    for spec in n.list_cfs() {
        let rows = n
            .iter(&spec.name, IterMode::Start)
            .unwrap()
            .map(|r| {
                let (k, _) = r.unwrap();
                let (v, meta) = n
                    .get_with_meta(&spec.name, &k)
                    .unwrap()
                    .expect("iterated key is readable");
                (k, v, meta)
            })
            .collect();
        out.insert(spec.name, rows);
    }
    out
}

/// Exact equality, with a short message instead of megabytes of Debug output.
pub fn assert_same(what: &str, a: &Dump, b: &Dump) {
    assert_eq!(
        a.keys().collect::<Vec<_>>(),
        b.keys().collect::<Vec<_>>(),
        "{what}: CF lists differ"
    );
    for (cf, rows) in a {
        let other = &b[cf];
        assert_eq!(
            rows.len(),
            other.len(),
            "{what}: CF {cf} has {} vs {} keys",
            rows.len(),
            other.len()
        );
        for (x, y) in rows.iter().zip(other) {
            assert!(
                x == y,
                "{what}: CF {cf} differs at a key of {} bytes (meta {:?} vs {:?})",
                x.0.len(),
                x.2,
                y.2
            );
        }
    }
}

/// Waits until every slave has applied the master's `last_seq`, then compares dumps exactly.
pub async fn assert_converged(master: &dyn Node, slaves: &[(&str, &Slave)]) {
    let seq = master.last_seq();
    let expected = dump(master);
    for (name, s) in slaves {
        s.wait_for_seq(seq, WAIT)
            .await
            .unwrap_or_else(|e| panic!("{name} did not reach {seq}: {e}"));
        assert_eq!(s.last_seq(), seq, "{name} is past the master");
        assert_same(name, &expected, &dump(*s));
    }
}

/// Zero-length and NUL-byte keys and values, a 1 MiB key, a value close to the 16 MiB entry limit.
pub fn edge_cases() -> Vec<(Vec<u8>, Vec<u8>)> {
    vec![
        (b"".to_vec(), b"".to_vec()),
        (b"\0".to_vec(), b"\0".to_vec()),
        (b"\0k\0".to_vec(), b"\0\0\0".to_vec()),
        (vec![0; 64], vec![0; 64]),
        (vec![0x5A; 1 << 20], b"".to_vec()),
        (b"big".to_vec(), vec![0xC3; (16 << 20) - 4096]),
    ]
}

/// Creates `cf` and writes every edge case, one entry each.
pub fn write_edge_cases(m: &Master, cf: &str) {
    m.create_cf(CfSpec::new(cf)).unwrap();
    for (k, v) in edge_cases() {
        m.put(cf, &k, &v).unwrap();
    }
}

/// Checks the edge cases through point reads on `n`.
pub fn check_edge_cases(n: &dyn Node, cf: &str) {
    for (k, v) in edge_cases() {
        let got = n
            .get(cf, &k)
            .unwrap()
            .unwrap_or_else(|| panic!("key of {} bytes missing", k.len()));
        assert!(got == v, "value of key with {} bytes differs", k.len());
    }
    assert_eq!(
        n.get(cf, b"").unwrap(),
        Some(vec![]),
        "an empty value is a value"
    );
    assert_eq!(
        n.get(cf, b"\0\0").unwrap(),
        None,
        "b\"\\0\\0\" was never written"
    );
}

/// Copies a closed node directory (a consistent backup of a stopped node).
pub fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Replaces `dir` with a copy of `backup`.
pub fn restore_dir(backup: &Path, dir: &Path) {
    std::fs::remove_dir_all(dir).unwrap();
    copy_dir(backup, dir);
}

/// Records listener calls. Violations are collected, not panicked (a panic would disable
/// the listener): within a process, entry seqs must be strictly increasing between bootstraps.
#[derive(Default)]
pub struct RecordingListener {
    pub entries: Mutex<Vec<Seq>>,
    pub bootstraps_begun: AtomicUsize,
    pub bootstraps: AtomicUsize,
    pub states: Mutex<Vec<SlaveState>>,
    pub violations: Mutex<Vec<String>>,
    last: AtomicU64,
}

impl RecordingListener {
    pub fn bootstraps(&self) -> usize {
        self.bootstraps.load(Ordering::SeqCst)
    }

    pub fn entries(&self) -> Vec<Seq> {
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn states(&self) -> Vec<SlaveState> {
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub fn assert_no_violations(&self) {
        let v = self
            .violations
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        assert!(v.is_empty(), "listener violations: {v:?}");
    }
}

impl ReplicationListener for RecordingListener {
    fn on_entry(&self, entry: &LogEntry) {
        let last = self.last.swap(entry.seq, Ordering::SeqCst);
        if entry.seq <= last {
            self.violations
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .push(format!("entry {} after {last}", entry.seq));
        }
        self.entries
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(entry.seq);
    }

    fn on_bootstrap_begin(&self, _: Seq) {
        self.bootstraps_begun.fetch_add(1, Ordering::SeqCst);
    }

    fn on_bootstrap_complete(&self, snapshot_seq: Seq) {
        self.last.store(snapshot_seq, Ordering::SeqCst);
        self.bootstraps.fetch_add(1, Ordering::SeqCst);
    }

    fn on_state_change(&self, state: &SlaveState) {
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(state.clone());
    }
}
