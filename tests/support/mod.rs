//! Multi-node test harness: real nodes in one process, talking gRPC over 127.0.0.1.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU32, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use replicated_rocksdb::{
    AllowAll, Backoff, CfSpec, IterMode, LogApi, LogEntry, ManualClock, Master, MasterConfig,
    ReadApi, ReplicationListener, Seq, ServeConfig, Slave, SlaveConfig, SlaveState, UpstreamConfig,
    ValueMeta, VerifyReport,
};

pub const T0: u64 = 1_700_000_000_000;
pub const HEARTBEAT: Duration = Duration::from_millis(500);
pub const WAIT: Duration = Duration::from_secs(30);

/// A port nobody else will take, for tests that need the number before the listener exists
/// (repointing a slave at a master that is started later, "unreachable" upstreams, restarts
/// on a known address).
///
/// Binding port 0 and dropping the listener is a race: the kernel hands such ports out again
/// as ephemeral source ports of outgoing connections (range 32768-60999 here) or to another
/// test binding port 0. This allocator therefore stays in 20000-31999, below the ephemeral
/// range, where the kernel never assigns ports on its own: a port stays free until we bind
/// it ourselves. The sequence starts at an offset derived from the process id so concurrent
/// test binaries do not collide, advances with an atomic counter, and each candidate is
/// probed by binding (and dropping) a listener, skipping it on failure.
pub fn reserved_port() -> u16 {
    const BASE: u32 = 20_000;
    const SPAN: u32 = 12_000; // 20000..=31999
    static NEXT: AtomicU32 = AtomicU32::new(0);
    let start = std::process::id().wrapping_mul(97) % SPAN;
    for _ in 0..SPAN {
        let n = NEXT.fetch_add(1, Ordering::Relaxed);
        let port = (BASE + (start + n) % SPAN) as u16;
        if std::net::TcpListener::bind((std::net::Ipv4Addr::LOCALHOST, port)).is_ok() {
            return port;
        }
    }
    panic!("no free port in 20000-31999");
}

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

    /// Plaintext, `AllowAll`, on a [`reserved_port`]. Not port 0: tests restart nodes on their
    /// old address (`restart_master`, `restart_slave`, a restored master on the lost master's
    /// port), and an ephemeral port released by a shut-down server can be taken as the source
    /// port of another connection before it is bound again (AddrInUse).
    pub fn serve(&self) -> ServeConfig {
        ServeConfig::new(localhost(), reserved_port(), Arc::new(AllowAll))
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

    /// Deletes `key` of user CF `cf` straight in the directory of the stopped node `name`
    /// (plain RocksDB), so the node no longer matches its upstream.
    pub fn delete_behind_its_back(&self, name: &str, cf: &str, key: &[u8]) {
        let path = self.path(name);
        let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), &path).unwrap();
        let mut o = rocksdb::Options::default();
        o.create_if_missing(false);
        let db = rocksdb::DB::open_cf(&o, &path, &names).unwrap();
        let handle = db
            .cf_handle(names.iter().find(|n| logical_cf(n) == Some(cf)).unwrap())
            .unwrap();
        db.delete_cf(&handle, key).unwrap();
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

/// A (master) → B (slave), in sync. With `a_ahead`, B stops first and A takes two entries B
/// never sees. A stops; B reopens and is promoted (plain `promote`: A is down). Returns A's
/// master config, B as the new master, and the first seq B does not share with A.
pub async fn failover(c: &Cluster, a_ahead: bool) -> (MasterConfig, Master, Seq) {
    let a_cfg = c.master_config("a");
    let a = Master::open(a_cfg.clone()).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    b.shutdown().await.unwrap();
    if a_ahead {
        a.put("c", b"only on a", b"2").unwrap();
        a.put("c", b"also only on a", b"3").unwrap();
    }
    a.shutdown().await.unwrap();
    let b = Slave::open(b_cfg).await.unwrap();
    let fork = b.last_seq() + 1;
    let b = b.promote(c.master_config("b")).await.unwrap();
    (a_cfg, b, fork)
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
    pub verifies: Mutex<Vec<VerifyReport>>,
    pub rewinds: AtomicUsize,
    pub violations: Mutex<Vec<String>>,
    last: AtomicU64,
}

impl RecordingListener {
    pub fn bootstraps(&self) -> usize {
        self.bootstraps.load(Ordering::SeqCst)
    }

    pub fn rewinds(&self) -> usize {
        self.rewinds.load(Ordering::SeqCst)
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

    pub fn verifies(&self) -> Vec<VerifyReport> {
        self.verifies
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

    fn on_verify_complete(&self, report: &VerifyReport) {
        self.verifies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(report.clone());
    }

    fn on_rewind(&self, last_seq: Seq) {
        self.last.store(last_seq, Ordering::SeqCst);
        self.rewinds.fetch_add(1, Ordering::SeqCst);
    }
}

/// A **stopped** node's directory opened with plain RocksDB, bypassing the crate, to inject
/// damage replication itself never causes. `comparators` lists `(logical CF, comparator
/// name, ordering)` for each CF created with a custom comparator (RocksDB refuses to open a
/// CF with another comparator name).
pub struct RawDb {
    db: rocksdb::DB,
    names: Vec<String>,
}

/// The logical name of a physical user CF `u:{name}:{create_seq}`, split at the last `:` (a
/// logical name may itself contain `:`); `None` for any other CF.
pub fn logical_cf(physical: &str) -> Option<&str> {
    let rest = physical.strip_prefix("u:")?;
    let (name, seq) = rest.rsplit_once(':')?;
    seq.parse::<u64>().ok().map(|_| name)
}

/// A key ordering for [`RawDb::open_with`].
pub type KeyCmp = fn(&[u8], &[u8]) -> std::cmp::Ordering;

impl RawDb {
    pub fn open(path: &Path) -> RawDb {
        Self::open_with(path, &[])
    }

    pub fn open_with(path: &Path, comparators: &[(&str, &str, KeyCmp)]) -> RawDb {
        let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), path).unwrap();
        let descriptors: Vec<rocksdb::ColumnFamilyDescriptor> = names
            .iter()
            .map(|n| {
                let mut opts = rocksdb::Options::default();
                for &(cf, name, f) in comparators {
                    if logical_cf(n) == Some(cf) {
                        opts.set_comparator(name, Box::new(f));
                    }
                }
                rocksdb::ColumnFamilyDescriptor::new(n, opts)
            })
            .collect();
        let db = rocksdb::DB::open_cf_descriptors(&rocksdb::Options::default(), path, descriptors)
            .unwrap();
        RawDb { db, names }
    }

    /// The physical CF of logical CF `name` (`u:{name}:{create_seq}`); there must be one.
    fn cf(&self, name: &str) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        let mut found = self.names.iter().filter(|n| logical_cf(n) == Some(name));
        let physical = found
            .next()
            .unwrap_or_else(|| panic!("no physical CF for {name}"));
        assert!(found.next().is_none(), "{name} has several physical CFs");
        self.db.cf_handle(physical).unwrap()
    }

    /// A `__meta` value as stored.
    pub fn meta(&self, key: &[u8]) -> Option<Vec<u8>> {
        let meta = self.db.cf_handle("__meta").unwrap();
        self.db.get_cf(&meta, key).unwrap()
    }

    /// `__meta.last_seq` as stored: the node's durable position.
    pub fn last_seq(&self) -> u64 {
        let meta = self.db.cf_handle("__meta").unwrap();
        let raw = self
            .db
            .get_cf(&meta, b"last_seq")
            .unwrap()
            .unwrap_or_default();
        raw.try_into().map(u64::from_be_bytes).unwrap_or(0)
    }

    /// The stored envelope.
    pub fn get(&self, cf: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get_cf(&self.cf(cf), key).unwrap()
    }

    pub fn put(&self, cf: &str, key: &[u8], envelope: &[u8]) {
        self.db.put_cf(&self.cf(cf), key, envelope).unwrap();
    }

    pub fn delete(&self, cf: &str, key: &[u8]) {
        self.db.delete_cf(&self.cf(cf), key).unwrap();
    }

    /// Flips the last byte of a stored value, keeping its envelope header (and `write_seq`):
    /// the damage only a `Full` verify can see.
    pub fn corrupt_value(&self, cf: &str, key: &[u8]) {
        let mut env = self.get(cf, key).expect("the key to corrupt exists");
        let header = replicated_rocksdb::codec::envelope::decode(&env)
            .unwrap()
            .header_len;
        assert!(env.len() > header, "the value must be non-empty");
        let last = env.len() - 1;
        env[last] ^= 0xFF;
        self.put(cf, key, &env);
    }
}

#[test]
fn physical_cf_names_are_matched_exactly() {
    assert_eq!(logical_cf("u:a:3"), Some("a"));
    assert_eq!(
        logical_cf("u:a:b:12"),
        Some("a:b"),
        "split at the last colon"
    );
    assert_eq!(
        logical_cf("u:a:b:12").filter(|n| *n == "a"),
        None,
        "a is not a:b"
    );
    assert_eq!(logical_cf("__meta"), None);
    assert_eq!(logical_cf("u:a:x"), None);
}
