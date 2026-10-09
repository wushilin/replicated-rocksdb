//! Core value types shared by the whole crate.

use std::time::Duration;

use bytes::Bytes;

/// Sequence number of a log entry. Assigned by the master; 0 means "nothing applied".
pub type Seq = u64;

/// Identity of the origin master. The same on every node of a topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DbIdentity {
    pub db_uuid: uuid::Uuid,
    pub epoch: u64,
}

/// Role persisted in `__meta`. A directory created for one role cannot be opened as the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Master,
    Slave,
}

#[allow(dead_code)]
impl Role {
    pub(crate) fn as_bytes(self) -> &'static [u8] {
        match self {
            Role::Master => b"master",
            Role::Slave => b"slave",
        }
    }

    pub(crate) fn from_bytes(b: &[u8]) -> Option<Role> {
        match b {
            b"master" => Some(Role::Master),
            b"slave" => Some(Role::Slave),
            _ => None,
        }
    }
}

/// A user column family as the application sees it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CfSpec {
    pub name: String,
    /// Name of a comparator registered on every node; `None` means bytewise order.
    pub comparator: Option<String>,
}

impl CfSpec {
    pub fn new(name: impl Into<String>) -> Self {
        CfSpec { name: name.into(), comparator: None }
    }

    pub fn with_comparator(mut self, comparator: impl Into<String>) -> Self {
        self.comparator = Some(comparator.into());
        self
    }
}

/// A registered user CF: its spec plus the seq of the `CreateCf` entry that created it.
#[derive(Clone, Debug, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct CfEntry {
    pub spec: CfSpec,
    pub create_seq: Seq,
}

#[allow(dead_code)]
impl CfEntry {
    /// Physical RocksDB CF name, `u:{name}:{create_seq}`. Never parsed back.
    pub fn physical_name(&self) -> String {
        format!("u:{}:{}", self.spec.name, self.create_seq)
    }
}

/// One absolute operation. Applying it never depends on prior state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put { cf: String, key: Vec<u8>, value: Vec<u8>, expire_at_ms: Option<u64> },
    Delete { cf: String, key: Vec<u8> },
    /// Removes keys in `[start, end)` under the CF's comparator.
    DeleteRange { cf: String, start: Vec<u8>, end: Vec<u8> },
    CreateCf { spec: CfSpec },
    DropCf { name: String },
}

impl Op {
    /// The logical CF this op touches, or creates or drops.
    pub fn cf_name(&self) -> &str {
        match self {
            Op::Put { cf, .. } | Op::Delete { cf, .. } | Op::DeleteRange { cf, .. } => cf,
            Op::CreateCf { spec } => &spec.name,
            Op::DropCf { name } => name,
        }
    }
}

/// One replicated log entry. `ops` holds the encoded `OpList` exactly as the master produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub seq: Seq,
    pub epoch: u64,
    /// Master commit time in ms since the Unix epoch; non-decreasing in `seq`.
    pub ts_ms: u64,
    pub ops: Bytes,
    pub crc32c: u32,
}

/// Metadata stored in a value's envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueMeta {
    /// Seq of the entry that wrote the value.
    pub write_seq: Seq,
    /// Absolute expiry in ms since the Unix epoch, if the value was written with a TTL.
    pub expire_at_ms: Option<u64>,
}

/// Start of one epoch in the epoch history (PROTOCOL.md §5.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochStart {
    pub epoch: u64,
    /// Seq of the first entry written in this epoch.
    pub start_seq: Seq,
}

/// The epoch an entry with this seq must carry, given a history sorted by epoch.
#[allow(dead_code)]
pub(crate) fn epoch_for(history: &[EpochStart], seq: Seq) -> Option<u64> {
    history.iter().rev().find(|e| e.start_seq <= seq).map(|e| e.epoch)
}

/// The fork point after `epoch`: the first seq of the next epoch, if any.
#[allow(dead_code)]
pub(crate) fn fork_after(history: &[EpochStart], epoch: u64) -> Option<Seq> {
    history.iter().find(|e| e.epoch > epoch).map(|e| e.start_seq)
}

#[derive(Clone, Debug)]
#[allow(dead_code)]
pub(crate) enum BatchOp {
    Put { cf: String, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration> },
    Delete { cf: String, key: Vec<u8> },
    DeleteRange { cf: String, start: Vec<u8>, end: Vec<u8> },
}

/// Several data operations committed as one log entry. May span CFs; must not be empty.
#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub(crate) ops: Vec<BatchOp>,
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Put { cf: cf.into(), key: key.into(), value: value.into(), ttl: None });
        self
    }

    pub fn put_with_ttl(&mut self, cf: &str, key: &[u8], value: &[u8], ttl: Duration) -> &mut Self {
        self.ops.push(BatchOp::Put { cf: cf.into(), key: key.into(), value: value.into(), ttl: Some(ttl) });
        self
    }

    pub fn delete(&mut self, cf: &str, key: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Delete { cf: cf.into(), key: key.into() });
        self
    }

    pub fn delete_range(&mut self, cf: &str, start: &[u8], end: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::DeleteRange { cf: cf.into(), start: start.into(), end: end.into() });
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Converts to absolute ops: each TTL becomes `expire_at_ms = ts_ms + ttl` (saturating).
    #[allow(dead_code)]
    pub(crate) fn into_ops(self, ts_ms: u64) -> Vec<Op> {
        self.ops
            .into_iter()
            .map(|op| match op {
                BatchOp::Put { cf, key, value, ttl } => Op::Put {
                    cf,
                    key,
                    value,
                    expire_at_ms: ttl.map(|t| expire_at(ts_ms, t)),
                },
                BatchOp::Delete { cf, key } => Op::Delete { cf, key },
                BatchOp::DeleteRange { cf, start, end } => Op::DeleteRange { cf, start, end },
            })
            .collect()
    }
}

/// `ts_ms + ttl` in ms, saturating at `u64::MAX`.
#[allow(dead_code)]
pub(crate) fn expire_at(ts_ms: u64, ttl: Duration) -> u64 {
    let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
    ts_ms.saturating_add(ttl_ms)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Reverse,
}

/// Where an iterator starts.
#[derive(Clone, Copy, Debug)]
pub enum IterMode<'a> {
    Start,
    End,
    From(&'a [u8], Direction),
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn physical_name_uses_create_seq() {
        let e = CfEntry { spec: CfSpec::new("a:b"), create_seq: 42 };
        assert_eq!(e.physical_name(), "u:a:b:42");
    }

    #[test]
    fn batch_resolves_ttl_against_commit_time() {
        let mut b = Batch::new();
        b.put("c", b"k1", b"v1")
            .put_with_ttl("c", b"k2", b"v2", Duration::from_secs(5))
            .put_with_ttl("c", b"k3", b"v3", Duration::MAX)
            .delete("c", b"k4")
            .delete_range("c", b"a", b"b");
        assert_eq!(b.len(), 5);
        let ops = b.into_ops(1_000);
        assert_eq!(
            ops[0],
            Op::Put { cf: "c".into(), key: b"k1".to_vec(), value: b"v1".to_vec(), expire_at_ms: None }
        );
        assert_eq!(
            ops[1],
            Op::Put { cf: "c".into(), key: b"k2".to_vec(), value: b"v2".to_vec(), expire_at_ms: Some(6_000) }
        );
        assert_eq!(
            ops[2],
            Op::Put { cf: "c".into(), key: b"k3".to_vec(), value: b"v3".to_vec(), expire_at_ms: Some(u64::MAX) }
        );
        assert_eq!(ops[3], Op::Delete { cf: "c".into(), key: b"k4".to_vec() });
        assert_eq!(
            ops[4],
            Op::DeleteRange { cf: "c".into(), start: b"a".to_vec(), end: b"b".to_vec() }
        );
    }

    #[test]
    fn epoch_history_lookups() {
        let h = [
            EpochStart { epoch: 1, start_seq: 1 },
            EpochStart { epoch: 2, start_seq: 701 },
            EpochStart { epoch: 4, start_seq: 900 },
        ];
        assert_eq!(epoch_for(&h, 0), None);
        assert_eq!(epoch_for(&h, 1), Some(1));
        assert_eq!(epoch_for(&h, 700), Some(1));
        assert_eq!(epoch_for(&h, 701), Some(2));
        assert_eq!(epoch_for(&h, 10_000), Some(4));
        assert_eq!(fork_after(&h, 1), Some(701));
        assert_eq!(fork_after(&h, 2), Some(900));
        assert_eq!(fork_after(&h, 3), Some(900));
        assert_eq!(fork_after(&h, 4), None);
    }

    #[test]
    fn role_round_trips() {
        for r in [Role::Master, Role::Slave] {
            assert_eq!(Role::from_bytes(r.as_bytes()), Some(r));
        }
        assert_eq!(Role::from_bytes(b"other"), None);
    }
}
