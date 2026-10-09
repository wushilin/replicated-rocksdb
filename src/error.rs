use crate::types::{Role, Seq};

/// Errors returned by this crate.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// A slave that is still bootstrapping cannot serve reads yet.
    #[error("node is not ready: it is bootstrapping")]
    NotReady,
    #[error("no such column family: {0}")]
    NoSuchCf(String),
    #[error("column family already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid column family name {0:?}: must be 1-255 bytes and must not start with \"__\"")]
    InvalidCfName(String),
    /// A `Batch` with no operations cannot be committed.
    #[error("batch is empty")]
    EmptyBatch,
    /// `delete_range` needs `start` to sort strictly before `end` under the CF comparator.
    #[error("invalid range: start must sort strictly before end")]
    InvalidRange,
    /// The encoded log entry exceeds `max_entry_bytes`; split the batch.
    #[error("entry too large: {size} bytes exceeds max_entry_bytes {max}")]
    EntryTooLarge { size: u64, max: u64 },
    /// The CF names a comparator that is not in the `ComparatorRegistry`.
    #[error("unknown comparator: {0}")]
    UnknownComparator(String),
    /// For a hole in the middle of the log, `first_retained` is the next seq actually present.
    #[error("log gap: seq {requested} requested, first retained is {first_retained}")]
    Gap { requested: Seq, first_retained: Seq },
    /// Stored or received data failed a structural or integrity check.
    #[error("corrupt data: {0}")]
    Corrupt(String),
    /// The directory was created by a master and opened as a slave, or vice versa.
    #[error("database was created as {found:?} and cannot be opened as {expected:?}")]
    WrongRole { expected: Role, found: Role },
    /// A protocol, envelope or storage version differs from this build.
    #[error("{what} is {found}, this build supports {expected}")]
    VersionMismatch { what: &'static str, found: u64, expected: u64 },
    /// A write failed with unknown durability, so further writes are refused to avoid reusing seqs.
    #[error("the writer is fenced after a failed write; reopen the database")]
    WriterFenced,
    #[error("invalid configuration: {0}")]
    Config(String),
    /// An operation did not finish within its deadline.
    #[error("timed out")]
    Timeout,
    /// A bootstrap or a replicated `CreateCf` needs a physical column family that a reader's
    /// snapshot still references with a different spec. Nothing was written. Retriable: the
    /// slave driver retries with backoff and shows it in `SlaveStatus::last_error`.
    #[error("column family {physical} is still referenced by a snapshot; release it to continue")]
    CfInUse { physical: String },
    /// The slave stopped advancing; see `SlaveStatus` and `Slave::clear_halt`.
    #[error("replication is halted: {0:?}")]
    Halted(crate::slave::HaltReason),
    /// `promote_checked` found a reachable peer that holds more of the log. `diverged` lists
    /// the peers proven to hold different entries (`node_id`, fork), for the operator.
    #[error("{leader} is more advanced (last_seq {seq}); promote it instead (diverged peers: {diverged:?})")]
    NotMostAdvanced { leader: String, seq: Seq, diverged: Vec<(String, Seq)> },
    /// `promote_checked` found a reachable peer that is at least as far by position (newest
    /// epoch, then `last_seq`) but whose compatibility cannot be proven, because the entry that
    /// would prove it is no longer held. Plain `promote` remains available as an explicit
    /// operator decision. `diverged` as for `NotMostAdvanced`.
    #[error("{peer} (last_seq {seq}) is at least as far and its history cannot be verified; promote explicitly if intended (diverged peers: {diverged:?})")]
    PromotionUnverifiable { peer: String, seq: Seq, diverged: Vec<(String, Seq)> },
    /// `promote_checked` found a reachable peer that is itself a master. Promoting would give
    /// the database two masters: fence it (stop it), or `demote` it if it was promoted by
    /// mistake and has taken no writes, then check again.
    #[error("{peer} is a reachable master; fence it, or demote it if it was promoted by mistake and has no writes, before promoting")]
    MasterReachable { peer: String },
    #[error(transparent)]
    RocksDb(#[from] rocksdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn corrupt(msg: impl Into<String>) -> Self {
        Error::Corrupt(msg.into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
