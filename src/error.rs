use crate::types::{Role, Seq};

/// Errors returned by this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("node is not ready: it is bootstrapping")]
    NotReady,
    #[error("no such column family: {0}")]
    NoSuchCf(String),
    #[error("column family already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid column family name {0:?}: must be 1-255 bytes and must not start with \"__\"")]
    InvalidCfName(String),
    #[error("batch is empty")]
    EmptyBatch,
    #[error("invalid range: start must sort strictly before end")]
    InvalidRange,
    #[error("entry too large: {size} bytes exceeds max_entry_bytes {max}")]
    EntryTooLarge { size: u64, max: u64 },
    #[error("unknown comparator: {0}")]
    UnknownComparator(String),
    #[error("log gap: seq {requested} requested, first retained is {first_retained}")]
    Gap { requested: Seq, first_retained: Seq },
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("database was created as {found:?} and cannot be opened as {expected:?}")]
    WrongRole { expected: Role, found: Role },
    #[error("{what} is {found}, this build supports {expected}")]
    VersionMismatch { what: &'static str, found: u64, expected: u64 },
    #[error("the writer is fenced after a failed write; reopen the database")]
    WriterFenced,
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("timed out")]
    Timeout,
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
