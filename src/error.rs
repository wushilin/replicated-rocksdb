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
    VersionMismatch {
        what: &'static str,
        found: u64,
        expected: u64,
    },
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
    /// Fewer replicas are in sync than the write concern requires; nothing was written.
    #[error(
        "{in_sync} replicas in sync, the write concern requires {required}; nothing was written"
    )]
    NotEnoughReplicas { required: usize, in_sync: usize },
    /// The write is committed on the master (and keeps replicating), but `required` in-sync
    /// acknowledgements were not seen within the write concern's `ack_timeout`. Retrying the
    /// write would apply it a second time.
    #[error(
        "entry {seq} is committed but only {acked} of {required} in-sync replicas acknowledged it in time"
    )]
    AckTimeout {
        seq: Seq,
        acked: usize,
        required: usize,
    },
    /// `promote_checked` found a reachable peer that holds more of the log. `diverged` lists
    /// the peers proven to hold different entries (`node_id`, fork), for the operator.
    #[error(
        "{leader} is more advanced (last_seq {seq}); promote it instead (diverged peers: {diverged:?})"
    )]
    NotMostAdvanced {
        leader: String,
        seq: Seq,
        diverged: Vec<(String, Seq)>,
    },
    /// `promote_checked` reached a peer that answered `Status` with an error (permission
    /// denied, throttled, another protocol, ...): it is alive and its state is unknown, so it
    /// may be ahead. `peer` holds its address and the error, `seq` is 0. Plain `promote`
    /// remains available as an explicit operator decision. `diverged` as for
    /// `NotMostAdvanced`.
    #[error(
        "{peer} answered Status with an error, so its state is unknown; promote explicitly if intended (diverged peers: {diverged:?})"
    )]
    PromotionUnverifiable {
        peer: String,
        seq: Seq,
        diverged: Vec<(String, Seq)>,
    },
    /// The local log holds entries the upstream does not: `divergent_entries` entries from
    /// `fork_seq` to `local_last_seq` (for a mismatch at the same epoch, `fork_seq` is the
    /// last entry; earlier ones may differ too). Local data is intact.
    #[error(
        "local log diverged from the upstream at seq {fork_seq} ({divergent_entries} entries up to {local_last_seq})"
    )]
    Diverged {
        fork_seq: Seq,
        local_last_seq: Seq,
        divergent_entries: u64,
    },
    /// A `Slave::repoint` was replaced by a later repoint, or the slave shut down, before the
    /// first handshake with its upstream settled. The later repoint (if any) is in force.
    #[error("the repoint was superseded by a later repoint or the slave shut down")]
    RepointSuperseded,
    /// The operation is not available in this version of the crate.
    #[error("not supported by this version: {0}")]
    Unsupported(String),
    /// `promote_checked` found a reachable peer that is itself a master. Promoting would give
    /// the database two masters: fence it (stop it), or `demote` it if it was promoted by
    /// mistake and has taken no writes, then check again.
    #[error(
        "{peer} is a reachable master; fence it, or demote it if it was promoted by mistake and has no writes, before promoting"
    )]
    MasterReachable { peer: String },
    /// `promote_checked` found a reachable peer whose history forks from this slave's at
    /// `fork` while the peer holds entries at or past it (up to `peer_last_seq`): promoting this
    /// slave would discard them. Promote that peer instead, or use plain `promote` to accept
    /// the loss.
    #[error(
        "{peer} holds entries {fork}..={peer_last_seq} that promoting this slave would discard; promote that node instead, or use plain promote to accept the loss"
    )]
    PromotionWouldDiscard {
        peer: String,
        fork: Seq,
        peer_last_seq: Seq,
    },
    /// `admin::node_status` (and `node_status_probed`) could not get an answer from `peer`:
    /// connect error, timeout, or a transport failure. `reason` is one short line (the root
    /// cause); the whole chain is available through `source` (and `{:?}`). A peer that answered
    /// with an error status is reported as `Config` with the status text instead.
    #[error("{peer} is unreachable: {reason}")]
    PeerUnreachable {
        peer: String,
        reason: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    /// A verify run was abandoned before it produced a report.
    #[error("verify abandoned: {0:?}")]
    VerifyAborted(crate::verify::VerifyAbort),
    #[error(transparent)]
    RocksDb(#[from] rocksdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn corrupt(msg: impl Into<String>) -> Self {
        Error::Corrupt(msg.into())
    }

    /// A storage I/O failure that may pass (disk full, a transient device error, RocksDB
    /// busy or timed out), as opposed to an integrity failure (corruption, invalid input).
    pub(crate) fn is_transient_io(&self) -> bool {
        use rocksdb::ErrorKind as K;
        match self {
            Error::Io(_) => true,
            Error::RocksDb(e) => {
                matches!(e.kind(), K::IOError | K::Busy | K::TimedOut | K::TryAgain)
            }
            _ => false,
        }
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_storage_io_failures_are_transient() {
        assert!(Error::Io(std::io::Error::other("disk full")).is_transient_io());
        assert!(!Error::corrupt("bad crc").is_transient_io());
        assert!(!Error::NoSuchCf("c".into()).is_transient_io());
        assert!(
            !Error::CfInUse {
                physical: "u:c:1".into()
            }
            .is_transient_io()
        );
    }
}
