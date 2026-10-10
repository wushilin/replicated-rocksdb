//! Integrity verification (DESIGN.md §12, PROTOCOL.md §9): the public types, and the range
//! logic shared by the server (hashes and listings over a pinned snapshot) and the slave
//! verifier (contiguous ranges, bisection, listing and key-by-key diff).

pub(crate) mod compare;
pub(crate) mod range;
pub(crate) mod scan;

use std::time::Duration;

use crate::error::{Error, Result};
use crate::pb;
use crate::types::{CfSpec, Seq};

/// Range requests in flight at once on one session (PROTOCOL.md §9.2, `verify_window`).
pub(crate) const VERIFY_WINDOW: usize = 16;
/// The largest `ListRange.limit` a client may ask for, and the one this client always uses.
pub(crate) const MAX_LIST_LIMIT: u32 = 1024;
/// A `ListResult` is cut (`truncated`) once its keys reach about this many bytes, but it
/// always holds at least two keys when the range has two.
pub(crate) const LIST_SOFT_BYTES: usize = 1 << 20;
/// Keys longer than this never become range boundaries, so a range request stays far below
/// the server's 4 MiB request limit.
pub(crate) const MAX_BOUNDARY_KEY_BYTES: usize = 1 << 20;
/// A truncated listing is continued from its last key only if that key is at most this long;
/// otherwise the rest of the range is reported as [`Mismatch::Range`].
pub(crate) const MAX_RESUME_KEY_BYTES: usize = 2 << 20;

/// What a verify run compares per key (PROTOCOL.md §9.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum VerifyMode {
    /// Envelope headers only (`write_seq`, TTL): finds missing, extra and stale keys without
    /// hashing values.
    #[default]
    Quick,
    /// Whole envelopes: also finds corrupted values.
    Full,
}

impl VerifyMode {
    pub(crate) fn to_pb(self) -> pb::VerifyMode {
        match self {
            VerifyMode::Quick => pb::VerifyMode::Quick,
            VerifyMode::Full => pb::VerifyMode::Full,
        }
    }

    pub(crate) fn from_pb(v: i32) -> Option<VerifyMode> {
        match pb::VerifyMode::try_from(v).ok()? {
            pb::VerifyMode::Quick => Some(VerifyMode::Quick),
            pb::VerifyMode::Full => Some(VerifyMode::Full),
            pb::VerifyMode::Unspecified => None,
        }
    }
}

/// What a slave does when a verify run finds a difference.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MismatchPolicy {
    /// Report it (status, listener, logs) and keep replicating.
    #[default]
    Report,
    /// Report it, then discard local data and bootstrap from the upstream.
    Rebootstrap,
}

/// Verification settings of a slave (DESIGN.md §12, §16).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyConfig {
    /// Run a verify this often. `None` (default): only on [`Slave::verify_now`](crate::Slave::verify_now).
    pub interval: Option<Duration>,
    /// Mode of the scheduled runs. Default `Quick`.
    pub mode: VerifyMode,
    /// A range is closed after this many local keys. Default 1 000.
    pub chunk_keys: usize,
    /// … or after this many local bytes (keys plus envelopes). Default 4 MiB.
    pub chunk_bytes: usize,
    /// A mismatched range is listed key by key once it holds at most this many local keys.
    /// Default 16.
    pub min_chunk_keys: usize,
    /// Local read rate limit. `None` (default): unlimited.
    pub max_bytes_per_sec: Option<u64>,
    pub on_mismatch: MismatchPolicy,
    /// How long to wait for the local applier to reach the upstream's snapshot seq. Default 60 s.
    pub capture_timeout: Duration,
    /// At most this many [`Mismatch::Key`] per run; the rest of a range is then reported as
    /// [`Mismatch::Range`]. Default 10 000.
    pub max_reported_keys: usize,
}

impl Default for VerifyConfig {
    fn default() -> Self {
        VerifyConfig {
            interval: None,
            mode: VerifyMode::Quick,
            chunk_keys: 1_000,
            chunk_bytes: 4 << 20,
            min_chunk_keys: 16,
            max_bytes_per_sec: None,
            on_mismatch: MismatchPolicy::Report,
            capture_timeout: Duration::from_secs(60),
            max_reported_keys: 10_000,
        }
    }
}

impl VerifyConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.chunk_keys == 0
            || self.chunk_bytes == 0
            || self.min_chunk_keys == 0
            || self.max_reported_keys == 0
        {
            return Err(Error::Config(
                "verify chunk sizes and max_reported_keys must be positive".into(),
            ));
        }
        if self.max_bytes_per_sec == Some(0) {
            return Err(Error::Config(
                "verify max_bytes_per_sec must be positive (None = unlimited)".into(),
            ));
        }
        if self.interval.is_some_and(|i| i.is_zero()) || self.capture_timeout.is_zero() {
            return Err(Error::Config(
                "verify interval and capture_timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// A user CF as one side of a verify sees it: its spec and the seq that created it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CfVersion {
    pub spec: CfSpec,
    pub create_seq: Seq,
}

/// One difference between a slave and its upstream at the same seq. More kinds may be added,
/// hence `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mismatch {
    /// The CF lists differ: a CF exists on one side only, or with another comparator or
    /// `create_seq`. Its data is compared only if both sides have it with the same comparator.
    CfSpec {
        name: String,
        local: Option<CfVersion>,
        upstream: Option<CfVersion>,
    },
    /// A key differs. `None` means the key is absent on that side; `Some` holds that side's
    /// `write_seq` (equal `write_seq`s mean the values differ).
    Key {
        cf: String,
        key: Vec<u8>,
        local: Option<Seq>,
        upstream: Option<Seq>,
    },
    /// Keys in `[lower, upper)` (`None` = unbounded) were not compared one by one: the
    /// listing could not be continued, or `max_reported_keys` was reached.
    Range {
        cf: String,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    },
}

/// The result of one verify run. Fields may be added, hence `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyReport {
    /// The seq both sides were compared at (the upstream's snapshot).
    pub seq: Seq,
    pub mode: VerifyMode,
    /// CFs whose data was compared.
    pub cfs_checked: usize,
    /// Local keys hashed.
    pub keys_checked: u64,
    pub mismatches: Vec<Mismatch>,
    pub duration: Duration,
}

impl VerifyReport {
    /// True when nothing differs.
    pub fn is_clean(&self) -> bool {
        self.mismatches.is_empty()
    }
}

/// Why a verify run was abandoned without a report.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum VerifyAbort {
    /// The upstream already serves `max_verify_sessions` sessions (`BUSY`). Retry later.
    Busy,
    /// The upstream no longer knows the session (`SESSION_NOT_FOUND`): it was idle longer
    /// than the upstream's `verify_session_timeout`.
    SessionExpired,
    /// The upstream follows another database identity than this slave.
    IdentityChanged,
    /// This slave had already applied entries past the upstream's snapshot (possible when the
    /// upstream is a slave that lost unflushed entries, DESIGN.md §6). Retry later.
    PassedSnapshot { snapshot_seq: Seq, applied_seq: Seq },
    /// This slave did not reach the upstream's snapshot seq within `capture_timeout` (it is
    /// halted, far behind, or started a bootstrap).
    CaptureTimeout { snapshot_seq: Seq },
    /// Any other failure talking to the upstream.
    Upstream(String),
}
