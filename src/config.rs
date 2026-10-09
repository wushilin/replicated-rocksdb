//! Configuration shared by master and slave.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::clock::{Clock, SystemClock};
use crate::error::{Error, Result};
use crate::tuning::RocksTuning;

/// Changelog retention. Runs on every node; age is measured from the master's `ts_ms`.
#[derive(Clone, Debug)]
pub struct Retention {
    pub max_age: Option<Duration>,
    pub max_entries: Option<u64>,
    pub check_interval: Duration,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            max_age: Some(Duration::from_secs(14 * 24 * 3600)),
            max_entries: None,
            check_interval: Duration::from_secs(600),
        }
    }
}

/// TTL reaper (master only).
#[derive(Clone, Debug)]
pub struct ReaperConfig {
    pub interval: Duration,
    pub max_deletes_per_entry: usize,
    pub max_deletes_per_round: usize,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        ReaperConfig { interval: Duration::from_secs(30), max_deletes_per_entry: 1_000, max_deletes_per_round: 100_000 }
    }
}

#[derive(Clone)]
pub struct MasterConfig {
    /// Unique within the topology: 1–128 characters of `[A-Za-z0-9._-]`.
    pub identifier: String,
    pub path: PathBuf,
    pub retention: Retention,
    pub reaper: ReaperConfig,
    /// Maximum encoded size of one log entry. Default 16 MiB.
    pub max_entry_bytes: u64,
    pub heartbeat: Duration,
    /// Set when opening a database restored from a backup (DESIGN.md §13).
    pub bump_epoch_on_open: bool,
    pub rocksdb: RocksTuning,
    pub clock: Arc<dyn Clock>,
}

impl MasterConfig {
    pub fn new(identifier: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        MasterConfig {
            identifier: identifier.into(),
            path: path.into(),
            retention: Retention::default(),
            reaper: ReaperConfig::default(),
            max_entry_bytes: 16 << 20,
            heartbeat: Duration::from_secs(5),
            bump_epoch_on_open: false,
            rocksdb: RocksTuning::default(),
            clock: Arc::new(SystemClock),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_identifier(&self.identifier)?;
        if self.max_entry_bytes == 0 {
            return Err(Error::Config("max_entry_bytes must be positive".into()));
        }
        if self.reaper.max_deletes_per_entry == 0 || self.reaper.max_deletes_per_round == 0 {
            return Err(Error::Config("reaper limits must be positive".into()));
        }
        if self.retention.check_interval.is_zero() || self.reaper.interval.is_zero() {
            return Err(Error::Config("maintenance intervals must be positive".into()));
        }
        Ok(())
    }
}

pub(crate) fn validate_identifier(id: &str) -> Result<()> {
    let ok = (1..=128).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(Error::Config(format!("identifier {id:?} must be 1-128 characters of [A-Za-z0-9._-]")))
    }
}
