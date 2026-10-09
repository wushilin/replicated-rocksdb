#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{LogApi, LogEntry, ManualClock, MasterConfig};

pub const T0: u64 = 1_700_000_000_000;

/// Master config with a manual clock and maintenance effectively disabled.
pub fn config(dir: &Path, clock: &Arc<ManualClock>) -> MasterConfig {
    let mut cfg = MasterConfig::new("master-a", dir);
    cfg.clock = clock.clone();
    cfg.retention.check_interval = Duration::from_secs(3600);
    cfg.reaper.interval = Duration::from_secs(3600);
    cfg
}

pub fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(T0))
}

pub fn log(m: &impl LogApi) -> Vec<LogEntry> {
    m.entries_since(0).unwrap().map(Result::unwrap).collect()
}
