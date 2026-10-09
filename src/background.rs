//! Helpers for background and blocking work, shared by master and slave.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{Instant, Interval, MissedTickBehavior, interval_at};

use crate::error::{Error, Result};

/// A tick every `period`, the first one after one period; missed ticks are delayed.
pub(crate) fn every(period: Duration) -> Interval {
    let mut t = interval_at(Instant::now() + period, period);
    t.set_missed_tick_behavior(MissedTickBehavior::Delay);
    t
}

/// Runs one unit of maintenance on a blocking thread. Failures are logged, not returned.
pub(crate) async fn run_logged<T: Send + Sync + 'static>(
    inner: Arc<T>,
    task: &'static str,
    f: fn(&T) -> Result<()>,
) {
    match tokio::task::spawn_blocking(move || f(&inner)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(task, error = %e, "maintenance task failed"),
        Err(e) => tracing::error!(task, error = %e, "maintenance task panicked"),
    }
}

/// Runs `f` (RocksDB I/O, fsync) on a blocking thread, off the async workers.
/// A panic in `f` becomes `Error::Io`.
pub(crate) async fn blocking<T: Send + 'static>(
    f: impl FnOnce() -> Result<T> + Send + 'static,
) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Io(std::io::Error::other(format!("blocking task failed: {e}"))))?
}
