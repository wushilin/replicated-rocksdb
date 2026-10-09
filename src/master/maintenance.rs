//! Background work: retention, TTL reaping and physical CF drops.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use super::writer::Inner;
use crate::background::{every, run_logged};
use crate::error::Result;

const CF_DROP_INTERVAL: Duration = Duration::from_secs(5);

/// The task holds only a `Weak<Inner>`, upgraded for the duration of one tick, so dropping the
/// `Master` releases the database (and its LOCK) without waiting for the task.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let mut shutdown = inner.shutdown_tx.subscribe();
    let (retention_every, reaper_every) = (inner.cfg.retention.check_interval, inner.cfg.reaper.interval);
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        let mut retention = every(retention_every);
        let mut reaper = every(reaper_every);
        let mut cf_drops = every(CF_DROP_INTERVAL);
        loop {
            let (name, f): (&'static str, fn(&Inner) -> Result<()>) = tokio::select! {
                _ = shutdown.changed() => break,
                _ = retention.tick() => ("retention", |i| i.trim_changelog().map(drop)),
                _ = reaper.tick() => ("reaper", |i| i.reap_round().map(drop)),
                _ = cf_drops.tick() => ("cf-drop", |i| i.store.drop_unreferenced_cfs().map(drop)),
            };
            let Some(inner) = weak.upgrade() else { break };
            run_logged(inner, name, f).await;
        }
    })
}
