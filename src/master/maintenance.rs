//! Background work: retention, TTL reaping and physical CF drops.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::{interval_at, Instant, MissedTickBehavior};

use super::writer::Inner;
use crate::error::Result;

const CF_DROP_INTERVAL: Duration = Duration::from_secs(5);

/// The task holds only a `Weak<Inner>`, upgraded for the duration of one tick, so dropping the
/// `Master` releases the database (and its LOCK) without waiting for the task.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let mut shutdown = inner.shutdown_tx.subscribe();
    let (retention_every, reaper_every) = (inner.cfg.retention.check_interval, inner.cfg.reaper.interval);
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        let every = |period: Duration| {
            let mut t = interval_at(Instant::now() + period, period);
            t.set_missed_tick_behavior(MissedTickBehavior::Delay);
            t
        };
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
            run(inner, name, f).await;
        }
    })
}

async fn run(inner: Arc<Inner>, task: &'static str, f: fn(&Inner) -> Result<()>) {
    match tokio::task::spawn_blocking(move || f(&inner)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(task, error = %e, "maintenance task failed"),
        Err(e) => tracing::error!(task, error = %e, "maintenance task panicked"),
    }
}
