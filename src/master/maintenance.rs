//! Background work: retention, TTL reaping and physical CF drops.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::{interval_at, Instant, MissedTickBehavior};

use super::writer::Inner;
use crate::error::Result;

const CF_DROP_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) fn spawn(inner: Arc<Inner>) -> JoinHandle<()> {
    let mut shutdown = inner.shutdown_tx.subscribe();
    tokio::spawn(async move {
        let every = |period: Duration| {
            let mut t = interval_at(Instant::now() + period, period);
            t.set_missed_tick_behavior(MissedTickBehavior::Delay);
            t
        };
        let mut retention = every(inner.cfg.retention.check_interval);
        let mut reaper = every(inner.cfg.reaper.interval);
        let mut cf_drops = every(CF_DROP_INTERVAL);
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = retention.tick() => run(&inner, "retention", |i| i.trim_changelog().map(drop)).await,
                _ = reaper.tick() => run(&inner, "reaper", |i| i.reap_round().map(drop)).await,
                _ = cf_drops.tick() => run(&inner, "cf-drop", |i| i.store.drop_unreferenced_cfs().map(drop)).await,
            }
        }
    })
}

async fn run(inner: &Arc<Inner>, task: &'static str, f: fn(&Inner) -> Result<()>) {
    let i = inner.clone();
    match tokio::task::spawn_blocking(move || f(&i)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(task, error = %e, "maintenance task failed"),
        Err(e) => tracing::error!(task, error = %e, "maintenance task panicked"),
    }
}
