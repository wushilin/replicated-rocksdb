//! Background work on a slave: changelog retention and physical CF drops.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use super::inner::Inner;
use crate::background::{every, run_logged};
use crate::error::Result;

const CF_DROP_INTERVAL: Duration = Duration::from_secs(5);

/// Holds only a `Weak<Inner>`, upgraded for one tick, so dropping the `Slave` releases the
/// database without waiting for this task.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let mut shutdown = inner.shutdown_tx.subscribe();
    let retention_every = inner.cfg.retention.check_interval;
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        let mut retention = every(retention_every);
        let mut cf_drops = every(CF_DROP_INTERVAL);
        loop {
            let (name, f): (&'static str, fn(&Inner) -> Result<()>) = tokio::select! {
                _ = shutdown.changed() => break,
                _ = retention.tick() => ("retention", |i| i.store.trim_changelog(i.now_ms(), &i.cfg.retention).map(drop)),
                _ = cf_drops.tick() => ("cf-drop", |i| i.store.drop_unreferenced_cfs().map(drop)),
            };
            let Some(inner) = weak.upgrade() else { break };
            run_logged(inner, name, f).await;
        }
    })
}
