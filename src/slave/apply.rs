//! Applying one received entry (PROTOCOL.md §7.4, DESIGN.md §6).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::config::GapPolicy;
use super::driver::SessionEnd;
use super::inner::Inner;
use super::state::HaltReason;
use crate::error::Error;
use crate::failpoint;
use crate::types::{epoch_for, LogEntry};

pub(crate) enum Applied {
    Committed,
    /// Already applied (re-sent after a reconnect).
    Skipped,
    End(SessionEnd),
}

enum Failure {
    Prepare(Error),
    Commit(Error),
}

pub(crate) async fn apply(inner: &Arc<Inner>, generation: u64, entry: LogEntry) -> Applied {
    let seq = entry.seq;
    // 1. CRC
    if let Err(e) = entry.verify_crc() {
        tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "received a corrupt entry");
        return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
    }
    // 2. Epoch: the upstream's history changed; the next handshake brings it.
    let history = inner.ident().map(|(_, h)| h).unwrap_or_default();
    if epoch_for(&history, seq) != Some(entry.epoch) {
        tracing::info!(node = %inner.cfg.identifier, seq, epoch = entry.epoch, "entry epoch differs from local history; handshaking again");
        return Applied::End(SessionEnd::Again);
    }
    // 3, 4. Exactly once, no gaps.
    let last = inner.store.position().last_seq;
    if seq <= last {
        return Applied::Skipped;
    }
    if seq != last + 1 {
        tracing::warn!(node = %inner.cfg.identifier, seq, last, "gap in the tail stream");
        return Applied::End(match inner.cfg.on_log_gap {
            GapPolicy::Halt => SessionEnd::Halt(HaltReason::LogGap),
            GapPolicy::Rebootstrap => {
                inner.force_bootstrap.request(generation);
                SessionEnd::Again
            }
        });
    }
    // 5. Decode and validate, then apply in one batch with the changelog copy and last_seq.
    let ops = match entry.decode_ops() {
        Ok(ops) => ops,
        Err(e) => {
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "undecodable entry");
            return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
        }
    };
    let i = inner.clone();
    let e = entry.clone();
    let result = tokio::task::spawn_blocking(move || {
        let prepared = i.store.prepare(&e, &ops).map_err(Failure::Prepare)?;
        i.store.commit(prepared, i.cfg.sync_writes).map_err(Failure::Commit)
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(Failure::Prepare(Error::UnknownComparator(name)))) => {
            tracing::error!(node = %inner.cfg.identifier, seq, comparator = %name, "comparator is not registered");
            return Applied::End(SessionEnd::Halt(HaltReason::SpecMismatch));
        }
        Ok(Err(Failure::Prepare(e))) => {
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "entry does not apply to local state");
            return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
        }
        // Refused before anything was written (a snapshot still holds a different CF of the
        // same physical name): retriable, never a halt. The next session applies it again.
        Ok(Err(Failure::Commit(e @ Error::CfInUse { .. }))) => {
            tracing::warn!(node = %inner.cfg.identifier, seq, error = %e, "column family still in use by a snapshot; will retry");
            return Applied::End(SessionEnd::Retry(e.to_string()));
        }
        // A failed commit is never retried in-process: its CF side effects (a created
        // physical CF) must not be reused with another entry. Halt; restart re-evaluates.
        Ok(Err(Failure::Commit(e))) => {
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "commit failed; halting");
            return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
        }
        Err(e) => {
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "apply task failed; halting");
            return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
        }
    }
    inner.upstream_last_seq.fetch_max(seq, Ordering::SeqCst);
    inner.origin_last_seq.fetch_max(seq, Ordering::SeqCst);
    tracing::trace!(node = %inner.cfg.identifier, seq, "applied");
    if failpoint::fire("slave-after-apply") {
        // Emulated crash between commit and listener: freeze until the test kills the node.
        std::future::pending::<()>().await;
    }
    inner.notify(|l| l.on_entry(&entry));
    // Published after the listener, so whoever waits for this seq also sees its effects.
    inner.seq_tx.send_replace(seq);
    Applied::Committed
}
