//! Applying one received entry (PROTOCOL.md §7.4, DESIGN.md §6).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::config::GapPolicy;
use super::driver::SessionEnd;
use super::inner::Inner;
use super::state::HaltReason;
use crate::error::Error;
use crate::failpoint;
use crate::types::{LogEntry, epoch_for};

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

/// `last_error` when an upstream sends an entry whose epoch disagrees with the history the
/// session started with.
pub(crate) const EPOCH_CONTRADICTION: &str =
    "upstream sent an entry whose epoch contradicts the shared history";

/// PROTOCOL.md §7.4 check 2: `Some(Retry)` (normal backoff, `last_error` set) when `entry.epoch`
/// is not the epoch of `entry.seq` in `history`.
fn epoch_mismatch(history: &[crate::types::EpochStart], entry: &LogEntry) -> Option<SessionEnd> {
    (epoch_for(history, entry.seq) != Some(entry.epoch)).then(|| {
        SessionEnd::Retry(format!(
            "{EPOCH_CONTRADICTION} (entry {} has epoch {})",
            entry.seq, entry.epoch
        ))
    })
}

pub(crate) async fn apply(inner: &Arc<Inner>, generation: u64, entry: LogEntry) -> Applied {
    let seq = entry.seq;
    // 1. CRC
    if let Err(e) = entry.verify_crc() {
        tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "received a corrupt entry");
        return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
    }
    // 2. Epoch. A server ends its streams when its identity changes, and the handshake that
    // started this session reconciled both histories (an equal identity, an adopted epoch, or a
    // bootstrap). An entry whose epoch disagrees therefore contradicts the shared history: back
    // off and handshake again, never loop at once (the upstream would send it again).
    let history = inner.ident().map(|(_, h)| h).unwrap_or_default();
    if let Some(end) = epoch_mismatch(&history, &entry) {
        tracing::warn!(node = %inner.cfg.identifier, seq, epoch = entry.epoch, "entry epoch contradicts the local history; retrying");
        return Applied::End(end);
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
        failpoint::io_error("slave-commit-io").map_err(Failure::Commit)?;
        i.store
            .commit(prepared, i.cfg.sync_writes)
            .map_err(Failure::Commit)
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
        // A storage I/O failure (disk full, a device error) says nothing about the data: back
        // off and retry, reported in `last_error`. Nothing was committed (`last_seq` did not
        // move). A physical CF it may have created is guarded by the persisted create intent:
        // the store refuses to reuse it for an entry with another spec (`Corrupt`, below).
        Ok(Err(Failure::Commit(e))) if e.is_transient_io() => {
            tracing::warn!(node = %inner.cfg.identifier, seq, error = %e, "commit failed with an I/O error; will retry");
            return Applied::End(SessionEnd::Retry(format!(
                "could not commit entry {seq}: {e}"
            )));
        }
        // Integrity failures (corruption, a commit that contradicts local state) halt.
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::EpochStart;

    /// C1: an entry whose epoch contradicts the history the session started with ends the
    /// session with a backed-off `Retry` and a `last_error`, never an immediate `Again` (which
    /// looped every 50 ms against an upstream that sends the same entry again).
    #[test]
    fn an_epoch_contradiction_is_retried_with_backoff() {
        let history = [
            EpochStart {
                epoch: 1,
                start_seq: 1,
            },
            EpochStart {
                epoch: 2,
                start_seq: 11,
            },
        ];
        let entry = |seq, epoch| LogEntry::new(seq, epoch, 0, bytes::Bytes::new());
        assert!(epoch_mismatch(&history, &entry(10, 1)).is_none());
        assert!(epoch_mismatch(&history, &entry(11, 2)).is_none());
        for (seq, epoch) in [(11, 1), (10, 2), (12, 3)] {
            match epoch_mismatch(&history, &entry(seq, epoch)) {
                Some(SessionEnd::Retry(m)) => assert!(m.starts_with(EPOCH_CONTRADICTION), "{m}"),
                other => panic!("entry {seq}@{epoch}: expected Retry, got {other:?}"),
            }
        }
        assert!(
            matches!(
                epoch_mismatch(&[], &entry(1, 1)),
                Some(SessionEnd::Retry(_))
            ),
            "no history"
        );
    }
}
