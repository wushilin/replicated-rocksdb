//! Applying received entries, in groups of consecutive entries (PROTOCOL.md §7.4, DESIGN.md §6).

use std::sync::Arc;
use std::sync::atomic::Ordering;

use super::config::{GapPolicy, MAX_GROUP_OPS_BYTES};
use super::driver::SessionEnd;
use super::inner::Inner;
use super::policy::AutoCause;
use super::state::{HaltReason, Problem};
use crate::error::Error;
use crate::failpoint;
use crate::store::apply::{PreparedGroup, is_cf_entry};
use crate::types::{LogEntry, Op, Seq, epoch_for};

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
        SessionEnd::Retry(Problem::upstream(format!(
            "{EPOCH_CONTRADICTION} (entry {} has epoch {})",
            entry.seq, entry.epoch
        )))
    })
}

/// A received entry that passed its checks and waits in the open group.
struct Received {
    entry: LogEntry,
    ops: Vec<Op>,
    /// `ts_ms` of the received entry after this one (0 = none): the oldest unapplied one once
    /// this one is applied.
    next_ts: u64,
}

/// Applies the entries of one received batch (DESIGN.md §6, PROTOCOL.md §7.4). Consecutive
/// entries are committed together in one `WriteBatch` (group commit); an entry is never split.
/// A group ends after a CF entry (which commits alone), at a verify's capture seq, at the
/// repair target, at `SlaveConfig::max_group_entries` entries or [`MAX_GROUP_OPS_BYTES`], when
/// a verify holds the applier, and at the end of the batch ([`Applier::finish`]). An entry
/// that fails its checks ends the group before it: the group commits, then the failure is
/// handled as for a single entry.
pub(crate) struct Applier<'a> {
    inner: &'a Arc<Inner>,
    generation: u64,
    /// Set once something committed in this session; `last_error` is cleared then.
    cleared: &'a mut bool,
    group: Vec<Received>,
    bytes: usize,
}

impl<'a> Applier<'a> {
    pub(crate) fn new(inner: &'a Arc<Inner>, generation: u64, cleared: &'a mut bool) -> Self {
        Applier {
            inner,
            generation,
            cleared,
            group: Vec::new(),
            bytes: 0,
        }
    }

    /// The seq the next entry must have to follow: after the open group, or the stored
    /// position.
    fn tail(&self) -> Seq {
        match self.group.last() {
            Some(r) => r.entry.seq,
            None => self.inner.store.position().last_seq,
        }
    }

    /// Checks `entry` and adds it to the open group, committing the group whenever it must
    /// end. `Err` ends the session (everything before the failing entry is committed).
    pub(crate) async fn push(
        &mut self,
        entry: LogEntry,
        next_ts: u64,
    ) -> std::result::Result<(), SessionEnd> {
        let inner = self.inner;
        let seq = entry.seq;
        // 1. CRC
        if let Err(e) = entry.verify_crc() {
            self.flush().await?;
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "received a corrupt entry");
            return Err(SessionEnd::Halt(HaltReason::Corrupt));
        }
        // 2. Epoch. A server ends its streams when its identity changes, and the handshake that
        // started this session reconciled both histories (an equal identity, an adopted epoch, or
        // a bootstrap). An entry whose epoch disagrees therefore contradicts the shared history:
        // back off and handshake again, never loop at once (the upstream would send it again).
        let history = inner.ident().map(|(_, h)| h).unwrap_or_default();
        if let Some(end) = epoch_mismatch(&history, &entry) {
            self.flush().await?;
            tracing::warn!(node = %inner.cfg.identifier, seq, epoch = entry.epoch, "entry epoch contradicts the local history; retrying");
            return Err(end);
        }
        // 3, 4. Exactly once, no gaps.
        let last = self.tail();
        if seq <= last {
            match self.group.last_mut() {
                // Becomes the oldest unapplied ts once the group commits.
                Some(r) => r.next_ts = next_ts,
                None => inner.pending_ts.store(next_ts, Ordering::SeqCst),
            }
            return Ok(());
        }
        if seq != last + 1 {
            self.flush().await?;
            tracing::warn!(node = %inner.cfg.identifier, seq, last, "gap in the tail stream");
            return Err(match inner.cfg.on_log_gap {
                GapPolicy::Halt => SessionEnd::Halt(HaltReason::LogGap),
                GapPolicy::Rebootstrap => {
                    // Consent to re-copy this same database, not the operator's (final review I2).
                    if let Some((id, _)) = inner.ident() {
                        inner
                            .auto_rebootstrap
                            .request(self.generation, id.db_uuid, AutoCause::Gap);
                    }
                    // Test hook: the bootstrap is requested; the next handshake has not happened.
                    failpoint::pause("slave-after-gap").await;
                    SessionEnd::Again
                }
            });
        }
        // 5. Decode and validate; it is committed with the changelog copy and last_seq.
        let ops = match entry.decode_ops() {
            Ok(ops) => ops,
            Err(e) => {
                self.flush().await?;
                tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "undecodable entry");
                return Err(SessionEnd::Halt(HaltReason::Corrupt));
            }
        };
        let cf = is_cf_entry(&ops);
        let size = entry.ops.len();
        // The open group ends before this entry when the entry must commit alone, would pass
        // the byte limit, or comes after a verify's capture seq that is in the group, or when
        // a verify holds the applier (never wait with an uncommitted group).
        if let Some(first) = self.group.first().map(|r| r.entry.seq) {
            let target_in_group = inner
                .capture_target()
                .is_some_and(|want| (first..seq).contains(&want));
            if cf
                || self.bytes + size > MAX_GROUP_OPS_BYTES
                || target_in_group
                || inner.applier_held()
            {
                self.flush().await?;
            }
        }
        if self.group.is_empty() {
            // A verify holds the applier while the upstream pins its snapshot (DESIGN.md §12).
            inner.wait_while_held().await;
        }
        self.bytes += size;
        self.group.push(Received {
            entry,
            ops,
            next_ts,
        });
        // The group ends after this entry when it is a CF entry, a verify's capture seq, the
        // repair target, or a limit is reached.
        if cf
            || self.group.len() >= inner.cfg.max_group_entries
            || self.bytes >= MAX_GROUP_OPS_BYTES
            || inner.capture_target() == Some(seq)
            || inner.repairing().is_some_and(|until| seq >= until)
        {
            self.flush().await?;
        }
        Ok(())
    }

    /// Commits the open group: the end of a received batch.
    pub(crate) async fn finish(&mut self) -> std::result::Result<(), SessionEnd> {
        self.flush().await
    }

    /// Commits the open group in one `WriteBatch`, then, per entry in order, publishes it.
    /// On a failure, the entries before the failing one are committed and published, and the
    /// failure ends the session as it would for a single entry.
    async fn flush(&mut self) -> std::result::Result<(), SessionEnd> {
        if self.group.is_empty() {
            return Ok(());
        }
        let inner = self.inner;
        let group = std::mem::take(&mut self.group);
        self.bytes = 0;
        failpoint::pause("slave-before-apply").await;
        let i = inner.clone();
        let result = tokio::task::spawn_blocking(move || {
            let (applied, failure) = commit_blocking(&i, &group);
            (group, applied, failure)
        })
        .await;
        let (group, applied, failure) = match result {
            Ok(r) => r,
            Err(e) => {
                tracing::error!(node = %inner.cfg.identifier, error = %e, "apply task failed; halting");
                return Err(SessionEnd::Halt(HaltReason::Corrupt));
            }
        };
        for r in group.into_iter().take(applied) {
            let seq = r.entry.seq;
            inner.upstream_last_seq.fetch_max(seq, Ordering::SeqCst);
            inner.origin_last_seq.fetch_max(seq, Ordering::SeqCst);
            inner.last_applied_ts.store(r.entry.ts_ms, Ordering::SeqCst);
            // The next received entry (if any) is now the oldest unapplied one.
            inner.pending_ts.store(r.next_ts, Ordering::SeqCst);
            tracing::trace!(node = %inner.cfg.identifier, seq, "applied");
            if failpoint::fire("slave-after-apply") {
                // Emulated crash between commit and listener: freeze until the test kills the node.
                std::future::pending::<()>().await;
            }
            inner.notify(|l| l.on_entry(&r.entry));
            // Published after the listener, so whoever waits for this seq also sees its effects.
            inner.seq_tx.send_replace(seq);
        }
        if applied > 0 && !*self.cleared {
            inner.set_last_error(None);
            *self.cleared = true;
        }
        let Some((seq, failure)) = failure else {
            return Ok(());
        };
        Err(match failure {
            Failure::Prepare(Error::UnknownComparator(name)) => {
                tracing::error!(node = %inner.cfg.identifier, seq, comparator = %name, "comparator is not registered");
                SessionEnd::Halt(HaltReason::SpecMismatch)
            }
            Failure::Prepare(e) => {
                tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "entry does not apply to local state");
                SessionEnd::Halt(HaltReason::Corrupt)
            }
            // Refused before anything was written (a snapshot still holds a different CF of the
            // same physical name): retriable, never a halt. The next session applies it again.
            Failure::Commit(e @ Error::CfInUse { .. }) => {
                tracing::warn!(node = %inner.cfg.identifier, seq, error = %e, "column family still in use by a snapshot; will retry");
                SessionEnd::Retry(Problem::local(e.to_string()))
            }
            // A storage I/O failure (disk full, a device error) says nothing about the data: back
            // off and retry, reported in `last_error`. Nothing of the group was committed
            // (`last_seq` did not move). A physical CF it may have created is guarded by the
            // persisted create intent: the store refuses to reuse it for an entry with another
            // spec (`Corrupt`, below).
            Failure::Commit(e) if e.is_transient_io() => {
                tracing::warn!(node = %inner.cfg.identifier, seq, error = %e, "commit failed with an I/O error; will retry");
                SessionEnd::Retry(Problem::local(format!(
                    "could not commit entry {seq}: {e}"
                )))
            }
            // Integrity failures (corruption, a commit that contradicts local state) halt.
            Failure::Commit(e) => {
                tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "commit failed; halting");
                SessionEnd::Halt(HaltReason::Corrupt)
            }
        })
    }
}

/// On the applier's blocking thread: prepares and commits `group` (one entry through the
/// ordinary path, several in one `WriteBatch`), then runs the steps that must happen after a
/// commit and before the next one. Returns how many entries were committed, and the failure
/// (with the seq it concerns) that stopped the rest.
fn commit_blocking(i: &Inner, group: &[Received]) -> (usize, Option<(Seq, Failure)>) {
    let sync = i.cfg.sync_writes;
    let first = group[0].entry.seq;
    let (applied, failure) = if let [r] = group {
        let prepared = match i.store.prepare(&r.entry, &r.ops) {
            Ok(p) => p,
            Err(e) => return (0, Some((first, Failure::Prepare(e)))),
        };
        if let Err(e) = failpoint::io_error("slave-commit-io")
            .and_then(|()| i.store.commit(prepared, sync))
        {
            return (0, Some((first, Failure::Commit(e))));
        }
        (1, None)
    } else {
        let mut prepared = PreparedGroup::after(i.store.position());
        let mut failure = None;
        for r in group {
            if let Err(e) = i.store.prepare_in_group(&mut prepared, &r.entry, &r.ops) {
                failure = Some((r.entry.seq, Failure::Prepare(e)));
                break;
            }
        }
        let applied = prepared.len();
        if applied > 0 {
            if let Err(e) = failpoint::io_error("slave-commit-io")
                .and_then(|()| i.store.commit_group(prepared, sync))
            {
                // Nothing of the group was committed; a later failing entry is seen again
                // when the stream is retried.
                return (0, Some((first, Failure::Commit(e))));
            }
        }
        (applied, failure)
    };
    #[cfg(feature = "failpoints")]
    if applied > 0 {
        i.record_commit(applied);
    }
    for r in &group[..applied] {
        // A repair is complete once replay reaches its target (DESIGN.md §12.1). Before the
        // capture below, so a verify waiting for exactly that seq is served.
        i.finish_repair_at(r.entry.seq);
        // Still on the applier's thread, before the next group can commit: a verify waiting
        // for this seq gets a snapshot of exactly this state (DESIGN.md §12). A group ends at a
        // capture seq it knows of, so the snapshot is taken right after that seq's commit.
        i.capture_after_commit(r.entry.seq);
    }
    (applied, failure)
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
                nonce: 0,
            },
            EpochStart {
                epoch: 2,
                start_seq: 11,
                nonce: 0,
            },
        ];
        let entry = |seq, epoch| LogEntry::new(seq, epoch, 0, bytes::Bytes::new());
        assert!(epoch_mismatch(&history, &entry(10, 1)).is_none());
        assert!(epoch_mismatch(&history, &entry(11, 2)).is_none());
        for (seq, epoch) in [(11, 1), (10, 2), (12, 3)] {
            match epoch_mismatch(&history, &entry(seq, epoch)) {
                Some(SessionEnd::Retry(m)) => {
                    assert!(m.message.starts_with(EPOCH_CONTRADICTION), "{m}")
                }
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
