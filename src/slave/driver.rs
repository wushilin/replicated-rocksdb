//! The replication driver: one task per slave that connects, handshakes and acts on the
//! answer, forever (PROTOCOL.md §11.3).

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;

use tonic::Status;

use super::apply::{self, Applied};
use super::bootstrap;
use super::client::{self, Client};
use super::config::BackoffState;
use super::inner::Inner;
use super::policy::{self, Action, Force};
use super::repoint::{self, RepointDivergence, RepointOutcome};
use super::state::{HaltReason, NodeState, SlaveState};
use crate::background::blocking;
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::failpoint;
use crate::net::DECODE_SLACK;
use crate::net::convert::{
    checked_history, entry_from_pb, epoch_to_pb, identity_from_pb, identity_to_pb,
};
use crate::pb;
use crate::pb::tail_client_message::Kind as ClientKind;
use crate::pb::tail_message::Kind;
use crate::store::meta::key;

/// How a session ended.
#[derive(Debug)]
pub(crate) enum SessionEnd {
    /// Back off, then handshake again.
    Retry(String),
    /// Handshake again after a short pause.
    Again,
    Halt(HaltReason),
}

/// Pause before an immediate re-handshake, so a confused upstream cannot cause a hot loop.
const AGAIN_PAUSE: Duration = Duration::from_millis(50);
/// Backoff resets after this much uninterrupted streaming (PROTOCOL.md §11.4).
const STABLE_STREAMING: Duration = Duration::from_secs(60);

/// Maps a policy action for a failed call to how the session ends.
pub(crate) fn end_for(inner: &Inner, generation: u64, action: Action) -> SessionEnd {
    match action {
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Bootstrap => {
            inner.force_bootstrap.request(generation);
            SessionEnd::Again
        }
        Action::Again | Action::Tail | Action::AdoptEpochThenTail => SessionEnd::Again,
    }
}

/// Awaits `f` (a network call), or ends the session with `Again` once the slave is repointed
/// away from the upstream of session `generation`. Store writes are never cancelled this way: the
/// driver checks only between them.
pub(crate) async fn unless_repointed<T>(
    inner: &Inner,
    generation: u64,
    f: impl Future<Output = T>,
) -> Result<T, SessionEnd> {
    let mut repoints = inner.repoint_tx.subscribe();
    tokio::select! {
        biased;
        _ = repoints.wait_for(|g| *g != generation) => Err(SessionEnd::Again),
        v = f => Ok(v),
    }
}

pub(crate) fn status_end(inner: &Inner, generation: u64, s: &Status) -> SessionEnd {
    // After a repoint, a Tail or Bootstrap ending with DIVERGED follows the repoint's mode.
    if crate::net::reason_of(s) == Some(pb::Reason::Diverged) {
        match inner.repoint_mode() {
            Some(RepointDivergence::Rebootstrap { .. }) => {
                return end_for(inner, generation, Action::Bootstrap);
            }
            // No handshake here to compute the fork from: handshake again, which answers
            // `DIVERGED` under the same mode and halts with the exact `Error::Diverged`.
            Some(RepointDivergence::Refuse | RepointDivergence::Repair { .. }) => {
                inner.diverged_on_stream.store(true, Ordering::SeqCst);
                return SessionEnd::Again;
            }
            None => {}
        }
    }
    end_for(
        inner,
        generation,
        policy::after_status(s, &inner.policies()),
    )
}

/// Starts the driver. It holds only a `Weak<Inner>` while it waits (halted, or backing off),
/// and a strong reference only for the duration of one session, which shutdown cancels.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let weak = Arc::downgrade(inner);
    let shutdown = inner.shutdown_tx.subscribe();
    let halt_cleared = inner.halt_cleared.clone();
    let repoints = inner.repoint_tx.subscribe();
    let backoff = BackoffState::new(inner.cfg.reconnect.clone());
    tokio::spawn(run(weak, shutdown, repoints, halt_cleared, backoff))
}

async fn run(
    weak: Weak<Inner>,
    mut shutdown: watch::Receiver<bool>,
    mut repoints: watch::Receiver<u64>,
    halt_cleared: Arc<Notify>,
    mut backoff: BackoffState,
) {
    // The client of the upstream of repoint generation `.0`.
    let mut client: Option<(u64, Client)> = None;
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        let Some(inner) = weak.upgrade() else { return };
        if matches!(inner.state(), SlaveState::Halted(_)) {
            drop(inner);
            tokio::select! {
                _ = halt_cleared.notified() => backoff.reset(),
                _ = repoints.changed() => {
                    // A repoint that raced the halt: it clears the halt in memory.
                    if let Some(inner) = weak.upgrade() {
                        clear_halt_for_repoint(&inner);
                    }
                    backoff.reset();
                }
                r = shutdown.changed() => if r.is_err() { return },
            }
            continue;
        }
        let generation = *repoints.borrow_and_update();
        if client.as_ref().is_some_and(|(g, _)| *g != generation) {
            client = None;
            backoff.reset();
        }
        let end = tokio::select! {
            end = session(&inner, generation, &mut client, &mut backoff) => end,
            r = shutdown.changed() => {
                if r.is_err() {
                    return;
                }
                continue;
            }
        };
        if !matches!(end, SessionEnd::Halt(_)) {
            inner.settle_repoint_end(generation, &end);
        }
        let pause = match end {
            SessionEnd::Halt(reason) => {
                let i = inner.clone();
                let _ = blocking(move || {
                    // Test hook: lets a repoint arrive between ending the session and the halt.
                    failpoint::fire("slave-before-halt");
                    i.halt(reason);
                    Ok(())
                })
                .await;
                // A repoint waiting on this session sees the halt in place.
                inner.settle_repoint_end(generation, &end);
                // A repoint that arrived while the halt was being persisted would find the driver
                // parked: it supersedes the halt.
                if *repoints.borrow() != generation {
                    clear_halt_for_repoint(&inner);
                }
                continue;
            }
            SessionEnd::Again => AGAIN_PAUSE,
            SessionEnd::Retry(msg) => {
                tracing::warn!(node = %inner.cfg.identifier, upstream = %inner.upstream_uri(), error = %msg, "replication retry");
                inner.set_last_error(Some(msg));
                inner.set_state(SlaveState::Connecting);
                backoff.next_delay()
            }
        };
        drop(inner);
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
            _ = repoints.changed() => {}
            r = shutdown.changed() => if r.is_err() { return },
        }
    }
}

/// A repoint supersedes a halt: clear it in memory for the new generation. Persisting
/// `ready` waits until the new upstream is accepted ([`commit_halt_clear`]).
fn clear_halt_for_repoint(inner: &Inner) {
    if matches!(inner.state(), SlaveState::Halted(_)) {
        inner.halt_clear_pending.store(true, Ordering::SeqCst);
        inner.set_state(SlaveState::Connecting);
    }
}

/// Persists `node_state = ready` for a halt a repoint cleared in memory, once the new
/// upstream has been accepted.
async fn commit_halt_clear(inner: &Arc<Inner>) {
    if inner.halt_clear_pending.swap(false, Ordering::SeqCst) {
        let i = inner.clone();
        let r = blocking(move || {
            i.store
                .write_meta(&[(key::NODE_STATE, &NodeState::Ready.encode())], true)
        })
        .await;
        if let Err(e) = r {
            tracing::warn!(node = %inner.cfg.identifier, error = %e, "could not persist the cleared halt");
            inner.halt_clear_pending.store(true, Ordering::SeqCst);
        }
    }
}

fn handshake_request(inner: &Inner) -> pb::HandshakeRequest {
    let ident = inner.ident();
    let pos = inner.store.position();
    pb::HandshakeRequest {
        protocol_version: PROTOCOL_VERSION,
        envelope_version: u32::from(ENVELOPE_VERSION),
        client_id: inner.cfg.identifier.clone(),
        known_identity: ident.as_ref().map(|(id, _)| identity_to_pb(id)),
        last_seq: if ident.is_some() { pos.last_seq } else { 0 },
        last_crc32c: if ident.is_some() {
            pos.last_entry_crc
        } else {
            None
        },
        // Tells the server which epochs these are, not only the number: start seqs and nonces
        // of the newest records (PROTOCOL §6.3).
        known_epochs: inner.known_epochs().iter().map(epoch_to_pb).collect(),
    }
}

/// One connect → handshake → act cycle against the upstream of repoint `generation`.
async fn session(
    inner: &Arc<Inner>,
    generation: u64,
    client: &mut Option<(u64, Client)>,
    backoff: &mut BackoffState,
) -> SessionEnd {
    inner.set_state(SlaveState::Connecting);
    let c = match client {
        Some((_, c)) => c.clone(),
        None => match client::connect(inner) {
            Ok(c) => client.insert((generation, c)).1.clone(),
            Err(e) => return SessionEnd::Retry(e.to_string()),
        },
    };
    let hs = match unless_repointed(
        inner,
        generation,
        c.clone().handshake(handshake_request(inner)),
    )
    .await
    {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(s)) => return status_end(inner, generation, &s),
        Err(end) => return end,
    };
    if hs.decision() != pb::Decision::Reject {
        if let Some(max) = inner.note_upstream(&hs) {
            let i = inner.clone();
            let persisted = blocking(move || {
                i.store
                    .write_meta(&[(key::MAX_ENTRY_BYTES, &max.to_be_bytes())], false)
            });
            if let Err(e) = persisted.await {
                tracing::warn!(error = %e, "could not persist max_entry_bytes");
            }
        }
    } else {
        inner.note_contact();
    }
    let hs_uuid = hs
        .identity
        .as_ref()
        .and_then(|i| identity_from_pb(i).ok())
        .map(|i| i.db_uuid);
    let force = if inner.force_bootstrap.is_requested_for(generation) {
        Force::Operator
    } else if inner.verify_rebootstrap.applies(generation, hs_uuid) {
        Force::Verify
    } else {
        Force::None
    };
    let after_stream_diverged = inner.diverged_on_stream.swap(false, Ordering::SeqCst);
    let mut action = policy::after_handshake(&hs, &inner.policies(), force);
    // An explicit repoint never resolves divergence by the configured policy.
    if let Some(mode) = inner.repoint_mode() {
        let diverged = hs.decision() == pb::Decision::Reject && hs.reason() == pb::Reason::Diverged;
        if diverged && force != Force::Operator {
            action = match mode {
                RepointDivergence::Rebootstrap { .. } => Action::Bootstrap,
                RepointDivergence::Refuse | RepointDivergence::Repair { .. } => {
                    let err = repoint::mode_error(mode, || {
                        repoint::divergence(
                            &inner.ident().map(|(_, h)| h).unwrap_or_default(),
                            inner.store.position().last_seq,
                            &hs,
                        )
                    });
                    tracing::warn!(node = %inner.cfg.identifier, error = %err, "repoint refused: diverged");
                    inner.fail_repoint_on_halt(generation, err);
                    Action::Halt(HaltReason::Diverged)
                }
            };
        }
    }
    // The retry arm of the driver loop logs the warning and sets `last_error`.
    if let Some(end) = contradicting_resume(after_stream_diverged, &action) {
        return end;
    }
    match action {
        Action::Tail => {
            commit_halt_clear(inner).await;
            inner.settle_repoint(generation, Ok(RepointOutcome::Resumed));
            tail(inner, generation, &c, &hs, None, backoff).await
        }
        Action::AdoptEpochThenTail => match adopt_epoch(inner, &hs).await {
            Ok(()) => {
                commit_halt_clear(inner).await;
                inner.settle_repoint(generation, Ok(RepointOutcome::EpochAdvanced));
                tail(inner, generation, &c, &hs, None, backoff).await
            }
            Err(end) => end,
        },
        Action::Bootstrap => match bootstrap::install(inner, generation, &c, &hs).await {
            Ok(lease) => {
                // The bootstrap commit wrote `ready` itself.
                inner.halt_clear_pending.store(false, Ordering::SeqCst);
                inner.settle_repoint(generation, Ok(RepointOutcome::Bootstrapped));
                tail(inner, generation, &c, &hs, Some(lease), backoff).await
            }
            Err(end) => end,
        },
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Again => SessionEnd::Again,
    }
}

/// The Tail decode limit (PROTOCOL.md §11.4): `max(max_entry_bytes, max_batch_bytes)` plus
/// framing slack, where `max_batch_bytes = 0` means the server's default of 1 MiB.
fn tail_decode_limit(max_entry_bytes: u64, max_batch_bytes: u32) -> usize {
    let batch = match max_batch_bytes {
        0 => crate::server::DEFAULT_MAX_BATCH_BYTES as u64,
        n => u64::from(n),
    };
    usize::try_from(max_entry_bytes.max(batch))
        .unwrap_or(usize::MAX)
        .saturating_add(DECODE_SLACK)
}

const CONTRADICTION: &str = "upstream reported DIVERGED on the stream but not in the handshake";

/// A handshake that answers RESUME right after the stream ended `DIVERGED` contradicts the
/// upstream's own report: retry with the normal reconnect backoff (and `last_error` set), not
/// the fixed `AGAIN_PAUSE`, so a misbehaving upstream is not hammered.
fn contradicting_resume(after_stream_diverged: bool, action: &Action) -> Option<SessionEnd> {
    (after_stream_diverged && matches!(action, Action::Tail | Action::AdoptEpochThenTail))
        .then(|| SessionEnd::Retry(CONTRADICTION.to_string()))
}

/// RESUME / EPOCH_ADVANCED: persist the new identity and history atomically, then tail.
/// An invalid identity or history from the upstream halts the slave (`Corrupt`).
async fn adopt_epoch(inner: &Arc<Inner>, hs: &pb::HandshakeResponse) -> Result<(), SessionEnd> {
    let id = hs
        .identity
        .as_ref()
        .ok_or(SessionEnd::Halt(HaltReason::Corrupt))?;
    let id = identity_from_pb(id).map_err(|_| SessionEnd::Halt(HaltReason::Corrupt))?;
    let history = checked_history(&id, &hs.epoch_history).map_err(|e| {
        tracing::error!(node = %inner.cfg.identifier, error = %e, "upstream sent an invalid epoch history");
        SessionEnd::Halt(HaltReason::Corrupt)
    })?;
    let (i, h) = (inner.clone(), history.clone());
    blocking(move || i.store.adopt_identity(&id, &h))
        .await
        .map_err(|e| SessionEnd::Retry(e.to_string()))?;
    tracing::info!(node = %inner.cfg.identifier, epoch = id.epoch, "adopted new epoch from upstream");
    inner.set_ident(Some((id, history)));
    inner.reset_downstream();
    Ok(())
}

/// Tails from `last_seq + 1` and applies entries until the stream ends.
async fn tail(
    inner: &Arc<Inner>,
    generation: u64,
    c: &Client,
    hs: &pb::HandshakeResponse,
    lease: Option<Vec<u8>>,
    backoff: &mut BackoffState,
) -> SessionEnd {
    let Some((identity, _)) = inner.ident() else {
        return SessionEnd::Again;
    };
    let pos = inner.store.position();
    let fetch = &inner.cfg.fetch;
    let limit = tail_decode_limit(hs.max_entry_bytes, fetch.max_batch_bytes);
    let req = pb::TailRequest {
        client_id: inner.cfg.identifier.clone(),
        identity: Some(identity_to_pb(&identity)),
        from_seq: pos.last_seq + 1,
        prev_crc32c: pos.last_entry_crc,
        bootstrap_lease: lease,
        min_batch_bytes: fetch.min_batch_bytes,
        max_wait_ms: u32::try_from(fetch.max_wait.as_millis()).unwrap_or(u32::MAX),
        max_batch_bytes: fetch.max_batch_bytes,
    };
    // The request stream: `start`, then an ack whenever `acks` changes. A watch coalesces
    // acks that back up: the upstream always gets the latest applied seq.
    let (acks, ack_rx) = watch::channel(pos.last_seq);
    let start = pb::TailClientMessage {
        kind: Some(ClientKind::Start(req)),
    };
    let ack_stream = WatchStream::from_changes(ack_rx).map(|applied_seq| pb::TailClientMessage {
        kind: Some(ClientKind::Ack(pb::TailAck { applied_seq })),
    });
    let outbound = tokio_stream::iter([start]).chain(ack_stream);
    let mut stream = match unless_repointed(
        inner,
        generation,
        c.clone().max_decoding_message_size(limit).tail(outbound),
    )
    .await
    {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(s)) => return status_end(inner, generation, &s),
        Err(end) => return end,
    };
    inner.set_state(SlaveState::Streaming);
    let idle = (Duration::from_millis(u64::from(hs.heartbeat_ms)) * 3).max(inner.cfg.heartbeat);
    let mut cleared = false;
    let started = Instant::now();
    loop {
        let received = tokio::select! {
            r = unless_repointed(inner, generation, tokio::time::timeout(idle, stream.message())) => r,
            // A verify asked for a re-bootstrap (`MismatchPolicy::Rebootstrap`): handshake again,
            // which bootstraps because the request is set. A stale wake-up is ignored.
            _ = inner.rebootstrap.notified() => {
                if inner.force_bootstrap.is_requested_for(generation)
                    || inner.verify_rebootstrap.pending_for(generation)
                {
                    return SessionEnd::Again;
                }
                continue;
            }
        };
        let next = match received {
            Ok(next) => next,
            Err(end) => return end,
        };
        let msg = match next {
            Err(_) => return SessionEnd::Retry(format!("no message from upstream for {idle:?}")),
            Ok(Err(s)) => return status_end(inner, generation, &s),
            Ok(Ok(None)) => return SessionEnd::Retry("upstream ended the tail stream".into()),
            Ok(Ok(Some(m))) => m,
        };
        inner.note_contact();
        if !cleared && matches!(msg.kind, Some(Kind::Heartbeat(_))) {
            inner.set_last_error(None);
            cleared = true;
        }
        if started.elapsed() >= STABLE_STREAMING {
            backoff.reset();
        }
        match msg.kind {
            Some(Kind::Heartbeat(h)) => {
                inner.note_positions(h.upstream_last_seq, h.origin_last_seq);
                // At least one ack per heartbeat interval keeps this replica in sync upstream.
                acks.send_replace(inner.store.position().last_seq);
            }
            Some(Kind::Batch(batch)) => {
                if batch.entries.is_empty() {
                    return SessionEnd::Retry("empty entry batch from upstream".into());
                }
                // Each entry is its own atomic write, so a crash mid-batch leaves a clean boundary.
                for e in batch.entries {
                    match apply::apply(inner, generation, entry_from_pb(e)).await {
                        Applied::Committed => {
                            if !cleared {
                                inner.set_last_error(None);
                                cleared = true;
                            }
                        }
                        Applied::Skipped => {}
                        Applied::End(end) => return end,
                    }
                }
                acks.send_replace(inner.store.position().last_seq);
            }
            None => return SessionEnd::Retry("tail message without content".into()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_after_a_stream_diverged_is_retried_with_backoff() {
        for a in [Action::Tail, Action::AdoptEpochThenTail] {
            assert!(
                matches!(contradicting_resume(true, &a), Some(SessionEnd::Retry(m)) if m == CONTRADICTION)
            );
            assert!(contradicting_resume(false, &a).is_none());
        }
        assert!(contradicting_resume(true, &Action::Bootstrap).is_none());
        assert!(contradicting_resume(true, &Action::Again).is_none());
    }

    /// M6: `max_batch_bytes = 0` is the server default (1 MiB), not 0, in the decode limit.
    #[test]
    fn the_tail_decode_limit_maps_a_zero_batch_size_to_the_default() {
        assert_eq!(tail_decode_limit(1000, 0), (1 << 20) + DECODE_SLACK);
        assert_eq!(tail_decode_limit(1000, 2 << 20), (2 << 20) + DECODE_SLACK);
        assert_eq!(tail_decode_limit(16 << 20, 0), (16 << 20) + DECODE_SLACK);
    }
}
