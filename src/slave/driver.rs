//! The replication driver: one task per slave that connects, handshakes and acts on the
//! answer, forever (PROTOCOL.md §11.3).

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;

use tonic::Status;

use super::apply::{self, Applied};
use super::bootstrap;
use super::client::{self, Client};
use super::config::BackoffState;
use super::inner::Inner;
use super::policy::{self, Action};
use super::state::{HaltReason, SlaveState};
use crate::background::blocking;
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::net::convert::{checked_history, entry_from_pb, identity_from_pb, identity_to_pb};
use crate::net::DECODE_SLACK;
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
pub(crate) fn end_for(inner: &Inner, action: Action) -> SessionEnd {
    match action {
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Bootstrap => {
            inner.force_bootstrap.store(true, Ordering::SeqCst);
            SessionEnd::Again
        }
        Action::Again | Action::Tail | Action::AdoptEpochThenTail => SessionEnd::Again,
    }
}

pub(crate) fn status_end(inner: &Inner, s: &Status) -> SessionEnd {
    end_for(inner, policy::after_status(s, &inner.policies()))
}

/// Starts the driver. It holds only a `Weak<Inner>` while it waits (halted, or backing off),
/// and a strong reference only for the duration of one session, which shutdown cancels.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let weak = Arc::downgrade(inner);
    let shutdown = inner.shutdown_tx.subscribe();
    let halt_cleared = inner.halt_cleared.clone();
    let backoff = BackoffState::new(inner.cfg.reconnect.clone());
    tokio::spawn(run(weak, shutdown, halt_cleared, backoff))
}

async fn run(weak: Weak<Inner>, mut shutdown: watch::Receiver<bool>, halt_cleared: Arc<Notify>, mut backoff: BackoffState) {
    let mut client: Option<Client> = None;
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        let Some(inner) = weak.upgrade() else { return };
        if matches!(inner.state(), SlaveState::Halted(_)) {
            drop(inner);
            tokio::select! {
                _ = halt_cleared.notified() => backoff.reset(),
                r = shutdown.changed() => if r.is_err() { return },
            }
            continue;
        }
        let end = tokio::select! {
            end = session(&inner, &mut client, &mut backoff) => end,
            r = shutdown.changed() => {
                if r.is_err() {
                    return;
                }
                continue;
            }
        };
        let pause = match end {
            SessionEnd::Halt(reason) => {
                let i = inner.clone();
                let _ = blocking(move || {
                    i.halt(reason);
                    Ok(())
                })
                .await;
                continue;
            }
            SessionEnd::Again => AGAIN_PAUSE,
            SessionEnd::Retry(msg) => {
                tracing::warn!(node = %inner.cfg.identifier, upstream = %inner.cfg.upstream.uri(), error = %msg, "replication retry");
                inner.set_last_error(Some(msg));
                inner.set_state(SlaveState::Connecting);
                backoff.next_delay()
            }
        };
        drop(inner);
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
            r = shutdown.changed() => if r.is_err() { return },
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
        last_crc32c: if ident.is_some() { pos.last_entry_crc } else { None },
    }
}

/// One connect → handshake → act cycle.
async fn session(inner: &Arc<Inner>, client: &mut Option<Client>, backoff: &mut BackoffState) -> SessionEnd {
    inner.set_state(SlaveState::Connecting);
    let c = match client {
        Some(c) => c.clone(),
        None => match client::connect(&inner.cfg) {
            Ok(c) => client.insert(c).clone(),
            Err(e) => return SessionEnd::Retry(e.to_string()),
        },
    };
    let hs = match c.clone().handshake(handshake_request(inner)).await {
        Ok(r) => r.into_inner(),
        Err(s) => return status_end(inner, &s),
    };
    if hs.decision() != pb::Decision::Reject {
        if let Some(max) = inner.note_upstream(&hs) {
            let i = inner.clone();
            let persisted = blocking(move || i.store.write_meta(&[(key::MAX_ENTRY_BYTES, &max.to_be_bytes())], false));
            if let Err(e) = persisted.await {
                tracing::warn!(error = %e, "could not persist max_entry_bytes");
            }
        }
    } else {
        inner.note_contact();
    }
    let force = inner.force_bootstrap.load(Ordering::SeqCst);
    match policy::after_handshake(&hs, &inner.policies(), force) {
        Action::Tail => tail(inner, &c, &hs, None, backoff).await,
        Action::AdoptEpochThenTail => match adopt_epoch(inner, &hs).await {
            Ok(()) => tail(inner, &c, &hs, None, backoff).await,
            Err(end) => end,
        },
        Action::Bootstrap => match bootstrap::install(inner, &c, &hs).await {
            Ok(lease) => tail(inner, &c, &hs, Some(lease), backoff).await,
            Err(end) => end,
        },
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Again => SessionEnd::Again,
    }
}

/// RESUME / EPOCH_ADVANCED: persist the new identity and history atomically, then tail.
/// An invalid identity or history from the upstream halts the slave (`Corrupt`).
async fn adopt_epoch(inner: &Arc<Inner>, hs: &pb::HandshakeResponse) -> Result<(), SessionEnd> {
    let id = hs.identity.as_ref().ok_or(SessionEnd::Halt(HaltReason::Corrupt))?;
    let id = identity_from_pb(id).map_err(|_| SessionEnd::Halt(HaltReason::Corrupt))?;
    let history = checked_history(&id, &hs.epoch_history).map_err(|e| {
        tracing::error!(node = %inner.cfg.identifier, error = %e, "upstream sent an invalid epoch history");
        SessionEnd::Halt(HaltReason::Corrupt)
    })?;
    let (i, h) = (inner.clone(), history.clone());
    blocking(move || i.store.adopt_identity(&id, &h)).await.map_err(|e| SessionEnd::Retry(e.to_string()))?;
    tracing::info!(node = %inner.cfg.identifier, epoch = id.epoch, "adopted new epoch from upstream");
    inner.set_ident(Some((id, history)));
    inner.reset_downstream();
    Ok(())
}

/// Tails from `last_seq + 1` and applies entries until the stream ends.
async fn tail(
    inner: &Arc<Inner>,
    c: &Client,
    hs: &pb::HandshakeResponse,
    lease: Option<Vec<u8>>,
    backoff: &mut BackoffState,
) -> SessionEnd {
    let Some((identity, _)) = inner.ident() else { return SessionEnd::Again };
    let pos = inner.store.position();
    let fetch = &inner.cfg.fetch;
    let limit = usize::try_from(hs.max_entry_bytes.max(u64::from(fetch.max_batch_bytes)))
        .unwrap_or(usize::MAX)
        .saturating_add(DECODE_SLACK);
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
    let start = pb::TailClientMessage { kind: Some(ClientKind::Start(req)) };
    let ack_stream = WatchStream::from_changes(ack_rx)
        .map(|applied_seq| pb::TailClientMessage { kind: Some(ClientKind::Ack(pb::TailAck { applied_seq })) });
    let outbound = tokio_stream::iter([start]).chain(ack_stream);
    let mut stream = match c.clone().max_decoding_message_size(limit).tail(outbound).await {
        Ok(r) => r.into_inner(),
        Err(s) => return status_end(inner, &s),
    };
    inner.set_state(SlaveState::Streaming);
    let idle = (Duration::from_millis(u64::from(hs.heartbeat_ms)) * 3).max(inner.cfg.heartbeat);
    let mut cleared = false;
    let started = Instant::now();
    loop {
        let msg = match tokio::time::timeout(idle, stream.message()).await {
            Err(_) => return SessionEnd::Retry(format!("no message from upstream for {idle:?}")),
            Ok(Err(s)) => return status_end(inner, &s),
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
                    match apply::apply(inner, entry_from_pb(e)).await {
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
