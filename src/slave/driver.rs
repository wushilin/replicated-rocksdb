//! The replication driver: one task per slave that connects, handshakes and acts on the
//! answer, forever (PROTOCOL.md §11.3).

use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{Notify, watch};
use tokio::task::JoinHandle;
use tokio_stream::StreamExt;
use tokio_stream::wrappers::WatchStream;
use tracing::Instrument;

use tonic::Status;

use super::apply::{self, Applied};
use super::bootstrap;
use super::client::{self, Client};
use super::config::BackoffState;
use super::inner::Inner;
use super::policy::{self, Action, AutoCause, Force};
use super::repair::{self, RepairError};
use super::repoint::{self, RepointDivergence, RepointOutcome};
use super::state::{HaltReason, NodeState, Problem, SlaveState, UpstreamErrorKind};
use crate::background::blocking;
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::failpoint;
use crate::net::DECODE_SLACK;
use crate::net::convert::{
    checked_history, entry_from_pb, epoch_to_pb, epochs_from_pb, identity_from_pb, identity_to_pb,
};
use crate::pb;
use crate::pb::tail_client_message::Kind as ClientKind;
use crate::pb::tail_message::Kind;
use crate::store::meta::key;

/// How a session ended.
#[derive(Debug)]
pub(crate) enum SessionEnd {
    /// Back off, then handshake again.
    Retry(Problem),
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
        // A stream that ended DIVERGED under a `Rebootstrap` policy or repoint mode: consent to
        // re-copy this same database on a DIVERGED answer only, never the operator's (I2).
        // DIVERGED implies the upstream follows this slave's database. Without an identity
        // (mid-bootstrap) the next handshake answers NO_LOCAL_STATE and bootstraps anyway.
        Action::Bootstrap => {
            if let Some((id, _)) = inner.ident() {
                inner
                    .auto_rebootstrap
                    .request(generation, id.db_uuid, AutoCause::Diverged);
            }
            SessionEnd::Again
        }
        // A failed call carries no handshake to repair from: the next handshake answers
        // `DIVERGED` and repairs.
        Action::Again | Action::Tail | Action::AdoptEpochThenTail | Action::Repair => {
            SessionEnd::Again
        }
    }
}

/// How a repair that did not commit ends.
enum RepairEnd {
    /// It cannot be done: bootstrap from the upstream that answered, in this session.
    Fallback,
    /// It can never be done against this upstream as things stand ([`RepairError::Refused`]).
    Refused(String),
    End(SessionEnd),
}

/// A fallback bootstraps (from the same upstream, in this session); a transient failure is
/// retried with backoff; a refusal is for the caller to decide.
fn repair_failed(inner: &Inner, e: RepairError) -> RepairEnd {
    match e {
        RepairError::Fallback(why) => {
            tracing::warn!(node = %inner.cfg.identifier, reason = %why, "repair not possible; bootstrapping instead");
            RepairEnd::Fallback
        }
        RepairError::Retry(why) => RepairEnd::End(SessionEnd::Retry(Problem::upstream(format!(
            "repair failed: {why}"
        )))),
        RepairError::Refused(why) => RepairEnd::Refused(format!("repair refused: {why}")),
    }
}

/// While a repair replays up to `S` (DESIGN.md §12.1), the keys it took hold the upstream's
/// state at `S`. A RESUME from an upstream whose history now leaves this slave's at or below
/// `S` (it lost entries up to `S`: a restored backup, a failover to a node that had not
/// received them) can never complete that state: replaying its history would make the slave
/// ready on a mix of two histories. The answer is turned into the divergence it is, so the
/// divergence policy (or the repoint's mode) decides; a repair then falls back to a bootstrap.
fn repair_target_lost(inner: &Inner, hs: &mut pb::HandshakeResponse) {
    let Some(until) = inner.repairing() else {
        return;
    };
    if hs.decision() != pb::Decision::Resume {
        return;
    }
    let upstream = epochs_from_pb(&hs.epoch_history);
    if let Some(fork) = repair::history_fork(&inner.known_epochs(), until, &upstream) {
        tracing::warn!(node = %inner.cfg.identifier, fork, repair_until = until, "the upstream's history leaves the repair's snapshot history; treating it as diverged");
        hs.set_decision(pb::Decision::Reject);
        hs.set_reason(pb::Reason::Diverged);
        hs.detail = format!(
            "the upstream's history leaves this slave's at {fork}, below the repair target {until}"
        );
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
    let action = match policy::after_status(s, &inner.policies()) {
        Action::Retry(p) if !inner.upstream_answered() => Action::Retry(p.unanswered(s)),
        action => action,
    };
    if action == Action::Repair {
        // Under the configured `DivergencePolicy::Repair` too: a RESUME on the next handshake
        // contradicts this stream and is retried with backoff (`contradicting_resume`).
        inner.diverged_on_stream.store(true, Ordering::SeqCst);
    }
    end_for(inner, generation, action)
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
                // A session a repoint replaced must not report its upstream's problem over
                // the new one (the repoint cleared `last_error`; its session reports anew).
                if *repoints.borrow() == generation {
                    inner.set_last_error(Some(msg));
                }
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
        // `ready`, and the cleared halt's fork point forgotten, in one synced batch.
        let r = blocking(move || i.store.clear_halt_meta(&NodeState::Ready.encode())).await;
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
            Err(e) => {
                return SessionEnd::Retry(Problem::new(UpstreamErrorKind::Config, e.to_string()));
            }
        },
    };
    let req = handshake_request(inner);
    let span = tracing::info_span!(
        "handshake",
        node_id = %inner.cfg.identifier,
        upstream = %inner.upstream_uri(),
        seq = req.last_seq,
        epoch = req.known_identity.as_ref().map(|id| id.epoch),
    );
    let mut hs = match unless_repointed(
        inner,
        generation,
        c.clone().handshake(req).instrument(span.clone()),
    )
    .await
    {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(s)) => return status_end(inner, generation, &s),
        Err(end) => return end,
    };
    if hs.decision() != pb::Decision::Reject {
        adopt_upstream(inner, &hs).await;
    } else {
        inner.note_contact();
    }
    repair_target_lost(inner, &mut hs);
    let hs_uuid = hs
        .identity
        .as_ref()
        .and_then(|i| identity_from_pb(i).ok())
        .map(|i| i.db_uuid);
    let after_stream_diverged = inner.diverged_on_stream.swap(false, Ordering::SeqCst);
    let force = choose_force(
        inner.force_bootstrap.is_requested_for(generation),
        inner.auto_rebootstrap.applies(generation, hs_uuid),
        inner.verify_rebootstrap.applies(generation, hs_uuid),
        after_stream_diverged,
    );
    let mut action = policy::after_handshake(&hs, &inner.policies(), force);
    if let Action::Retry(p) = &mut action
        && hs.reason() == pb::Reason::StaleEpoch
    {
        p.message.push_str(if hs.upstream_chain.is_empty() {
            " (the upstream is a master: it never reaches this epoch; repoint elsewhere, or \
             repoint to it with AcceptDataLoss to discard this node's newer entries)"
        } else {
            " (the upstream replica may still learn this epoch; if its master never will, \
             repoint to it with AcceptDataLoss to discard this node's newer entries)"
        });
    }
    // An explicit repoint never resolves divergence by the configured policy.
    if let Some(mode) = inner.repoint_mode() {
        let rejected = |hs: &pb::HandshakeResponse, r| {
            hs.decision() == pb::Decision::Reject && hs.reason() == r
        };
        // STALE_EPOCH: this node holds a newer epoch than the upstream. A master never reaches
        // it; a replica may (it has not learned it yet) or may not (its master is older). With
        // the repoint's consent to lose data the operator chose the upstream's history: the
        // newer epoch's entries are the divergence they are, and the consented repair rewinds
        // them (if the upstream later learns that epoch, they come back from it). Without
        // consent it stays a retry, as for any client (PROTOCOL.md §6.3 rule 7).
        let consented = !matches!(mode, RepointDivergence::Refuse);
        if rejected(&hs, pb::Reason::StaleEpoch) && consented && force != Force::Operator {
            hs.set_reason(pb::Reason::Diverged);
            hs.detail = format!(
                "{}; following the upstream's older epoch, as consented",
                hs.detail
            );
        }
        let diverged = rejected(&hs, pb::Reason::Diverged);
        if diverged && force != Force::Operator {
            action = match mode {
                RepointDivergence::Rebootstrap { .. } => Action::Bootstrap,
                RepointDivergence::Repair { .. } => Action::Repair,
                RepointDivergence::Refuse => {
                    let err = repoint::divergence(
                        &inner.ident().map(|(_, h)| h).unwrap_or_default(),
                        inner.store.position().last_seq,
                        &hs,
                    );
                    tracing::warn!(node = %inner.cfg.identifier, error = %err, "repoint refused: diverged");
                    inner.fail_repoint_on_halt(generation, err);
                    Action::Halt(HaltReason::Diverged)
                }
            };
        }
    }
    // A rejected answer that the session goes on from (a consented or configured repair or
    // bootstrap from this upstream) makes it this slave's upstream like an accepted one: its
    // chain is what this slave serves downstream (loop detection, "is my upstream a master").
    if hs.decision() == pb::Decision::Reject && matches!(action, Action::Repair | Action::Bootstrap)
    {
        adopt_upstream(inner, &hs).await;
    }
    // The retry arm of the driver loop logs the warning and sets `last_error`.
    let resumes = hs.decision() == pb::Decision::Resume && force != Force::Operator;
    let contradiction = contradicting_resume(after_stream_diverged, resumes);
    // Logged with the action actually taken; a retry (repeated on every backoff) at debug.
    span.in_scope(|| {
        let (decision, reason) = (hs.decision().as_str_name(), hs.reason().as_str_name());
        match (&contradiction, &action) {
            (Some(end), _) => {
                tracing::debug!(decision, reason, action = ?end, "handshake answered")
            }
            (None, Action::Retry(_)) => {
                tracing::debug!(decision, reason, ?action, "handshake answered")
            }
            (None, _) => tracing::info!(decision, reason, ?action, "handshake answered"),
        }
    });
    if let Some(end) = contradiction {
        return end;
    }
    match action {
        Action::Tail => {
            commit_halt_clear(inner).await;
            if let Some(end) = queued_repair(inner, generation, &c, &hs, hs_uuid, backoff).await {
                return end;
            }
            inner.settle_repoint(generation, Ok(RepointOutcome::Resumed));
            tail(inner, generation, &c, &hs, None, backoff).await
        }
        Action::AdoptEpochThenTail => match adopt_epoch(inner, &hs).await {
            Ok(()) => {
                commit_halt_clear(inner).await;
                inner.settle_repoint(generation, Ok(RepointOutcome::EpochAdvanced));
                // Every master open starts an epoch: a queued repair must not wait for the
                // next unrelated session end (final review m1).
                if let Some(end) = queued_repair(inner, generation, &c, &hs, hs_uuid, backoff).await
                {
                    return end;
                }
                tail(inner, generation, &c, &hs, None, backoff).await
            }
            Err(end) => end,
        },
        Action::Bootstrap => {
            // Every bootstrap after a handshake is pinned to the database and history that
            // handshake answered for; the address may serve another one by the time the
            // Bootstrap RPC runs. The operator's `clear_halt(Rebootstrap)` is the exception:
            // its consent is broad by design and covers whatever the upstream serves.
            let expected = if force == Force::Operator {
                None
            } else if let Some(u) = hs_uuid {
                Some(u)
            } else {
                return SessionEnd::Retry(Problem::upstream(
                    "bootstrap: the handshake carried no identity",
                ));
            };
            // Test hook: the handshake decided to bootstrap; the Bootstrap RPC has not started.
            failpoint::pause("slave-before-bootstrap-rpc").await;
            bootstrap_then_tail(inner, generation, &c, &hs, expected, backoff).await
        }
        Action::Repair => {
            let fetch = repair::prepare_divergence(inner, &c, &hs);
            match run_repair(inner, generation, fetch).await {
                Ok(r) => {
                    // The rewind batch wrote `node_state = ready` itself.
                    inner.halt_clear_pending.store(false, Ordering::SeqCst);
                    inner.settle_repoint(
                        generation,
                        Ok(RepointOutcome::Repaired {
                            discarded: r.discarded,
                        }),
                    );
                    // Handshake again from the rewound position, then tail.
                    SessionEnd::Again
                }
                // The consent to discard the divergent suffix (the policy, or the repoint's
                // `Repair { accept_loss }`) covers a bootstrap from this same upstream.
                Err(RepairEnd::Fallback) => {
                    fallback_bootstrap(inner, generation, &c, &hs, hs_uuid, backoff).await
                }
                // Retrying cannot fix it: fall back the same way.
                Err(RepairEnd::Refused(why)) => {
                    tracing::warn!(node = %inner.cfg.identifier, reason = %why, "repair not possible; bootstrapping instead");
                    fallback_bootstrap(inner, generation, &c, &hs, hs_uuid, backoff).await
                }
                Err(RepairEnd::End(end)) => end,
            }
        }
        Action::Halt(HaltReason::Diverged) => {
            // Where the local suffix starts, for `Slave::diverged_entries`.
            repair::record_fork(inner, &hs).await;
            SessionEnd::Halt(HaltReason::Diverged)
        }
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Again => SessionEnd::Again,
    }
}

/// Runs the repair a verify queued (`MismatchPolicy::Repair`) when this is the upstream it
/// verified (`hs` answered RESUME from that database) and a probe from the tail loop found that
/// it can run ([`repair::probe`]): repairs are only attempted from a running stream, so one the
/// upstream cannot serve never keeps this slave from tailing. `Some(end)` ends the session:
/// repaired (handshake again from the new position), a fallback bootstrap, or a repoint.
/// `None` means tail on: a repair that cannot run now (BUSY, a timeout, an upstream behind this
/// slave, a lost session) stays queued and is probed again after a backoff, and one that can
/// never run ([`RepairError::Refused`]) is dropped with an error log and `last_error`.
async fn queued_repair(
    inner: &Arc<Inner>,
    generation: u64,
    c: &Client,
    hs: &pb::HandshakeResponse,
    hs_uuid: Option<uuid::Uuid>,
    backoff: &mut BackoffState,
) -> Option<SessionEnd> {
    if inner.pending_repair(generation, hs_uuid).is_none() || !inner.take_repair_probed(generation)
    {
        return None;
    }
    let fetch = repair::prepare_pending(inner, generation, c, hs);
    match run_repair(inner, generation, fetch).await {
        Ok(_) => Some(SessionEnd::Again),
        Err(RepairEnd::Fallback) => {
            Some(fallback_bootstrap(inner, generation, c, hs, hs_uuid, backoff).await)
        }
        Err(RepairEnd::End(SessionEnd::Retry(msg))) => {
            postpone_repair(inner, msg);
            None
        }
        Err(RepairEnd::Refused(why)) => {
            drop_repair(inner, why);
            None
        }
        Err(RepairEnd::End(end)) => Some(end),
    }
}

/// A queued repair cannot run now: it stays queued, and the next probe waits for a backoff.
fn postpone_repair(inner: &Inner, why: Problem) {
    let delay = inner.defer_repair();
    tracing::warn!(node = %inner.cfg.identifier, upstream = %inner.upstream_uri(), error = %why, retry_in = ?delay, "verify repair postponed; replication goes on");
    inner.set_last_error(Some(Problem::new(
        why.kind,
        format!("verify repair postponed: {why}"),
    )));
}

/// A queued repair can never run against this upstream as things stand: dropped. The operator
/// fixes the cause and verifies again.
fn drop_repair(inner: &Inner, why: String) {
    tracing::error!(node = %inner.cfg.identifier, upstream = %inner.upstream_uri(), error = %why, "verify repair dropped: it cannot run against this upstream; fix the cause and verify again");
    inner.set_last_error(Some(Problem::upstream(format!(
        "verify repair dropped: {why}"
    ))));
    inner.clear_pending_repair();
}

/// Bootstraps from the upstream that answered `hs`, then tails. With `expected` (every path
/// except the operator's `clear_halt(Rebootstrap)`), only from that database and a history
/// no deeper-forking than `hs` showed (see `bootstrap::install`).
async fn bootstrap_then_tail(
    inner: &Arc<Inner>,
    generation: u64,
    c: &Client,
    hs: &pb::HandshakeResponse,
    expected: Option<uuid::Uuid>,
    backoff: &mut BackoffState,
) -> SessionEnd {
    // `seq` is the upstream's last_seq at the handshake; the snapshot seq is logged inside,
    // when the header arrives.
    let span = tracing::info_span!(
        "bootstrap",
        node_id = %inner.cfg.identifier,
        upstream = %inner.upstream_uri(),
        seq = hs.last_seq,
        epoch = hs.identity.as_ref().map(|id| id.epoch),
    );
    match bootstrap::install(inner, generation, c, hs, expected)
        .instrument(span)
        .await
    {
        Ok(lease) => {
            // The bootstrap commit wrote `ready` itself.
            inner.halt_clear_pending.store(false, Ordering::SeqCst);
            inner.settle_repoint(generation, Ok(RepointOutcome::Bootstrapped));
            tail(inner, generation, c, hs, Some(lease), backoff).await
        }
        Err(end) => end,
    }
}

/// A repair that cannot be done bootstraps from the same upstream, in this session, and only
/// from the database the consent covered (the one that answered `hs`): the address may serve
/// another one by now.
async fn fallback_bootstrap(
    inner: &Arc<Inner>,
    generation: u64,
    c: &Client,
    hs: &pb::HandshakeResponse,
    hs_uuid: Option<uuid::Uuid>,
    backoff: &mut BackoffState,
) -> SessionEnd {
    // Test hook: the repair fell back; the bootstrap has not started.
    failpoint::pause("slave-repair-before-fallback").await;
    let Some(expected) = hs_uuid else {
        return SessionEnd::Retry(Problem::upstream(
            "repair fallback: the handshake carried no identity",
        ));
    };
    bootstrap_then_tail(inner, generation, c, hs, Some(expected), backoff).await
}

/// Runs a repair: the network phase `fetch` bounded by `REPAIR_TIMEOUT` and abandoned on a
/// repoint (it writes nothing), then the local commit, which always runs to completion once
/// started.
async fn run_repair(
    inner: &Arc<Inner>,
    generation: u64,
    fetch: impl Future<Output = Result<repair::Prepared, RepairError>>,
) -> Result<repair::Repaired, RepairEnd> {
    let span = repair_span(inner);
    let bounded = tokio::time::timeout(repair::REPAIR_TIMEOUT, fetch.instrument(span.clone()));
    let prepared = match unless_repointed(inner, generation, bounded).await {
        Err(end) => return Err(RepairEnd::End(end)),
        Ok(Ok(Ok(p))) => p,
        Ok(Ok(Err(e))) => return Err(repair_failed(inner, e)),
        Ok(Err(_)) => {
            return Err(RepairEnd::End(SessionEnd::Retry(Problem::new(
                UpstreamErrorKind::Unreachable,
                format!("repair: no answer within {:?}", repair::REPAIR_TIMEOUT),
            ))));
        }
    };
    repair::commit(inner, prepared)
        .instrument(span)
        .await
        .map_err(|e| repair_failed(inner, e))
}

fn repair_span(inner: &Inner) -> tracing::Span {
    tracing::info_span!(
        "repair",
        node_id = %inner.cfg.identifier,
        upstream = %inner.upstream_uri(),
        seq = inner.store.position().last_seq,
        epoch = inner.ident().map(|(id, _)| id.epoch),
    )
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
///
/// `resumes` is about the handshake answer itself (RESUME, not forced by the operator), not the
/// action: a pending verify rebootstrap must not turn the contradiction into a bootstrap.
fn contradicting_resume(after_stream_diverged: bool, resumes: bool) -> Option<SessionEnd> {
    (after_stream_diverged && resumes).then(|| SessionEnd::Retry(Problem::upstream(CONTRADICTION)))
}

/// Which pending request, if any, applies to this handshake: the operator's consent first,
/// then the slave's own (`auto`), then a verify's. Neither automatic one ever applies right after
/// a stream reported DIVERGED (that answer is for the policy or the repoint's mode to decide).
fn choose_force(
    operator: bool,
    auto: Option<AutoCause>,
    verify_applies: bool,
    after_stream_diverged: bool,
) -> Force {
    if operator {
        Force::Operator
    } else if after_stream_diverged {
        Force::None
    } else if let Some(cause) = auto {
        Force::Policy(cause)
    } else if verify_applies {
        Force::Verify
    } else {
        Force::None
    }
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
        .map_err(|e| SessionEnd::Retry(Problem::local(e.to_string())))?;
    tracing::info!(node = %inner.cfg.identifier, epoch = id.epoch, "adopted new epoch from upstream");
    inner.set_ident(Some((id, history)));
    inner.reset_downstream();
    Ok(())
}

/// Records what an upstream this session goes on with said about itself (chain, positions,
/// contact) and persists a changed `max_entry_bytes`.
async fn adopt_upstream(inner: &Arc<Inner>, hs: &pb::HandshakeResponse) {
    if let Some(max) = inner.note_upstream(hs) {
        let i = inner.clone();
        let persisted = blocking(move || {
            i.store
                .write_meta(&[(key::MAX_ENTRY_BYTES, &max.to_be_bytes())], false)
        });
        if let Err(e) = persisted.await {
            tracing::warn!(error = %e, "could not persist max_entry_bytes");
        }
    }
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
    // Test hook: about to open the Tail stream (after a handshake or a bootstrap).
    failpoint::pause("slave-before-tail-rpc").await;
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
    // A probe of a queued verify repair, run while the stream keeps going.
    let mut probe: Option<Probe<'_>> = None;
    loop {
        // A queued repair is probed when due: right away, then after each backoff.
        if probe.is_none() && inner.has_pending_repair(generation) && inner.repair_due() {
            probe = Some(Box::pin(repair::probe(inner, c, hs)));
        }
        let retry_at = inner
            .repair_retry_at()
            .filter(|_| probe.is_none() && inner.has_pending_repair(generation));
        let received = tokio::select! {
            r = unless_repointed(inner, generation, tokio::time::timeout(idle, stream.message())) => r,
            // A verify asked for a re-bootstrap (`MismatchPolicy::Rebootstrap`): handshake
            // again, which bootstraps because the request is set. A repair request
            // (`MismatchPolicy::Repair`) is probed at the top of the loop. A stale wake-up is
            // ignored.
            _ = inner.rebootstrap.notified() => {
                if inner.force_bootstrap.is_requested_for(generation)
                    || inner.auto_rebootstrap.pending_for(generation)
                    || inner.verify_rebootstrap.pending_for(generation)
                {
                    return SessionEnd::Again;
                }
                continue;
            }
            _ = sleep_until(retry_at), if retry_at.is_some() => continue,
            r = async { probe.as_mut().expect("guarded").await }, if probe.is_some() => {
                probe = None;
                match r {
                    // The repair can run: handshake again, and the RESUME runs it.
                    Ok(()) | Err(RepairError::Fallback(_)) => {
                        inner.repair_probed(generation);
                        return SessionEnd::Again;
                    }
                    Err(RepairError::Retry(msg)) => postpone_repair(inner, Problem::upstream(msg)),
                    Err(RepairError::Refused(why)) => drop_repair(inner, why),
                }
                continue;
            }
        };
        let next = match received {
            Ok(next) => next,
            Err(end) => return end,
        };
        let msg = match next {
            Err(_) => {
                return SessionEnd::Retry(Problem::new(
                    UpstreamErrorKind::Unreachable,
                    format!("no message from upstream for {idle:?}"),
                ));
            }
            Ok(Err(s)) => return status_end(inner, generation, &s),
            Ok(Ok(None)) => {
                return SessionEnd::Retry(Problem::upstream("upstream ended the tail stream"));
            }
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
                    return SessionEnd::Retry(Problem::upstream("empty entry batch from upstream"));
                }
                // Received entries prove the origin is at least this far, and the oldest
                // unapplied one gives the lag.
                if let Some(last) = batch.entries.last() {
                    inner.origin_last_seq.fetch_max(last.seq, Ordering::SeqCst);
                }
                let ts: Vec<u64> = batch.entries.iter().map(|e| e.ts_ms).collect();
                inner.pending_ts.store(ts[0], Ordering::SeqCst);
                let _clear = ClearPending(inner);
                // Each entry is its own atomic write, so a crash mid-batch leaves a clean boundary.
                for (i, e) in batch.entries.into_iter().enumerate() {
                    let next_ts = ts.get(i + 1).copied().unwrap_or(0);
                    match apply::apply(inner, generation, entry_from_pb(e), next_ts).await {
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
            None => return SessionEnd::Retry(Problem::upstream("tail message without content")),
        }
    }
}

/// A running [`repair::probe`].
type Probe<'a> = Pin<Box<dyn Future<Output = Result<(), RepairError>> + Send + 'a>>;

/// Sleeps until `at` (forever when `None`; the caller disables that branch).
async fn sleep_until(at: Option<tokio::time::Instant>) {
    match at {
        Some(at) => tokio::time::sleep_until(at).await,
        None => std::future::pending().await,
    }
}

/// Clears `pending_ts` whenever a batch ends, however it ends.
struct ClearPending<'a>(&'a Inner);

impl Drop for ClearPending<'_> {
    fn drop(&mut self) {
        self.0.pending_ts.store(0, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn resume_after_a_stream_diverged_is_retried_with_backoff() {
        assert!(
            matches!(contradicting_resume(true, true), Some(SessionEnd::Retry(m)) if m.message == CONTRADICTION)
        );
        assert!(contradicting_resume(false, true).is_none());
        assert!(contradicting_resume(true, false).is_none());
    }

    /// I5: a verify request never applies right after a stream reported DIVERGED, whatever the
    /// handshake answers; the operator's own consent is unchanged.
    #[test]
    fn a_verify_request_never_applies_after_a_diverged_stream() {
        assert_eq!(choose_force(false, None, true, false), Force::Verify);
        assert_eq!(choose_force(false, None, true, true), Force::None);
        assert_eq!(choose_force(false, None, false, false), Force::None);
        assert_eq!(choose_force(true, None, true, false), Force::Operator);
        assert_eq!(choose_force(true, None, false, true), Force::Operator);
    }

    /// Final fix wave, I2: the order is operator, then the slave's own request, then a
    /// verify's; neither automatic one applies right after a stream reported DIVERGED.
    #[test]
    fn an_automatic_request_ranks_below_the_operator() {
        let gap = Some(AutoCause::Gap);
        assert_eq!(
            choose_force(true, gap, true, false),
            Force::Operator,
            "the operator's consent wins"
        );
        assert_eq!(
            choose_force(false, gap, true, false),
            Force::Policy(AutoCause::Gap)
        );
        assert_eq!(choose_force(false, gap, true, true), Force::None);
    }

    /// Fix round 1, B-M2: a Tail stream that ends `DIVERGED` under the configured
    /// `DivergencePolicy::Repair` marks the stream, so a RESUME that contradicts it is retried
    /// with backoff instead of tailing again every 50 ms.
    #[test]
    fn a_diverged_stream_under_the_repair_policy_is_remembered() {
        let dir = tempfile::tempdir().unwrap();
        let (store, meta) = crate::store::Store::open(
            dir.path(),
            crate::types::Role::Slave,
            crate::tuning::RocksTuning::new(),
        )
        .unwrap();
        let mut cfg = crate::slave::config::SlaveConfig::new(
            "t",
            dir.path(),
            crate::slave::config::UpstreamConfig::new("127.0.0.1", 1),
        );
        cfg.on_divergence = crate::slave::config::DivergencePolicy::Repair;
        let inner = Inner::new(store, cfg, &meta).unwrap();
        let s = crate::net::reason_status(pb::Reason::Diverged, "x");
        assert!(matches!(status_end(&inner, 0, &s), SessionEnd::Again));
        assert!(inner.diverged_on_stream.load(Ordering::SeqCst));
    }

    /// M6: `max_batch_bytes = 0` is the server default (1 MiB), not 0, in the decode limit.
    #[test]
    fn the_tail_decode_limit_maps_a_zero_batch_size_to_the_default() {
        assert_eq!(tail_decode_limit(1000, 0), (1 << 20) + DECODE_SLACK);
        assert_eq!(tail_decode_limit(1000, 2 << 20), (2 << 20) + DECODE_SLACK);
        assert_eq!(tail_decode_limit(16 << 20, 0), (16 << 20) + DECODE_SLACK);
    }
}
