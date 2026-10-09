//! Switching upstream at runtime (DESIGN.md §13.1).

use std::ops::RangeInclusive;
use std::sync::atomic::Ordering;

use tokio::sync::oneshot;

use super::client::connect_upstream;
use super::driver::SessionEnd;
use super::inner::Inner;
use super::state::SlaveState;
use super::{Slave, UpstreamConfig};
use crate::error::{Error, Result};
use crate::pb;
use crate::types::{fork_after, EpochStart, Seq};

/// Acknowledges that an operation discards data. A caller constructs it explicitly
/// (`AcceptDataLoss`) to opt in; nothing in the crate supplies one by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptDataLoss;

/// What a repoint does when the new upstream answers `DIVERGED`. The slave's configured
/// `on_divergence` policy is not used after an explicit repoint: this mode decides every
/// `DIVERGED` answer (handshake or Tail) until the next repoint or a restart.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RepointDivergence {
    /// `repoint` returns `Error::Diverged`; the slave halts `Diverged` with its data intact
    /// and readable.
    #[default]
    Refuse,
    /// Discard only the divergent suffix and resume (DESIGN.md §22, Plan 3). **Not available in
    /// this version:** `repoint` returns `Error::Unsupported` and the slave halts
    /// `Diverged` with its data intact. It needs verify sessions (Plan 3).
    Repair { accept_loss: AcceptDataLoss },
    /// Discard all local data and bootstrap from the new upstream.
    Rebootstrap { accept_loss: AcceptDataLoss },
}

/// Options of [`Slave::repoint`]. The default refuses divergence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepointOptions {
    pub on_divergence: RepointDivergence,
}

/// How the first handshake with the new upstream settled.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepointOutcome {
    /// Same epoch: tailing from `applied_seq() + 1`.
    Resumed,
    /// The new upstream is at a later epoch and continuity was proven; tailing.
    EpochAdvanced,
    /// Local data was replaced by a bootstrap from the new upstream (no local data, an
    /// unprovable epoch change or log gap under the configured policies, or
    /// `RepointDivergence::Rebootstrap`).
    Bootstrapped,
    /// The divergent suffix `discarded` was rolled back (Plan 3; never returned by this
    /// version).
    Repaired { discarded: RangeInclusive<Seq> },
    /// The new upstream could not be used yet (unreachable, `STALE_EPOCH`, `NOT_READY`, …).
    /// The slave keeps retrying it with backoff; `last_error` says why.
    Retrying { last_error: String },
}

pub(crate) struct PendingRepoint {
    pub generation: u64,
    /// The error to report when the session ends in a halt, instead of `Halted(reason)`.
    pub halt_error: Option<Error>,
    pub reply: oneshot::Sender<Result<RepointOutcome>>,
}

impl Inner {
    /// The divergence mode of the latest repoint, if the slave was repointed since it
    /// opened. It decides every `DIVERGED` answer from then on (handshake or Tail), so the
    /// configured `on_divergence` never applies to an explicitly chosen upstream.
    pub(crate) fn repoint_mode(&self) -> Option<RepointDivergence> {
        *self.repoint_divergence.lock().unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Settles the repoint that session `generation` was started for (no-op if there is none).
    pub(crate) fn settle_repoint(&self, generation: u64, result: Result<RepointOutcome>) {
        let mut pending = self.pending_repoint();
        if pending.as_ref().is_some_and(|p| p.generation == generation) {
            let p = pending.take().expect("checked");
            if result.is_ok() {
                // Downstream nodes learn the new upstream chain from a fresh handshake.
                self.reset_downstream();
            }
            let _ = p.reply.send(result);
        }
    }

    /// Makes the repoint of session `generation` fail with `e` once the session's halt is in
    /// place.
    pub(crate) fn fail_repoint_on_halt(&self, generation: u64, e: Error) {
        if let Some(p) = self.pending_repoint().as_mut().filter(|p| p.generation == generation) {
            p.halt_error = Some(e);
        }
    }

    /// Settles a repoint from the way its session ended: a halt (called once it is in
    /// place) is an error, a retry is `Retrying`, and `Again` waits for the next handshake.
    pub(crate) fn settle_repoint_end(&self, generation: u64, end: &SessionEnd) {
        match end {
            SessionEnd::Halt(r) => {
                let e = self.pending_repoint().as_mut().and_then(|p| p.halt_error.take());
                self.settle_repoint(generation, Err(e.unwrap_or(Error::Halted(*r))));
            }
            SessionEnd::Retry(m) => self.settle_repoint(generation, Ok(RepointOutcome::Retrying { last_error: m.clone() })),
            SessionEnd::Again => {}
        }
    }
}

/// `Error::Diverged` for a `DIVERGED` answer: where the local log leaves the upstream's.
pub(crate) fn divergence(local_epoch: u64, local_last: Seq, hs: &pb::HandshakeResponse) -> Error {
    let history: Vec<EpochStart> =
        hs.epoch_history.iter().map(|e| EpochStart { epoch: e.epoch, start_seq: e.start_seq }).collect();
    let fork_seq = match fork_after(&history, local_epoch) {
        // Rule 8: everything from the next epoch's start is not on the upstream's history.
        Some(fork) if fork <= local_last => fork,
        // Rule 11: the local log runs past the master's.
        _ if local_last > hs.last_seq => hs.last_seq + 1,
        // Rule 12: the last entry differs; earlier ones may too (repair finds out).
        _ => local_last,
    };
    Error::Diverged { fork_seq, local_last_seq: local_last, divergent_entries: local_last + 1 - fork_seq.min(local_last + 1) }
}

/// The error a repoint reports when `mode` halts on divergence: `diverged` for `Refuse`,
/// `Unsupported` for `Repair`.
pub(crate) fn mode_error(mode: RepointDivergence, diverged: impl FnOnce() -> Error) -> Error {
    match mode {
        RepointDivergence::Repair { .. } => Error::Unsupported("divergence repair needs verify sessions (Plan 3)".into()),
        _ => diverged(),
    }
}

impl Slave {
    /// Switches this slave to another upstream at runtime, without reopening; reads keep
    /// being served throughout. The new upstream is not persisted: after a restart, open
    /// the slave with it. Likewise a halt is cleared in memory only, and `ready` is persisted
    /// once the new upstream is accepted: if the process restarts before that, the slave
    /// reopens still halted (never past its divergence refusal) and the operator repoints again.
    ///
    /// The current upstream stream is closed (a bootstrap in progress is abandoned and
    /// starts from scratch against the new upstream), a halt is cleared, and the new
    /// upstream is handshaken under all the normal rules (loop detection, `STALE_EPOCH`,
    /// epoch continuity). `repoint` returns once that first handshake settles:
    ///
    /// - `Ok(Resumed | EpochAdvanced | Bootstrapped)`: replicating from the new upstream.
    /// - `Ok(Retrying { .. })`: the new upstream cannot be used yet (unreachable,
    ///   `STALE_EPOCH`, `NOT_READY`); the slave keeps retrying it.
    /// - `Err(Diverged { .. })` (with `RepointDivergence::Refuse`, the default): the local log
    ///   holds entries the new upstream does not. **Divergence is never resolved
    ///   implicitly:** the slave halts `Diverged`, its data intact and readable. Discarding
    ///   the divergent entries takes an explicit [`AcceptDataLoss`]:
    ///   `RepointDivergence::Rebootstrap { accept_loss: AcceptDataLoss }`.
    ///   `RepointDivergence::Repair` returns `Err(Unsupported)` in this version (it needs
    ///   Plan 3's verify sessions) and also leaves the slave halted with its data intact.
    /// - `Err(Halted(LoopDetected))`: the new upstream is this slave or replicates from it;
    ///   the slave halts until it is repointed again.
    /// - `Err(RepointSuperseded)`: a later repoint replaced this one (or the slave shut down)
    ///   before the handshake settled.
    /// - `Err(Halted(reason))` for any other halt, `Err(Config)` for an invalid address.
    ///
    /// The wait includes a whole bootstrap when one is needed. To bound it, wrap the call in
    /// `tokio::time::timeout`: dropping the future does not undo the repoint, the slave keeps
    /// following the new upstream.
    pub async fn repoint(&self, upstream: UpstreamConfig, opts: RepointOptions) -> Result<RepointOutcome> {
        if upstream.server.is_empty() || upstream.port == 0 {
            return Err(Error::Config("upstream server and port are required".into()));
        }
        connect_upstream(&upstream, &self.inner.cfg.identifier, self.inner.cfg.connect_timeout)?;
        let (reply, settled) = oneshot::channel();
        {
            let mut pending = self.inner.pending_repoint();
            tracing::info!(node = %self.inner.cfg.identifier, upstream = %upstream.uri(), "repointing");
            self.inner.set_upstream(upstream);
            self.inner.clear_upstream_chain();
            *self.inner.repoint_divergence.lock().unwrap_or_else(std::sync::PoisonError::into_inner) = Some(opts.on_divergence);
            let generation = *self.inner.repoint_tx.borrow() + 1;
            *pending = Some(PendingRepoint { generation, halt_error: None, reply });
            self.inner.repoint_tx.send_replace(generation);
        }
        self.inner.set_last_error(None);
        if let SlaveState::Halted(reason) = self.inner.state() {
            // Cleared in memory only; `ready` is persisted once the new upstream is accepted, so a
            // restart before that reopens halted.
            tracing::info!(node = %self.inner.cfg.identifier, reason = reason.name(), "halt cleared by repoint");
            self.inner.halt_clear_pending.store(true, Ordering::SeqCst);
            self.inner.set_state(SlaveState::Connecting);
            self.inner.halt_cleared.notify_one();
        }
        // The sender is dropped when a later repoint supersedes this one or the slave shuts down.
        settled.await.unwrap_or(Err(Error::RepointSuperseded))
    }
}
