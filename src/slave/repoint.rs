//! Switching upstream at runtime (DESIGN.md §13.1).

use std::ops::RangeInclusive;
use std::sync::atomic::Ordering;

use tokio::sync::oneshot;

use super::client::connect_upstream;
use super::driver::SessionEnd;
use super::inner::Inner;
use super::state::SlaveState;
use super::{Slave, UpstreamConfig};
use crate::admin::fork_between;
use crate::error::{Error, Result};
use crate::net::convert::epochs_from_pb;
use crate::pb;
use crate::types::{EpochStart, Seq};

/// Acknowledges that an operation discards data. A caller constructs it explicitly
/// (`AcceptDataLoss`) to opt in; nothing in the crate supplies one by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptDataLoss;

/// What a repoint does when the new upstream answers `DIVERGED`. The slave's configured
/// `on_divergence` policy is not used after an explicit repoint: the repoint's mode decides
/// every `DIVERGED` answer until the next repoint or a restart.
///
/// **Consent to lose data is single-use: it lasts until its own repoint settles.**
/// `Rebootstrap` and `Repair` apply only while their repoint is in progress. A repoint settles
/// when its first handshake is answered; with `Repair`, when the repair that answer started has
/// committed (`Repaired`), failed (`Retrying`), or fallen back to a bootstrap from the same
/// upstream (`Bootstrapped`). Any outcome settles it, including `Retrying` and an error; the
/// mode then becomes `Refuse`. A `DIVERGED` that arrives later —
/// from the Tail or Bootstrap that follows the handshake, or from any later session — halts the
/// slave with its data intact instead of discarding it again. Repoint again with the consent to
/// apply it once more.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RepointDivergence {
    /// `repoint` returns `Error::Diverged`; the slave halts `Diverged` with its data intact
    /// and readable.
    #[default]
    Refuse,
    /// Discard only the divergent suffix (DESIGN.md §12.1): the keys it touched are taken from
    /// the new upstream, the suffix is dropped, and the slave replays the upstream's history
    /// (`Repaired { discarded }`; reads answer `NotReady` until replay reaches the upstream's
    /// snapshot seq). When repair cannot be proven safe (a CF operation in the suffix, entries
    /// no longer retained, too much to fetch), the slave bootstraps from the new upstream
    /// instead (`Bootstrapped`).
    Repair { accept_loss: AcceptDataLoss },
    /// Discard all local data and bootstrap from the new upstream, if this repoint's upstream
    /// answers `DIVERGED` before the repoint settles.
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
    /// The divergent suffix `discarded` was rolled back and the keys it touched were taken
    /// from the new upstream (`RepointDivergence::Repair`). Reads answer `NotReady` until the
    /// slave has replayed up to the upstream's snapshot seq.
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
        *self
            .repoint_divergence
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Settles the repoint that session `generation` was started for (no-op if there is none).
    /// Its consent to lose data (`Rebootstrap`, `Repair`) ends here: later `DIVERGED` answers
    /// are refused.
    pub(crate) fn settle_repoint(&self, generation: u64, result: Result<RepointOutcome>) {
        let mut pending = self.pending_repoint();
        if pending.as_ref().is_some_and(|p| p.generation == generation) {
            let p = pending.take().expect("checked");
            // Under the `pending` lock, so a newer repoint's mode (set under it too) is never
            // overwritten.
            let mut mode = self
                .repoint_divergence
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            if matches!(
                *mode,
                Some(RepointDivergence::Rebootstrap { .. } | RepointDivergence::Repair { .. })
            ) {
                tracing::info!(node = %self.cfg.identifier, "repoint settled; its consent to discard data has ended");
                *mode = Some(RepointDivergence::Refuse);
            }
            drop(mode);
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
        if let Some(p) = self
            .pending_repoint()
            .as_mut()
            .filter(|p| p.generation == generation)
        {
            p.halt_error = Some(e);
        }
    }

    /// Settles a repoint from the way its session ended: a halt (called once it is in
    /// place) is an error, a retry is `Retrying`, and `Again` waits for the next handshake.
    pub(crate) fn settle_repoint_end(&self, generation: u64, end: &SessionEnd) {
        match end {
            SessionEnd::Halt(r) => {
                let e = self
                    .pending_repoint()
                    .as_mut()
                    .and_then(|p| p.halt_error.take());
                self.settle_repoint(generation, Err(e.unwrap_or(Error::Halted(*r))));
            }
            SessionEnd::Retry(m) => self.settle_repoint(
                generation,
                Ok(RepointOutcome::Retrying {
                    last_error: m.clone(),
                }),
            ),
            SessionEnd::Again => {}
        }
    }
}

/// Where the local log, whose full epoch history is `local`, leaves the upstream's after a
/// `DIVERGED` answer `hs`: the first local entry the upstream does not share. The one
/// computation behind both `Error::Diverged::fork_seq` and the fork point recorded for
/// `Slave::diverged_entries`.
pub(crate) fn fork_seq(local: &[EpochStart], local_last: Seq, hs: &pb::HandshakeResponse) -> Seq {
    let history = epochs_from_pb(&hs.epoch_history);
    match fork_between(local, &history) {
        // Rule 8: everything from the next epoch's start (or from the start of a same-numbered
        // epoch with another nonce) is not on the upstream's history.
        Some(fork) if fork <= local_last => fork,
        // Rule 11: the local log runs past the master's.
        _ if local_last > hs.last_seq => hs.last_seq + 1,
        // Rule 12: the last entry differs; earlier ones may too (repair finds out).
        _ => local_last,
    }
}

/// `Error::Diverged` for a `DIVERGED` answer: where the local log, whose full epoch history is
/// `local`, leaves the upstream's (the first record where the two histories differ, exactly:
/// [`fork_between`]).
pub(crate) fn divergence(
    local: &[EpochStart],
    local_last: Seq,
    hs: &pb::HandshakeResponse,
) -> Error {
    let fork_seq = fork_seq(local, local_last, hs);
    Error::Diverged {
        fork_seq,
        local_last_seq: local_last,
        divergent_entries: local_last + 1 - fork_seq.min(local_last + 1),
    }
}

impl Slave {
    /// The checks `repoint` runs before it adopts `upstream`: `Err(Config)` for an address
    /// (or TLS configuration) it would refuse. Nothing else answers `Config`.
    pub(crate) fn check_upstream(&self, upstream: &UpstreamConfig) -> Result<()> {
        if upstream.server.is_empty() || upstream.port == 0 {
            return Err(Error::Config(
                "upstream server and port are required".into(),
            ));
        }
        connect_upstream(
            upstream,
            &self.inner.cfg.identifier,
            self.inner.cfg.connect_timeout,
        )?;
        Ok(())
    }

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
    /// - `Ok(Resumed | EpochAdvanced | Bootstrapped | Repaired { .. })`: replicating from the
    ///   new upstream.
    /// - `Ok(Retrying { .. })`: the new upstream cannot be used yet (unreachable,
    ///   `STALE_EPOCH`, `NOT_READY`); the slave keeps retrying it.
    /// - `Err(Diverged { .. })` (with `RepointDivergence::Refuse`, the default): the local log
    ///   holds entries the new upstream does not. **Divergence is never resolved
    ///   implicitly:** the slave halts `Diverged`, its data intact and readable. Discarding
    ///   the divergent entries takes an explicit [`AcceptDataLoss`]:
    ///   `RepointDivergence::Rebootstrap { accept_loss: AcceptDataLoss }`.
    ///   `RepointDivergence::Repair { accept_loss: AcceptDataLoss }` discards only the
    ///   divergent suffix and returns `Ok(Repaired { discarded })` (or `Ok(Bootstrapped)` when
    ///   repair is not possible, `Ok(Retrying)` when it failed for a transient reason).
    ///   The consent is single-use: it covers `DIVERGED` answers until this repoint settles
    ///   (this call returns), then the mode reverts to `Refuse` (see [`RepointDivergence`]).
    /// - `Err(Halted(LoopDetected))`: the new upstream is this slave or replicates from it;
    ///   the slave halts until it is repointed again.
    /// - `Err(RepointSuperseded)`: a later repoint replaced this one (or the slave shut down)
    ///   before the handshake settled.
    /// - `Err(Halted(reason))` for any other halt, `Err(Config)` for an invalid address.
    ///
    /// The wait includes a whole bootstrap when one is needed. To bound it, wrap the call in
    /// `tokio::time::timeout`: dropping the future does not undo the repoint, the slave keeps
    /// following the new upstream.
    pub async fn repoint(
        &self,
        upstream: UpstreamConfig,
        opts: RepointOptions,
    ) -> Result<RepointOutcome> {
        self.check_upstream(&upstream)?;
        let (reply, settled) = oneshot::channel();
        {
            let mut pending = self.inner.pending_repoint();
            tracing::info!(node = %self.inner.cfg.identifier, upstream = %upstream.uri(), "repointing");
            self.inner.set_upstream(upstream);
            self.inner.clear_upstream_chain();
            *self
                .inner
                .repoint_divergence
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(opts.on_divergence);
            let generation = *self.inner.repoint_tx.borrow() + 1;
            *pending = Some(PendingRepoint {
                generation,
                halt_error: None,
                reply,
            });
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
