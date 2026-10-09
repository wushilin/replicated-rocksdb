//! What a slave does after a handshake answer or a failed call (PROTOCOL.md §11.2).
//! Pure, so every row of the table is unit-tested.

use tonic::{Code, Status};

use super::config::{DivergencePolicy, GapPolicy, IdentityPolicy};
use super::state::HaltReason;
use crate::net::reason_of;
use crate::pb;
use crate::pb::{Decision, Reason};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Policies {
    pub identity: IdentityPolicy,
    pub divergence: DivergencePolicy,
    pub gap: GapPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Tail from `last_seq + 1`.
    Tail,
    /// Persist the server's identity and epoch history, then tail.
    AdoptEpochThenTail,
    Bootstrap,
    Halt(HaltReason),
    /// Back off, then handshake again; the message goes to `SlaveStatus::last_error`.
    Retry(String),
    /// Handshake again at once.
    Again,
}

fn diverged(p: &Policies) -> Action {
    match p.divergence {
        DivergencePolicy::Halt => Action::Halt(HaltReason::Diverged),
        DivergencePolicy::Rebootstrap => Action::Bootstrap,
    }
}

/// The action for a handshake answer. `force_bootstrap` is set by `clear_halt(Rebootstrap)`
/// and by a gap seen mid-stream: any answer that lets the client proceed becomes a bootstrap.
pub(crate) fn after_handshake(r: &pb::HandshakeResponse, p: &Policies, force_bootstrap: bool) -> Action {
    let decision = Decision::try_from(r.decision).unwrap_or(Decision::Unspecified);
    let reason = Reason::try_from(r.reason).unwrap_or(Reason::Unspecified);
    let proceed = matches!(decision, Decision::Resume | Decision::Bootstrap)
        || (decision == Decision::Reject && reason == Reason::Diverged);
    if force_bootstrap && proceed {
        return Action::Bootstrap;
    }
    match (decision, reason) {
        (Decision::Resume, Reason::EpochAdvanced) => Action::AdoptEpochThenTail,
        (Decision::Resume, _) => Action::Tail,
        (Decision::Bootstrap, Reason::NoLocalState) => Action::Bootstrap,
        (Decision::Bootstrap, Reason::IdentityChanged) => match p.identity {
            IdentityPolicy::Refuse => Action::Halt(HaltReason::IdentityChanged),
            IdentityPolicy::Rebootstrap => Action::Bootstrap,
        },
        (Decision::Bootstrap, Reason::LogGap) => match p.gap {
            GapPolicy::Rebootstrap => Action::Bootstrap,
            GapPolicy::Halt => Action::Halt(HaltReason::LogGap),
        },
        (Decision::Reject, Reason::Diverged) => diverged(p),
        (Decision::Reject, Reason::StaleEpoch | Reason::NotReady) => {
            Action::Retry(format!("{}: {}", reason.as_str_name(), r.detail))
        }
        (Decision::Reject, Reason::ProtocolMismatch) => Action::Halt(HaltReason::ProtocolMismatch),
        (Decision::Reject, Reason::EnvelopeMismatch) => Action::Halt(HaltReason::EnvelopeMismatch),
        (Decision::Reject, Reason::LoopDetected) => Action::Halt(HaltReason::LoopDetected),
        (d, r) => Action::Retry(format!("unexpected handshake answer {}/{}", d.as_str_name(), r.as_str_name())),
    }
}

/// The action for a failed RPC or a stream that ended with an error.
pub(crate) fn after_status(s: &Status, p: &Policies) -> Action {
    if matches!(s.code(), Code::Unauthenticated | Code::PermissionDenied) {
        return Action::Halt(HaltReason::AuthRejected);
    }
    match reason_of(s) {
        Some(Reason::ProtocolMismatch) => Action::Halt(HaltReason::ProtocolMismatch),
        Some(Reason::EnvelopeMismatch) => Action::Halt(HaltReason::EnvelopeMismatch),
        Some(Reason::LoopDetected) => Action::Halt(HaltReason::LoopDetected),
        Some(Reason::Diverged) => diverged(p),
        // The next handshake decides (LOG_GAP and identity changes then follow policy).
        Some(Reason::LogGap | Reason::IdentityChanged | Reason::UpstreamReset) => Action::Again,
        _ => Action::Retry(format!("{:?}: {}", s.code(), s.message())),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::reason_status;

    const DEFAULTS: Policies = Policies {
        identity: IdentityPolicy::Refuse,
        divergence: DivergencePolicy::Halt,
        gap: GapPolicy::Rebootstrap,
    };
    const OPPOSITE: Policies = Policies {
        identity: IdentityPolicy::Rebootstrap,
        divergence: DivergencePolicy::Rebootstrap,
        gap: GapPolicy::Halt,
    };

    fn answer(d: Decision, r: Reason) -> pb::HandshakeResponse {
        pb::HandshakeResponse { decision: d as i32, reason: r as i32, detail: "why".into(), ..Default::default() }
    }

    fn act(d: Decision, r: Reason, p: &Policies) -> Action {
        after_handshake(&answer(d, r), p, false)
    }

    #[test]
    fn resume_rows() {
        assert_eq!(act(Decision::Resume, Reason::Unspecified, &DEFAULTS), Action::Tail);
        assert_eq!(act(Decision::Resume, Reason::EpochAdvanced, &DEFAULTS), Action::AdoptEpochThenTail);
    }

    #[test]
    fn bootstrap_rows_follow_policies() {
        assert_eq!(act(Decision::Bootstrap, Reason::NoLocalState, &DEFAULTS), Action::Bootstrap);
        assert_eq!(act(Decision::Bootstrap, Reason::IdentityChanged, &DEFAULTS), Action::Halt(HaltReason::IdentityChanged));
        assert_eq!(act(Decision::Bootstrap, Reason::IdentityChanged, &OPPOSITE), Action::Bootstrap);
        assert_eq!(act(Decision::Bootstrap, Reason::LogGap, &DEFAULTS), Action::Bootstrap);
        assert_eq!(act(Decision::Bootstrap, Reason::LogGap, &OPPOSITE), Action::Halt(HaltReason::LogGap));
    }

    #[test]
    fn reject_rows() {
        assert_eq!(act(Decision::Reject, Reason::Diverged, &DEFAULTS), Action::Halt(HaltReason::Diverged));
        assert_eq!(act(Decision::Reject, Reason::Diverged, &OPPOSITE), Action::Bootstrap);
        assert_eq!(act(Decision::Reject, Reason::StaleEpoch, &DEFAULTS), Action::Retry("STALE_EPOCH: why".into()));
        assert_eq!(act(Decision::Reject, Reason::NotReady, &DEFAULTS), Action::Retry("NOT_READY: why".into()));
        assert_eq!(act(Decision::Reject, Reason::ProtocolMismatch, &DEFAULTS), Action::Halt(HaltReason::ProtocolMismatch));
        assert_eq!(act(Decision::Reject, Reason::EnvelopeMismatch, &DEFAULTS), Action::Halt(HaltReason::EnvelopeMismatch));
        assert_eq!(act(Decision::Reject, Reason::LoopDetected, &DEFAULTS), Action::Halt(HaltReason::LoopDetected));
        assert!(matches!(act(Decision::Unspecified, Reason::Busy, &DEFAULTS), Action::Retry(_)));
    }

    #[test]
    fn forced_bootstrap_overrides_only_answers_that_let_the_client_proceed() {
        let forced = |d, r| after_handshake(&answer(d, r), &DEFAULTS, true);
        assert_eq!(forced(Decision::Resume, Reason::Unspecified), Action::Bootstrap);
        assert_eq!(forced(Decision::Bootstrap, Reason::IdentityChanged), Action::Bootstrap);
        assert_eq!(forced(Decision::Reject, Reason::Diverged), Action::Bootstrap);
        assert!(matches!(forced(Decision::Reject, Reason::StaleEpoch), Action::Retry(_)));
        assert_eq!(forced(Decision::Reject, Reason::LoopDetected), Action::Halt(HaltReason::LoopDetected));
    }

    #[test]
    fn status_rows() {
        let s = |r| reason_status(r, "x");
        assert_eq!(after_status(&Status::unauthenticated("x"), &DEFAULTS), Action::Halt(HaltReason::AuthRejected));
        assert_eq!(after_status(&Status::permission_denied("x"), &DEFAULTS), Action::Halt(HaltReason::AuthRejected));
        assert_eq!(after_status(&s(Reason::ProtocolMismatch), &DEFAULTS), Action::Halt(HaltReason::ProtocolMismatch));
        assert_eq!(after_status(&s(Reason::Diverged), &DEFAULTS), Action::Halt(HaltReason::Diverged));
        assert_eq!(after_status(&s(Reason::Diverged), &OPPOSITE), Action::Bootstrap);
        for r in [Reason::LogGap, Reason::IdentityChanged, Reason::UpstreamReset] {
            assert_eq!(after_status(&s(r), &DEFAULTS), Action::Again, "{r:?}");
        }
        for r in [Reason::NotReady, Reason::Busy, Reason::ShuttingDown, Reason::Corrupt] {
            assert!(matches!(after_status(&s(r), &DEFAULTS), Action::Retry(_)), "{r:?}");
        }
        assert!(matches!(after_status(&Status::unavailable("connection refused"), &DEFAULTS), Action::Retry(_)));
    }
}
