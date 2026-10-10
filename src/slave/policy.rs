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

/// What asked for a bootstrap before this handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Force {
    None,
    /// The operator (`clear_halt(Rebootstrap)`, a gap seen mid-stream): overrides refusals.
    Operator,
    /// A verify run found mismatches under `MismatchPolicy::Rebootstrap`, against this very
    /// upstream and database (the caller checked). Consent to re-copy, not to lose data on
    /// divergence or a foreign database: only RESUME and BOOTSTRAP/{NO_LOCAL_STATE, LOG_GAP}
    /// answers become a bootstrap; anything else follows the normal table.
    Verify,
}

/// The action for a handshake answer. `Force::Operator` is set by `clear_halt(Rebootstrap)`
/// and by a gap seen mid-stream: any answer that lets the client proceed (RESUME, BOOTSTRAP,
/// or REJECT/DIVERGED) becomes a bootstrap. A pending force therefore overrides
/// `IdentityPolicy::Refuse` and `DivergencePolicy::Halt`; that is intended, because the
/// operator asked for it through `clear_halt`. Rejections that retrying or reconfiguring
/// cannot fix (protocol, envelope, loop, stale epoch, not ready) are not overridden.
pub(crate) fn after_handshake(r: &pb::HandshakeResponse, p: &Policies, force: Force) -> Action {
    let decision = Decision::try_from(r.decision).unwrap_or(Decision::Unspecified);
    let reason = Reason::try_from(r.reason).unwrap_or(Reason::Unspecified);
    let proceed = matches!(decision, Decision::Resume | Decision::Bootstrap)
        || (decision == Decision::Reject && reason == Reason::Diverged);
    if force == Force::Operator && proceed {
        return Action::Bootstrap;
    }
    if force == Force::Verify
        && (decision == Decision::Resume
            || (decision == Decision::Bootstrap
                && matches!(reason, Reason::NoLocalState | Reason::LogGap)))
    {
        return Action::Bootstrap;
    }
    let unexpected = || {
        Action::Retry(format!(
            "unexpected handshake answer {}/{}",
            decision.as_str_name(),
            reason.as_str_name()
        ))
    };
    match decision {
        // Any reason on RESUME is informational, except an advanced epoch.
        Decision::Resume => match reason {
            Reason::EpochAdvanced => Action::AdoptEpochThenTail,
            _ => Action::Tail,
        },
        Decision::Bootstrap => match reason {
            Reason::NoLocalState => Action::Bootstrap,
            Reason::IdentityChanged => match p.identity {
                IdentityPolicy::Refuse => Action::Halt(HaltReason::IdentityChanged),
                IdentityPolicy::Rebootstrap => Action::Bootstrap,
            },
            Reason::LogGap => match p.gap {
                GapPolicy::Rebootstrap => Action::Bootstrap,
                GapPolicy::Halt => Action::Halt(HaltReason::LogGap),
            },
            Reason::Unspecified
            | Reason::ProtocolMismatch
            | Reason::EnvelopeMismatch
            | Reason::NotReady
            | Reason::LoopDetected
            | Reason::Diverged
            | Reason::Busy
            | Reason::SessionNotFound
            | Reason::UpstreamReset
            | Reason::Corrupt
            | Reason::ShuttingDown
            | Reason::EpochAdvanced
            | Reason::StaleEpoch => unexpected(),
        },
        Decision::Reject => match reason {
            Reason::Diverged => diverged(p),
            Reason::StaleEpoch | Reason::NotReady => {
                Action::Retry(format!("{}: {}", reason.as_str_name(), r.detail))
            }
            Reason::ProtocolMismatch => Action::Halt(HaltReason::ProtocolMismatch),
            Reason::EnvelopeMismatch => Action::Halt(HaltReason::EnvelopeMismatch),
            Reason::LoopDetected => Action::Halt(HaltReason::LoopDetected),
            Reason::Unspecified
            | Reason::NoLocalState
            | Reason::IdentityChanged
            | Reason::LogGap
            | Reason::Busy
            | Reason::SessionNotFound
            | Reason::UpstreamReset
            | Reason::Corrupt
            | Reason::ShuttingDown
            | Reason::EpochAdvanced => unexpected(),
        },
        Decision::Unspecified => unexpected(),
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
        Some(
            Reason::Unspecified
            | Reason::NoLocalState
            | Reason::NotReady
            | Reason::Busy
            | Reason::SessionNotFound
            | Reason::Corrupt
            | Reason::ShuttingDown
            | Reason::EpochAdvanced
            | Reason::StaleEpoch,
        )
        | None => Action::Retry(format!("{:?}: {}", s.code(), s.message())),
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
        pb::HandshakeResponse {
            decision: d as i32,
            reason: r as i32,
            detail: "why".into(),
            ..Default::default()
        }
    }

    fn act(d: Decision, r: Reason, p: &Policies) -> Action {
        after_handshake(&answer(d, r), p, Force::None)
    }

    #[test]
    fn resume_rows() {
        assert_eq!(
            act(Decision::Resume, Reason::Unspecified, &DEFAULTS),
            Action::Tail
        );
        assert_eq!(
            act(Decision::Resume, Reason::EpochAdvanced, &DEFAULTS),
            Action::AdoptEpochThenTail
        );
    }

    #[test]
    fn bootstrap_rows_follow_policies() {
        assert_eq!(
            act(Decision::Bootstrap, Reason::NoLocalState, &DEFAULTS),
            Action::Bootstrap
        );
        assert_eq!(
            act(Decision::Bootstrap, Reason::IdentityChanged, &DEFAULTS),
            Action::Halt(HaltReason::IdentityChanged)
        );
        assert_eq!(
            act(Decision::Bootstrap, Reason::IdentityChanged, &OPPOSITE),
            Action::Bootstrap
        );
        assert_eq!(
            act(Decision::Bootstrap, Reason::LogGap, &DEFAULTS),
            Action::Bootstrap
        );
        assert_eq!(
            act(Decision::Bootstrap, Reason::LogGap, &OPPOSITE),
            Action::Halt(HaltReason::LogGap)
        );
    }

    #[test]
    fn reject_rows() {
        assert_eq!(
            act(Decision::Reject, Reason::Diverged, &DEFAULTS),
            Action::Halt(HaltReason::Diverged)
        );
        assert_eq!(
            act(Decision::Reject, Reason::Diverged, &OPPOSITE),
            Action::Bootstrap
        );
        assert_eq!(
            act(Decision::Reject, Reason::StaleEpoch, &DEFAULTS),
            Action::Retry("STALE_EPOCH: why".into())
        );
        assert_eq!(
            act(Decision::Reject, Reason::NotReady, &DEFAULTS),
            Action::Retry("NOT_READY: why".into())
        );
        assert_eq!(
            act(Decision::Reject, Reason::ProtocolMismatch, &DEFAULTS),
            Action::Halt(HaltReason::ProtocolMismatch)
        );
        assert_eq!(
            act(Decision::Reject, Reason::EnvelopeMismatch, &DEFAULTS),
            Action::Halt(HaltReason::EnvelopeMismatch)
        );
        assert_eq!(
            act(Decision::Reject, Reason::LoopDetected, &DEFAULTS),
            Action::Halt(HaltReason::LoopDetected)
        );
        assert!(matches!(
            act(Decision::Unspecified, Reason::Busy, &DEFAULTS),
            Action::Retry(_)
        ));
    }

    #[test]
    fn forced_bootstrap_overrides_only_answers_that_let_the_client_proceed() {
        let forced = |d, r| after_handshake(&answer(d, r), &DEFAULTS, Force::Operator);
        assert_eq!(
            forced(Decision::Resume, Reason::Unspecified),
            Action::Bootstrap
        );
        assert_eq!(
            forced(Decision::Bootstrap, Reason::IdentityChanged),
            Action::Bootstrap
        );
        assert_eq!(
            forced(Decision::Reject, Reason::Diverged),
            Action::Bootstrap
        );
        assert!(matches!(
            forced(Decision::Reject, Reason::StaleEpoch),
            Action::Retry(_)
        ));
        assert_eq!(
            forced(Decision::Reject, Reason::LoopDetected),
            Action::Halt(HaltReason::LoopDetected)
        );
    }

    /// A verify's rebootstrap re-copies from the verified upstream. It turns RESUME,
    /// EPOCH_ADVANCED and same-identity BOOTSTRAP answers into a bootstrap, and never overrides
    /// DIVERGED or IDENTITY_CHANGED: those follow the configured policy (halt / refuse by default).
    #[test]
    fn a_verify_rebootstrap_never_overrides_divergence_or_a_foreign_identity() {
        use Decision::{Bootstrap, Reject, Resume};
        for p in [&DEFAULTS, &OPPOSITE] {
            let verify = |d, r| after_handshake(&answer(d, r), p, Force::Verify);
            let normal = |d, r| after_handshake(&answer(d, r), p, Force::None);
            for (d, r) in [
                (Resume, Reason::Unspecified),
                (Resume, Reason::EpochAdvanced),
                (Bootstrap, Reason::NoLocalState),
                (Bootstrap, Reason::LogGap),
            ] {
                assert_eq!(verify(d, r), Action::Bootstrap, "{d:?}/{r:?}");
            }
            // Everything else follows the normal table, row by row.
            for (d, r) in [
                (Bootstrap, Reason::IdentityChanged),
                (Reject, Reason::Diverged),
                (Reject, Reason::StaleEpoch),
                (Reject, Reason::NotReady),
                (Reject, Reason::ProtocolMismatch),
                (Reject, Reason::EnvelopeMismatch),
                (Reject, Reason::LoopDetected),
                (Decision::Unspecified, Reason::Unspecified),
            ] {
                assert_eq!(verify(d, r), normal(d, r), "{d:?}/{r:?}");
            }
        }
        let verify = |d, r| after_handshake(&answer(d, r), &DEFAULTS, Force::Verify);
        assert_eq!(
            verify(Reject, Reason::Diverged),
            Action::Halt(HaltReason::Diverged)
        );
        assert_eq!(
            verify(Bootstrap, Reason::IdentityChanged),
            Action::Halt(HaltReason::IdentityChanged)
        );
    }

    /// N1: a verify rebootstrap converts only an explicit allowlist of BOOTSTRAP reasons; a
    /// malformed or unknown combination follows the normal table.
    #[test]
    fn a_verify_rebootstrap_converts_only_the_need_data_bootstrap_reasons() {
        for p in [&DEFAULTS, &OPPOSITE] {
            for r in [
                Reason::Unspecified,
                Reason::Diverged,
                Reason::StaleEpoch,
                Reason::EpochAdvanced,
                Reason::NotReady,
                Reason::Busy,
                Reason::Corrupt,
                Reason::UpstreamReset,
            ] {
                assert_eq!(
                    after_handshake(&answer(Decision::Bootstrap, r), p, Force::Verify),
                    after_handshake(&answer(Decision::Bootstrap, r), p, Force::None),
                    "BOOTSTRAP/{r:?}"
                );
            }
        }
    }

    #[test]
    fn status_rows() {
        let s = |r| reason_status(r, "x");
        assert_eq!(
            after_status(&Status::unauthenticated("x"), &DEFAULTS),
            Action::Halt(HaltReason::AuthRejected)
        );
        assert_eq!(
            after_status(&Status::permission_denied("x"), &DEFAULTS),
            Action::Halt(HaltReason::AuthRejected)
        );
        assert_eq!(
            after_status(&s(Reason::ProtocolMismatch), &DEFAULTS),
            Action::Halt(HaltReason::ProtocolMismatch)
        );
        assert_eq!(
            after_status(&s(Reason::Diverged), &DEFAULTS),
            Action::Halt(HaltReason::Diverged)
        );
        assert_eq!(
            after_status(&s(Reason::Diverged), &OPPOSITE),
            Action::Bootstrap
        );
        for r in [
            Reason::LogGap,
            Reason::IdentityChanged,
            Reason::UpstreamReset,
        ] {
            assert_eq!(after_status(&s(r), &DEFAULTS), Action::Again, "{r:?}");
        }
        for r in [
            Reason::NotReady,
            Reason::Busy,
            Reason::ShuttingDown,
            Reason::Corrupt,
        ] {
            assert!(
                matches!(after_status(&s(r), &DEFAULTS), Action::Retry(_)),
                "{r:?}"
            );
        }
        assert!(matches!(
            after_status(&Status::unavailable("connection refused"), &DEFAULTS),
            Action::Retry(_)
        ));
    }
}
