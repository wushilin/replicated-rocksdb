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
    /// Repair the divergence from the upstream that answered (DESIGN.md §12.1); bootstrap
    /// from it if that cannot be proven safe.
    Repair,
    Halt(HaltReason),
    /// Back off, then handshake again; the message goes to `SlaveStatus::last_error`.
    Retry(String),
    /// Handshake again at once.
    Again,
}

fn diverged(p: &Policies) -> Action {
    match p.divergence {
        DivergencePolicy::Halt => Action::Halt(HaltReason::Diverged),
        DivergencePolicy::Repair => Action::Repair,
        DivergencePolicy::Rebootstrap => Action::Bootstrap,
    }
}

/// Why the slave itself asked for a bootstrap (see `inner::AutoRebootstrap`).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum AutoCause {
    /// A gap seen mid-stream, under `GapPolicy::Rebootstrap`.
    Gap,
    /// A Tail or Bootstrap stream that ended `DIVERGED`, under `DivergencePolicy::Rebootstrap`
    /// or a repoint's `Rebootstrap` mode.
    Diverged,
}

/// What asked for a bootstrap before this handshake.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum Force {
    None,
    /// The operator's own consent to lose data (`clear_halt(Rebootstrap)`): overrides
    /// refusals.
    Operator,
    /// The slave asked for it itself, under a configured policy or a repoint's mode, against
    /// this very upstream and database (the caller checked). Consent only to what caused it:
    /// after a gap, RESUME and BOOTSTRAP/{NO_LOCAL_STATE, LOG_GAP} become a bootstrap (re-copy
    /// the same history); after a stream that ended DIVERGED, only REJECT/DIVERGED does.
    /// IDENTITY_CHANGED, and a DIVERGED after a gap, follow the normal table.
    Policy(AutoCause),
    /// A verify run found mismatches under `MismatchPolicy::Rebootstrap`, against this very
    /// upstream and database (the caller checked). Consent to re-copy, not to lose data on
    /// divergence or a foreign database: only RESUME and BOOTSTRAP/{NO_LOCAL_STATE, LOG_GAP}
    /// answers become a bootstrap; anything else follows the normal table.
    Verify,
}

/// The action for a handshake answer. Only `Force::Operator` (`clear_halt(Rebootstrap)`)
/// turns any answer that lets the client proceed (RESUME, BOOTSTRAP, or REJECT/DIVERGED) into
/// a bootstrap, overriding `IdentityPolicy::Refuse` and `DivergencePolicy::Halt`: the operator
/// asked for it. `Force::Policy` and `Force::Verify` are automatic consents and convert only
/// their own allowlist (see [`Force`]); everything else follows the table. Rejections that
/// retrying or reconfiguring cannot fix (protocol, envelope, loop, stale epoch, not ready) are
/// never overridden.
pub(crate) fn after_handshake(r: &pb::HandshakeResponse, p: &Policies, force: Force) -> Action {
    let decision = Decision::try_from(r.decision).unwrap_or(Decision::Unspecified);
    let reason = Reason::try_from(r.reason).unwrap_or(Reason::Unspecified);
    let needs_data = decision == Decision::Resume
        || (decision == Decision::Bootstrap
            && matches!(reason, Reason::NoLocalState | Reason::LogGap));
    let is_diverged = decision == Decision::Reject && reason == Reason::Diverged;
    let converted = match force {
        Force::None => false,
        Force::Operator => {
            matches!(decision, Decision::Resume | Decision::Bootstrap) || is_diverged
        }
        Force::Verify | Force::Policy(AutoCause::Gap) => needs_data,
        Force::Policy(AutoCause::Diverged) => is_diverged,
    };
    if converted {
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
        let repair = Policies {
            divergence: DivergencePolicy::Repair,
            ..DEFAULTS
        };
        assert_eq!(
            act(Decision::Reject, Reason::Diverged, &repair),
            Action::Repair
        );
        assert_eq!(
            after_handshake(
                &answer(Decision::Reject, Reason::Diverged),
                &repair,
                Force::Operator
            ),
            Action::Bootstrap,
            "the operator's clear_halt(Rebootstrap) still bootstraps"
        );
        assert_eq!(
            after_handshake(
                &answer(Decision::Reject, Reason::Diverged),
                &repair,
                Force::Verify
            ),
            Action::Repair,
            "a verify's request never overrides the divergence policy"
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

    /// Final fix wave, I2: a bootstrap the slave requested itself (`Force::Policy`) is not the
    /// operator's consent. After a gap it re-copies the same history (RESUME and need-data
    /// BOOTSTRAP answers) and after a stream that ended DIVERGED it covers only DIVERGED; it never
    /// overrides IDENTITY_CHANGED, nor a DIVERGED the configured policy does not cover.
    #[test]
    fn a_policy_request_converts_only_its_own_allowlist() {
        use Decision::{Bootstrap, Reject, Resume};
        let rows = [
            (Resume, Reason::Unspecified),
            (Resume, Reason::EpochAdvanced),
            (Bootstrap, Reason::NoLocalState),
            (Bootstrap, Reason::LogGap),
            (Bootstrap, Reason::IdentityChanged),
            (Bootstrap, Reason::Unspecified),
            (Reject, Reason::Diverged),
            (Reject, Reason::StaleEpoch),
            (Reject, Reason::NotReady),
            (Reject, Reason::ProtocolMismatch),
            (Reject, Reason::EnvelopeMismatch),
            (Reject, Reason::LoopDetected),
            (Decision::Unspecified, Reason::Unspecified),
        ];
        for p in [&DEFAULTS, &OPPOSITE] {
            let normal = |d, r| after_handshake(&answer(d, r), p, Force::None);
            for (cause, converts) in [
                (
                    AutoCause::Gap,
                    &[
                        (Resume, Reason::Unspecified),
                        (Resume, Reason::EpochAdvanced),
                        (Bootstrap, Reason::NoLocalState),
                        (Bootstrap, Reason::LogGap),
                    ][..],
                ),
                (AutoCause::Diverged, &[(Reject, Reason::Diverged)][..]),
            ] {
                let auto = |d, r| after_handshake(&answer(d, r), p, Force::Policy(cause));
                for (d, r) in rows {
                    let want = if converts.contains(&(d, r)) {
                        Action::Bootstrap
                    } else {
                        normal(d, r)
                    };
                    assert_eq!(auto(d, r), want, "{cause:?}: {d:?}/{r:?}");
                }
            }
        }
        // Under the defaults: a gap request leaves a later divergence and a foreign database to
        // the policies, which halt.
        let gap = |d, r| after_handshake(&answer(d, r), &DEFAULTS, Force::Policy(AutoCause::Gap));
        assert_eq!(
            gap(Reject, Reason::Diverged),
            Action::Halt(HaltReason::Diverged)
        );
        assert_eq!(
            gap(Bootstrap, Reason::IdentityChanged),
            Action::Halt(HaltReason::IdentityChanged)
        );
        let div =
            |d, r| after_handshake(&answer(d, r), &DEFAULTS, Force::Policy(AutoCause::Diverged));
        assert_eq!(
            div(Bootstrap, Reason::IdentityChanged),
            Action::Halt(HaltReason::IdentityChanged)
        );
        assert_eq!(div(Resume, Reason::Unspecified), Action::Tail);
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
        let repair = Policies {
            divergence: DivergencePolicy::Repair,
            ..DEFAULTS
        };
        assert_eq!(after_status(&s(Reason::Diverged), &repair), Action::Repair);
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
