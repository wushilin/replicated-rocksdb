//! Public types of the `Node` handle (DESIGN.md §13.2).

use crate::slave::SlaveState;
use crate::types::DbIdentity;

/// Acknowledges that an operation can leave the database with two writers (split brain): a
/// node opens as master without proving that no other master exists. A caller constructs it
/// explicitly (`AcceptSplitBrainRisk`); nothing in the crate supplies one by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptSplitBrainRisk;

/// Why a stored master stays fenced (DESIGN.md §13.2). `peer` is the peer's node id when it
/// answered `Status`, its address otherwise. More reasons may be added, hence
/// `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FenceReason {
    /// `NodeConfig::peers` is empty: nothing can prove there is no other master.
    NoPeers,
    /// The peer did not answer `Status` within `admin::STATUS_TIMEOUT`.
    PeerUnreachable { peer: String },
    /// The peer reports `role == Master`.
    PeerIsMaster { peer: String },
    /// The peer holds more of a compatible history, or knows an epoch at or above this
    /// node's newest one (in its identity or `max_burned_epoch`).
    PeerAhead { peer: String },
    /// The peer's history and this node's hold different entries, or the peer's epoch history
    /// holds a record (epoch, start, nonce) this node's does not, at any epoch number: another
    /// writer exists.
    PeerDiverged { peer: String },
    /// The peer answered `Status` with an error, or belongs to another database.
    PeerError { peer: String, error: String },
}

impl std::fmt::Display for FenceReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FenceReason::NoPeers => write!(f, "no peers are configured to check against"),
            FenceReason::PeerUnreachable { peer } => write!(f, "peer {peer} did not answer"),
            FenceReason::PeerIsMaster { peer } => write!(f, "peer {peer} is a master"),
            FenceReason::PeerAhead { peer } => {
                write!(
                    f,
                    "peer {peer} is ahead of this node or knows a newer epoch"
                )
            }
            FenceReason::PeerDiverged { peer } => {
                write!(
                    f,
                    "peer {peer} holds a history that diverges from this node's"
                )
            }
            FenceReason::PeerError { peer, error } => {
                write!(f, "peer {peer} answered with an error: {error}")
            }
        }
    }
}

/// What a `Node` is right now (`Node::state`, `Node::watch_state`). More states may be
/// added, hence `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum NodeState {
    /// Running as the master; `identity` is its current epoch.
    Master { identity: DbIdentity },
    /// A stored master that has not been cleared by a peer check: no epoch started, no port
    /// served, reads and writes refused.
    Fenced { reason: FenceReason },
    /// Running as a slave; `state` follows the slave's own state machine live, `upstream` is
    /// the address it replicates from (as `SlaveStatus::upstream`).
    Slave { state: SlaveState, upstream: String },
    /// A role switch is in progress; calls return `Error::Switching`.
    Switching,
    /// A switch lost the running handle; `Node::reopen` opens the directory again.
    Failed { error: String },
    /// `Node::shutdown` completed; `Node::reopen` opens the directory again.
    Stopped,
}

/// Options of `Node::unfence`. The default refuses while any peer is unreachable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnfenceOptions {
    /// Clear the fence although some peers did not answer: they could be a live master.
    pub allow_unreachable: Option<AcceptSplitBrainRisk>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::slave::{AcceptDataLoss, RejoinOptions, RepointDivergence};

    #[test]
    fn defaults_refuse() {
        assert_eq!(UnfenceOptions::default().allow_unreachable, None);
        assert_eq!(
            RejoinOptions::default().on_divergence,
            RepointDivergence::Refuse
        );
        let repair = RejoinOptions {
            on_divergence: RepointDivergence::Repair {
                accept_loss: AcceptDataLoss,
            },
        };
        assert_ne!(repair, RejoinOptions::default());
    }

    #[test]
    fn fence_reasons_display_the_peer() {
        let cases = [
            (
                FenceReason::NoPeers,
                "no peers are configured to check against",
            ),
            (
                FenceReason::PeerUnreachable {
                    peer: "http://h:1".into(),
                },
                "peer http://h:1 did not answer",
            ),
            (
                FenceReason::PeerIsMaster { peer: "b".into() },
                "peer b is a master",
            ),
            (
                FenceReason::PeerAhead { peer: "b".into() },
                "peer b is ahead of this node or knows a newer epoch",
            ),
            (
                FenceReason::PeerDiverged { peer: "b".into() },
                "peer b holds a history that diverges from this node's",
            ),
            (
                FenceReason::PeerError {
                    peer: "b".into(),
                    error: "denied".into(),
                },
                "peer b answered with an error: denied",
            ),
        ];
        for (reason, text) in cases {
            assert_eq!(reason.to_string(), text);
        }
    }
}
