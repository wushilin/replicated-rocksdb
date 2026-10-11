//! The fence check of a stored master (DESIGN.md §13.2): may this directory open as the
//! master? It reads the node's own position from the closed store and every peer's `Status`,
//! and writes nothing.

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use tokio::task::JoinSet;

use super::types::FenceReason;
use crate::admin::{self, Comparison, NodeInfo, Side, StatusFailure, compare};
use crate::background::blocking;
use crate::error::{Error, Result};
use crate::slave::UpstreamConfig;
use crate::store::offline::OfflineLog;
use crate::types::{Role, Seq};

/// One peer's answer to `Status`. In [`decide`]'s input it is paired with the peer's address.
pub(crate) enum PeerAnswer {
    /// `local_probe`: this node's CRC at the peer's `last_seq`, when the peer is shorter.
    Answered {
        info: Box<NodeInfo>,
        local_probe: Option<(Seq, Option<u32>)>,
    },
    Unreachable,
    Error(String),
}

/// Severity ranks: lower wins (decision 8 of the plan).
const MASTER: u8 = 0;
const DIVERGED: u8 = 1;
const AHEAD: u8 = 2;
const ERROR: u8 = 3;
const UNREACHABLE: u8 = 4;

/// `None` when the fence clears: at least one peer, every peer answered (or
/// `allow_unreachable`), none is a master, none is later than or diverged from `me`, none is
/// of another database, none holds an epoch history record (epoch, start, nonce) that `me`
/// does not (at any epoch number: evidence of another writer), and none knows an epoch at
/// or above `me`'s newest one through its identity or `max_burned_epoch`. Otherwise the most
/// severe reason, first in `answers` order. `allow_unreachable` only waives unreachable
/// peers: an error answer, and every answered peer's evidence, always keeps the fence.
///
/// `answers` pairs each peer's address with its answer. A reason's `peer` is the peer's
/// `node_id` (from its `Status`) when it answered with its state (`PeerIsMaster`,
/// `PeerAhead`, `PeerDiverged`, and `PeerError` for another database), and that address
/// otherwise (`PeerUnreachable`, and `PeerError` for an error answer).
pub(crate) fn decide(
    me: &NodeInfo,
    answers: &[(String, PeerAnswer)],
    allow_unreachable: bool,
) -> Option<FenceReason> {
    if answers.is_empty() {
        return Some(FenceReason::NoPeers);
    }
    let mut worst: Option<(u8, FenceReason)> = None;
    for (uri, answer) in answers {
        let verdict = match answer {
            PeerAnswer::Unreachable if allow_unreachable => None,
            PeerAnswer::Unreachable => Some((
                UNREACHABLE,
                FenceReason::PeerUnreachable { peer: uri.clone() },
            )),
            PeerAnswer::Error(error) => Some((
                ERROR,
                FenceReason::PeerError {
                    peer: uri.clone(),
                    error: error.clone(),
                },
            )),
            PeerAnswer::Answered { info, local_probe } => judge(me, info, *local_probe),
        };
        if let Some((rank, reason)) = verdict {
            if worst.as_ref().is_none_or(|(w, _)| rank < *w) {
                worst = Some((rank, reason));
            }
        }
    }
    worst.map(|(_, reason)| reason)
}

/// One answered peer's verdict, `None` when it does not block.
fn judge(
    me: &NodeInfo,
    info: &NodeInfo,
    local_probe: Option<(Seq, Option<u32>)>,
) -> Option<(u8, FenceReason)> {
    let peer = info.node_id.clone();
    if info.role == Role::Master {
        return Some((MASTER, FenceReason::PeerIsMaster { peer }));
    }
    let mut mine = me.clone();
    if let Some((seq, crc)) = local_probe {
        mine.probes.insert(seq, crc);
    }
    match compare(&mine, info) {
        Comparison::DifferentDatabase => {
            return Some((
                ERROR,
                FenceReason::PeerError {
                    peer,
                    error: "a different database".into(),
                },
            ));
        }
        Comparison::Diverged { .. } => {
            return Some((DIVERGED, FenceReason::PeerDiverged { peer }));
        }
        Comparison::Later(Side::B) => return Some((AHEAD, FenceReason::PeerAhead { peer })),
        Comparison::Later(Side::A) | Comparison::Equal => {}
    }
    // Every epoch has one writer: a record this node does not hold, at any epoch number, means
    // another writer exists (a compatible peer holds a prefix of this node's history).
    if info
        .epoch_history
        .iter()
        .any(|r| !me.epoch_history.contains(r))
    {
        return Some((DIVERGED, FenceReason::PeerDiverged { peer }));
    }
    let newest = me.epoch_history.last().map_or(0, |e| e.epoch);
    let knows_newer = (info.max_burned_epoch > 0 && info.max_burned_epoch >= newest)
        || info.identity.is_some_and(|id| id.epoch > newest);
    knows_newer.then_some((AHEAD, FenceReason::PeerAhead { peer }))
}

/// Runs the fence check for the closed store at `path` (DESIGN.md §13.2). Every peer is asked
/// for `Status` concurrently (each bounded by `admin::STATUS_TIMEOUT`), probed at this node's
/// `last_seq`; this node's CRC at a shorter peer's `last_seq` is read from the closed store.
/// A peer whose `Status` task fails (panics) counts as an error answer for that peer.
///
/// `Err` only when the local store cannot be read (including a directory that holds no
/// database); nothing is written and no epoch starts. The read takes no LOCK: a directory
/// another process holds open is read as it stands, and a later writable open fails on the
/// LOCK.
pub(crate) async fn check(
    path: &Path,
    node_id: &str,
    peers: &[UpstreamConfig],
    allow_unreachable: bool,
) -> Result<Option<FenceReason>> {
    if peers.is_empty() {
        return Ok(Some(FenceReason::NoPeers));
    }
    let p = path.to_path_buf();
    let log = Arc::new(
        blocking(move || {
            OfflineLog::open(&p)?
                .ok_or_else(|| Error::Config(format!("{} holds no database", p.display())))
        })
        .await?,
    );
    let me = log.node_info(node_id);
    let probe = Some(me.last_seq).filter(|&s| s > 0);
    let mut calls = JoinSet::new();
    let mut slot_of = HashMap::new();
    for (i, peer) in peers.iter().cloned().enumerate() {
        let id = node_id.to_string();
        let handle = calls.spawn(async move { admin::status_classified(&peer, &id, probe).await });
        slot_of.insert(handle.id(), i);
    }
    let mut answers: Vec<Option<PeerAnswer>> = (0..peers.len()).map(|_| None).collect();
    while let Some(joined) = calls.join_next_with_id().await {
        let (task, r) = match joined {
            Ok((task, r)) => (task, Ok(r)),
            Err(e) => (e.id(), Err(e)),
        };
        let i = slot_of[&task];
        let uri = peers[i].uri();
        let answer = match r {
            Err(e) => {
                tracing::warn!(peer = %uri, error = %e, "fence check: the peer's Status task failed");
                PeerAnswer::Error(format!("Status task failed: {e}"))
            }
            Ok(Ok(info)) => {
                let local_probe = if info.last_seq > 0 && info.last_seq < me.last_seq {
                    let (l, seq) = (log.clone(), info.last_seq);
                    Some((seq, blocking(move || l.entry_crc(seq)).await?))
                } else {
                    None
                };
                PeerAnswer::Answered {
                    info: Box::new(info),
                    local_probe,
                }
            }
            Ok(Err(StatusFailure::Unreachable { reason, .. })) => {
                tracing::warn!(peer = %uri, reason = %reason, "fence check: peer unreachable");
                PeerAnswer::Unreachable
            }
            Ok(Err(StatusFailure::Answered(e))) => {
                tracing::warn!(peer = %uri, error = %e, "fence check: peer answered with an error");
                PeerAnswer::Error(e)
            }
        };
        answers[i] = Some(answer);
    }
    drop(log);
    let answers: Vec<(String, PeerAnswer)> = peers
        .iter()
        .zip(answers)
        .map(|(peer, answer)| {
            let answer = answer.unwrap_or_else(|| PeerAnswer::Error("no answer recorded".into()));
            (peer.uri(), answer)
        })
        .collect();
    let verdict = decide(&me, &answers, allow_unreachable);
    match &verdict {
        None => tracing::info!(node = %node_id, "fence check cleared"),
        Some(reason) => tracing::warn!(node = %node_id, %reason, "fence check: still fenced"),
    }
    Ok(verdict)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DbIdentity, EpochStart};

    const DB: u128 = 1;

    fn rec(epoch: u64, start_seq: Seq, nonce: u64) -> EpochStart {
        EpochStart {
            epoch,
            start_seq,
            nonce,
        }
    }

    fn node(id: &str, role: Role, db: u128, history: Vec<EpochStart>, last_seq: Seq) -> NodeInfo {
        let mut n = NodeInfo::new(id, role);
        n.identity = Some(DbIdentity {
            db_uuid: uuid::Uuid::from_u128(db),
            epoch: history.last().map_or(1, |e| e.epoch),
        });
        n.epoch_history = history;
        n.last_seq = last_seq;
        n
    }

    /// The fenced old master: epochs 1 and 2 (its last open), 10 entries.
    fn me() -> NodeInfo {
        node(
            "a",
            Role::Master,
            DB,
            vec![rec(1, 1, 11), rec(2, 6, 22)],
            10,
        )
    }

    fn answered(info: NodeInfo) -> PeerAnswer {
        PeerAnswer::Answered {
            info: Box::new(info),
            local_probe: None,
        }
    }

    #[test]
    fn no_peers_never_clears() {
        assert_eq!(decide(&me(), &[], true), Some(FenceReason::NoPeers));
    }

    #[test]
    fn equal_and_behind_slaves_clear() {
        let equal = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 22)], 10);
        let behind = node("d", Role::Slave, DB, vec![rec(1, 1, 11)], 4);
        let answers = [
            ("u1".to_string(), answered(equal)),
            ("u2".to_string(), answered(behind)),
        ];
        assert_eq!(decide(&me(), &answers, false), None);
    }

    #[test]
    fn unreachable_blocks_unless_accepted_but_errors_always_block() {
        let ok = node("c", Role::Slave, DB, vec![rec(1, 1, 11)], 4);
        let answers = [
            ("u1".to_string(), answered(ok.clone())),
            ("http://dead:1".to_string(), PeerAnswer::Unreachable),
        ];
        assert_eq!(
            decide(&me(), &answers, false),
            Some(FenceReason::PeerUnreachable {
                peer: "http://dead:1".into()
            })
        );
        assert_eq!(decide(&me(), &answers, true), None);
        let all_dead = [("http://dead:1".to_string(), PeerAnswer::Unreachable)];
        assert_eq!(
            decide(&me(), &all_dead, true),
            None,
            "the consent covers them all"
        );
        let error = [("u3".to_string(), PeerAnswer::Error("denied".into()))];
        assert_eq!(
            decide(&me(), &error, true),
            Some(FenceReason::PeerError {
                peer: "u3".into(),
                error: "denied".into()
            })
        );
    }

    #[test]
    fn a_master_peer_wins_over_every_other_reason() {
        let master = node(
            "b",
            Role::Master,
            DB,
            vec![rec(1, 1, 11), rec(2, 6, 99)],
            12,
        );
        let answers = [
            ("http://dead:1".to_string(), PeerAnswer::Unreachable),
            ("u3".to_string(), PeerAnswer::Error("denied".into())),
            ("u2".to_string(), answered(master)),
        ];
        assert_eq!(
            decide(&me(), &answers, false),
            Some(FenceReason::PeerIsMaster { peer: "b".into() })
        );
    }

    #[test]
    fn a_later_or_diverged_peer_blocks() {
        let later = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 22)], 15);
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(later))], false),
            Some(FenceReason::PeerAhead { peer: "c".into() })
        );
        // Promoted elsewhere at seq 6 with another nonce, holding entries past it.
        let other = node("d", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 33)], 8);
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(other))], false),
            Some(FenceReason::PeerDiverged { peer: "d".into() })
        );
    }

    #[test]
    fn a_newer_epoch_anywhere_blocks() {
        // Behind and compatible by position, but it holds another epoch 2 (another nonce,
        // starting after both positions): someone else started this node's newest epoch
        // number. A record this node does not hold is another writer: diverged.
        let knows = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 11, 33)], 4);
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(knows))], false),
            Some(FenceReason::PeerDiverged { peer: "c".into() })
        );
        // Burned an epoch at or above this node's newest (a promotion undone elsewhere).
        let mut burned = node("c", Role::Slave, DB, vec![rec(1, 1, 11)], 4);
        burned.max_burned_epoch = 2;
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(burned))], false),
            Some(FenceReason::PeerAhead { peer: "c".into() })
        );
    }

    /// A peer record below this node's newest epoch that this node does not hold is still
    /// another writer. A (epoch 2 at seq 6) failed over to lagging B, promoted as epoch 2
    /// (nonce 99) at seq 4; A was later reopened unchecked as epoch 3. C learned B's record but
    /// holds only 3 entries, so by position it is behind A; B is unreachable and the consent
    /// to skip it is given. C's evidence must still keep A fenced.
    #[test]
    fn a_foreign_record_below_this_nodes_epoch_blocks_even_with_consent() {
        let me = node(
            "a",
            Role::Master,
            DB,
            vec![rec(1, 1, 11), rec(2, 6, 22), rec(3, 11, 33)],
            10,
        );
        let c = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 4, 99)], 3);
        let answers = [
            ("http://b:1".to_string(), PeerAnswer::Unreachable),
            ("u".to_string(), answered(c)),
        ];
        assert_eq!(
            decide(&me, &answers, true),
            Some(FenceReason::PeerDiverged { peer: "c".into() })
        );
    }

    /// An empty, never-bootstrapped slave holds nothing this node lacks and is not a writer.
    #[test]
    fn a_peer_without_data_clears() {
        let empty = NodeInfo::new("e", Role::Slave);
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(empty))], false),
            None
        );
    }

    /// Divergence outranks an error answer and a peer of another database, whatever the order.
    #[test]
    fn divergence_outranks_errors() {
        let foreign = node("x", Role::Slave, 2, vec![rec(1, 1, 5)], 1);
        let other = node("d", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 33)], 8);
        let answers = [
            ("u1".to_string(), PeerAnswer::Error("denied".into())),
            ("u2".to_string(), answered(foreign)),
            ("u3".to_string(), answered(other)),
        ];
        assert_eq!(
            decide(&me(), &answers, true),
            Some(FenceReason::PeerDiverged { peer: "d".into() })
        );
    }

    /// Review focus: a peer of another database never clears the fence.
    #[test]
    fn a_peer_of_another_database_never_clears() {
        let foreign = node("x", Role::Slave, 2, vec![rec(1, 1, 5)], 1);
        assert!(matches!(
            decide(&me(), &[("u".to_string(), answered(foreign))], true),
            Some(FenceReason::PeerError { peer, .. }) if peer == "x"
        ));
    }

    #[test]
    fn a_crc_mismatch_at_the_peers_last_entry_is_divergence() {
        let mut me = me();
        me.last_entry_crc = Some(1);
        let mut peer = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 22)], 8);
        peer.last_entry_crc = Some(7);
        let answers = [(
            "u".to_string(),
            PeerAnswer::Answered {
                info: Box::new(peer),
                local_probe: Some((8, Some(9))),
            },
        )];
        assert_eq!(
            decide(&me, &answers, false),
            Some(FenceReason::PeerDiverged { peer: "c".into() })
        );
    }

    #[tokio::test]
    async fn check_reads_the_closed_store_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = crate::store::test_support::open_store(dir.path());
            let id = DbIdentity {
                db_uuid: uuid::Uuid::from_u128(DB),
                epoch: 1,
            };
            store
                .write_meta(
                    &[
                        (
                            crate::store::meta::key::ORIGIN_IDENTITY,
                            &crate::store::meta::encode_identity(&id),
                        ),
                        (
                            crate::store::meta::key::EPOCH_HISTORY,
                            &crate::store::meta::encode_epoch_history(&[rec(1, 1, 11)]),
                        ),
                    ],
                    true,
                )
                .unwrap();
        }
        let before = crate::store::offline::stored_meta(dir.path()).unwrap();
        assert_eq!(
            check(dir.path(), "a", &[], false).await.unwrap(),
            Some(FenceReason::NoPeers)
        );
        // Port 1 on loopback: nothing listens, the connection is refused at once.
        let dead = UpstreamConfig::new("127.0.0.1", 1);
        assert_eq!(
            check(dir.path(), "a", std::slice::from_ref(&dead), false)
                .await
                .unwrap(),
            Some(FenceReason::PeerUnreachable { peer: dead.uri() })
        );
        assert_eq!(check(dir.path(), "a", &[dead], true).await.unwrap(), None);
        assert_eq!(
            crate::store::offline::stored_meta(dir.path()).unwrap(),
            before,
            "nothing written"
        );
        let log = crate::store::offline::OfflineLog::open(dir.path())
            .unwrap()
            .unwrap();
        let info = log.node_info("a");
        assert_eq!(
            (info.role, info.last_seq, info.node_state.as_str()),
            (Role::Master, 0, "fenced")
        );
    }

    /// The read takes no LOCK: a directory a live store holds open is read as it stands,
    /// and nothing is written to it.
    #[tokio::test]
    async fn check_reads_a_directory_held_open_by_a_live_store() {
        let dir = tempfile::tempdir().unwrap();
        let store = crate::store::test_support::open_store(dir.path());
        let id = DbIdentity {
            db_uuid: uuid::Uuid::from_u128(DB),
            epoch: 1,
        };
        store
            .write_meta(
                &[
                    (
                        crate::store::meta::key::ORIGIN_IDENTITY,
                        &crate::store::meta::encode_identity(&id),
                    ),
                    (
                        crate::store::meta::key::EPOCH_HISTORY,
                        &crate::store::meta::encode_epoch_history(&[rec(1, 1, 11)]),
                    ),
                ],
                true,
            )
            .unwrap();
        let before = crate::store::offline::stored_meta(dir.path()).unwrap();
        let dead = UpstreamConfig::new("127.0.0.1", 1);
        assert_eq!(
            check(dir.path(), "a", std::slice::from_ref(&dead), false)
                .await
                .unwrap(),
            Some(FenceReason::PeerUnreachable { peer: dead.uri() })
        );
        assert_eq!(check(dir.path(), "a", &[dead], true).await.unwrap(), None);
        let info = crate::store::offline::OfflineLog::open(dir.path())
            .unwrap()
            .unwrap()
            .node_info("a");
        assert_eq!(info.identity, Some(id));
        assert_eq!(
            crate::store::offline::stored_meta(dir.path()).unwrap(),
            before
        );
        drop(store);
    }

    /// A directory without a database is a local read failure, never a cleared fence.
    #[tokio::test]
    async fn check_of_an_empty_directory_is_an_error() {
        let dir = tempfile::tempdir().unwrap();
        let dead = UpstreamConfig::new("127.0.0.1", 1);
        assert!(check(dir.path(), "a", &[dead], true).await.is_err());
    }
}
