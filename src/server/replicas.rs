//! Per-stream state of the replicas a node serves (addendum §1): what was sent, what the
//! replica acknowledged, and whether it is in sync.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use crate::source::LogSource;
use crate::types::Seq;

/// The fetch settings of one Tail stream, after the server clamped them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchSettings {
    pub min_batch_bytes: u64,
    pub max_wait: Duration,
    pub max_batch_bytes: u64,
}

/// A direct downstream replica, as its upstream sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaStatus {
    /// The replica's `x-rrdb-node-id` (or certificate CommonName under mutual TLS).
    pub node_id: String,
    pub remote_addr: SocketAddr,
    /// True when `node_id` comes from a verified client certificate.
    pub peer_verified: bool,
    /// The epoch of the identity the replica follows.
    pub epoch: u64,
    pub connected_since: SystemTime,
    /// The last seq sent to the replica.
    pub sent_seq: Seq,
    /// The last seq the replica acknowledged as committed.
    pub applied_seq: Seq,
    /// This node's `last_seq` minus `applied_seq`.
    pub lag_entries: u64,
    /// Now minus `ts_ms` of the first unacknowledged entry; 0 when caught up, `None` when that
    /// entry is no longer retained here.
    pub lag_ms: Option<u64>,
    /// Follows this node's current identity and was fully caught up within
    /// `ServeLimits::replica_lag_time_max`: it acknowledged a batch that held everything this
    /// node had when the batch was sent (PROTOCOL.md §7.6).
    pub in_sync: bool,
    pub fetch: FetchSettings,
}

#[derive(Debug)]
struct Progress {
    sent_seq: Seq,
    applied_seq: Seq,
    /// When the replica was last fully caught up: when a batch it has acknowledged was sent
    /// while it held everything this node had (Kafka's "caught up to the end offset at fetch
    /// time"), or when an ack reached this node's `last_seq` directly.
    caught_up_at: Option<Instant>,
    /// Sends not yet acknowledged that drained this node's log: `(last seq sent, when)`.
    drained: VecDeque<(Seq, Instant)>,
}

/// At most this many unacknowledged draining sends are remembered (oldest dropped first).
const MAX_DRAINED: usize = 1024;

impl Progress {
    /// In sync: follows the current `epoch` and was caught up within `lag_max`.
    fn in_sync(&self, cell_epoch: u64, epoch: u64, lag_max: Duration) -> bool {
        cell_epoch == epoch && self.caught_up_at.is_some_and(|t| t.elapsed() <= lag_max)
    }
}

/// One open Tail stream.
#[derive(Debug)]
pub(crate) struct ReplicaCell {
    node_id: String,
    remote_addr: SocketAddr,
    peer_verified: bool,
    epoch: u64,
    connected_since: SystemTime,
    fetch: FetchSettings,
    progress: Mutex<Progress>,
    registry: Arc<ReplicaRegistry>,
}

impl ReplicaCell {
    fn progress(&self) -> std::sync::MutexGuard<'_, Progress> {
        self.progress.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records that entries up to `seq` were handed to the stream; `node_last_seq` is this
    /// node's `last_seq` at that moment. A send that reaches it drained the log: the replica
    /// is caught up as of now once it acknowledges `seq`.
    pub(crate) fn note_sent(&self, seq: Seq, node_last_seq: Seq) {
        let mut p = self.progress();
        p.sent_seq = p.sent_seq.max(seq);
        if seq >= node_last_seq {
            if p.drained.len() == MAX_DRAINED {
                p.drained.pop_front();
            }
            p.drained.push_back((seq, Instant::now()));
        }
    }

    /// Records an ack. `node_last_seq` is this node's `last_seq` right now. An ack that goes
    /// backwards or above the last sent seq is a protocol violation.
    pub(crate) fn ack(&self, applied_seq: Seq, node_last_seq: Seq) -> Result<(), String> {
        {
            let mut p = self.progress();
            if applied_seq < p.applied_seq {
                return Err(format!("ack {applied_seq} is below the previous ack {}", p.applied_seq));
            }
            if applied_seq > p.sent_seq {
                return Err(format!("ack {applied_seq} is above the last sent seq {}", p.sent_seq));
            }
            p.applied_seq = applied_seq;
            let mut caught_up = (applied_seq >= node_last_seq).then(Instant::now);
            while p.drained.front().is_some_and(|&(seq, _)| seq <= applied_seq) {
                let (_, at) = p.drained.pop_front().expect("checked");
                caught_up = caught_up.max(Some(at));
            }
            if caught_up > p.caught_up_at {
                p.caught_up_at = caught_up;
            }
        }
        self.registry.notify();
        Ok(())
    }
}

/// Removes its stream from the registry when dropped (the Tail producer owns it).
pub(crate) struct ReplicaHandle {
    id: u64,
    cell: Arc<ReplicaCell>,
}

impl ReplicaHandle {
    pub(crate) fn cell(&self) -> &Arc<ReplicaCell> {
        &self.cell
    }
}

impl Drop for ReplicaHandle {
    fn drop(&mut self) {
        let reg = &self.cell.registry;
        reg.streams().remove(&self.id);
        reg.notify();
    }
}

/// The replicas of one node. Shared by its server and (on the master) its writer.
#[derive(Debug, Default)]
pub(crate) struct ReplicaRegistry {
    next_id: AtomicU64,
    streams: Mutex<HashMap<u64, Arc<ReplicaCell>>>,
    /// Bumped on every ack and every stream change; writers waiting for acks wait on it.
    changes: Mutex<u64>,
    changed: Condvar,
}

/// What a new stream registers.
pub(crate) struct NewReplica {
    pub node_id: String,
    pub remote_addr: SocketAddr,
    pub peer_verified: bool,
    pub epoch: u64,
    /// `from_seq - 1`: the replica already has everything up to here.
    pub has_up_to: Seq,
    pub fetch: FetchSettings,
}

impl ReplicaRegistry {
    fn streams(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Arc<ReplicaCell>>> {
        self.streams.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn notify(&self) {
        *self.changes.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        self.changed.notify_all();
    }

    pub(crate) fn open(self: &Arc<Self>, r: NewReplica) -> ReplicaHandle {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let cell = Arc::new(ReplicaCell {
            node_id: r.node_id,
            remote_addr: r.remote_addr,
            peer_verified: r.peer_verified,
            epoch: r.epoch,
            connected_since: SystemTime::now(),
            fetch: r.fetch,
            progress: Mutex::new(Progress {
                sent_seq: r.has_up_to,
                applied_seq: r.has_up_to,
                caught_up_at: None,
                drained: VecDeque::new(),
            }),
            registry: self.clone(),
        });
        self.streams().insert(id, cell.clone());
        self.notify();
        ReplicaHandle { id, cell }
    }

    /// `(applied_seq, in_sync)` of every open stream, for the given current epoch.
    #[cfg_attr(not(test), allow(dead_code))] // used by the write concern (later Plan 2 task)
    pub(crate) fn acks(&self, epoch: u64, lag_max: Duration) -> Vec<(Seq, bool)> {
        self.streams()
            .values()
            .map(|c| {
                let p = c.progress();
                (p.applied_seq, p.in_sync(c.epoch, epoch, lag_max))
            })
            .collect()
    }

    /// Blocks until `done` holds for the current acks, or `timeout` passes. Returns whether
    /// `done` held. For synchronous writers; never call it on an async worker thread.
    #[cfg_attr(not(test), allow(dead_code))] // used by the write concern (later Plan 2 task)
    pub(crate) fn wait_until(&self, timeout: Duration, mut done: impl FnMut(&Self) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        let mut changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            // `done` takes the stream locks; never hold `changes` while calling it.
            let seen = *changes;
            drop(changes);
            if done(self) {
                return true;
            }
            changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
            if *changes != seen {
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            changes = self.changed.wait_timeout(changes, deadline - now).unwrap_or_else(PoisonError::into_inner).0;
        }
    }

    /// `(node_id, applied_seq, in_sync)` of every open stream, for `Status`.
    pub(crate) fn summary(&self, epoch: u64, lag_max: Duration) -> Vec<(String, Seq, bool)> {
        let mut out: Vec<_> = self
            .streams()
            .values()
            .map(|c| {
                let p = c.progress();
                (c.node_id.clone(), p.applied_seq, p.in_sync(c.epoch, epoch, lag_max))
            })
            .collect();
        out.sort();
        out
    }

    /// The status of every open stream.
    pub(crate) fn status(&self, src: &dyn LogSource, now_ms: u64, lag_max: Duration) -> Vec<ReplicaStatus> {
        let epoch = src.identity().map_or(0, |(id, _)| id.epoch);
        let last = src.store().position().last_seq;
        let mut out: Vec<ReplicaStatus> = self
            .streams()
            .values()
            .map(|c| {
                let p = c.progress();
                let lag_entries = last.saturating_sub(p.applied_seq);
                let lag_ms = if lag_entries == 0 {
                    Some(0)
                } else {
                    src.store()
                        .read_entries(p.applied_seq + 1, 1, usize::MAX)
                        .ok()
                        .and_then(|v| v.first().map(|e| now_ms.saturating_sub(e.ts_ms)))
                };
                ReplicaStatus {
                    node_id: c.node_id.clone(),
                    remote_addr: c.remote_addr,
                    peer_verified: c.peer_verified,
                    epoch: c.epoch,
                    connected_since: c.connected_since,
                    sent_seq: p.sent_seq,
                    applied_seq: p.applied_seq,
                    lag_entries,
                    lag_ms,
                    in_sync: p.in_sync(c.epoch, epoch, lag_max),
                    fetch: c.fetch,
                }
            })
            .collect();
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id).then(a.connected_since.cmp(&b.connected_since)));
        out
    }
}

#[cfg(test)]
#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> Arc<ReplicaRegistry> {
        Arc::new(ReplicaRegistry::default())
    }

    fn new(has_up_to: Seq) -> NewReplica {
        NewReplica {
            node_id: "b".into(),
            remote_addr: "127.0.0.1:1".parse().unwrap(),
            peer_verified: false,
            epoch: 1,
            has_up_to,
            fetch: FetchSettings { min_batch_bytes: 0, max_wait: Duration::ZERO, max_batch_bytes: 1 << 20 },
        }
    }

    #[test]
    fn acks_must_be_monotonic_and_not_above_what_was_sent() {
        let reg = registry();
        let h = reg.open(new(5));
        h.cell().ack(5, 9).unwrap();
        assert!(h.cell().ack(6, 9).unwrap_err().contains("above the last sent"));
        h.cell().note_sent(9, 9);
        h.cell().ack(7, 9).unwrap();
        assert!(h.cell().ack(6, 9).unwrap_err().contains("below the previous"));
        assert_eq!(reg.acks(1, Duration::from_secs(10)), vec![(7, false)], "not caught up yet");
        h.cell().ack(9, 9).unwrap();
        assert_eq!(reg.acks(1, Duration::from_secs(10)), vec![(9, true)]);
        assert_eq!(reg.acks(2, Duration::from_secs(10)), vec![(9, false)], "other epoch: not in sync");
        drop(h);
        assert!(reg.acks(1, Duration::from_secs(10)).is_empty(), "closed streams leave the registry");
    }

    /// Kafka-style: an ack covering a send that drained the log makes the replica caught up
    /// as of that send, even if the node has moved on since.
    #[test]
    fn an_ack_of_a_draining_send_counts_as_caught_up_while_the_node_moves_on() {
        let reg = registry();
        let h = reg.open(new(0));
        h.cell().note_sent(3, 5); // did not drain: the node had 5
        h.cell().ack(3, 9).unwrap();
        assert_eq!(reg.acks(1, Duration::from_secs(10)), vec![(3, false)]);
        h.cell().note_sent(5, 5); // drained
        h.cell().ack(5, 9).unwrap(); // the node is at 9 by the time the ack arrives
        assert_eq!(reg.acks(1, Duration::from_secs(10)), vec![(5, true)]);
    }

    #[test]
    fn in_sync_expires_after_lag_time_max() {
        let reg = registry();
        let h = reg.open(new(3));
        h.cell().ack(3, 3).unwrap();
        assert_eq!(reg.acks(1, Duration::from_millis(50)), vec![(3, true)]);
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(reg.acks(1, Duration::from_millis(50)), vec![(3, false)]);
    }

    #[test]
    fn wait_until_wakes_on_acks_and_times_out() {
        let reg = registry();
        let h = reg.open(new(0));
        h.cell().note_sent(4, 4);
        let cell = h.cell().clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            cell.ack(4, 4).unwrap();
        });
        assert!(reg.wait_until(Duration::from_secs(5), |r| r.acks(1, Duration::from_secs(10)).iter().any(|a| a.0 >= 4)));
        t.join().unwrap();
        let t0 = Instant::now();
        assert!(!reg.wait_until(Duration::from_millis(50), |r| r.acks(1, Duration::from_secs(10)).iter().any(|a| a.0 >= 5)));
        assert!(t0.elapsed() >= Duration::from_millis(50));
    }
}
