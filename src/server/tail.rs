//! Tail: validation (PROTOCOL.md §7.2) and the per-stream producer (§7.3).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::replicas::{FetchSettings, NewReplica, ReplicaCell, ReplicaHandle};
use super::{call_info, store_status, Lease, ServerState, TailStream};
use crate::auth::Method;
use crate::clock::{Clock, SystemClock};
use crate::error::Error;
use crate::failpoint;
use crate::net::convert::{entry_to_pb, identity_from_pb};
use crate::net::{internal_status, invalid_argument, reason_status};
use crate::pb;
use crate::pb::tail_client_message::Kind as ClientKind;
use crate::pb::tail_message::Kind;
use crate::source::LogSource;
use crate::types::{LogEntry, Role, Seq};

/// Entries read from the changelog per blocking read.
pub(crate) const READ_BATCH: usize = 64;
/// Messages buffered per stream before back-pressure applies.
const CHANNEL: usize = 16;

/// How long the server waits for the client's `start` message.
const START_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn tail(
    state: &Arc<ServerState>,
    req: Request<tonic::Streaming<pb::TailClientMessage>>,
) -> Result<Response<TailStream>, Status> {
    let call = call_info(&req, Method::Tail)?;
    let src = state.authorize(&call, Method::Tail).await?;
    let mut inbound = req.into_inner();
    // The source is held while waiting, so the wait also ends on shutdown.
    let mut shutdown = state.shutdown.clone();
    let first = tokio::select! {
        m = tokio::time::timeout(START_TIMEOUT, inbound.message()) => m,
        _ = super::stopped(&mut shutdown) => return Err(shutting_down()),
    };
    let r = match first {
        Ok(Ok(Some(pb::TailClientMessage { kind: Some(ClientKind::Start(r)) }))) => r,
        Ok(Ok(_)) => return Err(invalid_argument("the first Tail message must be `start`")),
        Ok(Err(status)) => return Err(status),
        Err(_) => return Err(invalid_argument("no `start` message within 10 s")),
    };
    call.check_client_id(&r.client_id)?;
    // Subscribe before validating, so a reset after validation is never missed.
    let mut resets = src.resets();
    resets.borrow_and_update();
    let junction = validate(&*src, &r)?;
    state.expire_leases(std::time::Instant::now());
    let lease = r.bootstrap_lease.as_deref().and_then(|id| state.take_lease(id, &r.client_id, r.from_seq));
    let (tx, rx) = mpsc::channel(CHANNEL);
    let fetch = Fetch::clamp(&r, src.heartbeat(), state.cfg.limits.max_tail_batch_bytes);
    // `validate` checked the identity, so this is the epoch the replica follows.
    let epoch = r.identity.as_ref().map_or(0, |id| id.epoch);
    let replica = state.replicas.open(NewReplica {
        node_id: call.peer.peer_id.clone(),
        remote_addr: call.peer.remote_addr,
        peer_verified: call.peer.peer_id_verified,
        epoch,
        has_up_to: r.from_seq - 1,
        fetch: fetch.settings(),
    });
    let (violation_tx, violation_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    tokio::spawn(read_acks(inbound, replica.cell().clone(), src.subscribe(), violation_tx, done_rx));
    let stream = Stream { replica, violation: Some(violation_rx), _done: done_tx };
    let out = Out { tx, shutdown: state.shutdown.clone(), resets };
    tokio::spawn(produce(src, out, r.from_seq, fetch, lease, stream, junction));
    Ok(Response::new(ReceiverStream::new(rx)))
}

/// The client's `prev_crc32c` for entry `seq` (`from_seq - 1`), which the server did not hold
/// at validation (it is a replica behind the client). The producer checks it once the server
/// holds that entry, before sending `from_seq`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Junction {
    pub seq: Seq,
    pub crc: u32,
}

/// PROTOCOL.md §7.2. `Ok(Some(junction))` when `prev_crc32c` could not be checked yet.
fn validate(src: &dyn LogSource, r: &pb::TailRequest) -> Result<Option<Junction>, Status> {
    let ident = src.identity().filter(|_| src.is_ready());
    let Some((server_id, _)) = ident else {
        return Err(reason_status(pb::Reason::NotReady, "server is bootstrapping"));
    };
    let want = r.identity.as_ref().ok_or_else(|| crate::net::invalid_argument("identity is required"))?;
    let want = identity_from_pb(want).map_err(|e| crate::net::invalid_argument(e.to_string()))?;
    if want != server_id {
        return Err(reason_status(pb::Reason::IdentityChanged, format!("server identity is {server_id:?}")));
    }
    if r.from_seq == 0 {
        return Err(crate::net::invalid_argument("from_seq starts at 1"));
    }
    let store = src.store();
    let last = store.position().last_seq;
    if src.role() == Role::Master && r.from_seq > last + 1 {
        return Err(reason_status(pb::Reason::Diverged, format!("from_seq {} is past master last_seq {last}", r.from_seq)));
    }
    let mut junction = None;
    if let Some(prev) = r.prev_crc32c {
        match store.entry_crc(r.from_seq - 1).map_err(store_status)? {
            Some(have) if have != prev => {
                return Err(reason_status(pb::Reason::Diverged, format!("entry {} differs", r.from_seq - 1)));
            }
            Some(_) => {}
            // Not held yet (the client is ahead of this replica): checked before `from_seq`
            // is sent. A trimmed entry stays unknown and is not checked.
            None => junction = Some(Junction { seq: r.from_seq - 1, crc: prev }),
        }
    }
    let first = store.first_retained_seq();
    if r.from_seq < first {
        return Err(reason_status(pb::Reason::LogGap, format!("from_seq {} < first retained {first}", r.from_seq)));
    }
    Ok(junction)
}

/// The junction check of a stream that started ahead of this server: `Err(DIVERGED)` if the
/// server now holds entry `j.seq` and its CRC is not the client's `prev_crc32c`.
fn check_junction(src: &dyn LogSource, j: Junction) -> Result<(), Status> {
    match src.store().entry_crc(j.seq).map_err(store_status)? {
        Some(have) if have != j.crc => Err(reason_status(
            pb::Reason::Diverged,
            format!("entry {} differs (checked once this server caught up with the client)", j.seq),
        )),
        _ => Ok(()),
    }
}

fn heartbeat(src: &dyn LogSource) -> pb::TailMessage {
    pb::TailMessage {
        kind: Some(Kind::Heartbeat(pb::Heartbeat {
            upstream_last_seq: src.store().position().last_seq,
            origin_last_seq: src.origin_last_seq(),
            ts_ms: SystemClock.now_ms(),
        })),
    }
}

type Tx = mpsc::Sender<Result<pb::TailMessage, Status>>;

/// The producer's ties to its stream: its registry entry (removed when the producer ends),
/// a protocol violation found by the ack reader, and `_done`, whose drop stops the reader.
struct Stream {
    replica: ReplicaHandle,
    /// `None` once the ack reader has finished without a violation.
    violation: Option<oneshot::Receiver<Status>>,
    _done: oneshot::Sender<()>,
}

impl Stream {
    /// A violation the ack reader reported, if any (non-blocking).
    fn violation(&mut self) -> Option<Status> {
        let rx = self.violation.as_mut()?;
        match rx.try_recv() {
            Ok(status) => Some(status),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                self.violation = None;
                None
            }
        }
    }
}

/// Reads the client's acks (addendum §1) until the stream ends. It holds no reference to
/// the node, only the stream's registry cell and the node's seq channel.
async fn read_acks(
    mut inbound: tonic::Streaming<pb::TailClientMessage>,
    cell: Arc<ReplicaCell>,
    seqs: watch::Receiver<Seq>,
    violation: oneshot::Sender<Status>,
    mut done: oneshot::Receiver<()>,
) {
    loop {
        let msg = tokio::select! {
            m = inbound.message() => m,
            _ = &mut done => return,
        };
        let problem = match msg {
            Ok(Some(pb::TailClientMessage { kind: Some(ClientKind::Ack(a)) })) => {
                let node_last = *seqs.borrow();
                match cell.ack(a.applied_seq, node_last) {
                    Ok(()) => continue,
                    Err(e) => e,
                }
            }
            Ok(Some(_)) => "after `start`, only `ack` messages are allowed".to_string(),
            // The client half-closed or went away; the producer notices on its own.
            Ok(None) | Err(_) => return,
        };
        let _ = violation.send(invalid_argument(problem));
        return;
    }
}

/// The sending side of one stream. Every send also watches shutdown and resets, so a
/// client that stops reading can never keep the producer (and the node's database) alive.
struct Out {
    tx: Tx,
    shutdown: watch::Receiver<bool>,
    resets: watch::Receiver<u64>,
}

enum Sent {
    Ok,
    /// The client went away.
    Gone,
    /// The stream must end with this status.
    End(Status),
}

impl Out {
    async fn send(&mut self, msg: pb::TailMessage) -> Sent {
        tokio::select! {
            biased;
            _ = super::stopped(&mut self.shutdown) => Sent::End(shutting_down()),
            _ = self.resets.changed() => Sent::End(upstream_reset()),
            r = self.tx.send(Ok(msg)) => if r.is_ok() { Sent::Ok } else { Sent::Gone },
        }
    }

    async fn end(&mut self, status: Status) {
        super::end_stream(&self.tx, status, &mut self.shutdown, &mut self.resets).await;
    }
}

fn shutting_down() -> Status {
    reason_status(pb::Reason::ShuttingDown, "server is shutting down")
}

fn upstream_reset() -> Status {
    reason_status(pb::Reason::UpstreamReset, "server state was replaced")
}

/// Fetch batching parameters of one stream, clamped to the server's limits (PROTOCOL.md §7.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Fetch {
    pub min_bytes: usize,
    pub max_wait: Duration,
    pub max_bytes: usize,
}

/// `max_batch_bytes = 0` means this.
pub(crate) const DEFAULT_MAX_BATCH_BYTES: usize = 1 << 20;

impl Fetch {
    fn settings(&self) -> FetchSettings {
        FetchSettings {
            min_batch_bytes: self.min_bytes as u64,
            max_wait: self.max_wait,
            max_batch_bytes: self.max_bytes as u64,
        }
    }

    pub(crate) fn clamp(r: &pb::TailRequest, heartbeat: Duration, max_tail_batch_bytes: usize) -> Fetch {
        let max_bytes = match r.max_batch_bytes {
            0 => DEFAULT_MAX_BATCH_BYTES,
            n => n as usize,
        };
        Fetch {
            min_bytes: r.min_batch_bytes as usize,
            max_wait: Duration::from_millis(u64::from(r.max_wait_ms)).min(heartbeat),
            max_bytes: max_bytes.min(max_tail_batch_bytes).max(1),
        }
    }
}

/// Size of an entry inside an `EntryBatch`: its encoded length plus its framing (field tag
/// and length varint), so a batch never exceeds `max_batch_bytes` on the wire.
fn framed_len(entry_len: usize) -> usize {
    let mut varint = 1;
    let mut n = entry_len >> 7;
    while n > 0 {
        varint += 1;
        n >>= 7;
    }
    entry_len + 1 + varint
}

/// Entries read but not yet sent.
struct Pending {
    entries: Vec<pb::LogEntry>,
    bytes: usize,
    /// When the oldest pending entry became available to this stream.
    since: Option<Instant>,
}

impl Pending {
    fn take(&mut self) -> pb::TailMessage {
        self.bytes = 0;
        self.since = None;
        pb::TailMessage { kind: Some(Kind::Batch(pb::EntryBatch { entries: std::mem::take(&mut self.entries) })) }
    }
}

/// Streams entries from `next` in order, in batches (PROTOCOL.md §7.5), with heartbeats,
/// until the client goes away or the stream must end (reset, shutdown, gap, corruption).
async fn produce(
    src: Arc<dyn LogSource>,
    mut out: Out,
    mut next: Seq,
    fetch: Fetch,
    mut lease: Option<Lease>,
    mut stream: Stream,
    mut junction: Option<Junction>,
) {
    let hb: Duration = src.heartbeat();
    let mut seqs = src.subscribe();
    let mut ahead: VecDeque<LogEntry> = VecDeque::new();
    let mut pending = Pending { entries: Vec::new(), bytes: 0, since: None };
    // The first heartbeat goes out at once, so the client learns positions immediately.
    let mut next_hb = Instant::now();
    macro_rules! send {
        ($msg:expr) => {
            match out.send($msg).await {
                Sent::Ok => {}
                Sent::Gone => return,
                Sent::End(status) => return out.end(status).await,
            }
        };
    }
    // The registry learns what was sent before the client can see it, so an ack is never
    // ahead of `sent_seq`.
    macro_rules! send_pending {
        () => {{
            if let Some(last) = pending.entries.last() {
                stream.replica.cell().note_sent(last.seq, src.store().position().last_seq);
            }
            send!(pending.take());
        }};
    }
    macro_rules! flush {
        () => {{
            send_pending!();
            if Instant::now() >= next_hb {
                send!(heartbeat(&*src));
                next_hb = Instant::now() + hb;
            }
        }};
    }
    loop {
        if out.resets.has_changed().unwrap_or(true) {
            // Mark the reset seen, so `end_stream` waits for room instead of dropping the status.
            out.resets.borrow_and_update();
            return out.end(upstream_reset()).await;
        }
        if *out.shutdown.borrow() {
            return out.end(shutting_down()).await;
        }
        if let Some(status) = stream.violation() {
            return out.end(status).await;
        }
        // As soon as the server holds `from_seq - 1`, the client's `prev_crc32c` is checked,
        // before anything is sent (PROTOCOL.md §7.2, §7.3).
        if let Some(j) = junction.filter(|j| j.seq <= src.store().position().last_seq) {
            junction = None;
            if let Err(status) = check_junction(&*src, j) {
                return out.end(status).await;
            }
        }
        // Read ahead everything that is available (in blocks of READ_BATCH).
        if ahead.is_empty() && next <= src.store().position().last_seq {
            let s = src.clone();
            let read = tokio::task::spawn_blocking(move || s.store().read_entries(next, READ_BATCH, fetch.max_bytes)).await;
            let failure = match read {
                Ok(Ok(entries)) => {
                    // The lease only protects `S + 1`: release it once that entry is read.
                    if lease.as_ref().is_some_and(|l| entries.last().is_some_and(|e| e.seq > l.snapshot_seq)) {
                        lease = None;
                        tracing::debug!("bootstrap lease released: the entries after the snapshot are read");
                    }
                    ahead.extend(entries);
                    None
                }
                Ok(Err(e @ Error::Gap { .. })) => Some(reason_status(pb::Reason::LogGap, e.to_string())),
                Ok(Err(e)) => Some(store_status(e)),
                Err(e) => Some(internal_status(format!("log read failed: {e}"))),
            };
            if let Some(status) = failure {
                // Entries already read are valid: deliver them before ending the stream.
                if !pending.entries.is_empty() {
                    send_pending!();
                }
                return out.end(status).await;
            }
        }
        // Move available entries into the pending batch; flush when it is full or big enough.
        if let Some(entry) = ahead.pop_front() {
            if let Err(e) = entry.verify_crc() {
                return out.end(store_status(e)).await;
            }
            let size = framed_len(entry.encoded_len());
            if !pending.entries.is_empty() && pending.bytes + size > fetch.max_bytes {
                flush!();
            }
            next = entry.seq + 1;
            pending.since.get_or_insert_with(Instant::now);
            pending.bytes += size;
            pending.entries.push(entry_to_pb(entry));
            if failpoint::fire("server-tail-after-entry") {
                send_pending!();
                return out.end(Status::unavailable("failpoint server-tail-after-entry")).await;
            }
            if pending.bytes >= fetch.max_bytes || (fetch.min_bytes > 0 && pending.bytes >= fetch.min_bytes) {
                flush!();
            }
            continue;
        }
        // Everything available is pending: real-time mode sends it now, otherwise it waits
        // for min_batch_bytes or until the oldest pending entry has waited max_wait.
        let deadline = pending.since.map(|t| t + fetch.max_wait);
        if !pending.entries.is_empty() && (fetch.min_bytes == 0 || deadline.is_some_and(|d| Instant::now() >= d)) {
            flush!();
            continue;
        }
        let wake = match deadline {
            Some(d) if d < next_hb => d,
            _ => next_hb,
        };
        tokio::select! {
            r = seqs.changed() => if r.is_err() { return },
            _ = out.resets.changed() => return out.end(upstream_reset()).await,
            r = out.shutdown.changed() => if r.is_err() { return },
            _ = out.tx.closed() => return,
            v = async { stream.violation.as_mut().expect("guarded by the condition").await }, if stream.violation.is_some() => {
                match v {
                    Ok(status) => return out.end(status).await,
                    Err(_) => stream.violation = None,
                }
            }
            _ = tokio::time::sleep_until(wake) => {
                if Instant::now() >= next_hb {
                    send!(heartbeat(&*src));
                    next_hb = Instant::now() + hb;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(min: u32, wait_ms: u32, max: u32) -> pb::TailRequest {
        pb::TailRequest { min_batch_bytes: min, max_wait_ms: wait_ms, max_batch_bytes: max, ..Default::default() }
    }

    #[test]
    fn fetch_parameters_are_clamped_to_server_limits() {
        let hb = Duration::from_millis(500);
        let f = Fetch::clamp(&req(10, 100, 2048), hb, 4 << 20);
        assert_eq!(f, Fetch { min_bytes: 10, max_wait: Duration::from_millis(100), max_bytes: 2048 });
        assert_eq!(Fetch::clamp(&req(0, 60_000, 0), hb, 4 << 20).max_wait, hb, "max_wait <= heartbeat");
        assert_eq!(Fetch::clamp(&req(0, 0, 0), hb, 4 << 20).max_bytes, DEFAULT_MAX_BATCH_BYTES, "0 = 1 MiB");
        assert_eq!(Fetch::clamp(&req(0, 0, u32::MAX), hb, 4 << 20).max_bytes, 4 << 20, "capped");
        assert_eq!(Fetch::clamp(&req(0, 0, 0), hb, 1000).max_bytes, 1000, "default is capped too");
    }

    #[test]
    fn framing_counts_tag_and_length_varint() {
        assert_eq!(framed_len(0), 2);
        assert_eq!(framed_len(127), 127 + 2);
        assert_eq!(framed_len(128), 128 + 3);
        assert_eq!(framed_len(16_383), 16_383 + 3);
        assert_eq!(framed_len(16_384), 16_384 + 4);
    }

    /// A reset between validation and the first poll of the producer is not missed: the
    /// receiver is subscribed (and marked seen) first, as the handler does.
    #[tokio::test]
    async fn a_reset_after_subscription_but_before_streaming_ends_the_stream() {
        use crate::server::test_client::{serving_master, within};
        use crate::types::CfSpec;
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| cfg.serve = None).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let src = m.log_source();
        let mut resets = src.resets();
        resets.borrow_and_update();
        m.bump_epoch().unwrap();
        let (tx, rx) = mpsc::channel(CHANNEL);
        let (_shutdown_tx, shutdown) = watch::channel(false);
        let fetch = Fetch::clamp(&req(0, 0, 0), Duration::from_secs(1), 1 << 20);
        let registry = Arc::new(super::super::replicas::ReplicaRegistry::default());
        let replica = registry.open(NewReplica {
            node_id: "b".into(),
            remote_addr: "127.0.0.1:1".parse().unwrap(),
            peer_verified: false,
            epoch: 1,
            has_up_to: 0,
            fetch: fetch.settings(),
        });
        let (_violation_tx, violation) = oneshot::channel();
        let (done, _done_rx) = oneshot::channel();
        let stream = Stream { replica, violation: Some(violation), _done: done };
        tokio::spawn(produce(src, Out { tx, shutdown, resets }, 1, fetch, None, stream, None));
        let mut rx = rx;
        let err = within("the stream status", async {
            loop {
                match rx.recv().await {
                    Some(Err(e)) => return e,
                    Some(Ok(_)) => {}
                    None => panic!("stream ended without a status"),
                }
            }
        })
        .await;
        assert_eq!(crate::net::reason_of(&err), Some(pb::Reason::UpstreamReset));
    }

    /// Starts a producer over `m` at `from` (no validation) and returns its receiving end.
    fn producer(m: &crate::master::Master, from: Seq, junction: Option<Junction>) -> mpsc::Receiver<Result<pb::TailMessage, Status>> {
        let src = m.log_source();
        let mut resets = src.resets();
        resets.borrow_and_update();
        let (tx, rx) = mpsc::channel(CHANNEL);
        let (shutdown_tx, shutdown) = watch::channel(false);
        std::mem::forget(shutdown_tx); // keep the stream open for the test's duration
        let fetch = Fetch::clamp(&req(0, 0, 0), Duration::from_secs(1), 1 << 20);
        let registry = Arc::new(super::super::replicas::ReplicaRegistry::default());
        let replica = registry.open(NewReplica {
            node_id: "b".into(),
            remote_addr: "127.0.0.1:1".parse().unwrap(),
            peer_verified: false,
            epoch: 1,
            has_up_to: from - 1,
            fetch: fetch.settings(),
        });
        let (violation_tx, violation) = oneshot::channel();
        std::mem::forget(violation_tx);
        let (done, _done_rx) = oneshot::channel();
        let stream = Stream { replica, violation: Some(violation), _done: done };
        tokio::spawn(produce(src, Out { tx, shutdown, resets }, from, fetch, None, stream, junction));
        rx
    }

    /// The first entry batch, or the status that ended the stream.
    async fn first_batch_or_end(rx: &mut mpsc::Receiver<Result<pb::TailMessage, Status>>) -> Result<Vec<pb::LogEntry>, Status> {
        crate::server::test_client::within("a batch or the end of the stream", async {
            loop {
                match rx.recv().await {
                    Some(Ok(pb::TailMessage { kind: Some(Kind::Batch(b)) })) => return Ok(b.entries),
                    Some(Ok(_)) => {}
                    Some(Err(e)) => return Err(e),
                    None => panic!("stream ended without a status"),
                }
            }
        })
        .await
    }

    /// I4: a client ahead of this server (a replica that crashed unsynced) sent `prev_crc32c`
    /// for an entry the server did not hold yet. Once the server holds it, the CRC is checked
    /// before `from_seq` is sent: a different entry ends the stream DIVERGED, the same one
    /// streams normally.
    #[tokio::test]
    async fn the_junction_crc_is_checked_once_the_server_catches_up() {
        use crate::server::test_client::serving_master;
        use crate::types::CfSpec;
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| cfg.serve = None).await;
        m.create_cf(CfSpec::new("c")).unwrap(); // seq 1

        // The client claims entry 2 has CRC 0xDEAD; the server writes 2 and 3 later.
        let mut diverged = producer(&m, 3, Some(Junction { seq: 2, crc: 0xDEAD }));
        // The client's entry 2 is the server's (CRC known after the write below).
        m.put("c", b"k", b"v").unwrap(); // seq 2
        let crc2 = m.log_source().store().entry_crc(2).unwrap().unwrap();
        let mut matching = producer(&m, 4, Some(Junction { seq: 3, crc: 0 }));
        m.put("c", b"k2", b"v").unwrap(); // seq 3
        let e = first_batch_or_end(&mut diverged).await.expect_err("entry 2 differs");
        assert_eq!(crate::net::reason_of(&e), Some(pb::Reason::Diverged), "{e:?}");
        let e = first_batch_or_end(&mut matching).await.expect_err("entry 3 differs (crc 0)");
        assert_eq!(crate::net::reason_of(&e), Some(pb::Reason::Diverged), "{e:?}");

        let mut ok = producer(&m, 3, Some(Junction { seq: 2, crc: crc2 }));
        let batch = first_batch_or_end(&mut ok).await.expect("entry 2 matches");
        assert_eq!(batch[0].seq, 3);
        // Ahead with a matching junction: nothing is sent until the server catches up.
        let crc3 = m.log_source().store().entry_crc(3).unwrap().unwrap();
        let mut ahead = producer(&m, 5, Some(Junction { seq: 4, crc: 0 }));
        let mut fine = producer(&m, 4, Some(Junction { seq: 3, crc: crc3 }));
        m.put("c", b"k3", b"v").unwrap(); // seq 4
        m.put("c", b"k4", b"v").unwrap(); // seq 5
        assert_eq!(first_batch_or_end(&mut fine).await.unwrap()[0].seq, 4);
        assert!(first_batch_or_end(&mut ahead).await.is_err(), "entry 4 differs (crc 0)");
    }
}
