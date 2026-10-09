//! Tail: validation (PROTOCOL.md §7.2) and the per-stream producer (§7.3).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::{call_info, store_status, Lease, ServerState, TailStream};
use crate::auth::Method;
use crate::clock::{Clock, SystemClock};
use crate::error::Error;
use crate::failpoint;
use crate::net::convert::{entry_to_pb, identity_from_pb};
use crate::net::{internal_status, reason_status};
use crate::pb;
use crate::pb::tail_message::Kind;
use crate::source::LogSource;
use crate::types::{LogEntry, Role, Seq};

/// Entries read from the changelog per blocking read.
pub(crate) const READ_BATCH: usize = 64;
/// Messages buffered per stream before back-pressure applies.
const CHANNEL: usize = 16;

pub(super) async fn tail(state: &Arc<ServerState>, req: Request<pb::TailRequest>) -> Result<Response<TailStream>, Status> {
    let call = call_info(&req, Method::Tail)?;
    let src = state.authorize(&call, Method::Tail).await?;
    let r = req.into_inner();
    call.check_client_id(&r.client_id)?;
    // Subscribe before validating, so a reset after validation is never missed.
    let mut resets = src.resets();
    resets.borrow_and_update();
    validate(&*src, &r)?;
    state.expire_leases(std::time::Instant::now());
    let lease = r.bootstrap_lease.as_deref().and_then(|id| state.take_lease(id, &r.client_id, r.from_seq));
    let (tx, rx) = mpsc::channel(CHANNEL);
    let fetch = Fetch::clamp(&r, src.heartbeat(), state.cfg.limits.max_tail_batch_bytes);
    tokio::spawn(produce(src, tx, r.from_seq, fetch, lease, state.shutdown.clone(), resets));
    Ok(Response::new(ReceiverStream::new(rx)))
}

fn validate(src: &dyn LogSource, r: &pb::TailRequest) -> Result<(), Status> {
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
    if let Some(prev) = r.prev_crc32c {
        if let Some(have) = store.entry_crc(r.from_seq - 1).map_err(store_status)? {
            if have != prev {
                return Err(reason_status(pb::Reason::Diverged, format!("entry {} differs", r.from_seq - 1)));
            }
        }
    }
    let first = store.first_retained_seq();
    if r.from_seq < first {
        return Err(reason_status(pb::Reason::LogGap, format!("from_seq {} < first retained {first}", r.from_seq)));
    }
    Ok(())
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
    tx: Tx,
    mut next: Seq,
    fetch: Fetch,
    mut lease: Option<Lease>,
    shutdown: watch::Receiver<bool>,
    resets: watch::Receiver<u64>,
) {
    let hb: Duration = src.heartbeat();
    let mut seqs = src.subscribe();
    let mut out = Out { tx, shutdown, resets };
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
    macro_rules! flush {
        () => {{
            send!(pending.take());
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
                    send!(pending.take());
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
                send!(pending.take());
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
        tokio::spawn(produce(src, tx, 1, fetch, None, shutdown, resets));
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
}
