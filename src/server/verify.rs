//! Verify sessions (PROTOCOL.md §9). `VerifyBegin` pins a snapshot and reads `S`, the
//! identity and the CF list through it; `VerifyRange` answers `HashRange` and `ListRange`
//! from that snapshot, in request order; `VerifyEnd` (or `verify_session_timeout` of
//! idleness) releases it. A session never outlives a reset of the node's state, nor the
//! server's shutdown, so it can never keep a closed node's database open.

use std::collections::HashMap;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, MutexGuard, PoisonError};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status, Streaming};

use super::{ServerState, VerifyRangeStream, call_info, store_status};
use crate::auth::Method;
use crate::net::convert::{cf_to_pb, identity_to_pb};
use crate::net::{self, internal_status, reason_status};
use crate::pb;
use crate::pb::verify_range_request::Kind as ReqKind;
use crate::pb::verify_range_response::Kind as RespKind;
use crate::read::ReadSnapshot;
use crate::store::cfs::PhysCf;
use crate::types::Seq;
use crate::verify::range::{Bounds, KeyOrder, Lister, RangeHasher};
use crate::verify::scan;
use crate::verify::{LIST_SOFT_BYTES, MAX_LIST_LIMIT, VerifyMode};

/// Computations running at once on one server (each holds a blocking thread and a scan).
pub(crate) const MAX_COMPUTATIONS: usize = 8;

/// Answers buffered per stream (the client pipelines at most `VERIFY_WINDOW` = 16 requests).
const CHANNEL: usize = 16;

/// One open session: a snapshot at `S` and what is needed to read it.
pub(crate) struct Session {
    #[cfg(test)]
    id: [u8; 16],
    snap: ReadSnapshot,
    mode: VerifyMode,
    /// `S` and the CRC of entry `S`, read through the snapshot.
    seq: Seq,
    last_crc: Option<u32>,
    cfs: HashMap<String, (Arc<PhysCf>, KeyOrder)>,
    /// The node's reset generation when the session was opened; a later reset ends it.
    reset_gen: u64,
    span: tracing::Span,
    last_used: Mutex<Instant>,
}

impl Session {
    fn touch(&self) {
        *self
            .last_used
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Instant::now();
    }

    fn idle_since(&self) -> Instant {
        *self
            .last_used
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
    }
}

/// The open sessions of one server, bounded by `max_verify_sessions`.
pub(crate) struct Sessions {
    map: Mutex<HashMap<[u8; 16], Arc<Session>>>,
    max: usize,
    timeout: Duration,
}

fn session_not_found() -> Status {
    reason_status(
        pb::Reason::SessionNotFound,
        "unknown or expired verify session",
    )
}

impl Sessions {
    pub(crate) fn new(max: usize, timeout: Duration) -> Self {
        Sessions {
            map: Mutex::new(HashMap::new()),
            max,
            timeout,
        }
    }

    fn map(&self) -> MutexGuard<'_, HashMap<[u8; 16], Arc<Session>>> {
        self.map.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Drops sessions idle for longer than the timeout and, given the node's current reset
    /// generation, those opened before the last reset.
    pub(crate) fn expire(&self, now: Instant, reset_gen: Option<u64>) {
        let timeout = self.timeout;
        self.map().retain(|_, s| {
            now.saturating_duration_since(s.idle_since()) <= timeout
                && reset_gen.is_none_or(|g| g == s.reset_gen)
        });
    }

    fn is_full(&self) -> bool {
        self.map().len() >= self.max
    }

    fn insert(&self, id: [u8; 16], s: Session) -> Result<(), Status> {
        let mut map = self.map();
        if map.len() >= self.max {
            return Err(reason_status(pb::Reason::Busy, "too many verify sessions"));
        }
        map.insert(id, Arc::new(s));
        Ok(())
    }

    /// The session `id`, touched. `SESSION_NOT_FOUND` if unknown or idle too long.
    fn get(&self, id: &[u8]) -> Result<Arc<Session>, Status> {
        let key = <[u8; 16]>::try_from(id).map_err(|_| session_not_found())?;
        let mut map = self.map();
        let Some(s) = map.get(&key) else {
            return Err(session_not_found());
        };
        if Instant::now().saturating_duration_since(s.idle_since()) > self.timeout {
            map.remove(&key);
            return Err(session_not_found());
        }
        s.touch();
        Ok(s.clone())
    }

    fn remove(&self, id: &[u8]) -> bool {
        <[u8; 16]>::try_from(id).is_ok_and(|key| self.map().remove(&key).is_some())
    }

    /// Drops every session (server shutdown).
    pub(crate) fn clear(&self) {
        self.map().clear();
    }
}

pub(super) async fn begin(
    state: &Arc<ServerState>,
    req: Request<pb::VerifyBeginRequest>,
) -> Result<Response<pb::VerifyBeginResponse>, Status> {
    // Test hook: a stalled upstream.
    crate::failpoint::pause("server-verify-begin").await;
    let call = call_info(&req, Method::Verify)?;
    let src = state.authorize(&call, Method::Verify).await?;
    let r = req.into_inner();
    call.check_client_id(&r.client_id)?;
    let mode = VerifyMode::from_pb(r.mode)
        .ok_or_else(|| net::invalid_argument("mode must be QUICK or FULL"))?;
    if state.is_shutting_down() {
        return Err(reason_status(
            pb::Reason::ShuttingDown,
            "server is shutting down",
        ));
    }
    // Read the generation before readiness and the snapshot, so a reset in between is seen.
    let reset_gen = *src.resets().borrow();
    if !src.is_ready() {
        return Err(reason_status(
            pb::Reason::NotReady,
            "server is bootstrapping",
        ));
    }
    state
        .verify_sessions
        .expire(Instant::now(), Some(reset_gen));
    if state.verify_sessions.is_full() {
        return Err(reason_status(pb::Reason::Busy, "too many verify sessions"));
    }
    let comparators = src.store().tuning().comparators.clone();
    let node_id = src.node_id().to_string();
    let upstream = src.upstream_chain().into_iter().next();
    let s = src.clone();
    let (snap, meta) = tokio::task::spawn_blocking(move || -> crate::Result<_> {
        let snap = s.store().read_snapshot(0)?;
        let meta = snap.meta()?;
        Ok((snap, meta))
    })
    .await
    .map_err(|e| internal_status(format!("snapshot task failed: {e}")))?
    .map_err(store_status)?;
    drop(src);
    let Some(identity) = meta.identity else {
        return Err(reason_status(
            pb::Reason::NotReady,
            "server has no data yet",
        ));
    };
    let mut cfs = HashMap::new();
    for e in &meta.cf_specs {
        let phys = snap
            .phys(&e.spec.name)
            .filter(|p| p.entry == *e)
            .ok_or_else(|| {
                reason_status(
                    pb::Reason::Corrupt,
                    format!("CF {} is missing from the snapshot", e.spec.name),
                )
            })?;
        let order = order_for(&comparators, e.spec.comparator.as_deref())?;
        cfs.insert(e.spec.name.clone(), (phys, order));
    }
    let span = tracing::info_span!(
        "verify",
        node_id = %node_id,
        upstream = upstream.as_deref(),
        client = %r.client_id,
        seq = meta.last_seq,
        epoch = identity.epoch,
        mode = ?mode,
    );
    span.in_scope(|| tracing::info!(cfs = cfs.len(), "verify session opened"));
    let id = *uuid::Uuid::new_v4().as_bytes();
    let session = Session {
        #[cfg(test)]
        id,
        snap,
        mode,
        seq: meta.last_seq,
        last_crc: meta.last_entry_crc,
        cfs,
        reset_gen,
        span,
        last_used: Mutex::new(Instant::now()),
    };
    state.verify_sessions.insert(id, session)?;
    Ok(Response::new(pb::VerifyBeginResponse {
        session_id: id.to_vec(),
        identity: Some(identity_to_pb(&identity)),
        snapshot_seq: meta.last_seq,
        cf_specs: meta.cf_specs.iter().map(cf_to_pb).collect(),
        session_timeout_ms: u64::try_from(state.cfg.limits.verify_session_timeout.as_millis())
            .unwrap_or(u64::MAX),
    }))
}

/// The key order of a CF, or `INTERNAL` if its comparator is not registered on this node.
fn order_for(
    reg: &crate::tuning::ComparatorRegistry,
    name: Option<&str>,
) -> Result<KeyOrder, Status> {
    KeyOrder::new(reg, name).map_err(|_| {
        internal_status(format!(
            "comparator {} is not registered on this node",
            name.unwrap_or_default()
        ))
    })
}

pub(super) async fn end(
    state: &Arc<ServerState>,
    req: Request<pb::VerifyEndRequest>,
) -> Result<Response<pb::VerifyEndResponse>, Status> {
    let call = call_info(&req, Method::Verify)?;
    drop(state.authorize(&call, Method::Verify).await?);
    if state.verify_sessions.remove(&req.into_inner().session_id) {
        Ok(Response::new(pb::VerifyEndResponse {}))
    } else {
        Err(session_not_found())
    }
}

pub(super) async fn range(
    state: &Arc<ServerState>,
    req: Request<Streaming<pb::VerifyRangeRequest>>,
) -> Result<Response<VerifyRangeStream>, Status> {
    let call = call_info(&req, Method::Verify)?;
    let src = state.authorize(&call, Method::Verify).await?;
    let mut resets = src.resets();
    let reset_gen = *resets.borrow_and_update();
    drop(src);
    let (tx, rx) = mpsc::channel(CHANNEL);
    tokio::spawn(serve(
        state.clone(),
        req.into_inner(),
        tx,
        resets,
        reset_gen,
    ));
    Ok(Response::new(ReceiverStream::new(rx)))
}

type Tx = mpsc::Sender<Result<pb::VerifyRangeResponse, Status>>;

/// Answers the requests of one `VerifyRange` stream in order, until the client closes it,
/// a request fails, the node's state is reset, or the server shuts down.
async fn serve(
    state: Arc<ServerState>,
    mut input: Streaming<pb::VerifyRangeRequest>,
    tx: Tx,
    mut resets: watch::Receiver<u64>,
    reset_gen: u64,
) {
    let mut shutdown = state.shutdown.clone();
    let status = loop {
        let req = tokio::select! {
            m = input.message() => match m {
                Ok(Some(req)) => req,
                // The client closed its side, or the connection broke.
                Ok(None) | Err(_) => return,
            },
            _ = super::stopped(&mut shutdown) => break reason_status(pb::Reason::ShuttingDown, "server is shutting down"),
            _ = resets.changed() => break reason_status(pb::Reason::UpstreamReset, "server state was replaced"),
            _ = tx.closed() => return,
        };
        let answer = match answer(&state, req, reset_gen, &tx, &mut resets).await {
            Ok(a) => a,
            Err(status) => break status,
        };
        tokio::select! {
            r = tx.send(Ok(answer)) => if r.is_err() { return },
            _ = super::stopped(&mut shutdown) => return,
        }
    };
    super::end_stream(&tx, status, &mut shutdown, &mut resets).await;
}

async fn answer(
    state: &ServerState,
    req: pb::VerifyRangeRequest,
    reset_gen: u64,
    tx: &Tx,
    resets: &mut watch::Receiver<u64>,
) -> Result<pb::VerifyRangeResponse, Status> {
    let shutting_down = || reason_status(pb::Reason::ShuttingDown, "server is shutting down");
    // At most `MAX_COMPUTATIONS` scans run at once on this server. A request waits for a free
    // slot rather than failing: the client already limits itself to `VERIFY_WINDOW` requests
    // per session, so waiting is the natural backpressure (PROTOCOL.md §9.4). No session is
    // held while waiting.
    // An unknown session is refused before any waiting. The wait also ends when the client
    // is gone or the node's state is replaced: a dead client must not run a scan.
    state.verify_sessions.get(&req.session_id)?;
    let mut shutdown = state.shutdown.clone();
    let permit = tokio::select! {
        p = state.compute_permits.clone().acquire_owned() => p.map_err(|_| shutting_down())?,
        _ = super::stopped(&mut shutdown) => return Err(shutting_down()),
        _ = resets.changed() => return Err(reason_status(
            pb::Reason::UpstreamReset,
            "server state was replaced",
        )),
        _ = tx.closed() => return Err(Status::cancelled("client went away")),
    };
    let session = state.verify_sessions.get(&req.session_id)?;
    if session.reset_gen != reset_gen {
        state.verify_sessions.remove(&req.session_id);
        return Err(reason_status(
            pb::Reason::UpstreamReset,
            "server state was replaced since the session began",
        ));
    }
    let kind = req
        .kind
        .ok_or_else(|| net::invalid_argument("VerifyRangeRequest needs a request kind"))?;
    let cancel = state.cancel.clone();
    // The closure owns the session and the permit and drops the session first, so once
    // `shutdown` holds every permit no computation references the database any more.
    let kind = tokio::task::spawn_blocking(move || {
        let r = compute(&session, kind, &cancel);
        // Idle time counts from the end of the last request.
        session.touch();
        drop(session);
        drop(permit);
        r
    })
    .await
    .map_err(|e| internal_status(format!("verify task failed: {e}")))??;
    Ok(pb::VerifyRangeResponse {
        request_id: req.request_id,
        kind: Some(kind),
    })
}

fn target<'s>(
    s: &'s Session,
    r: Option<&pb::KeyRange>,
) -> Result<(&'s Arc<PhysCf>, &'s KeyOrder, Bounds), Status> {
    let r = r.ok_or_else(|| net::invalid_argument("range is required"))?;
    let (phys, order) = s.cfs.get(&r.cf).ok_or_else(|| {
        net::invalid_argument(format!(
            "column family {:?} is not in this session's snapshot",
            r.cf
        ))
    })?;
    Ok((phys, order, Bounds::from_pb(r)))
}

/// Runs on a blocking thread. Checks `cancel` before every key and stops with
/// `SHUTTING_DOWN` when it is set.
fn compute(s: &Session, kind: ReqKind, cancel: &AtomicBool) -> Result<RespKind, Status> {
    #[cfg(test)]
    hooks::pause(&s.id, cancel);
    let _span = s.span.enter();
    let stopped = || reason_status(pb::Reason::ShuttingDown, "server is shutting down");
    if cancel.load(Ordering::Relaxed) {
        return Err(stopped());
    }
    match kind {
        ReqKind::Hash(h) => {
            let (phys, order, bounds) = target(s, h.range.as_ref())?;
            let mut hasher = RangeHasher::new(s.mode);
            let done = scan::scan_until(
                &s.snap,
                phys.clone(),
                order,
                &bounds,
                Some(cancel),
                |k, v| hasher.add(k, v).map(|()| true),
            )
            .map_err(store_status)?;
            if !done {
                return Err(stopped());
            }
            let (count, hash) = hasher.finish();
            let matched = count == h.count && hash.as_slice() == h.hash.as_slice();
            Ok(RespKind::Hash(pb::HashResult {
                matched,
                count,
                hash: hash.to_vec(),
            }))
        }
        ReqKind::List(l) => {
            if l.limit == 0 || l.limit > MAX_LIST_LIMIT {
                return Err(net::invalid_argument(format!(
                    "ListRange.limit must be 1..={MAX_LIST_LIMIT}"
                )));
            }
            let (phys, order, bounds) = target(s, l.range.as_ref())?;
            let mut lister = Lister::new(s.mode, l.limit as usize, LIST_SOFT_BYTES);
            let done = scan::scan_until(
                &s.snap,
                phys.clone(),
                order,
                &bounds,
                Some(cancel),
                |k, v| lister.offer(k, v),
            )
            .map_err(store_status)?;
            if !done {
                return Err(stopped());
            }
            Ok(RespKind::List(pb::ListResult {
                keys: lister.keys,
                truncated: lister.truncated,
            }))
        }
        ReqKind::FetchKeys(f) => {
            if f.keys.is_empty() || f.keys.len() > MAX_LIST_LIMIT as usize {
                return Err(net::invalid_argument(format!(
                    "FetchKeys takes 1..={MAX_LIST_LIMIT} keys"
                )));
            }
            let range = pb::KeyRange {
                cf: f.cf,
                lower: None,
                upper: None,
            };
            let (phys, _, _) = target(s, Some(&range))?;
            let ctx = s.snap.raw_ctx();
            let (mut records, mut bytes, mut truncated) = (Vec::new(), 0usize, false);
            for key in f.keys {
                if cancel.load(Ordering::Relaxed) {
                    return Err(stopped());
                }
                // Cut the answer at about `LIST_SOFT_BYTES`, but always answer one key.
                if !records.is_empty() && bytes >= LIST_SOFT_BYTES {
                    truncated = true;
                    break;
                }
                let envelope = ctx.get_raw(phys, &key).map_err(store_status)?;
                bytes += key.len() + envelope.as_ref().map_or(0, Vec::len);
                records.push(pb::FetchedRecord { key, envelope });
            }
            Ok(RespKind::Fetch(pb::FetchResult { records, truncated }))
        }
        ReqKind::FetchRange(f) => {
            if f.limit == 0 || f.limit > MAX_LIST_LIMIT {
                return Err(net::invalid_argument(format!(
                    "FetchRange.limit must be 1..={MAX_LIST_LIMIT}"
                )));
            }
            let (phys, order, bounds) = target(s, f.range.as_ref())?;
            let fetched = scan::fetch_range(
                &s.snap,
                phys.clone(),
                order,
                &bounds,
                f.limit as usize,
                LIST_SOFT_BYTES,
                cancel,
            )
            .map_err(store_status)?;
            let Some((records, truncated)) = fetched else {
                return Err(stopped());
            };
            Ok(RespKind::Fetch(pb::FetchResult { records, truncated }))
        }
        ReqKind::EntryCrcs(e) => {
            if e.from_seq > e.to_seq || e.to_seq > s.seq {
                return Err(net::invalid_argument(format!(
                    "EntryCrcs needs from_seq <= to_seq <= {} (the snapshot's seq)",
                    s.seq
                )));
            }
            // Read through the session's snapshot: entries up to S never change, and a log
            // that moves on (retention, a repair or bootstrap of this node) does not matter.
            let last = e
                .to_seq
                .min(e.from_seq.saturating_add(u64::from(MAX_LIST_LIMIT) - 1));
            let mut entries = Vec::new();
            for seq in e.from_seq.max(1)..=last {
                if cancel.load(Ordering::Relaxed) {
                    return Err(stopped());
                }
                let crc = match s.snap.entry_crc(seq).map_err(store_status)? {
                    None if seq == s.seq => s.last_crc,
                    other => other,
                };
                if let Some(crc32c) = crc {
                    entries.push(pb::SeqCrc { seq, crc32c });
                }
            }
            Ok(RespKind::EntryCrcs(pb::EntryCrcResult { entries }))
        }
    }
}
/// Test hook: makes the computations of chosen sessions slow.
#[cfg(test)]
pub(crate) mod hooks {
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex, PoisonError};
    use std::time::{Duration, Instant};

    pub(crate) struct Slow {
        pub ignore_cancel: bool,
        pub started: Arc<AtomicBool>,
    }

    pub(crate) static SLOW: Mutex<Vec<(Vec<u8>, Slow)>> = Mutex::new(Vec::new());

    pub(crate) fn slow(id: &[u8], ignore_cancel: bool) -> Arc<AtomicBool> {
        let started = Arc::new(AtomicBool::new(false));
        SLOW.lock().unwrap_or_else(PoisonError::into_inner).push((
            id.to_vec(),
            Slow {
                ignore_cancel,
                started: started.clone(),
            },
        ));
        started
    }

    /// Blocks the computing thread for a while (6 s if it ignores cancellation, else up to 8 s
    /// or until `cancel`).
    pub(crate) fn pause(id: &[u8], cancel: &AtomicBool) {
        let (ignore, started) = {
            let slow = SLOW.lock().unwrap_or_else(PoisonError::into_inner);
            match slow.iter().find(|(k, _)| k == id) {
                Some((_, s)) => (s.ignore_cancel, s.started.clone()),
                None => return,
            }
        };
        started.store(true, Ordering::SeqCst);
        let end = Instant::now() + Duration::from_secs(if ignore { 6 } else { 8 });
        while Instant::now() < end && (ignore || !cancel.load(Ordering::SeqCst)) {
            std::thread::sleep(Duration::from_millis(10));
        }
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::Code;

    use crate::codec::envelope;
    use crate::log::LogApi;
    use crate::net::convert::identity_to_pb;
    use crate::net::reason_of;
    use crate::pb;
    use crate::pb::verify_range_request::Kind as Req;
    use crate::pb::verify_range_response::Kind as Resp;
    use crate::server::test_client::{Client, T0, client, serving_master};
    use crate::types::CfSpec;
    use crate::verify::VerifyMode;
    use crate::verify::range::RangeHasher;

    async fn begin(
        c: &mut Client,
        mode: pb::VerifyMode,
    ) -> Result<pb::VerifyBeginResponse, tonic::Status> {
        c.verify_begin(pb::VerifyBeginRequest {
            client_id: "b".into(),
            mode: mode as i32,
        })
        .await
        .map(|r| r.into_inner())
    }

    fn range(lower: Option<&[u8]>, upper: Option<&[u8]>) -> pb::KeyRange {
        pb::KeyRange {
            cf: "c".into(),
            lower: lower.map(<[u8]>::to_vec),
            upper: upper.map(<[u8]>::to_vec),
        }
    }

    fn hash(
        session: &[u8],
        id: u64,
        r: pb::KeyRange,
        count: u64,
        hash: Vec<u8>,
    ) -> pb::VerifyRangeRequest {
        let kind = Req::Hash(pb::HashRange {
            range: Some(r),
            count,
            hash,
        });
        pb::VerifyRangeRequest {
            session_id: session.to_vec(),
            request_id: id,
            kind: Some(kind),
        }
    }

    fn list(session: &[u8], id: u64, r: pb::KeyRange, limit: u32) -> pb::VerifyRangeRequest {
        let kind = Req::List(pb::ListRange {
            range: Some(r),
            limit,
        });
        pb::VerifyRangeRequest {
            session_id: session.to_vec(),
            request_id: id,
            kind: Some(kind),
        }
    }

    /// Sends `reqs` on one `VerifyRange` stream; returns the answers and the error that ended it.
    async fn exchange(
        c: &mut Client,
        reqs: Vec<pb::VerifyRangeRequest>,
    ) -> (Vec<pb::VerifyRangeResponse>, Option<tonic::Status>) {
        let mut s = match c.verify_range(tokio_stream::iter(reqs)).await {
            Ok(r) => r.into_inner(),
            Err(e) => return (Vec::new(), Some(e)),
        };
        let mut out = Vec::new();
        loop {
            match s.message().await {
                Ok(Some(m)) => out.push(m),
                Ok(None) => return (out, None),
                Err(e) => return (out, Some(e)),
            }
        }
    }

    fn reason(e: &tonic::Status) -> (Code, Option<pb::Reason>) {
        (e.code(), reason_of(e))
    }

    #[tokio::test]
    async fn a_session_answers_hashes_and_listings_from_its_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        for k in [&b""[..], b"a", b"b", b"c"] {
            m.put("c", k, b"v").unwrap(); // seqs 2..=5
        }
        let mut c = client(m.serve_addr().unwrap(), "b");
        let s = begin(&mut c, pb::VerifyMode::Full).await.unwrap();
        assert_eq!(s.snapshot_seq, 5);
        assert_eq!(s.identity, Some(identity_to_pb(&m.identity())));
        assert_eq!(s.cf_specs.len(), 1);
        assert_eq!(s.session_timeout_ms, 300_000);
        // Later writes are not in the session's snapshot.
        m.put("c", b"later", b"v").unwrap();
        m.delete("c", b"a").unwrap();

        // What a client holding "" (seq 2) and "a" (seq 3) sends for ["", "b").
        let mut h = RangeHasher::new(VerifyMode::Full);
        h.add(b"", &envelope::encode(2, None, b"v")).unwrap();
        h.add(b"a", &envelope::encode(3, None, b"v")).unwrap();
        let (count, digest) = h.finish();
        let sid = s.session_id.clone();
        let (answers, err) = exchange(
            &mut c,
            vec![
                hash(
                    &sid,
                    1,
                    range(Some(b""), Some(b"b")),
                    count,
                    digest.to_vec(),
                ),
                hash(&sid, 2, range(None, None), 0, vec![]),
                list(&sid, 3, range(Some(b"a"), None), 2),
            ],
        )
        .await;
        assert!(err.is_none(), "{err:?}");
        assert_eq!(
            answers.iter().map(|a| a.request_id).collect::<Vec<_>>(),
            vec![1, 2, 3],
            "answered in order"
        );
        match &answers[0].kind {
            Some(Resp::Hash(r)) => assert!(r.matched && r.count == 2, "{r:?}"),
            other => panic!("{other:?}"),
        }
        match &answers[1].kind {
            Some(Resp::Hash(r)) => assert_eq!((r.matched, r.count), (false, 4)),
            other => panic!("{other:?}"),
        }
        match &answers[2].kind {
            Some(Resp::List(l)) => {
                assert_eq!(
                    l.keys.iter().map(|k| k.key.clone()).collect::<Vec<_>>(),
                    vec![b"a".to_vec(), b"b".to_vec()]
                );
                assert!(l.truncated);
                assert_eq!(l.keys[0].write_seq, 3);
                assert_eq!(
                    l.keys[0].payload_hash,
                    blake3::hash(&envelope::encode(3, None, b"v"))
                        .as_bytes()
                        .to_vec()
                );
            }
            other => panic!("{other:?}"),
        }
        c.verify_end(pb::VerifyEndRequest {
            session_id: sid.clone(),
        })
        .await
        .unwrap();
        let (_, err) = exchange(&mut c, vec![hash(&sid, 4, range(None, None), 0, vec![])]).await;
        assert_eq!(
            reason(&err.unwrap()),
            (Code::NotFound, Some(pb::Reason::SessionNotFound))
        );
    }

    fn request(session: &[u8], id: u64, kind: Req) -> pb::VerifyRangeRequest {
        pb::VerifyRangeRequest {
            session_id: session.to_vec(),
            request_id: id,
            kind: Some(kind),
        }
    }

    /// Fetches answer from the session's snapshot; entry CRCs from the log, up to `S`.
    #[tokio::test]
    async fn fetches_and_entry_crcs_answer_at_the_snapshot_seq() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        for k in [&b"a"[..], b"b", b"c"] {
            m.put("c", k, b"v").unwrap(); // seqs 2..=4
        }
        let mut c = client(m.serve_addr().unwrap(), "b");
        let sid = begin(&mut c, pb::VerifyMode::Quick)
            .await
            .unwrap()
            .session_id;
        m.put("c", b"a", b"later").unwrap(); // seq 5: after the snapshot
        m.put("c", b"b", b"later").unwrap();
        m.delete("c", b"c").unwrap();
        m.put("c", b"d", b"new").unwrap();
        let keys = vec![b"a".to_vec(), b"zz".to_vec()];
        let (answers, err) = exchange(
            &mut c,
            vec![
                request(
                    &sid,
                    1,
                    Req::FetchKeys(pb::FetchKeys {
                        cf: "c".into(),
                        keys,
                    }),
                ),
                request(
                    &sid,
                    2,
                    Req::FetchRange(pb::FetchRange {
                        range: Some(range(Some(b"b"), None)),
                        limit: 1024,
                    }),
                ),
                request(
                    &sid,
                    3,
                    Req::EntryCrcs(pb::EntryCrcs {
                        from_seq: 3,
                        to_seq: 4,
                    }),
                ),
            ],
        )
        .await;
        assert!(err.is_none(), "{err:?}");
        match &answers[0].kind {
            Some(Resp::Fetch(f)) => {
                assert_eq!(f.records.len(), 2);
                assert_eq!(
                    f.records[0].envelope.as_deref(),
                    Some(envelope::encode(2, None, b"v").as_slice()),
                    "the snapshot's value"
                );
                assert_eq!(f.records[1].envelope, None, "no such key");
                assert!(!f.truncated);
            }
            other => panic!("{other:?}"),
        }
        match &answers[1].kind {
            Some(Resp::Fetch(f)) => {
                assert_eq!(
                    f.records.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
                    vec![b"b".to_vec(), b"c".to_vec()]
                );
                // The snapshot's values: b is not rewritten, c is not deleted, d is absent.
                assert_eq!(
                    f.records
                        .iter()
                        .map(|r| r.envelope.clone())
                        .collect::<Vec<_>>(),
                    vec![
                        Some(envelope::encode(3, None, b"v")),
                        Some(envelope::encode(4, None, b"v"))
                    ]
                );
            }
            other => panic!("{other:?}"),
        }
        let expected: Vec<pb::SeqCrc> = m
            .entries_since(2)
            .unwrap()
            .map(Result::unwrap)
            .filter(|e| e.seq <= 4)
            .map(|e| pb::SeqCrc {
                seq: e.seq,
                crc32c: e.crc32c,
            })
            .collect();
        assert_eq!(expected.len(), 2, "entries 3 and 4 were compared");
        match &answers[2].kind {
            Some(Resp::EntryCrcs(r)) => assert_eq!(r.entries, expected),
            other => panic!("{other:?}"),
        }
    }

    /// Malformed fetches end the stream with INVALID_ARGUMENT.
    #[tokio::test]
    async fn malformed_fetches_are_invalid_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        m.put("c", b"a", b"v").unwrap(); // S = 2
        let mut c = client(m.serve_addr().unwrap(), "b");
        let sid = begin(&mut c, pb::VerifyMode::Quick)
            .await
            .unwrap()
            .session_id;
        let keys = |cf: &str, n: usize| {
            Req::FetchKeys(pb::FetchKeys {
                cf: cf.into(),
                keys: vec![vec![1]; n],
            })
        };
        let fetch_range = |limit: u32, range: Option<pb::KeyRange>| {
            Req::FetchRange(pb::FetchRange { range, limit })
        };
        let crcs = |from_seq: u64, to_seq: u64| Req::EntryCrcs(pb::EntryCrcs { from_seq, to_seq });
        let bad = [
            ("empty keys", keys("c", 0)),
            ("too many keys", keys("c", 1025)),
            ("unknown cf", keys("nope", 1)),
            ("zero limit", fetch_range(0, Some(range(None, None)))),
            ("huge limit", fetch_range(1025, Some(range(None, None)))),
            ("no range", fetch_range(10, None)),
            ("unknown range cf", {
                let mut r = range(None, None);
                r.cf = "nope".into();
                fetch_range(10, Some(r))
            }),
            ("from > to", crcs(2, 1)),
            ("to > S", crcs(1, 3)),
        ];
        for (what, kind) in bad {
            let (_, err) = exchange(&mut c, vec![request(&sid, 1, kind)]).await;
            assert_eq!(err.map(|e| e.code()), Some(Code::InvalidArgument), "{what}");
        }
        // The edge of the valid: 1024 keys, the whole log.
        let (answers, err) = exchange(
            &mut c,
            vec![
                request(&sid, 1, keys("c", 1024)),
                request(&sid, 2, crcs(0, 2)),
            ],
        )
        .await;
        assert!(err.is_none(), "{err:?}");
        match &answers[0].kind {
            Some(Resp::Fetch(f)) => assert_eq!(f.records.len(), 1024),
            other => panic!("{other:?}"),
        }
        match &answers[1].kind {
            Some(Resp::EntryCrcs(r)) => {
                assert_eq!(r.entries.iter().map(|e| e.seq).collect::<Vec<_>>(), [1, 2])
            }
            other => panic!("{other:?}"),
        }
    }

    /// A single entry at `S` is answered.
    #[tokio::test]
    async fn the_entry_at_s_is_answered_alone() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        m.put("c", b"a", b"v").unwrap();
        let last = m.entries_since(1).unwrap().last().unwrap().unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let sid = begin(&mut c, pb::VerifyMode::Quick)
            .await
            .unwrap()
            .session_id;
        let req = Req::EntryCrcs(pb::EntryCrcs {
            from_seq: last.seq,
            to_seq: last.seq,
        });
        let (answers, err) = exchange(&mut c, vec![request(&sid, 1, req)]).await;
        assert!(err.is_none(), "{err:?}");
        match &answers[0].kind {
            Some(Resp::EntryCrcs(r)) => assert_eq!(
                r.entries,
                vec![pb::SeqCrc {
                    seq: last.seq,
                    crc32c: last.crc32c
                }]
            ),
            other => panic!("{other:?}"),
        }
    }

    /// Entry `S` is known from `last_entry_crc` when retention has emptied the log; entries
    /// before it are then not held.
    #[tokio::test]
    async fn the_entry_at_s_survives_an_emptied_log() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        m.put("c", b"a", b"v").unwrap(); // S = 2
        let last = m.entries_since(1).unwrap().next().unwrap().unwrap();
        assert_eq!(last.seq, 2);
        let empty = crate::config::Retention {
            max_age: None,
            max_entries: Some(0),
            check_interval: Duration::from_secs(1),
        };
        m.log_source().store().trim_changelog(0, &empty).unwrap();
        assert_eq!(m.first_retained_seq(), 3, "the log is empty");
        let mut c = client(m.serve_addr().unwrap(), "b");
        let sid = begin(&mut c, pb::VerifyMode::Quick)
            .await
            .unwrap()
            .session_id;
        let req = Req::EntryCrcs(pb::EntryCrcs {
            from_seq: 1,
            to_seq: 2,
        });
        let (answers, err) = exchange(&mut c, vec![request(&sid, 1, req)]).await;
        assert!(err.is_none(), "{err:?}");
        match &answers[0].kind {
            Some(Resp::EntryCrcs(r)) => assert_eq!(
                r.entries,
                vec![pb::SeqCrc {
                    seq: 2,
                    crc32c: last.crc32c
                }],
                "entry 1 is trimmed, entry S comes from last_entry_crc"
            ),
            other => panic!("{other:?}"),
        }
    }

    /// Values past their expiry that were not reaped are copied raw, by both fetches.
    #[tokio::test]
    async fn expired_but_unreaped_envelopes_are_fetched_raw() {
        let dir = tempfile::tempdir().unwrap();
        let (m, clock) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        m.put_with_ttl("c", b"t", b"v", Duration::from_secs(10))
            .unwrap();
        clock.advance(Duration::from_secs(60));
        assert!(m.get("c", b"t").unwrap().is_none(), "expired for readers");
        let mut c = client(m.serve_addr().unwrap(), "b");
        let sid = begin(&mut c, pb::VerifyMode::Quick)
            .await
            .unwrap()
            .session_id;
        let (answers, err) = exchange(
            &mut c,
            vec![
                request(
                    &sid,
                    1,
                    Req::FetchKeys(pb::FetchKeys {
                        cf: "c".into(),
                        keys: vec![b"t".to_vec()],
                    }),
                ),
                request(
                    &sid,
                    2,
                    Req::FetchRange(pb::FetchRange {
                        range: Some(range(None, None)),
                        limit: 10,
                    }),
                ),
            ],
        )
        .await;
        assert!(err.is_none(), "{err:?}");
        for a in &answers {
            let Some(Resp::Fetch(f)) = &a.kind else {
                panic!("{a:?}")
            };
            assert_eq!(f.records.len(), 1);
            let raw = f.records[0].envelope.as_deref().expect("the raw envelope");
            let env = envelope::decode(raw).unwrap();
            assert_eq!(env.meta.expire_at_ms, Some(T0 + 10_000));
        }
    }

    /// A fetch is a computation: it holds a permit, and shutdown cancels it.
    #[tokio::test]
    async fn fetches_hold_a_permit_and_stop_at_shutdown() {
        let dir = tempfile::tempdir().unwrap();
        let (m, h, state) = standalone_server(dir.path()).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        m.put("c", b"a", b"v").unwrap();
        let mut c = client(h.addr(), "b");
        let s = begin(&mut c, pb::VerifyMode::Full).await.unwrap();
        let started = super::hooks::slow(&s.session_id, false);
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut stream = c
            .verify_range(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        let kind = Req::FetchRange(pb::FetchRange {
            range: Some(range(None, None)),
            limit: 10,
        });
        tx.send(request(&s.session_id, 1, kind)).await.unwrap();
        while !started.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            state.compute_permits.available_permits(),
            super::MAX_COMPUTATIONS - 1
        );
        h.shutdown().await.unwrap();
        drop(stream.message().await);
        assert_eq!(
            state.compute_permits.available_permits(),
            super::MAX_COMPUTATIONS,
            "every permit is back after shutdown"
        );
        drop(state);
        m.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn sessions_are_limited_and_ending_one_frees_a_slot() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| {
            cfg.serve.as_mut().unwrap().limits.max_verify_sessions = 2
        })
        .await;
        let mut c = client(m.serve_addr().unwrap(), "b");
        let first = begin(&mut c, pb::VerifyMode::Quick).await.unwrap();
        begin(&mut c, pb::VerifyMode::Quick).await.unwrap();
        let busy = begin(&mut c, pb::VerifyMode::Quick).await.unwrap_err();
        assert_eq!(
            reason(&busy),
            (Code::ResourceExhausted, Some(pb::Reason::Busy))
        );
        c.verify_end(pb::VerifyEndRequest {
            session_id: first.session_id.clone(),
        })
        .await
        .unwrap();
        begin(&mut c, pb::VerifyMode::Quick).await.unwrap();
        let gone = c
            .verify_end(pb::VerifyEndRequest {
                session_id: first.session_id,
            })
            .await
            .unwrap_err();
        assert_eq!(
            reason(&gone),
            (Code::NotFound, Some(pb::Reason::SessionNotFound))
        );
    }

    #[tokio::test]
    async fn idle_sessions_expire_and_free_their_slot() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| {
            let limits = &mut cfg.serve.as_mut().unwrap().limits;
            limits.verify_session_timeout = Duration::from_millis(200);
            limits.max_verify_sessions = 1;
        })
        .await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let s = begin(&mut c, pb::VerifyMode::Quick).await.unwrap();
        assert_eq!(s.session_timeout_ms, 200);
        // A request in time keeps it alive.
        let (answers, err) = exchange(
            &mut c,
            vec![hash(&s.session_id, 1, range(None, None), 0, vec![])],
        )
        .await;
        assert!(err.is_none() && answers.len() == 1);
        tokio::time::sleep(Duration::from_millis(400)).await;
        let (_, err) = exchange(
            &mut c,
            vec![hash(&s.session_id, 2, range(None, None), 0, vec![])],
        )
        .await;
        assert_eq!(
            reason(&err.unwrap()),
            (Code::NotFound, Some(pb::Reason::SessionNotFound))
        );
        begin(&mut c, pb::VerifyMode::Quick)
            .await
            .expect("the expired session no longer counts");
    }

    #[tokio::test]
    async fn malformed_requests_are_invalid_arguments() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let e = begin(&mut c, pb::VerifyMode::Unspecified)
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument);
        let sid = begin(&mut c, pb::VerifyMode::Quick)
            .await
            .unwrap()
            .session_id;
        let unknown_cf = pb::KeyRange {
            cf: "nope".into(),
            lower: None,
            upper: None,
        };
        for bad in [
            list(&sid, 1, range(None, None), 0),
            list(&sid, 1, range(None, None), 1025),
            hash(&sid, 1, unknown_cf, 0, vec![]),
            pb::VerifyRangeRequest {
                session_id: sid.clone(),
                request_id: 1,
                kind: None,
            },
            pb::VerifyRangeRequest {
                session_id: sid.clone(),
                request_id: 1,
                kind: Some(Req::Hash(pb::HashRange::default())),
            },
        ] {
            let (_, err) = exchange(&mut c, vec![bad.clone()]).await;
            assert_eq!(
                err.map(|e| e.code()),
                Some(Code::InvalidArgument),
                "{bad:?}"
            );
        }
        // The largest allowed listing is fine.
        let (answers, err) = exchange(&mut c, vec![list(&sid, 2, range(None, None), 1024)]).await;
        assert!(err.is_none() && answers.len() == 1);
    }

    #[test]
    fn an_unregistered_comparator_is_internal() {
        let reg = crate::tuning::ComparatorRegistry::default();
        let e = super::order_for(&reg, Some("nope")).err().unwrap();
        assert_eq!(e.code(), Code::Internal);
        assert!(
            e.message()
                .contains("comparator nope is not registered on this node"),
            "{}",
            e.message()
        );
        assert!(super::order_for(&reg, None).is_ok());
    }

    /// An unknown session is refused at once, even when every computation slot is taken.
    #[tokio::test]
    async fn an_unknown_session_does_not_wait_for_a_slot() {
        let dir = tempfile::tempdir().unwrap();
        let (m, h, state) = standalone_server(dir.path()).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(h.addr(), "b");
        let all = state
            .compute_permits
            .clone()
            .acquire_many_owned(super::MAX_COMPUTATIONS as u32)
            .await
            .unwrap();
        let (_, err) = tokio::time::timeout(
            Duration::from_secs(3),
            exchange(
                &mut c,
                vec![hash(&[7; 16], 1, range(None, None), 0, vec![])],
            ),
        )
        .await
        .expect("answered without waiting for a slot");
        assert_eq!(
            reason(&err.unwrap()),
            (Code::NotFound, Some(pb::Reason::SessionNotFound))
        );
        drop(all);
        drop(state);
        h.shutdown().await.unwrap();
        m.shutdown().await.unwrap();
    }

    /// A master that does not serve, plus a separately started server over it. Returns an extra
    /// handle on the server state, so a leak through the state would be visible after shutdown.
    async fn standalone_server(
        dir: &std::path::Path,
    ) -> (
        crate::master::Master,
        crate::server::ServerHandle,
        std::sync::Arc<crate::server::ServerState>,
    ) {
        let (m, _) = serving_master(dir, |cfg| cfg.serve = None).await;
        let cfg = crate::config::ServeConfig::new(
            [127, 0, 0, 1].into(),
            0,
            std::sync::Arc::new(crate::auth::AllowAll),
        );
        let src = m.log_source();
        let h = crate::server::start(
            std::sync::Arc::downgrade(&src),
            cfg,
            std::sync::Arc::new(crate::server::replicas::ReplicaRegistry::default()),
        )
        .await
        .unwrap();
        let state = h.state.clone();
        (m, h, state)
    }

    /// A client that opens a session and walks away must not keep the database open, even
    /// while something else still holds the server's state.
    #[tokio::test]
    async fn open_sessions_do_not_keep_the_database_open() {
        let dir = tempfile::tempdir().unwrap();
        let (m, h, state) = standalone_server(dir.path()).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let db = m.log_source().store().db().clone();
        let baseline = std::sync::Arc::strong_count(&db) - 1;
        let mut c = client(h.addr(), "b");
        let s = begin(&mut c, pb::VerifyMode::Full).await.unwrap();
        assert!(
            std::sync::Arc::strong_count(&db) - 1 > baseline,
            "the session holds it"
        );
        // An open VerifyRange stream with an unanswered client, too.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let _stream = c
            .verify_range(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap();
        tx.send(hash(&s.session_id, 1, range(None, None), 0, vec![]))
            .await
            .unwrap();
        h.shutdown().await.expect("shutdown succeeds");
        assert_eq!(
            std::sync::Arc::strong_count(&db) - 1,
            baseline,
            "shutdown released the session's snapshot although the state is still referenced"
        );
        drop(state);
        drop(db);
        m.shutdown().await.unwrap();
    }

    /// A long computation in flight must not outlive `shutdown`, and the directory must be
    /// reopenable at once.
    #[tokio::test]
    async fn shutdown_aborts_a_computation_in_flight_and_releases_the_database() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        m.put("c", b"k", b"v").unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let s = begin(&mut c, pb::VerifyMode::Full).await.unwrap();
        let started = super::hooks::slow(&s.session_id, false);
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut stream = c
            .verify_range(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        tx.send(hash(&s.session_id, 1, range(None, None), 0, vec![]))
            .await
            .unwrap();
        while !started.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        let t = std::time::Instant::now();
        m.shutdown().await.expect("shutdown");
        assert!(t.elapsed() < Duration::from_secs(4), "{:?}", t.elapsed());
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        assert_eq!(m.last_seq(), 2);
        drop(stream.message().await);
    }

    /// If a computation cannot be stopped, `shutdown` says so instead of returning `Ok`.
    #[tokio::test]
    async fn shutdown_fails_while_a_computation_cannot_be_stopped() {
        let dir = tempfile::tempdir().unwrap();
        let (m, h, _state) = standalone_server(dir.path()).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(h.addr(), "b");
        let s = begin(&mut c, pb::VerifyMode::Full).await.unwrap();
        let started = super::hooks::slow(&s.session_id, true);
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let _stream = c
            .verify_range(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap();
        tx.send(hash(&s.session_id, 1, range(None, None), 0, vec![]))
            .await
            .unwrap();
        while !started.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert!(h.shutdown().await.is_err());
    }

    /// Computations are bounded by permits, which are returned when they end.
    #[tokio::test]
    async fn computations_hold_a_permit() {
        let dir = tempfile::tempdir().unwrap();
        let (m, h, state) = standalone_server(dir.path()).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(h.addr(), "b");
        let s = begin(&mut c, pb::VerifyMode::Full).await.unwrap();
        let started = super::hooks::slow(&s.session_id, false);
        assert_eq!(
            state.compute_permits.available_permits(),
            super::MAX_COMPUTATIONS
        );
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut stream = c
            .verify_range(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        tx.send(hash(&s.session_id, 1, range(None, None), 0, vec![]))
            .await
            .unwrap();
        while !started.load(std::sync::atomic::Ordering::SeqCst) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        assert_eq!(
            state.compute_permits.available_permits(),
            super::MAX_COMPUTATIONS - 1
        );
        h.shutdown().await.unwrap();
        drop(stream.message().await);
        assert_eq!(
            state.compute_permits.available_permits(),
            super::MAX_COMPUTATIONS,
            "every permit is back after shutdown"
        );
        drop(state);
        m.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn an_epoch_bump_ends_verify_streams_and_sessions() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let sid = begin(&mut c, pb::VerifyMode::Quick)
            .await
            .unwrap()
            .session_id;
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let mut stream = c
            .verify_range(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap()
            .into_inner();
        tx.send(hash(&sid, 1, range(None, None), 0, vec![]))
            .await
            .unwrap();
        assert!(stream.message().await.unwrap().is_some());
        m.bump_epoch().unwrap();
        let err = stream.message().await.unwrap_err();
        assert_eq!(
            reason(&err),
            (Code::Unavailable, Some(pb::Reason::UpstreamReset))
        );
        let (_, err) = exchange(&mut c, vec![hash(&sid, 2, range(None, None), 0, vec![])]).await;
        let r = reason_of(&err.unwrap());
        assert!(
            matches!(
                r,
                Some(pb::Reason::UpstreamReset | pb::Reason::SessionNotFound)
            ),
            "{r:?}"
        );
    }
}
