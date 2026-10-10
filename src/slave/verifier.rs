//! The slave side of Verify (DESIGN.md §12, PROTOCOL.md §9): open a session upstream,
//! capture a local snapshot at exactly the upstream's seq `S` (taken by the applier right
//! after it commits `S`), compare the CF lists, then compare each common CF range by range.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval_at};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Code, Status, Streaming};
use tracing::Instrument;

use super::client::{self, Client};
use super::inner::{CaptureHold, CaptureMiss, Inner, VerifyWork};
use super::repair::AffectedSet;
use super::state::SlaveState;
use crate::background::blocking;
use crate::error::{Error, Result};
use crate::net::convert::{cf_from_pb, identity_from_pb};
use crate::net::{DECODE_SLACK, reason_of};
use crate::pb;
use crate::pb::verify_range_request::Kind as ReqKind;
use crate::pb::verify_range_response::Kind as RespKind;
use crate::read::ReadSnapshot;
use crate::store::cfs::PhysCf;
use crate::types::{CfEntry, DbIdentity, Seq};
use crate::verify::compare::{Budget, CfRun, LocalSide, Oracle, compare_cf, compare_specs};
use crate::verify::range::{Bounds, KeyOrder, LocalRange, LocalRecord, Throttle};
use crate::verify::scan::{chunk_cf, load_range};
use crate::verify::{
    LIST_SOFT_BYTES, MAX_LIST_LIMIT, MAX_RESUME_KEY_BYTES, MismatchPolicy, VERIFY_WINDOW,
    VerifyAbort, VerifyMode, VerifyReport,
};

/// The longest the applier stays held waiting for `VerifyBegin` (bounded by `capture_timeout`
/// too). While held the slave applies and acknowledges nothing (DESIGN.md §12).
const BEGIN_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest one answer on the verify stream may take.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(60);
/// The longest `VerifyEnd` may take: it is best effort and delays the report.
const END_TIMEOUT: Duration = Duration::from_secs(2);
/// The most key bytes one `FetchKeys` request carries (the server accepts 4 MiB requests).
pub(crate) const FETCH_REQUEST_BYTES: usize = 1 << 20;

/// Sets the run's stop flag when the run ends or is dropped (cancelled, or its `verify_now`
/// future was dropped): blocking scans check it, so none keeps the database open.
struct StopOnDrop(Arc<AtomicBool>);

impl Drop for StopOnDrop {
    fn drop(&mut self) {
        self.0.store(true, Ordering::SeqCst);
    }
}

fn abort(a: VerifyAbort) -> Error {
    Error::VerifyAborted(a)
}

fn status_abort(s: &Status) -> Error {
    abort(match reason_of(s) {
        Some(pb::Reason::Busy) => VerifyAbort::Busy,
        // The session is gone: expired, or dropped by a reset of the upstream's state.
        Some(pb::Reason::SessionNotFound | pb::Reason::UpstreamReset) => {
            VerifyAbort::SessionExpired
        }
        _ if matches!(
            s.code(),
            Code::PermissionDenied | Code::Unauthenticated | Code::InvalidArgument
        ) =>
        {
            VerifyAbort::Refused(format!("{:?}: {}", s.code(), s.message()))
        }
        _ => VerifyAbort::Upstream(format!("{:?}: {}", s.code(), s.message())),
    })
}

/// The largest answer: a listing cut at `LIST_SOFT_BYTES` plus up to two keys, each at most
/// one entry long.
fn decode_limit(max_entry_bytes: u64) -> usize {
    let entry = usize::try_from(max_entry_bytes)
        .unwrap_or(usize::MAX)
        .max(16 << 20);
    entry
        .saturating_mul(2)
        .saturating_add(LIST_SOFT_BYTES)
        .saturating_add(DECODE_SLACK)
}

/// Runs one verify against the upstream now. One run at a time per slave; a second caller
/// waits for the first. The report is stored in the status and passed to the listener.
pub(crate) async fn verify(inner: &Arc<Inner>, mode: VerifyMode) -> Result<VerifyReport> {
    let _turn = inner.verify_lock.lock().await;
    let Some((identity, _)) = inner.ident() else {
        return Err(Error::NotReady);
    };
    // Mid-repair the local state is a mix of two seqs (DESIGN.md §12.1): nothing to compare.
    if inner.repairing().is_some() {
        return Err(Error::NotReady);
    }
    // The repoint generation the run belongs to, read before it connects to the current
    // upstream: a repoint during the run makes its policy action inert.
    let generation = *inner.repoint_tx.borrow();
    let stop = StopOnDrop(Arc::new(AtomicBool::new(false)));
    let span = tracing::info_span!(
        "verify",
        node_id = %inner.cfg.identifier,
        upstream = %inner.upstream_uri(),
        seq = tracing::field::Empty,
        epoch = identity.epoch,
        mode = ?mode,
    );
    let result = session(inner, identity, mode, &span, &stop.0)
        .instrument(span.clone())
        .await;
    if result.is_ok() {
        // Test hook: lets a test act between the comparison and its consequences.
        crate::failpoint::pause("verify-before-finish").await;
    }
    span.in_scope(|| match &result {
        Ok(report) => finish(inner, report, generation, identity),
        Err(e) => tracing::warn!(error = %e, "verify abandoned"),
    });
    result
}

/// Whether a report with mismatches may start its policy's action (`Rebootstrap` or `Repair`).
/// Never on a halted slave (it may be on another history than its upstream: a divergence the
/// handshake refused), and never when the slave was repointed during the run (the report is
/// about the old upstream).
fn may_act(
    policy: MismatchPolicy,
    clean: bool,
    halted: bool,
    started_generation: u64,
    generation: u64,
) -> bool {
    !clean && !halted && started_generation == generation && policy != MismatchPolicy::Report
}

fn finish(inner: &Inner, report: &VerifyReport, started_generation: u64, verified: DbIdentity) {
    let halted = matches!(inner.state(), SlaveState::Halted(_));
    let generation = *inner.repoint_tx.borrow();
    if report.is_clean() {
        tracing::info!(
            seq = report.seq,
            keys = report.keys_checked,
            ms = report.duration.as_millis() as u64,
            "verify clean"
        );
    } else {
        tracing::error!(
            seq = report.seq,
            mismatches = report.mismatches.len(),
            "verify found mismatches"
        );
        let policy = inner.cfg.verify.on_mismatch;
        if may_act(policy, false, halted, started_generation, generation) {
            // A CF that differs cannot be repaired key by key: re-bootstrap instead.
            let repair = match policy {
                MismatchPolicy::Repair => AffectedSet::from_mismatches(&report.mismatches),
                MismatchPolicy::Report | MismatchPolicy::Rebootstrap => None,
            };
            // Consent to re-copy from the upstream that was verified, nothing more: not the
            // operator's `force_bootstrap`, which also overrides DIVERGED and IDENTITY_CHANGED.
            match repair {
                Some(set) => {
                    inner.request_repair(started_generation, verified.db_uuid, set);
                }
                None => {
                    inner
                        .verify_rebootstrap
                        .request(started_generation, verified.db_uuid);
                }
            }
            inner.rebootstrap.notify_one();
        } else if policy != MismatchPolicy::Report && started_generation != generation {
            tracing::warn!("the slave was repointed during the run; not acting on its report");
        }
    }
    inner.set_last_verify(report.clone());
    inner.notify(|l| l.on_verify_complete(report));
}

async fn session(
    inner: &Arc<Inner>,
    identity: DbIdentity,
    mode: VerifyMode,
    span: &tracing::Span,
    stop: &Arc<AtomicBool>,
) -> Result<VerifyReport> {
    let started = Instant::now();
    // Connected before the applier is held, so the pause never includes TCP or TLS setup.
    let client = client::connect_now(inner)
        .await
        .map_err(|e| abort(VerifyAbort::Upstream(format!("connect: {e}"))))?;
    // The applier commits nothing while the upstream pins its snapshot, so this slave cannot
    // pass the snapshot's seq before the capture is set up. The pause is bounded.
    let hold = inner.hold_applier();
    let bound = BEGIN_TIMEOUT.min(inner.cfg.verify.capture_timeout);
    let mut session = UpstreamSession::open(inner, &client, mode, bound).await?;
    span.record("seq", session.begin.snapshot_seq);
    let result = compare(inner, &mut session, hold, identity, mode, started, stop).await;
    session.close().await;
    result
}

/// An open session on the upstream (PROTOCOL.md §9): its `VerifyBegin` answer and the
/// `VerifyRange` stream used for hashes, listings, fetches and entry CRCs.
pub(crate) struct UpstreamSession {
    client: Client,
    pub begin: pb::VerifyBeginResponse,
    pub upstream: Upstream,
}

impl UpstreamSession {
    /// `VerifyBegin`, then the `VerifyRange` stream, both within `bound` altogether. `client`
    /// should be connected already ([`client::connect_now`]).
    pub(crate) async fn open(
        inner: &Inner,
        client: &Client,
        mode: VerifyMode,
        bound: Duration,
    ) -> Result<UpstreamSession> {
        let limit = decode_limit(inner.max_entry_bytes.load(Ordering::SeqCst));
        let mut client = client.clone().max_decoding_message_size(limit);
        let req = pb::VerifyBeginRequest {
            client_id: inner.cfg.identifier.clone(),
            mode: mode.to_pb() as i32,
        };
        let timed_out = || {
            abort(VerifyAbort::Upstream(format!(
                "VerifyBegin got no answer within {bound:?}"
            )))
        };
        let deadline = tokio::time::Instant::now() + bound;
        let begin = tokio::time::timeout_at(deadline, client.verify_begin(req))
            .await
            .map_err(|_| timed_out())?
            .map_err(|s| status_abort(&s))?
            .into_inner();
        let (tx, rx) = mpsc::channel(2 * VERIFY_WINDOW);
        let opened =
            tokio::time::timeout_at(deadline, client.verify_range(ReceiverStream::new(rx))).await;
        let answers = match opened {
            Ok(Ok(r)) => r.into_inner(),
            failed => {
                // Do not leave the snapshot pinned upstream until it expires.
                let end = pb::VerifyEndRequest {
                    session_id: begin.session_id.clone(),
                };
                let _ = tokio::time::timeout(END_TIMEOUT, client.verify_end(end)).await;
                return Err(match failed {
                    Ok(Err(s)) => status_abort(&s),
                    _ => abort(VerifyAbort::Upstream(format!(
                        "VerifyRange was not opened within {bound:?}"
                    ))),
                });
            }
        };
        let upstream = Upstream {
            tx,
            answers,
            session_id: begin.session_id.clone(),
            snapshot_seq: begin.snapshot_seq,
            next_id: 0,
        };
        Ok(UpstreamSession {
            client,
            begin,
            upstream,
        })
    }

    /// Ends the session. Best effort and bounded: the upstream also expires idle sessions.
    pub(crate) async fn close(self) {
        let UpstreamSession {
            mut client,
            begin,
            upstream,
        } = self;
        drop(upstream);
        let end = pb::VerifyEndRequest {
            session_id: begin.session_id,
        };
        let _ = tokio::time::timeout(END_TIMEOUT, client.verify_end(end)).await;
    }
}

#[allow(clippy::too_many_arguments)]
async fn compare(
    inner: &Arc<Inner>,
    session: &mut UpstreamSession,
    hold: CaptureHold,
    identity: DbIdentity,
    mode: VerifyMode,
    started: Instant,
    stop: &Arc<AtomicBool>,
) -> Result<VerifyReport> {
    let begin = &session.begin;
    let upstream_identity = begin.identity.as_ref().map(identity_from_pb).transpose()?;
    if upstream_identity != Some(identity) {
        return Err(abort(VerifyAbort::IdentityChanged));
    }
    let s = begin.snapshot_seq;
    let snap = Arc::new(capture(inner, hold, s).await?);
    debug_assert_eq!(snap.seq(), s);
    let meta_snap = snap.clone();
    // Counted like every blocking read that holds the snapshot: shutdown waits for it.
    let work = inner.verify_work.enter();
    let local_cfs = blocking(move || {
        let _work = work;
        meta_snap.meta()
    })
    .await?
    .cf_specs;
    let upstream_cfs: Vec<CfEntry> = begin.cf_specs.iter().map(cf_from_pb).collect();
    let (mut mismatches, common) = compare_specs(&local_cfs, &upstream_cfs);

    let upstream = &mut session.upstream;
    let cfg = &inner.cfg.verify;
    let throttle = Arc::new(Mutex::new(Throttle::new(cfg.max_bytes_per_sec)));
    let mut budget = Budget::new(cfg.max_reported_keys);
    let mut keys_checked = 0;
    for cf in &common {
        let name = cf.spec.name.clone();
        let phys = snap.phys(&name).ok_or_else(|| {
            Error::corrupt(format!("CF {name} is missing from the verify snapshot"))
        })?;
        let order = KeyOrder::new(
            &inner.store.tuning().comparators,
            cf.spec.comparator.as_deref(),
        )?;
        let (range_tx, mut ranges) = mpsc::channel(VERIFY_WINDOW);
        let producer = {
            let (snap, phys, throttle) = (snap.clone(), phys.clone(), throttle.clone());
            let (chunk_keys, chunk_bytes) = (cfg.chunk_keys, cfg.chunk_bytes);
            let (stop, work) = (stop.clone(), inner.verify_work.enter());
            tokio::task::spawn_blocking(move || {
                // Counted until the scan returns: shutdown waits for it.
                let _work = work;
                let emit = |r: LocalRange| range_tx.blocking_send(Ok(r)).is_ok();
                match chunk_cf(
                    &snap,
                    phys,
                    mode,
                    chunk_keys,
                    chunk_bytes,
                    &throttle,
                    &stop,
                    emit,
                ) {
                    Ok(keys) => keys,
                    // The error goes into the channel before it closes: a closed channel
                    // alone reads as "all ranges sent".
                    Err(e) => {
                        let _ = range_tx.blocking_send(Err(e));
                        None
                    }
                }
            })
        };
        let local = Local {
            snap: snap.clone(),
            phys,
            order: order.clone(),
            mode,
            throttle: throttle.clone(),
            stop: stop.clone(),
            work: inner.verify_work.clone(),
        };
        let mut run = CfRun {
            cf: &name,
            order,
            min_chunk_keys: cfg.min_chunk_keys,
            budget: &mut budget,
            out: &mut mismatches,
        };
        let compared = compare_cf(upstream, &local, &mut run, &mut ranges).await;
        // Unblocks the producer if the comparison stopped early.
        drop(ranges);
        let keys = producer
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        compared?;
        // `None` means the scan stopped early because the receiver was gone, which cannot
        // happen before the comparison returned: never count it as a full scan.
        let keys = keys.ok_or_else(|| {
            Error::Io(std::io::Error::other(format!(
                "the local scan of CF {name} stopped early"
            )))
        })?;
        keys_checked += keys;
    }
    Ok(VerifyReport {
        seq: s,
        mode,
        cfs_checked: common.len(),
        keys_checked,
        mismatches,
        duration: started.elapsed(),
    })
}

/// A local snapshot at exactly `s`, taken while `hold` keeps the applier from passing it:
/// at once if the slave is at `s`, otherwise by the applier right after it commits `s`. A
/// slave already past `s` is only possible with an upstream that is behind it (DESIGN.md §6).
async fn capture(inner: &Arc<Inner>, hold: CaptureHold, s: Seq) -> Result<ReadSnapshot> {
    // `target` may take a snapshot (a RocksDB call): off the async worker.
    let (hold, reply) = blocking(move || {
        let reply = hold.target(s);
        Ok((hold, reply))
    })
    .await?;
    let mut states = inner.state_tx.subscribe();
    let timeout = inner.cfg.verify.capture_timeout;
    let outcome = tokio::select! {
        r = tokio::time::timeout(timeout, reply) => r.ok().and_then(|r| r.ok()),
        _ = async { let _ = states.wait_for(|st| matches!(st, SlaveState::Halted(_))).await; } => None,
    };
    drop(hold);
    match outcome {
        Some(Ok(snap)) => Ok(snap),
        Some(Err(CaptureMiss::Passed(applied))) => Err(abort(VerifyAbort::PassedSnapshot {
            snapshot_seq: s,
            applied_seq: applied,
        })),
        Some(Err(CaptureMiss::Cancelled)) | None => {
            Err(abort(VerifyAbort::CaptureTimeout { snapshot_seq: s }))
        }
    }
}

/// How many of `keys` the next `FetchKeys` request carries: at most `MAX_LIST_LIMIT` keys and
/// [`FETCH_REQUEST_BYTES`] of keys, but always at least one.
fn fetch_batch_len(keys: &[Vec<u8>]) -> usize {
    let mut n = 0;
    let mut bytes = 0;
    for k in keys.iter().take(MAX_LIST_LIMIT as usize) {
        if n > 0 && bytes + k.len() > FETCH_REQUEST_BYTES {
            break;
        }
        n += 1;
        bytes += k.len();
    }
    n
}

/// Checks the records of a `FetchRange` as they arrive: each lies in the page that was asked
/// for and sorts strictly after the one before (under the CF's own order), across pages. An
/// upstream that breaks this is misbehaving; repair must not write what it sends.
struct RangeCheck<'a> {
    order: &'a KeyOrder,
    prev: Option<Vec<u8>>,
}

impl RangeCheck<'_> {
    fn accept(&mut self, page: &Bounds, key: &[u8]) -> Result<()> {
        let protocol = |what: &str| abort(VerifyAbort::Upstream(format!("FetchRange: {what}")));
        if !page.contains(self.order, key) {
            return Err(protocol("a record outside the requested range"));
        }
        if let Some(prev) = &self.prev
            && self.order.cmp(prev, key) != std::cmp::Ordering::Less
        {
            return Err(protocol("records are not strictly increasing"));
        }
        self.prev = Some(key.to_vec());
        Ok(())
    }
}

/// Checks that `records` answer the first keys of `ask`, in order (a prefix, never empty).
fn check_fetched(ask: &[Vec<u8>], records: &[pb::FetchedRecord]) -> Result<()> {
    let wrong = records.is_empty()
        || records.len() > ask.len()
        || records.iter().zip(ask).any(|(r, k)| r.key != *k);
    if wrong {
        return Err(abort(VerifyAbort::Upstream(
            "FetchKeys answered other keys".into(),
        )));
    }
    Ok(())
}

/// The upstream's half of a session, over one `VerifyRange` stream.
pub(crate) struct Upstream {
    tx: mpsc::Sender<pb::VerifyRangeRequest>,
    answers: Streaming<pb::VerifyRangeResponse>,
    session_id: Vec<u8>,
    /// The session's `S`: `entry_crc` never asks for a later seq (the upstream refuses it).
    snapshot_seq: Seq,
    next_id: u64,
}

impl Upstream {
    async fn send(&mut self, kind: ReqKind) -> Result<u64> {
        self.next_id += 1;
        let req = pb::VerifyRangeRequest {
            session_id: self.session_id.clone(),
            request_id: self.next_id,
            kind: Some(kind),
        };
        self.tx
            .send(req)
            .await
            .map_err(|_| abort(VerifyAbort::Upstream("the verify stream closed".into())))?;
        Ok(self.next_id)
    }

    async fn answer(&mut self, id: u64) -> Result<RespKind> {
        let next = tokio::time::timeout(ANSWER_TIMEOUT, self.answers.message())
            .await
            .map_err(|_| {
                abort(VerifyAbort::Upstream(format!(
                    "no answer within {ANSWER_TIMEOUT:?}"
                )))
            })?;
        match next {
            Ok(Some(r)) if r.request_id == id => r
                .kind
                .ok_or_else(|| abort(VerifyAbort::Upstream("empty answer".into()))),
            Ok(Some(r)) => Err(abort(VerifyAbort::Upstream(format!(
                "answer {} arrived for request {id}",
                r.request_id
            )))),
            Ok(None) => Err(abort(VerifyAbort::Upstream(
                "the upstream ended the verify stream".into(),
            ))),
            Err(s) => Err(status_abort(&s)),
        }
    }

    /// The upstream's envelopes of `keys` at the session's snapshot (`None`: no such key),
    /// passed to `sink` in the order asked. Requests carry at most [`FETCH_REQUEST_BYTES`] of
    /// keys (and at most `MAX_LIST_LIMIT` keys); a single larger key is sent alone (the caller
    /// should not ask for keys near the server's 4 MiB request limit). Stops early, returning
    /// `Ok(false)`, when `sink` returns `false`.
    pub(crate) async fn fetch_keys(
        &mut self,
        cf: &str,
        keys: &[Vec<u8>],
        sink: &mut (dyn FnMut(Vec<u8>, Option<Vec<u8>>) -> bool + Send),
    ) -> Result<bool> {
        let mut rest = keys;
        while !rest.is_empty() {
            let ask = &rest[..fetch_batch_len(rest)];
            let req = pb::FetchKeys {
                cf: cf.to_string(),
                keys: ask.to_vec(),
            };
            let id = self.send(ReqKind::FetchKeys(req)).await?;
            let RespKind::Fetch(f) = self.answer(id).await? else {
                return Err(abort(VerifyAbort::Upstream(
                    "unexpected answer to FetchKeys".into(),
                )));
            };
            check_fetched(ask, &f.records)?;
            rest = &rest[f.records.len()..];
            for r in f.records {
                if !sink(r.key, r.envelope) {
                    return Ok(false);
                }
            }
        }
        Ok(true)
    }

    /// Every key of `bounds` with its envelope at the session's snapshot, passed to `sink` in
    /// key order under `order` (the CF's comparator), paging through truncated answers like a
    /// listing. Records outside the range or out of order are a protocol error. Stops early,
    /// returning `Ok(false)`, when `sink` returns `false`.
    pub(crate) async fn fetch_range(
        &mut self,
        cf: &str,
        bounds: &Bounds,
        order: &KeyOrder,
        sink: &mut (dyn FnMut(Vec<u8>, Option<Vec<u8>>) -> bool + Send),
    ) -> Result<bool> {
        let mut page = bounds.clone();
        let mut resume: Option<Vec<u8>> = None;
        let mut check = RangeCheck { order, prev: None };
        loop {
            let req = pb::FetchRange {
                range: Some(page.to_pb(cf)),
                limit: MAX_LIST_LIMIT,
            };
            let id = self.send(ReqKind::FetchRange(req)).await?;
            let RespKind::Fetch(f) = self.answer(id).await? else {
                return Err(abort(VerifyAbort::Upstream(
                    "unexpected answer to FetchRange".into(),
                )));
            };
            if f.records.len() > MAX_LIST_LIMIT as usize {
                return Err(abort(VerifyAbort::Upstream(
                    "FetchRange answered more records than asked".into(),
                )));
            }
            let mut last = None;
            for (i, r) in f.records.into_iter().enumerate() {
                // Only the first record may repeat the key the page resumes from.
                if i == 0 && resume.as_deref() == Some(r.key.as_slice()) {
                    continue;
                }
                if r.envelope.is_none() {
                    return Err(abort(VerifyAbort::Upstream(
                        "FetchRange record without envelope".into(),
                    )));
                }
                check.accept(&page, &r.key)?;
                last = Some(r.key.clone());
                if !sink(r.key, r.envelope) {
                    return Ok(false);
                }
            }
            if !f.truncated {
                return Ok(true);
            }
            match last {
                Some(k) if k.len() <= MAX_RESUME_KEY_BYTES => {
                    page.lower = Some(k.clone());
                    resume = Some(k);
                }
                _ => {
                    return Err(abort(VerifyAbort::Upstream(
                        "FetchRange cannot be continued".into(),
                    )));
                }
            }
        }
    }

    /// The `crc32c` of the upstream's entry `seq`, if it still holds it and `seq <= S`.
    pub(crate) async fn entry_crc(&mut self, seq: Seq) -> Result<Option<u32>> {
        // The upstream refuses seqs past the snapshot; they are known not to be held.
        if seq == 0 || seq > self.snapshot_seq {
            return Ok(None);
        }
        let req = pb::EntryCrcs {
            from_seq: seq,
            to_seq: seq,
        };
        let id = self.send(ReqKind::EntryCrcs(req)).await?;
        let RespKind::EntryCrcs(r) = self.answer(id).await? else {
            return Err(abort(VerifyAbort::Upstream(
                "unexpected answer to EntryCrcs".into(),
            )));
        };
        Ok(r.entries
            .into_iter()
            .find(|e| e.seq == seq)
            .map(|e| e.crc32c))
    }
}

impl Oracle for Upstream {
    async fn hash(&mut self, cf: &str, ranges: &[LocalRange]) -> Result<Vec<bool>> {
        let mut ids = Vec::with_capacity(ranges.len());
        for r in ranges {
            let req = pb::HashRange {
                range: Some(r.bounds.to_pb(cf)),
                count: r.count,
                hash: r.hash.to_vec(),
            };
            ids.push(self.send(ReqKind::Hash(req)).await?);
        }
        let mut matched = Vec::with_capacity(ids.len());
        for id in ids {
            match self.answer(id).await? {
                RespKind::Hash(h) => matched.push(h.matched),
                _ => {
                    return Err(abort(VerifyAbort::Upstream(
                        "unexpected answer to HashRange".into(),
                    )));
                }
            }
        }
        Ok(matched)
    }

    async fn list(
        &mut self,
        cf: &str,
        bounds: &Bounds,
        limit: u32,
    ) -> Result<(Vec<pb::KeyDigest>, bool)> {
        let id = self
            .send(ReqKind::List(pb::ListRange {
                range: Some(bounds.to_pb(cf)),
                limit,
            }))
            .await?;
        match self.answer(id).await? {
            RespKind::List(l) => Ok((l.keys, l.truncated)),
            _ => Err(abort(VerifyAbort::Upstream(
                "unexpected answer to ListRange".into(),
            ))),
        }
    }
}

/// This slave's half: records read from the captured snapshot.
struct Local {
    snap: Arc<ReadSnapshot>,
    phys: Arc<PhysCf>,
    order: KeyOrder,
    mode: VerifyMode,
    throttle: Arc<Mutex<Throttle>>,
    stop: Arc<AtomicBool>,
    work: Arc<VerifyWork>,
}

impl LocalSide for Local {
    async fn load(&self, bounds: &Bounds) -> Result<Vec<LocalRecord>> {
        let (snap, phys, order, throttle) = (
            self.snap.clone(),
            self.phys.clone(),
            self.order.clone(),
            self.throttle.clone(),
        );
        let (mode, bounds) = (self.mode, bounds.clone());
        let (stop, work) = (self.stop.clone(), self.work.enter());
        blocking(move || {
            let _work = work;
            load_range(&snap, phys, &order, &bounds, mode, &throttle, &stop)
        })
        .await
    }
}

/// Runs a verify every `VerifyConfig::interval` while the slave is streaming. Holds only a
/// `Weak<Inner>` between runs; shutdown cancels a run in progress.
pub(crate) fn spawn(inner: &Arc<Inner>) -> Option<JoinHandle<()>> {
    let every = inner.cfg.verify.interval?;
    let mode = inner.cfg.verify.mode;
    let weak = Arc::downgrade(inner);
    let mut shutdown = inner.shutdown_tx.subscribe();
    Some(tokio::spawn(async move {
        let mut tick = interval_at(tokio::time::Instant::now() + every, every);
        tick.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = tick.tick() => {}
                _ = shutdown.changed() => return,
            }
            let Some(inner) = weak.upgrade() else { return };
            if inner.state() != SlaveState::Streaming {
                continue;
            }
            tokio::select! {
                r = verify(&inner, mode) => if let Err(e) = r {
                    tracing::warn!(node = %inner.cfg.identifier, error = %e, "scheduled verify did not complete");
                },
                _ = shutdown.changed() => return,
            }
        }
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Final fix wave, I1: refusals that retrying cannot fix are their own abort kind.
    #[test]
    fn permission_and_invalid_argument_answers_are_refusals() {
        let refused = |s: Status| {
            matches!(
                status_abort(&s),
                Error::VerifyAborted(VerifyAbort::Refused(_))
            )
        };
        assert!(refused(Status::permission_denied("no")));
        assert!(refused(Status::unauthenticated("no")));
        assert!(refused(Status::invalid_argument("bad")));
        assert!(!refused(Status::unavailable("down")));
        assert!(matches!(
            status_abort(&crate::net::reason_status(pb::Reason::Busy, "full")),
            Error::VerifyAborted(VerifyAbort::Busy)
        ));
    }

    #[test]
    fn fetch_requests_are_bounded_by_count_and_bytes() {
        let small = vec![vec![0u8; 10]; 3000];
        assert_eq!(fetch_batch_len(&small), MAX_LIST_LIMIT as usize);
        let big = vec![vec![0u8; 400 << 10]; 5];
        assert_eq!(
            fetch_batch_len(&big),
            2,
            "two 400 KiB keys fit in 1 MiB, three do not"
        );
        let huge = vec![vec![0u8; 3 << 20], vec![0u8; 1]];
        assert_eq!(
            fetch_batch_len(&huge),
            1,
            "a single large key is sent alone"
        );
        assert_eq!(fetch_batch_len(&[]), 0);
    }

    /// An `Inner` that connects to `addr` (nothing runs: the tests only open sessions).
    fn inner_for(dir: &std::path::Path, addr: std::net::SocketAddr) -> Arc<Inner> {
        inner_with(dir, addr, crate::tuning::RocksTuning::new())
    }

    fn inner_with(
        dir: &std::path::Path,
        addr: std::net::SocketAddr,
        tuning: crate::tuning::RocksTuning,
    ) -> Arc<Inner> {
        use crate::slave::config::{SlaveConfig, UpstreamConfig};
        use crate::types::Role;
        let (store, meta) = crate::store::Store::open(dir, Role::Slave, tuning).unwrap();
        let cfg = SlaveConfig::new("t", dir, UpstreamConfig::from_addr(addr));
        Arc::new(Inner::new(store, cfg, &meta).unwrap())
    }

    async fn open_session(inner: &Arc<Inner>) -> UpstreamSession {
        let client = client::connect_now(inner).await.unwrap();
        UpstreamSession::open(inner, &client, VerifyMode::Full, Duration::from_secs(10))
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn an_upstream_session_fetches_keys_ranges_and_entry_crcs() {
        use crate::log::LogApi;
        use crate::server::test_client::serving_master;
        use crate::types::{Batch, CfSpec};
        let mdir = tempfile::tempdir().unwrap();
        let sdir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(mdir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut batch = Batch::new();
        for i in 0..2500u32 {
            batch.put("c", &i.to_be_bytes(), &i.to_le_bytes());
        }
        let s = m.write(batch).unwrap(); // seq 2
        let inner = inner_for(sdir.path(), m.serve_addr().unwrap());
        let mut session = open_session(&inner).await;
        assert_eq!(session.begin.snapshot_seq, s);
        m.put("c", &0u32.to_be_bytes(), b"later").unwrap(); // after the snapshot

        // Keys: more than one request's worth, in the order asked, with absent ones.
        let mut keys: Vec<Vec<u8>> = (0..1500u32)
            .rev()
            .map(|i| i.to_be_bytes().to_vec())
            .collect();
        keys.push(b"absent".to_vec());
        let mut got = Vec::new();
        let done = session
            .upstream
            .fetch_keys("c", &keys, &mut |k, e| {
                got.push((k, e));
                true
            })
            .await
            .unwrap();
        assert!(done);
        assert_eq!(got.len(), keys.len());
        assert!(got.iter().zip(&keys).all(|((k, _), want)| k == want));
        let want = crate::codec::envelope::encode(s, None, &1499u32.to_le_bytes());
        assert_eq!(got[0].1.as_deref(), Some(want.as_slice()));
        assert_eq!(got[1500].1, None);
        let old = crate::codec::envelope::encode(s, None, &0u32.to_le_bytes());
        assert_eq!(
            got[1499].1.as_deref(),
            Some(old.as_slice()),
            "the snapshot's value"
        );

        // A sink that stops early stops the fetch.
        let mut n = 0;
        let done = session
            .upstream
            .fetch_keys("c", &keys, &mut |_, _| {
                n += 1;
                n < 3
            })
            .await
            .unwrap();
        assert!(!done);
        assert_eq!(n, 3);

        // A range: three pages, every key once, in order.
        let mut seen = Vec::new();
        let done = session
            .upstream
            .fetch_range(
                "c",
                &Bounds::default(),
                &KeyOrder::bytewise(),
                &mut |k, e| {
                    seen.push((k, e));
                    true
                },
            )
            .await
            .unwrap();
        assert!(done);
        let all: Vec<Vec<u8>> = (0..2500u32).map(|i| i.to_be_bytes().to_vec()).collect();
        assert_eq!(seen.iter().map(|(k, _)| k.clone()).collect::<Vec<_>>(), all);
        // Values are the snapshot's, not the later write to key 0.
        assert_eq!(seen[0].1.as_deref(), Some(old.as_slice()));
        assert!(seen.iter().all(|(_, e)| e.is_some()));
        let sub = Bounds {
            lower: Some(10u32.to_be_bytes().to_vec()),
            upper: Some(20u32.to_be_bytes().to_vec()),
        };
        let mut n = 0;
        session
            .upstream
            .fetch_range("c", &sub, &KeyOrder::bytewise(), &mut |_, _| {
                n += 1;
                true
            })
            .await
            .unwrap();
        assert_eq!(n, 10);

        // Entry CRCs: the entry at S, one before it, and none past the snapshot.
        let entries: Vec<_> = m.entries_since(0).unwrap().map(Result::unwrap).collect();
        let mut checked = 0;
        for e in entries.iter().filter(|e| e.seq <= s) {
            let crc = session.upstream.entry_crc(e.seq).await.unwrap();
            assert_eq!(crc, Some(e.crc32c));
            checked += 1;
        }
        assert_eq!(checked, 2, "the loop ran over entries 1 and 2");
        assert_eq!(session.upstream.entry_crc(s + 1).await.unwrap(), None);
        assert_eq!(session.upstream.entry_crc(0).await.unwrap(), None);
        // The session is still usable after those, and closes cleanly.
        session.close().await;
        drop(inner);
        m.shutdown().await.unwrap();
    }

    fn reverse_tuning() -> crate::tuning::RocksTuning {
        let mut t = crate::tuning::RocksTuning::new();
        t.comparators
            .register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        t
    }

    /// Pages cut by the server's byte limit (not the client's key limits): every key arrives
    /// exactly once and in the CF's order, with an empty key and NUL keys at page boundaries.
    /// `keys` are in the CF's order.
    async fn pages_cut_by_bytes(comparator: Option<&str>, keys: Vec<Vec<u8>>) {
        use crate::server::test_client::serving_master;
        use crate::types::{Batch, CfSpec};
        let mdir = tempfile::tempdir().unwrap();
        let sdir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(mdir.path(), |cfg| cfg.rocksdb = reverse_tuning()).await;
        let mut spec = CfSpec::new("c");
        if let Some(c) = comparator {
            spec = spec.with_comparator(c);
        }
        m.create_cf(spec).unwrap();
        let mut batch = Batch::new();
        // 600 KiB values: the server cuts a page after two of them.
        for (i, k) in keys.iter().enumerate() {
            batch.put("c", k, &vec![i as u8; 600 << 10]);
        }
        let s = m.write(batch).unwrap();
        let inner = inner_with(sdir.path(), m.serve_addr().unwrap(), reverse_tuning());
        let order = KeyOrder::new(&reverse_tuning().comparators, comparator).unwrap();
        let mut session = open_session(&inner).await;
        let envelope_of =
            |i: usize| crate::codec::envelope::encode(s, None, &vec![i as u8; 600 << 10]);

        // A range, paged.
        let mut got = Vec::new();
        let before = session.upstream.next_id;
        session
            .upstream
            .fetch_range("c", &Bounds::default(), &order, &mut |k, e| {
                got.push((k, e));
                true
            })
            .await
            .unwrap();
        assert!(
            session.upstream.next_id - before >= 3,
            "the range was paged"
        );
        let want: Vec<_> = keys
            .iter()
            .enumerate()
            .map(|(i, k)| (k.clone(), Some(envelope_of(i))))
            .collect();
        assert!(got == want, "every key once, in the CF's order");

        // Keys, with an absent one among them, answered as prefixes and continued.
        let mut ask: Vec<Vec<u8>> = keys.clone();
        ask.insert(1, b"absent".to_vec());
        let mut got = Vec::new();
        let before = session.upstream.next_id;
        session
            .upstream
            .fetch_keys("c", &ask, &mut |k, e| {
                got.push((k, e));
                true
            })
            .await
            .unwrap();
        assert!(
            session.upstream.next_id - before >= 3,
            "the keys were split"
        );
        let mut want = Vec::new();
        for k in &ask {
            let e = keys.iter().position(|x| x == k).map(envelope_of);
            want.push((k.clone(), e));
        }
        assert!(got == want, "every key once, in the order asked");
        session.close().await;
        drop(inner);
        m.shutdown().await.unwrap();
    }

    #[tokio::test]
    async fn byte_cut_pages_continue_in_bytewise_order() {
        let keys = [&b""[..], b"\0", b"\0\0", b"a", b"b", b"c"];
        pages_cut_by_bytes(None, keys.iter().map(|k| k.to_vec()).collect()).await;
    }

    #[tokio::test]
    async fn byte_cut_pages_continue_in_a_reverse_comparator_order() {
        let keys = [&b"c"[..], b"b", b"a", b"\0\0", b"\0", b""];
        pages_cut_by_bytes(Some("reverse"), keys.iter().map(|k| k.to_vec()).collect()).await;
    }

    /// An empty key is a real bound, never "absent".
    #[tokio::test]
    async fn an_empty_key_is_a_real_bound_of_a_fetched_range() {
        use crate::server::test_client::serving_master;
        use crate::types::CfSpec;
        let mdir = tempfile::tempdir().unwrap();
        let sdir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(mdir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        for k in [&b""[..], b"a"] {
            m.put("c", k, b"v").unwrap();
        }
        let inner = inner_for(sdir.path(), m.serve_addr().unwrap());
        let mut session = open_session(&inner).await;
        let o = KeyOrder::bytewise();
        for (lower, upper, want) in [
            (Some(&b""[..]), None, vec![b"".to_vec(), b"a".to_vec()]),
            (None, Some(&b""[..]), vec![]),
            (None, Some(&b"a"[..]), vec![b"".to_vec()]),
        ] {
            let b = Bounds {
                lower: lower.map(<[u8]>::to_vec),
                upper: upper.map(<[u8]>::to_vec),
            };
            let mut got = Vec::new();
            session
                .upstream
                .fetch_range("c", &b, &o, &mut |k, _| {
                    got.push(k);
                    true
                })
                .await
                .unwrap();
            assert_eq!(got, want, "{lower:?}..{upper:?}");
        }
        session.close().await;
        drop(inner);
        m.shutdown().await.unwrap();
    }

    /// An upstream that sends records outside the range or out of order is refused.
    #[test]
    fn fetched_ranges_must_be_inside_and_strictly_increasing() {
        let bytewise = KeyOrder::bytewise();
        let page = Bounds {
            lower: Some(b"b".to_vec()),
            upper: Some(b"e".to_vec()),
        };
        let is_protocol_error =
            |r: Result<()>| matches!(r, Err(Error::VerifyAborted(VerifyAbort::Upstream(_))));
        let mut c = RangeCheck {
            order: &bytewise,
            prev: None,
        };
        assert!(c.accept(&page, b"b").is_ok());
        assert!(c.accept(&page, b"c").is_ok());
        assert!(is_protocol_error(c.accept(&page, b"c")), "a repeat");
        assert!(is_protocol_error(c.accept(&page, b"b")), "going back");
        assert!(is_protocol_error(c.accept(&page, b"e")), "the upper bound");
        assert!(is_protocol_error(c.accept(&page, b"a")), "below the lower");
        assert!(c.accept(&page, b"d").is_ok());
        // Under a reverse comparator the same checks follow the CF's order.
        let reverse = KeyOrder::new(&reverse_tuning().comparators, Some("reverse")).unwrap();
        let page = Bounds {
            lower: Some(b"e".to_vec()),
            upper: Some(b"b".to_vec()),
        };
        let mut c = RangeCheck {
            order: &reverse,
            prev: None,
        };
        assert!(c.accept(&page, b"e").is_ok());
        assert!(c.accept(&page, b"d").is_ok());
        assert!(is_protocol_error(c.accept(&page, b"e")), "going back");
        assert!(is_protocol_error(c.accept(&page, b"a")), "past the upper");
    }

    #[test]
    fn fetched_keys_must_answer_a_prefix_of_the_request() {
        let rec = |k: &[u8]| pb::FetchedRecord {
            key: k.to_vec(),
            envelope: None,
        };
        let ask = vec![b"a".to_vec(), b"b".to_vec(), b"c".to_vec()];
        let bad = |r: &[pb::FetchedRecord]| {
            matches!(
                check_fetched(&ask, r),
                Err(Error::VerifyAborted(VerifyAbort::Upstream(_)))
            )
        };
        assert!(check_fetched(&ask, &[rec(b"a")]).is_ok());
        assert!(check_fetched(&ask, &[rec(b"a"), rec(b"b"), rec(b"c")]).is_ok());
        assert!(bad(&[]), "nothing answered");
        assert!(bad(&[rec(b"b")]), "a skipped key");
        assert!(bad(&[rec(b"a"), rec(b"c")]), "a gap");
        assert!(bad(&[rec(b"b"), rec(b"a")]), "swapped");
        assert!(bad(&[rec(b"a"), rec(b"b"), rec(b"c"), rec(b"d")]), "extra");
    }

    #[tokio::test]
    async fn a_fetch_of_an_unknown_cf_aborts_the_run() {
        use crate::server::test_client::serving_master;
        let mdir = tempfile::tempdir().unwrap();
        let sdir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(mdir.path(), |_| {}).await;
        let inner = inner_for(sdir.path(), m.serve_addr().unwrap());
        let mut session = open_session(&inner).await;
        let err = session
            .upstream
            .fetch_keys("nope", &[vec![1]], &mut |_, _| true)
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::VerifyAborted(VerifyAbort::Refused(ref m)) if m.contains("InvalidArgument")),
            "{err:?}"
        );
        session.close().await;
        drop(inner);
        m.shutdown().await.unwrap();
    }

    #[test]
    fn answers_are_sized_for_two_maximal_keys() {
        assert_eq!(decode_limit(0), (32 << 20) + LIST_SOFT_BYTES + DECODE_SLACK);
        assert_eq!(
            decode_limit(64 << 20),
            (128 << 20) + LIST_SOFT_BYTES + DECODE_SLACK
        );
        let busy = crate::net::reason_status(pb::Reason::Busy, "x");
        assert!(matches!(
            status_abort(&busy),
            Error::VerifyAborted(VerifyAbort::Busy)
        ));
        let gone = crate::net::reason_status(pb::Reason::SessionNotFound, "x");
        assert!(matches!(
            status_abort(&gone),
            Error::VerifyAborted(VerifyAbort::SessionExpired)
        ));
        // A reset of the upstream's state drops its sessions too.
        let reset = crate::net::reason_status(pb::Reason::UpstreamReset, "x");
        assert!(matches!(
            status_abort(&reset),
            Error::VerifyAborted(VerifyAbort::SessionExpired)
        ));
        assert!(matches!(
            status_abort(&Status::internal("x")),
            Error::VerifyAborted(VerifyAbort::Upstream(_))
        ));
    }

    /// The policy acts only on a mismatch, on a slave that is not halted, in the repoint
    /// generation the run started in.
    #[test]
    fn a_rebootstrap_needs_a_mismatch_a_live_slave_and_the_same_generation() {
        use MismatchPolicy::{Rebootstrap, Repair, Report};
        // (policy, clean, halted, started, now) -> acts
        let rows = [
            (Rebootstrap, false, false, 2, 2, true),
            (Rebootstrap, true, false, 2, 2, false),
            (Rebootstrap, false, true, 2, 2, false),
            (Rebootstrap, false, false, 2, 3, false),
            (Repair, false, false, 2, 2, true),
            (Repair, true, false, 2, 2, false),
            (Repair, false, true, 2, 2, false),
            (Repair, false, false, 2, 3, false),
            (Report, false, false, 2, 2, false),
        ];
        for (policy, clean, halted, started, now, acts) in rows {
            assert_eq!(
                may_act(policy, clean, halted, started, now),
                acts,
                "{policy:?} clean={clean} halted={halted} {started}->{now}"
            );
        }
    }
}
