//! The slave side of Verify (DESIGN.md §12, PROTOCOL.md §9): open a session upstream,
//! capture a local snapshot at exactly the upstream's seq `S` (taken by the applier right
//! after it commits `S`), compare the CF lists, then compare each common CF range by range.

use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{MissedTickBehavior, interval_at};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Status, Streaming};
use tracing::Instrument;

use super::client::{self, Client};
use super::inner::{CaptureHold, CaptureMiss, Inner};
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
    LIST_SOFT_BYTES, MismatchPolicy, VERIFY_WINDOW, VerifyAbort, VerifyMode, VerifyReport,
};

/// The longest the applier stays held waiting for `VerifyBegin` (bounded by `capture_timeout`
/// too). While held the slave applies and acknowledges nothing (DESIGN.md §12).
const BEGIN_TIMEOUT: Duration = Duration::from_secs(10);
/// The longest one answer on the verify stream may take.
const ANSWER_TIMEOUT: Duration = Duration::from_secs(60);

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
    let span = tracing::info_span!(
        "verify",
        node_id = %inner.cfg.identifier,
        upstream = %inner.upstream_uri(),
        seq = tracing::field::Empty,
        epoch = identity.epoch,
        mode = ?mode,
    );
    let result = session(inner, identity, mode, &span)
        .instrument(span.clone())
        .await;
    span.in_scope(|| match &result {
        Ok(report) => finish(inner, report),
        Err(e) => tracing::warn!(error = %e, "verify abandoned"),
    });
    result
}

fn finish(inner: &Inner, report: &VerifyReport) {
    // A halted slave may be on another history than its upstream (a divergence the handshake
    // refused): a mismatch policy must not act on that. The report is still kept.
    let halted = matches!(inner.state(), SlaveState::Halted(_));
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
        if !halted && inner.cfg.verify.on_mismatch == MismatchPolicy::Rebootstrap {
            // For the upstream the slave follows now; a later repoint makes it inert.
            inner.force_bootstrap.request(*inner.repoint_tx.borrow());
            inner.rebootstrap.notify_one();
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
) -> Result<VerifyReport> {
    let started = Instant::now();
    let limit = decode_limit(inner.max_entry_bytes.load(Ordering::SeqCst));
    // Connected before the applier is held, so the pause never includes TCP or TLS setup.
    let mut client = client::connect_now(inner)
        .await
        .map_err(|e| abort(VerifyAbort::Upstream(format!("connect: {e}"))))?
        .max_decoding_message_size(limit);
    let req = pb::VerifyBeginRequest {
        client_id: inner.cfg.identifier.clone(),
        mode: mode.to_pb() as i32,
    };
    // The applier commits nothing while the upstream pins its snapshot, so this slave cannot
    // pass the snapshot's seq before the capture is set up. The pause is bounded.
    let hold = inner.hold_applier();
    let bound = BEGIN_TIMEOUT.min(inner.cfg.verify.capture_timeout);
    let begin = tokio::time::timeout(bound, client.verify_begin(req))
        .await
        .map_err(|_| {
            abort(VerifyAbort::Upstream(format!(
                "VerifyBegin got no answer within {bound:?}"
            )))
        })?
        .map_err(|s| status_abort(&s))?
        .into_inner();
    span.record("seq", begin.snapshot_seq);
    let result = compare(inner, &mut client, &begin, hold, identity, mode, started).await;
    // Best effort: the upstream also expires idle sessions.
    let _ = client
        .verify_end(pb::VerifyEndRequest {
            session_id: begin.session_id.clone(),
        })
        .await;
    result
}

async fn compare(
    inner: &Arc<Inner>,
    client: &mut Client,
    begin: &pb::VerifyBeginResponse,
    hold: CaptureHold,
    identity: DbIdentity,
    mode: VerifyMode,
    started: Instant,
) -> Result<VerifyReport> {
    let upstream_identity = begin.identity.as_ref().map(identity_from_pb).transpose()?;
    if upstream_identity != Some(identity) {
        return Err(abort(VerifyAbort::IdentityChanged));
    }
    let s = begin.snapshot_seq;
    let snap = Arc::new(capture(inner, hold, s).await?);
    let meta_snap = snap.clone();
    let local_cfs = blocking(move || meta_snap.meta()).await?.cf_specs;
    let upstream_cfs: Vec<CfEntry> = begin.cf_specs.iter().map(cf_from_pb).collect();
    let (mut mismatches, common) = compare_specs(&local_cfs, &upstream_cfs);

    let (tx, rx) = mpsc::channel(2 * VERIFY_WINDOW);
    let answers = client
        .verify_range(ReceiverStream::new(rx))
        .await
        .map_err(|s| status_abort(&s))?
        .into_inner();
    let mut upstream = Upstream {
        tx,
        answers,
        session_id: begin.session_id.clone(),
        next_id: 0,
    };
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
            tokio::task::spawn_blocking(move || {
                let emit = |r: LocalRange| range_tx.blocking_send(Ok(r)).is_ok();
                match chunk_cf(&snap, phys, mode, chunk_keys, chunk_bytes, &throttle, emit) {
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
        };
        let mut run = CfRun {
            cf: &name,
            order,
            min_chunk_keys: cfg.min_chunk_keys,
            budget: &mut budget,
            out: &mut mismatches,
        };
        let compared = compare_cf(&mut upstream, &local, &mut run, &mut ranges).await;
        // Unblocks the producer if the comparison stopped early.
        drop(ranges);
        let keys = producer
            .await
            .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        compared?;
        // `None` means the scan stopped early because the receiver was gone, which cannot
        // happen before the comparison returned: never count it as a full scan.
        let keys = keys.ok_or_else(|| {
            abort(VerifyAbort::Upstream(format!(
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

/// The upstream's half of a session, over one `VerifyRange` stream.
struct Upstream {
    tx: mpsc::Sender<pb::VerifyRangeRequest>,
    answers: Streaming<pb::VerifyRangeResponse>,
    session_id: Vec<u8>,
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
                RespKind::List(_) => {
                    return Err(abort(VerifyAbort::Upstream(
                        "a listing answered a hash request".into(),
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
            RespKind::Hash(_) => Err(abort(VerifyAbort::Upstream(
                "a hash answered a listing request".into(),
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
        blocking(move || load_range(&snap, phys, &order, &bounds, mode, &throttle)).await
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
        assert!(matches!(
            status_abort(&Status::internal("x")),
            Error::VerifyAborted(VerifyAbort::Upstream(_))
        ));
    }
}
