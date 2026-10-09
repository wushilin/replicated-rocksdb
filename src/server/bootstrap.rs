//! Bootstrap: a consistent snapshot streamed as header, records and footer, with a
//! retention guard kept as a lease until the client's Tail (PROTOCOL.md §8).

use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::{call_info, store_status, BootstrapStream, ServerState};
use crate::auth::Method;
use crate::codec::digest::Digest;
use crate::codec::ENVELOPE_VERSION;
use crate::net::convert::{cf_to_pb, epochs_to_pb, identity_to_pb};
use crate::net::reason_status;
use crate::pb;
use crate::pb::bootstrap_chunk::Kind;
use crate::read::ReadSnapshot;
use crate::store::meta::MetaSnapshot;
use crate::store::retention::RetentionGuard;

const CHANNEL: usize = 4;

pub(super) async fn bootstrap(
    state: &Arc<ServerState>,
    req: Request<pb::BootstrapRequest>,
) -> Result<Response<BootstrapStream>, Status> {
    let call = call_info(&req, Method::Bootstrap)?;
    let src = state.authorize(&call, Method::Bootstrap).await?;
    let client_id = req.into_inner().client_id;
    call.check_client_id(&client_id)?;
    state.expire_leases(std::time::Instant::now());
    if !src.is_ready() {
        return Err(reason_status(pb::Reason::NotReady, "server is bootstrapping"));
    }
    let permit = state
        .bootstraps
        .clone()
        .try_acquire_owned()
        .map_err(|_| reason_status(pb::Reason::Busy, "too many bootstraps in progress"))?;
    let store = src.store();
    // Guard first (at a seq no later than the snapshot's), so retention cannot remove
    // entry S + 1 between taking the snapshot and registering the real guard.
    let provisional = store.guard(store.position().last_seq);
    let snap = store.read_snapshot(0).map_err(store_status)?;
    let meta = snap.meta().map_err(store_status)?;
    let guard = store.guard(meta.last_seq);
    drop(provisional);
    let Some(identity) = meta.identity else {
        return Err(reason_status(pb::Reason::NotReady, "server has no data yet"));
    };
    let lease_id = *uuid::Uuid::new_v4().as_bytes();
    let header = pb::BootstrapHeader {
        identity: Some(identity_to_pb(&identity)),
        snapshot_seq: meta.last_seq,
        snapshot_crc32c: meta.last_entry_crc,
        cf_specs: meta.cf_specs.iter().map(cf_to_pb).collect(),
        max_entry_bytes: src.max_entry_bytes(),
        envelope_version: u32::from(ENVELOPE_VERSION),
        lease_id: lease_id.to_vec(),
        lease_ttl_ms: u64::try_from(state.cfg.limits.bootstrap_lease_ttl.as_millis()).unwrap_or(u64::MAX),
        epoch_history: epochs_to_pb(&meta.epoch_history),
    };
    tracing::info!(client = %client_id, snapshot_seq = meta.last_seq, cfs = meta.cf_specs.len(), "serving bootstrap");
    let (tx, rx) = mpsc::channel(CHANNEL);
    let mut resets = src.resets();
    resets.borrow_and_update();
    let job = Job {
        state: state.clone(),
        snap,
        meta,
        header,
        guard,
        lease_id,
        client_id,
        tx,
        resets,
        runtime: tokio::runtime::Handle::current(),
    };
    // The permit is returned only after the job, and with it the snapshot, is gone:
    // `ServerHandle::shutdown` waits for all permits, so no snapshot outlives a shutdown.
    tokio::task::spawn_blocking(move || {
        job.run();
        drop(permit);
    });
    Ok(Response::new(ReceiverStream::new(rx)))
}

struct Job {
    state: Arc<ServerState>,
    snap: ReadSnapshot,
    meta: MetaSnapshot,
    header: pb::BootstrapHeader,
    guard: RetentionGuard,
    lease_id: [u8; 16],
    client_id: String,
    tx: mpsc::Sender<Result<pb::BootstrapChunk, Status>>,
    resets: watch::Receiver<u64>,
    runtime: tokio::runtime::Handle,
}

fn chunk(kind: Kind) -> Result<pb::BootstrapChunk, Status> {
    Ok(pb::BootstrapChunk { kind: Some(kind) })
}

impl Job {
    /// Runs on a blocking thread and consumes the job: the snapshot and the guard are dropped
    /// when it returns. Any send failure means the client went away.
    fn run(self) {
        let Job { state, snap, meta, header, guard, lease_id, client_id, tx, resets, runtime } = self;
        // A send also gives up on shutdown or reset, so a client that stops reading cannot
        // keep this job (and the node's database) alive.
        let mut stop = state.shutdown.clone();
        let mut reset = resets.clone();
        let mut send = |m: Result<pb::BootstrapChunk, Status>| {
            runtime.block_on(async {
                tokio::select! {
                    r = tx.send(m) => r.is_ok(),
                    _ = super::stopped(&mut stop) => false,
                    _ = reset.changed() => false,
                }
            })
        };
        // Ends the stream with an error (see `end_stream`).
        let (mut stop_end, mut reset_end) = (state.shutdown.clone(), resets.clone());
        let mut end = |status: Status| runtime.block_on(super::end_stream(&tx, status, &mut stop_end, &mut reset_end));
        if !send(chunk(Kind::Header(header))) {
            return;
        }
        let chunk_bytes = state.cfg.limits.bootstrap_chunk_bytes;
        let mut digest = Digest::new();
        let ctx = snap.raw_ctx();
        for cf in &meta.cf_specs {
            digest.cf_marker(&cf.spec.name, cf.create_seq);
            let Some(phys) = snap.phys(&cf.spec.name).filter(|p| p.entry == *cf) else {
                end(reason_status(pb::Reason::Corrupt, format!("CF {} is missing from the snapshot", cf.spec.name)));
                return;
            };
            let iter = match ctx.iter_raw(phys) {
                Ok(iter) => iter,
                Err(e) => {
                    end(store_status(e));
                    return;
                }
            };
            let mut records = Vec::new();
            let mut size = 0usize;
            for item in iter {
                if state.is_shutting_down() {
                    end(reason_status(pb::Reason::ShuttingDown, "server is shutting down"));
                    return;
                }
                if resets.has_changed().unwrap_or(true) {
                    end(reason_status(pb::Reason::UpstreamReset, "server state was replaced"));
                    return;
                }
                let (key, envelope) = match item {
                    Ok(kv) => kv,
                    Err(e) => {
                        end(store_status(e));
                        return;
                    }
                };
                digest.record(&key, &envelope);
                size += key.len() + envelope.len();
                records.push(pb::Record { key: key.into_vec(), envelope: envelope.into_vec() });
                if size >= chunk_bytes {
                    let msg = pb::BootstrapRecords { cf: cf.spec.name.clone(), records: std::mem::take(&mut records) };
                    if !send(chunk(Kind::Records(msg))) {
                        return;
                    }
                    size = 0;
                }
            }
            if !records.is_empty() && !send(chunk(Kind::Records(pb::BootstrapRecords { cf: cf.spec.name.clone(), records }))) {
                return;
            }
        }
        let (record_count, digest) = digest.finish_bootstrap(meta.last_seq);
        // The lease exists before the client can see the footer and call Tail.
        state.insert_lease(lease_id, client_id, meta.last_seq, guard);
        let footer = pb::BootstrapFooter { record_count, digest: digest.to_vec() };
        if !send(chunk(Kind::Footer(footer))) {
            state.remove_lease(&lease_id);
        }
    }
}
