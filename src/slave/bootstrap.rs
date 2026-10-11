//! Installing a bootstrap from the upstream (PROTOCOL.md §8.3, DESIGN.md §10).

use std::cmp::Ordering as KeyOrdering;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tonic::Streaming;

use super::client::Client;
use super::config::IdentityPolicy;
use super::driver::{SessionEnd, status_end, unless_repointed};
use super::inner::Inner;
use super::repair::deeper_fork;
use super::state::{HaltReason, SlaveState};
use crate::codec::digest::Digest;
use crate::codec::{ENVELOPE_VERSION, envelope, validate_cf_name};
use crate::config::MAX_BOOTSTRAP_CHUNK_BYTES;
use crate::error::{Error, Result};
use crate::failpoint;
use crate::net::DECODE_SLACK;
use crate::net::convert::{cf_from_pb, checked_history, epochs_from_pb, identity_from_pb};
use crate::pb;
use crate::pb::bootstrap_chunk::Kind;
use crate::store::bootstrap::BootstrapFinish;
use crate::types::{CfEntry, DbIdentity, EpochStart, Seq};

/// Runs `f` with the slave's state on a blocking thread.
async fn blocking<T: Send + 'static>(
    inner: &Arc<Inner>,
    f: impl FnOnce(&Inner) -> Result<T> + Send + 'static,
) -> Result<T> {
    let i = inner.clone();
    crate::background::blocking(move || f(&i)).await
}

fn retry(msg: impl Into<String>) -> SessionEnd {
    SessionEnd::Retry(msg.into())
}

/// The header, validated.
struct Plan {
    identity: DbIdentity,
    history: Vec<EpochStart>,
    snapshot_seq: Seq,
    snapshot_crc: Option<u32>,
    cfs: Vec<CfEntry>,
    max_entry_bytes: u64,
    lease_id: Vec<u8>,
}

impl Plan {
    /// A header that cannot be valid (bad identity or epoch history) halts with `Corrupt`;
    /// an inconsistent one (versions, CF list) is retried.
    fn from_header(h: pb::BootstrapHeader) -> std::result::Result<Plan, SessionEnd> {
        if h.envelope_version != u32::from(ENVELOPE_VERSION) {
            return Err(retry(format!(
                "bootstrap envelope version {} != {ENVELOPE_VERSION}",
                h.envelope_version
            )));
        }
        let corrupt = |e: Error| {
            tracing::error!(error = %e, "invalid bootstrap header");
            SessionEnd::Halt(HaltReason::Corrupt)
        };
        let identity = h
            .identity
            .as_ref()
            .ok_or_else(|| Error::corrupt("bootstrap header without identity"));
        let identity = identity.and_then(identity_from_pb).map_err(corrupt)?;
        let history = checked_history(&identity, &h.epoch_history).map_err(corrupt)?;
        let cfs: Vec<CfEntry> = h.cf_specs.iter().map(cf_from_pb).collect();
        for c in &cfs {
            validate_cf_name(&c.spec.name).map_err(|e| retry(e.to_string()))?;
        }
        if !cfs
            .windows(2)
            .all(|w| w[0].spec.name.as_bytes() < w[1].spec.name.as_bytes())
        {
            return Err(retry("bootstrap cf_specs are not sorted by name"));
        }
        Ok(Plan {
            identity,
            history,
            snapshot_seq: h.snapshot_seq,
            snapshot_crc: h.snapshot_crc32c,
            cfs,
            max_entry_bytes: h.max_entry_bytes,
            lease_id: h.lease_id,
        })
    }
}

async fn next(
    inner: &Inner,
    generation: u64,
    s: &mut Streaming<pb::BootstrapChunk>,
) -> std::result::Result<Kind, SessionEnd> {
    match unless_repointed(inner, generation, s.message()).await? {
        Ok(Some(chunk)) => chunk.kind.ok_or_else(|| retry("empty bootstrap chunk")),
        Ok(None) => Err(retry("bootstrap stream ended before the footer")),
        Err(status) => Err(status_end(inner, generation, &status)),
    }
}

/// Replaces all local state with the upstream's snapshot. Returns the lease to present in
/// the following Tail. On any failure after the reset the slave stays in `bootstrapping`
/// (persisted) and the next session starts over.
///
/// The header is read before anything local changes. With `expected` (a repair's fallback, or
/// any bootstrap an automatic request, a policy or a repoint's mode asked for: the consent
/// covers only the database that answered `hs`, and only the divergence `hs` showed), a header of
/// another database follows the identity policy instead (`Refuse` halts `IdentityChanged`),
/// and a header of the same database whose history forks deeper than `hs` showed (any fork at
/// or below `last_seq` after a RESUME, as for a verify repair) ends with `Retry`. Nothing is
/// installed in either case. `None` is used only for the operator's `clear_halt(Rebootstrap)`,
/// whose consent is broad by design: it installs whatever the upstream serves.
pub(crate) async fn install(
    inner: &Arc<Inner>,
    generation: u64,
    c: &Client,
    hs: &pb::HandshakeResponse,
    expected: Option<uuid::Uuid>,
) -> std::result::Result<Vec<u8>, SessionEnd> {
    let limit = usize::try_from(hs.max_entry_bytes)
        .unwrap_or(usize::MAX)
        .max(MAX_BOOTSTRAP_CHUNK_BYTES)
        .saturating_add(DECODE_SLACK);
    let req = pb::BootstrapRequest {
        client_id: inner.cfg.identifier.clone(),
    };
    let mut stream = match unless_repointed(
        inner,
        generation,
        c.clone().max_decoding_message_size(limit).bootstrap(req),
    )
    .await?
    {
        Ok(r) => r.into_inner(),
        Err(s) => return Err(status_end(inner, generation, &s)),
    };
    let Kind::Header(header) = next(inner, generation, &mut stream).await? else {
        return Err(retry("bootstrap stream did not start with a header"));
    };
    let plan = Plan::from_header(header)?;
    if let Some(want) = expected {
        if plan.identity.db_uuid != want {
            tracing::warn!(node = %inner.cfg.identifier, expected = %want, got = %plan.identity.db_uuid, "the bootstrap is from another database than the handshake answered for");
            match inner.cfg.on_identity_change {
                IdentityPolicy::Refuse => {
                    return Err(SessionEnd::Halt(HaltReason::IdentityChanged));
                }
                // Configured consent to replace the data with another database.
                IdentityPolicy::Rebootstrap => {}
            }
        } else if let Some(fork) = deeper_fork(
            &inner.known_epochs(),
            inner.store.position().last_seq,
            &epochs_from_pb(&hs.epoch_history),
            &plan.history,
        ) {
            // The same database, but its history now leaves this slave's earlier than the
            // answer the consent was given for: installing would discard entries the consent
            // never covered. Nothing changed locally; the next handshake decides afresh.
            tracing::warn!(node = %inner.cfg.identifier, fork, "the bootstrap's history forks deeper than consented");
            return Err(retry(format!(
                "bootstrap refused: the upstream's history now forks deeper (at {fork}) than the consent was given for"
            )));
        }
    }
    // A repoint since this session started: the consent and the header are about the old
    // upstream. Nothing local has changed yet; the next session decides afresh.
    if *inner.repoint_tx.borrow() != generation {
        return Err(SessionEnd::Again);
    }
    inner.force_bootstrap.clear(generation);
    // Automatic and verify requests are spent once a bootstrap installs.
    inner.auto_rebootstrap.clear();
    inner.verify_rebootstrap.clear();
    // A verify waiting for a seq of the old state can never be served now.
    inner.cancel_capture();
    // A bootstrap replaces everything a repair would have fixed (the store removes the
    // persisted `repair_until` and `diverged_at` with the rest of the state).
    inner.clear_pending_repair();
    // Not ready first (no identity), then the repair target goes: a mid-replay mixed state is
    // never ready, not even between the two.
    inner.set_ident(None);
    inner.begin_repair(0);
    // The snapshot's last entry is not transferred: its ts_ms is unknown until the next entry.
    inner.last_applied_ts.store(0, Ordering::SeqCst);
    inner.pending_ts.store(0, Ordering::SeqCst);
    inner.reset_downstream();
    inner.seq_tx.send_replace(0);
    inner.set_state(SlaveState::Bootstrapping {
        target_seq: 0,
        records: 0,
    });
    blocking(inner, |i| i.store.reset_for_bootstrap())
        .await
        .map_err(|e| retry(format!("could not reset for bootstrap: {e}")))?;
    let s = plan.snapshot_seq;
    tracing::info!(node = %inner.cfg.identifier, snapshot_seq = s, cfs = plan.cfs.len(), "bootstrapping");
    inner.notify(|l| l.on_bootstrap_begin(s));
    inner.set_state(SlaveState::Bootstrapping {
        target_seq: s,
        records: 0,
    });

    for cf in &plan.cfs {
        let cf = cf.clone();
        match blocking(inner, move |i| i.store.begin_bootstrap_cf(&cf)).await {
            Ok(()) => {}
            Err(Error::UnknownComparator(name)) => {
                tracing::error!(node = %inner.cfg.identifier, comparator = %name, "bootstrap needs an unregistered comparator");
                return Err(SessionEnd::Halt(HaltReason::SpecMismatch));
            }
            Err(e) => {
                return Err(retry(format!(
                    "could not create bootstrap column family: {e}"
                )));
            }
        }
    }

    let mut digest = Digest::new();
    let mut marked = 0usize; // CFs whose digest marker has been added
    let mut last_key: Option<Vec<u8>> = None;
    let mut count = 0u64;
    let footer = loop {
        match next(inner, generation, &mut stream).await? {
            Kind::Records(r) => {
                let j = plan
                    .cfs
                    .iter()
                    .position(|c| c.spec.name == r.cf)
                    .ok_or_else(|| retry(format!("records for unknown CF {}", r.cf)))?;
                if j + 1 < marked {
                    return Err(retry(format!(
                        "records for CF {} arrived out of order",
                        r.cf
                    )));
                }
                if j + 1 > marked {
                    for cf in &plan.cfs[marked..=j] {
                        digest.cf_marker(&cf.spec.name, cf.create_seq);
                    }
                    marked = j + 1;
                    last_key = None;
                }
                if r.records.is_empty() {
                    return Err(retry("empty records message"));
                }
                let comparator = plan.cfs[j].spec.comparator.as_deref();
                let mut batch = Vec::with_capacity(r.records.len());
                for rec in r.records {
                    if let Some(prev) = &last_key {
                        if inner
                            .store
                            .tuning()
                            .comparators
                            .compare(comparator, prev, &rec.key)
                            != KeyOrdering::Less
                        {
                            return Err(retry(format!(
                                "bootstrap keys of CF {} are not strictly ascending",
                                r.cf
                            )));
                        }
                    }
                    envelope::decode(&rec.envelope)
                        .map_err(|e| retry(format!("bad envelope from upstream: {e}")))?;
                    digest.record(&rec.key, &rec.envelope);
                    last_key = Some(rec.key.clone());
                    batch.push((rec.key, rec.envelope));
                }
                count += batch.len() as u64;
                let cf = plan.cfs[j].clone();
                blocking(inner, move |i| i.store.write_bootstrap_records(&cf, &batch))
                    .await
                    .map_err(|e| retry(format!("could not write bootstrap records: {e}")))?;
                inner.set_state(SlaveState::Bootstrapping {
                    target_seq: s,
                    records: count,
                });
                if failpoint::fire("slave-bootstrap-after-chunk") {
                    // Emulated stall mid-bootstrap: freeze until the test kills the node or
                    // repoints it.
                    unless_repointed(inner, generation, std::future::pending::<()>()).await?;
                }
            }
            Kind::Footer(f) => break f,
            Kind::Header(_) => return Err(retry("second bootstrap header")),
        }
    };
    for cf in &plan.cfs[marked..] {
        digest.cf_marker(&cf.spec.name, cf.create_seq);
    }
    let (n, d) = digest.finish_bootstrap(s);
    if n != footer.record_count || d.as_slice() != footer.digest.as_slice() {
        return Err(retry(format!(
            "bootstrap footer mismatch: {n} records received, {} announced, or the digest differs; starting again",
            footer.record_count
        )));
    }
    let finish = BootstrapFinish {
        identity: plan.identity,
        epoch_history: plan.history.clone(),
        cf_specs: plan.cfs.clone(),
        snapshot_seq: s,
        snapshot_crc: plan.snapshot_crc,
        max_entry_bytes: plan.max_entry_bytes,
    };
    blocking(inner, move |i| i.store.finish_bootstrap(&finish))
        .await
        .map_err(|e| retry(format!("could not commit the bootstrap: {e}")))?;
    inner
        .max_entry_bytes
        .store(plan.max_entry_bytes, Ordering::SeqCst);
    inner.set_ident(Some((plan.identity, plan.history)));
    // The bootstrap is over and the data ready: the slave is about to tail, as after a resumed
    // handshake (`Streaming` once the Tail opens). Set before `seq_tx`, so nothing woken by
    // the new seq (`wait_for_seq`) still sees `Bootstrapping`.
    inner.set_state(SlaveState::Connecting);
    tracing::info!(node = %inner.cfg.identifier, snapshot_seq = s, records = count, "bootstrap complete");
    inner.notify(|l| l.on_bootstrap_complete(s));
    inner.seq_tx.send_replace(s);
    Ok(plan.lease_id)
}
