//! Installing a bootstrap from the upstream (PROTOCOL.md §8.3, DESIGN.md §10).

use std::cmp::Ordering as KeyOrdering;
use std::sync::Arc;
use std::sync::atomic::Ordering;

use tonic::Streaming;

use super::client::Client;
use super::driver::{SessionEnd, status_end, unless_repointed};
use super::inner::Inner;
use super::state::{HaltReason, SlaveState};
use crate::codec::digest::Digest;
use crate::codec::{ENVELOPE_VERSION, envelope, validate_cf_name};
use crate::config::MAX_BOOTSTRAP_CHUNK_BYTES;
use crate::error::{Error, Result};
use crate::failpoint;
use crate::net::DECODE_SLACK;
use crate::net::convert::{cf_from_pb, checked_history, identity_from_pb};
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
/// the following Tail. On any failure the slave stays in `bootstrapping` (persisted) and the
/// next session starts over.
pub(crate) async fn install(
    inner: &Arc<Inner>,
    generation: u64,
    c: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<Vec<u8>, SessionEnd> {
    inner.force_bootstrap.clear(generation);
    inner.set_ident(None);
    inner.reset_downstream();
    inner.seq_tx.send_replace(0);
    inner.set_state(SlaveState::Bootstrapping {
        target_seq: 0,
        records: 0,
    });
    blocking(inner, |i| i.store.reset_for_bootstrap())
        .await
        .map_err(|e| retry(format!("could not reset for bootstrap: {e}")))?;

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
    tracing::info!(node = %inner.cfg.identifier, snapshot_seq = s, records = count, "bootstrap complete");
    inner.notify(|l| l.on_bootstrap_complete(s));
    inner.seq_tx.send_replace(s);
    Ok(plan.lease_id)
}
