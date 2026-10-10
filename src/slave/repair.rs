//! Repair (DESIGN.md §12.1): bring a slave back in line with its upstream by rewriting only
//! the keys that differ, instead of a full bootstrap.
//!
//! Both kinds run in the driver, while no entry is being applied, in two phases: a network
//! phase that reads the local suffix and fetches the upstream's envelopes over a verify
//! session (bounded in time, abandoned on a repoint, writes nothing), and a local commit that
//! is one synced batch, never cancelled once it starts.
//! - **Divergence repair**: the local log has entries `F..=L` the upstream's history does not
//!   have. `F` comes from the epoch histories (nonces included, addendum §7), with the CRC of
//!   entry `F - 1` checked where both sides hold it. The keys the suffix touched are set to the
//!   upstream's state at its snapshot `S >= F - 1`, the suffix is dropped, `last_seq` becomes
//!   `F - 1`, and the slave tails from `F`. Replaying `F..=S` (absolute ops) over the repaired
//!   keys gives exactly the upstream's state at `S`; until then reads answer `NotReady`.
//! - **Verify repair** (`MismatchPolicy::Repair`): the keys a verify reported are set to the
//!   upstream's state at a fresh snapshot `S >= L`; replaying `L + 1..=S` completes it. An
//!   upstream behind this slave (`S < L`) is retried later.
//!
//! Anything this cannot prove falls back to a full bootstrap from the same upstream: a CF
//! operation in the suffix, a suffix or fork entry that is no longer retained, histories that
//! do not show the fork, an unfinished earlier repair, a CF that differs between the sides, an
//! undecodable entry or envelope, a key or range bound too large to fetch, or more than
//! `repair_max_bytes` to read, fetch or delete.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
use std::sync::atomic::Ordering;
use std::time::Duration;

use super::client::Client;
use super::inner::Inner;
use super::verifier::{FETCH_REQUEST_BYTES, UpstreamSession};
use crate::background::blocking;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::failpoint;
use crate::net::convert::{cf_from_pb, checked_history, epochs_from_pb, identity_from_pb};
use crate::pb;
use crate::store::cfs::PhysCf;
use crate::store::repair::{KeyWrite, Rewind};
use crate::types::{CfEntry, DbIdentity, EpochStart, LogEntry, Op, Seq, fork_from};
use crate::verify::range::{Bounds, KeyOrder};
use crate::verify::{MAX_RESUME_KEY_BYTES, Mismatch, VerifyAbort, VerifyMode};

/// The network phase of a repair (session, fetches) must finish within this time.
pub(crate) const REPAIR_TIMEOUT: Duration = Duration::from_secs(120);
/// How long `VerifyBegin` and opening the session's stream may take for a repair.
const OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a repair did not happen. Nothing was written in either case.
#[derive(Debug)]
pub(crate) enum RepairError {
    /// Repair cannot handle this case: bootstrap from the same upstream instead.
    Fallback(String),
    /// A transient failure (upstream unreachable or busy, session lost, upstream behind): try
    /// again later.
    Retry(String),
    /// A failure that retrying cannot fix: the upstream refuses verify sessions
    /// (`PERMISSION_DENIED`, `UNAUTHENTICATED`) or rejects a request as invalid
    /// (`INVALID_ARGUMENT`), it sent an invalid identity or epoch history, or a comparator is
    /// not registered here. A divergence repair falls back to a bootstrap (its consent covers
    /// that); a verify repair is dropped until the operator verifies again.
    Refused(String),
}

impl From<Error> for RepairError {
    fn from(e: Error) -> Self {
        match e {
            Error::VerifyAborted(VerifyAbort::Refused(_)) | Error::UnknownComparator(_) => {
                RepairError::Refused(e.to_string())
            }
            e => RepairError::Retry(e.to_string()),
        }
    }
}

/// What a repair did.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Repaired {
    /// Local entries that were dropped (empty when none were).
    pub discarded: RangeInclusive<Seq>,
    /// The new `last_seq`.
    pub rewound_to: Seq,
    /// Reads answer `NotReady` until this seq is applied again.
    pub ready_at: Seq,
}

/// The keys and ranges to take from the upstream, per CF.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct AffectedSet {
    pub keys: BTreeMap<String, BTreeSet<Vec<u8>>>,
    pub ranges: BTreeMap<String, Vec<Bounds>>,
}

impl AffectedSet {
    /// What the entries of a divergent suffix touched. `None` if one of them creates or
    /// drops a CF (repair cannot undo that).
    pub(crate) fn from_entries(entries: &[LogEntry]) -> Result<Option<AffectedSet>> {
        let mut set = AffectedSet::default();
        for e in entries {
            for op in e.decode_ops()? {
                match op {
                    Op::Put { cf, key, .. } | Op::Delete { cf, key } => {
                        set.keys.entry(cf).or_default().insert(key);
                    }
                    Op::DeleteRange { cf, start, end } => {
                        set.ranges.entry(cf).or_default().push(Bounds {
                            lower: Some(start),
                            upper: Some(end),
                        });
                    }
                    Op::CreateCf { .. } | Op::DropCf { .. } => return Ok(None),
                }
            }
        }
        set.dedup_ranges();
        Ok(Some(set))
    }

    /// What a verify reported. `None` if the CF lists differ (repair cannot fix that).
    pub(crate) fn from_mismatches(mismatches: &[Mismatch]) -> Option<AffectedSet> {
        let mut set = AffectedSet::default();
        for m in mismatches {
            match m {
                Mismatch::Key { cf, key, .. } => {
                    set.keys.entry(cf.clone()).or_default().insert(key.clone());
                }
                Mismatch::Range { cf, lower, upper } => {
                    set.ranges.entry(cf.clone()).or_default().push(Bounds {
                        lower: lower.clone(),
                        upper: upper.clone(),
                    });
                }
                Mismatch::CfSpec { .. } => return None,
            }
        }
        set.dedup_ranges();
        Some(set)
    }

    pub(crate) fn merge(&mut self, other: AffectedSet) {
        for (cf, keys) in other.keys {
            self.keys.entry(cf).or_default().extend(keys);
        }
        for (cf, ranges) in other.ranges {
            self.ranges.entry(cf).or_default().extend(ranges);
        }
        self.dedup_ranges();
    }

    /// Sorts each CF's ranges by their bound bytes and drops identical ones: a range deleted
    /// (or reported) several times is fetched, cleared and counted against the budget once.
    fn dedup_ranges(&mut self) {
        for ranges in self.ranges.values_mut() {
            ranges.sort_by(|a, b| (&a.lower, &a.upper).cmp(&(&b.lower, &b.upper)));
            ranges.dedup();
        }
    }

    fn cfs(&self) -> BTreeSet<&String> {
        self.keys.keys().chain(self.ranges.keys()).collect()
    }
}

fn fallback(why: impl Into<String>) -> RepairError {
    RepairError::Fallback(why.into())
}

/// Runs `f` on a blocking thread, counted as blocking work `Slave::shutdown` waits for.
async fn tracked<T: Send + 'static>(
    inner: &Arc<Inner>,
    f: impl FnOnce(&Inner) -> Result<T> + Send + 'static,
) -> Result<T> {
    let (i, work) = (inner.clone(), inner.verify_work.enter());
    blocking(move || {
        let _work = work;
        f(&i)
    })
    .await
}

/// Where a log whose newest epoch records are `local` (at most `KNOWN_EPOCHS`, as sent in a
/// handshake) leaves `upstream`'s history, if at or below `until`: the first seq whose entry
/// the upstream's history does not share (PROTOCOL.md §6.3, `fork_from`, nonces included).
/// A fork computed too low is safe for a repair (more is rewritten than needed); the
/// histories never show one too high.
pub(crate) fn history_fork(
    local: &[EpochStart],
    until: Seq,
    upstream: &[EpochStart],
) -> Option<Seq> {
    fork_from(upstream, local).filter(|&f| f <= until)
}

/// Whether a fallback bootstrap may install an `offered` epoch history: `Some(fork)` when it
/// leaves this slave's history (newest records `local`, log up to `last`) earlier than the
/// `consented` history of the handshake whose answer the consent was given for.
///
/// - Divergence repair (`consented` shows a fork `F <= last`): the consent covers discarding
///   `F..=last`; an offered history that forks below `F` would discard more.
/// - Verify repair (`consented` answered RESUME: no fork at or below `last`): the consent
///   covers no divergence at all; an offered history that forks at or below `last` would
///   discard entries the default policy keeps (`DIVERGED` halts).
///
/// Records are compared with their nonces, as the handshake does (`fork_from`).
pub(crate) fn deeper_fork(
    local: &[EpochStart],
    last: Seq,
    consented: &[EpochStart],
    offered: &[EpochStart],
) -> Option<Seq> {
    let bound = fork_from(consented, local).map_or(last + 1, |f| f.min(last + 1));
    fork_from(offered, local).filter(|&f| f < bound)
}

/// Finds where the local divergent suffix starts and records it for `Slave::diverged_entries`,
/// with the computation `Error::Diverged` reports ([`super::repoint::fork_seq`]: from the full
/// epoch histories; for a slave ahead of its master on the same history, the master's
/// `last_seq + 1`; otherwise the last entry, which differs). `None` (recorded as unknown)
/// without a local identity.
pub(crate) async fn record_fork(inner: &Arc<Inner>, hs: &pb::HandshakeResponse) -> Option<Seq> {
    let last = inner.store.position().last_seq;
    let fork = inner
        .ident()
        .map(|(_, history)| super::repoint::fork_seq(&history, last, hs));
    if let Err(e) = tracked(inner, move |i| i.store.set_diverged_at(fork)).await {
        tracing::warn!(node = %inner.cfg.identifier, error = %e, "could not record the fork point");
    }
    fork
}

/// Everything a repair commits, prepared by the network phase.
pub(crate) struct Prepared {
    rewind: Rewind,
    ready_at: Seq,
    /// For a verify repair: the version of the queued request it repairs, taken once
    /// committed.
    pending: Option<u64>,
}

/// Opens a session and checks it follows the identity the handshake announced.
async fn open(
    inner: &Inner,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<UpstreamSession, RepairError> {
    let session = UpstreamSession::open(inner, client, VerifyMode::Quick, OPEN_TIMEOUT).await?;
    if session.begin.identity != hs.identity {
        session.close().await;
        return Err(RepairError::Retry(
            "the upstream's identity changed since the handshake".into(),
        ));
    }
    Ok(session)
}

/// The network phase of a divergence repair against the upstream that answered `hs`
/// (`REJECT / DIVERGED`). Writes nothing.
pub(crate) async fn prepare_divergence(
    inner: &Arc<Inner>,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<Prepared, RepairError> {
    if let Some(until) = inner.repairing() {
        // The keys the earlier repair took (as of its `S`) are not recorded: a second rewind
        // could not undo them.
        return Err(fallback(format!(
            "an earlier repair has not finished replaying (up to {until})"
        )));
    }
    let last = inner.store.position().last_seq;
    let upstream = epochs_from_pb(&hs.epoch_history);
    let Some(fork) = history_fork(&inner.known_epochs(), last, &upstream) else {
        return Err(fallback(
            "the epoch histories do not show where the logs part",
        ));
    };
    if fork < hs.first_retained_seq {
        return Err(fallback(format!(
            "the upstream no longer holds entry {fork}"
        )));
    }
    // Keeps the local suffix (and entry `fork - 1`) from being trimmed while this runs.
    let _keep = inner.store.guard(fork.saturating_sub(2));
    let max = usize::try_from(inner.cfg.repair_max_bytes).unwrap_or(usize::MAX);
    let suffix = tracked(inner, move |i| i.store.read_entries(fork, usize::MAX, max))
        .await
        .map_err(|e| match e {
            Error::Gap { .. } => fallback(format!(
                "the local suffix from {fork} is no longer retained"
            )),
            other => RepairError::Retry(other.to_string()),
        })?;
    if suffix.first().map(|e| e.seq) != Some(fork) || suffix.last().map(|e| e.seq) != Some(last) {
        return Err(fallback(format!(
            "the divergent suffix {fork}..={last} is larger than repair_max_bytes"
        )));
    }
    let set = AffectedSet::from_entries(&suffix)
        .map_err(|e| fallback(format!("an entry of the suffix does not decode: {e}")))?
        .ok_or_else(|| fallback("the divergent suffix creates or drops a column family"))?;
    drop(suffix);
    let mut session = open(inner, client, hs).await?;
    let result = async {
        let s = session.begin.snapshot_seq;
        divergence_target(fork, s)?;
        // Second line of defence: entry F - 1 must not differ where both sides hold it.
        // (`entry_crc` never asks for a seq past `S`.)
        let keep = fork - 1;
        let mine = match keep {
            0 => None,
            k => tracked(inner, move |i| i.store.entry_crc(k)).await?,
        };
        let theirs = session.upstream.entry_crc(keep).await?;
        if let (Some(a), Some(b)) = (mine, theirs)
            && a != b
        {
            return Err(fallback(format!(
                "entry {keep} differs although the histories agree"
            )));
        }
        prepare(inner, hs, &mut session, keep, mine.or(theirs), last, s, set).await
    }
    .await;
    session.close().await;
    result.map(|rewind| Prepared {
        ready_at: rewind.repair_until.unwrap_or(fork - 1),
        rewind,
        pending: None,
    })
}

/// The network phase of repairing the keys a verify reported (`MismatchPolicy::Repair`),
/// taken from the upstream of `generation` that answered `hs` (`RESUME`). Writes nothing. The
/// request stays queued until the commit, so an interrupted repair is retried.
pub(crate) async fn prepare_pending(
    inner: &Arc<Inner>,
    generation: u64,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<Prepared, RepairError> {
    let db_uuid = hs
        .identity
        .as_ref()
        .and_then(|i| identity_from_pb(i).ok())
        .map(|i| i.db_uuid);
    let Some((version, set)) = inner.pending_repair(generation, db_uuid) else {
        return Err(RepairError::Retry("nothing to repair".into()));
    };
    let mut session = open(inner, client, hs).await?;
    let result = async {
        let s = session.begin.snapshot_seq;
        let pos = inner.store.position();
        let target = pending_target(pos.last_seq, s, inner.repairing())?;
        let keep_crc = match pos.last_seq {
            0 => None,
            k => tracked(inner, move |i| i.store.entry_crc(k)).await?,
        };
        prepare(
            inner,
            hs,
            &mut session,
            pos.last_seq,
            keep_crc,
            pos.last_seq,
            target,
            set,
        )
        .await
    }
    .await;
    session.close().await;
    result.map(|rewind| Prepared {
        ready_at: rewind.repair_until.unwrap_or(rewind.keep_through),
        rewind,
        pending: Some(version),
    })
}

/// Whether the queued verify repair could run against the upstream that answered `hs` now:
/// opens a verify session as the repair would (`VerifyBegin`, the identity check, an upstream
/// not behind this slave) and closes it again. The driver runs it while it keeps tailing, so a
/// repair the upstream cannot serve now (BUSY, a timeout, an upstream behind) or ever (a
/// refusal) never ends the tail stream. Writes nothing.
pub(crate) async fn probe(
    inner: &Arc<Inner>,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<(), RepairError> {
    let session = open(inner, client, hs).await?;
    let s = session.begin.snapshot_seq;
    let ready = pending_target(inner.store.position().last_seq, s, inner.repairing()).map(|_| ());
    session.close().await;
    ready
}

/// A divergence repair rewinds to `fork - 1` and replays the upstream's entries from `fork`
/// up to its snapshot `s`: the upstream must hold `fork - 1` (else retried later).
fn divergence_target(fork: Seq, s: Seq) -> std::result::Result<Seq, RepairError> {
    if s + 1 < fork {
        return Err(RepairError::Retry(format!(
            "the upstream's snapshot {s} is behind the fork point {fork}"
        )));
    }
    Ok(s)
}

/// The seq a verify repair of a slave at `last` makes it ready at, against a snapshot `s`:
/// `s`, and never before an unfinished earlier repair's target (same history: the handshake
/// answered RESUME). An upstream behind the slave (`s < last`) is retried later: the keys
/// would be set to an older state than the entries already applied, and rewinding instead
/// would drop entries the verify never reported (DESIGN.md §12.1).
fn pending_target(
    last: Seq,
    s: Seq,
    repairing: Option<Seq>,
) -> std::result::Result<Seq, RepairError> {
    if last > s {
        return Err(RepairError::Retry(format!(
            "the upstream (at {s}) is behind this slave (at {last}); repairing later"
        )));
    }
    Ok(s.max(repairing.unwrap_or(0)))
}

/// The identity and epoch history the data at the session's `S` belongs to, read by the
/// upstream through the session's snapshot: what the rewind stores. The handshake that started
/// the repair (and its consent) saw the same history, or the repair falls back: the upstream
/// moved to another history in between (the identity alone, without nonces, cannot show it).
fn snapshot_history(
    hs: &pb::HandshakeResponse,
    begin: &pb::VerifyBeginResponse,
) -> std::result::Result<(DbIdentity, Vec<EpochStart>), RepairError> {
    let invalid = |why: String| RepairError::Refused(format!("the upstream sent {why}"));
    let identity = begin
        .identity
        .as_ref()
        .ok_or_else(|| invalid("no identity in VerifyBegin".into()))?;
    let identity: DbIdentity =
        identity_from_pb(identity).map_err(|e| invalid(format!("an invalid identity: {e}")))?;
    if begin.identity != hs.identity || begin.epoch_history != hs.epoch_history {
        return Err(fallback(
            "the upstream's epoch history at the snapshot differs from its handshake",
        ));
    }
    let history = checked_history(&identity, &begin.epoch_history)
        .map_err(|e| invalid(format!("an invalid epoch history: {e}")))?;
    Ok((identity, history))
}

/// Collects fetched records of one CF: checks every envelope and counts the byte budget,
/// stopping the fetch on a problem.
struct Fetched {
    cf: Arc<PhysCf>,
    bytes: u64,
    max_bytes: u64,
    problem: Option<RepairError>,
    /// Length of the last key received (a range page cannot resume after a key over 2 MiB).
    last_key_len: usize,
    writes: Vec<KeyWrite>,
}

impl Fetched {
    fn take(&mut self, key: Vec<u8>, env: Option<Vec<u8>>) -> bool {
        if let Some(v) = &env
            && let Err(e) = envelope::decode(v)
        {
            self.problem = Some(fallback(format!("the upstream sent a bad envelope: {e}")));
            return false;
        }
        self.bytes += (key.len() + env.as_ref().map_or(0, Vec::len)) as u64;
        if self.bytes > self.max_bytes {
            self.problem = Some(fallback(format!(
                "more than repair_max_bytes ({}) to fetch",
                self.max_bytes
            )));
            return false;
        }
        self.last_key_len = key.len();
        self.writes.push((self.cf.clone(), key, env));
        true
    }

    fn stopped(&mut self) -> RepairError {
        self.problem
            .take()
            .unwrap_or_else(|| fallback("the fetch stopped"))
    }
}

/// Fetches what `set` needs at the session's snapshot `s` and builds the rewind to
/// `keep_through` (dropping local entries after it, up to `last`), ready at `ready_at`, within
/// `repair_max_bytes`.
#[allow(clippy::too_many_arguments)]
async fn prepare(
    inner: &Arc<Inner>,
    hs: &pb::HandshakeResponse,
    session: &mut UpstreamSession,
    keep_through: Seq,
    keep_crc: Option<u32>,
    last: Seq,
    ready_at: Seq,
    set: AffectedSet,
) -> std::result::Result<Rewind, RepairError> {
    let (identity, history) = snapshot_history(hs, &session.begin)?;
    let upstream_cfs: Vec<CfEntry> = session.begin.cf_specs.iter().map(cf_from_pb).collect();
    // Test hook: the session pins `S`; nothing is fetched yet.
    failpoint::pause("slave-repair-before-fetch").await;
    let max_bytes = inner.cfg.repair_max_bytes;
    let mut clear = Vec::new();
    let mut writes: Vec<KeyWrite> = Vec::new();
    let mut fetched = 0u64;
    for cf in set.cfs() {
        let local = inner
            .store
            .live_cf(cf)
            .map_err(|_| fallback(format!("CF {cf} does not exist locally")))?;
        if !upstream_cfs.contains(&local.entry) {
            return Err(fallback(format!(
                "CF {cf} differs between this slave and its upstream"
            )));
        }
        let order = KeyOrder::new(
            &inner.store.tuning().comparators,
            local.entry.spec.comparator.as_deref(),
        )?;
        let keys: Vec<Vec<u8>> = set.keys.get(cf).into_iter().flatten().cloned().collect();
        if keys.iter().any(|k| k.len() > FETCH_REQUEST_BYTES) {
            return Err(fallback("a key is too large to fetch"));
        }
        let ranges = set.ranges.get(cf).cloned().unwrap_or_default();
        let too_long =
            |b: &Option<Vec<u8>>| b.as_ref().is_some_and(|k| k.len() > MAX_RESUME_KEY_BYTES);
        if ranges
            .iter()
            .any(|b| too_long(&b.lower) || too_long(&b.upper))
        {
            return Err(fallback("a range bound is too large to fetch"));
        }
        let mut acc = Fetched {
            cf: local.clone(),
            bytes: fetched,
            max_bytes,
            problem: None,
            last_key_len: 0,
            writes: Vec::new(),
        };
        if !keys.is_empty()
            && !session
                .upstream
                .fetch_keys(cf, &keys, &mut |k, e| acc.take(k, e))
                .await?
        {
            return Err(acc.stopped());
        }
        for bounds in &ranges {
            acc.last_key_len = 0;
            match session
                .upstream
                .fetch_range(cf, bounds, &order, &mut |k, e| acc.take(k, e))
                .await
            {
                Ok(true) => {}
                Ok(false) => return Err(acc.stopped()),
                // A page cut right after a key the listing cannot resume from: this repeats.
                Err(_) if acc.last_key_len > MAX_RESUME_KEY_BYTES => {
                    return Err(fallback("a key in a range is too large to page past"));
                }
                Err(e) => return Err(e.into()),
            }
            clear.push((local.clone(), bounds.clone()));
        }
        fetched = acc.bytes;
        writes.extend(acc.writes);
    }
    // Local keys in the cleared ranges are deleted in the same batch: they count too.
    let to_clear = clear.clone();
    let room = max_bytes.saturating_sub(fetched);
    if tracked(inner, move |i| i.store.range_bytes(&to_clear, room))
        .await?
        .is_none()
    {
        return Err(fallback(format!(
            "more than repair_max_bytes ({max_bytes}) to delete"
        )));
    }
    Ok(Rewind {
        keep_through,
        keep_crc: if keep_through == 0 { None } else { keep_crc },
        drop_through: last,
        identity,
        epoch_history: history,
        repair_until: (ready_at > keep_through).then_some(ready_at),
        clear,
        writes,
    })
}

/// The local phase: commits a prepared repair in one synced batch. Run it to completion (it is
/// not cancelled by a repoint or a timeout): the in-memory state follows the batch, on the
/// same blocking thread, so it does so even if the caller is dropped meanwhile.
pub(crate) async fn commit(
    inner: &Arc<Inner>,
    prepared: Prepared,
) -> std::result::Result<Repaired, RepairError> {
    let Prepared {
        rewind,
        ready_at,
        pending,
    } = prepared;
    let (keep_through, last) = (rewind.keep_through, rewind.drop_through);
    // Readers and downstream replicas must not see the mixed state: refused from before the
    // batch is visible. A verify waiting for a seq of the old state can never be served now.
    let before = inner.repairing().unwrap_or(0);
    if let Some(until) = rewind.repair_until {
        inner.begin_repair(until);
    }
    inner.reset_downstream();
    inner.cancel_capture();
    let committed = tracked(inner, move |i| {
        i.store.rewind(&rewind)?;
        i.set_ident(Some((rewind.identity, rewind.epoch_history.clone())));
        // The divergent suffix said nothing about the origin's position.
        let origin = i.upstream_last_seq.load(Ordering::SeqCst).max(ready_at);
        i.origin_last_seq.store(origin, Ordering::SeqCst);
        i.last_applied_ts.store(0, Ordering::SeqCst);
        if let Some(version) = pending {
            i.take_pending_repair(version);
        }
        i.reset_downstream();
        Ok(())
    })
    .await;
    if let Err(e) = committed {
        // The batch is atomic and the position is checked first: nothing was written, the old
        // state (and an earlier repair's target) stands.
        inner.begin_repair(before);
        return Err(e.into());
    }
    tracing::info!(
        node = %inner.cfg.identifier,
        rewound_to = keep_through,
        discarded = last - keep_through,
        ready_at,
        "repaired from the upstream"
    );
    // The listener hears of the rewind before the rewound seq is published (its contract).
    inner.notify(|l| l.on_rewind(keep_through));
    inner.seq_tx.send_replace(keep_through);
    if failpoint::fire_async("slave-repair-after-rewind").await {
        // Emulated crash right after the rewind: freeze until the test kills the node.
        std::future::pending::<()>().await;
    }
    Ok(Repaired {
        discarded: keep_through + 1..=last,
        rewound_to: keep_through,
        ready_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_ops;
    use crate::types::CfSpec;

    fn rec(epoch: u64, start_seq: Seq, nonce: u64) -> EpochStart {
        EpochStart {
            epoch,
            start_seq,
            nonce,
        }
    }

    /// Final review A-M4: repeated or overlapping DeleteRanges with identical bounds are
    /// fetched and counted once, also across a merge.
    #[test]
    fn identical_ranges_are_deduplicated() {
        let del = |seq, s: &[u8], e: &[u8]| {
            entry(
                seq,
                vec![Op::DeleteRange {
                    cf: "c".into(),
                    start: s.to_vec(),
                    end: e.to_vec(),
                }],
            )
        };
        let b = |s: &[u8], e: &[u8]| Bounds {
            lower: Some(s.to_vec()),
            upper: Some(e.to_vec()),
        };
        let mut set = AffectedSet::from_entries(&[
            del(1, b"m", b"p"),
            del(2, b"a", b"c"),
            del(3, b"m", b"p"),
        ])
        .unwrap()
        .unwrap();
        assert_eq!(set.ranges["c"], vec![b(b"a", b"c"), b(b"m", b"p")]);
        let other = AffectedSet::from_entries(&[del(4, b"a", b"c"), del(5, b"x", b"y")])
            .unwrap()
            .unwrap();
        set.merge(other);
        assert_eq!(
            set.ranges["c"],
            vec![b(b"a", b"c"), b(b"m", b"p"), b(b"x", b"y")]
        );
    }

    /// Final fix wave, I1 / A-M6: failures retrying cannot fix are told apart from transient
    /// ones, so a verify repair is dropped instead of retried forever.
    #[test]
    fn refusals_and_unregistered_comparators_are_permanent() {
        let refused = |e: Error| matches!(RepairError::from(e), RepairError::Refused(_));
        assert!(refused(Error::VerifyAborted(VerifyAbort::Refused(
            "PermissionDenied".into()
        ))));
        assert!(refused(Error::UnknownComparator("rev".into())));
        assert!(!refused(Error::VerifyAborted(VerifyAbort::Busy)));
        assert!(!refused(Error::VerifyAborted(VerifyAbort::Upstream(
            "no answer".into()
        ))));
        assert!(!refused(Error::Timeout));
    }

    #[test]
    fn the_fork_comes_from_the_epoch_histories() {
        let upstream = vec![rec(1, 1, 1), rec(2, 10, 2)];
        // Still in epoch 1 with entries from 10 on: those are not the upstream's.
        assert_eq!(history_fork(&[rec(1, 1, 1)], 12, &upstream), Some(10));
        // Behind the fork: nothing diverged.
        assert_eq!(history_fork(&[rec(1, 1, 1)], 9, &upstream), None);
        // Epoch 2 with another nonce (a restored backup started it again): from its start.
        assert_eq!(
            history_fork(&[rec(1, 1, 1), rec(2, 10, 7)], 15, &upstream),
            Some(10)
        );
        // Another epoch 2 that starts later: from the lower start.
        assert_eq!(
            history_fork(&[rec(1, 1, 1), rec(2, 12, 7)], 15, &upstream),
            Some(10)
        );
        assert_eq!(history_fork(&upstream, 15, &upstream), None);
        // A repair target `S` on a history the upstream has since lost below it.
        let lost = vec![rec(1, 1, 1), rec(2, 10, 2), rec(3, 20, 3)];
        let now = vec![rec(1, 1, 1), rec(2, 10, 2), rec(3, 15, 9)];
        assert_eq!(history_fork(&lost, 25, &now), Some(15));
        assert_eq!(history_fork(&lost, 14, &now), None);
    }

    /// Fix round 1, A-M2: the rewind stores the history read through the session's snapshot,
    /// and a history that differs from the handshake's (same identity, another nonce) falls
    /// back instead of being mixed with the handshake's.
    #[test]
    fn the_stored_history_is_the_snapshots_and_must_match_the_handshake() {
        use crate::net::convert::{epochs_to_pb, identity_to_pb};
        let id = DbIdentity {
            db_uuid: uuid::Uuid::from_u128(7),
            epoch: 2,
        };
        let h = vec![rec(1, 1, 1), rec(2, 10, 2)];
        let hs = pb::HandshakeResponse {
            identity: Some(identity_to_pb(&id)),
            epoch_history: epochs_to_pb(&h),
            ..Default::default()
        };
        let begin = |history: &[EpochStart]| pb::VerifyBeginResponse {
            identity: Some(identity_to_pb(&id)),
            epoch_history: epochs_to_pb(history),
            ..Default::default()
        };
        assert_eq!(snapshot_history(&hs, &begin(&h)).unwrap(), (id, h.clone()));
        let other = vec![rec(1, 1, 1), rec(2, 10, 99)];
        assert!(matches!(
            snapshot_history(&hs, &begin(&other)),
            Err(RepairError::Fallback(_))
        ));
        assert!(matches!(
            snapshot_history(&hs, &begin(&[])),
            Err(RepairError::Fallback(_))
        ));
    }

    /// Fix round 2: a fallback installs only a history that leaves this slave's no earlier
    /// than the consented handshake's did.
    #[test]
    fn a_fallback_never_installs_a_history_that_forks_deeper_than_consented() {
        let local = vec![rec(1, 1, 1), rec(2, 10, 2)];
        let same = local.clone();
        let later = vec![rec(1, 1, 1), rec(2, 10, 2), rec(3, 25, 3)];
        let forked_at_20 = vec![rec(1, 1, 1), rec(2, 10, 2), rec(3, 20, 3)];
        let forked_at_10 = vec![rec(1, 1, 1), rec(2, 10, 9)];
        let forked_at_15 = vec![rec(1, 1, 1), rec(2, 10, 2), rec(3, 15, 4)];
        // Verify repair at last_seq 22 (RESUME: the consented history shows no fork <= 22).
        assert_eq!(deeper_fork(&local, 22, &same, &same), None);
        assert_eq!(
            deeper_fork(&local, 22, &same, &later),
            None,
            "a fork after last_seq"
        );
        assert_eq!(deeper_fork(&local, 22, &same, &forked_at_20), Some(20));
        assert_eq!(deeper_fork(&local, 22, &same, &forked_at_10), Some(10));
        // Divergence repair consented at fork 20 (the slave holds up to 22).
        assert_eq!(deeper_fork(&local, 22, &forked_at_20, &forked_at_20), None);
        assert_eq!(deeper_fork(&local, 22, &forked_at_20, &later), None);
        assert_eq!(
            deeper_fork(&local, 22, &forked_at_20, &forked_at_15),
            Some(15)
        );
        assert_eq!(
            deeper_fork(&local, 22, &forked_at_20, &forked_at_10),
            Some(10)
        );
    }

    #[test]
    fn a_repair_is_ready_at_the_snapshot_and_never_before_an_earlier_target() {
        let retry =
            |r: std::result::Result<Seq, RepairError>| matches!(r, Err(RepairError::Retry(_)));
        // Verify repair at last_seq 10.
        assert_eq!(pending_target(10, 10, None).unwrap(), 10);
        assert_eq!(pending_target(10, 15, None).unwrap(), 15);
        assert_eq!(pending_target(10, 15, Some(20)).unwrap(), 20);
        assert!(
            retry(pending_target(10, 9, None)),
            "upstream behind the slave"
        );
        assert!(retry(pending_target(10, 9, Some(20))));
        // Divergence repair with fork 10: the upstream must hold entry 9.
        assert_eq!(divergence_target(10, 9).unwrap(), 9);
        assert_eq!(divergence_target(10, 30).unwrap(), 30);
        assert!(retry(divergence_target(10, 8)));
    }

    fn entry(seq: Seq, ops: Vec<Op>) -> LogEntry {
        LogEntry::new(seq, 1, 1_000, encode_ops(&ops))
    }

    #[test]
    fn the_affected_set_of_a_suffix_covers_every_key_and_range() {
        let put = |cf: &str, k: &[u8]| Op::Put {
            cf: cf.into(),
            key: k.to_vec(),
            value: b"v".to_vec(),
            expire_at_ms: None,
        };
        let suffix = vec![
            entry(7, vec![put("a", b"k1"), put("b", b"k2")]),
            entry(
                8,
                vec![
                    Op::Delete {
                        cf: "a".into(),
                        key: b"k3".to_vec(),
                    },
                    put("a", b"k1"),
                ],
            ),
            entry(
                9,
                vec![Op::DeleteRange {
                    cf: "b".into(),
                    start: b"m".to_vec(),
                    end: b"p".to_vec(),
                }],
            ),
        ];
        let set = AffectedSet::from_entries(&suffix).unwrap().unwrap();
        assert_eq!(
            set.keys["a"],
            [b"k1".to_vec(), b"k3".to_vec()].into_iter().collect()
        );
        assert_eq!(set.keys["b"], [b"k2".to_vec()].into_iter().collect());
        assert_eq!(
            set.ranges["b"],
            vec![Bounds {
                lower: Some(b"m".to_vec()),
                upper: Some(b"p".to_vec())
            }]
        );
        for op in [
            Op::CreateCf {
                spec: CfSpec::new("n"),
            },
            Op::DropCf { name: "a".into() },
        ] {
            let with_cf_op = vec![entry(7, vec![put("a", b"k")]), entry(8, vec![op])];
            assert_eq!(AffectedSet::from_entries(&with_cf_op).unwrap(), None);
        }
    }

    #[test]
    fn verify_mismatches_become_keys_and_ranges_but_cf_differences_do_not() {
        let ms = vec![
            Mismatch::Key {
                cf: "a".into(),
                key: b"k".to_vec(),
                local: Some(3),
                upstream: None,
            },
            Mismatch::Range {
                cf: "a".into(),
                lower: None,
                upper: Some(b"z".to_vec()),
            },
        ];
        let mut set = AffectedSet::from_mismatches(&ms).unwrap();
        assert_eq!(set.keys["a"].len(), 1);
        assert_eq!(
            set.ranges["a"],
            vec![Bounds {
                lower: None,
                upper: Some(b"z".to_vec())
            }]
        );
        set.merge(
            AffectedSet::from_mismatches(&[Mismatch::Key {
                cf: "b".into(),
                key: b"x".to_vec(),
                local: None,
                upstream: Some(1),
            }])
            .unwrap(),
        );
        assert_eq!(set.cfs().len(), 2);
        let cf = Mismatch::CfSpec {
            name: "a".into(),
            local: None,
            upstream: None,
        };
        assert_eq!(AffectedSet::from_mismatches(&[cf]), None);
    }
}
