# Plan 3 (part 2: Tasks 6–11)

> **Split note:** Part 2 of Plan 3 (Tasks 6–11). Tasks 0–5 are in [2026-10-09-plan3-verify.md](2026-10-09-plan3-verify.md); their code is assumed implemented. The header below is copied verbatim from part 1.

# Plan 3 of 3 — Integrity Verification, Repair, Observability and the Full Test Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Verify end to end — server sessions pinned to a snapshot with `HashRange`/`ListRange`, and a slave verifier that captures its own snapshot at exactly the upstream's seq, compares contiguous ranges, bisects and lists mismatches down to the key, and reports, repairs or re-bootstraps — plus divergence repair (addendum §5), `lag_ms`, tracing spans, and a sweep that gives every DESIGN.md §21 test item a real test. First, Task 0 fixes four findings carried over from Plan 2's final review, Task 1 gives every epoch a random nonce and starts a new epoch on every master open (addendum §7, so histories are compared exactly), and Task 2 adds the API helpers the examples asked for (addendum §8).

**Architecture:** A new crate-private `src/verify/` module holds everything both sides share: key order, half-open bounds, per-mode payloads, range hashes, listings, the chunker, the throttle (`range.rs`), range reads through a `ReadSnapshot` (`scan.rs`), and the comparison algorithm (`compare.rs`) written against two small traits (`Oracle` = the upstream, `LocalSide` = local records) so bisection, paging and the key-by-key diff are unit-tested without a network. The server (`src/server/verify.rs`) keeps sessions that own a `ReadSnapshot`; the slave (`src/slave/verifier.rs`) implements `Oracle` over the `VerifyRange` bidi stream and gets its snapshot from the applier right after it commits seq `S`; it pauses the applier before asking for the session, so it cannot run past `S` meanwhile. Repair reuses the session: three more request kinds (`FetchKeys`/`FetchRange` to read envelopes at `S`, `EntryCrcs` to check the entry before a fork) let a diverged slave — or one a verify found damaged — rewind to a common seq in one synced batch with only the affected keys taken from the upstream, then replay (`src/slave/repair.rs`, `src/store/repair.rs`); the fork point itself comes from the epoch histories, which carry nonces from Task 1. Observability adds `lag_ms` from the last applied entry's `ts_ms` and `tracing` spans on both ends of every handshake, bootstrap and verify session and on each reaper round. Every integrity test is real: nodes in one process over gRPC on 127.0.0.1, corruption injected by opening a stopped slave's directory with plain RocksDB.

**Tech Stack:** Plan 1 and Plan 2's stack (Rust 1.99 toolchain, edition 2024, MSRV 1.85, rocksdb 0.25.0 with `multi-threaded-cf`, prost 0.14.4, tokio 1.53, tonic 0.14.6, tonic-types, tokio-stream 0.1.19, blake3 1.8, tracing 0.1.44, …). No new dependencies. Tests use `rocksdb` (a normal dependency) to tamper with stopped node directories, and implement a tiny `tracing::Subscriber` to capture spans.

**Spec:** [DESIGN.md](../../../DESIGN.md) §5.2, §12, §13, §15, §16, §18, §19, §21; [PROTOCOL.md](../../../PROTOCOL.md) §5.4, §5.5, §6, §9, §10, §11.2, §12, §13, §14, §15; the "Plan 3: Verify" section of [the Plan 2/3 design notes](../specs/2026-10-09-plan2-3-design-notes.md); §5, §7 and §8 (and the §3–§5 tests) of [the acks/failover/repair addendum](../specs/2026-10-09-failover-acks-repair.md), which wins over DESIGN/PROTOCOL until merged (Tasks 1, 2 and 10 merge §7, §8 and §5). Read DESIGN.md §12 and PROTOCOL.md §9 before starting. This plan builds on [Plan 1](2026-10-09-plan1-storage-and-master.md) and [Plan 2](2026-10-09-plan2-replication.md) (with its addendum §1–§4 amendment: acks over a bidirectional Tail, `ReplicaStatus` / `replicas()`, write concern, `repoint`, `Status`, `promote_checked`, `demote`), **both fully implemented: the code at commit `f4ddca1` (branch `feat/storage-and-master`; the code is unchanged since `fae88e4`, and `examples/` came in `57c37b2`) is the source of truth.** Every snippet below that is quoted for replacement is quoted from that tree with this plan's earlier tasks applied, and was applied and tested in that order: each task compiles, passes its tests and leaves `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` (with and without `--features failpoints`) clean. If an edit does not apply verbatim, the tree has moved on since `f4ddca1`: apply the same change to the equivalent lines. Several edits are given as exact before/after excerpts generated from the tested tree; apply them top to bottom within a file.

**Series:**
- Plan 1: storage engine and master (done).
- Plan 2: replication server, auth/TLS, `Slave`, chaining, promotion, failure/resume (done before this plan).
- **Plan 3 (this):** carried review fixes, epoch nonces and per-open epochs, API ergonomics, Verify, repair (divergence and verify mismatches; the `repoint` `Repair` arm), `lag_ms`, `last_verify`, tracing spans, DESIGN.md §21 sweep. (`replicas()` / `ReplicaStatus` and the addendum §3–§4 failover tests — `promote_checked`, `demote`, the chooser, `repoint` refusal — already exist from Plan 2: `tests/promotion.rs`, `tests/repoint.rs`, `src/admin.rs`.)

## Global Constraints

- Everything in Plan 1's and Plan 2's Global Constraints still holds: absolute ops only (`Put`, `Delete`, `DeleteRange`, `CreateCf`, `DropCf`), no merge; mutation + `__changelog[seq]` + `__meta.last_seq` in one `WriteBatch`; `__meta.last_seq` is the source of truth; the master always fsyncs; slaves forward entries unchanged; `LogEntry.ops` is never re-encoded.
- **No read-modify-write API** (no `update`, CAS or merge). Nothing here adds one; the rustdoc of `Master::put`, `Master::write`, `Master::get` and the crate docs keep the DESIGN.md §5.1 warning.
- **Verify reads raw envelopes, never stripped values**, including values that are expired but not yet reaped (PROTOCOL.md §5.3). `Quick` hashes the envelope header (10 bytes, 18 with TTL); `Full` hashes the whole envelope.
- **Range hash** = BLAKE3 over `0x02 ‖ BE64(len(key)) ‖ key ‖ BE64(len(payload)) ‖ payload` per key, in the CF's comparator order, no markers, no trailer; the empty range hashes the empty input (PROTOCOL.md §5.4, vectors §15 V3). Use `codec::digest::Digest::{record, finish_range}`.
- **Ranges are half-open `[lower, upper)` under the CF comparator and contiguous**: the first has no `lower`, each `upper` is the next `lower`, the last has no `upper`. An empty key is a real bound and is never the same as "absent" (`KeyRange.lower/upper` are `optional bytes`).
- **The upstream's `S`, identity and `cf_specs` are read through the same snapshot as the data** (the session's `ReadSnapshot`); the slave's side is a snapshot taken by the applier **right after committing exactly `S`**, before `S + 1` can commit. A slave already past `S` abandons the run (`VerifyAbort::PassedSnapshot`), never reports false mismatches.
- `PROTOCOL_VERSION` checks, authentication (`Method::Verify`) and `ErrorInfo { domain: "rrdb.replication.v1", reason }` apply to all three Verify RPCs. Errors: `RESOURCE_EXHAUSTED/BUSY` (more than `max_verify_sessions`), `NOT_FOUND/SESSION_NOT_FOUND` (unknown or idle longer than `verify_session_timeout`), `UNAVAILABLE/NOT_READY` (server bootstrapping), `UNAVAILABLE/UPSTREAM_RESET` (server state replaced), `UNAVAILABLE/SHUTTING_DOWN`, `INVALID_ARGUMENT` for malformed requests (`ListRange.limit` outside 1..=1024, unknown CF, missing range or kind, unspecified mode).
- At most 16 range requests in flight per session (`VERIFY_WINDOW`); answers come in request order with `request_id` echoed.
- Defaults (DESIGN.md §16, §18 as amended here): `VerifyConfig { interval: None, mode: Quick, chunk_keys: 1 000, chunk_bytes: 4 MiB, min_chunk_keys: 16, max_bytes_per_sec: None, on_mismatch: Report, capture_timeout: 60 s, max_reported_keys: 10 000 }`; server `max_verify_sessions: 4`, `verify_session_timeout: 5 min`.
- **Repair is one synced batch**: affected keys, removal of the local log suffix, `last_seq`/`last_entry_crc`, identity and epoch history, and `node_state = repairing:<S>` commit together; reads answer `NotReady` (and the node serves no replicas) until replay reaches `S`. Anything repair cannot prove or handle falls back to a full bootstrap; it never guesses.
- **Every master open starts a new epoch** with a random nonce (Task 1); ordering uses only the numbers (epoch, then `last_seq`), the nonce only identity. Anything that compares histories (the handshake, `admin::compare`, `check_promotion`, repair) compares records with their nonces; entry CRCs are a second line of defence, never a requirement.
- **One writer per epoch, mutations serialized.** The master commits one entry at a time under its writer lock (each write is one log entry with the next seq; concurrent callers queue, and only acknowledgement waits run outside the lock). Nothing in this plan adds a second writer or a concurrent commit path.
- **RocksDB is never read on an async worker** in new or touched server code: reads go through `tokio::task::spawn_blocking` or `background::blocking` (Task 8 moves the remaining Plan 2 cases).
- A verify session (server) or verify run (slave) must never keep a node's database open after `shutdown()`: sessions are cleared on shutdown and on reset, and background tasks hold `Weak<Inner>` between runs (Plan 2's rule).
- Tests that matter are **real**: nodes in one process, gRPC over `127.0.0.1` port 0, corruption injected by opening a **stopped** node's directory with plain `rocksdb::DB`. Failpoint tests live only in `tests/failpoints.rs` and need `--features failpoints`.
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Epochs with nonces** (Task 1): every master open adds a record, so all existing tests that restart a master now see `EPOCH_ADVANCED`; a restarted old master starts its own epoch (same number as the new master's ⇒ `DIVERGED`, lower ⇒ `STALE_EPOCH`). The handshake checks up to five newest client records, and continuity proven by the history needs no retained entry. (Tests in Task 1; the restore-without-bump and trimmed-peer cases would silently resume or block as "unverifiable" without it.)
2. **Capturing exactly `S`.** The verifier connects first, pauses the applier (`Inner::hold_applier`) *before* `VerifyBegin` (bounded by `min(10 s, capture_timeout)`: a held slave acknowledges nothing), then lets it run to `S` and snapshot right after that commit. Without the hold, a slave following a busy master is past `S` by the time the answer arrives and every run aborts. A slave genuinely ahead of its upstream's snapshot (C ahead of B after B lost unflushed entries, DESIGN.md §6) must still abandon the run with `VerifyAbort::PassedSnapshot`, never compare different seqs and report phantom mismatches. (Tests `verify_pins_the_snapshot_while_concurrent_writes_continue` in Task 5, `a_slave_ahead_of_the_upstream_snapshot_abandons_the_run` in Task 6.)
3. **A halted slave behind `S`** can never reach the snapshot seq: `verify_now` must return `CaptureTimeout` as soon as the slave is halted, not hang for `capture_timeout` (60 s). (Test `a_halted_slave_behind_the_snapshot_gives_up_at_once` in Task 6.)
4. **Very long keys** (the protocol allows keys up to `max_entry_bytes`, but the server accepts 4 MiB requests): keys longer than 1 MiB never become range bounds, a listing whose last key is over 2 MiB is reported as `Mismatch::Range` instead of sending an oversized request, and the server always returns at least two keys per `ListResult` so paging never stalls. (Tests `oversized_keys_never_become_boundaries` and `split_points_avoid_long_keys_and_small_ranges`, `a_listing_that_cannot_progress_is_reported_as_a_range` in Task 3; the 1 MiB edge-case key in `edge_case_data_verifies_and_its_corruption_is_found` in Task 6.)
5. **Open verify sessions must not pin the database**: a client that opens a session and walks away must not make `Master::shutdown` time out, and an epoch bump or re-bootstrap must end verify streams with `UPSTREAM_RESET` and drop their snapshots. (Tests `open_sessions_do_not_keep_the_database_open`, `an_epoch_bump_ends_verify_streams_and_sessions` in Task 4.)
6. **A crash between a repair's rewind and replay reaching `S`** leaves keys at `S` next to keys at `F − 1`: that mixed state must never be read or served, before or after a restart, and the slave must converge without a bootstrap. The target is persisted as its own `__meta.repair_until` key, because a halt during the replay rewrites `node_state`. Repair must fall back to a bootstrap whenever it cannot prove the result (no fork in the histories, a CF op, a trimmed suffix, too many bytes) and must never drop entries a verify did not report (`S < L` is retried). (Tests `crash_mid_repair_then_converge`, the fallbacks in `tests/repair.rs`, Task 10.)

## Design deltas this plan introduces

Each owning task updates DESIGN.md or PROTOCOL.md with the exact text given there.

- **Epoch nonces and per-open epochs** (Task 1, addendum §7; PROTOCOL.md §5.5, §6.1, §6.3, §6.4, §14, §15; DESIGN.md §3.2, §13, §13.1, §16, §21, §22): `EpochStart.nonce` (proto field 3); `HandshakeRequest.known_epochs` (field 8: the client's newest records, at most 5; field 7 `known_epoch_start` reserved); rule 9 `EPOCH_ADVANCED` when every record is in the server's history, 10a (proven CRC mismatch) checked first; `Comparison::Unverifiable` and `PromotionCheck::unverifiable` removed, `PromotionUnverifiable` only for peers that answered `Status` with an error; `bump_epoch_on_open` redundant.
- **API ergonomics** (Task 2, addendum §8; DESIGN.md §5.2, §13.1, §16, §16.2): CF operations ignore the write concern; `Master::wait_in_sync`, `Slave::wait_streaming`, `PromoteRefused::into_parts` / `DemoteRefused::into_parts`, `MasterConfig::from_slave_config`, inherent `last_seq()`; `Error::PeerUnreachable` gains `source`, its `reason` is one line; the examples use them.
- **The verifier holds the applier before `VerifyBegin`** (Task 5, DESIGN.md §12 step 2): the design notes had the applier simply "capture at `S`", which a slave keeping up with a busy master never manages (it is already past `S` when the answer arrives). It connects first and bounds `VerifyBegin` by `min(10 s, capture_timeout)`; every later answer by 60 s. A held verify that a bootstrap or repair overtakes gets `Cancelled`; a report never triggers `on_mismatch` while the slave is halted.
- **`ListRange` paging and size** (Task 4, PROTOCOL.md §9.2, §9.4, §12): `limit` must be 1..=1024; the server may stop early to bound the answer (~1 MiB) but returns at least two keys when the range has two; a client continues a truncated listing with `lower` = the last key received and skips that key when it is repeated. This replaces the design notes' "`lower = last key + \0`", which is only a successor under bytewise order (under a reverse comparator it would re-list or skip keys).
- **Long keys are never bounds** (Task 4, PROTOCOL.md §9.4; DESIGN.md §12): keys over 1 MiB are not used as range boundaries; listing resumes only from keys of at most 2 MiB.
- **Result and failure types** (Task 5, DESIGN.md §12, §16, §16.2): `Mismatch::CfSpec { name, local: Option<CfVersion>, upstream: Option<CfVersion> }` (`CfVersion { spec, create_seq }`, so a `create_seq` difference is visible; the notes had `Option<CfSpec>`); `Mismatch::Range` is also used once `max_reported_keys` is reached; `Error::VerifyAborted(VerifyAbort)` with `Busy`, `SessionExpired`, `IdentityChanged`, `PassedSnapshot`, `CaptureTimeout`, `Upstream`; `VerifyConfig` gains `capture_timeout` and `max_reported_keys`; `VerifyReport::is_clean()`.
- **`replicas()` / `ReplicaStatus` move to Plan 2's acks amendment** (addendum §1 defines them with real acks); the design notes' Plan 3 version (`acked_seq = last sent`) is dropped.
- **Repair** (Tasks 9–10, PROTOCOL.md §6.5, §9.2, §11.2, §13, §14; DESIGN.md §3.2, §12.1 new, §13, §13.1, §15, §16, §16.3, §21, §22): `FetchKeys`, `FetchRange`, `EntryCrcs` request kinds on `VerifyRange`; `DivergencePolicy::Repair`, `MismatchPolicy::Repair`, `SlaveConfig.repair_max_bytes` (64 MiB), `Slave::diverged_entries` (fork point persisted as `__meta.diverged_at` when the slave halts `Diverged`), listener `on_rewind`, `Status.node_state = repairing:<seq>` (from `__meta.repair_until`, not persisted in `node_state`). The fork point comes from the epoch histories (no CRC search); the CRC of entry `F − 1` is checked where both sides hold it. `NotReady` until replay passes `S` rests on `__meta.repair_until`. The network phase is bounded (120 s) and abandoned on a repoint; the commit always completes. Verify repair keeps every local entry and retries when `S < L`. `MismatchPolicy::Repair` documents that verify cannot tell which side is damaged. `repoint(.., RepointDivergence::Repair { accept_loss })` now returns `Repaired { discarded }` (Plan 2 returned `Error::Unsupported`) and `repoint::mode_error` goes away. After a repair the driver handshakes again rather than tailing on the `DIVERGED` answer.
- **`lag_ms`** (Task 7, DESIGN.md §16): `Some(0)` when `applied_seq >= origin_last_seq`; while behind, `now − ts_ms` of the last applied entry; `None` while that `ts_ms` is unknown (after a bootstrap, until the next entry).
- **Span names and fields** (Task 8, DESIGN.md §19): `handshake`, `bootstrap`, `verify`, `reaper`, each with `node_id`, `upstream` (absent on the master), `seq`, `epoch`; server-side spans add `client`, `verify` adds `mode`.
- **Plan 2 review items** (Task 0, Task 8): the Tail junction race, an orphan CF of another name, BUSY peers in `check_promotion` (test only), the `RepointDivergence` rustdoc; RocksDB reads off async workers and `ReplicaRegistry::status` outside its locks. No spec change.
- **`MismatchPolicy::Rebootstrap`** ends the current Tail stream (a `Notify` the tail loop selects on) so the bootstrap starts at once, not at the next disconnect (Task 5). A halted slave stays halted.

## File Structure

```
src/server/tail.rs            junction race (Task 0); validation and junction reads on blocking threads (Task 8)
src/store/apply.rs            orphan CF of another name is dropped before its intent is replaced (Task 0)
src/admin.rs                  BUSY test (Task 0); history-based compare, no Unverifiable (Task 1); one-line
                              unreachable reasons (Task 2)
src/types.rs, src/net/convert.rs, src/store/meta.rs   EpochStart.nonce, fork_from, newest_records (Task 1)
src/server/handshake.rs       known_epochs, rules 8–10 with nonces (Task 1)
src/master/{mod,writer}.rs    a new epoch on every open, open_promoted (Task 1); wait_in_sync, last_seq,
                              CF ops exempt from the write concern, DemoteRefused::into_parts (Task 2)
src/config.rs                 bump_epoch_on_open redundant (Task 1); MasterConfig::from_slave_config (Task 2)
src/slave/promote.rs          history-based check (Task 1); PromoteRefused::into_parts (Task 2)
src/slave/repoint.rs          RepointDivergence docs (Task 0); divergence from records (Task 1); Repair arm,
                              mode_error removed (Task 10)
src/verify/mod.rs             public types (VerifyMode, VerifyConfig, MismatchPolicy, CfVersion, Mismatch,
                              VerifyReport, VerifyAbort), protocol constants (Task 3); MismatchPolicy::Repair (Task 10)
src/verify/range.rs           KeyOrder, Bounds, payload, RangeHasher, Lister, LocalRange, LocalRecord,
                              Chunker, Throttle (Task 3)
src/verify/scan.rs            range reads through a ReadSnapshot (Task 3); fetch_range (Task 9)
src/verify/compare.rs         Oracle / LocalSide traits, compare_cf, compare_specs, Budget, CfRun (Task 3)
src/store/repl.rs             + ReadCtx::iter_raw_from (Task 3); ReadSnapshot::entry_crc (Task 9)
src/store/repair.rs           Rewind, Store::{rewind, range_bytes, repair_until, set_diverged_at, ...} (Task 10)
src/error.rs                  + Error::VerifyAborted (Task 3); PeerUnreachable.source (Task 2)
src/lib.rs                    module + re-exports (Task 3)
src/server/verify.rs          Sessions, VerifyBegin / VerifyRange / VerifyEnd (Task 4); FetchKeys / FetchRange /
                              EntryCrcs through the session snapshot (Task 9)
src/server/mod.rs             ServerState field, Svc delegates, sweep, shutdown (Task 4); spans, blocking reads (Task 8)
src/slave/client.rs           connect_now (Task 5)
src/slave/verifier.rs         verify run, applier hold and capture, Upstream oracle, scheduling (Task 5);
                              UpstreamSession and fetches with sinks (Task 9); MismatchPolicy::Repair (Task 10)
src/slave/repair.rs           AffectedSet, history fork, prepare/commit of both repairs, record_fork (Task 10)
src/slave/inner.rs            known_epochs (Task 1); capture slot, verify state (Task 5); lag (Task 7);
                              repair state, is_ready false while repairing (Task 10)
src/slave/{apply,bootstrap,driver}.rs  capture, hold (Task 5); lag (Task 7); spans (Task 8); repair (Task 10)
src/slave/{policy,source,config,state,mod}.rs  as listed in each task
src/listener.rs               + on_verify_complete (Task 5), on_rewind (Task 10)
proto/replication.proto       EpochStart.nonce, known_epochs (Task 1); FetchKeys, FetchRange, EntryCrcs (Task 9);
                              node_state comment (Task 10)
tests/ergonomics.rs           wait_streaming, from_slave_config, into_parts (Task 2)
tests/verify.rs, tests/integrity.rs, tests/observability.rs, tests/spans.rs   (Tasks 5, 6, 7, 8)
tests/repair.rs               divergence and verify repair, fallbacks, diverged_entries (Task 10)
tests/{promotion,repoint,rebootstrap,failure_resume,master_write,write_concern,failpoints,replication}.rs
                              updated and extended (Tasks 0, 1, 2, 10, 11)
tests/support/mod.rs          RecordingListener::{verifies, rewinds}, RawDb, RawDb::last_seq (Tasks 5, 6, 10)
examples/                     helpers of Task 2 in the existing examples; verify.rs, repair.rs (Task 11)
DESIGN.md, PROTOCOL.md, docs/superpowers/specs/   amendments (Tasks 1, 2, 4, 5, 7, 8, 9, 10)
```

## Tasks

| # | Task | Main output |
|---|---|---|
| 0 | Carried fixes from Plan 2's final review | tail junction, orphan CF, BUSY peers, repoint docs |
| 1 | Epoch nonces and per-open epochs | `EpochStart.nonce`, `known_epochs`, history-based compare |
| 2 | API ergonomics | wait helpers, `into_parts`, `from_slave_config`, CF ops exempt from the write concern, examples |
| 3 | Verify core | `src/verify/` |
| 4 | Server Verify RPCs | `src/server/verify.rs` |
| 5 | Slave verifier | `src/slave/verifier.rs`, `verify_now` |
| 6 | Integrity tests with on-disk damage | `tests/integrity.rs` |
| 7 | `lag_ms` | `SlaveStatus.lag_ms` |
| 8 | Spans and blocking reads | `tests/spans.rs` |
| 9 | Repair fetches on the verify session | `FetchKeys`, `FetchRange`, `EntryCrcs` |
| 10 | Divergence repair and `MismatchPolicy::Repair` | `src/slave/repair.rs`, `tests/repair.rs` |
| 11 | DESIGN.md §21 sweep | coverage map, four new tests, verify and repair examples |


---

### Task 6: Integrity scans with real on-disk damage, chains, policies and limits

**Files:**
- Create: `tests/integrity.rs`
- Modify: `tests/support/mod.rs` (add `RawDb`, `KeyCmp`)

**Interfaces:**
- Consumes: Task 5 `Slave::verify_now`, `SlaveConfig.verify`, `SlaveStatus.last_verify`, `RecordingListener::verifies`; Task 3 public `Mismatch`, `MismatchPolicy`, `VerifyAbort`, `VerifyMode`; Plan 1 `replicated_rocksdb::codec::envelope::encode` (public, doc-hidden), `ComparatorRegistry::register`; Plan 2 harness `Cluster::{new, master, master_config, slave_config, path}`, `assert_converged`, `wait_state`, `wait_until`, `write_edge_cases`, `edge_cases`, `copy_dir`, `restore_dir`, `RecordingListener::{bootstraps, assert_no_violations}`, `Slave::{shutdown, serve_addr, last_seq}`, `ServeLimits::{verify_session_timeout, max_verify_sessions}`; the `rocksdb` crate (a normal dependency, so integration tests can use it).
- Produces: harness `RawDb { open(&Path), open_with(&Path, &[(&str /* logical CF */, &str /* comparator name */, KeyCmp)]), get(cf, key) -> Option<Vec<u8>>, put(cf, key, envelope), delete(cf, key), corrupt_value(cf, key) }`, `type KeyCmp = fn(&[u8], &[u8]) -> std::cmp::Ordering`.

This task adds no production code: it proves Tasks 3–5 against real damage. If a test fails, fix the code it points at (`src/verify/compare.rs` for wrong or missing mismatches, `src/slave/verifier.rs` for aborts and policy, `src/server/verify.rs` for limits).

- [ ] **Step 1: Add the raw-access helper to the harness**

Append to `tests/support/mod.rs`:

```rust
/// A **stopped** node's directory opened with plain RocksDB, bypassing the crate, to inject
/// damage replication itself never causes. `comparators` lists `(logical CF, comparator
/// name, ordering)` for each CF created with a custom comparator (RocksDB refuses to open a
/// CF with another comparator name).
pub struct RawDb {
    db: rocksdb::DB,
    names: Vec<String>,
}

/// A key ordering for [`RawDb::open_with`].
pub type KeyCmp = fn(&[u8], &[u8]) -> std::cmp::Ordering;

impl RawDb {
    pub fn open(path: &Path) -> RawDb {
        Self::open_with(path, &[])
    }

    pub fn open_with(path: &Path, comparators: &[(&str, &str, KeyCmp)]) -> RawDb {
        let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), path).unwrap();
        let descriptors: Vec<rocksdb::ColumnFamilyDescriptor> = names
            .iter()
            .map(|n| {
                let mut opts = rocksdb::Options::default();
                for &(cf, name, f) in comparators {
                    if n.starts_with(&format!("u:{cf}:")) {
                        opts.set_comparator(name, Box::new(f));
                    }
                }
                rocksdb::ColumnFamilyDescriptor::new(n, opts)
            })
            .collect();
        let db = rocksdb::DB::open_cf_descriptors(&rocksdb::Options::default(), path, descriptors)
            .unwrap();
        RawDb { db, names }
    }

    /// The physical CF of logical CF `name` (`u:{name}:{create_seq}`); there must be one.
    fn cf(&self, name: &str) -> Arc<rocksdb::BoundColumnFamily<'_>> {
        let prefix = format!("u:{name}:");
        let mut found = self.names.iter().filter(|n| n.starts_with(&prefix));
        let physical = found
            .next()
            .unwrap_or_else(|| panic!("no physical CF for {name}"));
        assert!(found.next().is_none(), "{name} has several physical CFs");
        self.db.cf_handle(physical).unwrap()
    }

    /// The stored envelope.
    pub fn get(&self, cf: &str, key: &[u8]) -> Option<Vec<u8>> {
        self.db.get_cf(&self.cf(cf), key).unwrap()
    }

    pub fn put(&self, cf: &str, key: &[u8], envelope: &[u8]) {
        self.db.put_cf(&self.cf(cf), key, envelope).unwrap();
    }

    pub fn delete(&self, cf: &str, key: &[u8]) {
        self.db.delete_cf(&self.cf(cf), key).unwrap();
    }

    /// Flips the last byte of a stored value, keeping its envelope header (and `write_seq`):
    /// the damage only a `Full` verify can see.
    pub fn corrupt_value(&self, cf: &str, key: &[u8]) {
        let mut env = self.get(cf, key).expect("the key to corrupt exists");
        let header = replicated_rocksdb::codec::envelope::decode(&env)
            .unwrap()
            .header_len;
        assert!(env.len() > header, "the value must be non-empty");
        let last = env.len() - 1;
        env[last] ^= 0xFF;
        self.put(cf, key, &env);
    }
}
```

(`RawDb` must only open a **stopped** node's directory: RocksDB holds a lock on an open one. `Slave::shutdown` returns once the database is closed.)

- [ ] **Step 2: Write the integrity tests**

Create `tests/integrity.rs`:

```rust
//! Integrity scans, end to end: damage is injected into a stopped slave's files with plain
//! RocksDB, then the slave verifies against its upstream over real gRPC.

mod support;

use std::cmp::Ordering;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use replicated_rocksdb::codec::envelope;
use replicated_rocksdb::{
    CfSpec, Error, HaltReason, Master, Mismatch, MismatchPolicy, ReadApi, Slave, SlaveConfig,
    SlaveState, VerifyAbort, VerifyMode,
};
use support::{
    Cluster, RawDb, RecordingListener, assert_converged, copy_dir, edge_cases, restore_dir,
    wait_state, wait_until, write_edge_cases,
};

const KEYS: u32 = 200;

fn key(i: u32) -> Vec<u8> {
    format!("k{i:03}").into_bytes()
}

fn fill(m: &Master) {
    m.create_cf(CfSpec::new("data")).unwrap();
    for i in 0..KEYS {
        m.put("data", &key(i), format!("value-{i}").as_bytes())
            .unwrap();
    }
}

async fn primary(c: &Cluster) -> Master {
    let m = c.master("a").await;
    fill(&m);
    m
}

/// A replica with small ranges (16 keys, listed at 2), so a mismatch is narrowed through
/// several bisection levels.
async fn replica(
    c: &Cluster,
    name: &str,
    upstream: SocketAddr,
    tweak: impl FnOnce(&mut SlaveConfig),
) -> (Slave, SlaveConfig, Arc<RecordingListener>) {
    let (mut cfg, listener) = c.slave_config(name, upstream);
    cfg.verify.chunk_keys = 16;
    cfg.verify.min_chunk_keys = 2;
    tweak(&mut cfg);
    (Slave::open(cfg.clone()).await.unwrap(), cfg, listener)
}

/// Stops `s`, changes its files with plain RocksDB, and opens it again.
async fn tamper(s: Slave, cfg: &SlaveConfig, f: impl FnOnce(&RawDb)) -> Slave {
    tamper_with(s, cfg, &[], f).await
}

async fn tamper_with(
    s: Slave,
    cfg: &SlaveConfig,
    comparators: &[(&str, &str, support::KeyCmp)],
    f: impl FnOnce(&RawDb),
) -> Slave {
    s.shutdown().await.unwrap();
    f(&RawDb::open_with(&cfg.path, comparators));
    Slave::open(cfg.clone()).await.unwrap()
}

fn ws(m: &Master, cf: &str, k: &[u8]) -> u64 {
    m.get_with_meta(cf, k).unwrap().unwrap().1.write_seq
}

fn mismatch(cf: &str, k: &[u8], local: Option<u64>, upstream: Option<u64>) -> Mismatch {
    Mismatch::Key {
        cf: cf.into(),
        key: k.to_vec(),
        local,
        upstream,
    }
}

fn reverse(a: &[u8], b: &[u8]) -> Ordering {
    b.cmp(a)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_corrupted_value_is_found_by_full_verify_only() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b)]).await;
    let b = tamper(b, &cfg, |raw| raw.corrupt_value("data", &key(100))).await;

    let quick = b.verify_now(VerifyMode::Quick).await.unwrap();
    assert!(
        quick.is_clean(),
        "Quick hashes envelope headers only: {:?}",
        quick.mismatches
    );
    let full = b.verify_now(VerifyMode::Full).await.unwrap();
    let w = ws(&m, "data", &key(100));
    assert_eq!(
        full.mismatches,
        vec![mismatch("data", &key(100), Some(w), Some(w))]
    );
    assert_eq!(full.keys_checked, u64::from(KEYS));
    assert_eq!(b.status().last_verify, Some(full));
}

/// The first and the last key sit in the two ranges without a lower / upper bound.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn keys_deleted_on_disk_are_reported_in_both_modes() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b)]).await;
    let b = tamper(b, &cfg, |raw| {
        raw.delete("data", &key(0));
        raw.delete("data", &key(KEYS - 1));
    })
    .await;
    let expected = vec![
        mismatch("data", &key(0), None, Some(ws(&m, "data", &key(0)))),
        mismatch(
            "data",
            &key(KEYS - 1),
            None,
            Some(ws(&m, "data", &key(KEYS - 1))),
        ),
    ];
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        assert_eq!(
            b.verify_now(mode).await.unwrap().mismatches,
            expected,
            "{mode:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn extra_keys_on_disk_are_reported_with_no_upstream_value() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b)]).await;
    let b = tamper(b, &cfg, |raw| {
        raw.put("data", b"k050x", &envelope::encode(7, None, b"stray"));
        raw.put("data", b"zzz", &envelope::encode(8, Some(1), b"stray"));
    })
    .await;
    let expected = vec![
        mismatch("data", b"k050x", Some(7), None),
        mismatch("data", b"zzz", Some(8), None),
    ];
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        assert_eq!(
            b.verify_now(mode).await.unwrap().mismatches,
            expected,
            "{mode:?}"
        );
    }
}

/// The slave cuts ranges at its own keys (every 16th: k016, k032, k048, …). A key that exists
/// only upstream, strictly between two boundaries, is still inside a range, and bisection
/// plus listing finds it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_key_only_upstream_between_two_chunk_boundaries_is_found() {
    let c = Cluster::new();
    let m = primary(&c).await;
    m.put("data", b"k040a", b"only upstream").unwrap();
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b)]).await;
    let b = tamper(b, &cfg, |raw| raw.delete("data", b"k040a")).await;
    let w = ws(&m, "data", b"k040a");
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let r = b.verify_now(mode).await.unwrap();
        assert_eq!(
            r.mismatches,
            vec![mismatch("data", b"k040a", None, Some(w))],
            "{mode:?}"
        );
        assert_eq!(r.keys_checked, u64::from(KEYS), "k040a is not a local key");
    }
}

/// Empty and NUL keys, an all-NUL key, a 1 MiB key (as a range boundary: chunks of one key)
/// and a ~16 MiB value verify clean in both modes; then damage to the 1 MiB key and to the
/// big value is found.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_case_data_verifies_and_its_corruption_is_found() {
    let c = Cluster::new();
    let m = c.master("a").await;
    write_edge_cases(&m, "edge");
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        cfg.verify.chunk_keys = 1;
        cfg.verify.min_chunk_keys = 1;
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let r = b.verify_now(mode).await.unwrap();
        assert!(r.is_clean(), "{mode:?}: {} mismatches", r.mismatches.len());
        assert_eq!(r.keys_checked, edge_cases().len() as u64);
    }

    let long_key = vec![0x5A; 1 << 20];
    let b = tamper(b, &cfg, |raw| {
        raw.delete("edge", &long_key);
        raw.corrupt_value("edge", b"big");
    })
    .await;
    let missing = mismatch("edge", &long_key, None, Some(ws(&m, "edge", &long_key)));
    let w = ws(&m, "edge", b"big");
    let quick = b.verify_now(VerifyMode::Quick).await.unwrap();
    assert!(
        quick.mismatches == vec![missing.clone()],
        "Quick: {} mismatches",
        quick.mismatches.len()
    );
    let full = b.verify_now(VerifyMode::Full).await.unwrap();
    assert!(
        full.mismatches == vec![missing, mismatch("edge", b"big", Some(w), Some(w))],
        "Full: {} mismatches",
        full.mismatches.len()
    );
}

/// Ranges, bisection and listings follow the CF's comparator: under reverse order k050
/// comes before k020, and that is the order mismatches are reported in.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_custom_comparator_cf_verifies_and_localises_damage() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.rocksdb.comparators.register("reverse", reverse);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("rev").with_comparator("reverse"))
        .unwrap();
    for i in 0..100 {
        m.put("rev", &key(i), format!("value-{i}").as_bytes())
            .unwrap();
    }
    let (b, cfg, _) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        cfg.rocksdb.comparators.register("reverse", reverse);
        cfg.verify.chunk_keys = 7;
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        assert!(b.verify_now(mode).await.unwrap().is_clean(), "{mode:?}");
    }
    let b = tamper_with(b, &cfg, &[("rev", "reverse", reverse)], |raw| {
        raw.delete("rev", &key(20));
        raw.corrupt_value("rev", &key(50));
    })
    .await;
    let (w20, w50) = (ws(&m, "rev", &key(20)), ws(&m, "rev", &key(50)));
    let quick = b.verify_now(VerifyMode::Quick).await.unwrap();
    assert_eq!(
        quick.mismatches,
        vec![mismatch("rev", &key(20), None, Some(w20))]
    );
    let full = b.verify_now(VerifyMode::Full).await.unwrap();
    assert_eq!(
        full.mismatches,
        vec![
            mismatch("rev", &key(50), Some(w50), Some(w50)),
            mismatch("rev", &key(20), None, Some(w20))
        ]
    );
}

/// A→B→C: each slave verifies against its own upstream; damage on C shows on C's link only.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn every_link_of_a_chain_verifies_against_its_own_upstream() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, _, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await;
    let (cn, c_cfg, _) = replica(&c, "c", b.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b), ("c", &cn)]).await;
    for (name, s) in [("b", &b), ("c", &cn)] {
        let r = s.verify_now(VerifyMode::Full).await.unwrap();
        assert!(r.is_clean(), "{name}: {:?}", r.mismatches);
        assert_eq!(r.seq, m.last_seq(), "{name}");
    }
    let cn = tamper(cn, &c_cfg, |raw| raw.corrupt_value("data", &key(7))).await;
    let w = ws(&m, "data", &key(7));
    assert_eq!(
        cn.verify_now(VerifyMode::Full).await.unwrap().mismatches,
        vec![mismatch("data", &key(7), Some(w), Some(w))]
    );
    assert!(
        b.verify_now(VerifyMode::Full).await.unwrap().is_clean(),
        "B's link is unaffected"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mismatch_policy_rebootstrap_repairs_and_a_reverify_is_clean() {
    let c = Cluster::new();
    let m = primary(&c).await;
    let (b, cfg, listener) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        cfg.verify.on_mismatch = MismatchPolicy::Rebootstrap
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    assert_eq!(listener.bootstraps(), 1);
    let b = tamper(b, &cfg, |raw| {
        raw.delete("data", &key(3));
        raw.corrupt_value("data", &key(150));
    })
    .await;
    let report = b.verify_now(VerifyMode::Full).await.unwrap();
    assert_eq!(report.mismatches.len(), 2, "{:?}", report.mismatches);
    wait_until("the policy re-bootstraps the slave", || {
        listener.bootstraps() == 2
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.get("data", &key(3)).unwrap(), Some(b"value-3".to_vec()));
    assert!(b.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(listener.verifies().len(), 2);
    listener.assert_no_violations();
}

/// A session idle for longer than the upstream's `verify_session_timeout` is gone: the run is
/// abandoned with `SessionExpired`. (The slave reads its single range at 2 000 bytes/s, so its
/// first request comes ~2 s after `VerifyBegin`.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_idle_session_expires_and_the_run_is_abandoned() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().limits.verify_session_timeout = Duration::from_millis(300);
    let m = Master::open(mcfg).await.unwrap();
    fill(&m);
    let (b, _, _) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        cfg.verify.chunk_keys = usize::MAX;
        cfg.verify.chunk_bytes = usize::MAX;
        cfg.verify.max_bytes_per_sec = Some(2_000);
    })
    .await;
    assert_converged(&m, &[("b", &b)]).await;
    let e = b.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::SessionExpired)),
        "{e:?}"
    );
}

/// With `max_verify_sessions = 1`, two slaves verifying at once cannot both get a session:
/// one run is refused with `Busy`, and once the slot is free both verify clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_upstream_limits_concurrent_verify_sessions() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().limits.max_verify_sessions = 1;
    let m = Master::open(mcfg).await.unwrap();
    fill(&m);
    let (b, _, _) = replica(&c, "b", m.serve_addr().unwrap(), |cfg| {
        // A slow run (~2 s) that holds the only session.
        cfg.verify.chunk_keys = usize::MAX;
        cfg.verify.chunk_bytes = usize::MAX;
        cfg.verify.max_bytes_per_sec = Some(2_000);
    })
    .await;
    let (cn, _, _) = replica(&c, "c", m.serve_addr().unwrap(), |_| {}).await;
    assert_converged(&m, &[("b", &b), ("c", &cn)]).await;

    let b = Arc::new(b);
    let slow = {
        let b = b.clone();
        tokio::spawn(async move { b.verify_now(VerifyMode::Quick).await })
    };
    let mut refused = 0;
    while !slow.is_finished() {
        match cn.verify_now(VerifyMode::Quick).await {
            Err(Error::VerifyAborted(VerifyAbort::Busy)) => refused += 1,
            Ok(r) => assert!(r.is_clean()),
            Err(e) => panic!("{e:?}"),
        }
        tokio::task::yield_now().await;
    }
    let slow = slow.await.unwrap();
    let slow_refused = matches!(slow, Err(Error::VerifyAborted(VerifyAbort::Busy)));
    assert!(
        refused > 0 || slow_refused,
        "one of two overlapping sessions must have been refused"
    );
    if !slow_refused {
        assert!(slow.unwrap().is_clean());
    }
    assert!(
        cn.verify_now(VerifyMode::Quick).await.unwrap().is_clean(),
        "the slot is free again"
    );
}

/// C is ahead of B (B lost entries it had not flushed, DESIGN.md §6). C cannot capture B's
/// snapshot seq and abandons the run instead of reporting phantom differences; once B has
/// caught up, C verifies clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slave_ahead_of_the_upstream_snapshot_abandons_the_run() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    let b_port = b.serve_addr().unwrap().port();
    let at_port = |mut cfg: SlaveConfig| {
        cfg.serve.as_mut().unwrap().port = b_port;
        cfg
    };
    b.shutdown().await.unwrap();
    copy_dir(&c.path("b"), &c.path("b-old"));
    let b = Slave::open(at_port(b_cfg.clone())).await.unwrap();
    let (c_cfg, _) = c.slave_config("c", b.serve_addr().unwrap());
    let cn = Slave::open(c_cfg).await.unwrap();
    for i in 0..10u8 {
        a.put("c", &[i], b"v").unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let c_seq = cn.last_seq();

    // B loses its newer entries; A is stopped, so B cannot catch up yet.
    let a_port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    restore_dir(&c.path("b-old"), &c.path("b"));
    let b = Slave::open(at_port(b_cfg.clone())).await.unwrap();
    let b_seq = b.last_seq();
    assert!(b_seq < c_seq);
    let e = cn.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::PassedSnapshot { snapshot_seq, applied_seq })
            if snapshot_seq == b_seq && applied_seq == c_seq),
        "{e:?}"
    );

    let mut a_cfg = c.master_config("a");
    a_cfg.serve.as_mut().unwrap().port = a_port;
    let a = Master::open(a_cfg).await.unwrap();
    a.put("c", b"after", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    assert!(cn.verify_now(VerifyMode::Quick).await.unwrap().is_clean());
}

/// A slave halted behind the upstream's snapshot can never reach it: the run gives up at once
/// instead of waiting `capture_timeout` (60 s).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_halted_slave_behind_the_snapshot_gives_up_at_once() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.rocksdb.comparators.register("reverse", reverse);
    let m = Master::open(mcfg).await.unwrap();
    fill(&m);
    let (b, _, _) = replica(&c, "b", m.serve_addr().unwrap(), |_| {}).await; // no "reverse" here
    assert_converged(&m, &[("b", &b)]).await;
    m.create_cf(CfSpec::new("rev").with_comparator("reverse"))
        .unwrap();
    wait_state(&b, "SpecMismatch halt", |s| {
        *s == SlaveState::Halted(HaltReason::SpecMismatch)
    })
    .await;
    let started = Instant::now();
    let e = b.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::CaptureTimeout { snapshot_seq }) if snapshot_seq == m.last_seq()),
        "{e:?}"
    );
    assert!(
        started.elapsed() < Duration::from_secs(10),
        "gave up without waiting for capture_timeout"
    );
}
```

- [ ] **Step 3: Run them**

Run: `cargo test --test integrity`
Expected: PASS, 12 tests: `a_corrupted_value_is_found_by_full_verify_only`, `keys_deleted_on_disk_are_reported_in_both_modes`, `extra_keys_on_disk_are_reported_with_no_upstream_value`, `a_key_only_upstream_between_two_chunk_boundaries_is_found`, `edge_case_data_verifies_and_its_corruption_is_found` (moves a ~16 MiB value through Full hashing on both sides; allow it a few seconds in debug builds), `a_custom_comparator_cf_verifies_and_localises_damage`, `every_link_of_a_chain_verifies_against_its_own_upstream`, `mismatch_policy_rebootstrap_repairs_and_a_reverify_is_clean`, `an_idle_session_expires_and_the_run_is_abandoned`, `the_upstream_limits_concurrent_verify_sessions`, `a_slave_ahead_of_the_upstream_snapshot_abandons_the_run`, `a_halted_slave_behind_the_snapshot_gives_up_at_once`.

Run: `cargo test`
Expected: PASS.

- [ ] **Step 4: Commit**

```bash
cargo fmt
git add tests
git commit -m "test: integrity scans against on-disk corruption, chains, policies and limits

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Replication lag (`SlaveStatus.lag_ms`)

**Files:**
- Create: `tests/observability.rs`
- Modify: `src/slave/state.rs`, `src/slave/inner.rs`, `src/slave/apply.rs`, `src/slave/bootstrap.rs`, `DESIGN.md` (§16)

`Master::replicas()` / `Slave::replicas()` (`ReplicaStatus` with acks, `lag_entries`, `lag_ms`, `in_sync`) already exist from Plan 2's acks work; this task adds the slave's own lag behind the origin.

**Interfaces:**
- Consumes: Task 5 `Inner.last_applied_ts`, `Inner::last_verify`; Plan 2 `Inner::{status, now_ms}`, `slave::apply::apply`, `slave::bootstrap::install`, harness `Cluster`, `assert_converged`, `wait_state`, `Slave::clear_halt`.
- Produces: `SlaveStatus.lag_ms: Option<u64>`; `slave::state::lag_ms(applied: Seq, origin_last: Seq, last_applied_ts: u64, now_ms: u64) -> Option<u64>`.

- [ ] **Step 1: Write the failing tests**

Append to `src/slave/state.rs`:

```rust
#[cfg(test)]
mod lag_tests {
    use super::lag_ms;

    #[test]
    fn lag_is_zero_when_caught_up_and_unknown_without_a_timestamp() {
        assert_eq!(
            lag_ms(10, 10, 0, 5),
            Some(0),
            "caught up, even without a timestamp"
        );
        assert_eq!(lag_ms(10, 12, 1_000, 4_000), Some(3_000));
        assert_eq!(
            lag_ms(10, 12, 0, 4_000),
            None,
            "behind, timestamp unknown (after a bootstrap)"
        );
        assert_eq!(
            lag_ms(10, 12, 5_000, 4_000),
            Some(0),
            "clock skew never makes it negative"
        );
    }
}
```

Create `tests/observability.rs`:

```rust
//! Observability, end to end: replication lag.

mod support;

use std::time::Duration;

use replicated_rocksdb::{CfSpec, HaltAction, HaltReason, Master, Slave, SlaveState};
use support::{Cluster, assert_converged, wait_state};

fn reverse(a: &[u8], b: &[u8]) -> std::cmp::Ordering {
    b.cmp(a)
}

/// `lag_ms` is 0 when caught up and, while behind, the age of the last applied entry. B is
/// stopped behind by an entry whose comparator it lacks (it halts `SpecMismatch` there).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn lag_ms_is_the_age_of_the_last_applied_entry_while_behind() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.rocksdb.comparators.register("reverse", reverse);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    // Tailed, not bootstrapped: B keeps these entries in its log, with their ts_ms.
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap(); // ts_ms = T0: the clock has not moved
    }
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().lag_ms, Some(0), "caught up");
    b.shutdown().await.unwrap();

    let behind = m.last_seq();
    c.clock.advance(Duration::from_secs(1));
    m.create_cf(CfSpec::new("rev").with_comparator("reverse"))
        .unwrap();
    m.put("c", b"later", b"v").unwrap();
    let b = Slave::open(cfg.clone()).await.unwrap();
    wait_state(&b, "SpecMismatch halt", |s| {
        *s == SlaveState::Halted(HaltReason::SpecMismatch)
    })
    .await;
    c.clock.advance(Duration::from_secs(7));
    let st = b.status();
    assert_eq!((st.applied_seq, st.origin_last_seq), (behind, m.last_seq()));
    assert_eq!(
        st.lag_ms,
        Some(8_000),
        "now − ts_ms of entry {behind}, which was written at T0"
    );

    b.shutdown().await.unwrap();
    let mut fixed = cfg;
    fixed.rocksdb.comparators.register("reverse", reverse);
    let b = Slave::open(fixed).await.unwrap();
    b.clear_halt(HaltAction::Retry).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().lag_ms, Some(0));
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib slave::state`
Expected: FAIL to compile: `unresolved import super::lag_ms`.

Run: `cargo test --test observability`
Expected: FAIL to compile: `no field lag_ms on type SlaveStatus`.

- [ ] **Step 3: Implement `lag_ms`**

In `src/slave/state.rs`, add the field to `SlaveStatus`:

```rust
    pub upstream_last_seq: Seq,
    pub origin_last_seq: Seq,
```

with:

```rust
    pub upstream_last_seq: Seq,
    pub origin_last_seq: Seq,
    /// 0 when `applied_seq >= origin_last_seq`; while behind, `now − ts_ms` of the last
    /// applied entry; `None` while that timestamp is unknown (after a bootstrap, until the
    /// next entry).
    pub lag_ms: Option<u64>,
```

and the function, above the first test module:

```rust
#[cfg(test)]
mod tests {
```

insert:

```rust
/// Replication lag in ms; see [`SlaveStatus::lag_ms`]. `last_applied_ts = 0` means unknown.
pub(crate) fn lag_ms(
    applied: Seq,
    origin_last: Seq,
    last_applied_ts: u64,
    now_ms: u64,
) -> Option<u64> {
    if applied >= origin_last {
        return Some(0);
    }
    (last_applied_ts > 0).then(|| now_ms.saturating_sub(last_applied_ts))
}
```

In `src/slave/inner.rs`, import it:

```rust
use super::state::{HaltReason, NodeState, SlaveState, SlaveStatus};
```

with:

```rust
use super::state::{HaltReason, NodeState, SlaveState, SlaveStatus, lag_ms};
```

and compute it in `Inner::status`:

```rust
    pub(crate) fn status(&self) -> SlaveStatus {
        let pos = self.store.position();
        SlaveStatus {
            state: self.state(),
            origin_identity: self.ident().map(|(id, _)| id),
            applied_seq: pos.last_seq,
            first_retained_seq: self.store.first_retained_seq(),
            upstream_last_seq: self.upstream_last_seq.load(Ordering::SeqCst),
            origin_last_seq: self
                .origin_last_seq
                .load(Ordering::SeqCst)
                .max(pos.last_seq),
```

with:

```rust
    pub(crate) fn status(&self) -> SlaveStatus {
        let pos = self.store.position();
        let origin_last_seq = self
            .origin_last_seq
            .load(Ordering::SeqCst)
            .max(pos.last_seq);
        SlaveStatus {
            state: self.state(),
            origin_identity: self.ident().map(|(id, _)| id),
            applied_seq: pos.last_seq,
            first_retained_seq: self.store.first_retained_seq(),
            upstream_last_seq: self.upstream_last_seq.load(Ordering::SeqCst),
            origin_last_seq,
            lag_ms: lag_ms(
                pos.last_seq,
                origin_last_seq,
                self.last_applied_ts.load(Ordering::SeqCst),
                self.now_ms(),
            ),
```

In `src/slave/apply.rs`, after the commit, record the entry's timestamp:

```rust
    inner.origin_last_seq.fetch_max(seq, Ordering::SeqCst);
```

with:

```rust
    inner.origin_last_seq.fetch_max(seq, Ordering::SeqCst);
    inner.last_applied_ts.store(entry.ts_ms, Ordering::SeqCst);
```

In `src/slave/bootstrap.rs`, in `install`, forget it:

```rust
    inner.cancel_capture();
    inner.set_ident(None);
```

with:

```rust
    inner.cancel_capture();
    // The snapshot's last entry is not transferred: its ts_ms is unknown until the next entry.
    inner.last_applied_ts.store(0, Ordering::SeqCst);
    inner.set_ident(None);
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib slave::state && cargo test --test observability`
Expected: PASS: `lag_is_zero_when_caught_up_and_unknown_without_a_timestamp` and `lag_ms_is_the_age_of_the_last_applied_entry_while_behind`.

Run: `cargo test`
Expected: PASS.

- [ ] **Step 5: Update DESIGN.md §16**

In DESIGN.md §16, in `SlaveStatus`, replace:

```
    pub lag_ms: Option<u64>,                 // now − ts_ms of the last applied entry, while behind
```

with:

```
    pub lag_ms: Option<u64>,                 // 0 when caught up; while behind, now − ts_ms of the last
                                             // applied entry; None while that ts is unknown (after a bootstrap)
```

and replace:

```
    // `SlaveStatus` is #[non_exhaustive]; `lag_ms` and `last_verify` above arrive in Plan 3.
```

with:

```
    // `SlaveStatus` is #[non_exhaustive].
```

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add src tests DESIGN.md
git commit -m "feat: SlaveStatus.lag_ms from the last applied entry's timestamp

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: Tracing spans, and RocksDB reads off the async workers

**Files:**
- Create: `tests/spans.rs`
- Modify: `src/server/mod.rs` (Handshake, Status), `src/server/tail.rs`, `src/server/bootstrap.rs`, `src/server/replicas.rs`, `src/slave/driver.rs`, `src/master/reaper.rs`, `DESIGN.md` (§19)

Two parts. **Spans** (DESIGN.md §19): `handshake`, `bootstrap`, `verify` and `reaper`, with `node_id`, `upstream` (absent on the master), `seq` and `epoch`; server-side spans add `client`, `verify` adds `mode`. The two `verify` spans exist since Tasks 4–5. **Deferred items from Plan 2's review:** RocksDB reads that ran on async workers move to blocking threads (the handshake's server view, Tail validation and the junction check, the `Status` RPC), and `ReplicaRegistry::status` no longer reads the log while holding the registry's locks.

**Interfaces:**
- Consumes: Plan 2 `Svc::{handshake, status}`, `server_view`, `status::node_info`, `tail::{tail, validate, check_junction, produce}`, `server::bootstrap::bootstrap` (its `"serving bootstrap"` event and the `spawn_blocking` that runs the job), `ReplicaRegistry::status`, `slave::driver::{session, handshake_request}`, `master::writer::Inner::{reap_round, identity}`, `net::internal_status`; harness `Cluster`, `assert_converged`; `tracing::{Subscriber, span::{Attributes, Id, Record}, field::{Field, Visit}, Instrument}`.
- Produces: spans named `handshake`, `bootstrap`, `verify`, `reaper` with fields `node_id`, `upstream` (the client's current upstream URI, or the server's first `upstream_chain` entry; absent on the master), `seq`, `epoch` (when known); server spans add `client`; `verify` adds `mode`. No API change.

- [ ] **Step 1: Write the failing test**

Create `tests/spans.rs`:

```rust
//! Tracing spans, end to end (DESIGN.md §19). A test binary of its own: it installs a global
//! subscriber that records every span.

mod support;

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use tracing::field::{Field, Visit};
use tracing::span::{Attributes, Id, Record};
use tracing::{Event, Metadata, Subscriber};

use replicated_rocksdb::{CfSpec, VerifyMode};
use support::{Cluster, assert_converged};

/// Every span opened in this test binary: its name and recorded fields (values as text).
#[derive(Default)]
struct Capture {
    spans: Mutex<Vec<(String, BTreeMap<String, String>)>>,
}

impl Capture {
    /// Field maps of the spans named `name` opened by node `node_id`.
    fn find(&self, name: &str, node_id: &str) -> Vec<BTreeMap<String, String>> {
        let spans = self.spans.lock().unwrap();
        spans
            .iter()
            .filter(|(n, f)| n == name && f.get("node_id").map(String::as_str) == Some(node_id))
            .map(|(_, f)| f.clone())
            .collect()
    }
}

struct Fields<'a>(&'a mut BTreeMap<String, String>);

impl Visit for Fields<'_> {
    fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
        self.0
            .insert(field.name().to_string(), format!("{value:?}"));
    }

    fn record_str(&mut self, field: &Field, value: &str) {
        self.0.insert(field.name().to_string(), value.to_string());
    }
}

/// A minimal subscriber that records spans and ignores events.
struct Recorder(Arc<Capture>);

impl Subscriber for Recorder {
    fn enabled(&self, _: &Metadata<'_>) -> bool {
        true
    }

    fn new_span(&self, attrs: &Attributes<'_>) -> Id {
        let mut fields = BTreeMap::new();
        attrs.record(&mut Fields(&mut fields));
        let mut spans = self.0.spans.lock().unwrap();
        spans.push((attrs.metadata().name().to_string(), fields));
        Id::from_u64(spans.len() as u64)
    }

    fn record(&self, id: &Id, values: &Record<'_>) {
        let mut spans = self.0.spans.lock().unwrap();
        if let Some((_, fields)) = spans.get_mut(id.into_u64() as usize - 1) {
            values.record(&mut Fields(fields));
        }
    }

    fn record_follows_from(&self, _: &Id, _: &Id) {}

    fn event(&self, _: &Event<'_>) {}

    fn enter(&self, _: &Id) {}

    fn exit(&self, _: &Id) {}
}

/// Both ends of a handshake, a bootstrap and a verify session, and a reaper round, open a span
/// with `node_id`, `upstream` (absent on the master), `seq` and `epoch`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn spans_carry_node_upstream_seq_and_epoch() {
    let capture = Arc::new(Capture::default());
    tracing::subscriber::set_global_default(Recorder(capture.clone())).unwrap();

    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    a.put_with_ttl("c", b"t", b"v", Duration::from_secs(1))
        .unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b)]).await;
    assert!(b.verify_now(VerifyMode::Quick).await.unwrap().is_clean());
    c.clock.advance(Duration::from_secs(2));
    assert_eq!(a.reap_expired().unwrap().deleted, 1);

    let upstream = format!("http://{}", a.serve_addr().unwrap());
    let epoch = a.identity().epoch.to_string();
    for (name, node) in [
        ("handshake", "a"),
        ("bootstrap", "a"),
        ("verify", "a"),
        ("reaper", "a"),
        ("handshake", "b"),
        ("bootstrap", "b"),
        ("verify", "b"),
    ] {
        let spans = capture.find(name, node);
        assert!(!spans.is_empty(), "no {name} span on {node}");
        for f in &spans {
            assert!(f.contains_key("seq"), "{name} on {node}: {f:?}");
            if node == "b" {
                assert_eq!(f.get("upstream"), Some(&upstream), "{name} on {node}");
            } else {
                assert!(
                    !f.contains_key("upstream"),
                    "the master has no upstream: {f:?}"
                );
            }
        }
    }
    for (name, node) in [
        ("bootstrap", "a"),
        ("verify", "a"),
        ("reaper", "a"),
        ("verify", "b"),
        ("handshake", "a"),
    ] {
        assert!(
            capture
                .find(name, node)
                .iter()
                .any(|f| f.get("epoch") == Some(&epoch)),
            "{name} on {node} has no epoch"
        );
    }
    let clients = |name: &str| {
        capture
            .find(name, "a")
            .into_iter()
            .filter_map(|f| f.get("client").cloned())
            .collect::<Vec<_>>()
    };
    assert!(clients("handshake").contains(&"b".to_string()));
    assert!(clients("bootstrap").contains(&"b".to_string()));
    assert!(
        capture
            .find("verify", "b")
            .iter()
            .any(|f| f.get("mode").map(String::as_str) == Some("Quick"))
    );
}
```

- [ ] **Step 2: Run it to confirm it fails**

Run: `cargo test --test spans`
Expected: FAIL: `no handshake span on a` (only the `verify` spans exist so far).

- [ ] **Step 3: Server-side spans, and the handshake and `Status` reads off the async workers**

In `src/server/mod.rs`, in `Svc::handshake`, replace:

```rust
        let view = server_view(&*src, r.last_seq)?;
        let v = handshake::decide(&view, &r);
        tracing::info!(
            server = %view.server_id, client = %r.client_id, last_seq = r.last_seq,
            decision = v.decision.as_str_name(), reason = v.reason.as_str_name(), detail = %v.detail,
            "handshake"
        );
```

with:

```rust
        // `server_view` reads the log (the CRC at the client's last seq): a blocking thread.
        let (s, client_last) = (src.clone(), r.last_seq);
        let view = tokio::task::spawn_blocking(move || server_view(&*s, client_last))
            .await
            .map_err(|e| net::internal_status(format!("handshake task failed: {e}")))??;
        let v = handshake::decide(&view, &r);
        let span = tracing::info_span!(
            "handshake",
            node_id = %view.server_id,
            upstream = view.upstream_chain.first().map(String::as_str),
            client = %r.client_id,
            seq = view.last_seq,
            epoch = view.identity.map(|id| id.epoch),
        );
        span.in_scope(|| {
            tracing::info!(
                client_last_seq = r.last_seq,
                decision = v.decision.as_str_name(),
                reason = v.reason.as_str_name(),
                detail = %v.detail,
                "handshake"
            )
        });
```

and in `Svc::status`, replace:

```rust
        let probes: Vec<Seq> = r.probe_seq.into_iter().collect();
        let info = status::node_info(
            &*src,
            &self.0.replicas,
            self.0.cfg.limits.replica_lag_time_max,
            &probes,
        );
```

with:

```rust
        let probes: Vec<Seq> = r.probe_seq.into_iter().collect();
        // `node_info` reads the log and `__meta`: a blocking thread.
        let replicas = self.0.replicas.clone();
        let lag_max = self.0.cfg.limits.replica_lag_time_max;
        let info = tokio::task::spawn_blocking(move || {
            status::node_info(&*src, &replicas, lag_max, &probes)
        })
        .await
        .map_err(|e| net::internal_status(format!("status task failed: {e}")))?;
```

In `src/server/bootstrap.rs`, in `bootstrap`, replace:

```rust
    tracing::info!(client = %client_id, snapshot_seq = meta.last_seq, cfs = meta.cf_specs.len(), "serving bootstrap");
```

with:

```rust
    let chain = src.upstream_chain();
    let span = tracing::info_span!(
        "bootstrap",
        node_id = %src.node_id(),
        upstream = chain.first().map(String::as_str),
        client = %client_id,
        seq = meta.last_seq,
        epoch = identity.epoch,
    );
    span.in_scope(|| tracing::info!(cfs = meta.cf_specs.len(), "serving bootstrap"));
```

and run the job inside the span:

```rust
    tokio::task::spawn_blocking(move || {
        let (tx, end) = job.run();
```

with:

```rust
    tokio::task::spawn_blocking(move || {
        let _entered = span.enter();
        let (tx, end) = job.run();
```

- [ ] **Step 4: Tail validation and the junction check on blocking threads**

In `src/server/tail.rs`, in `tail`, replace:

```rust
    let junction = validate(&*src, &r)?;
```

with:

```rust
    // `validate` reads the log (the CRC at `from_seq - 1`): a blocking thread.
    let (s, req) = (src.clone(), r.clone());
    let junction = tokio::task::spawn_blocking(move || validate(&*s, &req))
        .await
        .map_err(|e| internal_status(format!("tail validation failed: {e}")))??;
```

and in `produce`, replace:

```rust
            if let Err(status) = check_junction(&*src, j) {
                return out.end(status).await;
            }
```

with:

```rust
            let s = src.clone();
            match tokio::task::spawn_blocking(move || check_junction(&*s, j)).await {
                Ok(Ok(())) => {}
                Ok(Err(status)) => return out.end(status).await,
                Err(e) => {
                    let status = internal_status(format!("junction check failed: {e}"));
                    return out.end(status).await;
                }
            }
```

- [ ] **Step 5: `ReplicaRegistry::status` reads the log after releasing its locks**

In `src/server/replicas.rs`, in `ReplicaRegistry::status`, replace:

```rust
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
```

with:

```rust
        let epoch = src.identity().map_or(0, |(id, _)| id.epoch);
        let last = src.store().position().last_seq;
        // Copy what is needed under the registry's locks, then read the log without them.
        let streams: Vec<(Arc<ReplicaCell>, Seq, Seq, bool)> = self
            .streams()
            .values()
            .map(|c| {
                let p = c.progress();
                let in_sync = p.in_sync(c.epoch, epoch, lag_max);
                (c.clone(), p.sent_seq, p.applied_seq, in_sync)
            })
            .collect();
        let mut out: Vec<ReplicaStatus> = streams
            .into_iter()
            .map(|(c, sent_seq, applied_seq, in_sync)| {
                let lag_entries = last.saturating_sub(applied_seq);
                let lag_ms = if lag_entries == 0 {
                    Some(0)
                } else {
                    src.store()
                        .read_entries(applied_seq + 1, 1, usize::MAX)
                        .ok()
                        .and_then(|v| v.first().map(|e| now_ms.saturating_sub(e.ts_ms)))
                };
                ReplicaStatus {
                    node_id: c.node_id.clone(),
                    remote_addr: c.remote_addr,
                    peer_verified: c.peer_verified,
                    epoch: c.epoch,
                    connected_since: c.connected_since,
                    sent_seq,
                    applied_seq,
                    lag_entries,
                    lag_ms,
                    in_sync,
                    fetch: c.fetch,
                }
            })
            .collect();
```

- [ ] **Step 6: Slave-side spans**

In `src/slave/driver.rs`, import `Instrument`:

```rust
use tokio_stream::wrappers::WatchStream;
```

with:

```rust
use tokio_stream::wrappers::WatchStream;
use tracing::Instrument;
```

in `session`, replace:

```rust
    let hs = match unless_repointed(
        inner,
        generation,
        c.clone().handshake(handshake_request(inner)),
    )
    .await
    {
```

with:

```rust
    let req = handshake_request(inner);
    let span = tracing::info_span!(
        "handshake",
        node_id = %inner.cfg.identifier,
        upstream = %inner.upstream_uri(),
        seq = req.last_seq,
        epoch = req.known_identity.as_ref().map(|id| id.epoch),
    );
    let hs = match unless_repointed(
        inner,
        generation,
        c.clone().handshake(req).instrument(span.clone()),
    )
    .await
    {
```

log the decision in the handshake's span:

```rust
    // The retry arm of the driver loop logs the warning and sets `last_error`.
    if let Some(end) = contradicting_resume(after_stream_diverged, &action) {
```

with:

```rust
    span.in_scope(|| {
        tracing::info!(
            decision = hs.decision().as_str_name(),
            reason = hs.reason().as_str_name(),
            ?action,
            "handshake answered"
        )
    });
    // The retry arm of the driver loop logs the warning and sets `last_error`.
    if let Some(end) = contradicting_resume(after_stream_diverged, &action) {
```

and run the bootstrap in its own span:

```rust
        Action::Bootstrap => match bootstrap::install(inner, generation, &c, &hs).await {
            Ok(lease) => {
                // The bootstrap commit wrote `ready` itself.
                inner.halt_clear_pending.store(false, Ordering::SeqCst);
                inner.settle_repoint(generation, Ok(RepointOutcome::Bootstrapped));
                tail(inner, generation, &c, &hs, Some(lease), backoff).await
            }
            Err(end) => end,
        },
```

with:

```rust
        Action::Bootstrap => {
            // `seq` is the upstream's last_seq at the handshake; the snapshot seq is logged
            // inside, when the header arrives.
            let span = tracing::info_span!(
                "bootstrap",
                node_id = %inner.cfg.identifier,
                upstream = %inner.upstream_uri(),
                seq = hs.last_seq,
                epoch = hs.identity.as_ref().map(|id| id.epoch),
            );
            match bootstrap::install(inner, generation, &c, &hs)
                .instrument(span)
                .await
            {
                Ok(lease) => {
                    // The bootstrap commit wrote `ready` itself.
                    inner.halt_clear_pending.store(false, Ordering::SeqCst);
                    inner.settle_repoint(generation, Ok(RepointOutcome::Bootstrapped));
                    tail(inner, generation, &c, &hs, Some(lease), backoff).await
                }
                Err(end) => end,
            }
        }
```

- [ ] **Step 7: The reaper span**

In `src/master/reaper.rs`, at the start of `reap_round`:

```rust
    pub(crate) fn reap_round(&self) -> Result<ReapStats> {
        let cfg = &self.cfg.reaper;
```

with:

```rust
    pub(crate) fn reap_round(&self) -> Result<ReapStats> {
        let _span = tracing::info_span!(
            "reaper",
            node_id = %self.cfg.identifier,
            seq = self.store.position().last_seq,
            epoch = self.identity().epoch,
        )
        .entered();
        let cfg = &self.cfg.reaper;
```

- [ ] **Step 8: Run the tests**

Run: `cargo test --test spans`
Expected: PASS, 1 test.

Run: `cargo test && cargo test --features failpoints`
Expected: PASS (behaviour is unchanged by the blocking-thread moves; the server, replica-status and Status tests cover them).

- [ ] **Step 9: Update DESIGN.md §19**

In `DESIGN.md`, replace:

```markdown
- **`tracing` spans** for each handshake, bootstrap, verify session and reaper round, with `node_id`, `upstream`, `seq` and `epoch` fields. Each applied entry is logged at `trace` level only.
```

with:

```markdown
- **`tracing` spans** named `handshake`, `bootstrap`, `verify` and `reaper`, with fields `node_id` (this node), `upstream` (this node's upstream; absent on the master), `seq` and `epoch` (when known). Both ends of a handshake, bootstrap and verify session open one; the server's also carry `client`, and `verify` carries `mode`. Each applied entry is logged at `trace` level only.
- `SlaveStatus` carries `lag_ms` and `last_verify` (§16).
```

- [ ] **Step 10: Commit**

```bash
cargo fmt
git add src tests DESIGN.md
git commit -m "feat: tracing spans; move RocksDB reads off async workers and out of registry locks

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: Repair fetches on the verify session — `FetchKeys`, `FetchRange`, `EntryCrcs`

**Files:**
- Modify: `proto/replication.proto`, `PROTOCOL.md` (§9.2, §13, §14), `src/server/verify.rs`, `src/verify/scan.rs`, `src/store/repl.rs`, `src/slave/verifier.rs`

This task adds the wire half of divergence repair (spec addendum `docs/superpowers/specs/2026-10-09-failover-acks-repair.md` §5): three request kinds on the `VerifyRange` stream of a session pinned to the upstream's snapshot at `S`. `FetchKeys` returns the envelopes (or absence) of given keys, `FetchRange` every key of a range with its envelope (paged like `ListRange`), and `EntryCrcs` the `crc32c` of log entries up to `S` as of the session's snapshot (to check the entry before a fork point). The client side becomes a reusable `UpstreamSession`, used by verify and, in Task 10, by repair.

**Interfaces:**
- Consumes: Task 4 `server::verify::{Session, begin, answer, compute, target}`, Task 5 `slave::verifier::{verify, session, compare, capture, Upstream}`, Task 3 `verify::scan::scan`, `verify::{LIST_SOFT_BYTES, MAX_LIST_LIMIT, MAX_RESUME_KEY_BYTES}`; Plan 2 `ReadSnapshot::{snap, seq}`, `OwnedSnapshot::get_cf`, `ReadCtx::get_raw`; Task 5 `slave::client::connect_now`, `BEGIN_TIMEOUT`, `ANSWER_TIMEOUT`.
- Produces:
  - Generated `pb::{FetchKeys { cf, keys }, FetchRange { range, limit }, EntryCrcs { from_seq, to_seq }, FetchResult { records, truncated }, FetchedRecord { key, envelope: Option<Vec<u8>> }, EntryCrcResult { entries }, SeqCrc { seq, crc32c }}`, `pb::verify_range_request::Kind::{FetchKeys, FetchRange, EntryCrcs}`, `pb::verify_range_response::Kind::{Fetch, EntryCrcs}`.
  - `verify::scan::fetch_range(&ReadSnapshot, Arc<PhysCf>, &KeyOrder, &Bounds, limit: usize, soft_bytes: usize) -> Result<(Vec<pb::FetchedRecord>, bool)>`.
  - `ReadSnapshot::entry_crc(&self, Seq) -> Result<Option<u32>>` (crate); `slave::verifier::{UpstreamSession { pub begin: pb::VerifyBeginResponse, pub upstream: Upstream } (open(&Inner, &Client, VerifyMode, bound: Duration) -> Result<Self>, close(self)), Upstream::{fetch_keys(&mut self, cf: &str, keys: &[Vec<u8>], sink: &mut (dyn FnMut(Vec<u8>, Option<Vec<u8>>) -> bool + Send)) -> Result<bool>, fetch_range(&mut self, cf: &str, &Bounds, sink) -> Result<bool>, entry_crc(&mut self, Seq) -> Result<Option<u32>>}, FETCH_REQUEST_BYTES = 1 MiB, fetch_batch_len}`.

- [ ] **Step 1: Extend the protocol**

In `proto/replication.proto` **and** in the PROTOCOL.md §14 protobuf block (they must stay byte-identical; `tests/proto_sync.rs` checks), make the same replacement:

In `proto/replication.proto`, replace:

```proto
message VerifyRangeRequest {
  bytes  session_id = 1;
  uint64 request_id = 2;
  oneof kind { HashRange hash = 3; ListRange list = 4; }
}

message VerifyRangeResponse {
  uint64 request_id = 1;
  oneof kind { HashResult hash = 2; ListResult list = 3; }
}

message HashResult { bool matched = 1; uint64 count = 2; bytes hash = 3; }
message ListResult { repeated KeyDigest keys = 1; bool truncated = 2; }
message KeyDigest  { bytes key = 1; uint64 write_seq = 2; bytes payload_hash = 3; }
```

with:

```proto
message FetchKeys  { string cf = 1; repeated bytes keys = 2; }
message FetchRange { KeyRange range = 1; uint32 limit = 2; }
message EntryCrcs  { uint64 from_seq = 1; uint64 to_seq = 2; }

message VerifyRangeRequest {
  bytes  session_id = 1;
  uint64 request_id = 2;
  oneof kind {
    HashRange  hash        = 3;
    ListRange  list        = 4;
    FetchKeys  fetch_keys  = 5;
    FetchRange fetch_range = 6;
    EntryCrcs  entry_crcs  = 7;
  }
}

message VerifyRangeResponse {
  uint64 request_id = 1;
  oneof kind {
    HashResult     hash       = 2;
    ListResult     list       = 3;
    FetchResult    fetch      = 4;
    EntryCrcResult entry_crcs = 5;
  }
}

message HashResult     { bool matched = 1; uint64 count = 2; bytes hash = 3; }
message ListResult     { repeated KeyDigest keys = 1; bool truncated = 2; }
message KeyDigest      { bytes key = 1; uint64 write_seq = 2; bytes payload_hash = 3; }
message FetchResult    { repeated FetchedRecord records = 1; bool truncated = 2; }
message FetchedRecord  { bytes key = 1; optional bytes envelope = 2; }   // no envelope: no such key
message EntryCrcResult { repeated SeqCrc entries = 1; }
message SeqCrc         { uint64 seq = 1; fixed32 crc32c = 2; }
```

In PROTOCOL.md §14, replace the same text:

```proto
message VerifyRangeRequest {
  bytes  session_id = 1;
  uint64 request_id = 2;
  oneof kind { HashRange hash = 3; ListRange list = 4; }
}

message VerifyRangeResponse {
  uint64 request_id = 1;
  oneof kind { HashResult hash = 2; ListResult list = 3; }
}

message HashResult { bool matched = 1; uint64 count = 2; bytes hash = 3; }
message ListResult { repeated KeyDigest keys = 1; bool truncated = 2; }
message KeyDigest  { bytes key = 1; uint64 write_seq = 2; bytes payload_hash = 3; }
```

with:

```proto
message FetchKeys  { string cf = 1; repeated bytes keys = 2; }
message FetchRange { KeyRange range = 1; uint32 limit = 2; }
message EntryCrcs  { uint64 from_seq = 1; uint64 to_seq = 2; }

message VerifyRangeRequest {
  bytes  session_id = 1;
  uint64 request_id = 2;
  oneof kind {
    HashRange  hash        = 3;
    ListRange  list        = 4;
    FetchKeys  fetch_keys  = 5;
    FetchRange fetch_range = 6;
    EntryCrcs  entry_crcs  = 7;
  }
}

message VerifyRangeResponse {
  uint64 request_id = 1;
  oneof kind {
    HashResult     hash       = 2;
    ListResult     list       = 3;
    FetchResult    fetch      = 4;
    EntryCrcResult entry_crcs = 5;
  }
}

message HashResult     { bool matched = 1; uint64 count = 2; bytes hash = 3; }
message ListResult     { repeated KeyDigest keys = 1; bool truncated = 2; }
message KeyDigest      { bytes key = 1; uint64 write_seq = 2; bytes payload_hash = 3; }
message FetchResult    { repeated FetchedRecord records = 1; bool truncated = 2; }
message FetchedRecord  { bytes key = 1; optional bytes envelope = 2; }   // no envelope: no such key
message EntryCrcResult { repeated SeqCrc entries = 1; }
message SeqCrc         { uint64 seq = 1; fixed32 crc32c = 2; }
```

In the PROTOCOL.md §9.2 snippet, replace:

```proto
message VerifyRangeRequest  { bytes session_id = 1; uint64 request_id = 2;
                              oneof kind { HashRange hash = 3; ListRange list = 4; } }
message VerifyRangeResponse { uint64 request_id = 1;
                              oneof kind { HashResult hash = 2; ListResult list = 3; } }
message HashResult { bool matched = 1; uint64 count = 2; bytes hash = 3; }
message ListResult { repeated KeyDigest keys = 1; bool truncated = 2; }
message KeyDigest  { bytes key = 1; uint64 write_seq = 2; bytes payload_hash = 3; }
```

with:

```proto
message FetchKeys  { string cf = 1; repeated bytes keys = 2; }
message FetchRange { KeyRange range = 1; uint32 limit = 2; }
message EntryCrcs  { uint64 from_seq = 1; uint64 to_seq = 2; }

message VerifyRangeRequest  { bytes session_id = 1; uint64 request_id = 2;
                              oneof kind { HashRange hash = 3; ListRange list = 4; FetchKeys fetch_keys = 5;
                                           FetchRange fetch_range = 6; EntryCrcs entry_crcs = 7; } }
message VerifyRangeResponse { uint64 request_id = 1;
                              oneof kind { HashResult hash = 2; ListResult list = 3; FetchResult fetch = 4;
                                           EntryCrcResult entry_crcs = 5; } }
message HashResult     { bool matched = 1; uint64 count = 2; bytes hash = 3; }
message ListResult     { repeated KeyDigest keys = 1; bool truncated = 2; }
message KeyDigest      { bytes key = 1; uint64 write_seq = 2; bytes payload_hash = 3; }
message FetchResult    { repeated FetchedRecord records = 1; bool truncated = 2; }
message FetchedRecord  { bytes key = 1; optional bytes envelope = 2; }   // no envelope: no such key
message EntryCrcResult { repeated SeqCrc entries = 1; }
message SeqCrc         { uint64 seq = 1; fixed32 crc32c = 2; }
```

In PROTOCOL.md §9.2, after the bullet about `UPSTREAM_RESET` (added in Task 4), add the new kinds:

```markdown
- The server ends every `VerifyRange` stream, and drops every session, with `UNAVAILABLE` / `UPSTREAM_RESET` when its state is replaced (it starts bootstrapping itself, or its identity changes), as it does for `Tail` (§7.3).
```

with:

```markdown
- The server ends every `VerifyRange` stream, and drops every session, with `UNAVAILABLE` / `UPSTREAM_RESET` when its state is replaced (it starts bootstrapping itself, or its identity changes), as it does for `Tail` (§7.3).
- `FetchKeys` (repair): the server returns, in the order asked, a `FetchedRecord` per key with its whole envelope as of the session's snapshot, or without `envelope` when the key does not exist. `keys` MUST NOT hold more than 1024 keys. The server MAY answer only a prefix of the keys (setting `truncated`) to bound the answer (RECOMMENDED: stop once ~1 MiB of envelopes is reached), but answers at least one; the client asks again for the rest.
- `FetchRange` (repair): like `ListRange`, but each record carries the whole envelope. The same limits, paging rule and two-record minimum apply.
- `EntryCrcs` (repair): the server returns `(seq, crc32c)` for every entry with `from_seq ≤ seq ≤ min(to_seq, snapshot_seq, from_seq + 1023)` that it held at the snapshot (entry `snapshot_seq` is always known). They are read through the session's snapshot, so they never change during the session; a client compares them with its own entries (repair checks the entry before the fork its epoch history shows).
- Envelopes expired but not yet reaped are returned like any other: repair copies what is stored.
```

In PROTOCOL.md §13, replace:

```markdown
- **Authorisation scope.** A credential that can call `Bootstrap` can read the whole database.
```

with:

```markdown
- **Authorisation scope.** A credential that can call `Bootstrap`, or `VerifyRange` with `FetchKeys` or `FetchRange`, can read the whole database.
```

Run: `cargo test --test proto_sync`
Expected: PASS.

Run: `cargo build`
Expected: FAIL to compile: `non-exhaustive patterns: ReqKind::FetchKeys(_), ReqKind::FetchRange(_) and ReqKind::EntryCrcs(_) not covered` in `src/server/verify.rs`, and `non-exhaustive patterns` in `src/slave/verifier.rs` (`RespKind::Fetch(_)`, `RespKind::EntryCrcs(_)`).

- [ ] **Step 2: Write the failing tests**

In the test module of `src/server/verify.rs`, import `LogApi`:

```rust
    use crate::codec::envelope;
    use crate::net::convert::identity_to_pb;
```

with:

```rust
    use crate::codec::envelope;
    use crate::log::LogApi;
    use crate::net::convert::identity_to_pb;
```

and add, before `sessions_are_limited_and_ending_one_frees_a_slot`:

```rust
    #[tokio::test]
    async fn sessions_are_limited_and_ending_one_frees_a_slot() {
```

insert:

```rust
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
                        to_seq: 9,
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
                assert!(f.records.iter().all(|r| r.envelope.is_some()));
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
        match &answers[2].kind {
            Some(Resp::EntryCrcs(r)) => {
                assert_eq!(r.entries, expected, "3..=4: nothing past the snapshot")
            }
            other => panic!("{other:?}"),
        }
        let too_many = Req::FetchKeys(pb::FetchKeys {
            cf: "c".into(),
            keys: vec![vec![1]; 1025],
        });
        let (_, err) = exchange(&mut c, vec![request(&sid, 4, too_many)]).await;
        assert_eq!(err.map(|e| e.code()), Some(Code::InvalidArgument));
        let no_limit = Req::FetchRange(pb::FetchRange {
            range: Some(range(None, None)),
            limit: 0,
        });
        let (_, err) = exchange(&mut c, vec![request(&sid, 5, no_limit)]).await;
        assert_eq!(err.map(|e| e.code()), Some(Code::InvalidArgument));
    }
```

In the test module of `src/verify/scan.rs`, add, before `the_snapshot_pins_what_is_hashed`:

```rust
    #[test]
    fn the_snapshot_pins_what_is_hashed() {
```

insert:

```rust
    #[test]
    fn fetch_range_returns_whole_envelopes_in_pages() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap();
        apply(
            &store,
            (b'a'..=b'e').map(|k| put("c", &[k], &[k; 3])).collect(),
        )
        .unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let cf = snap.phys("c").unwrap();
        let o = KeyOrder::bytewise();
        let (all, more) = fetch_range(
            &snap,
            cf.clone(),
            &o,
            &b(Some(b"b"), Some(b"e")),
            1024,
            usize::MAX,
        )
        .unwrap();
        assert!(!more);
        assert_eq!(
            all.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
            vec![b"b".to_vec(), b"c".to_vec(), b"d".to_vec()]
        );
        assert_eq!(
            all[0].envelope.as_deref(),
            Some(envelope::encode(2, None, b"bbb").as_slice())
        );
        let (page, more) = fetch_range(&snap, cf, &o, &all_bounds(), 1024, 1).unwrap();
        assert_eq!(
            (page.len(), more),
            (2, true),
            "two records even past the soft size"
        );
    }

    fn all_bounds() -> Bounds {
        Bounds::default()
    }
```

- [ ] **Step 3: Serve the new kinds**

In `src/verify/scan.rs`, above the test module, add:

```rust
#[cfg(test)]
mod tests {
```

insert:

```rust
/// Up to `limit` keys of a range with their whole envelopes, for repair; `true` if there are
/// more. Like a listing, the answer is cut at about `soft_bytes`, but never below two keys.
pub(crate) fn fetch_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    limit: usize,
    soft_bytes: usize,
) -> Result<(Vec<pb::FetchedRecord>, bool)> {
    let (mut records, mut bytes, mut truncated) = (Vec::new(), 0usize, false);
    scan(snap, cf, order, bounds, |k, v| {
        if records.len() >= limit || (records.len() >= 2 && bytes >= soft_bytes) {
            truncated = true;
            return Ok(false);
        }
        bytes += k.len() + v.len();
        records.push(pb::FetchedRecord {
            key: k.to_vec(),
            envelope: Some(v.to_vec()),
        });
        Ok(true)
    })?;
    Ok((records, truncated))
}
```

In `src/store/repl.rs`, in `impl ReadSnapshot`, add before `phys`:

```rust
    /// The physical CF this snapshot holds for logical CF `name`.
    pub(crate) fn phys(&self, name: &str) -> Option<Arc<PhysCf>> {
```

insert:

```rust
    /// The `crc32c` of changelog entry `seq` as of this snapshot (`None` if it was not
    /// retained then, or `seq` is after the snapshot's seq).
    pub(crate) fn entry_crc(&self, seq: Seq) -> Result<Option<u32>> {
        if seq == 0 || seq > self.seq {
            return Ok(None);
        }
        let cf = self
            .snap
            .db()
            .cf_handle(CF_CHANGELOG)
            .ok_or_else(|| Error::corrupt("__changelog is not open"))?;
        self.snap
            .get_cf(&cf, &seq.to_be_bytes())?
            .map(|v| LogEntry::decode(&v).map(|e| e.crc32c))
            .transpose()
    }
```

In `src/server/verify.rs`, add an import:

```rust
use crate::read::ReadSnapshot;
use crate::store::cfs::PhysCf;
```

with:

```rust
use crate::read::ReadSnapshot;
use crate::store::cfs::PhysCf;
use crate::types::Seq;
```

keep `S` and its CRC in the session:

```rust
    snap: ReadSnapshot,
    mode: VerifyMode,
    cfs: HashMap<String, (Arc<PhysCf>, KeyOrder)>,
```

with:

```rust
    snap: ReadSnapshot,
    mode: VerifyMode,
    /// `S` and the CRC of entry `S`, read through the snapshot.
    seq: Seq,
    last_crc: Option<u32>,
    cfs: HashMap<String, (Arc<PhysCf>, KeyOrder)>,
```

(in `begin`):

```rust
    let session = Session {
        snap,
        mode,
        cfs,
```

with:

```rust
    let session = Session {
        snap,
        mode,
        seq: meta.last_seq,
        last_crc: meta.last_entry_crc,
        cfs,
```

and add the arms after the `ReqKind::List` arm:

```rust
            Ok(RespKind::List(pb::ListResult { keys, truncated }))
        }
    }
}
```

with:

```rust
            Ok(RespKind::List(pb::ListResult { keys, truncated }))
        }
        ReqKind::FetchKeys(f) => {
            if f.keys.len() > MAX_LIST_LIMIT as usize {
                return Err(net::invalid_argument(format!(
                    "FetchKeys takes at most {MAX_LIST_LIMIT} keys"
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
            let (records, truncated) = scan::fetch_range(
                &s.snap,
                phys.clone(),
                order,
                &bounds,
                f.limit as usize,
                LIST_SOFT_BYTES,
            )
            .map_err(store_status)?;
            Ok(RespKind::Fetch(pb::FetchResult { records, truncated }))
        }
        ReqKind::EntryCrcs(e) => {
            // Read through the session's snapshot: entries up to S never change, and a log
            // that moves on (retention, a repair or bootstrap of this node) does not matter.
            let last = e
                .to_seq
                .min(s.seq)
                .min(e.from_seq.saturating_add(u64::from(MAX_LIST_LIMIT) - 1));
            let mut entries = Vec::new();
            for seq in e.from_seq.max(1)..=last {
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
```

- [ ] **Step 4: The client side: a reusable session with fetches**

Changes to `src/slave/verifier.rs`: `UpstreamSession` opens (within the `VerifyBegin` bound, both `VerifyBegin` and the `VerifyRange` stream) and closes a session for any user; `Upstream` is `pub(crate)` and gains `fetch_keys` (requests bounded to 1 MiB of keys and 1024 keys), `fetch_range` (paged) and `entry_crc`; fetched records go to a sink that can stop early (repair counts its byte budget there); answers of an unexpected kind are errors.

In `src/slave/verifier.rs`, replace:

```rust
use crate::verify::{
    LIST_SOFT_BYTES, MismatchPolicy, VERIFY_WINDOW, VerifyAbort, VerifyMode, VerifyReport,
};
```

with:

```rust
use crate::verify::{
    LIST_SOFT_BYTES, MAX_LIST_LIMIT, MAX_RESUME_KEY_BYTES, MismatchPolicy, VERIFY_WINDOW,
    VerifyAbort, VerifyMode, VerifyReport,
};
```

In `src/slave/verifier.rs`, replace:

```rust
const ANSWER_TIMEOUT: Duration = Duration::from_secs(60);
```

with:

```rust
const ANSWER_TIMEOUT: Duration = Duration::from_secs(60);
/// The most key bytes one `FetchKeys` request carries (the server accepts 4 MiB requests).
pub(crate) const FETCH_REQUEST_BYTES: usize = 1 << 20;
```

In `src/slave/verifier.rs`, replace:

```rust

async fn session(
```

with:

```rust

/// An open session on the upstream (PROTOCOL.md §9): its `VerifyBegin` answer and the
/// `VerifyRange` stream used for hashes, listings, fetches and entry CRCs.
pub(crate) struct UpstreamSession {
    client: Client,
    pub begin: pb::VerifyBeginResponse,
    pub upstream: Upstream,
}

impl UpstreamSession {
    /// `VerifyBegin`, then the `VerifyRange` stream, both within `bound`. `client` should be
    /// connected already ([`client::connect_now`]).
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
                let end = pb::VerifyEndRequest {
                    session_id: begin.session_id.clone(),
                };
                let _ = client.verify_end(end).await;
                return Err(match failed {
                    Ok(Err(s)) => status_abort(&s),
                    _ => timed_out(),
                });
            }
        };
        let upstream = Upstream {
            tx,
            answers,
            session_id: begin.session_id.clone(),
            next_id: 0,
        };
        Ok(UpstreamSession {
            client,
            begin,
            upstream,
        })
    }

    /// Ends the session. Best effort: the upstream also expires idle sessions.
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
        let _ = client.verify_end(end).await;
    }
}

async fn session(
```

In `src/slave/verifier.rs`, replace:

```rust
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
```

with:

```rust
    let started = Instant::now();
    // Connected before the applier is held, so the pause never includes TCP or TLS setup.
    let client = client::connect_now(inner)
        .await
        .map_err(|e| abort(VerifyAbort::Upstream(format!("connect: {e}"))))?;
    // The applier commits nothing while the upstream pins its snapshot, so this slave cannot
```

In `src/slave/verifier.rs`, replace:

```rust
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
```

with:

```rust
    let bound = BEGIN_TIMEOUT.min(inner.cfg.verify.capture_timeout);
    let mut session = UpstreamSession::open(inner, &client, mode, bound).await?;
    span.record("seq", session.begin.snapshot_seq);
    let result = compare(inner, &mut session, hold, identity, mode, started).await;
    session.close().await;
    result
```

In `src/slave/verifier.rs`, replace:

```rust
    inner: &Arc<Inner>,
    client: &mut Client,
    begin: &pb::VerifyBeginResponse,
    hold: CaptureHold,
```

with:

```rust
    inner: &Arc<Inner>,
    session: &mut UpstreamSession,
    hold: CaptureHold,
```

In `src/slave/verifier.rs`, replace:

```rust
) -> Result<VerifyReport> {
    let upstream_identity = begin.identity.as_ref().map(identity_from_pb).transpose()?;
```

with:

```rust
) -> Result<VerifyReport> {
    let begin = &session.begin;
    let upstream_identity = begin.identity.as_ref().map(identity_from_pb).transpose()?;
```

In `src/slave/verifier.rs`, replace:

```rust

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
```

with:

```rust

    let upstream = &mut session.upstream;
    let cfg = &inner.cfg.verify;
```

In `src/slave/verifier.rs`, replace:

```rust
        };
        let compared = compare_cf(&mut upstream, &local, &mut run, &mut ranges).await;
        // Unblocks the producer if the comparison stopped early.
```

with:

```rust
        };
        let compared = compare_cf(upstream, &local, &mut run, &mut ranges).await;
        // Unblocks the producer if the comparison stopped early.
```

In `src/slave/verifier.rs`, replace:

```rust

/// The upstream's half of a session, over one `VerifyRange` stream.
struct Upstream {
    tx: mpsc::Sender<pb::VerifyRangeRequest>,
```

with:

```rust

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

/// The upstream's half of a session, over one `VerifyRange` stream.
pub(crate) struct Upstream {
    tx: mpsc::Sender<pb::VerifyRangeRequest>,
```

In `src/slave/verifier.rs`, replace:

```rust
            Err(s) => Err(status_abort(&s)),
        }
    }
}

impl Oracle for Upstream {
```

with:

```rust
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
            let answered_others = f.records.is_empty()
                || f.records.len() > ask.len()
                || f.records.iter().zip(ask).any(|(r, k)| r.key != *k);
            if answered_others {
                return Err(abort(VerifyAbort::Upstream(
                    "FetchKeys answered other keys".into(),
                )));
            }
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
    /// key order, paging through truncated answers like a listing. Stops early, returning
    /// `Ok(false)`, when `sink` returns `false`.
    pub(crate) async fn fetch_range(
        &mut self,
        cf: &str,
        bounds: &Bounds,
        sink: &mut (dyn FnMut(Vec<u8>, Option<Vec<u8>>) -> bool + Send),
    ) -> Result<bool> {
        let mut page = bounds.clone();
        let mut resume: Option<Vec<u8>> = None;
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
            let mut last = None;
            for r in f.records {
                if resume.as_deref() == Some(r.key.as_slice()) {
                    continue;
                }
                if r.envelope.is_none() {
                    return Err(abort(VerifyAbort::Upstream(
                        "FetchRange record without envelope".into(),
                    )));
                }
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
```

In `src/slave/verifier.rs`, replace:

```rust
                RespKind::Hash(h) => matched.push(h.matched),
                RespKind::List(_) => {
                    return Err(abort(VerifyAbort::Upstream(
                        "a listing answered a hash request".into(),
                    )));
```

with:

```rust
                RespKind::Hash(h) => matched.push(h.matched),
                _ => {
                    return Err(abort(VerifyAbort::Upstream(
                        "unexpected answer to HashRange".into(),
                    )));
```

In `src/slave/verifier.rs`, replace:

```rust
            RespKind::List(l) => Ok((l.keys, l.truncated)),
            RespKind::Hash(_) => Err(abort(VerifyAbort::Upstream(
                "a hash answered a listing request".into(),
            ))),
```

with:

```rust
            RespKind::List(l) => Ok((l.keys, l.truncated)),
            _ => Err(abort(VerifyAbort::Upstream(
                "unexpected answer to ListRange".into(),
            ))),
```

In `src/slave/verifier.rs`, replace:

```rust
    #[test]
    fn answers_are_sized_for_two_maximal_keys() {
```

with:

```rust
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

    #[test]
    fn answers_are_sized_for_two_maximal_keys() {
```

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib server::verify && cargo test --lib verify::scan && cargo test --lib slave::verifier`
Expected: PASS, including `fetches_and_entry_crcs_answer_at_the_snapshot_seq` and `fetch_range_returns_whole_envelopes_in_pages`.

Run: `cargo test --test verify --test integrity`
Expected: PASS (verify itself is unchanged).

- [ ] **Step 6: Commit**

```bash
cargo fmt
git add proto src PROTOCOL.md
git commit -m "feat: FetchKeys, FetchRange and EntryCrcs on verify sessions for repair

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Divergence repair and `MismatchPolicy::Repair`

**Files:**
- Create: `src/store/repair.rs`, `src/slave/repair.rs`, `tests/repair.rs`
- Modify: `src/store/{mod,meta,bootstrap}.rs`, `src/slave/{mod,inner,config,policy,driver,repoint,apply,bootstrap,source,verifier}.rs`, `src/source.rs`, `src/failpoint.rs`, `src/verify/mod.rs`, `src/listener.rs`, `proto/replication.proto`, `tests/support/mod.rs`, `tests/repoint.rs`, `tests/failpoints.rs`, `DESIGN.md` (§3.2, §11, §12, §12.1 new, §13, §13.1, §15, §16, §16.3, §18, §19, §21, §22), `PROTOCOL.md` (§6.5, §11.2, §12, §14), `docs/superpowers/specs/2026-10-09-failover-acks-repair.md`, `docs/superpowers/specs/2026-10-09-plan2-3-design-notes.md`

Spec addendum §5, with the epoch nonces of Task 1. A slave whose log holds entries `F..=L` its upstream's history does not have (`DIVERGED`) can, instead of a full bootstrap, take **only the keys those entries touched** from the upstream's snapshot at `S`, drop `F..=L`, set `last_seq = F − 1` in the same synced batch, and tail from `F`. Replaying `F..=S` (absolute ops only) over the repaired keys gives the upstream's exact state at `S`. The same machinery repairs the keys a verify reports (`MismatchPolicy::Repair`).

Design points (each pinned by a test):
- **The fork comes from the epoch histories** (`types::fork_from` over the slave's newest records and the upstream's history, nonces included): within one epoch there is one writer process, so entries before `F` are identical, and a slave that does not even hold entry `F − 1` (bootstrapped past it) can repair. The CRC of entry `F − 1` is compared where both sides hold it, as a second line of defence. No CRC binary search is needed. A slave halted `Diverged` records `F` the same way (`__meta.diverged_at`, for `Slave::diverged_entries`), without asking the upstream.
- **Two phases.** The network phase (read the suffix, open a verify session, fetch envelopes) is bounded by `REPAIR_TIMEOUT` (120 s) and abandoned on a repoint; the commit (one synced batch, then the in-memory identity and position) runs to completion once started, so the in-memory state always follows the batch.
- **`NotReady` until replay passes `S`**: the batch writes `__meta.repair_until = S`, a key of its own. A halt during the replay rewrites `node_state` but cannot clear it, so the mixed state is never served, also after a restart. Reads, snapshots, promotions, downstream handshakes and verify captures (target and after-commit snapshots answer `Cancelled`) all see `is_ready() == false`; `Status` reports `repairing:<S>`.
- **Bounded memory and requests**: the suffix is read with `repair_max_bytes` as its byte limit; fetches go through a sink that counts the same budget and stops early; local keys of cleared ranges are counted before the batch is built; `FetchKeys` requests carry at most 1 MiB of keys; a key over 1 MiB or a range bound over 2 MiB falls back. Deterministic problems (a bad envelope, an undecodable suffix entry, an oversized key) fall back to a bootstrap instead of retrying forever.
- **Verify repair never drops unreported entries**: with `S < L` (the upstream is behind) it retries later. The pending set stays queued until the commit, so an interrupted repair is retried.

`repoint(.., RepointDivergence::Repair { accept_loss })` now works (it returned `Error::Unsupported` in Plan 2) and settles `Repaired { discarded }`; `DivergencePolicy::Repair` does the same on reconnects.

**Interfaces:**
- Consumes: Task 1 `types::{fork_from, EpochStart}`, `net::convert::epochs_from_pb`, `Inner::known_epochs`; Task 2 `Slave::wait_streaming`; Task 9 `UpstreamSession::{open(&Inner, &Client, VerifyMode, Duration), close}`, `Upstream::{fetch_keys, fetch_range, entry_crc}` with their sinks, `verifier::FETCH_REQUEST_BYTES`; Task 5 `Inner::{cancel_capture, capture_after_commit, rebootstrap}`, `CaptureHold::target`, `verifier::finish`; Task 3 `verify::range::{Bounds, KeyOrder}`, `Mismatch`, `MAX_RESUME_KEY_BYTES`; Plan 2 `Store::{entry_crc, read_entries(from, max, max_bytes), live_ctx, handle, cf, write_meta, set_position, set_first_retained_seq, first_changelog_seq, tuning}` and `ReadCtx::iter_raw_from` (through `live_ctx(0)`), `meta::{encode_identity, encode_epoch_history, be64}`, `net::convert::{cf_from_pb, checked_history, identity_from_pb}`, `driver::{session, end_for, status_end, unless_repointed, commit_halt_clear}`, `Inner::{settle_repoint, fail_repoint_on_halt, force_bootstrap, halt_clear_pending, reset_downstream, set_ident, seq_tx, notify}`, `repoint::divergence`, `background::blocking`, `failpoint::fire`; harness `Cluster::{master, master_config, slave_config, restart_master_after_backup, path}`, `restore_dir`, `RawDb`, `RecordingListener`, `wait_state`, `wait_until`, `assert_converged`.
- Produces:
  - `store::repair::{Rewind { keep_through, keep_crc, drop_through, identity, epoch_history, repair_until: Option<Seq>, clear, writes }, KeyWrite}`, `Store::{rewind, range_bytes(&[(Arc<PhysCf>, Bounds)], limit) -> Result<Option<u64>>, repair_until, clear_repair_until, set_diverged_at, diverged_at}`, `meta::key::{DIVERGED_AT, REPAIR_UNTIL}`.
  - `slave::repair::{AffectedSet { keys, ranges } (from_entries, from_mismatches, merge), RepairError::{Fallback, Retry}, Repaired { discarded, rewound_to, ready_at }, Prepared, REPAIR_TIMEOUT, history_fork, record_fork, prepare_divergence, prepare_pending, commit}`; failpoint `slave-repair-after-rewind`.
  - `Inner::{repairing, begin_repair, finish_repair_at, request_repair, pending_repair, take_pending_repair, has_pending_repair}`; `Inner::is_ready` is false while repairing; `driver::{run_repair, repair_failed, repair_span}`.
  - Public: `DivergencePolicy::Repair`, `MismatchPolicy::Repair`, `SlaveConfig::repair_max_bytes: u64` (default 64 MiB, must be positive), `Slave::diverged_entries(&self) -> Result<Vec<LogEntry>>`, `ReplicationListener::on_rewind(&self, last_seq: Seq)`, `RepointOutcome::Repaired { discarded }` is now returned; `policy::Action::Repair`; `repoint::mode_error` is removed.
  - Harness: `RecordingListener::rewinds() -> usize`, `RawDb::last_seq() -> u64`.

- [ ] **Step 1: Write the failing tests**

In `tests/support/mod.rs`, replace:

```rust
    pub verifies: Mutex<Vec<VerifyReport>>,
    pub violations: Mutex<Vec<String>>,
```

with:

```rust
    pub verifies: Mutex<Vec<VerifyReport>>,
    pub rewinds: AtomicUsize,
    pub violations: Mutex<Vec<String>>,
```

In `tests/support/mod.rs`, replace:

```rust
        self.bootstraps.load(Ordering::SeqCst)
    }
```

with:

```rust
        self.bootstraps.load(Ordering::SeqCst)
    }

    pub fn rewinds(&self) -> usize {
        self.rewinds.load(Ordering::SeqCst)
    }
```

In `tests/support/mod.rs`, replace:

```rust
            .push(report.clone());
    }
}
```

with:

```rust
            .push(report.clone());
    }

    fn on_rewind(&self, last_seq: Seq) {
        self.last.store(last_seq, Ordering::SeqCst);
        self.rewinds.fetch_add(1, Ordering::SeqCst);
    }
}
```

In `tests/support/mod.rs`, replace:

```rust

    /// The stored envelope.
```

with:

```rust

    /// `__meta.last_seq` as stored: the node's durable position.
    pub fn last_seq(&self) -> u64 {
        let meta = self.db.cf_handle("__meta").unwrap();
        let raw = self
            .db
            .get_cf(&meta, b"last_seq")
            .unwrap()
            .unwrap_or_default();
        raw.try_into().map(u64::from_be_bytes).unwrap_or(0)
    }

    /// The stored envelope.
```

Create `tests/repair.rs`:

```rust
//! Repair, end to end (DESIGN.md §12.1): a diverged slave, or one a verify found damaged,
//! takes only the affected keys from its upstream instead of bootstrapping.

mod support;

use std::time::Duration;

use replicated_rocksdb::codec::envelope;
use replicated_rocksdb::{
    CfSpec, DivergencePolicy, Error, HaltReason, LogApi, Master, MismatchPolicy, ReadApi, Slave,
    SlaveConfig, SlaveState, VerifyMode,
};
use support::{
    Cluster, RawDb, RecordingListener, assert_converged, restore_dir, wait_state, wait_until,
};

fn key(i: u32) -> Vec<u8> {
    format!("k{i:03}").into_bytes()
}

/// A master with 50 keys and a converged slave `d`, bootstrapped after them: its log holds no
/// entry before a later fork point, and the epoch histories locate the fork anyway.
async fn setup(
    c: &Cluster,
    tweak: impl FnOnce(&mut SlaveConfig),
) -> (
    Master,
    Slave,
    SlaveConfig,
    std::sync::Arc<RecordingListener>,
) {
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..50u32 {
        a.put("c", &key(i), b"base").unwrap();
    }
    let (mut cfg, listener) = c.slave_config("d", a.serve_addr().unwrap());
    tweak(&mut cfg);
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    wait_state(&d, "streaming", |s| *s == SlaveState::Streaming).await;
    (a, d, cfg, listener)
}

/// Backs the master up, lets `write_lost` write entries the slave receives, then restores the
/// backup with an epoch bump (DESIGN.md §13): those entries are the slave's divergent suffix.
/// `write_new` writes the restored master's own history.
async fn diverge(
    c: &Cluster,
    a: Master,
    d: &Slave,
    write_lost: impl FnOnce(&Master),
    write_new: impl FnOnce(&Master),
) -> Master {
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    write_lost(&a);
    assert_converged(&a, &[("d", d)]).await;
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut cfg = c.master_config("a");
    cfg.serve.as_mut().unwrap().port = port;
    cfg.bump_epoch_on_open = true;
    let a = Master::open(cfg).await.unwrap();
    write_new(&a);
    a
}

fn lost(a: &Master) {
    a.put("c", &key(1), b"lost").unwrap();
    a.put("c", b"only-in-the-lost-history", b"x").unwrap();
    a.delete("c", &key(2)).unwrap();
    a.delete_range("c", &key(10), &key(20)).unwrap();
}

fn new_history(a: &Master) {
    a.put("c", &key(3), b"new").unwrap();
    a.delete("c", &key(15)).unwrap();
    a.put("c", b"only-in-the-new-history", b"y").unwrap();
}

/// Puts, a delete and a range delete in the divergent suffix; the slave rewrites only those
/// keys, drops the suffix, replays the new history and matches the master exactly, with no
/// second bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_diverged_slave_repairs_without_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, _, ld) = setup(&c, |cfg| cfg.on_divergence = DivergencePolicy::Repair).await;
    let a = diverge(&c, a, &d, lost, new_history).await;
    wait_until("the divergence is repaired", || ld.rewinds() == 1).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(d.get("c", b"only-in-the-lost-history").unwrap(), None);
    assert_eq!(
        d.get("c", &key(12)).unwrap(),
        Some(b"base".to_vec()),
        "the lost range delete is undone"
    );
    assert_eq!(d.get("c", &key(1)).unwrap(), Some(b"base".to_vec()));
    assert_eq!(d.status().origin_identity, Some(a.identity()));
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    ld.assert_no_violations();
}

/// Under the default `Halt` policy the data stays as it was, readable, and the divergent
/// suffix can be read back for salvage.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_halted_diverged_slave_lists_its_divergent_entries() {
    let c = Cluster::new();
    let (a, d, _, _) = setup(&c, |_| {}).await;
    assert!(
        matches!(d.diverged_entries(), Err(Error::Config(_))),
        "not halted"
    );
    let fork = a.last_seq() + 1;
    let a = diverge(&c, a, &d, lost, new_history).await;
    wait_state(&d, "Diverged halt", |s| {
        *s == SlaveState::Halted(HaltReason::Diverged)
    })
    .await;
    let suffix = d.diverged_entries().unwrap();
    assert_eq!(
        suffix.iter().map(|e| e.seq).collect::<Vec<_>>(),
        (fork..fork + 4).collect::<Vec<_>>()
    );
    assert_eq!(
        d.get("c", b"only-in-the-lost-history").unwrap(),
        Some(b"x".to_vec()),
        "data intact and readable"
    );
    drop(a);
}

/// A CF created in the divergent suffix cannot be undone key by key: the slave bootstraps.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cf_operation_in_the_suffix_falls_back_to_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, _, ld) = setup(&c, |cfg| cfg.on_divergence = DivergencePolicy::Repair).await;
    let a = diverge(
        &c,
        a,
        &d,
        |a| {
            a.create_cf(CfSpec::new("extra")).unwrap();
            a.put("extra", b"k", b"v").unwrap();
        },
        new_history,
    )
    .await;
    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.rewinds(), 0);
}

/// The slave's own retention removed its divergent suffix: it cannot know what to repair.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trimmed_local_suffix_falls_back_to_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, _, ld) = setup(&c, |cfg| {
        cfg.on_divergence = DivergencePolicy::Repair;
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    let a = diverge(&c, a, &d, lost, new_history).await;
    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.rewinds(), 0);
}

/// The upstream no longer holds the fork point: it cannot be proven where the histories
/// split.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upstream_without_the_fork_point_falls_back_to_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, d_cfg, ld) = setup(&c, |cfg| cfg.on_divergence = DivergencePolicy::Repair).await;
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    lost(&a);
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut cfg = c.master_config("a");
    cfg.serve.as_mut().unwrap().port = port;
    cfg.bump_epoch_on_open = true;
    cfg.retention.max_entries = Some(1);
    cfg.retention.check_interval = Duration::from_millis(20);
    let a = Master::open(cfg).await.unwrap();
    new_history(&a);
    let before = a.last_seq();
    wait_until("the restored master trims its log", || {
        a.first_retained_seq() == before
    })
    .await;
    let d = Slave::open(d_cfg).await.unwrap();
    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.rewinds(), 0);
}

/// `MismatchPolicy::Repair`: the keys a verify reports — corrupted, missing, extra — are
/// taken from the upstream; no bootstrap, and the next verify is clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_mismatches_are_repaired_from_the_upstream() {
    let c = Cluster::new();
    let (a, d, cfg, ld) = setup(&c, |cfg| {
        cfg.verify.on_mismatch = MismatchPolicy::Repair;
        cfg.verify.chunk_keys = 8;
        cfg.verify.min_chunk_keys = 2;
    })
    .await;
    d.shutdown().await.unwrap();
    {
        let raw = RawDb::open(&cfg.path);
        raw.corrupt_value("c", &key(40));
        raw.delete("c", &key(3));
        raw.put("c", b"k020x", &envelope::encode(7, None, b"stray"));
    }
    let d = Slave::open(cfg).await.unwrap();
    let report = d.verify_now(VerifyMode::Full).await.unwrap();
    assert_eq!(report.mismatches.len(), 3, "{:?}", report.mismatches);
    wait_until("the reported keys are repaired", || ld.rewinds() == 1).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert!(d.verify_now(VerifyMode::Full).await.unwrap().is_clean());
    assert_eq!(ld.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(d.get("c", &key(3)).unwrap(), Some(b"base".to_vec()));
    assert_eq!(d.get("c", b"k020x").unwrap(), None);
}

/// A divergent suffix larger than `repair_max_bytes` is not read into memory: bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn more_than_repair_max_bytes_falls_back_to_a_bootstrap() {
    let c = Cluster::new();
    let (a, d, _, ld) = setup(&c, |cfg| {
        cfg.on_divergence = DivergencePolicy::Repair;
        cfg.repair_max_bytes = 4 << 10;
    })
    .await;
    let a = diverge(
        &c,
        a,
        &d,
        |a| {
            for i in 0..8u32 {
                a.put("c", &key(i), &[7u8; 1024]).unwrap();
            }
        },
        new_history,
    )
    .await;
    wait_until("the fallback bootstrap", || ld.bootstraps() == 2).await;
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(ld.rewinds(), 0);
}
```

In `tests/repoint.rs`, replace:

```rust

    let repair = RepointOptions {
```

with:

```rust

    let d_last = d.last_seq();
    assert_eq!(
        d.diverged_entries()
            .unwrap()
            .iter()
            .map(|e| e.seq)
            .collect::<Vec<_>>(),
        (fork..=d_last).collect::<Vec<_>>(),
        "the divergent suffix can be salvaged"
    );

    let repair = RepointOptions {
```

In `tests/repoint.rs`, replace:

```rust
    };
    assert!(matches!(
        repoint(&d, b_up.clone(), repair).await,
        Err(Error::Unsupported(_))
    ));
    assert_eq!(d.status().state, SlaveState::Halted(HaltReason::Diverged));
    assert_eq!(d.get("c", b"also only on d").unwrap(), Some(b"3".to_vec()));

    let rebootstrap = RepointOptions {
        on_divergence: RepointDivergence::Rebootstrap {
            accept_loss: AcceptDataLoss,
        },
    };
    assert_eq!(
        repoint(&d, b_up, rebootstrap).await.unwrap(),
        RepointOutcome::Bootstrapped
    );
```

with:

```rust
    };
    assert_eq!(
        repoint(&d, b_up, repair).await.unwrap(),
        RepointOutcome::Repaired {
            discarded: fork..=d_last
        }
    );
```

In `tests/repoint.rs`, replace:

```rust
    );
    assert_eq!(ld.bootstraps(), 2);
}
```

with:

```rust
    );
    assert_eq!(d.get("c", b"k").unwrap(), Some(b"1".to_vec()));
    assert_eq!(ld.bootstraps(), 1, "repaired without a bootstrap");
    assert_eq!(ld.rewinds(), 1);
    assert!(
        d.verify_now(replicated_rocksdb::VerifyMode::Full)
            .await
            .unwrap()
            .is_clean()
    );
}
```

Append to `tests/failpoints.rs`:

```rust
/// A slave killed right after its repair committed holds keys at the upstream's snapshot `S`
/// next to keys at the fork: it stays `NotReady` until replay passes `S` — after a restart with
/// no upstream (`node_state` `repairing:<S>`), and after a halt that rewrote `node_state`
/// during the replay — then converges, without any bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_repair_then_converge() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, first) = c.slave_config("d", a.serve_addr().unwrap());
    cfg.on_divergence = replicated_rocksdb::DivergencePolicy::Repair;
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    for i in 0..20u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    assert_converged(&a, &[("d", &d)]).await;

    // D receives entries the restored master will not have.
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    let fork = a.last_seq() + 1;
    a.put("c", &[1], b"lost").unwrap();
    a.delete("c", &[2]).unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let a = Master::open(mcfg).await.unwrap();
    for i in 0..5u8 {
        a.put("c", &[100 + i], b"new").unwrap(); // so replay must run past the rewind point
    }
    let s = a.last_seq();

    fail::cfg("slave-repair-after-rewind", "1*return").unwrap();
    let listener = std::sync::Arc::new(support::RecordingListener::default());
    let mut frozen_cfg = cfg.clone();
    frozen_cfg.listener = Some(listener.clone());
    let d = Slave::open(frozen_cfg).await.unwrap();
    wait_until("frozen right after the rewind", || listener.rewinds() == 1).await;
    assert!(
        matches!(d.get("c", &[0]), Err(Error::NotReady)),
        "mixed state is never served"
    );
    d.kill().await.unwrap();
    fail::remove("slave-repair-after-rewind");
    assert_eq!(listener.bootstraps(), 0);
    assert_eq!(
        support::RawDb::open(&cfg.path).last_seq(),
        fork - 1,
        "rewound to the fork"
    );

    // Restarted with no reachable upstream: still not ready, and Status says why.
    let dead = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let (mut lonely, _) = c.slave_config("d", dead);
    lonely.listener = None;
    let d = Slave::open(lonely).await.unwrap();
    assert!(matches!(d.get("c", &[0]), Err(Error::NotReady)));
    assert_eq!(d.promotion_info().node_state, format!("repairing:{s}"));
    d.shutdown().await.unwrap();

    // A halt that persists (another database: IdentityChanged) rewrites node_state; the
    // repair target survives it, also across a restart.
    let x = c.master("x").await;
    let (mut foreign, _) = c.slave_config("d", x.serve_addr().unwrap());
    foreign.listener = None;
    let d = Slave::open(foreign).await.unwrap();
    d.wait_streaming(support::WAIT).await.unwrap_err();
    assert!(matches!(d.get("c", &[0]), Err(Error::NotReady)));
    d.shutdown().await.unwrap();
    let d = Slave::open(cfg).await.unwrap();
    assert_eq!(
        d.status().state,
        SlaveState::Halted(replicated_rocksdb::HaltReason::IdentityChanged)
    );
    assert!(
        matches!(d.get("c", &[0]), Err(Error::NotReady)),
        "a halted slave mid-replay is not ready either"
    );
    d.clear_halt(replicated_rocksdb::HaltAction::Retry)
        .await
        .unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(d.get("c", &[1]).unwrap(), Some(b"base".to_vec()));
    assert_eq!(first.bootstraps(), 1, "only the initial bootstrap, ever");
    assert!(
        d.verify_now(replicated_rocksdb::VerifyMode::Full)
            .await
            .unwrap()
            .is_clean()
    );
    drop(x);
    scenario.teardown();
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --test repair --test repoint`
Expected: FAIL to compile: `no variant named Repair` in `DivergencePolicy` and `MismatchPolicy`, `no method named diverged_entries`, `no field repair_max_bytes`, `on_rewind is not a member of trait ReplicationListener`.

- [ ] **Step 3: The store side — an atomic rewind, the repair target, the fork point**

In `src/store/meta.rs`, replace:

```rust
    pub const MAX_BURNED_EPOCH: &[u8] = b"max_burned_epoch";
}
```

with:

```rust
    pub const MAX_BURNED_EPOCH: &[u8] = b"max_burned_epoch";
    /// The first seq of the local divergent suffix, recorded when a slave halts `Diverged`
    /// (`Slave::diverged_entries`). Cleared by a repair or a bootstrap.
    pub const DIVERGED_AT: &[u8] = b"diverged_at";
    /// While a repair replays (DESIGN.md §12.1): reads answer `NotReady` until `last_seq`
    /// reaches this seq. Kept apart from `node_state`, which a halt rewrites.
    pub const REPAIR_UNTIL: &[u8] = b"repair_until";
}
```

In `src/store/bootstrap.rs`, replace:

```rust
                key::MAX_ENTRY_BYTES,
            ] {
```

with:

```rust
                key::MAX_ENTRY_BYTES,
                key::DIVERGED_AT,
                key::REPAIR_UNTIL,
            ] {
```

In `src/store/mod.rs`, replace:

```rust
pub(crate) mod read;
pub(crate) mod repl;
```

with:

```rust
pub(crate) mod read;
pub(crate) mod repair;
pub(crate) mod repl;
```

Create `src/store/repair.rs`:

```rust
//! Store side of repair (DESIGN.md §12.1): rewinding a slave to a common seq in one synced
//! batch, with the affected keys set to the upstream's state; the persisted repair target
//! (`__meta.repair_until`); and the recorded fork point of a divergence.

use std::sync::Arc;

use rocksdb::{WriteBatch, WriteOptions};

use super::bootstrap::NODE_READY;
use super::cfs::PhysCf;
use super::meta::{self, CF_CHANGELOG, CF_META, key};
use super::{Position, Store};
use crate::error::Result;
use crate::types::{DbIdentity, EpochStart, Seq};
use crate::verify::range::{Bounds, KeyOrder};

/// A key to set to the upstream's envelope, or to delete (`None`).
pub(crate) type KeyWrite = (Arc<PhysCf>, Vec<u8>, Option<Vec<u8>>);

/// Everything one repair commits atomically.
pub(crate) struct Rewind {
    /// The new `last_seq`: the last entry both sides share.
    pub keep_through: Seq,
    /// `crc32c` of entry `keep_through` (`None` when it is 0).
    pub keep_crc: Option<u32>,
    /// The old `last_seq`: local log entries `keep_through + 1 ..= drop_through` are removed.
    pub drop_through: Seq,
    pub identity: DbIdentity,
    pub epoch_history: Vec<EpochStart>,
    /// Reads answer `NotReady` until `last_seq` reaches this seq again (`None`: ready now).
    /// Persisted as `__meta.repair_until`, apart from `node_state`, so a halt during the
    /// replay (which rewrites `node_state`) cannot make the node ready too early.
    pub repair_until: Option<Seq>,
    /// Every local key in these ranges is deleted first.
    pub clear: Vec<(Arc<PhysCf>, Bounds)>,
    /// Then these keys are set to the upstream's envelope, or deleted (`None`).
    pub writes: Vec<KeyWrite>,
}

impl Store {
    /// Commits a repair in one synced batch. The caller must have stopped applying entries.
    pub(crate) fn rewind(&self, r: &Rewind) -> Result<()> {
        let mut batch = WriteBatch::default();
        for (cf, bounds) in &r.clear {
            let order = KeyOrder::new(
                &self.tuning().comparators,
                cf.entry.spec.comparator.as_deref(),
            );
            let handle = self.handle(cf)?;
            for item in self
                .live_ctx(0)
                .iter_raw_from(cf.clone(), bounds.lower.as_deref())?
            {
                let (k, _) = item?;
                if !bounds.below_upper(&order, &k) {
                    break;
                }
                batch.delete_cf(&handle, &k);
            }
        }
        for (cf, k, envelope) in &r.writes {
            let handle = self.handle(cf)?;
            match envelope {
                Some(v) => batch.put_cf(&handle, k, v),
                None => batch.delete_cf(&handle, k),
            }
        }
        if r.drop_through > r.keep_through {
            let from = (r.keep_through + 1).to_be_bytes();
            let to = (r.drop_through + 1).to_be_bytes();
            batch.delete_range_cf(&self.cf(CF_CHANGELOG), &from[..], &to[..]);
        }
        let meta_cf = self.cf(CF_META);
        batch.put_cf(&meta_cf, key::LAST_SEQ, r.keep_through.to_be_bytes());
        match r.keep_crc {
            Some(crc) => batch.put_cf(&meta_cf, key::LAST_ENTRY_CRC, crc.to_be_bytes()),
            None => batch.delete_cf(&meta_cf, key::LAST_ENTRY_CRC),
        }
        batch.put_cf(
            &meta_cf,
            key::ORIGIN_IDENTITY,
            meta::encode_identity(&r.identity),
        );
        batch.put_cf(
            &meta_cf,
            key::EPOCH_HISTORY,
            meta::encode_epoch_history(&r.epoch_history),
        );
        // The divergence is resolved: a persisted `Diverged` halt goes too.
        batch.put_cf(&meta_cf, key::NODE_STATE, NODE_READY);
        match r.repair_until {
            Some(seq) => batch.put_cf(&meta_cf, key::REPAIR_UNTIL, seq.to_be_bytes()),
            None => batch.delete_cf(&meta_cf, key::REPAIR_UNTIL),
        }
        batch.delete_cf(&meta_cf, key::DIVERGED_AT);
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        self.set_position(Position {
            last_seq: r.keep_through,
            last_entry_crc: r.keep_crc,
        });
        let first = self.first_changelog_seq()?.unwrap_or(r.keep_through + 1);
        self.set_first_retained_seq(first);
        Ok(())
    }

    /// The bytes of local keys (keys and envelopes) in these ranges, or `None` once more than
    /// `limit` (a repair that would delete that much falls back to a bootstrap).
    pub(crate) fn range_bytes(
        &self,
        ranges: &[(Arc<PhysCf>, Bounds)],
        limit: u64,
    ) -> Result<Option<u64>> {
        let mut bytes = 0u64;
        for (cf, bounds) in ranges {
            let order = KeyOrder::new(
                &self.tuning().comparators,
                cf.entry.spec.comparator.as_deref(),
            );
            for item in self
                .live_ctx(0)
                .iter_raw_from(cf.clone(), bounds.lower.as_deref())?
            {
                let (k, v) = item?;
                if !bounds.below_upper(&order, &k) {
                    break;
                }
                bytes += (k.len() + v.len()) as u64;
                if bytes > limit {
                    return Ok(None);
                }
            }
        }
        Ok(Some(bytes))
    }

    /// `__meta.repair_until`: the seq replay must reach before the node is ready again.
    pub(crate) fn repair_until(&self) -> Result<Option<Seq>> {
        let raw = self.db().get_cf(&self.cf(CF_META), key::REPAIR_UNTIL)?;
        raw.map(|v| meta::be64(&v)).transpose()
    }

    /// Forgets `__meta.repair_until` (replay reached it). Synced.
    pub(crate) fn clear_repair_until(&self) -> Result<()> {
        self.delete_meta_synced(key::REPAIR_UNTIL)
    }

    fn delete_meta_synced(&self, k: &[u8]) -> Result<()> {
        let mut batch = WriteBatch::default();
        batch.delete_cf(&self.cf(CF_META), k);
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        Ok(self.db().write_opt(batch, &wo)?)
    }

    /// Records (or, with `None`, forgets) where the local divergent suffix starts. Synced.
    pub(crate) fn set_diverged_at(&self, fork: Option<Seq>) -> Result<()> {
        match fork {
            Some(seq) => self.write_meta(&[(key::DIVERGED_AT, &seq.to_be_bytes())], true),
            None => self.delete_meta_synced(key::DIVERGED_AT),
        }
    }

    pub(crate) fn diverged_at(&self) -> Result<Option<Seq>> {
        let raw = self.db().get_cf(&self.cf(CF_META), key::DIVERGED_AT)?;
        raw.map(|v| meta::be64(&v)).transpose()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope;
    use crate::read::ReadApi;
    use crate::store::test_support::{apply, open_store};
    use crate::types::{CfSpec, Op};

    fn put(k: &[u8], v: &[u8]) -> Op {
        Op::Put {
            cf: "c".into(),
            key: k.to_vec(),
            value: v.to_vec(),
            expire_at_ms: None,
        }
    }

    #[test]
    fn rewind_sets_keys_drops_the_suffix_and_moves_the_position() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap(); // 1
        apply(
            &store,
            vec![
                put(b"a", b"1"),
                put(b"b", b"1"),
                put(b"r1", b"1"),
                put(b"r2", b"1"),
            ],
        )
        .unwrap(); // 2
        let crc2 = store.entry_crc(2).unwrap();
        apply(&store, vec![put(b"a", b"diverged")]).unwrap(); // 3
        apply(
            &store,
            vec![Op::Delete {
                cf: "c".into(),
                key: b"b".to_vec(),
            }],
        )
        .unwrap(); // 4
        let cf = store.live_cf("c").unwrap();
        let identity = DbIdentity {
            db_uuid: uuid::Uuid::new_v4(),
            epoch: 2,
        };
        let history = vec![
            EpochStart {
                epoch: 1,
                start_seq: 1,
                nonce: 0,
            },
            EpochStart {
                epoch: 2,
                start_seq: 3,
                nonce: 0,
            },
        ];
        let bounds = Bounds {
            lower: Some(b"r".to_vec()),
            upper: Some(b"s".to_vec()),
        };
        store
            .rewind(&Rewind {
                keep_through: 2,
                keep_crc: crc2,
                drop_through: 4,
                identity,
                epoch_history: history.clone(),
                repair_until: Some(9),
                clear: vec![(cf.clone(), bounds)],
                writes: vec![
                    (
                        cf.clone(),
                        b"a".to_vec(),
                        Some(envelope::encode(5, None, b"upstream")),
                    ),
                    (
                        cf.clone(),
                        b"b".to_vec(),
                        Some(envelope::encode(2, None, b"1")),
                    ),
                    (
                        cf.clone(),
                        b"r2".to_vec(),
                        Some(envelope::encode(6, None, b"kept")),
                    ),
                ],
            })
            .unwrap();
        assert_eq!(
            store.position(),
            Position {
                last_seq: 2,
                last_entry_crc: crc2
            }
        );
        assert_eq!(store.first_retained_seq(), 1);
        assert!(
            store.read_entries(3, 10, usize::MAX).unwrap().is_empty(),
            "the suffix is gone"
        );
        let snap = store.read_snapshot(0).unwrap();
        assert_eq!(snap.get("c", b"a").unwrap(), Some(b"upstream".to_vec()));
        assert_eq!(snap.get("c", b"b").unwrap(), Some(b"1".to_vec()));
        assert_eq!(snap.get("c", b"r1").unwrap(), None, "cleared range");
        assert_eq!(
            snap.get_with_meta("c", b"r2").unwrap().unwrap().1.write_seq,
            6
        );
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!((m.identity, m.epoch_history), (Some(identity), history));
        assert_eq!(m.node_state.as_deref(), Some(NODE_READY));
        assert_eq!(store.repair_until().unwrap(), Some(9));
        // a: 1 + 10 + 8, b: 1 + 10 + 1, r2: 2 + 10 + 4 (key, envelope header, value).
        assert_eq!(
            store
                .range_bytes(&[(cf.clone(), Bounds::default())], u64::MAX)
                .unwrap(),
            Some(47)
        );
        assert_eq!(
            store.range_bytes(&[(cf, Bounds::default())], 3).unwrap(),
            None
        );
        store.clear_repair_until().unwrap();
        assert_eq!(store.repair_until().unwrap(), None);
    }

    #[test]
    fn the_fork_point_is_recorded_and_forgotten() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        assert_eq!(store.diverged_at().unwrap(), None);
        store.set_diverged_at(Some(42)).unwrap();
        assert_eq!(store.diverged_at().unwrap(), Some(42));
        store.set_diverged_at(None).unwrap();
        assert_eq!(store.diverged_at().unwrap(), None);
    }
}
```

Run: `cargo test --lib store::repair`
Expected: PASS (2 tests).

- [ ] **Step 4: The new policies, the setting and the listener callback**

In `src/slave/config.rs`, replace:

```rust

/// What to do when the upstream reports `DIVERGED`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DivergencePolicy {
    /// Halt with `Diverged` (persisted).
    #[default]
    Halt,
    Rebootstrap,
```

with:

```rust

/// What to do when the upstream reports `DIVERGED` on a reconnect (an explicit
/// `Slave::repoint` uses `RepointOptions::on_divergence` instead).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DivergencePolicy {
    /// Halt with `Diverged` (persisted). The data stays readable; `Slave::diverged_entries`
    /// returns the divergent suffix for salvage.
    #[default]
    Halt,
    /// Discard only the divergent suffix (DESIGN.md §12.1): the keys it touched are taken
    /// from the upstream, the suffix is dropped, and the slave replays the upstream's history.
    /// Falls back to a bootstrap when that cannot be proven safe.
    Repair,
    /// Discard all local data and bootstrap.
    Rebootstrap,
```

In `src/slave/config.rs`, replace:

```rust
    pub fetch: FetchConfig,
    /// Fsync every applied entry before acknowledging it. Default `false`: an ack then means
```

with:

```rust
    pub fetch: FetchConfig,
    /// A repair (DESIGN.md §12.1) that would fetch more than this many bytes of keys and
    /// envelopes falls back to a bootstrap. Default 64 MiB.
    pub repair_max_bytes: u64,
    /// Fsync every applied entry before acknowledging it. Default `false`: an ack then means
```

In `src/slave/config.rs`, replace:

```rust
            fetch: FetchConfig::default(),
            sync_writes: false,
```

with:

```rust
            fetch: FetchConfig::default(),
            repair_max_bytes: 64 << 20,
            sync_writes: false,
```

In `src/slave/config.rs`, replace:

```rust
                "fetch.max_batch_bytes must be positive".into(),
            ));
        }
        if self.heartbeat.is_zero()
```

with:

```rust
                "fetch.max_batch_bytes must be positive".into(),
            ));
        }
        if self.repair_max_bytes == 0 {
            return Err(Error::Config("repair_max_bytes must be positive".into()));
        }
        if self.heartbeat.is_zero()
```

In `src/slave/config.rs`, replace:

```rust
        assert!(cfg.validate().is_err());
        // Serving: the heartbeat must be at most half of the in-sync window.
```

with:

```rust
        assert!(cfg.validate().is_err());
        let mut cfg = SlaveConfig::new("b", "/tmp/x", up.clone());
        cfg.repair_max_bytes = 0;
        assert!(matches!(cfg.validate(), Err(Error::Config(m)) if m.contains("repair_max_bytes")));
        // Serving: the heartbeat must be at most half of the in-sync window.
```

In `src/verify/mod.rs`, replace:

```rust

/// What a slave does when a verify run finds a difference.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
```

with:

```rust

/// What a slave does when a verify run finds a difference. Not applied while the slave is
/// halted (it may be on another history than its upstream).
///
/// **Verify cannot tell which side is damaged:** `Rebootstrap` and `Repair` assume the upstream
/// is right. In a chain A→B→C, a damaged B copies its damage onto a correct C (and further
/// down). Choosing either is the consent to that; prefer `Report` on nodes whose upstream is
/// not the master, or verify the upstream first.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
```

In `src/verify/mod.rs`, replace:

```rust
    Rebootstrap,
}
```

with:

```rust
    Rebootstrap,
    /// Report it, then take the reported keys and ranges from the upstream (DESIGN.md
    /// §12.1), without a bootstrap. A `CfSpec` mismatch, or a repair that cannot be done,
    /// bootstraps instead.
    Repair,
}
```

In `src/listener.rs`, replace:

```rust
/// Delivery is at-most-once across crashes. Within a process, `on_entry` seqs are strictly
/// increasing between bootstraps; after `on_bootstrap_complete(S)` delivery continues at
/// `S + 1` (state was replaced wholesale, so rebuild derived state). For at-least-once
/// delivery keep your own cursor and use `LogApi::entries_since`.
```

with:

```rust
/// Delivery is at-most-once across crashes. Within a process, `on_entry` seqs are strictly
/// increasing between bootstraps and rewinds; after `on_bootstrap_complete(S)` or
/// `on_rewind(S)` delivery continues at `S + 1` (state was replaced wholesale, or in part, so
/// rebuild derived state). For at-least-once
/// delivery keep your own cursor and use `LogApi::entries_since`.
```

In `src/listener.rs`, replace:

```rust
    fn on_verify_complete(&self, _report: &VerifyReport) {}
}
```

with:

```rust
    fn on_verify_complete(&self, _report: &VerifyReport) {}
    /// A repair (DESIGN.md §12.1) rewrote some keys and moved the applied position back to
    /// `last_seq`. Entries after it are delivered again (`on_entry` continues at
    /// `last_seq + 1`); derived state of the keys involved should be rebuilt from reads once
    /// the slave is ready.
    fn on_rewind(&self, _last_seq: Seq) {}
}
```

In `src/slave/policy.rs`, replace:

```rust
    Bootstrap,
    Halt(HaltReason),
```

with:

```rust
    Bootstrap,
    /// Repair the divergence from the upstream (DESIGN.md §12.1); bootstrap if that fails.
    Repair,
    Halt(HaltReason),
```

In `src/slave/policy.rs`, replace:

```rust
        DivergencePolicy::Halt => Action::Halt(HaltReason::Diverged),
        DivergencePolicy::Rebootstrap => Action::Bootstrap,
```

with:

```rust
        DivergencePolicy::Halt => Action::Halt(HaltReason::Diverged),
        DivergencePolicy::Repair => Action::Repair,
        DivergencePolicy::Rebootstrap => Action::Bootstrap,
```

In `src/slave/policy.rs`, replace:

```rust
            Action::Bootstrap
        );
        assert_eq!(
            act(Decision::Reject, Reason::StaleEpoch, &DEFAULTS),
```

with:

```rust
            Action::Bootstrap
        );
        let repair = Policies {
            divergence: DivergencePolicy::Repair,
            ..DEFAULTS
        };
        assert_eq!(
            act(Decision::Reject, Reason::Diverged, &repair),
            Action::Repair
        );
        assert_eq!(
            act(Decision::Reject, Reason::StaleEpoch, &DEFAULTS),
```

In `src/slave/policy.rs`, replace:

```rust
        );
        for r in [
```

with:

```rust
        );
        let repair = Policies {
            divergence: DivergencePolicy::Repair,
            ..DEFAULTS
        };
        assert_eq!(after_status(&s(Reason::Diverged), &repair), Action::Repair);
        for r in [
```

- [ ] **Step 5: The slave's repair state: not ready while replaying, no capture of a mixed state**

In `src/slave/inner.rs`, replace:

```rust
use super::policy::Policies;
use super::repoint::PendingRepoint;
```

with:

```rust
use super::policy::Policies;
use super::repair::AffectedSet;
use super::repoint::PendingRepoint;
```

In `src/slave/inner.rs`, replace:

```rust
    pub last_applied_ts: AtomicU64,
    /// Wakes the tail loop when a verify asks for a re-bootstrap.
    pub rebootstrap: Notify,
}
```

with:

```rust
    pub last_applied_ts: AtomicU64,
    /// Wakes the tail loop when a verify asks for a re-bootstrap or a repair.
    pub rebootstrap: Notify,
    /// Non-zero while a repair waits for replay to reach this seq: reads answer `NotReady`.
    repair_until: AtomicU64,
    /// Keys a verify asked to repair (`MismatchPolicy::Repair`), done at the next session.
    pending_repair: Mutex<Option<AffectedSet>>,
}
```

In `src/slave/inner.rs`, replace:

```rust
        let last_seq = store.position().last_seq;
        // The entry at last_seq, if still in the log, gives the lag's time base after a restart.
```

with:

```rust
        let last_seq = store.position().last_seq;
        // A repair interrupted before replay reached its target stays not ready.
        // A repair interrupted before replay reached its target stays not ready, also when a
        // halt rewrote `node_state` since (DESIGN.md §12.1).
        let repair_until = store
            .repair_until()?
            .filter(|&until| until > last_seq)
            .unwrap_or(0);
        // The entry at last_seq, if still in the log, gives the lag's time base after a restart.
```

In `src/slave/inner.rs`, replace:

```rust
            rebootstrap: Notify::new(),
        })
```

with:

```rust
            rebootstrap: Notify::new(),
            repair_until: AtomicU64::new(repair_until),
            pending_repair: Mutex::new(None),
        })
```

In `src/slave/inner.rs`, replace:

```rust

    /// Readable and servable: the slave holds a consistent state at some seq.
    pub(crate) fn is_ready(&self) -> bool {
```

with:

```rust

    /// Readable and servable: the slave holds a consistent state at some seq (not during a
    /// bootstrap, nor while a repair replays up to its target).
    pub(crate) fn is_ready(&self) -> bool {
```

In `src/slave/inner.rs`, replace:

```rust
            .is_some()
    }
```

with:

```rust
            .is_some()
            && self.repair_until.load(Ordering::SeqCst) == 0
    }
```

In `src/slave/inner.rs`, replace:

```rust
        let mut slot = self.inner.capture();
        if matches!(*slot, Capture::Cancelled) {
            let _ = tx.send(Err(CaptureMiss::Cancelled));
```

with:

```rust
        let mut slot = self.inner.capture();
        // A repairing slave holds a mix of two seqs until replay reaches the repair's target.
        if matches!(*slot, Capture::Cancelled) || self.inner.repairing().is_some() {
            let _ = tx.send(Err(CaptureMiss::Cancelled));
```

In `src/slave/inner.rs`, replace:

```rust
        let Some((want, tx)) = tx else { return };
        let reply = if want == seq {
            self.store.read_snapshot(0).map_err(|e| {
```

with:

```rust
        let Some((want, tx)) = tx else { return };
        let reply = if self.repairing().is_some() {
            // Replaying after a repair: the state at `seq` is not one the upstream ever had.
            Err(CaptureMiss::Cancelled)
        } else if want == seq {
            self.store.read_snapshot(0).map_err(|e| {
```

In `src/slave/inner.rs`, replace:

```rust
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
```

with:

```rust
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}

impl Inner {
    /// `Some(seq)` while a repair waits for replay to reach `seq` (reads answer `NotReady`).
    pub(crate) fn repairing(&self) -> Option<Seq> {
        match self.repair_until.load(Ordering::SeqCst) {
            0 => None,
            seq => Some(seq),
        }
    }

    /// Refuses reads (and serving) until `ready_at` is applied again; 0 = ready now.
    pub(crate) fn begin_repair(&self, ready_at: Seq) {
        self.repair_until.store(ready_at, Ordering::SeqCst);
    }

    /// Called by the applier after committing `seq`: a repair is complete once replay has
    /// reached its target (DESIGN.md §12.1).
    pub(crate) fn finish_repair_at(&self, seq: Seq) {
        let until = self.repair_until.load(Ordering::SeqCst);
        if until == 0 || seq < until {
            return;
        }
        if let Err(e) = self.store.clear_repair_until() {
            // On restart, a `repair_until` at or below `last_seq` is ignored.
            tracing::warn!(node = %self.cfg.identifier, error = %e, "could not persist the end of a repair");
        }
        self.repair_until.store(0, Ordering::SeqCst);
        tracing::info!(node = %self.cfg.identifier, seq, "repair complete: replay reached the upstream's snapshot");
    }

    /// Asks the driver to repair `set` at its next session (`MismatchPolicy::Repair`).
    pub(crate) fn request_repair(&self, set: AffectedSet) {
        let mut pending = self
            .pending_repair
            .lock()
            .unwrap_or_else(PoisonError::into_inner);
        match pending.as_mut() {
            Some(p) => p.merge(set),
            None => *pending = Some(set),
        }
    }

    /// The keys waiting to be repaired, left in place (a repair takes them only once it
    /// committed, so an interrupted one is retried).
    pub(crate) fn pending_repair(&self) -> Option<AffectedSet> {
        self.pending_repair
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }

    pub(crate) fn take_pending_repair(&self) -> Option<AffectedSet> {
        self.pending_repair
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .take()
    }

    pub(crate) fn has_pending_repair(&self) -> bool {
        self.pending_repair
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }
}
```

In `src/slave/source.rs`, replace:

```rust
                    "ready".into()
                } else {
```

with:

```rust
                    "ready".into()
                } else if let Some(seq) = self.repairing() {
                    format!("repairing:{seq}")
                } else {
```

In `src/source.rs`, replace:

```rust
    fn resets(&self) -> watch::Receiver<u64>;
    /// `ready`, `bootstrapping` or `halted:<Reason>` (the `Status` RPC).
    fn node_state(&self) -> String {
```

with:

```rust
    fn resets(&self) -> watch::Receiver<u64>;
    /// `ready`, `bootstrapping`, `halted:<Reason>` or `repairing:<seq>` (the `Status` RPC).
    fn node_state(&self) -> String {
```

In `src/slave/apply.rs`, replace:

```rust
            .map_err(Failure::Commit)?;
        // Still on the applier's thread, before the next entry can commit: a verify waiting
```

with:

```rust
            .map_err(Failure::Commit)?;
        // A repair is complete once replay reaches its target (DESIGN.md §12.1).
        i.finish_repair_at(e.seq);
        // Still on the applier's thread, before the next entry can commit: a verify waiting
```

In `src/slave/bootstrap.rs`, replace:

```rust
    inner.cancel_capture();
    // The snapshot's last entry is not transferred: its ts_ms is unknown until the next entry.
```

with:

```rust
    inner.cancel_capture();
    // A bootstrap replaces everything a repair would have fixed.
    inner.take_pending_repair();
    inner.begin_repair(0);
    // The snapshot's last entry is not transferred: its ts_ms is unknown until the next entry.
```

In `src/failpoint.rs`, replace:

```rust
//! `slave-commit-io` (a slave's commit of a received entry fails with an I/O error),
//! `slave-before-halt` (a session ended in a halt, before it is persisted; use `cfg_callback`).
//!
```

with:

```rust
//! `slave-commit-io` (a slave's commit of a received entry fails with an I/O error),
//! `slave-before-halt` (a session ended in a halt, before it is persisted; use `cfg_callback`),
//! `slave-repair-after-rewind` (a repair committed its rewind; the driver then freezes).
//!
```

- [ ] **Step 6: The repair itself**

Create `src/slave/repair.rs`:

```rust
//! Repair (DESIGN.md §12.1): bring a slave back in line with its upstream by rewriting only
//! the keys that differ, instead of a full bootstrap.
//!
//! Both kinds run in the driver, while no entry is being applied, in two phases: a network
//! phase that reads the local suffix and fetches the upstream's envelopes over a verify
//! session (bounded in time, abandoned on a repoint), and a local commit that is never
//! cancelled once it starts.
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
//! Anything this cannot prove falls back to a full bootstrap: a CF operation in the suffix, a
//! suffix or fork entry that is no longer retained, histories that do not show the fork, a CF
//! that differs between the sides, an undecodable entry or envelope, a key or range bound too
//! large to fetch, or more than `repair_max_bytes` to read, fetch or delete.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::sync::Arc;
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
use crate::store::repair::{KeyWrite, Rewind};
use crate::types::{CfEntry, DbIdentity, EpochStart, LogEntry, Op, Seq, fork_from};
use crate::verify::range::Bounds;
use crate::verify::{MAX_RESUME_KEY_BYTES, Mismatch, VerifyMode};

/// The network phase of a repair (session, fetches) must finish within this time.
pub(crate) const REPAIR_TIMEOUT: Duration = Duration::from_secs(120);
/// How long `VerifyBegin` may take for a repair session.
const OPEN_TIMEOUT: Duration = Duration::from_secs(10);

/// Why a repair did not happen.
#[derive(Debug)]
pub(crate) enum RepairError {
    /// Repair cannot handle this case: bootstrap instead.
    Fallback(String),
    /// A transient failure (upstream unreachable, session lost): try again later.
    Retry(String),
}

impl From<Error> for RepairError {
    fn from(e: Error) -> Self {
        RepairError::Retry(e.to_string())
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
        Some(set)
    }

    pub(crate) fn merge(&mut self, other: AffectedSet) {
        for (cf, keys) in other.keys {
            self.keys.entry(cf).or_default().extend(keys);
        }
        for (cf, ranges) in other.ranges {
            self.ranges.entry(cf).or_default().extend(ranges);
        }
    }

    fn cfs(&self) -> BTreeSet<&String> {
        self.keys.keys().chain(self.ranges.keys()).collect()
    }
}

fn fallback(why: impl Into<String>) -> RepairError {
    RepairError::Fallback(why.into())
}

/// Where the local log leaves the upstream's history (the first local entry the upstream does
/// not have), from the epoch histories: the local newest records against the upstream's
/// history, nonces included. `None` if the histories do not show a fork at or below the local
/// `last_seq`.
pub(crate) fn history_fork(
    local: &[EpochStart],
    local_last: Seq,
    upstream: &[EpochStart],
) -> Option<Seq> {
    fork_from(upstream, local).filter(|&f| f <= local_last)
}

/// Finds where the local divergent suffix starts and records it for `Slave::diverged_entries`
/// (from the histories; for a slave ahead of its master on the same history, the master's
/// `last_seq + 1`). `None` (recorded as unknown) when neither shows it.
pub(crate) async fn record_fork(inner: &Arc<Inner>, hs: &pb::HandshakeResponse) -> Option<Seq> {
    let last = inner.store.position().last_seq;
    let upstream = epochs_from_pb(&hs.epoch_history);
    let fork = history_fork(&inner.known_epochs(), last, &upstream).or_else(|| {
        (hs.upstream_chain.is_empty() && last > hs.last_seq).then_some(hs.last_seq + 1)
    });
    let i = inner.clone();
    if let Err(e) = blocking(move || i.store.set_diverged_at(fork)).await {
        tracing::warn!(node = %inner.cfg.identifier, error = %e, "could not record the fork point");
    }
    fork
}

/// Everything a repair commits, prepared by the network phase.
pub(crate) struct Prepared {
    rewind: Rewind,
    ready_at: Seq,
    /// True for a verify repair: the pending set is taken once committed.
    pending: bool,
}

/// The local CRC of entry `seq` (a RocksDB read: on a blocking thread).
async fn local_crc(inner: &Arc<Inner>, seq: Seq) -> Result<Option<u32>> {
    let i = inner.clone();
    blocking(move || i.store.entry_crc(seq)).await
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
/// (`REJECT / DIVERGED`).
pub(crate) async fn prepare_divergence(
    inner: &Arc<Inner>,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<Prepared, RepairError> {
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
    let max = usize::try_from(inner.cfg.repair_max_bytes).unwrap_or(usize::MAX);
    let i = inner.clone();
    let suffix = blocking(move || i.store.read_entries(fork, usize::MAX, max))
        .await
        .map_err(|e| match e {
            Error::Gap { .. } => fallback(format!(
                "the local suffix from {fork} is no longer retained"
            )),
            other => RepairError::Retry(other.to_string()),
        })?;
    if suffix.last().map(|e| e.seq) != Some(last) {
        return Err(fallback(
            "the divergent suffix is larger than repair_max_bytes",
        ));
    }
    let set = AffectedSet::from_entries(&suffix)
        .map_err(|e| fallback(format!("an entry of the suffix does not decode: {e}")))?
        .ok_or_else(|| fallback("the divergent suffix creates or drops a column family"))?;
    let mut session = open(inner, client, hs).await?;
    let result = async {
        let s = session.begin.snapshot_seq;
        if s + 1 < fork {
            return Err(RepairError::Retry(format!(
                "the upstream's snapshot {s} is behind the fork point {fork}"
            )));
        }
        // Second line of defence: entry F - 1 must not differ where both sides hold it.
        if fork > 1 {
            let mine = local_crc(inner, fork - 1).await?;
            let theirs = session.upstream.entry_crc(fork - 1).await?;
            if let (Some(a), Some(b)) = (mine, theirs) {
                if a != b {
                    return Err(fallback(format!(
                        "entry {} differs although the histories agree",
                        fork - 1
                    )));
                }
            }
        }
        prepare(inner, hs, &mut session, fork - 1, last, s, set).await
    }
    .await;
    session.close().await;
    result.map(|rewind| Prepared {
        ready_at: rewind.repair_until.unwrap_or(fork - 1),
        rewind,
        pending: false,
    })
}

/// The network phase of repairing the keys a verify reported (`MismatchPolicy::Repair`),
/// taken from the upstream that answered `hs` (`RESUME`). The pending set stays queued until
/// the commit, so an interrupted repair is retried.
pub(crate) async fn prepare_pending(
    inner: &Arc<Inner>,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<Prepared, RepairError> {
    let Some(set) = inner.pending_repair() else {
        return Err(RepairError::Retry("nothing to repair".into()));
    };
    let mut session = open(inner, client, hs).await?;
    let result = async {
        let s = session.begin.snapshot_seq;
        let last = inner.store.position().last_seq;
        if last > s {
            // Repairing would drop entries the verify never reported (DESIGN.md §12.1).
            return Err(RepairError::Retry(format!(
                "the upstream (at {s}) is behind this slave (at {last}); repairing later"
            )));
        }
        prepare(inner, hs, &mut session, last, last, s, set).await
    }
    .await;
    session.close().await;
    result.map(|rewind| Prepared {
        ready_at: rewind.repair_until.unwrap_or(rewind.keep_through),
        rewind,
        pending: true,
    })
}

/// Collects fetched records of one CF: checks every envelope and counts the byte budget,
/// stopping the fetch on a problem.
struct Fetched {
    cf: Arc<crate::store::cfs::PhysCf>,
    bytes: u64,
    max_bytes: u64,
    problem: Option<RepairError>,
    writes: Vec<KeyWrite>,
}

impl Fetched {
    fn take(&mut self, key: Vec<u8>, env: Option<Vec<u8>>) -> bool {
        if let Some(v) = &env {
            if let Err(e) = envelope::decode(v) {
                self.problem = Some(fallback(format!("the upstream sent a bad envelope: {e}")));
                return false;
            }
        }
        self.bytes += (key.len() + env.as_ref().map_or(0, Vec::len)) as u64;
        if self.bytes > self.max_bytes {
            self.problem = Some(fallback(format!(
                "more than repair_max_bytes ({}) to fetch",
                self.max_bytes
            )));
            return false;
        }
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
/// `keep_through` (dropping local entries up to `last`), within `repair_max_bytes`.
async fn prepare(
    inner: &Arc<Inner>,
    hs: &pb::HandshakeResponse,
    session: &mut UpstreamSession,
    keep_through: Seq,
    last: Seq,
    s: Seq,
    set: AffectedSet,
) -> std::result::Result<Rewind, RepairError> {
    let identity = hs
        .identity
        .as_ref()
        .ok_or_else(|| RepairError::Retry("handshake without identity".into()))?;
    let identity: DbIdentity = identity_from_pb(identity)?;
    let history = checked_history(&identity, &hs.epoch_history)?;
    let upstream_cfs: Vec<CfEntry> = session.begin.cf_specs.iter().map(cf_from_pb).collect();
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
            if !session
                .upstream
                .fetch_range(cf, bounds, &mut |k, e| acc.take(k, e))
                .await?
            {
                return Err(acc.stopped());
            }
            clear.push((local.clone(), bounds.clone()));
        }
        fetched = acc.bytes;
        writes.extend(acc.writes);
    }
    // Local keys in the cleared ranges are deleted in the same batch: they count too.
    let i = inner.clone();
    let to_clear = clear.clone();
    let room = max_bytes.saturating_sub(fetched);
    if blocking(move || i.store.range_bytes(&to_clear, room))
        .await?
        .is_none()
    {
        return Err(fallback(format!(
            "more than repair_max_bytes ({max_bytes}) to delete"
        )));
    }
    let keep_crc = match keep_through {
        0 => None,
        k => local_crc(inner, k).await?,
    };
    Ok(Rewind {
        keep_through,
        keep_crc,
        drop_through: last,
        identity,
        epoch_history: history,
        repair_until: (s > keep_through).then_some(s),
        clear,
        writes,
    })
}

/// The local phase: commits a prepared repair in one synced batch. Run it to completion (it is
/// not cancelled by a repoint or a timeout): the in-memory state follows the batch.
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
    let (identity, history) = (rewind.identity, rewind.epoch_history.clone());
    // Readers and downstream replicas must not see the mixed state, and a verify waiting for
    // a seq of the old state can never be served now.
    inner.begin_repair(rewind.repair_until.unwrap_or(0));
    inner.reset_downstream();
    inner.cancel_capture();
    let i = inner.clone();
    if let Err(e) = blocking(move || i.store.rewind(&rewind)).await {
        // The batch is atomic: nothing was written, the old state stands.
        inner.begin_repair(0);
        return Err(e.into());
    }
    if pending {
        inner.take_pending_repair();
    }
    inner.set_ident(Some((identity, history)));
    inner.seq_tx.send_replace(keep_through);
    tracing::info!(
        node = %inner.cfg.identifier,
        rewound_to = keep_through,
        discarded = last.saturating_sub(keep_through),
        ready_at,
        "repaired from the upstream"
    );
    inner.notify(|l| l.on_rewind(keep_through));
    if failpoint::fire("slave-repair-after-rewind") {
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
        assert_eq!(history_fork(&upstream, 15, &upstream), None);
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
        let with_cf_op = vec![
            entry(7, vec![put("a", b"k")]),
            entry(
                8,
                vec![Op::CreateCf {
                    spec: CfSpec::new("n"),
                }],
            ),
        ];
        assert_eq!(AffectedSet::from_entries(&with_cf_op).unwrap(), None);
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
```

In `src/slave/mod.rs`, replace:

```rust
mod promote;
mod repoint;
```

with:

```rust
mod promote;
mod repair;
mod repoint;
```

In `src/slave/mod.rs`, replace:

```rust
use crate::store::read::DbIter;
use crate::types::{CfSpec, Direction, IterMode, Role, Seq, ValueMeta};
use crate::verify::{VerifyMode, VerifyReport};
```

with:

```rust
use crate::store::read::DbIter;
use crate::types::{CfSpec, Direction, IterMode, LogEntry, Role, Seq, ValueMeta};
use crate::verify::{VerifyMode, VerifyReport};
```

In `src/slave/mod.rs`, replace:

```rust
    /// listener's `on_verify_complete`; under `MismatchPolicy::Rebootstrap` a report with
    /// mismatches makes the slave bootstrap. `cfg.verify` sets range sizes, the rate limit and
    /// the policy; `mode` overrides `cfg.verify.mode`. One run at a time: a concurrent call
```

with:

```rust
    /// listener's `on_verify_complete`; under `MismatchPolicy::Rebootstrap` a report with
    /// mismatches makes the slave bootstrap, under `MismatchPolicy::Repair` it takes the
    /// reported keys from the upstream. `cfg.verify` sets range sizes, the rate limit and
    /// the policy; `mode` overrides `cfg.verify.mode`. One run at a time: a concurrent call
```

In `src/slave/mod.rs`, replace:

```rust
        verifier::verify(&self.inner, mode).await
    }
```

with:

```rust
        verifier::verify(&self.inner, mode).await
    }

    /// On a slave halted `Diverged`: its local entries past the fork point, for salvage (DESIGN.md
    /// §12.1). The fork point is recorded when the slave halts, from the epoch histories (or,
    /// for a slave ahead of its master, the master's `last_seq + 1`). `Err(Config)` if the slave
    /// is not halted `Diverged`, or the fork point is not known (neither shows it); `Err(Gap)`
    /// if local retention already removed part of the suffix.
    pub fn diverged_entries(&self) -> Result<Vec<LogEntry>> {
        if self.inner.state() != SlaveState::Halted(HaltReason::Diverged) {
            return Err(Error::Config("the slave is not halted Diverged".into()));
        }
        let fork = self.inner.store.diverged_at()?.ok_or_else(|| {
            Error::Config("the fork point is not known: the epoch histories do not show it".into())
        })?;
        self.inner.store.read_entries(fork, usize::MAX, usize::MAX)
    }
```

- [ ] **Step 7: Wire it into the driver, the verifier and `repoint`**

In `src/slave/driver.rs`, replace:

```rust
use super::policy::{self, Action};
use super::repoint::{self, RepointDivergence, RepointOutcome};
```

with:

```rust
use super::policy::{self, Action};
use super::repair::{self, RepairError};
use super::repoint::{self, RepointDivergence, RepointOutcome};
```

In `src/slave/driver.rs`, replace:

```rust
        }
        Action::Again | Action::Tail | Action::AdoptEpochThenTail => SessionEnd::Again,
    }
```

with:

```rust
        }
        // A failed call carries no handshake to repair from: the next handshake answers
        // `DIVERGED` and repairs.
        Action::Again | Action::Tail | Action::AdoptEpochThenTail | Action::Repair => {
            SessionEnd::Again
        }
    }
}

/// How a repair that did not happen ends the session: a fallback bootstraps (for this repoint
/// generation only), a transient failure is retried with backoff.
fn repair_failed(inner: &Inner, generation: u64, e: RepairError) -> SessionEnd {
    match e {
        RepairError::Fallback(why) => {
            tracing::warn!(node = %inner.cfg.identifier, reason = %why, "repair not possible; bootstrapping instead");
            inner.force_bootstrap.request(generation);
            SessionEnd::Again
        }
        RepairError::Retry(why) => SessionEnd::Retry(format!("repair failed: {why}")),
    }
```

In `src/slave/driver.rs`, replace:

```rust
                RepointDivergence::Rebootstrap { .. } => Action::Bootstrap,
                RepointDivergence::Refuse | RepointDivergence::Repair { .. } => {
                    let err = repoint::mode_error(mode, || {
                        repoint::divergence(
                            &inner.known_epochs(),
                            inner.store.position().last_seq,
                            &hs,
                        )
                    });
                    tracing::warn!(node = %inner.cfg.identifier, error = %err, "repoint refused: diverged");
```

with:

```rust
                RepointDivergence::Rebootstrap { .. } => Action::Bootstrap,
                RepointDivergence::Repair { .. } => Action::Repair,
                RepointDivergence::Refuse => {
                    let err = repoint::divergence(
                        &inner.known_epochs(),
                        inner.store.position().last_seq,
                        &hs,
                    );
                    tracing::warn!(node = %inner.cfg.identifier, error = %err, "repoint refused: diverged");
```

In `src/slave/driver.rs`, replace:

```rust
            commit_halt_clear(inner).await;
            inner.settle_repoint(generation, Ok(RepointOutcome::Resumed));
```

with:

```rust
            commit_halt_clear(inner).await;
            if inner.has_pending_repair() {
                // A verify reported keys to repair (`MismatchPolicy::Repair`): fix them, then
                // handshake again from the new position.
                let fetch = repair::prepare_pending(inner, &c, &hs);
                return match run_repair(inner, generation, fetch).await {
                    Ok(_) => SessionEnd::Again,
                    Err(end) => end,
                };
            }
            inner.settle_repoint(generation, Ok(RepointOutcome::Resumed));
```

In `src/slave/driver.rs`, replace:

```rust
        }
        Action::Halt(r) => SessionEnd::Halt(r),
```

with:

```rust
        }
        Action::Repair => {
            let fetch = repair::prepare_divergence(inner, &c, &hs);
            match run_repair(inner, generation, fetch).await {
                Ok(r) => {
                    // The rewind batch wrote `node_state` itself.
                    inner.halt_clear_pending.store(false, Ordering::SeqCst);
                    inner.settle_repoint(
                        generation,
                        Ok(RepointOutcome::Repaired {
                            discarded: r.discarded,
                        }),
                    );
                    // Handshake again from the rewound position, then tail.
                    SessionEnd::Again
                }
                Err(end) => end,
            }
        }
        Action::Halt(HaltReason::Diverged) => {
            // Where the local suffix starts, for `Slave::diverged_entries`.
            repair::record_fork(inner, &hs).await;
            SessionEnd::Halt(HaltReason::Diverged)
        }
        Action::Halt(r) => SessionEnd::Halt(r),
```

In `src/slave/driver.rs`, replace:

```rust
        Action::Again => SessionEnd::Again,
    }
}
```

with:

```rust
        Action::Again => SessionEnd::Again,
    }
}

/// Runs a repair: the network phase `fetch` bounded by `REPAIR_TIMEOUT` and abandoned on a
/// repoint, then the local commit, which always runs to completion once started.
async fn run_repair(
    inner: &Arc<Inner>,
    generation: u64,
    fetch: impl Future<Output = Result<repair::Prepared, RepairError>>,
) -> Result<repair::Repaired, SessionEnd> {
    let span = repair_span(inner);
    let bounded = tokio::time::timeout(repair::REPAIR_TIMEOUT, fetch.instrument(span.clone()));
    let prepared = match unless_repointed(inner, generation, bounded).await? {
        Ok(Ok(p)) => p,
        Ok(Err(e)) => return Err(repair_failed(inner, generation, e)),
        Err(_) => {
            return Err(SessionEnd::Retry(format!(
                "repair: no answer within {:?}",
                repair::REPAIR_TIMEOUT
            )));
        }
    };
    repair::commit(inner, prepared)
        .instrument(span)
        .await
        .map_err(|e| repair_failed(inner, generation, e))
}

fn repair_span(inner: &Inner) -> tracing::Span {
    tracing::info_span!(
        "repair",
        node_id = %inner.cfg.identifier,
        upstream = %inner.upstream_uri(),
        seq = inner.store.position().last_seq,
        epoch = inner.ident().map(|(id, _)| id.epoch),
    )
}
```

In `src/slave/driver.rs`, replace:

```rust
            r = unless_repointed(inner, generation, tokio::time::timeout(idle, stream.message())) => r,
            // A verify asked for a re-bootstrap (`MismatchPolicy::Rebootstrap`): handshake again,
            // which bootstraps because the request is set. A stale wake-up is ignored.
            _ = inner.rebootstrap.notified() => {
                if inner.force_bootstrap.is_requested_for(generation) {
                    return SessionEnd::Again;
```

with:

```rust
            r = unless_repointed(inner, generation, tokio::time::timeout(idle, stream.message())) => r,
            // A verify asked for a re-bootstrap (`MismatchPolicy::Rebootstrap`) or a repair
            // (`MismatchPolicy::Repair`): handshake again, which bootstraps or repairs because
            // the request is set. A stale wake-up is ignored.
            _ = inner.rebootstrap.notified() => {
                if inner.force_bootstrap.is_requested_for(generation) || inner.has_pending_repair() {
                    return SessionEnd::Again;
```

In `src/slave/verifier.rs`, replace:

```rust
use super::inner::{CaptureHold, CaptureMiss, Inner};
use super::state::SlaveState;
```

with:

```rust
use super::inner::{CaptureHold, CaptureMiss, Inner};
use super::repair::AffectedSet;
use super::state::SlaveState;
```

In `src/slave/verifier.rs`, replace:

```rust
        );
        if !halted && inner.cfg.verify.on_mismatch == MismatchPolicy::Rebootstrap {
            // For the upstream the slave follows now; a later repoint makes it inert.
            inner.force_bootstrap.request(*inner.repoint_tx.borrow());
            inner.rebootstrap.notify_one();
        }
```

with:

```rust
        );
        let set = match inner.cfg.verify.on_mismatch {
            _ if halted => None,
            MismatchPolicy::Report => None,
            MismatchPolicy::Rebootstrap => Some(None),
            // A CF that differs cannot be repaired key by key: bootstrap.
            MismatchPolicy::Repair => Some(AffectedSet::from_mismatches(&report.mismatches)),
        };
        match set {
            None => {}
            Some(Some(set)) => {
                inner.request_repair(set);
                inner.rebootstrap.notify_one();
            }
            Some(None) => {
                // For the upstream the slave follows now; a later repoint makes it inert.
                inner.force_bootstrap.request(*inner.repoint_tx.borrow());
                inner.rebootstrap.notify_one();
            }
        }
```

In `src/slave/repoint.rs`, replace:

```rust
///
/// **Consent to lose data is single-use, and in practice covers the repoint's first handshake
/// only.** `Rebootstrap` and `Repair` apply only while their own repoint is in progress, and a
/// repoint settles as soon as that handshake is answered (with any outcome, including
/// `Retrying` and an error); the mode then becomes `Refuse`. A `DIVERGED` that arrives later —
/// from the Tail or Bootstrap that follows the handshake, or from any later session — halts the
```

with:

```rust
///
/// **Consent to lose data is single-use: it lasts until its own repoint settles.**
/// `Rebootstrap` and `Repair` apply only while their repoint is in progress. A repoint settles
/// when its first handshake is answered; with `Repair`, when the repair that answer started has
/// committed (`Repaired`) or failed (`Retrying`), or, when it fell back to a bootstrap, when the
/// next handshake is answered. Any outcome settles it, including `Retrying` and an error; the
/// mode then becomes `Refuse`. A `DIVERGED` that arrives later —
/// from the Tail or Bootstrap that follows the handshake, or from any later session — halts the
```

In `src/slave/repoint.rs`, replace:

```rust
    Refuse,
    /// Discard only the divergent suffix and resume (DESIGN.md §22, Plan 3). **Not available in
    /// this version:** `repoint` returns `Error::Unsupported` and the slave halts
    /// `Diverged` with its data intact. It needs verify sessions (Plan 3).
    Repair { accept_loss: AcceptDataLoss },
```

with:

```rust
    Refuse,
    /// Discard only the divergent suffix (DESIGN.md §12.1): the keys it touched are taken from
    /// the new upstream, the suffix is dropped, and the slave replays the upstream's history
    /// (`Repaired { discarded }`; reads answer `NotReady` until replay reaches the upstream's
    /// snapshot seq). When repair cannot be proven safe (a CF operation in the suffix, entries
    /// no longer retained, too much to fetch), the slave bootstraps instead (`Bootstrapped`).
    Repair { accept_loss: AcceptDataLoss },
```

In `src/slave/repoint.rs`, replace:

```rust
    Bootstrapped,
    /// The divergent suffix `discarded` was rolled back (Plan 3; never returned by this
    /// version).
    Repaired { discarded: RangeInclusive<Seq> },
```

with:

```rust
    Bootstrapped,
    /// The divergent suffix `discarded` was rolled back and the keys it touched were taken
    /// from the new upstream (`RepointDivergence::Repair`). Reads answer `NotReady` until the
    /// slave has replayed up to the upstream's snapshot seq.
    Repaired { discarded: RangeInclusive<Seq> },
```

In `src/slave/repoint.rs`, replace:

```rust

/// The error a repoint reports when `mode` halts on divergence: `diverged` for `Refuse`,
/// `Unsupported` for `Repair`.
pub(crate) fn mode_error(mode: RepointDivergence, diverged: impl FnOnce() -> Error) -> Error {
    match mode {
        RepointDivergence::Repair { .. } => {
            Error::Unsupported("divergence repair needs verify sessions (Plan 3)".into())
        }
        _ => diverged(),
    }
}

impl Slave {
```

with:

```rust

impl Slave {
```

In `src/slave/repoint.rs`, replace:

```rust
    ///   `RepointDivergence::Rebootstrap { accept_loss: AcceptDataLoss }`.
    ///   `RepointDivergence::Repair` returns `Err(Unsupported)` in this version (it needs
    ///   Plan 3's verify sessions) and also leaves the slave halted with its data intact.
    ///   The consent is single-use: it covers `DIVERGED` answers until this repoint settles
```

with:

```rust
    ///   `RepointDivergence::Rebootstrap { accept_loss: AcceptDataLoss }`.
    ///   `RepointDivergence::Repair { accept_loss: AcceptDataLoss }` discards only the
    ///   divergent suffix and returns `Ok(Repaired { discarded })` (or `Ok(Bootstrapped)` when
    ///   repair is not possible).
    ///   The consent is single-use: it covers `DIVERGED` answers until this repoint settles
```

Run: `cargo build && cargo test --lib`
Expected: builds without warnings; all unit tests PASS, including `store::repair` (2), `slave::repair` (3), `reject_rows`, `status_rows` and `config_is_validated`.

- [ ] **Step 8: Run the tests**

Run: `cargo test --test repair --test repoint`
Expected: PASS (7 tests in `repair`, 9 in `repoint`).

Run: `cargo test --features failpoints --test failpoints crash_mid_repair_then_converge`
Expected: PASS.

- [ ] **Step 9: Document it**

In `proto/replication.proto`, replace:

```proto
  string   node_state               = 8;   // "ready", "bootstrapping" or "halted:<Reason>"
```

with:

```proto
  string   node_state               = 8;   // "ready", "bootstrapping", "halted:<Reason>" or "repairing:<seq>"
```

and the same line in the PROTOCOL.md §14 block (it must stay byte-identical):

```proto
  string   node_state               = 8;   // "ready", "bootstrapping" or "halted:<Reason>"
```

with:

```proto
  string   node_state               = 8;   // "ready", "bootstrapping", "halted:<Reason>" or "repairing:<seq>"
```

In DESIGN.md §3.2, after the `node_state` row, add:

```markdown
| `node_state` | slave | text: `ready`, `bootstrapping` or `halted:<Reason>` (data-related reasons only); absent means `ready`. Persisted, see §16.3 |
```

with:

```markdown
| `node_state` | slave | text: `ready`, `bootstrapping` or `halted:<Reason>` (data-related reasons only); absent means `ready`. Persisted, see §16.3 |
| `repair_until` | slave | while a repair replays (§12.1): the seq `last_seq` must reach before the node is ready again; written by the repair's batch, removed when replay reaches it or by a bootstrap. Kept apart from `node_state`, so a halt during the replay cannot make the node ready early |
| `diverged_at` | slave | first seq of the local divergent suffix, recorded when the slave halts `Diverged` (`Slave::diverged_entries`); cleared by a repair or a bootstrap |
```

In DESIGN.md §11, the `LogSource` sketch:

```markdown
    fn node_state(&self) -> String { "ready".into() }          // Status RPC: ready | bootstrapping | halted:<Reason>
```

with:

```markdown
    fn node_state(&self) -> String { "ready".into() }          // Status RPC: ready | bootstrapping | halted:<Reason> | repairing:<seq>
```

In DESIGN.md §12, replace:

```markdown
- Every report is stored in `SlaveStatus.last_verify` and passed to `on_verify_complete`. Under `MismatchPolicy::Rebootstrap`, a report with mismatches ends the current Tail stream and the slave bootstraps at once (a halted slave stays halted).
- `MismatchPolicy::Report` (default) or `MismatchPolicy::Rebootstrap`. There is no automatic range repair in v1.
```

with:

```markdown
- Every report is stored in `SlaveStatus.last_verify` and passed to `on_verify_complete`. Under `MismatchPolicy::Rebootstrap`, a report with mismatches ends the current Tail stream and the slave bootstraps at once. Under `MismatchPolicy::Repair`, it ends the stream and the reported keys and ranges are repaired from the upstream (§12.1); a `CfSpec` mismatch bootstraps instead. A halted slave stays halted, and its reports never trigger a policy: it may be on another history than its upstream (a divergence the handshake refused).
- `MismatchPolicy::Report` (default), `MismatchPolicy::Rebootstrap` or `MismatchPolicy::Repair`. **Verify cannot tell which side is damaged.** `Rebootstrap` and `Repair` assume the upstream is right: in a chain A→B→C, a damaged B copies its damage onto a correct C (and further down). Choosing one of these policies is the consent to that; prefer `Report` on nodes whose upstream is not the master, or verify the upstream first.
```

In DESIGN.md, insert before `## 13. Identity, epoch, versioning`:

```markdown
## 13. Identity, epoch, versioning
```

with:

```markdown
### 12.1 Repair

A slave can take **only the keys that differ** from its upstream instead of bootstrapping. Repair runs in the driver between sessions (no entry is applied meanwhile), in two phases. The **network phase** reads the local suffix and fetches the upstream's whole envelopes over a verify session pinned to the upstream's snapshot at `S` (PROTOCOL.md §9.2); it is bounded by 120 s and abandoned on a repoint. The **local phase** commits one synced `WriteBatch` and is never cancelled once started.

- **Divergence** (`REJECT` / `DIVERGED` under `DivergencePolicy::Repair`, or a `repoint` with `RepointDivergence::Repair { accept_loss }`):
  1. The fork `F` comes from the epoch histories (§13, nonces included): where the slave's newest records leave the upstream's history. Within one epoch there is one writer process, so entries before `F` are the same on both sides. Second line of defence: entry `F − 1`'s CRC is compared where both sides hold it. Histories that show no fork at or below the local `last_seq` fall back to a bootstrap.
  2. Read the local suffix `F..=L` (at most `repair_max_bytes`) and collect what it touched: keys of `Put` and `Delete`, ranges of `DeleteRange`.
  3. Fetch those keys (`FetchKeys`, at most 1 MiB of keys per request) and every key of those ranges (`FetchRange`) as the upstream's envelopes at `S`; `S ≥ F − 1` is required (else retried later).
  4. One synced `WriteBatch`: clear the ranges, write or delete the keys with the fetched envelopes, delete changelog entries `F..=L`, set `last_seq = F − 1` and `last_entry_crc` = CRC of local entry `F − 1` (unknown if not held), the upstream's identity and epoch history, `node_state = ready`, and `repair_until = S` (when `S > F − 1`). Downstream streams are reset and a pending verify capture is cancelled.
  5. Tail from `F`. Replaying `F..=S` (absolute ops only) over the repaired keys gives exactly the upstream's state at `S`; every other key was never touched by the suffix and already held its state at `F − 1`. Until `last_seq` reaches `repair_until`, reads, snapshots, verify captures, promotions and downstream handshakes answer `NotReady` (`Status` reports `repairing:<S>`), also after a restart and after a halt during the replay. Then `repair_until` is removed.
- **Verify mismatches** (`MismatchPolicy::Repair`): the reported keys and ranges are fetched at a fresh snapshot `S ≥ L` and written the same way, without dropping any entry; replaying `L + 1..=S` completes the repair. An upstream behind this slave (`S < L`) is retried later: rewinding would drop entries the verify never reported.
- **Fallback to a full bootstrap:** a CF operation in the suffix, a suffix this slave no longer retains, a fork entry the upstream no longer holds (`F < first_retained_seq`), histories that do not show the fork, a CF that differs between the sides (or a `CfSpec` mismatch), an undecodable entry or envelope, a key or range bound too large to fetch, or more than `SlaveConfig::repair_max_bytes` (default 64 MiB) to read, fetch or delete. A transient failure (upstream unreachable, session lost, timeout) retries with backoff.
- **Salvage.** A slave that halts `Diverged` records its fork point (`__meta.diverged_at`, from the histories); `Slave::diverged_entries()` returns the local entries `F..=L`.
- The listener's `on_rewind(F − 1)` is called after the batch; `on_entry` then continues at `F`. `RepointOutcome::Repaired { discarded: F..=L }` is what `repoint` returns.

## 13. Identity, epoch, versioning
```

In DESIGN.md §13, replace:

```markdown
  - `on_divergence: DivergencePolicy` — `Halt` (default) halts with `Diverged`; `Rebootstrap` bootstraps.
```

with:

```markdown
  - `on_divergence: DivergencePolicy` — `Halt` (default) halts with `Diverged`; `Repair` repairs only the divergent suffix (§12.1); `Rebootstrap` bootstraps.
```

In DESIGN.md §13.1, replace:

```markdown
- `Repair { accept_loss: AcceptDataLoss }`: roll back only the divergent entries (§22). Until verify sessions exist it returns `Error::Unsupported`, and the slave halts `Diverged` with its data intact.
```

with:

```markdown
- `Repair { accept_loss: AcceptDataLoss }`: roll back only the divergent entries and take the keys they touched from the new upstream (§12.1); returns `Repaired { discarded }`, or `Bootstrapped` when repair falls back.
```

In DESIGN.md §15, replace:

```markdown
    fn on_verify_complete(&self, report: &VerifyReport) {}
}
```

with:

```markdown
    fn on_verify_complete(&self, report: &VerifyReport) {}
    fn on_rewind(&self, last_seq: Seq) {}  // a repair (§12.1) moved the position back; on_entry continues at last_seq + 1
}
```

and:

```markdown
- Strictly increasing `seq`, with no duplicates within a process, between bootstraps. After `on_bootstrap_complete(S)` delivery continues at `S + 1`;
```

with:

```markdown
- Strictly increasing `seq`, with no duplicates within a process, between bootstraps and rewinds. After `on_bootstrap_complete(S)` or `on_rewind(S)` delivery continues at `S + 1`;
```

In DESIGN.md §16, `SlaveConfig`:

```markdown
    pub on_divergence: DivergencePolicy,     // Halt (default) | Rebootstrap
```

with:

```markdown
    pub on_divergence: DivergencePolicy,     // Halt (default) | Repair | Rebootstrap
    pub repair_max_bytes: u64,               // a larger repair bootstraps instead; default 64 MiB
```

and the `Slave` methods:

```markdown
    pub async fn verify_now(&self, mode: VerifyMode) -> Result<VerifyReport>;
```

with:

```markdown
    pub async fn verify_now(&self, mode: VerifyMode) -> Result<VerifyReport>;
    pub fn diverged_entries(&self) -> Result<Vec<LogEntry>>;   // halted Diverged: the local suffix (§12.1)
```

In DESIGN.md §16.3, after the table:

```markdown
| `halted:<Reason>`, data-related reasons only | `Halted(reason)`. The slave does not contact its upstream until `clear_halt`. |
```

with:

```markdown
| `halted:<Reason>`, data-related reasons only | `Halted(reason)`. The slave does not contact its upstream until `clear_halt`. |

`__meta.repair_until` is independent of this table: while it is above `last_seq` (a repair is replaying, §12.1), the slave answers `NotReady` in every state, and `Status` reports `repairing:<seq>` unless it is bootstrapping or halted.
```

In DESIGN.md §18, add a row:

```markdown
| Verify session timeout | 5 min |
```

with:

```markdown
| Verify session timeout | 5 min |
| Repair (`repair_max_bytes`, network phase) | 64 MiB, 120 s; a larger repair bootstraps |
```

In DESIGN.md §19:

```markdown
- **`tracing` spans** named `handshake`, `bootstrap`, `verify` and `reaper`,
```

with:

```markdown
- **`tracing` spans** named `handshake`, `bootstrap`, `verify`, `repair` and `reaper`,
```

In DESIGN.md §21, add after the corruption bullet:

```markdown
- Injected corruption on a slave is detected by `Full` verify and localised to the key. A key that exists only on the upstream, between the slave's chunk boundaries, is found.
```

with:

```markdown
- Injected corruption on a slave is detected by `Full` verify and localised to the key. A key that exists only on the upstream, between the slave's chunk boundaries, is found.
- **Repair:** a diverged slave with puts, deletes and range deletes in its suffix (and no entry before the fork in its own log) repairs without a bootstrap and matches under `Full` verify; `repoint(.., Repair { AcceptDataLoss })` returns `Repaired` and verifies clean; a CF op in the suffix, a trimmed local suffix, an upstream without the fork point and more than `repair_max_bytes` fall back to a bootstrap; a crash right after the rewind stays `NotReady` until replay passes `S` (after a restart with no upstream, and after a persisted halt during the replay), then converges; `MismatchPolicy::Repair` fixes corrupted, missing and extra keys; `diverged_entries` returns the suffix of a halted slave.
```

In DESIGN.md §22, replace:

```markdown
- Divergence is detected by CRC comparison on connect and by rejecting slaves ahead of the master. It is not repaired automatically: an explicit `repoint` refuses by default and discards entries only with `AcceptDataLoss`.
```

with:

```markdown
- Divergence is detected from the epoch histories (nonces included), with CRC comparison and the rejection of slaves ahead of the master as backstops. It is resolved only by policy (`DivergencePolicy::Repair` or `Rebootstrap`) or with explicit consent: a `repoint` refuses by default and discards entries only with `AcceptDataLoss`. Repair rewrites only the keys the divergent suffix touched (§12.1) and falls back to a bootstrap whenever it cannot prove the result.
```

and delete the future-work item (it is `MismatchPolicy::Repair` now):

```markdown
- Automatic range repair after a verify mismatch.
```

with:

```markdown

```

In PROTOCOL.md §6.5, the `Status` field table:

```markdown
| `node_state` | `"ready"`, `"bootstrapping"` or `"halted:<Reason>"`. |
```

with:

```markdown
| `node_state` | `"ready"`, `"bootstrapping"`, `"halted:<Reason>"` or `"repairing:<seq>"` (a repair is replaying up to `seq`; the node answers `NOT_READY` until then). |
```

In PROTOCOL.md §11.2:

```markdown
| `REJECT` / `DIVERGED` | Divergence policy *Halt* (default) → halt `Diverged`. *Rebootstrap* → bootstrap. |
```

with:

```markdown
| `REJECT` / `DIVERGED` | Divergence policy *Halt* (default) → halt `Diverged`. *Repair* → repair from a verify session (the fork from the epoch histories, `EntryCrcs` to check the entry before it, `FetchKeys`/`FetchRange` for the keys the suffix touched; §9.2), falling back to a bootstrap. *Rebootstrap* → bootstrap. |
```

In the PROTOCOL.md §12 table, add rows:

```markdown
| `ListResult` size | server | stop after ~1 MiB of keys, but never fewer than 2 keys |
```

with:

```markdown
| `ListResult` size | server | stop after ~1 MiB of keys, but never fewer than 2 keys |
| `FetchKeys.keys` | client | ≤ 1024 keys (RECOMMENDED ≤ 1 MiB of keys per request) |
| `FetchRange.limit` | client | 1 to 1024 |
| `FetchResult` size | server | stop after ~1 MiB of envelopes; at least 1 record (`FetchKeys`), 2 (`FetchRange`) |
| `EntryCrcs` | server | at most 1024 entries, none after the snapshot seq |
```

In `docs/superpowers/specs/2026-10-09-failover-acks-repair.md`, mark it merged:

```markdown
Status: agreed with the user, 2026-10-09. To be merged into DESIGN.md and PROTOCOL.md by the owning tasks (Plan 2 amendment for §1–§4, Plan 3 for §5). Where this addendum and DESIGN/PROTOCOL disagree, this addendum wins until the merge.
```

with:

```markdown
Status: agreed with the user, 2026-10-09. To be merged into DESIGN.md and PROTOCOL.md by the owning tasks (Plan 2 amendment for §1–§4, Plan 3 for §5, §7 and §8). Where this addendum and DESIGN/PROTOCOL disagree, this addendum wins until the merge. **Merged:** §1–§4 by Plan 2; §5 (DESIGN §12.1), §7 (DESIGN §13, PROTOCOL §5.5, §6.3) and §8 (DESIGN §5.2, §16) by Plan 3, which supersede this text.
```

In `docs/superpowers/specs/2026-10-09-plan2-3-design-notes.md`, mark the Plan 3 section superseded:

```markdown
## Plan 3: Verify
```

with:

```markdown
## Plan 3: Verify

> **Superseded** by DESIGN.md §12 and §12.1 and PROTOCOL.md §9 as Plan 3 merged them: listings resume from the last key received (not `last key + \0`), `Mismatch::CfSpec` carries `CfVersion`s, and `replicas()` reports real acks (Plan 2). Kept for history.
```

- [ ] **Step 10: Run everything and commit**

Run: `cargo fmt --check && cargo test && cargo test --features failpoints && cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings`
Expected: PASS, no warnings.

```bash
git add src tests proto docs DESIGN.md PROTOCOL.md
git commit -m "feat: divergence repair from the epoch histories, MismatchPolicy::Repair and diverged_entries

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: DESIGN.md §21 sweep — every listed test exists, and the missing ones are added

**Files:**
- Create: `examples/verify.rs`, `examples/repair.rs`
- Modify: `tests/failure_resume.rs`, `tests/replication.rs`, `tests/failpoints.rs`, `tests/promotion.rs`, `examples/README.md`

**Interfaces:**
- Consumes: Task 5 `Slave::verify_now`, `VerifyMode`; Plan 2 harness (`Cluster::{new, master, master_config, slave, slave_config, restart_master, path, clock}`, `assert_converged`, `wait_state`, `wait_until`, `RecordingListener`, `RawDb`, `WAIT`), `Slave::{snapshot, wait_for_seq, kill (feature failpoints), shutdown}`, `serial()` in `tests/failpoints.rs`, the locals `b`, `cn`, `e`, `f` at the end of `promotion_end_to_end` in `tests/promotion.rs`; `LogApi::{last_seq, first_retained_seq, entries_since}`.
- Produces: tests, and two runnable examples.

The coverage map below is the sweep. Every DESIGN.md §21 item (including the repair bullet Task 10 adds) names the test(s) that cover it; the four marked **new** are added in this task.

| DESIGN.md §21 item | Test(s) |
|---|---|
| A→B convergence under concurrent writes; verify `Full` matches | `tests/verify.rs::verify_pins_the_snapshot_while_concurrent_writes_continue` (Task 5) |
| A→B→C chain, C bootstrapping from B while A writes and creates/drops CFs; A, B, C identical | `tests/chain.rs::a_chain_replicates_everything_and_bootstraps_from_the_middle` (Plan 2); `tests/integrity.rs::every_link_of_a_chain_verifies_against_its_own_upstream` (Task 6) |
| Acks and write concern: `replicas()` follows acks and lag; bad acks end the stream; `NotEnoughReplicas`; `AckTimeout`; promote the in-sync replica after losing the master | `src/server/tests.rs::{acks_drive_replica_status_and_in_sync, ack_violations_end_the_stream_with_invalid_argument}`, `src/server/replicas.rs::acks_must_be_monotonic_and_not_above_what_was_sent`, `tests/write_concern.rs` (all four; `the_configured_write_concern_applies_to_every_write` also covers the CF-operation exemption and `wait_in_sync`, Task 2), `tests/failpoints.rs::a_replica_that_stops_acking_times_the_write_out` (Plan 2) |
| Slave crash and restart at random points: no gaps, no duplicates, listener order preserved | `tests/failpoints.rs::crash_after_apply_resumes_without_gap_or_duplicate`, `crash_mid_bootstrap_restarts_the_bootstrap` (Plan 2); **new** `tests/failpoints.rs::random_kills_while_writing_never_lose_or_repeat_an_entry` |
| C ahead of B after B crashes unsynced: C waits, then converges | `tests/failure_resume.rs::a_downstream_ahead_of_its_upstream_waits_and_converges` (Plan 2); `tests/integrity.rs::a_slave_ahead_of_the_upstream_snapshot_abandons_the_run` (Task 6) |
| Retention trims past a stopped slave; it bootstraps again on restart | `tests/rebootstrap.rs::retention_gap_triggers_a_rebootstrap_that_converges` (Plan 2) |
| Bootstrap lease under `max_entries = 1` | `tests/failpoints.rs::bootstrap_lease_survives_aggressive_retention`, `src/server/tests.rs::bootstrap_lease_holds_retention_until_tail_presents_it` (Plan 2) |
| Quiet master with every changelog entry expired: `last_seq` kept, slaves resume | `src/store/retention.rs::quiet_store_can_trim_everything_but_keeps_position` (Plan 1); **new** `tests/failure_resume.rs::a_quiet_master_with_an_empty_log_keeps_last_seq_and_slaves_resume` |
| TTL: visible expiry before reaping, reaper `Delete` replicates, overwrite clears TTL, stale index cleaned, several entries past `max_deletes_per_entry` | `tests/master_ttl.rs` (Plan 1: `expired_values_are_hidden_then_reaped_by_a_logged_delete`, `overwrite_clears_ttl_and_leaves_a_stale_index_entry`, `range_deleted_and_dropped_cf_entries_are_stale`, `large_rounds_split_into_bounded_entries`, `ttl_on_empty_and_nul_keys_is_reaped`); `tests/replication.rs::ttl_values_replicate_and_the_reapers_delete_replicates` (Plan 2) |
| Divergence: a restore is `DIVERGED` past the restore point by the history alone (no flag needed); `EPOCH_ADVANCED` behind it | `tests/rebootstrap.rs::master_restored_without_epoch_bump_makes_ahead_slaves_diverge`, `restored_master_with_epoch_bump_keeps_behind_slaves_and_rejects_ahead_ones` (Plan 2); `tests/rebootstrap.rs::a_restore_without_a_bump_is_diverged_by_the_history_alone`, `src/server/handshake.rs::{the_same_epoch_with_another_nonce_is_another_epoch, every_record_the_client_sends_is_checked}` (Task 1) |
| Promotion: siblings continue or diverge per policy, old master `STALE_EPOCH`, writes and TTLs on the new master, **`Full` verify on every link** | `tests/promotion.rs::promotion_end_to_end`, `the_old_master_is_refused_with_stale_epoch_and_the_client_retries`, `promote_checked_refuses_when_a_reachable_peer_is_later`, `demote_undoes_a_wrong_promotion_with_zero_loss`, `tests/repoint.rs::failover_runbook_end_to_end` (Plan 2); **new** verify block appended to `promotion_end_to_end`; `tests/promotion.rs::{a_trimmed_compatible_peer_does_not_block_a_checked_promotion, a_peer_on_another_history_blocks_a_checked_promotion_without_its_entries}`, `tests/ergonomics.rs::promotion_config_and_refusal_parts` (Tasks 1, 2) |
| Failure and resume: master, mid-chain and leaf restarts; crash mid-bootstrap and mid-apply; dropped connections | `tests/failure_resume.rs::restarts_of_master_mid_chain_and_leaf_resume_without_bootstrap`, `tests/failpoints.rs::{crash_mid_bootstrap_restarts_the_bootstrap, crash_after_apply_resumes_without_gap_or_duplicate, connection_drop_mid_tail_resumes}` (Plan 2); `tests/failure_resume.rs::a_master_restart_moves_followers_to_the_new_epoch_without_a_bootstrap` (Task 1) |
| Edge-case data on every path (write, reopen, tail, bootstrap, verify, promotion) | `tests/master_write.rs::edge_case_keys_and_values_through_the_master`, `src/store/apply.rs::nul_empty_and_large_keys_and_values_survive_reopen` (Plan 1); `tests/replication.rs::edge_case_data_through_tail_and_bootstrap`, `tests/chain.rs::a_chain_replicates_everything_and_bootstraps_from_the_middle`, `tests/promotion.rs::promotion_end_to_end` (Plan 2); `tests/integrity.rs::edge_case_data_verifies_and_its_corruption_is_found` (Task 6) |
| Epoch bump: streams end `UPSTREAM_RESET`; seq continues; a `Rebootstrap` slave converges | `tests/rebootstrap.rs::live_epoch_bump_resets_streams_and_slaves_continue`, `src/server/tests.rs::bump_epoch_ends_streams_with_upstream_reset` (Plan 2); `src/server/verify.rs::an_epoch_bump_ends_verify_streams_and_sessions` (Task 4) |
| Identity change → `Halted`; protocol mismatch → `REJECT`; loop → `LoopDetected` | `tests/rebootstrap.rs::identity_change_is_refused_persistently_until_clear_halt_rebootstraps`, `src/server/tests.rs::version_mismatch_is_answered_in_the_handshake_body`, `tests/chain.rs::{a_loop_is_detected_and_the_halt_is_not_persisted, a_slave_pointed_at_itself_halts_loop_detected}` (Plan 2) |
| Halt persistence; `clear_halt(Rebootstrap)` recovers | `tests/failure_resume.rs::a_log_gap_halt_persists_across_restart_until_cleared`, `tests/rebootstrap.rs::identity_change_is_refused_persistently_until_clear_halt_rebootstraps` (Plan 2) |
| CF drop while a `ReadSnapshot` is open: still readable; physical drop after release | `src/store/read.rs::snapshot_keeps_dropped_cf_readable_until_released`, `tests/master_retention.rs::maintenance_drops_released_cfs` (Plan 1); `tests/replication.rs::a_cf_held_by_a_snapshot_is_retried_not_halted` (Plan 2); **new** `tests/replication.rs::a_replicated_cf_drop_waits_for_a_slave_snapshot` |
| Unregistered comparator: master rejects; slave halts `SpecMismatch`, recovers once registered | `tests/rebootstrap.rs::missing_comparator_halts_until_registered_and_cleared` (Plan 2) |
| Token auth accept/reject; per-method authorisation; token without TLS refused | `tests/security.rs` (Plan 2: `tokens_over_tls_accept_any_valid_secret_and_reject_others`, `bootstrap_can_be_denied_per_method`, `token_authentication_on_plaintext_is_refused_unless_allowed`, `mutual_tls_allow_list_by_certificate_subject`) |
| Entry over `max_entry_bytes` rejected; a large valid entry passes through the chain | `tests/master_write.rs::max_entry_bytes_is_enforced_at_the_boundary` (Plan 1); `tests/chain.rs::a_chain_replicates_everything_and_bootstraps_from_the_middle` (the ~16 MiB edge-case value through A→B→C, Plan 2) |
| Injected corruption found by `Full` verify and localised to the key; an upstream-only key between chunk boundaries is found | `tests/integrity.rs::{a_corrupted_value_is_found_by_full_verify_only, a_key_only_upstream_between_two_chunk_boundaries_is_found, keys_deleted_on_disk_are_reported_in_both_modes, extra_keys_on_disk_are_reported_with_no_upstream_value, a_custom_comparator_cf_verifies_and_localises_damage}` (Task 6) |
| Repair: divergence repaired without bootstrap (from the histories, without the entry before the fork); fallbacks (CF op, trimmed suffix, fork not held upstream, `repair_max_bytes`); crash after the rewind, `NotReady` across a restart and a halt; verify-mismatch repair; `diverged_entries` | `tests/repair.rs` (all seven), `tests/failpoints.rs::crash_mid_repair_then_converge`, `tests/repoint.rs::divergence_is_refused_unless_data_loss_is_accepted` (Task 10) |
| Wire codec matches PROTOCOL.md §15 vectors | `src/codec/entry.rs::vector_v1_ops_bytes_and_crc`, `src/codec/envelope.rs::vector_v2_envelopes`, `src/codec/digest.rs::vector_v3_digests` (Plan 1); `src/verify/range.rs::range_hashes_match_the_v3_vectors`, `src/verify/scan.rs::a_snapshot_range_hashes_to_the_v3_vectors` (Task 3) |

- [ ] **Step 1: A quiet master with an empty log**

Append to `tests/failure_resume.rs`:

```rust
/// DESIGN.md §21: a quiet master whose whole changelog expired keeps its `last_seq`, across
/// a restart too, and a slave stopped at that seq resumes without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_quiet_master_with_an_empty_log_keeps_last_seq_and_slaves_resume() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_age = Some(Duration::from_secs(60));
    mcfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(mcfg.clone()).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let (b_cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    // The Tail presenting the bootstrap lease has started: the lease no longer holds retention.
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    b.shutdown().await.unwrap();

    let last = m.last_seq();
    c.clock.advance(Duration::from_secs(120));
    wait_until("the whole log expired", || {
        m.first_retained_seq() == last + 1
    })
    .await;
    let m = c.restart_master(m, mcfg).await;
    assert_eq!(
        (m.last_seq(), m.first_retained_seq()),
        (last, last + 1),
        "the position survives an empty log"
    );
    assert_eq!(m.entries_since(last).unwrap().count(), 0);

    let b = Slave::open(b_cfg).await.unwrap();
    wait_state(&b, "streaming without a bootstrap", |s| {
        *s == SlaveState::Streaming
    })
    .await;
    m.put("c", b"after", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1, "only the initial bootstrap");
}
```

Run: `cargo test --test failure_resume a_quiet_master`
Expected: PASS (it exercises Plan 1 retention and Task 1: the restarted master is at a new epoch with an empty log, and the slave moves to it with `EPOCH_ADVANCED`; if it fails, the bug is there).

- [ ] **Step 2: A replicated CF drop waits for a slave snapshot**

Append to `tests/replication.rs`:

```rust
/// DESIGN.md §21: a CF dropped while a `ReadSnapshot` of a slave is open stays readable
/// through the snapshot; the slave drops it physically once the snapshot is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replicated_cf_drop_waits_for_a_slave_snapshot() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("gone")).unwrap();
    m.put("gone", b"k", b"v").unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    let snap = b.snapshot().unwrap();
    m.drop_cf("gone").unwrap();
    b.wait_for_seq(m.last_seq(), WAIT).await.unwrap();
    assert!(b.list_cfs().is_empty(), "dropped for live reads");
    assert_eq!(
        snap.get("gone", b"k").unwrap(),
        Some(b"v".to_vec()),
        "the snapshot still reads it"
    );
    assert_eq!(snap.list_cfs(), vec![CfSpec::new("gone")]);
    // Physical CFs of the running slave, from its MANIFEST.
    let physical = |dir: &std::path::Path| {
        rocksdb::DB::list_cf(&rocksdb::Options::default(), dir)
            .map(|names| names.iter().filter(|n| n.starts_with("u:gone:")).count())
    };
    assert_eq!(
        physical(&c.path("b")).unwrap(),
        1,
        "kept while the snapshot is alive"
    );
    drop(snap);
    wait_until("the slave drops the physical CF", || {
        physical(&c.path("b")).is_ok_and(|n| n == 0)
    })
    .await;
}
```

Run: `cargo test --test replication a_replicated_cf_drop`
Expected: PASS (within one slave CF-drop tick of releasing the snapshot).

- [ ] **Step 3: Random kills while writing**

`RawDb::last_seq` (Task 10) reads a stopped node's durable position, so the test knows exactly where each restart must resume.

Append to `tests/failpoints.rs`:

```rust
/// DESIGN.md §21: the slave is killed at random points while the master keeps writing, five
/// times. Each time it resumes from its durable seq without a bootstrap, each process's
/// listener sees strictly increasing seqs starting right after that seq, and in the end
/// the slave holds every entry exactly once.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn random_kills_while_writing_never_lose_or_repeat_an_entry() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = std::sync::Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("c")).unwrap();
    let stop = std::sync::Arc::new(std::sync::atomic::AtomicBool::new(false));
    let writer = {
        let (m, stop) = (m.clone(), stop.clone());
        tokio::task::spawn_blocking(move || {
            let mut n = 0u32;
            while !stop.load(std::sync::atomic::Ordering::SeqCst) {
                m.put("c", &n.to_be_bytes(), &n.to_le_bytes()).unwrap();
                n += 1;
            }
            n
        })
    };
    let (cfg, first) = c.slave_config("b", m.serve_addr().unwrap());
    let mut b = Slave::open(cfg.clone()).await.unwrap();
    wait_until("the initial bootstrap", || first.bootstraps() == 1).await;
    for round in 0..5 {
        // A random point: 1–50 entries further on.
        let target = b.last_seq() + 1 + (uuid::Uuid::new_v4().as_u128() % 50) as u64;
        wait_until("progress before the kill", || b.last_seq() >= target).await;
        b.kill().await.unwrap();
        let durable = support::RawDb::open(&cfg.path).last_seq();
        let listener = std::sync::Arc::new(support::RecordingListener::default());
        let mut next = cfg.clone();
        next.listener = Some(listener.clone());
        b = Slave::open(next).await.unwrap();
        wait_until("delivery resumed", || !listener.entries().is_empty()).await;
        assert_eq!(
            listener.entries()[0],
            durable + 1,
            "round {round}: no gap and no repeat at the restart"
        );
        assert_eq!(
            listener.bootstraps(),
            0,
            "round {round}: resumed without a bootstrap"
        );
        listener.assert_no_violations();
    }
    stop.store(true, std::sync::atomic::Ordering::SeqCst);
    assert!(writer.await.unwrap() > 0);
    assert_converged(&*m, &[("b", &b)]).await;
    let first_retained = b.first_retained_seq();
    let seqs: Vec<u64> = b
        .entries_since(first_retained - 1)
        .unwrap()
        .map(|e| e.unwrap().seq)
        .collect();
    assert_eq!(
        seqs,
        (first_retained..=b.last_seq()).collect::<Vec<_>>(),
        "the slave's log has no gap or duplicate"
    );
    scenario.teardown();
}
```

Run: `cargo test --features failpoints --test failpoints random_kills`
Expected: PASS.

- [ ] **Step 4: `Full` verify on every link after promotion**

In `tests/promotion.rs`, at the end of `promotion_end_to_end`, replace:

```rust
    assert_eq!(lf.bootstraps(), 1);
    check_edge_cases(&f, "edge");
}
```

with:

```rust
    assert_eq!(lf.bootstraps(), 1);
    check_edge_cases(&f, "edge");

    // DESIGN.md §21: `Full` verify matches on every link of the new topology.
    for (name, s) in [("c", &cn), ("e", &e), ("f", &f)] {
        let r = s
            .verify_now(replicated_rocksdb::VerifyMode::Full)
            .await
            .unwrap();
        assert!(r.is_clean(), "{name}: {} mismatches", r.mismatches.len());
        assert_eq!(r.seq, b.last_seq(), "{name}");
    }
}
```

Run: `cargo test --test promotion promotion_end_to_end`
Expected: PASS.

- [ ] **Step 5: Examples for verify and repair**

Two runnable examples in the style of the existing ones (temp directories, `127.0.0.1`, exit 0).

Create `examples/verify.rs`:

```rust
//! Integrity verification: compare a replica's data with its upstream's, at the same seq.
//!
//! Shows `Slave::verify_now` in both modes and what a `VerifyReport` says: `Quick` compares
//! envelope headers (missing, extra or stale writes), `Full` compares whole values (it also
//! finds a value corrupted on disk). The damage here is injected the way it happens in real
//! life: behind the crate's back, by opening the stopped replica's directory with plain RocksDB.
//! Scheduled verification (`VerifyConfig::interval`) and the reaction to a mismatch
//! (`MismatchPolicy`) are configured on the slave; see the `repair` example for `Repair`.
//!
//! Run with: `cargo run --example verify`

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    AllowAll, CfSpec, Master, MasterConfig, ServeConfig, Slave, SlaveConfig, UpstreamConfig,
    VerifyMode,
};

const WAIT: Duration = Duration::from_secs(10);

/// Flips the last byte of the value stored under `key` in CF `cf` of a STOPPED node. The
/// envelope header (and its `write_seq`) stays intact: only a `Full` verify can see this.
fn corrupt_on_disk(dir: &Path, cf: &str, key: &[u8]) -> Result<(), Box<dyn Error>> {
    let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), dir)?;
    let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), dir, &names)?;
    // User CFs are stored as `u:{name}:{create_seq}`.
    let physical = names
        .iter()
        .find(|n| n.starts_with(&format!("u:{cf}:")))
        .ok_or("no such CF on disk")?;
    let handle = db.cf_handle(physical).ok_or("CF not open")?;
    let mut envelope = db.get_cf(&handle, key)?.ok_or("no such key on disk")?;
    let last = envelope.len() - 1;
    envelope[last] ^= 0xFF;
    db.put_cf(&handle, key, envelope)?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;
    let mut mcfg = MasterConfig::new("master", dir.path().join("master"));
    mcfg.serve = Some(ServeConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        0,
        Arc::new(AllowAll),
    ));
    let master = Master::open(mcfg).await?;
    master.create_cf(CfSpec::new("kv"))?;
    for i in 0..1_000u32 {
        master.put("kv", format!("key-{i:04}").as_bytes(), b"some value")?;
    }

    let scfg = SlaveConfig::new(
        "replica",
        dir.path().join("replica"),
        UpstreamConfig::from_addr(master.serve_addr().unwrap()),
    );
    let replica = Slave::open(scfg.clone()).await?;
    replica.wait_for_seq(master.last_seq(), WAIT).await?;

    // ---- A clean run. Both sides are compared at exactly the same seq S, even while the
    // master keeps writing: the replica pauses applying until it has a snapshot at S.
    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let report = replica.verify_now(mode).await?;
        println!(
            "{mode:?} verify at seq {}: {} CFs, {} keys, clean = {} ({:?})",
            report.seq,
            report.cfs_checked,
            report.keys_checked,
            report.is_clean(),
            report.duration
        );
        assert!(report.is_clean());
    }

    // ---- Damage the replica's copy of one value on disk, then look again.
    replica.shutdown().await?;
    corrupt_on_disk(&dir.path().join("replica"), "kv", b"key-0500")?;
    let replica = Slave::open(scfg).await?;
    replica.wait_streaming(WAIT).await?;

    let quick = replica.verify_now(VerifyMode::Quick).await?;
    println!(
        "Quick after the damage: clean = {} (headers still match)",
        quick.is_clean()
    );
    let full = replica.verify_now(VerifyMode::Full).await?;
    println!(
        "Full after the damage: {} mismatch(es)",
        full.mismatches.len()
    );
    for m in &full.mismatches {
        // `Mismatch::Key { cf, key, local, upstream }`: the write_seq on each side; equal
        // write_seqs mean the stored values differ.
        println!("  {m:?}");
    }
    assert_eq!(full.mismatches.len(), 1);
    // The latest report is also in the status (and passed to the listener).
    assert!(replica.status().last_verify.is_some());

    replica.shutdown().await?;
    master.shutdown().await?;
    println!("done");
    Ok(())
}
```

Create `examples/repair.rs`:

```rust
//! Repair: fix a replica by rewriting only the keys that differ, instead of re-copying all.
//!
//! Two situations, both from DESIGN.md §12.1:
//!
//! 1. **Divergence.** A replica holds entries its new upstream's history does not have (here:
//!    the operator promoted a replica that was behind). `repoint` refuses by default; with
//!    `RepointDivergence::Repair { accept_loss: AcceptDataLoss }` the replica drops only its
//!    divergent entries, takes the keys they touched from the new upstream, and replays.
//!    `Slave::diverged_entries` lets you salvage the dropped entries first.
//! 2. **Damage found by verify.** With `MismatchPolicy::Repair`, the keys a verify reports are
//!    taken from the upstream at once. (Verify cannot tell which side is damaged: this policy
//!    trusts the upstream. Use it on replicas of the master.)
//!
//! Run with: `cargo run --example repair`

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    AcceptDataLoss, AllowAll, CfSpec, Master, MasterConfig, MismatchPolicy, ReadApi,
    RepointDivergence, RepointOptions, RepointOutcome, ServeConfig, Slave, SlaveConfig,
    UpstreamConfig, VerifyMode,
};

const WAIT: Duration = Duration::from_secs(10);

fn serve() -> ServeConfig {
    ServeConfig::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, Arc::new(AllowAll))
}

fn replica_cfg(dir: &Path, id: &str, upstream: SocketAddr) -> SlaveConfig {
    let mut cfg = SlaveConfig::new(id, dir.join(id), UpstreamConfig::from_addr(upstream));
    cfg.serve = Some(serve());
    cfg
}

async fn divergence(dir: &Path) -> Result<(), Box<dyn Error>> {
    println!("=== 1. Repairing a divergence ===");
    let mut acfg = MasterConfig::new("A", dir.join("A"));
    acfg.serve = Some(serve());
    let a = Master::open(acfg).await?;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("kv"))?;
    let b_cfg = replica_cfg(dir, "B", a_addr);
    let b = Slave::open(b_cfg.clone()).await?;
    let c = Slave::open(replica_cfg(dir, "C", a_addr)).await?;
    let seq = a.put("kv", b"shared", b"everyone has this")?;
    b.wait_for_seq(seq, WAIT).await?;
    c.wait_for_seq(seq, WAIT).await?;

    // B stops; C receives two more entries; A dies.
    b.shutdown().await?;
    a.put("kv", b"only-c-1", b"x")?;
    let seq = a.delete("kv", b"shared")?;
    c.wait_for_seq(seq, WAIT).await?;
    a.shutdown().await?;

    // The operator promotes the stale B (plain `promote` checks no peers).
    let b = Slave::open(b_cfg.clone()).await?;
    let b = b
        .promote(MasterConfig::from_slave_config(&b_cfg, Some(serve())))
        .await?;
    let b_up = UpstreamConfig::from_addr(b.serve_addr().unwrap());

    // The default repoint refuses: C would lose entries. Its data stays readable.
    match c.repoint(b_up.clone(), RepointOptions::default()).await {
        Err(e) => println!("repoint refused: {e}"),
        Ok(o) => panic!("expected a refusal, got {o:?}"),
    }
    for e in c.diverged_entries()? {
        println!(
            "  divergent entry {} ({} bytes of ops) can be salvaged now",
            e.seq,
            e.ops.len()
        );
    }

    // Accept the loss of exactly those entries: only the keys they touched are rewritten.
    let repair = RepointOptions {
        on_divergence: RepointDivergence::Repair {
            accept_loss: AcceptDataLoss,
        },
    };
    match c.repoint(b_up, repair).await? {
        RepointOutcome::Repaired { discarded } => {
            println!("C repaired: dropped entries {discarded:?}")
        }
        other => println!("C: {other:?}"),
    }
    let seq = b.put("kv", b"after", b"written on the new master")?;
    c.wait_for_seq(seq, WAIT).await?;
    println!(
        "C now has shared = {:?}, only-c-1 = {:?}",
        c.get("kv", b"shared")?.map(String::from_utf8),
        c.get("kv", b"only-c-1")?
    );
    assert!(c.verify_now(VerifyMode::Full).await?.is_clean());
    c.shutdown().await?;
    b.shutdown().await?;
    Ok(())
}

async fn verify_repair(dir: &Path) -> Result<(), Box<dyn Error>> {
    println!("=== 2. Repairing what a verify found ===");
    let mut mcfg = MasterConfig::new("M", dir.join("M"));
    mcfg.serve = Some(serve());
    let m = Master::open(mcfg).await?;
    m.create_cf(CfSpec::new("kv"))?;
    for i in 0..100u32 {
        m.put("kv", format!("k{i:03}").as_bytes(), b"v")?;
    }
    let mut cfg = replica_cfg(dir, "R", m.serve_addr().unwrap());
    cfg.verify.on_mismatch = MismatchPolicy::Repair;
    // `cfg.verify.interval = Some(..)` would also run verifies on a schedule.
    let r = Slave::open(cfg.clone()).await?;
    r.wait_for_seq(m.last_seq(), WAIT).await?;

    // Lose a key behind the crate's back (a stopped node opened with plain RocksDB).
    r.shutdown().await?;
    {
        let path = cfg.path.clone();
        let names = rocksdb::DB::list_cf(&rocksdb::Options::default(), &path)?;
        let db = rocksdb::DB::open_cf(&rocksdb::Options::default(), &path, &names)?;
        let cf = names
            .iter()
            .find(|n| n.starts_with("u:kv:"))
            .ok_or("no CF")?;
        db.delete_cf(&db.cf_handle(cf).ok_or("no CF")?, b"k042")?;
    }
    let r = Slave::open(cfg).await?;
    r.wait_streaming(WAIT).await?;

    let report = r.verify_now(VerifyMode::Quick).await?;
    println!(
        "verify found {} mismatch(es): {:?}",
        report.mismatches.len(),
        report.mismatches
    );
    // The repair runs in the background; the next verify is clean.
    let deadline = std::time::Instant::now() + WAIT;
    while r.get("kv", b"k042")?.is_none() && std::time::Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    println!("k042 is back: {:?}", r.get("kv", b"k042")?);
    assert!(r.verify_now(VerifyMode::Full).await?.is_clean());
    r.shutdown().await?;
    m.shutdown().await?;
    Ok(())
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let tmp = tempfile::tempdir()?;
    divergence(tmp.path()).await?;
    verify_repair(tmp.path()).await?;
    println!("done");
    Ok(())
}
```

In `examples/README.md`, add two rows after `secure_replication`:

```markdown
| `secure_replication` | TLS (runtime `rcgen` CA and server cert) plus bearer-token auth; mTLS allow-list explained in comments. | A good-token replica replicates; a bad-token replica halts with `AuthRejected`. |
```

with:

```markdown
| `secure_replication` | TLS (runtime `rcgen` CA and server cert) plus bearer-token auth; mTLS allow-list explained in comments. | A good-token replica replicates; a bad-token replica halts with `AuthRejected`. |
| `verify` | `Slave::verify_now` in `Quick` and `Full` mode and the `VerifyReport`; damage injected into a stopped replica with plain RocksDB. | Two clean runs, then `Quick` still clean and `Full` reporting the corrupted key. |
| `repair` | Divergence repair with `repoint(.., RepointDivergence::Repair { accept_loss: AcceptDataLoss })` after `diverged_entries`, and `MismatchPolicy::Repair` fixing a key a verify found missing. | The refused repoint, the salvageable entries, `Repaired { discarded: 3..=4 }`, then the lost key restored and a clean `Full` verify. |
```

Run: `cargo run -q --example verify && cargo run -q --example repair`
Expected: both exit 0; `verify` prints `Full after the damage: 1 mismatch(es)`, `repair` prints `C repaired: dropped entries 3..=4` and `k042 is back: Some([118])`.

- [ ] **Step 6: Run everything and lint**

Run: `cargo fmt --check && cargo test && cargo test --features failpoints`
Expected: PASS: every unit, integration and doc test, with and without the feature.

Run: `cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings && cargo doc --no-deps`
Expected: no warnings; docs build.

Run: `cargo test --test proto_sync`
Expected: PASS (`proto/replication.proto` still matches PROTOCOL.md §14 after Tasks 9 and 10).

- [ ] **Step 7: Commit**

```bash
git add tests examples
git commit -m "test: complete the DESIGN.md §21 sweep; verify and repair examples

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

