# Plan 3 of 3 — Integrity Verification, Repair, Observability and the Full Test Sweep Implementation Plan

> **Split:** this file holds Tasks 0–7; Tasks 8–11 are in [part 2](2026-10-09-plan3-verify-part2.md).

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

### Task 0: Carried fixes from Plan 2's final review

**Files:**
- Modify: `src/server/tail.rs`, `src/store/apply.rs`, `src/admin.rs`, `src/slave/repoint.rs`, `tests/promotion.rs`

Four findings of Plan 2's final review, fixed before Plan 3 builds on the same code:

1. **Tail junction race.** The producer read the node's `last_seq` twice per iteration: once to decide whether the client's `prev_crc32c` (the "junction", for a client that was ahead of this replica) can be checked, once to decide whether to read ahead. An entry committed between the two readings let the producer read — and send — `from_seq` before the junction was checked. Now it reads `last_seq` once per iteration, and never reads ahead while a junction is unchecked.
2. **Orphan CF of another name.** A create whose commit failed after `create_cf` leaves `u:a:s` and a create intent naming it. If the next create (at the same seq, after the failure consumed nothing) is for another name, overwriting the intent lost track of `u:a:s`, and the next open failed (`physical column family u:a:s is not in __meta`). Now the orphan is dropped before its intent is overwritten.
3. **A throttled peer blocks a checked promotion.** A peer whose `Status` answered `RESOURCE_EXHAUSTED / BUSY` (for example the failed-authentication limiter) is alive with an unknown state: `check_promotion` must treat it as answered-with-an-error (blocking, `PromotionUnverifiable`), never as unreachable. The classification already does this; this task pins it with tests.
4. **`RepointDivergence` rustdoc.** A repoint settles when its first handshake is answered, so its consent covers that handshake's `DIVERGED` only; a `DIVERGED` from the Tail or Bootstrap that follows is met with `Refuse`. The docs said the mode decides "Tail or Bootstrap" answers too.

**Interfaces:**
- Consumes: Plan 2 `server::tail::{produce, Junction, check_junction}`, `Store::commit` (`CfChange::Create`), `store::test_support::{apply, open_store}`, `admin::{is_transport_failure, node_status}`, `Slave::check_promotion`, `Error::PromotionUnverifiable { peer, seq, diverged }`, `TokenAuthenticator`, `TokenCredentials`, `ServeLimits::max_auth_failures_per_minute`.
- Produces: `server::tail::may_read_ahead(Option<Junction>, ahead_empty: bool, next: Seq, last: Seq) -> bool`; no public API change.

- [ ] **Step 1: Write the failing tests**

In `src/server/tail.rs`, at the start of the test module (after `use super::*;`), add:

```rust
mod tests {
    use super::*;
```

add:

```rust

    /// The node may pass the junction between two readings of its position. With one reading
    /// per iteration, the junction is checked first and only a later iteration reads ahead:
    /// nothing at or after `from_seq` is read before `prev_crc32c` was compared.
    #[test]
    fn nothing_is_read_ahead_while_the_junction_is_unchecked() {
        let j = Junction { seq: 10, crc: 7 };
        assert!(!may_read_ahead(Some(j), true, 11, 12));
        assert!(may_read_ahead(None, true, 11, 12));
        assert!(
            !may_read_ahead(None, false, 11, 12),
            "entries already read ahead go first"
        );
        assert!(!may_read_ahead(None, true, 13, 12), "nothing new yet");
    }
```

In `src/store/apply.rs`, in the test module, insert before the test documented `/// M3: a physical CF left by a commit that failed after `create_cf` is reused only by the`:

```rust
    /// M3: a physical CF left by a commit that failed after `create_cf` is reused only by the
```

insert:

```rust
    /// A failed create left `u:a:1` and its intent; the next create at the same seq is for
    /// another name. The orphan is dropped before its intent is overwritten, or the next open
    /// would find a physical CF that `__meta` does not know.
    #[test]
    fn an_orphan_of_another_name_is_dropped_before_its_intent_is_replaced() {
        use crate::store::{Store, meta};
        use crate::types::{CfEntry, Role};
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            let orphan = CfEntry {
                spec: CfSpec::new("a"),
                create_seq: 1,
            };
            store
                .db()
                .create_cf(orphan.physical_name(), &rocksdb::Options::default())
                .unwrap();
            store
                .write_meta(
                    &[(key::CF_CREATE_INTENT, &meta::encode_cf_entry(&orphan))],
                    true,
                )
                .unwrap();
            assert_eq!(apply(&store, vec![create("b")]).unwrap(), 1);
            assert!(
                store.db().cf_handle("u:a:1").is_none(),
                "the orphan is gone"
            );
        }
        let (_store, m) = Store::open(dir.path(), Role::Master, RocksTuning::new())
            .expect("reopens: no physical CF unknown to __meta");
        let names: Vec<&str> = m.cf_specs.iter().map(|e| e.spec.name.as_str()).collect();
        assert_eq!(names, vec!["b"]);
    }
```

In `src/admin.rs`, at the start of the test module (after `use super::*;`), add:

```rust
mod tests {
    use super::*;
```

add:

```rust

    /// A throttled answer (`BUSY`: `RESOURCE_EXHAUSTED`) comes from a live peer whose state is
    /// unknown: answered with an error, never a transport failure (unreachable).
    #[test]
    fn a_throttled_status_answer_is_not_a_transport_failure() {
        let busy = crate::net::reason_status(pb::Reason::Busy, "too many failed attempts");
        assert_eq!(busy.code(), tonic::Code::ResourceExhausted);
        assert!(!is_transport_failure(&busy));
        assert!(is_transport_failure(&tonic::Status::unavailable(
            "connection refused"
        )));
    }
```

Append to `tests/promotion.rs`:

```rust
/// A peer that throttles the promotion check (too many failed authentications from this
/// address) answers `BUSY`: it is alive and its state is unknown, so a checked promotion is
/// refused as unverifiable instead of ignoring the peer as unreachable.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_throttled_peer_blocks_a_checked_promotion() {
    use replicated_rocksdb::{TokenAuthenticator, TokenCredentials};
    use std::sync::Arc;
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    let (mut cfg, _) = c.slave_config("c", a.serve_addr().unwrap());
    let mut serve = c.serve();
    serve.authenticator = Arc::new(TokenAuthenticator::new(["s3cret"]));
    serve.allow_plaintext_token = true;
    serve.limits.max_auth_failures_per_minute = 1;
    cfg.serve = Some(serve);
    let peer = Slave::open(cfg).await.unwrap();
    assert_converged(&a, &[("b", &b), ("c", &peer)]).await;
    let addr = peer.serve_addr().unwrap();
    // One failed authentication from 127.0.0.1 uses up the peer's budget for this address.
    let mut wrong = UpstreamConfig::from_addr(addr);
    wrong.credentials = Arc::new(TokenCredentials::new("wrong"));
    assert!(admin::node_status(&wrong, "b").await.is_err());
    let mut right = UpstreamConfig::from_addr(addr);
    right.credentials = Arc::new(TokenCredentials::new("s3cret"));
    match b.check_promotion(&[right]).await {
        Err(Error::PromotionUnverifiable { peer, seq, .. }) => {
            assert_eq!(seq, 0, "its state is unknown");
            assert!(peer.contains("too many failed authentication"), "{peer}");
        }
        other => panic!("expected PromotionUnverifiable, got {other:?}"),
    }
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib server::tail && cargo test --lib store::apply`
Expected: FAIL to compile: `cannot find function may_read_ahead in this scope`. (After Step 3 adds it, `an_orphan_of_another_name_is_dropped_before_its_intent_is_replaced` fails until Step 4: `assertion failed: the orphan is gone`.)

Run: `cargo test --lib admin && cargo test --test promotion a_throttled_peer`
Expected: PASS — the classification is already right; these tests pin it.

- [ ] **Step 3: One reading of `last_seq` per producer iteration**

In `src/server/tail.rs`, in `produce`, replace:

```rust
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
```

with:

```rust
        // One reading of the node's position per iteration: the junction check and the
        // read-ahead must agree on it, or an entry committed in between could let `from_seq` be
        // read (and sent) before `prev_crc32c` was checked.
        let last = src.store().position().last_seq;
        // As soon as the server holds `from_seq - 1`, the client's `prev_crc32c` is checked,
        // before anything is sent (PROTOCOL.md §7.2, §7.3).
        if let Some(j) = junction.filter(|j| j.seq <= last) {
            junction = None;
            if let Err(status) = check_junction(&*src, j) {
                return out.end(status).await;
            }
        }
        // Read ahead everything that is available (in blocks of READ_BATCH).
        if may_read_ahead(junction, ahead.is_empty(), next, last) {
```

and add, above `fn heartbeat`:

```rust
fn heartbeat(src: &dyn LogSource) -> pb::TailMessage {
```

insert:

```rust
/// Whether the producer may read entries from `next`, given one reading `last` of the node's
/// `last_seq`. Never while a junction is unchecked: nothing at or after `from_seq` may be read
/// before the client's `prev_crc32c` was compared.
fn may_read_ahead(junction: Option<Junction>, ahead_empty: bool, next: Seq, last: Seq) -> bool {
    junction.is_none() && ahead_empty && next <= last
}
```

- [ ] **Step 4: Drop an orphan before its intent is overwritten**

In `src/store/apply.rs`, in `Store::commit`, replace:

```rust
                if let Some(prev) = leftover.map(|v| meta::decode_cf_entry(&v)).transpose()? {
                    if prev.physical_name() == physical
                        && prev != entry
                        && reuse.is_none()
                        && self.db().cf_handle(&physical).is_some()
                    {
                        return Err(Error::corrupt(format!(
                            "physical CF {physical} was created for {:?} by an unfinished commit; entry {} creates {:?}",
                            prev.spec, p.seq, entry.spec
                        )));
                    }
                }
```

with:

```rust
                if let Some(prev) = leftover.map(|v| meta::decode_cf_entry(&v)).transpose()? {
                    let orphan = prev.physical_name();
                    if orphan == physical
                        && prev != entry
                        && reuse.is_none()
                        && self.db().cf_handle(&physical).is_some()
                    {
                        return Err(Error::corrupt(format!(
                            "physical CF {physical} was created for {:?} by an unfinished commit; entry {} creates {:?}",
                            prev.spec, p.seq, entry.spec
                        )));
                    }
                    // A leftover of another physical name: the intent below overwrites the only
                    // record of it, so drop it now, or the next open would find a physical CF
                    // that `__meta` does not know.
                    if orphan != physical
                        && !live.values().any(|c| c.physical == orphan)
                        && !pending.iter().any(|c| c.physical == orphan)
                        && self.db().cf_handle(&orphan).is_some()
                    {
                        tracing::warn!(cf = %orphan, "dropping a column family left by an unfinished create");
                        self.db().drop_cf(&orphan)?;
                    }
                }
```

- [ ] **Step 5: Fix the `RepointDivergence` rustdoc**

In `src/slave/repoint.rs`, replace:

```rust
/// What a repoint does when the new upstream answers `DIVERGED`. The slave's configured
/// `on_divergence` policy is not used after an explicit repoint: the repoint's mode decides
/// every `DIVERGED` answer (handshake, Tail or Bootstrap) until the next repoint or a restart.
///
/// **Consent to lose data is single-use.** `Rebootstrap` and `Repair` apply only while their own
/// repoint is in progress. Once that repoint settles (with any outcome, including
/// `Retrying` and an error), the mode becomes `Refuse`: a later, unrelated `DIVERGED` halts
/// the slave with its data intact instead of discarding it again. Repoint again with the
/// consent to apply it once more.
```

with:

```rust
/// What a repoint does when the new upstream answers `DIVERGED`. The slave's configured
/// `on_divergence` policy is not used after an explicit repoint: the repoint's mode decides
/// every `DIVERGED` answer until the next repoint or a restart.
///
/// **Consent to lose data is single-use, and in practice covers the repoint's first handshake
/// only.** `Rebootstrap` and `Repair` apply only while their own repoint is in progress, and a
/// repoint settles as soon as that handshake is answered (with any outcome, including
/// `Retrying` and an error); the mode then becomes `Refuse`. A `DIVERGED` that arrives later —
/// from the Tail or Bootstrap that follows the handshake, or from any later session — halts the
/// slave with its data intact instead of discarding it again. Repoint again with the consent to
/// apply it once more.
```

- [ ] **Step 6: Run the tests**

Run: `cargo test --lib server::tail && cargo test --lib store::apply && cargo test --lib admin`
Expected: PASS, including `nothing_is_read_ahead_while_the_junction_is_unchecked`, `an_orphan_of_another_name_is_dropped_before_its_intent_is_replaced`, `a_throttled_status_answer_is_not_a_transport_failure`.

Run: `cargo test --test promotion`
Expected: PASS, including `a_throttled_peer_blocks_a_checked_promotion`.

Run: `cargo test && cargo test --features failpoints`
Expected: PASS.

- [ ] **Step 7: Commit**

```bash
cargo fmt
git add src tests
git commit -m "fix: tail junction race, orphan CF of another name, pin BUSY as blocking, repoint consent docs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 1: Epoch nonces and per-open epochs

**Files:**
- Modify: `proto/replication.proto`, `tests/failpoints.rs`, `src/types.rs`, `src/net/convert.rs`, `src/store/meta.rs`, `src/master/{mod,writer}.rs`, `src/config.rs`, `src/store/promote.rs`, `src/slave/{promote,driver,inner,repoint}.rs`, `src/server/handshake.rs`, `src/server/test_client.rs`, `src/admin.rs`, `src/error.rs`; unit-test literals in `src/server/tests.rs`, `src/slave/apply.rs`, `src/store/{bootstrap,mod}.rs`; `tests/master_write.rs`, `tests/promotion.rs`, `tests/repoint.rs`, `tests/rebootstrap.rs`, `tests/failure_resume.rs`; `PROTOCOL.md` (§5.5, §6.1, §6.3, §6.4, §14, §15), `DESIGN.md` (§3.2, §13, §13.1, §16, §21, §22)

Spec addendum §7 (agreed 2026-10-10). Compatibility of two histories is decided exactly from the epoch history, so a peer whose entries were trimmed is never "unverifiable":

- `EpochStart` gains `nonce: u64` (proto field 3; the local `__meta` history uses the same message). It is random, made when the epoch starts (`EpochStart::fresh`). **Ordering uses only the numbers** (epoch, then `last_seq`); the nonce only says whether two records with the same number are the same epoch. Same number, different nonce ⇒ the histories forked at the lower start seq.
- **Every master open starts a new epoch** (`bump_epoch` before the replication port opens), so one epoch has exactly one writer process and a restored backup needs no flag (`bump_epoch_on_open` is kept, documented as redundant). A promotion starts its own epoch, so `promote` opens the master with `Master::open_promoted`, which does not add a second one.
- The handshake sends the client's newest epoch records as `known_epochs` (field 8, oldest first, ending with its own epoch: the last 5, or all when it has fewer; field 7 `known_epoch_start` is reserved). The server checks every one of them: any record it does not hold identically (start seq or nonce) changes the client's epoch and lowers the fork to that record's start. Rule 9 (`EPOCH_ADVANCED`) holds when all of them are in the server's history (no CRC needed) or the CRCs match; a proven CRC mismatch (10a) is checked first; 10b only without records.
- `admin::compare` decides by the whole histories (nonces included), with CRC probes only as a second line of defence: `Comparison::Unverifiable` and `PromotionCheck::unverifiable` are removed. `PromotionUnverifiable` remains only for a peer that answered `Status` with an error. `repoint::divergence` uses the same history fork (`types::fork_from` over the slave's newest records).

Consequence for existing tests: a master restart (also `a_failed_epoch_bump_fences_the_master_writer`'s reopen) now advances followers by one epoch (`EPOCH_ADVANCED`), and a restarted old master starts its own epoch, which can carry the number the new master already uses (then `DIVERGED`, not `STALE_EPOCH`). The two `STALE_EPOCH` tests bump the new master's epoch first so they still exercise rule 7. Histories are compared as `(epoch, start_seq)` pairs where nonces are random.

**Interfaces:**
- Consumes: Plan 2 `server::handshake::{ServerView, decide, changed_epoch_fork}`, `slave::driver::handshake_request`, `slave::repoint::{divergence, mode_error}`, `admin::{compare, fork_between, choose_promotion_candidate}`, `Slave::check_promotion`, `master::writer::Inner::bump_epoch`, `Store::promote_to_master`, `meta::{encode_epoch_history, decode_epoch_history}`, `net::convert::{epochs_to_pb, epochs_from_pb}`; harness `Cluster::{master, master_config, slave, slave_config, restart_master, restart_master_after_backup, path}`, `restore_dir`, `wait_state`, `wait_until`, `assert_converged`.
- Produces:
  - `pub struct EpochStart { pub epoch: u64, pub start_seq: Seq, pub nonce: u64 }`, `EpochStart::fresh(epoch, start_seq) -> EpochStart` (crate); `types::{KNOWN_EPOCHS = 5, newest_records(history, epoch) -> &[EpochStart], fork_from(history, newest: &[EpochStart]) -> Option<Seq>}` (crate).
  - `pb::EpochStart.nonce`, `pb::HandshakeRequest.known_epochs: Vec<pb::EpochStart>` (field 8, 7 reserved).
  - `net::convert::{epoch_to_pb, epoch_from_pb}`; `Inner::known_epochs() -> Vec<EpochStart>` (slave); `repoint::divergence(local: &[EpochStart], local_last: Seq, hs) -> Error`.
  - `Master::open` starts a new epoch on every open of an existing database; `Master::open_promoted(cfg)` (crate) does not.
  - `admin::Comparison` without `Unverifiable`; `PromotionCheck` without `unverifiable`.
  - Tests: units `rule_09_the_history_proves_continuity_without_the_entry`, `the_same_epoch_with_another_nonce_is_another_epoch`, `every_record_the_client_sends_is_checked`, `up_to_five_newest_records_up_to_an_epoch`, `the_fork_from_a_set_of_records_is_the_lowest_difference`; e2e `a_trimmed_compatible_peer_does_not_block_a_checked_promotion`, `a_peer_on_another_history_blocks_a_checked_promotion_without_its_entries`, `a_restore_without_a_bump_is_diverged_by_the_history_alone`, `a_master_restart_moves_followers_to_the_new_epoch_without_a_bootstrap`.

Every edit below was generated from the tested tree; apply them in order. `cargo fmt` leaves them unchanged.

- [ ] **Step 1: Write the failing end-to-end tests**

Append the new tests (and update the assertions that compare histories with literal records, whose nonces are now random):

In `tests/promotion.rs`, replace:

```rust
use replicated_rocksdb::{
    AllowAll, AllowMethods, CfSpec, DivergencePolicy, EpochStart, Error, HaltReason, LogApi,
    Master, Method, Op, ReadApi, Role, Slave, SlaveState, UpstreamConfig,
};
```

with:

```rust
use replicated_rocksdb::{
    AllowAll, AllowMethods, CfSpec, DivergencePolicy, Error, HaltReason, LogApi, Master, Method,
    Op, ReadApi, Role, Slave, SlaveState, UpstreamConfig,
};
```

In `tests/promotion.rs`, replace:

```rust
    assert_eq!(
        b.epoch_history(),
        vec![
            EpochStart {
                epoch: 1,
                start_seq: 1
            },
            EpochStart {
                epoch: 2,
                start_seq: fork
            }
        ]
    );
```

with:

```rust
    assert_eq!(
        b.epoch_history()
            .iter()
            .map(|e| (e.epoch, e.start_seq))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, fork)]
    );
```

In `tests/promotion.rs`, replace:

```rust
    assert_eq!(cn.status().origin_identity.unwrap().epoch, 2);
```

with:

```rust
    assert_eq!(cn.status().origin_identity.unwrap().epoch, 2);
    // A reopened old master starts an epoch of its own (here number 2, B's number, with
    // another nonce: the handshake answers DIVERGED, see the handshake unit tests). B moves on to
    // epoch 3, so the old master is behind by number: STALE_EPOCH.
    b.bump_epoch().unwrap();
    b.put("c", b"k1", b"v1").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(cn.status().origin_identity.unwrap().epoch, 3);
```

In `tests/promotion.rs`, replace:

```rust
    // B and C tail every entry, so C still holds B's last entry for the CRC probe. D joins
    // later: it is bootstrapped past entry 1 and cannot prove it holds B's history.
    let (b, _) = c.slave("b", a_addr).await;
```

with:

```rust
    // B and C tail every entry, so C still holds B's last entry for the CRC probe. D joins
    // later: it is bootstrapped past entry 1 and does not hold it.
    let (b, _) = c.slave("b", a_addr).await;
```

In `tests/promotion.rs`, replace:

```rust
    let b = refused.into_slave().expect("a refusal returns the slave");
    // D is ahead too, but its history cannot be verified (bootstrapped past B's last entry):
    // refused as well, not treated as diverged and ignored.
    let d_addr = UpstreamConfig::from_addr(d.serve_addr().unwrap());
```

with:

```rust
    let b = refused.into_slave().expect("a refusal returns the slave");
    // D is ahead too. It no longer holds B's last entry (bootstrapped past it), but the
    // epoch histories (nonces included) prove it holds B's history: later, not unverifiable.
    let d_addr = UpstreamConfig::from_addr(d.serve_addr().unwrap());
```

In `tests/promotion.rs`, replace:

```rust
        .err()
        .expect("d is unverifiable");
    assert!(
        matches!(&refused.error, Error::PromotionUnverifiable { peer, seq: 2, .. } if peer == "d"),
        "{refused}"
```

with:

```rust
        .err()
        .expect("d is later");
    assert!(
        matches!(&refused.error, Error::NotMostAdvanced { leader, seq: 2, .. } if leader == "d"),
        "{refused}"
```

In `tests/promotion.rs`, replace:

```rust
    assert_eq!(check.unreachable.len(), 1);
    assert!(check.diverged.is_empty() && check.foreign.is_empty() && check.unverifiable.is_empty());
    let new_master = cn
```

with:

```rust
    assert_eq!(check.unreachable.len(), 1);
    assert!(check.diverged.is_empty() && check.foreign.is_empty());
    let new_master = cn
```

In `tests/promotion.rs`, replace:

```rust
    assert_eq!(
        w_master.epoch_history().last(),
        Some(&EpochStart {
            epoch: 2,
            start_seq: s_w + 1
        })
    );
```

with:

```rust
    assert_eq!(
        w_master
            .epoch_history()
            .last()
            .map(|e| (e.epoch, e.start_seq)),
        Some((2, s_w + 1))
    );
```

In `tests/promotion.rs`, replace:

```rust
    assert_eq!(
        x.promotion_info().epoch_history.last(),
        Some(&EpochStart {
            epoch: 2,
            start_seq: s_w + 1
        })
    );
```

with:

```rust
    assert_eq!(
        x.promotion_info()
            .epoch_history
            .last()
            .map(|e| (e.epoch, e.start_seq)),
        Some((2, s_w + 1))
    );
```

In `tests/promotion.rs`, replace:

```rust

/// Plain `promote` asks no peer and picks epoch 2 again (from another seq). The handshake's
/// `known_epoch_start` tells the two epochs 2 apart: X is not resumed into a history that
/// contradicts its own (which looped every 50 ms before), it advances to R's epoch 2 and
```

with:

```rust

/// Plain `promote` asks no peer and picks epoch 2 again (from another seq, with another
/// nonce). The handshake's `known_epochs` tells the two epochs 2 apart: X is not resumed into a history that
/// contradicts its own (which looped every 50 ms before), it advances to R's epoch 2 and
```

In `tests/promotion.rs`, replace:

```rust
    assert_eq!(
        r.epoch_history().last(),
        Some(&EpochStart {
            epoch: 2,
            start_seq: s_r + 1
        }),
        "same number, other start"
```

with:

```rust
    assert_eq!(
        r.epoch_history().last().map(|e| (e.epoch, e.start_seq)),
        Some((2, s_r + 1)),
        "same number, other start"
```

Append to `tests/promotion.rs`:

```rust
/// Addendum §7: a peer that no longer holds the entry that would prove compatibility is still
/// compared exactly, by its epoch history (nonces included). C is behind B and B has trimmed
/// C's last entry: C does not block `promote_checked`, and then follows the new master
/// without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trimmed_compatible_peer_does_not_block_a_checked_promotion() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = reopen(&c, "b", a_addr, |cfg| {
        cfg.retention.max_entries = Some(5);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    let (cn, _) = c.slave("c", a_addr).await;
    for (name, s) in [("b", &b), ("c", &cn)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    a.put("c", b"seen by c", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let c_last = cn.last_seq();
    cn.shutdown().await.unwrap();
    for i in 0..5u8 {
        a.put("c", &[i], b"only b").unwrap();
    }
    assert_converged(&a, &[("b", &b)]).await;
    wait_until("b keeps only the entries after c's last one", || {
        b.first_retained_seq() == c_last + 1
    })
    .await;
    a.shutdown().await.unwrap();

    // C answers Status (its upstream is gone).
    let (cn, lc) = reopen(&c, "c", a_addr, |_| {}).await;
    let c_up = UpstreamConfig::from_addr(cn.serve_addr().unwrap());
    let check = b
        .check_promotion(std::slice::from_ref(&c_up))
        .await
        .unwrap();
    assert!(
        check.diverged.is_empty() && check.foreign.is_empty() && check.unreachable.is_empty(),
        "{check:?}"
    );
    let b = b
        .promote_checked(c.master_config("b"), std::slice::from_ref(&c_up))
        .await
        .unwrap();
    let outcome = tokio::time::timeout(
        support::WAIT,
        cn.repoint(
            UpstreamConfig::from_addr(b.serve_addr().unwrap()),
            replicated_rocksdb::RepointOptions::default(),
        ),
    )
    .await
    .expect("repoint settles")
    .unwrap();
    assert_eq!(outcome, replicated_rocksdb::RepointOutcome::EpochAdvanced);
    b.put("c", b"after", b"v").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 0, "the history proved continuity");
}

/// Addendum §7: B followed a master that was then restored from a backup. The restored
/// master's open started epoch 2 again, with another nonce, and D bootstrapped from it past
/// every entry B holds. The histories alone prove that D holds entries B's history lacks:
/// promoting B would discard them (`PromotionWouldDiscard`), not "unverifiable".
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_peer_on_another_history_blocks_a_checked_promotion_without_its_entries() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    a.put("c", b"k", b"1").unwrap();
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    let fork = a.last_seq() + 1;
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    for i in 0..3u8 {
        a.put("c", &[i], b"lost").unwrap();
    }
    assert_converged(&a, &[("b", &b)]).await;
    b.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();

    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let a = Master::open(mcfg).await.unwrap();
    assert_eq!(a.identity().epoch, 2, "the same number as the lost epoch");
    for i in 0..5u8 {
        a.put("c", &[i], b"new").unwrap();
    }
    let (d, _) = c.slave("d", a.serve_addr().unwrap()).await;
    assert_converged(&a, &[("d", &d)]).await;
    let d_last = d.last_seq();
    a.shutdown().await.unwrap();

    let (b, _) = reopen(&c, "b", SocketAddr::from(([127, 0, 0, 1], port)), |_| {}).await;
    let d_up = UpstreamConfig::from_addr(d.serve_addr().unwrap());
    let refused = b
        .promote_checked(c.master_config("b"), std::slice::from_ref(&d_up))
        .await
        .err()
        .expect("d holds entries past the fork");
    match refused.error {
        Error::PromotionWouldDiscard {
            ref peer,
            fork: f,
            peer_last_seq,
        } => assert_eq!((peer.as_str(), f, peer_last_seq), ("d", fork, d_last)),
        ref e => panic!("expected PromotionWouldDiscard, got {e}"),
    }
}
```

In `tests/rebootstrap.rs`, replace:

```rust

/// A master restored from an older backup without an epoch bump: a slave that is ahead is
/// DIVERGED (rule 11) and halts by default, or re-bootstraps with `DivergencePolicy::Rebootstrap`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
```

with:

```rust

/// A master restored from an older backup without an epoch bump: its open starts a new epoch at
/// the restore point, so a slave that holds entries past it is DIVERGED (rule 8) and halts by
/// default, or re-bootstraps with `DivergencePolicy::Rebootstrap`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
```

Append to `tests/rebootstrap.rs`:

```rust
/// Addendum §7: a restore without `bump_epoch_on_open` is detected by the epoch history alone.
/// The restored master writes past the slave's position (so rule 11 does not apply) and trims
/// the slave's last entry (so no CRC can be compared): only the new epoch its open started
/// tells the histories apart. Before per-open epochs this resumed silently.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_restore_without_a_bump_is_diverged_by_the_history_alone() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"1").unwrap();
    let m = c.restart_master_after_backup(m, "a", "a-backup").await;
    let (s, _) = c.slave("s", m.serve_addr().unwrap()).await;
    for i in 0..3u8 {
        m.put("c", &[i], b"lost").unwrap();
    }
    assert_converged(&m, &[("s", &s)]).await;
    let s_last = s.last_seq();
    let s_cfg = c.slave_config("s", m.serve_addr().unwrap()).0;
    s.shutdown().await.unwrap();
    let port = m.serve_addr().unwrap().port();
    m.shutdown().await.unwrap();

    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    mcfg.retention.max_entries = Some(1);
    mcfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(mcfg).await.unwrap();
    for i in 0..10u8 {
        m.put("c", &[i], b"new history").unwrap();
    }
    assert!(m.last_seq() > s_last);
    wait_until("the restored master trims the slave's last entry", || {
        m.first_retained_seq() > s_last
    })
    .await;

    let s = Slave::open(s_cfg).await.unwrap();
    wait_state(&s, "Diverged halt", halted(HaltReason::Diverged)).await;
    assert_eq!(
        s.get("c", &[0]).unwrap(),
        Some(b"lost".to_vec()),
        "data intact"
    );
}
```

Append to `tests/failure_resume.rs`:

```rust
/// Addendum §7: every master open starts a new epoch, and its followers move to it with
/// `EPOCH_ADVANCED`, without a bootstrap, even when the master no longer holds a follower's
/// last entry (the history proves continuity; no CRC is needed).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_master_restart_moves_followers_to_the_new_epoch_without_a_bootstrap() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_entries = Some(3);
    mcfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(mcfg.clone()).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    m.put("c", b"k", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let b_last = b.last_seq();
    b.shutdown().await.unwrap();
    for i in 0..3u8 {
        m.put("c", &[i], b"w").unwrap();
    }
    wait_until("the master keeps only the entries after b's", || {
        m.first_retained_seq() == b_last + 1
    })
    .await;
    let m = c.restart_master(m, mcfg).await;
    assert_eq!(m.identity().epoch, 2);

    let b = Slave::open(b_cfg).await.unwrap();
    wait_until("b adopts the new epoch", || {
        b.status().origin_identity == Some(m.identity())
    })
    .await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    m.put("c", b"after the restart", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().origin_identity, Some(m.identity()));
    assert_eq!(b.promotion_info().epoch_history, m.epoch_history());
    assert_eq!(listener.bootstraps(), 1, "only the initial bootstrap");
    listener.assert_no_violations();
}
```

In `tests/master_write.rs`, replace:

```rust
use common::{T0, clock, config, log};
use replicated_rocksdb::{Batch, CfSpec, EpochStart, Error, IterMode, LogApi, Master, Op, ReadApi};

#[tokio::test]
async fn first_open_creates_identity_and_reopen_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
```

with:

```rust
use common::{T0, clock, config, log};
use replicated_rocksdb::{Batch, CfSpec, Error, IterMode, LogApi, Master, Op, ReadApi};

/// Every open of an existing database starts a new epoch (addendum §7): same `db_uuid`, the
/// next epoch number, starting at `last_seq + 1`, with a fresh nonce.
#[tokio::test]
async fn first_open_creates_identity_and_reopen_starts_a_new_epoch() {
    let dir = tempfile::tempdir().unwrap();
```

In `tests/master_write.rs`, replace:

```rust
    assert_eq!(
        m.epoch_history(),
        vec![EpochStart {
            epoch: 1,
            start_seq: 1
        }]
    );
    m.create_cf(CfSpec::new("users")).unwrap();
    m.put("users", b"k", b"v").unwrap();
    m.shutdown().await.unwrap();

    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(m.identity(), id);
    assert_eq!(m.last_seq(), 2);
    assert_eq!(m.get("users", b"k").unwrap().unwrap(), b"v");
    assert_eq!(m.put("users", b"k2", b"v").unwrap(), 3);
}
```

with:

```rust
    assert_eq!(
        m.epoch_history()
            .iter()
            .map(|e| (e.epoch, e.start_seq))
            .collect::<Vec<_>>(),
        vec![(1, 1)]
    );
    m.create_cf(CfSpec::new("users")).unwrap();
    m.put("users", b"k", b"v").unwrap();
    let first = m.epoch_history()[0];
    m.shutdown().await.unwrap();

    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(m.identity().db_uuid, id.db_uuid);
    assert_eq!(m.identity().epoch, 2);
    let history = m.epoch_history();
    assert_eq!(history[0], first, "the old record is kept as it was");
    assert_eq!((history[1].epoch, history[1].start_seq), (2, 3));
    assert_ne!(history[1].nonce, first.nonce);
    assert_eq!(m.last_seq(), 2);
    assert_eq!(m.get("users", b"k").unwrap().unwrap(), b"v");
    assert_eq!(m.put("users", b"k2", b"v").unwrap(), 3);
    assert_eq!(log(&m).last().unwrap().epoch, 2);
    m.shutdown().await.unwrap();
    // An open without writes still starts an epoch (empty epochs share their start seq).
    Master::open(config(dir.path(), &c))
        .await
        .unwrap()
        .shutdown()
        .await
        .unwrap();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(
        m.epoch_history()
            .iter()
            .map(|e| (e.epoch, e.start_seq))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 3), (3, 4), (4, 4)]
    );
}
```

In `tests/master_write.rs`, replace:

```rust
    assert_eq!(
        m.epoch_history(),
        vec![
            EpochStart {
                epoch: 1,
                start_seq: 1
            },
            EpochStart {
                epoch: 2,
                start_seq: 2
            }
        ]
    );
```

with:

```rust
    assert_eq!(
        m.epoch_history()
            .iter()
            .map(|e| (e.epoch, e.start_seq))
            .collect::<Vec<_>>(),
        vec![(1, 1), (2, 2)]
    );
```

In `tests/master_write.rs`, replace:

```rust
    assert_eq!(
        m.epoch_history().last(),
        Some(&EpochStart {
            epoch: 3,
            start_seq: 3
        })
    );
```

with:

```rust
    assert_eq!(
        m.epoch_history().last().map(|e| (e.epoch, e.start_seq)),
        Some((3, 3))
    );
```

In `tests/master_write.rs`, replace:

```rust
    assert_eq!(
        m.epoch_history(),
        vec![EpochStart {
            epoch: 1,
            start_seq: 1
        }]
    );
```

with:

```rust
    assert_eq!(
        m.epoch_history()
            .iter()
            .map(|e| (e.epoch, e.start_seq))
            .collect::<Vec<_>>(),
        vec![(1, 1)]
    );
```

In `tests/repoint.rs`, replace:

```rust
    b.put("c", b"k2", b"v2").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
```

with:

```rust
    b.put("c", b"k2", b"v2").unwrap();
    // The reopened old master starts an epoch of its own, numbered 2 like B's (another nonce:
    // that would be DIVERGED). B moves on to epoch 3, so the old master is behind: STALE_EPOCH.
    b.bump_epoch().unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
```

In `tests/failpoints.rs`, replace:

```rust
/// A failed epoch bump fences the master's writer until reopen (the durability of the bump
/// is unknown); the reopened master is at the old epoch and accepts writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
```

with:

```rust
/// A failed epoch bump fences the master's writer until reopen (the durability of the bump
/// is unknown); the reopened master starts the next epoch and accepts writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
```

In `tests/failpoints.rs`, replace:

```rust
    m.shutdown().await.unwrap();
    let m = Master::open(c.master_config("a")).await.unwrap();
    assert_eq!(m.identity().epoch, 1);
    assert_eq!(m.put("c", b"k", b"v").unwrap(), 2);
```

with:

```rust
    m.shutdown().await.unwrap();
    // The failed bump persisted nothing: the reopen starts epoch 2 (every open starts one).
    let m = Master::open(c.master_config("a")).await.unwrap();
    assert_eq!(m.identity().epoch, 2);
    assert_eq!(m.epoch_history().len(), 2);
    assert_eq!(m.put("c", b"k", b"v").unwrap(), 2);
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --test promotion --test rebootstrap --test failure_resume --test master_write --test repoint`
Expected: FAIL to compile: `no field nonce on type EpochStart` (`tests/master_write.rs`). With that test file set aside, the new tests fail: `a_trimmed_compatible_peer_does_not_block_a_checked_promotion` (the repoint halts `IdentityChanged`: continuity cannot be proven without the trimmed entry), `a_peer_on_another_history_blocks_a_checked_promotion_without_its_entries` (`PromotionUnverifiable`, and the restored master is still at epoch 1), `a_restore_without_a_bump_is_diverged_by_the_history_alone` (the slave resumes) and `a_master_restart_moves_followers_to_the_new_epoch_without_a_bootstrap` (epoch 1 after the restart).

- [ ] **Step 3: The record and the wire**

In `proto/replication.proto` **and** in the PROTOCOL.md §14 block (byte-identical), replace:

In `proto/replication.proto`, replace:

```proto
message EpochStart { uint64 epoch = 1; uint64 start_seq = 2; }
```

with:

```proto
message EpochStart { uint64 epoch = 1; uint64 start_seq = 2; uint64 nonce = 3; }
```

In PROTOCOL.md, the same line appears in §5.5 and in §14; replace it in both places (all 2 occurrences)

```proto
message EpochStart { uint64 epoch = 1; uint64 start_seq = 2; }
```

with:

```proto
message EpochStart { uint64 epoch = 1; uint64 start_seq = 2; uint64 nonce = 3; }
```

In `proto/replication.proto`, in `HandshakeRequest`, replace:

```proto
  optional uint64 known_epoch_start = 7;  // start_seq of known_identity.epoch in the client's history
}
```

with:

```proto
  reserved 7;                              // was known_epoch_start
  repeated EpochStart known_epochs = 8;   // the client's newest epoch records (up to 5), oldest first
}
```

and the same in the PROTOCOL.md §14 block:

```proto
  optional uint64 known_epoch_start = 7;  // start_seq of known_identity.epoch in the client's history
}
```

with:

```proto
  reserved 7;                              // was known_epoch_start
  repeated EpochStart known_epochs = 8;   // the client's newest epoch records (up to 5), oldest first
}
```

In `src/types.rs`, replace:

```rust
/// Start of one epoch in the epoch history (PROTOCOL.md §5.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
```

with:

```rust
/// Start of one epoch in the epoch history (PROTOCOL.md §5.5).
///
/// Which of two nodes is later is decided by `epoch` and `last_seq` only. `nonce` only says
/// whether two records with the same `epoch` are the same epoch: it is random, created when
/// the epoch starts (a fresh master, every master open, `bump_epoch`, a promotion), and two
/// histories that hold the same `epoch` with different nonces forked at the lower of the two
/// `start_seq`s.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
```

In `src/types.rs`, replace:

```rust
    pub start_seq: Seq,
}
```

with:

```rust
    pub start_seq: Seq,
    /// Random identity of this epoch.
    pub nonce: u64,
}

impl EpochStart {
    /// A new epoch with a fresh random nonce.
    pub(crate) fn fresh(epoch: u64, start_seq: Seq) -> EpochStart {
        EpochStart {
            epoch,
            start_seq,
            nonce: uuid::Uuid::new_v4().as_u64_pair().0,
        }
    }
}
```

In `src/types.rs`, replace:

```rust
        .map(|e| e.start_seq)
}
```

with:

```rust
        .map(|e| e.start_seq)
}

/// How many of its newest epoch records a node presents to prove its history (PROTOCOL.md
/// §6.1, `known_epochs`); all of them when it has fewer.
pub(crate) const KNOWN_EPOCHS: usize = 5;

/// The newest [`KNOWN_EPOCHS`] records of `history` up to and including `epoch`, oldest first.
pub(crate) fn newest_records(history: &[EpochStart], epoch: u64) -> &[EpochStart] {
    let end = history.iter().take_while(|e| e.epoch <= epoch).count();
    &history[end.saturating_sub(KNOWN_EPOCHS)..end]
}

/// Where a history whose newest records are `newest` (oldest first; the last one is that node's
/// epoch) leaves `history` (PROTOCOL.md §6.3): the start of the first later epoch in `history`,
/// lowered to the lower of the two start seqs for every record of `newest` that `history` does
/// not hold identically (another start or nonce, or no record of that number). `None` when
/// `history` holds all of `newest` and has no later epoch, or `newest` is empty.
pub(crate) fn fork_from(history: &[EpochStart], newest: &[EpochStart]) -> Option<Seq> {
    let mut fork = fork_after(history, newest.last()?.epoch);
    for k in newest.iter().filter(|k| !history.contains(k)) {
        let own = history.iter().find(|e| e.epoch == k.epoch);
        let start = own.map_or(k.start_seq, |o| o.start_seq.min(k.start_seq));
        fork = Some(fork.map_or(start, |f| f.min(start)));
    }
    fork
}
```

In `src/types.rs`, replace:

```rust
    use super::*;
    use std::time::Duration;
```

with:

```rust
    use super::*;

    fn rec(epoch: u64, start_seq: Seq, nonce: u64) -> EpochStart {
        EpochStart {
            epoch,
            start_seq,
            nonce,
        }
    }

    #[test]
    fn up_to_five_newest_records_up_to_an_epoch() {
        let h: Vec<EpochStart> = (1..=7).map(|e| rec(e, e * 10, e)).collect();
        assert_eq!(newest_records(&h, 7), &h[2..7]);
        assert_eq!(
            newest_records(&h, 3),
            &h[0..3],
            "all of them when there are fewer"
        );
        assert_eq!(newest_records(&h, 6), &h[1..6]);
        assert!(newest_records(&h, 0).is_empty());
    }

    #[test]
    fn the_fork_from_a_set_of_records_is_the_lowest_difference() {
        let server = vec![rec(1, 1, 1), rec(2, 10, 2), rec(3, 20, 3), rec(4, 30, 4)];
        // A prefix: the fork is the next epoch's start.
        assert_eq!(fork_from(&server, &server[..2]), Some(20));
        assert_eq!(fork_from(&server, &server), None);
        // Epoch 3 with another nonce: the histories differ from its start.
        assert_eq!(
            fork_from(&server, &[rec(2, 10, 2), rec(3, 20, 9)]),
            Some(20)
        );
        // An older record differs: the fork moves down to it.
        assert_eq!(
            fork_from(&server, &[rec(2, 12, 7), rec(3, 20, 3)]),
            Some(10)
        );
        // A number the server never had forks at the client's start.
        assert_eq!(
            fork_from(&server[..2], &[rec(1, 1, 1), rec(3, 15, 3)]),
            Some(15)
        );
        assert_eq!(fork_from(&server, &[]), None);
    }
    use std::time::Duration;
```

In `src/types.rs`, replace:

```rust
                start_seq: 1,
            },
```

with:

```rust
                start_seq: 1,
                nonce: 0,
            },
```

In `src/types.rs`, replace:

```rust
                start_seq: 701,
            },
```

with:

```rust
                start_seq: 701,
                nonce: 0,
            },
```

In `src/types.rs`, replace:

```rust
                start_seq: 900,
            },
```

with:

```rust
                start_seq: 900,
                nonce: 0,
            },
```

In `src/net/convert.rs`, replace:

```rust

pub(crate) fn epochs_to_pb(h: &[EpochStart]) -> Vec<pb::EpochStart> {
    h.iter()
        .map(|e| pb::EpochStart {
            epoch: e.epoch,
            start_seq: e.start_seq,
        })
        .collect()
}

pub(crate) fn epochs_from_pb(h: &[pb::EpochStart]) -> Vec<EpochStart> {
    h.iter()
        .map(|e| EpochStart {
            epoch: e.epoch,
            start_seq: e.start_seq,
        })
        .collect()
}
```

with:

```rust

pub(crate) fn epoch_to_pb(e: &EpochStart) -> pb::EpochStart {
    pb::EpochStart {
        epoch: e.epoch,
        start_seq: e.start_seq,
        nonce: e.nonce,
    }
}

pub(crate) fn epoch_from_pb(e: &pb::EpochStart) -> EpochStart {
    EpochStart {
        epoch: e.epoch,
        start_seq: e.start_seq,
        nonce: e.nonce,
    }
}

pub(crate) fn epochs_to_pb(h: &[EpochStart]) -> Vec<pb::EpochStart> {
    h.iter().map(epoch_to_pb).collect()
}

pub(crate) fn epochs_from_pb(h: &[pb::EpochStart]) -> Vec<EpochStart> {
    h.iter().map(epoch_from_pb).collect()
}
```

In `src/net/convert.rs`, replace:

```rust
                start_seq: 1,
            },
```

with:

```rust
                start_seq: 1,
                nonce: 0,
            },
```

In `src/net/convert.rs`, replace:

```rust
                start_seq: 70,
            },
```

with:

```rust
                start_seq: 70,
                nonce: 0,
            },
```

In `src/net/convert.rs`, replace:

```rust
            v.iter()
                .map(|&(epoch, start_seq)| pb::EpochStart { epoch, start_seq })
                .collect::<Vec<_>>()
```

with:

```rust
            v.iter()
                .map(|&(epoch, start_seq)| pb::EpochStart {
                    epoch,
                    start_seq,
                    nonce: 0,
                })
                .collect::<Vec<_>>()
```

In `src/net/convert.rs`, replace:

```rust
                epoch: 1,
                start_seq: 1
            }]
```

with:

```rust
                epoch: 1,
                start_seq: 1,
                nonce: 0,
            }]
```

In `src/store/meta.rs`, replace:

```rust
            epoch: e.epoch,
            start_seq: e.start_seq,
        })
        .collect();
```

with:

```rust
            epoch: e.epoch,
            start_seq: e.start_seq,
            nonce: e.nonce,
        })
        .collect();
```

In `src/store/meta.rs`, replace:

```rust
            start_seq: e.start_seq,
        })
```

with:

```rust
            start_seq: e.start_seq,
            nonce: e.nonce,
        })
```

- [ ] **Step 4: Every master open starts an epoch**

In `src/master/mod.rs`, replace:

```rust
    /// Opens or creates a master database. Must be called within a tokio runtime.
    pub async fn open(cfg: MasterConfig) -> Result<Self> {
        cfg.validate()?;
```

with:

```rust
    /// Opens or creates a master database. Must be called within a tokio runtime.
    ///
    /// Every open of an existing database starts a new epoch (a fresh nonce, starting at
    /// `last_seq + 1`) before the replication port opens: within one epoch there is exactly
    /// one writer process, so two histories can only differ from an epoch's start on, and a
    /// database restored from a backup is told apart from the history its replicas followed
    /// (DESIGN.md §13). Replicas continue across the restart with `EPOCH_ADVANCED`.
    pub async fn open(cfg: MasterConfig) -> Result<Self> {
        Self::open_with(cfg, true).await
    }

    /// Opens a database that a promotion just turned into a master: the promotion already
    /// started its epoch.
    pub(crate) async fn open_promoted(cfg: MasterConfig) -> Result<Self> {
        Self::open_with(cfg, false).await
    }

    async fn open_with(cfg: MasterConfig, new_epoch: bool) -> Result<Self> {
        cfg.validate()?;
```

In `src/master/mod.rs`, replace:

```rust
                };
                let history = vec![EpochStart {
                    epoch: 1,
                    start_seq: 1,
                }];
                store.write_meta(
```

with:

```rust
                };
                let history = vec![EpochStart::fresh(1, 1)];
                store.write_meta(
```

In `src/master/mod.rs`, replace:

```rust
        };
        let bump = cfg.bump_epoch_on_open;
        let inner = Arc::new(Inner::new(store, cfg, meta.last_ts, identity, history));
        if bump && !fresh {
            inner.bump_epoch()?;
        }
        // The epoch bump above happens before the replication port opens (DESIGN.md §13).
        let server =
```

with:

```rust
        };
        let inner = Arc::new(Inner::new(store, cfg, meta.last_ts, identity, history));
        if new_epoch && !fresh {
            inner.bump_epoch()?;
        }
        // The new epoch above starts before the replication port opens (DESIGN.md §13).
        let server =
```

In `src/master/writer.rs`, replace:

```rust
        let start = self.store.position().last_seq + 1;
        history.push(EpochStart {
            epoch: id.epoch,
            start_seq: start,
        });
        let written = crate::failpoint::io_error("master-bump-epoch-write").and_then(|()| {
```

with:

```rust
        let start = self.store.position().last_seq + 1;
        history.push(EpochStart::fresh(id.epoch, start));
        let written = crate::failpoint::io_error("master-bump-epoch-write").and_then(|()| {
```

In `src/config.rs`, replace:

```rust
    pub heartbeat: Duration,
    /// Set when opening a database restored from a backup (DESIGN.md §13).
    pub bump_epoch_on_open: bool,
```

with:

```rust
    pub heartbeat: Duration,
    /// Redundant, kept for compatibility: every open of an existing master database starts a
    /// new epoch anyway (DESIGN.md §13), so a database restored from a backup never reuses the
    /// epoch its replicas may have followed past the restore point.
    pub bump_epoch_on_open: bool,
```

In `src/store/promote.rs`, replace:

```rust
        let epoch = own.max(m.max_burned_epoch).max(floor) + 1;
        history.push(EpochStart {
            epoch,
            start_seq: m.last_seq + 1,
        });
        let identity = DbIdentity {
```

with:

```rust
        let epoch = own.max(m.max_burned_epoch).max(floor) + 1;
        history.push(EpochStart::fresh(epoch, m.last_seq + 1));
        let identity = DbIdentity {
```

In `src/store/promote.rs`, replace:

```rust
                    epoch: 1,
                    start_seq: 1,
                },
                EpochStart {
```

with:

```rust
                    epoch: 1,
                    start_seq: 1,
                    nonce: 0,
                },
                EpochStart {
```

In `src/store/promote.rs`, replace:

```rust
                    start_seq: 1,
                },
```

with:

```rust
                    start_seq: 1,
                    nonce: 0,
                },
```

In `src/store/promote.rs`, replace:

```rust
        assert_eq!(
            m.epoch_history.last(),
            Some(&EpochStart {
                epoch: 3,
                start_seq: 3
            })
        );
```

with:

```rust
        assert_eq!(
            m.epoch_history.last().map(|e| (e.epoch, e.start_seq)),
            Some((3, 3))
        );
```

In `src/store/promote.rs`, replace:

```rust
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path(), Role::Slave, Default::default())
                .unwrap()
                .0;
            let id = DbIdentity {
                db_uuid: uuid::Uuid::from_u128(5),
                epoch: 1,
            };
            store
                .finish_bootstrap(&BootstrapFinish {
                    identity: id,
                    epoch_history: vec![EpochStart {
                        epoch: 1,
                        start_seq: 1,
                    }],
                    cf_specs: vec![],
                    snapshot_seq: 0,
                    snapshot_crc: None,
                    max_entry_bytes: 1 << 20,
                })
                .unwrap();
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new("c"),
                }],
            )
            .unwrap();
            store.promote_to_master(0).unwrap();
```

with:

```rust
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path(), Role::Slave, Default::default())
                .unwrap()
                .0;
            let id = DbIdentity {
                db_uuid: uuid::Uuid::from_u128(5),
                epoch: 1,
            };
            store
                .finish_bootstrap(&BootstrapFinish {
                    identity: id,
                    epoch_history: vec![EpochStart {
                        epoch: 1,
                        start_seq: 1,
                        nonce: 0,
                    }],
                    cf_specs: vec![],
                    snapshot_seq: 0,
                    snapshot_crc: None,
                    max_entry_bytes: 1 << 20,
                })
                .unwrap();
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new("c"),
                }],
            )
            .unwrap();
            store.promote_to_master(0).unwrap();
```

In `src/store/promote.rs`, replace:

```rust
                epoch: 1,
                start_seq: 1
            }]
```

with:

```rust
                epoch: 1,
                start_seq: 1,
                nonce: 0,
            }]
```

In `src/store/promote.rs`, replace:

```rust
                        start_seq: 1,
                    }],
```

with:

```rust
                        start_seq: 1,
                        nonce: 0,
                    }],
```

- [ ] **Step 5: The handshake, the client's record, and the divergence report**

In `src/server/handshake.rs`, replace:

```rust
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::pb;
use crate::types::{DbIdentity, EpochStart, Seq, fork_after};
```

with:

```rust
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::net::convert::epoch_from_pb;
use crate::pb;
use crate::types::{DbIdentity, EpochStart, Seq, fork_after, fork_from};
```

In `src/server/handshake.rs`, replace:

```rust

/// Evaluates PROTOCOL.md §6.3 rules 1–14 (with 10a/10b) in order; the first match wins.
pub(crate) fn decide(v: &ServerView, r: &pb::HandshakeRequest) -> Verdict {
```

with:

```rust

/// Evaluates PROTOCOL.md §6.3 rules 1–14 (with 10a/10b) in order, 10a before 9; the first
/// match wins.
pub(crate) fn decide(v: &ServerView, r: &pb::HandshakeRequest) -> Verdict {
```

In `src/server/handshake.rs`, replace:

```rust
    }
    if let Some(cfork) = changed_epoch_fork(v, known.epoch, ident.epoch, r.known_epoch_start) {
        // 8: the client holds entries at or past the point where its history leaves the server's.
```

with:

```rust
    }
    // The client's newest records count only if they end with its own epoch.
    let known_records: Vec<EpochStart> = r.known_epochs.iter().map(epoch_from_pb).collect();
    let known_records = match known_records.last() {
        Some(k) if k.epoch == known.epoch => known_records,
        _ => Vec::new(),
    };
    if let Some(cfork) = changed_epoch_fork(v, known.epoch, ident.epoch, &known_records) {
        // 8: the client holds entries at or past the point where its history leaves the server's.
```

In `src/server/handshake.rs`, replace:

```rust
        }
        // 9: continuity proven by the CRC of the client's last entry.
        if client_crc_matches {
            return verdict(
```

with:

```rust
        }
        // 10a: a mismatch the server can prove below the fork outweighs any other evidence.
        if r.last_crc32c.is_some() && v.crc_at_client_last.is_some() && !client_crc_matches {
            return verdict(Reject, R::Diverged, format!("entry {} differs", r.last_seq));
        }
        // 9: continuity is proven by the history (the client's newest records, nonces included,
        // are all in the server's history, and the client holds nothing at or past the fork) or
        // by the CRC of the client's last entry.
        let history_proves =
            !known_records.is_empty() && known_records.iter().all(|k| v.epoch_history.contains(k));
        if history_proves || client_crc_matches {
            return verdict(
```

In `src/server/handshake.rs`, replace:

```rust
            );
        }
        // 10a: a proven mismatch below the fork.
        if r.last_crc32c.is_some() && v.crc_at_client_last.is_some() {
            return verdict(Reject, R::Diverged, format!("entry {} differs", r.last_seq));
        }
```

with:

```rust
            );
        }
```

In `src/server/handshake.rs`, replace:

```rust
/// `Some(cfork)` when the client's epoch is *changed* (PROTOCOL.md §6.3): an older epoch number,
/// or the same number with another start seq (`known_epoch_start`) than the server's record of
/// it. `cfork` is where the client's history leaves the server's (`None`: inconsistent).
/// `None` when the client follows the server's epoch.
fn changed_epoch_fork(
```

with:

```rust
/// `Some(cfork)` when the client's epoch is *changed* (PROTOCOL.md §6.3): an older epoch number,
/// or one of the client's newest records (`known_epochs`, its own epoch last) is not in the
/// server's history (another start seq or another nonce, or a number the server never had).
/// `cfork` is where the client's history leaves the server's (`None`: inconsistent). `None`
/// when the client follows the server's epoch.
fn changed_epoch_fork(
```

In `src/server/handshake.rs`, replace:

```rust
    server_epoch: u64,
    known_start: Option<Seq>,
) -> Option<Option<Seq>> {
    let server_start = v
        .epoch_history
        .iter()
        .find(|e| e.epoch == known_epoch)
        .map(|e| e.start_seq);
    // The client's epoch began at another seq than the server's epoch of the same number (or
    // the server never had that epoch): the histories differ from the lower start on.
    let start_fork = known_start
        .filter(|&k| server_start != Some(k))
        .map(|k| server_start.map_or(k, |s| s.min(k)));
    if known_epoch == server_epoch && start_fork.is_none() {
        return None;
    }
    let fork = match (fork_after(&v.epoch_history, known_epoch), start_fork) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    Some(fork)
}
```

with:

```rust
    server_epoch: u64,
    known: &[EpochStart],
) -> Option<Option<Seq>> {
    let differs = known.iter().any(|k| !v.epoch_history.contains(k));
    if known_epoch == server_epoch && !differs {
        return None;
    }
    if known.is_empty() {
        return Some(fork_after(&v.epoch_history, known_epoch));
    }
    Some(fork_from(&v.epoch_history, known))
}
```

In `src/server/handshake.rs`, replace:

```rust

    /// A master at epoch 2 (fork after epoch 1 at seq 101), last_seq 200, log from 50.
    fn master() -> ServerView {
```

with:

```rust

    fn record(epoch: u64, start_seq: Seq, nonce: u64) -> EpochStart {
        EpochStart {
            epoch,
            start_seq,
            nonce,
        }
    }

    /// The client's newest records, oldest first.
    fn known(records: &[(u64, Seq, u64)]) -> Vec<pb::EpochStart> {
        records
            .iter()
            .map(|&(epoch, start_seq, nonce)| pb::EpochStart {
                epoch,
                start_seq,
                nonce,
            })
            .collect()
    }

    /// A master at epoch 2 (fork after epoch 1 at seq 101; nonces 11 and 22), last_seq 200, log
    /// from 50.
    fn master() -> ServerView {
```

In `src/server/handshake.rs`, replace:

```rust
            }),
            epoch_history: vec![
                EpochStart {
                    epoch: 1,
                    start_seq: 1,
                },
                EpochStart {
                    epoch: 2,
                    start_seq: 101,
                },
            ],
            upstream_chain: vec![],
```

with:

```rust
            }),
            epoch_history: vec![record(1, 1, 11), record(2, 101, 22)],
            upstream_chain: vec![],
```

In `src/server/handshake.rs`, replace:

```rust
            last_crc32c: Some(0xAAAA),
            known_epoch_start: Some(101),
        }
```

with:

```rust
            last_crc32c: Some(0xAAAA),
            known_epochs: known(&[(1, 1, 11), (2, 101, 22)]),
        }
```

In `src/server/handshake.rs`, replace:

```rust
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.known_epoch_start = Some(1);
        r.last_seq = last_seq;
```

with:

```rust
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.known_epochs = known(&[(1, 1, 11)]);
        r.last_seq = last_seq;
```

In `src/server/handshake.rs`, replace:

```rust
        check(&master(), &r, Reject, R::Diverged);
        // Without known_epoch_start (the field is optional) the fork is the same.
        let mut r = epoch_1(101);
        r.known_epoch_start = None;
        check(&master(), &r, Reject, R::Diverged);
```

with:

```rust
        check(&master(), &r, Reject, R::Diverged);
        // Without known_epochs (the field is optional) the fork is the same.
        let mut r = epoch_1(101);
        r.known_epochs = Vec::new();
        check(&master(), &r, Reject, R::Diverged);
```

In `src/server/handshake.rs`, replace:

```rust

    #[test]
    fn rule_10b_epoch_change_without_proof() {
        let mut r = epoch_1(30);
        r.last_crc32c = Some(0xAAAA);
        let mut v = master();
```

with:

```rust

    /// The history proves continuity without any entry: the client's record of epoch 1, nonce
    /// included, is in the server's history, so a trimmed entry or a missing CRC is no obstacle.
    #[test]
    fn rule_09_the_history_proves_continuity_without_the_entry() {
        let mut r = epoch_1(30);
        let mut v = master();
        v.crc_at_client_last = None; // entry 30 was trimmed
        check(&v, &r, Resume, R::EpochAdvanced);
        r.last_crc32c = None;
        check(&master(), &r, Resume, R::EpochAdvanced);
        // Still: a proven CRC mismatch wins over the history.
        let mut r = epoch_1(30);
        r.last_crc32c = Some(0xBBBB);
        check(&master(), &r, Reject, R::Diverged);
    }

    #[test]
    fn rule_10b_epoch_change_without_proof() {
        // No record sent: only the CRC can prove continuity.
        let mut r = epoch_1(30);
        r.known_epochs = Vec::new();
        let mut v = master();
```

In `src/server/handshake.rs`, replace:

```rust

    #[test]
    fn rule_08_inconsistent_history_fails_closed() {
        let mut v = master();
        v.epoch_history = vec![EpochStart {
            epoch: 1,
            start_seq: 1,
        }];
        let r = epoch_1(100);
```

with:

```rust

    /// Restoring a master from a backup without a bump: the restored node opens a new epoch 2
    /// at 101 with another nonce. A client of the lost epoch 2 holding entries from 101 on is
    /// DIVERGED by the history alone, even with matching CRCs.
    #[test]
    fn the_same_epoch_with_another_nonce_is_another_epoch() {
        let mut r = req();
        r.known_epochs = known(&[(1, 1, 11), (2, 101, 23)]);
        check(&master(), &r, Reject, R::Diverged);
        check(&replica(), &r, Reject, R::Diverged);
        // Below the start nothing differs, but only the CRC proves it.
        r.last_seq = 100;
        check(&master(), &r, Resume, R::EpochAdvanced);
        let mut v = master();
        v.crc_at_client_last = None;
        check(&v, &r, Bootstrap, R::IdentityChanged);
    }

    /// Up to five newest records are compared, not only the client's own epoch: an older
    /// record with another nonce (a history that forked earlier and reached the same numbers)
    /// moves the fork down to it.
    #[test]
    fn every_record_the_client_sends_is_checked() {
        let mut r = req();
        r.known_epochs = known(&[(1, 1, 99), (2, 101, 22)]);
        check(&master(), &r, Reject, R::Diverged);
        // Records that do not end with the client's own epoch are ignored: identities decide.
        let mut r = req();
        r.known_epochs = known(&[(1, 1, 11)]);
        check(&master(), &r, Resume, R::Unspecified);
    }

    #[test]
    fn rule_08_inconsistent_history_fails_closed() {
        let mut v = master();
        v.epoch_history = vec![record(1, 1, 11)];
        let r = epoch_1(100);
```

In `src/server/handshake.rs`, replace:

```rust
        let mut r = req();
        r.known_epoch_start = Some(81);
        r.last_seq = 80;
```

with:

```rust
        let mut r = req();
        r.known_epochs = known(&[(1, 1, 11), (2, 81, 33)]);
        r.last_seq = 80;
```

In `src/server/handshake.rs`, replace:

```rust
        let mut r = req();
        r.known_epoch_start = Some(121);
        r.last_seq = 101;
```

with:

```rust
        let mut r = req();
        r.known_epochs = known(&[(1, 1, 11), (2, 121, 33)]);
        r.last_seq = 101;
```

In `src/server/handshake.rs`, replace:

```rust
        let mut r = req();
        r.known_epoch_start = None;
        check(&master(), &r, Resume, R::Unspecified);
```

with:

```rust
        let mut r = req();
        r.known_epochs = Vec::new();
        check(&master(), &r, Resume, R::Unspecified);
```

In `src/server/handshake.rs`, replace:

```rust
        });
        v.epoch_history = vec![
            EpochStart {
                epoch: 1,
                start_seq: 1,
            },
            EpochStart {
                epoch: 3,
                start_seq: 101,
            },
        ];
        let mut r = req(); // epoch 2 from 81
        r.known_epoch_start = Some(81);
        r.last_seq = 90;
```

with:

```rust
        });
        v.epoch_history = vec![record(1, 1, 11), record(3, 101, 33)];
        let mut r = req(); // epoch 2 from 81
        r.known_epochs = known(&[(1, 1, 11), (2, 81, 22)]);
        r.last_seq = 90;
```

In `src/server/test_client.rs`, replace:

```rust
        last_crc32c: None,
        known_epoch_start: None,
    }
```

with:

```rust
        last_crc32c: None,
        known_epochs: Vec::new(),
    }
```

In `src/slave/inner.rs`, replace:

```rust
use crate::store::meta::{MetaSnapshot, key};
use crate::types::{DbIdentity, EpochStart, Seq};
```

with:

```rust
use crate::store::meta::{MetaSnapshot, key};
use crate::types::{DbIdentity, EpochStart, Seq, newest_records};
```

In `src/slave/inner.rs`, replace:

```rust
        *self.ident.write().unwrap_or_else(PoisonError::into_inner) = v;
    }
```

with:

```rust
        *self.ident.write().unwrap_or_else(PoisonError::into_inner) = v;
    }

    /// This slave's newest epoch records, up to the epoch it follows (at most
    /// `KNOWN_EPOCHS`, oldest first; empty without data).
    pub(crate) fn known_epochs(&self) -> Vec<EpochStart> {
        self.ident()
            .map(|(id, history)| newest_records(&history, id.epoch).to_vec())
            .unwrap_or_default()
    }
```

In `src/slave/driver.rs`, replace:

```rust
use crate::net::DECODE_SLACK;
use crate::net::convert::{checked_history, entry_from_pb, identity_from_pb, identity_to_pb};
use crate::pb;
```

with:

```rust
use crate::net::DECODE_SLACK;
use crate::net::convert::{
    checked_history, entry_from_pb, epoch_to_pb, identity_from_pb, identity_to_pb,
};
use crate::pb;
```

In `src/slave/driver.rs`, replace:

```rust
        },
        // Tells the server which promotion's epoch this is, not only its number (PROTOCOL §6.3).
        known_epoch_start: ident
            .as_ref()
            .and_then(|(id, h)| h.iter().find(|e| e.epoch == id.epoch).map(|e| e.start_seq)),
    }
```

with:

```rust
        },
        // Tells the server which epochs these are, not only the number: start seqs and nonces
        // of the newest records (PROTOCOL §6.3).
        known_epochs: inner.known_epochs().iter().map(epoch_to_pb).collect(),
    }
```

In `src/slave/driver.rs`, replace:

```rust
                RepointDivergence::Refuse | RepointDivergence::Repair { .. } => {
                    let local_epoch = inner.ident().map_or(0, |(id, _)| id.epoch);
                    let err = repoint::mode_error(mode, || {
                        repoint::divergence(local_epoch, inner.store.position().last_seq, &hs)
                    });
```

with:

```rust
                RepointDivergence::Refuse | RepointDivergence::Repair { .. } => {
                    let err = repoint::mode_error(mode, || {
                        repoint::divergence(
                            &inner.known_epochs(),
                            inner.store.position().last_seq,
                            &hs,
                        )
                    });
```

In `src/slave/repoint.rs`, replace:

```rust
use crate::error::{Error, Result};
use crate::pb;
use crate::types::{EpochStart, Seq, fork_after};
```

with:

```rust
use crate::error::{Error, Result};
use crate::net::convert::epochs_from_pb;
use crate::pb;
use crate::types::{EpochStart, Seq, fork_after, fork_from};
```

In `src/slave/repoint.rs`, replace:

```rust

/// `Error::Diverged` for a `DIVERGED` answer: where the local log leaves the upstream's.
pub(crate) fn divergence(local_epoch: u64, local_last: Seq, hs: &pb::HandshakeResponse) -> Error {
    let history: Vec<EpochStart> = hs
        .epoch_history
        .iter()
        .map(|e| EpochStart {
            epoch: e.epoch,
            start_seq: e.start_seq,
        })
        .collect();
    let fork_seq = match fork_after(&history, local_epoch) {
        // Rule 8: everything from the next epoch's start is not on the upstream's history.
        Some(fork) if fork <= local_last => fork,
```

with:

```rust

/// `Error::Diverged` for a `DIVERGED` answer: where the local log, whose newest epoch records
/// are `local` (oldest first), leaves the upstream's.
pub(crate) fn divergence(
    local: &[EpochStart],
    local_last: Seq,
    hs: &pb::HandshakeResponse,
) -> Error {
    let history = epochs_from_pb(&hs.epoch_history);
    let fork = match local {
        [] => fork_after(&history, 0),
        records => fork_from(&history, records),
    };
    let fork_seq = match fork {
        // Rule 8: everything from the next epoch's start (or from the start of a same-numbered
        // epoch with another nonce) is not on the upstream's history.
        Some(fork) if fork <= local_last => fork,
```

- [ ] **Step 6: Comparing nodes and checking a promotion by history**

In `src/admin.rs`, replace:

```rust
//! 2. Gather the status of every replica with [`gather`], which also probes the entry CRCs
//!    that [`compare`] needs ([`node_status`] reads one node).
//! 3. Promote the most advanced compatible one: [`choose_promotion_candidate`], then
```

with:

```rust
//! 2. Gather the status of every replica with [`gather`], which also probes the entry CRCs
//!    that [`compare`] checks as a second line of defence ([`node_status`] reads one node).
//! 3. Promote the most advanced compatible one: [`choose_promotion_candidate`], then
```

In `src/admin.rs`, replace:

```rust
    Equal,
    /// Both hold different entries from `fork` on: a CRC mismatch was proven (or the epoch
    /// histories fork below both positions).
    Diverged {
        fork: Seq,
    },
    /// Compatibility cannot be proven: the node that would have to hold the other's last
    /// entry no longer does (trimmed, or bootstrapped past it), or no probe was taken.
    /// `later` is the side that is later by position (newest epoch, then `last_seq`); `None`
    /// when both positions are equal.
    Unverifiable {
        later: Option<Side>,
    },
```

with:

```rust
    Equal,
    /// Both hold different entries from `fork` on: the epoch histories (nonces included) fork
    /// below both positions, or a CRC mismatch was proven.
    Diverged {
        fork: Seq,
    },
```

In `src/admin.rs`, replace:

```rust

/// The first seq whose epoch differs between two histories, if any.
fn fork_between(a: &[EpochStart], b: &[EpochStart]) -> Option<Seq> {
```

with:

```rust

/// The first seq whose epoch differs between two histories, if any. Records are compared with
/// their nonces: the same epoch number with another nonce is another epoch.
fn fork_between(a: &[EpochStart], b: &[EpochStart]) -> Option<Seq> {
```

In `src/admin.rs`, replace:

```rust
///
/// Otherwise the nodes are compatible when no seq both hold lies at or past a point where
/// their epoch histories differ, **and** the shorter node's last entry is present in the
/// longer node with the same CRC: equal `last_entry_crc` at equal `last_seq`, or the longer
/// node's probe at the shorter node's `last_seq` ([`NodeInfo::probes`]). The later of two
/// compatible nodes has the newer epoch, then the higher `last_seq`: a node whose newest
/// epoch is still empty (a promotion that took no writes) is later than a node of the older
/// epoch that holds *fewer* entries than its epoch's start, and **diverged** from one that
/// holds entries at or past that start (`fork` is that epoch's `start_seq`; demoting the
/// empty-epoch node and promoting the other would lose nothing). Anything that cannot be
/// shown compatible because of a CRC mismatch is `Diverged`; when the
/// histories agree, `fork` is then the shorter node's `last_seq` (the first seq known not to
/// match may be earlier). A comparison that needs a CRC nobody can supply is `Unverifiable`,
/// never `Compatible` or `Diverged`.
pub fn compare(a: &NodeInfo, b: &NodeInfo) -> Comparison {
```

with:

```rust
///
/// Otherwise the epoch histories decide (addendum §7): every epoch has a random nonce and
/// exactly one writer process, so two nodes whose histories (nonces included) agree below
/// both positions hold the same entries up to the shorter node's `last_seq`, and two whose
/// histories differ below both positions are `Diverged` at the first differing record. Entry
/// CRCs are a second line of defence only: equal `last_seq`s with different
/// `last_entry_crc`s, or the longer node's probe at the shorter node's `last_seq`
/// ([`NodeInfo::probes`]) differing from it, is `Diverged` at that seq; a missing CRC or
/// probe never makes nodes incomparable. The later of two compatible nodes has the newer
/// epoch, then the higher `last_seq`: a node whose newest epoch is still empty (a promotion
/// that took no writes) is later than a node of the older epoch that holds *fewer* entries
/// than its epoch's start, and **diverged** from one that holds entries at or past that start
/// (`fork` is that epoch's `start_seq`; demoting the empty-epoch node and promoting the other
/// would lose nothing).
pub fn compare(a: &NodeInfo, b: &NodeInfo) -> Comparison {
```

In `src/admin.rs`, replace:

```rust
            return match (a.last_entry_crc, b.last_entry_crc) {
                _ if a.last_seq == 0 => by_position.map_or(Comparison::Equal, Comparison::Later),
                (Some(x), Some(y)) if x == y => {
                    by_position.map_or(Comparison::Equal, Comparison::Later)
                }
                (Some(_), Some(_)) => Comparison::Diverged { fork: a.last_seq },
                _ => Comparison::Unverifiable { later: by_position },
            };
```

with:

```rust
            return match (a.last_entry_crc, b.last_entry_crc) {
                (Some(x), Some(y)) if a.last_seq > 0 && x != y => {
                    Comparison::Diverged { fork: a.last_seq }
                }
                _ => by_position.map_or(Comparison::Equal, Comparison::Later),
            };
```

In `src/admin.rs`, replace:

```rust
    }
    if short.last_seq > 0 {
        let held = long.probes.get(&short.last_seq).copied().flatten();
        match (short.last_entry_crc, held) {
            (Some(x), Some(y)) if x == y => {}
            (Some(_), Some(_)) => {
                return Comparison::Diverged {
                    fork: short.last_seq,
                };
            }
            _ => return Comparison::Unverifiable { later: by_position },
        }
```

with:

```rust
    }
    // Second line of defence: a CRC mismatch at the shorter node's last entry.
    let held = long.probes.get(&short.last_seq).copied().flatten();
    if let (Some(x), Some(y)) = (short.last_entry_crc, held) {
        if short.last_seq > 0 && x != y {
            return Comparison::Diverged {
                fork: short.last_seq,
            };
        }
```

In `src/admin.rs`, replace:

```rust
    pub foreign: Vec<String>,
    /// Nodes whose compatibility with the candidate cannot be proven (see
    /// [`Comparison::Unverifiable`]). They are not known to be safe to follow it.
    pub unverifiable: Vec<String>,
}

/// Compatibility is judged by entries. A node whose newest epoch is empty and differs from the
/// candidate's (for example a wrongly promoted master with no writes) is not safe to follow the
/// candidate as it is: `demote` it (or rebootstrap it) first.
```

with:

```rust
    pub foreign: Vec<String>,
    /// Nodes that compare as later than the candidate although the candidate is the latest by
    /// position. This cannot happen with consistent inputs; such a node is not known to be safe
    /// to follow the candidate.
    pub unverifiable: Vec<String>,
}

/// Compatibility is judged by epoch histories (see [`compare`]). A node whose newest epoch is
/// empty and differs from the candidate's (for example a wrongly promoted master with no writes) is not safe to follow the
/// candidate as it is: `demote` it (or rebootstrap it) first.
```

In `src/admin.rs`, replace:

```rust
            Comparison::DifferentDatabase => choice.foreign.push(n.node_id.clone()),
            Comparison::Unverifiable { .. } => choice.unverifiable.push(n.node_id.clone()),
        }
```

with:

```rust
            Comparison::DifferentDatabase => choice.foreign.push(n.node_id.clone()),
        }
```

In `src/admin.rs`, replace:

```rust
            .iter()
            .map(|&(epoch, start_seq)| EpochStart { epoch, start_seq })
            .collect();
```

with:

```rust
            .iter()
            // The same (epoch, start) is the same epoch in these tests: nonce = start.
            .map(|&(epoch, start_seq)| EpochStart {
                epoch,
                start_seq,
                nonce: start_seq,
            })
            .collect();
```

In `src/admin.rs`, replace:

```rust
        assert_eq!(compare(&other_8, &b), Comparison::Diverged { fork: 8 });
        // a no longer holds entry 8 (or was not probed): unverifiable, a still later by position.
        assert_eq!(
            compare(&node("a", 1, &[(1, 1)], 10, Some(1)), &b),
            Comparison::Unverifiable {
                later: Some(Side::A)
            }
        );
        assert_eq!(
            compare(&b, &probed(node("a", 1, &[(1, 1)], 10, Some(1)), 8, None)),
            Comparison::Unverifiable {
                later: Some(Side::B)
            }
        );
```

with:

```rust
        assert_eq!(compare(&other_8, &b), Comparison::Diverged { fork: 8 });
        // a no longer holds entry 8 (or was not probed): the shared history (epoch 1, same
        // nonce) proves b's entries are a's; a is later.
        assert_eq!(
            compare(&node("a", 1, &[(1, 1)], 10, Some(1)), &b),
            Comparison::Later(Side::A)
        );
        assert_eq!(
            compare(&b, &probed(node("a", 1, &[(1, 1)], 10, Some(1)), 8, None)),
            Comparison::Later(Side::B)
        );
        // The same epoch number and start with another nonce is another epoch: a master
        // restored from a backup and reopened. Diverged at that epoch's start.
        let mut restored = node("r", 1, &[(1, 1), (2, 5)], 9, None);
        restored.epoch_history[1].nonce = 77;
        assert_eq!(
            compare(&node("a", 1, &[(1, 1), (2, 5)], 10, None), &restored),
            Comparison::Diverged { fork: 5 }
        );
```

In `src/admin.rs`, replace:

```rust
        );
        // c no longer holds entry 10: b is listed as unverifiable, neither compatible nor diverged.
        let trimmed = vec![
```

with:

```rust
        );
        // c no longer holds entry 10: the shared history still proves b compatible.
        let trimmed = vec![
```

In `src/admin.rs`, replace:

```rust
            (choice.unverifiable, choice.compatible, choice.diverged),
            (vec!["b".to_string()], vec![], vec![])
        );
```

with:

```rust
            (choice.unverifiable, choice.compatible, choice.diverged),
            (vec![], vec!["b".to_string()], vec![])
        );
```

In `src/admin.rs`, replace:

```rust
        assert_eq!(compare(&past, &w), Comparison::Diverged { fork: 11 });
        // Equal position with an unverifiable CRC keeps the order by epoch.
        let no_crc = node("b", 1, &[(1, 1)], 10, None);
        assert_eq!(
            compare(&w, &no_crc),
            Comparison::Unverifiable {
                later: Some(Side::A)
            }
        );
    }
```

with:

```rust
        assert_eq!(compare(&past, &w), Comparison::Diverged { fork: 11 });
        // Equal position without a CRC: the history decides, ordered by epoch.
        let no_crc = node("b", 1, &[(1, 1)], 10, None);
        assert_eq!(compare(&w, &no_crc), Comparison::Later(Side::A));
    }
```

In `src/slave/promote.rs`, replace:

```rust
    /// may hold the same epoch number this promotion picks. The handshake still tells the two
    /// epochs apart by their start seq (PROTOCOL.md §6.3, `known_epoch_start`), so no history
    /// is mixed, but [`promote_checked`](Self::promote_checked), which chooses an epoch above
```

with:

```rust
    /// may hold the same epoch number this promotion picks. The handshake still tells the two
    /// epochs apart by their nonce (PROTOCOL.md §6.3, `known_epochs`), so no history
    /// is mixed, but [`promote_checked`](Self::promote_checked), which chooses an epoch above
```

In `src/slave/promote.rs`, replace:

```rust
        .await?;
        Master::open(cfg).await
    }
```

with:

```rust
        .await?;
        Master::open_promoted(cfg).await
    }
```

In `src/slave/promote.rs`, replace:

```rust
    pub foreign: Vec<String>,
    /// Peers behind this slave whose history cannot be verified against it (`node_id`,
    /// `last_seq`): following it may discard entries they hold.
    pub unverifiable: Vec<(String, Seq)>,
    /// The highest epoch number any reachable peer of this database reported (its identity,
```

with:

```rust
    pub foreign: Vec<String>,
    /// The highest epoch number any reachable peer of this database reported (its identity,
```

In `src/slave/promote.rs`, replace:

```rust

    /// Asks every reachable peer for its `Status` and **refuses by default** (addendum §4):
    /// `Error::NotMostAdvanced` if a peer is proven to hold more of the log, and
    /// `Error::PromotionUnverifiable` if a peer is at least as far by position but its history
    /// cannot be verified (the entry that would prove it is trimmed or before its bootstrap),
    /// `Error::MasterReachable` if a peer reports `role == Master`, and
    /// `Error::PromotionWouldDiscard` (below).
    /// Each peer is probed at this slave's `last_seq`, and this slave's entry at the peer's
    /// `last_seq` is read locally, so [`admin::compare`] can check the shorter node's last
    /// entry. A peer whose history forks from this slave's at `fork` and that holds entries at
    /// or past it blocks with `Error::PromotionWouldDiscard` (promoting this slave would
    /// discard them; promote that node instead, or use plain `promote` to accept the loss).
    /// A diverged peer with nothing at or past the fork is only reported (in the result, and
    /// in a refusal's error); unreachable peers (no answer within `admin::STATUS_TIMEOUT`), foreign
    /// peers and unverifiable peers that are behind are reported too: the operator decides.
    /// A peer that is reachable but answered `Status` with an error (permission denied, another
```

with:

```rust

    /// Asks every reachable peer for its `Status` and **refuses by default** (addendum §4, §7):
    /// `Error::NotMostAdvanced` if a peer holds more of a compatible history,
    /// `Error::MasterReachable` if a peer reports `role == Master`, and
    /// `Error::PromotionWouldDiscard` (below). Compatibility is decided by the epoch histories,
    /// nonces included ([`admin::compare`]), so a peer whose entries were trimmed is never
    /// incomparable; each peer is also probed at this slave's `last_seq`, and this slave's
    /// entry at the peer's `last_seq` is read locally, so a CRC mismatch is caught as a second
    /// line of defence. A peer whose history forks from this slave's at `fork` and that holds
    /// entries at or past it blocks with `Error::PromotionWouldDiscard` (promoting this slave
    /// would discard them; promote that node instead, or use plain `promote` to accept the
    /// loss). A diverged peer with nothing at or past the fork is only reported (in the
    /// result, and in a refusal's error); unreachable peers (no answer within
    /// `admin::STATUS_TIMEOUT`) and foreign peers are reported too: the operator decides.
    /// A peer that is reachable but answered `Status` with an error (permission denied, another
```

In `src/slave/promote.rs`, replace:

```rust
        let mut check = PromotionCheck::default();
        let (mut later, mut unverified_ahead, mut would_discard) =
            (Vec::new(), Vec::new(), Vec::new());
        for peer in peers {
```

with:

```rust
        let mut check = PromotionCheck::default();
        let (mut later, mut unanswered, mut would_discard) = (Vec::new(), Vec::new(), Vec::new());
        for peer in peers {
```

In `src/slave/promote.rs`, replace:

```rust
                // The peer is alive but its state is unknown (refused the call, spoke another
                // protocol, ...): it may be ahead, so it blocks like an unverifiable peer.
                Err(admin::StatusFailure::Answered(e)) => {
                    tracing::warn!(peer = %peer.uri(), error = %e, "promotion check: peer answered with an error");
                    unverified_ahead.push((format!("{} ({e})", peer.uri()), 0));
                    continue;
```

with:

```rust
                // The peer is alive but its state is unknown (refused the call, spoke another
                // protocol, ...): it may be ahead, so it blocks as unverifiable.
                Err(admin::StatusFailure::Answered(e)) => {
                    tracing::warn!(peer = %peer.uri(), error = %e, "promotion check: peer answered with an error");
                    unanswered.push((format!("{} ({e})", peer.uri()), 0));
                    continue;
```

In `src/slave/promote.rs`, replace:

```rust
                Comparison::Later(Side::B) => later.push((info.node_id, info.last_seq)),
                Comparison::Unverifiable {
                    later: Some(Side::B) | None,
                } => unverified_ahead.push((info.node_id, info.last_seq)),
                Comparison::Unverifiable {
                    later: Some(Side::A),
                } => check.unverifiable.push((info.node_id, info.last_seq)),
                // The peer holds entries this slave would discard: refused by default.
```

with:

```rust
                Comparison::Later(Side::B) => later.push((info.node_id, info.last_seq)),
                // The peer holds entries this slave would discard: refused by default.
```

In `src/slave/promote.rs`, replace:

```rust
        }
        if let Some((peer, seq)) = unverified_ahead.into_iter().next() {
            return Err(Error::PromotionUnverifiable {
```

with:

```rust
        }
        if let Some((peer, seq)) = unanswered.into_iter().next() {
            return Err(Error::PromotionUnverifiable {
```

In `src/slave/promote.rs`, replace:

```rust
    /// halted `Corrupt`/`SpecMismatch`, wrong `cfg.path`, invalid `cfg`), a later reachable peer
    /// (`Error::NotMostAdvanced`), a peer at least as far whose history cannot be verified
    /// (`Error::PromotionUnverifiable`), a peer that reports itself a master
```

with:

```rust
    /// halted `Corrupt`/`SpecMismatch`, wrong `cfg.path`, invalid `cfg`), a later reachable peer
    /// (`Error::NotMostAdvanced`), a peer that answered `Status` with an error
    /// (`Error::PromotionUnverifiable`), a peer that reports itself a master
```

In `src/slave/promote.rs`, replace:

```rust
                    || !check.foreign.is_empty()
                    || !check.unverifiable.is_empty()
                {
```

with:

```rust
                    || !check.foreign.is_empty()
                {
```

In `src/error.rs`, replace:

```rust
    },
    /// `promote_checked` found a reachable peer that is at least as far by position (newest
    /// epoch, then `last_seq`) but whose compatibility cannot be proven, because the entry that
    /// would prove it is no longer held. Plain `promote` remains available as an explicit
    /// operator decision. `diverged` as for `NotMostAdvanced`.
    #[error(
        "{peer} (last_seq {seq}) is at least as far and its history cannot be verified; promote explicitly if intended (diverged peers: {diverged:?})"
    )]
```

with:

```rust
    },
    /// `promote_checked` reached a peer that answered `Status` with an error (permission
    /// denied, throttled, another protocol, ...): it is alive and its state is unknown, so it
    /// may be ahead. `peer` holds its address and the error, `seq` is 0. Plain `promote`
    /// remains available as an explicit operator decision. `diverged` as for
    /// `NotMostAdvanced`.
    #[error(
        "{peer} answered Status with an error, so its state is unknown; promote explicitly if intended (diverged peers: {diverged:?})"
    )]
```

- [ ] **Step 7: Unit-test fixtures**

Record literals in unit tests get `nonce: 0`; assertions on a real node's history compare `(epoch, start_seq)`:

In `src/server/tests.rs`, replace:

```rust
use crate::log::LogApi;
use crate::net::convert::identity_to_pb;
use crate::net::reason_of;
```

with:

```rust
use crate::log::LogApi;
use crate::net::convert::{epochs_to_pb, identity_to_pb};
use crate::net::reason_of;
```

In `src/server/tests.rs`, replace:

```rust
    assert_eq!(r.identity, Some(identity_to_pb(&m.identity())));
    assert_eq!(
        r.epoch_history,
        vec![pb::EpochStart {
            epoch: 1,
            start_seq: 1
        }]
    );
```

with:

```rust
    assert_eq!(r.identity, Some(identity_to_pb(&m.identity())));
    assert_eq!(r.epoch_history, epochs_to_pb(&m.epoch_history()));
    assert_eq!(
        (r.epoch_history[0].epoch, r.epoch_history[0].start_seq),
        (1, 1)
    );
```

In `src/slave/apply.rs`, replace:

```rust
                start_seq: 1,
            },
```

with:

```rust
                start_seq: 1,
                nonce: 0,
            },
```

In `src/slave/apply.rs`, replace:

```rust
                start_seq: 11,
            },
```

with:

```rust
                start_seq: 11,
                nonce: 0,
            },
```

In `src/store/bootstrap.rs`, replace:

```rust
                    start_seq: 1,
                },
```

with:

```rust
                    start_seq: 1,
                    nonce: 0,
                },
```

In `src/store/bootstrap.rs`, replace:

```rust
                    start_seq: 3,
                },
```

with:

```rust
                    start_seq: 3,
                    nonce: 0,
                },
```

In `src/store/bootstrap.rs`, replace:

```rust
                start_seq: 1,
            },
```

with:

```rust
                start_seq: 1,
                nonce: 0,
            },
```

In `src/store/bootstrap.rs`, replace:

```rust
                start_seq: 40,
            },
```

with:

```rust
                start_seq: 40,
                nonce: 0,
            },
```

In `src/store/mod.rs`, replace:

```rust
                start_seq: 1,
            },
```

with:

```rust
                start_seq: 1,
                nonce: 0,
            },
```

In `src/store/mod.rs`, replace:

```rust
                start_seq: 50,
            },
```

with:

```rust
                start_seq: 50,
                nonce: 0,
            },
```

- [ ] **Step 8: Run the tests**

Run: `cargo test --lib`
Expected: PASS (195), including every `server::handshake` rule test and the `admin` comparison tests.

Run: `cargo test --test promotion --test rebootstrap --test failure_resume --test master_write --test repoint --test proto_sync`
Expected: PASS, including the four new end-to-end tests.

- [ ] **Step 9: Document it**

In PROTOCOL.md §5.5, replace the first paragraph under **Epoch history**:

```markdown
The **epoch history** of a database is the list of `EpochStart { epoch, start_seq }` records, one per epoch, sorted by `epoch` ascending. The first record is always `{1, 1}`. A new epoch starts when the master bumps its epoch (for example after a restore) or when a replica is promoted to master (§6.4). The new record's `start_seq` is the seq of the first entry written in the new epoch, which is the node's `last_seq + 1` at that moment.
```

with:

```markdown
The **epoch history** of a database is the list of `EpochStart { epoch, start_seq, nonce }` records, one per epoch, sorted by `epoch` ascending. The first record is always `{1, 1, n}`. A new epoch starts **every time a master opens an existing database**, when the master bumps its epoch, and when a replica is promoted to master (§6.4). The new record's `start_seq` is the seq of the first entry written in the new epoch, which is the node's `last_seq + 1` at that moment; an epoch that took no writes shares its `start_seq` with the next one. Within one epoch there is exactly one writer process, so two histories can only differ from the start of an epoch on.

`nonce` is a random 64-bit value chosen when the epoch starts. It never takes part in ordering: which of two nodes is later is decided by the epoch number, then `last_seq`. It only decides **identity**: two records with the same `epoch` are the same epoch if and only if their nonces are equal (their `start_seq`s are then equal too). Two histories that hold the same epoch number with different nonces forked at the lower of the two `start_seq`s. (A restored backup that opens and starts epoch `e` again, or two promotions that chose the same number, are told apart this way.)
```

In §6.1, replace the `known_epoch_start` row:

```markdown
| `known_epoch_start` | The `start_seq` of the record for `known_identity.epoch` in the client's epoch history. Absent if the client has no data. A client with data SHOULD send it: two histories can carry the same epoch *number* with different start seqs (two promotions that chose the same number), and only this field lets the server tell them apart (§6.3). |
```

with:

```markdown
| `known_epochs` | The client's newest epoch records (`epoch`, `start_seq`, `nonce`; §5.5), oldest first and ending with its record of `known_identity.epoch`: the last 5, or all of them when its history is shorter. Empty if the client has no data. A client with data SHOULD send them: they let the server tell apart two epochs with the same number (§6.3) and prove continuity across an epoch change from the history alone, without any entry. A list that does not end with `known_identity.epoch` is ignored. Field 7 (formerly `known_epoch_start`) is reserved. |
```

In §6.3, replace the two derived values:

```markdown
- The client's epoch is **changed** when `known_identity.epoch < identity.epoch`, or when `known_identity.epoch = identity.epoch`, `known_epoch_start` is present and differs from the `start_seq` of the server's record for that epoch. In the second case the two histories share the epoch number but not the epoch: the client follows another promotion that happened to choose the same number, and it is treated exactly like an older epoch.
- The **client fork** `cfork` is `fork(known_identity.epoch)` in the server's history, lowered to `min(known_epoch_start, start_seq of the server's record for known_identity.epoch)` when `known_epoch_start` is present and the server has no record of that epoch with that `start_seq` (if the server has no record of the epoch at all, the server's side of the minimum is omitted). Entries before `cfork` are shared; entries at or after it may differ. `cfork` is "none" only when the epoch is changed and the server's history has no later epoch and no differing start, which is an inconsistent history.
```

with:

```markdown
- The client's epoch is **changed** when `known_identity.epoch < identity.epoch`, or when `known_identity.epoch = identity.epoch` and one of the records in `known_epochs` is not in the server's history (another `start_seq` or another `nonce`, or a number the server never had). In the second case the two histories share the epoch number but not the epoch (a restored backup that started the number again, or another promotion that chose it), and the client is treated exactly like one of an older epoch.
- The **client fork** `cfork` is `fork(known_identity.epoch)` in the server's history, lowered, for every record of `known_epochs` that is not in the server's history, to `min(its start_seq, start_seq of the server's record of the same number)` (if the server has no record of that number, the server's side of the minimum is omitted). Entries before `cfork` are shared; entries at or after it may differ. `cfork` is "none" only when the epoch is changed and the server's history has no later epoch and no differing record, which is an inconsistent history.
- **Continuity is proven by the history** when every record of `known_epochs` is in the server's history (same `epoch`, `start_seq` and `nonce`): every entry before `cfork` was written by the same processes on both sides.
```

and the sentence before the table:

```markdown
The server evaluates these rules **in order** and applies the first that matches:
```

with:

```markdown
The server evaluates these rules **in order** (rule 10a before rule 9) and applies the first that matches:
```

and rows 9–10b:

```markdown
| 11 | The client's epoch is changed, the server holds entry `last_seq`, and its `crc32c` equals `last_crc32c` | `RESUME` | `EPOCH_ADVANCED` |
| 10a | The client's epoch is changed, `last_crc32c` is present, the server holds entry `last_seq`, and its `crc32c` differs | `REJECT` | `DIVERGED` |
| 10b | The client's epoch is changed (continuity cannot be proven: the entry is not held, or `last_crc32c` is absent) | `BOOTSTRAP` | `IDENTITY_CHANGED` |
```

with:

```markdown
| 11 | The client's epoch is changed and continuity is proven: by the history (every record of `known_epochs` is in the server's history), or because the server holds entry `last_seq` and its `crc32c` equals `last_crc32c` | `RESUME` | `EPOCH_ADVANCED` |
| 10a | The client's epoch is changed, `last_crc32c` is present, the server holds entry `last_seq`, and its `crc32c` differs (checked before rule 9) | `REJECT` | `DIVERGED` |
| 10b | The client's epoch is changed (continuity cannot be proven: `known_epochs` is empty, and the entry is not held or `last_crc32c` is absent) | `BOOTSTRAP` | `IDENTITY_CHANGED` |
```

and the notes on rules 8–10 and on same-numbered epochs:

```markdown
- **Rules 8–10, epoch continuity.** After an epoch bump or a promotion, a replica of an older epoch can keep its data if its history is a prefix of the server's. That holds when its last entry comes before the fork point and the server holds the same entry, proven by `crc32c`. Such a client continues without a bootstrap (`EPOCH_ADVANCED`): it adopts the server's identity and history, which replaces its own record of a same-numbered epoch. A client that has entries at or past the fork point holds writes the new history does not have (`DIVERGED`), and so does a client whose last entry is held by the server with a different `crc32c` (rule 10a: the mismatch is proven). Only when the needed entry is not held (trimmed, or before the server's bootstrap) or the client sent no `last_crc32c` is continuity unprovable, and the client is treated as having a changed identity (rule 10b).
- **Same epoch number, different start (`known_epoch_start`).** Epoch numbers are not globally coordinated: a promotion that was undone (`demote`) may have handed its number to replicas that followed it, and another node may later start the same number at another seq. Without `known_epoch_start` such a client would get a plain `RESUME` (equal identities) and then receive entries whose epoch contradicts its history. With it, the server sees that the epochs differ and applies rules 8–10 with the fork at the lower of the two start seqs. A server that receives no `known_epoch_start` compares identities only.
```

with:

```markdown
- **Rules 8–10, epoch continuity.** After a master restart, an epoch bump or a promotion, a replica of an older epoch can keep its data if its history is a prefix of the server's. That holds when its last entry comes before the fork point and its newest records (`known_epochs`), nonces included, are in the server's history: both sides then hold entries written by the same processes. Such a client continues without a bootstrap (`EPOCH_ADVANCED`), even when the server no longer holds the client's last entry: it adopts the server's identity and history, which replaces its own record of a same-numbered epoch. Entry CRCs are a second line of defence: a client whose last entry is held by the server with a different `crc32c` is `DIVERGED` (rule 10a, checked first), and a client that sent no `known_epochs` can still prove continuity by CRC (rule 9). A client that has entries at or past the fork point holds writes the new history does not have (`DIVERGED`). Only a client without `known_epochs` whose entry cannot be compared is treated as having a changed identity (rule 10b).
- **Same epoch number, another epoch (`known_epochs`).** Epoch numbers are not globally coordinated: a master restored from a backup starts again a number its replicas may already have followed past the restore point, a promotion that was undone (`demote`) may have handed its number to replicas that followed it, and another node may later start the same number. Without `known_epochs` such a client would get a plain `RESUME` (equal identities) and then receive entries whose epoch contradicts its history. With them, the server sees that a record differs (start seq or nonce) and applies rules 8–10 with the fork at the lower of the two start seqs; comparing up to five records also catches histories that forked at an older epoch and later reached the same numbers. A server that receives no `known_epochs` compares identities only.
```

and the note on rule 11:

```markdown
- **Rule 11.** The master commits with fsync, so a replica can never be ahead of it legitimately. If one is, the master has lost history, typically because it was restored from a backup without an epoch bump. Accepting the replica would let the same seq carry different writes on different nodes.
```

with:

```markdown
- **Rule 11.** The master commits with fsync, so a replica can never be ahead of it legitimately. If one is, the master has lost history, typically because it was restored from a backup. Since every master open starts a new epoch, rule 8 normally catches this first; rule 11 remains as a backstop. Accepting the replica would let the same seq carry different writes on different nodes.
```

In §6.4, replace the old-master bullet and the closing paragraph:

```markdown
- The old master, if restarted and asked by a new-epoch replica, answers `STALE_EPOCH` (rule 7). It can rejoin only as a new replica with an empty directory.

The protocol cannot stop an operator from running two masters at once. If two nodes both start the same epoch number at different seqs, `known_epoch_start` makes the handshake treat them as different epochs (§6.3); if they start it at the same seq, rule 12 detects the divergence wherever the conflicting entry is still retained.
```

with:

```markdown
- The old master, if restarted, starts an epoch of its own (§5.5). A new-epoch replica that asks it gets `STALE_EPOCH` (rule 7) when the old master's number is lower, and `DIVERGED` otherwise (its record of that number has another nonce, or the old master never had the replica's epoch). It can rejoin only as a new replica with an empty directory.

The protocol cannot stop an operator from running two masters at once. If two nodes both start the same epoch number, their records differ by nonce (and possibly start seq), and `known_epochs` makes the handshake treat them as different epochs (§6.3), wherever the conflicting entries are, retained or not.
```

In the §15 examples, replace:

````markdown
**Master restored from backup without an epoch bump.**

```
B → A  Handshake{known_identity:(U,1), last_seq: 5000}     A.last_seq = 4800
A → B  {decision: REJECT, reason: DIVERGED}               (rule 11)
````

with:

````markdown
**Master restored from backup without an epoch bump.** Its open started epoch 2 at 4801.

```
B → A  Handshake{known_identity:(U,1), last_seq: 5000, known_epochs:[{1,1,n1}]}     A.last_seq = 4800
A → B  {decision: REJECT, reason: DIVERGED}               (rule 8: 5000 ≥ fork(1) = 4801)
````

and:

```markdown
A → …  restarted old master: any (U,2) replica asking A gets STALE_EPOCH (rule 7)
```

with:

```markdown
A → …  restarted old master: its open starts its own epoch 2 (another nonce); a replica of
       B's epoch 2 asking A gets DIVERGED (rule 8), one of a later epoch STALE_EPOCH (rule 7)
```

In DESIGN.md §3.2, replace the `epoch_history` row:

```markdown
| `epoch_history` | all | `Vec<EpochStart { epoch, start_seq }>`, sorted by epoch; first is `{1, 1}` (PROTOCOL.md §5.5). Written atomically with `origin_identity`. |
```

with:

```markdown
| `epoch_history` | all | `Vec<EpochStart { epoch, start_seq, nonce }>`, sorted by epoch; first is `{1, 1, n}` (PROTOCOL.md §5.5). Written atomically with `origin_identity`. |
```

In DESIGN.md §13, replace the first four bullets:

```markdown
- `DbIdentity { db_uuid: Uuid, epoch: u64 }` is created when a master DB is first opened, with `epoch = 1` and epoch history `[{1, 1}]`.
- **Epoch history** (PROTOCOL.md §5.5): one `{epoch, start_seq}` record per epoch. Every entry satisfies `entry.epoch == epoch_for(entry.seq)`. Every node persists the history with its identity and forwards it in the handshake and the bootstrap header.
- **`Master::bump_epoch()`** (admin action): under the writer lock, `epoch += 1`, append `{epoch, last_seq + 1}` to the history, and persist both in one synced batch. **Seq numbering continues.** Then end every replication stream with `UPSTREAM_RESET`.
- **Epoch continuity.** A slave of an older epoch whose last entry lies before the fork point, and whose last entry's CRC matches the upstream's, continues without a bootstrap (`EPOCH_ADVANCED`). It persists the new identity and history before tailing. A slave with entries at or past the fork point is `DIVERGED`. Rules: PROTOCOL.md §6.3 rules 6–10.
```

with:

```markdown
- `DbIdentity { db_uuid: Uuid, epoch: u64 }` is created when a master DB is first opened, with `epoch = 1` and epoch history `[{1, 1, nonce}]`.
- **Epoch history** (PROTOCOL.md §5.5): one `{epoch, start_seq, nonce}` record per epoch; `nonce` is random, chosen when the epoch starts. Ordering uses the numbers only (epoch, then `last_seq`); the nonce only says whether two records of the same number are the same epoch. Every entry satisfies `entry.epoch == epoch_for(entry.seq)`. Every node persists the history with its identity and forwards it in the handshake, the bootstrap header and `Status`.
- **Every master open starts a new epoch** (like a Raft term): `epoch + 1` with a fresh nonce at `last_seq + 1`, persisted before the replication port opens. Within one epoch there is exactly one writer process, so "same seq, same epoch, different content" cannot happen and divergence can only begin at an epoch's start. The history grows by one record per open. A promotion starts its epoch itself; the master it opens does not add another.
- **`Master::bump_epoch()`** (admin action): under the writer lock, `epoch += 1`, append `{epoch, last_seq + 1, fresh nonce}` to the history, and persist both in one synced batch. **Seq numbering continues.** Then end every replication stream with `UPSTREAM_RESET`.
- **Epoch continuity.** A slave of an older epoch with nothing at or past the fork point continues without a bootstrap (`EPOCH_ADVANCED`) when its newest epoch records (up to 5), nonces included, are all in the upstream's history, or (without records) when its last entry's CRC matches the upstream's. A proven CRC mismatch is `DIVERGED` regardless. It persists the new identity and history before tailing. A slave with entries at or past the fork point is `DIVERGED`. The handshake carries those records as `known_epochs`. Rules: PROTOCOL.md §6.3 rules 6–10.
```

and the restore bullet:

```markdown
- **Restoring a master from a backup** must change its epoch before any replica connects. Open it with `MasterConfig { bump_epoch_on_open: true, .. }`. That bumps the epoch before the replication port opens. Slaves at or behind the restored seq with matching entries continue; slaves past it get `DIVERGED`. If the bump is forgotten, the backstop still applies: a slave ahead of the master, or one whose last entry has a different CRC, gets `DIVERGED` instead of silently diverging.
```

with:

```markdown
- **Restoring a master from a backup** needs no special step: its open starts a new epoch at the restored `last_seq + 1` before the replication port opens. Slaves at or behind the restored seq continue (`EPOCH_ADVANCED`); slaves holding entries past it get `DIVERGED`, from the history alone (even when the restored node never held those entries, or already trimmed them). `MasterConfig::bump_epoch_on_open` is kept for compatibility and is redundant. Rules 11 and 12 (a slave ahead of the master, a last entry with a different CRC) remain as backstops.
```

In §13.1, replace the `admin::compare` bullet:

```markdown
- `admin::compare(a, b)` → `Later(A | B)`, `Equal`, `Diverged { fork }` or `DifferentDatabase`. Two nodes are compatible when no seq both hold lies at or past a point where their epoch histories differ **and** the longer node holds the shorter node's last entry with the same CRC (`NodeInfo::probes`, filled by asking the longer node's `Status` with `probe_seq`; equal positions compare `last_entry_crc`). Then the node with the newer epoch is later, then the one with the higher `last_seq`. A proven CRC mismatch is `Diverged`, so same-epoch divergence (a restore without an epoch bump, two masters in one epoch) is caught. When the longer node no longer holds the probed entry (trimmed, or bootstrapped past it), the result is `Unverifiable { later }`: the order by position is still reported, compatibility is not. A node without data is behind every node with data.
```

with:

```markdown
- `admin::compare(a, b)` → `Later(A | B)`, `Equal`, `Diverged { fork }` or `DifferentDatabase`. The epoch histories decide, nonces included: two nodes are compatible when no seq both hold lies at or past the first record where their histories differ, and `Diverged { fork }` at that record's (lower) start seq otherwise. Then the node with the newer epoch is later, then the one with the higher `last_seq`. Entry CRCs are a second line of defence: a CRC mismatch at the shorter node's last entry (`NodeInfo::probes`, filled by asking the longer node's `Status` with `probe_seq`; equal positions compare `last_entry_crc`) is `Diverged` too, but a missing CRC (trimmed, or bootstrapped past it) never makes nodes incomparable. A node without data is behind every node with data.
```

in the chooser bullet, replace:

```markdown
lists the others as compatible, diverged (`(node_id, fork)`), unverifiable or foreign. Compatibility is judged by entries:
```

with:

```markdown
lists the others as compatible, diverged (`(node_id, fork)`) or foreign (`unverifiable` stays empty with consistent inputs). Compatibility is judged by histories:
```

in the `promote_checked` bullet, replace:

```markdown
`Error::NotMostAdvanced { leader, seq, diverged }` if a reachable peer is proven later, `Error::PromotionUnverifiable { peer, seq, diverged }` if a reachable peer is at least as far by position but its history cannot be verified. Both name the proven-diverged peers.
```

with:

```markdown
`Error::NotMostAdvanced { leader, seq, diverged }` if a reachable peer is later on a compatible history, naming the diverged peers.
```

and:

```markdown
Unreachable, foreign, behind-but-unverifiable and diverged peers with nothing at or past the fork are reported and do not block;
```

with:

```markdown
Unreachable and foreign peers and diverged peers with nothing at or past the fork are reported and do not block;
```

in the epoch-numbers bullet, replace:

```markdown
Plain `promote` checks no peer: it can still pick a number an unreachable replica holds. The handshake's `known_epoch_start` (PROTOCOL.md §6.3) then tells the two epochs apart by their start seq and applies the epoch-change rules (continuity proven by CRC, or `DIVERGED`), so histories never mix;
```

with:

```markdown
Plain `promote` checks no peer: it can still pick a number an unreachable replica holds. The handshake's `known_epochs` (PROTOCOL.md §6.3) then tells the two epochs apart by their nonce and applies the epoch-change rules (`DIVERGED` from the lower start seq on), so histories never mix;
```

and the old-master bullet after promotion:

```markdown
- The old master can never rejoin as master. Replicas of the new epoch refuse to follow it (`STALE_EPOCH`, retried and reported). To reuse the machine, wipe its directory and start it as a new slave, which bootstraps.
```

with:

```markdown
- The old master can never rejoin as master. A restart starts an epoch of its own; replicas of the new history refuse to follow it (`STALE_EPOCH` while its number is lower, retried and reported; `DIVERGED` otherwise). To reuse the machine, wipe its directory and start it as a new slave, which bootstraps.
```

In §16, `MasterConfig`:

```markdown
    pub bump_epoch_on_open: bool,       // set when opening a restored backup (§13)
```

with:

```markdown
    pub bump_epoch_on_open: bool,       // redundant: every open starts a new epoch (§13)
```

and `PromotionCheck`:

```markdown
    pub unverifiable: Vec<(String, Seq)>, pub max_peer_epoch: u64,
```

with:

```markdown
    pub max_peer_epoch: u64,
```

In §21, replace:

```markdown
- **Divergence:** master restored from an older backup without an epoch bump → slaves halt `Diverged`; with `bump_epoch_on_open` → slaves at or behind the restore point continue (`EPOCH_ADVANCED`), slaves past it are `DIVERGED` and follow `on_divergence`.
- **Promotion:** A→B, A→C, A→D. Stop A. Promote B. C (behind B) continues from B without a bootstrap; D (ahead of B) is `DIVERGED` and, with `Rebootstrap`, converges. A restarted A is refused with `STALE_EPOCH`.
```

with:

```markdown
- **Divergence:** master restored from an older backup (no flag needed) → slaves past the restore point are `DIVERGED` by the epoch history alone, also when the restored master writes past them and trims their last entry, and follow `on_divergence`; slaves at or behind it continue (`EPOCH_ADVANCED`).
- **Epoch nonces:** every master open starts an epoch; followers move to it with `EPOCH_ADVANCED` and no bootstrap even when their last entry is trimmed; the same epoch number with another nonce is `DIVERGED`; `promote_checked` passes a trimmed compatible peer and blocks a peer on another history (`PromotionWouldDiscard`) without needing its entries.
- **Promotion:** A→B, A→C, A→D. Stop A. Promote B. C (behind B) continues from B without a bootstrap; D (ahead of B) is `DIVERGED` and, with `Rebootstrap`, converges. A restarted A is refused (`STALE_EPOCH` once B's epoch number is above A's).
```

In §22, replace:

```markdown
- Epoch history with fork points lets replicas keep their data across epoch bumps and promotions when their history is a proven prefix.
```

with:

```markdown
- Epoch history with fork points lets replicas keep their data across epoch bumps and promotions when their history is a proven prefix. Every record carries a random nonce and every master open starts an epoch, so compatibility is decided exactly from the histories; entry CRCs are only a second line of defence.
```

In DESIGN.md §16, `Comparison` loses `Unverifiable`:

```markdown
    Diverged { fork: Seq },                     // proven: histories or a CRC differ
    Unverifiable { later: Option<Side> },       // compatibility cannot be proven (entry not held)
    DifferentDatabase,
```

with:

```markdown
    Diverged { fork: Seq },                     // proven: histories (with nonces) or a CRC differ
    DifferentDatabase,
```

In PROTOCOL.md §6.4, replace:

```markdown
appends `{e', last_seq + 1}` to its history, and begins accepting writes at `last_seq + 1`.
```

with:

```markdown
appends `{e', last_seq + 1, fresh nonce}` to its history, and begins accepting writes at `last_seq + 1`.
```

and:

```markdown
A promotion that checks no peers can still choose a number that some unreachable replica already holds; `known_epoch_start` (§6.3) then detects the clash.
```

with:

```markdown
A promotion that checks no peers can still choose a number that some unreachable replica already holds; `known_epochs` (§6.3) then detects the clash by its nonce.
```

- [ ] **Step 10: Run everything and commit**

Run: `cargo fmt --check && cargo test && cargo test --features failpoints && cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings`
Expected: PASS, no warnings.

```bash
git add proto src tests DESIGN.md PROTOCOL.md
git commit -m "feat: epoch nonces and a new epoch on every master open

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: API ergonomics

**Files:**
- Create: `tests/ergonomics.rs`
- Modify: `src/master/mod.rs`, `src/master/write_concern.rs` (an import), `src/slave/mod.rs`, `src/slave/promote.rs`, `src/config.rs`, `src/admin.rs`, `src/error.rs`, `tests/write_concern.rs`, `tests/master_ttl.rs`, `tests/repoint.rs` (unused imports), `examples/{failover,master_and_replica,write_concern,secure_replication}.rs`, `examples/README.md`, `DESIGN.md` (§5.2, §13.1, §16, §16.2)

Spec addendum §8 (agreed 2026-10-10), from writing the user-facing examples:

- **CF operations are exempt from the configured write concern**: `create_cf` / `drop_cf` always commit with the asynchronous default. A fresh master with `min_insync_replicas = 1` can create its CFs before any replica exists.
- `Master::wait_in_sync(n, timeout)` and `Slave::wait_streaming(timeout)` (`Err(Halted(r))` / `Err(Timeout)`) replace hand-rolled polling loops.
- `PromoteRefused::into_parts()` / `DemoteRefused::into_parts()` → `(Error, Option<node>)`: keep the node and still use `?`.
- `MasterConfig::from_slave_config(&SlaveConfig, Option<ServeConfig>)`: the configuration to promote with.
- Inherent `last_seq()` on `Master` and `Slave` (no `LogApi` import), which leaves a few `LogApi` imports unused.
- `admin` reports an unreachable peer with a **one-line** reason (the root cause); `Error::PeerUnreachable` gains `source` with the whole chain, and `Gathered::unreachable` holds the reason only.

**Interfaces:**
- Consumes: Plan 2 `Master::{create_cf, drop_cf, replicas, server}`, `master::writer::Inner::commit_with`, `WriteConcern::default`, `Slave.inner.state_tx`, `PromoteRefused { error, slave }`, `DemoteRefused { error, master }`, `admin::{status_classified, node_status_probed, gather, StatusFailure, is_transport_failure}`, `Slave::check_promotion`; Task 1 `Master::open`.
- Produces:
  - `Master::{last_seq(&self) -> Seq, wait_in_sync(&self, n: usize, timeout: Duration) -> Result<()> (async)}`; `Slave::{last_seq(&self) -> Seq, wait_streaming(&self, timeout: Duration) -> Result<()> (async)}`.
  - `PromoteRefused::into_parts(self) -> (Error, Option<Slave>)`, `DemoteRefused::into_parts(self) -> (Error, Option<Master>)`.
  - `MasterConfig::from_slave_config(slave: &SlaveConfig, serve: Option<ServeConfig>) -> MasterConfig`.
  - `Error::PeerUnreachable { peer, reason, source: Option<Box<dyn std::error::Error + Send + Sync>> }`; `admin::StatusFailure::Unreachable { reason, source }`; `admin::short_reason` (private).
  - Tests: `ergonomics::{wait_streaming_resolves_or_reports_the_halt, promotion_config_and_refusal_parts}`, `admin::tests::unreachable_reasons_are_the_root_cause_on_one_line`; `write_concern::the_configured_write_concern_applies_to_every_write` now checks the CF exemption and `wait_in_sync`.

- [ ] **Step 1: Write the failing tests**

Create `tests/ergonomics.rs`:

```rust
//! The small helpers of addendum §8, end to end.

mod support;

use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, Error, HaltReason, IdentityPolicy, MasterConfig, ReadApi, Slave, SlaveState,
};
use support::{Cluster, WAIT, assert_converged};

/// `Slave::wait_streaming` resolves once the slave tails, and fails at once with the halt
/// reason when it halts instead; `last_seq` is inherent on both node types.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn wait_streaming_resolves_or_reports_the_halt() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    b.wait_streaming(WAIT).await.unwrap();
    assert_eq!(b.status().state, SlaveState::Streaming);
    m.put("c", b"k", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.last_seq(), m.last_seq());

    // Another database at the same address, refused by policy: the wait reports the halt.
    let port = m.serve_addr().unwrap().port();
    m.shutdown().await.unwrap();
    let mut other = c.master_config("other");
    other.serve.as_mut().unwrap().port = port;
    let other = replicated_rocksdb::Master::open(other).await.unwrap();
    b.shutdown().await.unwrap();
    let (mut cfg, _) = c.slave_config("b", other.serve_addr().unwrap());
    cfg.on_identity_change = IdentityPolicy::Refuse;
    let b = Slave::open(cfg).await.unwrap();
    assert!(matches!(
        b.wait_streaming(WAIT).await,
        Err(Error::Halted(HaltReason::IdentityChanged))
    ));
    drop(other);
    // A short wait on a slave that cannot get there times out.
    let (lonely, _) = c.slave("lonely", "127.0.0.1:9".parse().unwrap()).await;
    assert!(matches!(
        lonely.wait_streaming(Duration::from_millis(100)).await,
        Err(Error::Timeout)
    ));
}

/// `MasterConfig::from_slave_config` keeps what a promotion must keep, and a refused
/// promotion's `into_parts` hands back the error and the still-running slave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_config_and_refusal_parts() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg.clone()).await.unwrap();
    b.wait_streaming(WAIT).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;

    let mcfg = MasterConfig::from_slave_config(&cfg, Some(c.serve()));
    assert_eq!(
        (mcfg.identifier.as_str(), &mcfg.path, mcfg.heartbeat),
        ("b", &cfg.path, cfg.heartbeat)
    );
    assert!(mcfg.serve.is_some());

    // The old master is still reachable: refused, and the slave comes back running.
    let master_up = replicated_rocksdb::UpstreamConfig::from_addr(m.serve_addr().unwrap());
    let refused = b
        .promote_checked(mcfg.clone(), std::slice::from_ref(&master_up))
        .await
        .err()
        .expect("the old master is reachable");
    let (error, b) = refused.into_parts();
    assert!(matches!(error, Error::MasterReachable { .. }), "{error}");
    let b = b.expect("a refusal returns the slave");
    assert_eq!(b.get("c", b"k").unwrap(), Some(b"v".to_vec()));

    m.shutdown().await.unwrap();
    let promoted = b.promote(mcfg).await.unwrap();
    assert_eq!(promoted.get("c", b"k").unwrap(), Some(b"v".to_vec()));
    promoted.put("c", b"after", b"w").unwrap();
}
```

In `tests/write_concern.rs`, replace:

```rust

use replicated_rocksdb::{CfSpec, Error, LogApi, Master, SlaveState, UpstreamConfig, WriteConcern};
use support::{Cluster, assert_converged, wait_state, wait_until};
```

with:

```rust

use replicated_rocksdb::{CfSpec, Error, Master, SlaveState, UpstreamConfig, WriteConcern};
use support::{Cluster, assert_converged, wait_state, wait_until};
```

In `tests/write_concern.rs`, replace:

```rust
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_configured_write_concern_applies_to_every_write() {
```

with:

```rust
}

/// The configured write concern applies to every data write; CF operations are exempt
/// (addendum §8): a fresh master with `min_insync_replicas = 1` creates its CF with no replica.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_configured_write_concern_applies_to_every_write() {
```

In `tests/write_concern.rs`, replace:

```rust
    let m = Master::open(cfg).await.unwrap();
    let r = m.create_cf(CfSpec::new("c"));
    assert!(
```

with:

```rust
    let m = Master::open(cfg).await.unwrap();
    assert_eq!(
        m.create_cf(CfSpec::new("c")).unwrap(),
        1,
        "no replica needed"
    );
    let r = m.put("c", b"k", b"v");
    assert!(
```

In `tests/write_concern.rs`, replace:

```rust
    );
    assert_eq!(m.last_seq(), 0);
    // An explicit asynchronous write needs no replica.
```

with:

```rust
    );
    assert_eq!(m.last_seq(), 1, "the refused write consumed no seq");
    // An explicit asynchronous write needs no replica.
```

In `tests/write_concern.rs`, replace:

```rust
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    // The slave bootstraps the empty database (S = 0) and tails; then it is in sync.
    wait_until("b in sync", || m.replicas().iter().any(|r| r.in_sync)).await;
    let m = Arc::new(m);
    let m2 = m.clone();
    tokio::task::spawn_blocking(move || m2.create_cf(CfSpec::new("c")))
        .await
```

with:

```rust
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    m.wait_in_sync(1, support::WAIT).await.unwrap();
    let m = Arc::new(m);
    let m2 = m.clone();
    tokio::task::spawn_blocking(move || m2.put("c", b"k", b"v"))
        .await
```

In `tests/write_concern.rs`, replace:

```rust
        .unwrap();
    assert_converged(&*m, &[("b", &b)]).await;
}
```

with:

```rust
        .unwrap();
    assert_converged(&*m, &[("b", &b)]).await;
    m.drop_cf("c").unwrap();
    assert!(matches!(
        m.wait_in_sync(2, Duration::from_millis(100)).await,
        Err(Error::Timeout)
    ));
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --test ergonomics --test write_concern`
Expected: FAIL to compile: `no method named wait_streaming`, `no function or associated item named from_slave_config`, `no method named into_parts`, `no method named wait_in_sync`.

- [ ] **Step 3: CF operations, `last_seq`, `wait_in_sync`, `into_parts`**

In `src/master/mod.rs`, replace:

```rust
            server,
        })
    }
```

with:

```rust
            server,
        })
    }

    /// The seq of the last committed entry (the same as `LogApi::last_seq`, without the
    /// import).
    pub fn last_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }

    /// Waits until at least `n` direct replicas are in sync (see [`replicas`](Self::replicas)),
    /// for example before writes with `min_insync_replicas = n`. `Err(Timeout)` after
    /// `timeout`; `Err(Config)` if `n > 0` and this master does not serve replication.
    pub async fn wait_in_sync(&self, n: usize, timeout: Duration) -> Result<()> {
        if n == 0 {
            return Ok(());
        }
        if self.server.is_none() {
            return Err(crate::Error::Config(
                "this master does not serve replication".into(),
            ));
        }
        let wait = async {
            loop {
                if self.replicas().iter().filter(|r| r.in_sync).count() >= n {
                    return;
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| crate::Error::Timeout)
    }
```

In `src/master/mod.rs`, replace:

```rust
    ///
    /// With a configured `MasterConfig::write_concern` it also waits for in-sync
    /// acknowledgements and can return `NotEnoughReplicas` or `AckTimeout` (an `AckTimeout` does
    /// not undo the write; see [`write_with`](Self::write_with)).
    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq> {
        validate_cf_name(&spec.name)?;
        self.inner.commit_with(&self.inner.cfg.write_concern, |_| {
            Ok(vec![Op::CreateCf { spec }])
```

with:

```rust
    ///
    /// **Exempt from `MasterConfig::write_concern`** (DESIGN.md §5.2): a CF operation carries
    /// no user data and converges once replicated, so it always commits asynchronously
    /// (fsynced on the master, no acks awaited). A fresh master configured with
    /// `min_insync_replicas = 1` can therefore create its CFs before any replica exists.
    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq> {
        validate_cf_name(&spec.name)?;
        self.inner.commit_with(&WriteConcern::default(), |_| {
            Ok(vec![Op::CreateCf { spec }])
```

In `src/master/mod.rs`, replace:

```rust
    ///
    /// With a configured `MasterConfig::write_concern` it also waits for in-sync
    /// acknowledgements and can return `NotEnoughReplicas` or `AckTimeout` (an `AckTimeout` does
    /// not undo the write; see [`write_with`](Self::write_with)).
    pub fn drop_cf(&self, name: &str) -> Result<Seq> {
```

with:

```rust
    ///
    /// **Exempt from `MasterConfig::write_concern`**, like [`create_cf`](Self::create_cf).
    pub fn drop_cf(&self, name: &str) -> Result<Seq> {
```

In `src/master/mod.rs`, replace:

```rust
        let op = Op::DropCf { name: name.into() };
        self.inner
            .commit_with(&self.inner.cfg.write_concern, |_| Ok(vec![op]))
    }
```

with:

```rust
        let op = Op::DropCf { name: name.into() };
        self.inner
            .commit_with(&WriteConcern::default(), |_| Ok(vec![op]))
    }
```

In `src/master/mod.rs`, replace:

```rust
        self.master.map(|m| *m)
    }
}
```

with:

````rust
        self.master.map(|m| *m)
    }

    /// The error and the master (if it was handed back), to keep the master while still
    /// propagating the error with `?`:
    ///
    /// ```ignore
    /// let slave = match master.demote(cfg).await {
    ///     Ok(slave) => slave,
    ///     Err(refused) => {
    ///         let (error, master) = refused.into_parts();
    ///         keep_serving(master);
    ///         return Err(error.into());
    ///     }
    /// };
    /// ```
    pub fn into_parts(self) -> (crate::Error, Option<Master>) {
        (self.error, self.master.map(|m| *m))
    }
}
````

In `src/master/write_concern.rs`, replace:

```rust
    use crate::error::Error;
    use crate::log::LogApi;
    use crate::server::test_client::{
```

with:

```rust
    use crate::error::Error;
    use crate::server::test_client::{
```

In `src/slave/mod.rs`, replace:

```rust
    pub fn applied_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }
```

with:

```rust
    pub fn applied_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }

    /// Last applied seq (the same as `LogApi::last_seq` and [`applied_seq`](Self::applied_seq),
    /// without the import).
    pub fn last_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }

    /// Waits until the slave is `Streaming` (connected and tailing its upstream).
    /// `Err(Halted(reason))` if it halts first, `Err(Timeout)` after `timeout`.
    pub async fn wait_streaming(&self, timeout: Duration) -> Result<()> {
        let mut states = self.inner.state_tx.subscribe();
        let wait = async {
            loop {
                match &*states.borrow_and_update() {
                    SlaveState::Streaming => return Ok(()),
                    SlaveState::Halted(r) => return Err(Error::Halted(*r)),
                    _ => {}
                }
                if states.changed().await.is_err() {
                    return Err(Error::NotReady);
                }
            }
        };
        tokio::time::timeout(timeout, wait)
            .await
            .map_err(|_| Error::Timeout)?
    }
```

In `src/slave/promote.rs`, replace:

```rust
                Ok(info) => info,
                Err(admin::StatusFailure::Unreachable(e)) => {
                    tracing::warn!(peer = %peer.uri(), error = %e, "promotion check: peer unreachable");
                    check.unreachable.push((peer.uri(), e));
                    continue;
```

with:

```rust
                Ok(info) => info,
                Err(admin::StatusFailure::Unreachable { reason, source }) => {
                    tracing::warn!(peer = %peer.uri(), reason = %reason, error = ?source, "promotion check: peer unreachable");
                    check.unreachable.push((peer.uri(), reason));
                    continue;
```

In `src/slave/promote.rs`, replace:

```rust
        self.slave.map(|s| *s)
    }
}
```

with:

````rust
        self.slave.map(|s| *s)
    }

    /// The error and the slave (if it was handed back), to keep the slave while still
    /// propagating the error with `?`:
    ///
    /// ```ignore
    /// let master = match slave.promote_checked(cfg, &peers).await {
    ///     Ok(master) => master,
    ///     Err(refused) => {
    ///         let (error, slave) = refused.into_parts();
    ///         keep_replicating(slave);
    ///         return Err(error.into());
    ///     }
    /// };
    /// ```
    pub fn into_parts(self) -> (Error, Option<Slave>) {
        (self.error, self.slave.map(|s| *s))
    }
}
````

In `src/config.rs`, replace:

```rust
impl MasterConfig {
    pub fn new(identifier: impl Into<String>, path: impl Into<PathBuf>) -> Self {
```

with:

```rust
impl MasterConfig {
    /// The configuration to promote a slave with: the same identifier, path, retention,
    /// heartbeat, RocksDB tuning and clock, and `serve` as given (usually the slave's own).
    /// Everything else is the default.
    pub fn from_slave_config(
        slave: &crate::slave::SlaveConfig,
        serve: Option<ServeConfig>,
    ) -> Self {
        MasterConfig {
            retention: slave.retention.clone(),
            heartbeat: slave.heartbeat,
            rocksdb: slave.rocksdb.clone(),
            clock: slave.clock.clone(),
            serve,
            ..MasterConfig::new(slave.identifier.clone(), slave.path.clone())
        }
    }

    pub fn new(identifier: impl Into<String>, path: impl Into<PathBuf>) -> Self {
```

- [ ] **Step 4: One-line reasons for unreachable peers**

In `src/error.rs`, replace:

```rust
    /// `admin::node_status` (and `node_status_probed`) could not get an answer from `peer`:
    /// connect error, timeout, or a transport failure. A peer that answered with an error
    /// status is reported as `Config` with the status text instead.
    #[error("{peer} is unreachable: {reason}")]
    PeerUnreachable { peer: String, reason: String },
    #[error(transparent)]
```

with:

```rust
    /// `admin::node_status` (and `node_status_probed`) could not get an answer from `peer`:
    /// connect error, timeout, or a transport failure. `reason` is one short line (the root
    /// cause); the whole chain is available through `source` (and `{:?}`). A peer that answered
    /// with an error status is reported as `Config` with the status text instead.
    #[error("{peer} is unreachable: {reason}")]
    PeerUnreachable {
        peer: String,
        reason: String,
        #[source]
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    #[error(transparent)]
```

In `src/admin.rs`, replace:

```rust
/// address, TLS and credentials. A peer that cannot be reached is
/// `Err(Error::PeerUnreachable { peer, reason })`.
pub async fn node_status(upstream: &UpstreamConfig, client_id: &str) -> Result<NodeInfo> {
```

with:

```rust
/// address, TLS and credentials. A peer that cannot be reached is
/// `Err(Error::PeerUnreachable { peer, reason, source })`, with a one-line `reason`.
pub async fn node_status(upstream: &UpstreamConfig, client_id: &str) -> Result<NodeInfo> {
```

In `src/admin.rs`, replace:

```rust
///
/// `Err(PeerUnreachable { peer, reason })` when no answer could be had (connect error, the
/// [`STATUS_TIMEOUT`], a transport failure); `Err(Config)` with the status text when the peer
```

with:

```rust
///
/// `Err(PeerUnreachable { peer, reason, source })` when no answer could be had (connect error, the
/// [`STATUS_TIMEOUT`], a transport failure); `Err(Config)` with the status text when the peer
```

In `src/admin.rs`, replace:

```rust
        .map_err(|f| match f {
            StatusFailure::Unreachable(reason) => Error::PeerUnreachable {
                peer: upstream.uri(),
                reason,
            },
```

with:

```rust
        .map_err(|f| match f {
            StatusFailure::Unreachable { reason, source } => Error::PeerUnreachable {
                peer: upstream.uri(),
                reason,
                source,
            },
```

In `src/admin.rs`, replace:

```rust
pub(crate) enum StatusFailure {
    Unreachable(String),
    Answered(String),
}
```

with:

```rust
pub(crate) enum StatusFailure {
    /// `reason` is one short line; `source` keeps the whole error.
    Unreachable {
        reason: String,
        source: Option<Box<dyn std::error::Error + Send + Sync>>,
    },
    Answered(String),
}

impl StatusFailure {
    fn unreachable(e: impl std::error::Error + Send + Sync + 'static) -> StatusFailure {
        StatusFailure::Unreachable {
            reason: short_reason(&e),
            source: Some(Box::new(e)),
        }
    }
}

/// One short line for an operator: the innermost cause of `e` (transport errors wrap the
/// useful message, such as "Connection refused", several levels deep), first line only.
fn short_reason(e: &(dyn std::error::Error + 'static)) -> String {
    let mut root = e;
    while let Some(next) = root.source() {
        root = next;
    }
    let text = root.to_string();
    let line = text.lines().next().unwrap_or("").trim();
    if line.is_empty() {
        e.to_string()
            .lines()
            .next()
            .unwrap_or("")
            .trim()
            .to_string()
    } else {
        line.to_string()
    }
}
```

In `src/admin.rs`, replace:

```rust
    let mut c = crate::slave::connect_upstream(upstream, client_id, STATUS_TIMEOUT)
        .map_err(|e| StatusFailure::Unreachable(format!("Status from {uri}: {e}")))?;
    let call = c.status(pb::StatusRequest {
```

with:

```rust
    let mut c = crate::slave::connect_upstream(upstream, client_id, STATUS_TIMEOUT)
        .map_err(StatusFailure::unreachable)?;
    let call = c.status(pb::StatusRequest {
```

In `src/admin.rs`, replace:

```rust
        .await
        .map_err(|_| {
            StatusFailure::Unreachable(format!(
                "Status from {uri}: no answer within {STATUS_TIMEOUT:?}"
            ))
        })?
        .map_err(|s| {
            let m = format!("Status from {uri}: {}: {}", s.code(), s.message());
            if is_transport_failure(&s) {
                StatusFailure::Unreachable(m)
            } else {
                StatusFailure::Answered(m)
            }
```

with:

```rust
        .await
        .map_err(|_| StatusFailure::Unreachable {
            reason: format!("no answer within {STATUS_TIMEOUT:?}"),
            source: None,
        })?
        .map_err(|s| {
            if is_transport_failure(&s) {
                StatusFailure::unreachable(s)
            } else {
                StatusFailure::Answered(format!("Status from {uri}: {}: {}", s.code(), s.message()))
            }
```

In `src/admin.rs`, replace:

```rust
    pub nodes: Vec<NodeInfo>,
    /// Nodes that did not answer, or answered with an error (address, error text);
    /// `Slave::check_promotion` treats the latter as blocking instead.
    pub unreachable: Vec<(String, String)>,
```

with:

```rust
    pub nodes: Vec<NodeInfo>,
    /// Nodes that did not answer, or answered with an error (address, a one-line reason for
    /// the former, the error text for the latter); `Slave::check_promotion` treats the latter
    /// as blocking instead.
    pub unreachable: Vec<(String, String)>,
```

In `src/admin.rs`, replace:

```rust
            Ok(info) => reached.push((up, info)),
            Err(e) => out.unreachable.push((up.uri(), e.to_string())),
```

with:

```rust
            Ok(info) => reached.push((up, info)),
            // One short line: the address is already in the pair.
            Err(Error::PeerUnreachable { reason, .. }) => out.unreachable.push((up.uri(), reason)),
            Err(e) => out.unreachable.push((up.uri(), e.to_string())),
```

In `src/admin.rs`, replace:

```rust

    /// M8: an unreachable peer is reported as such, not as a configuration error.
```

with:

```rust

    #[derive(Debug)]
    struct Wrapped(&'static str, Option<Box<Wrapped>>);
    impl std::fmt::Display for Wrapped {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }
    impl std::error::Error for Wrapped {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    /// The reason for an unreachable peer is the root cause, one line.
    #[test]
    fn unreachable_reasons_are_the_root_cause_on_one_line() {
        let chain = Wrapped(
            "transport error",
            Some(Box::new(Wrapped(
                "tcp connect error",
                Some(Box::new(Wrapped(
                    "Connection refused (os error 111)\nat line 2",
                    None,
                ))),
            ))),
        );
        assert_eq!(short_reason(&chain), "Connection refused (os error 111)");
        assert_eq!(short_reason(&Wrapped("just this", None)), "just this");
    }

    /// M8: an unreachable peer is reported as such, not as a configuration error.
```

In `src/admin.rs`, replace:

```rust
        match node_status(&up, "operator").await {
            Err(Error::PeerUnreachable { peer, reason }) => {
                assert_eq!(peer, up.uri());
                assert!(!reason.is_empty());
            }
```

with:

```rust
        match node_status(&up, "operator").await {
            Err(Error::PeerUnreachable {
                peer,
                reason,
                source,
            }) => {
                assert_eq!(peer, up.uri());
                assert!(!reason.is_empty() && !reason.contains('\n'), "{reason:?}");
                assert!(source.is_some(), "the whole chain is kept");
            }
```

- [ ] **Step 5: Unused imports**

`last_seq()` is now inherent, so these `LogApi` imports are unused (clippy `-D warnings`):

In `tests/master_ttl.rs`, replace:

```rust
use common::{clock, config, log};
use replicated_rocksdb::{Batch, CfSpec, LogApi, Master, Op, ReadApi, ReapStats};
```

with:

```rust
use common::{clock, config, log};
use replicated_rocksdb::{Batch, CfSpec, Master, Op, ReadApi, ReapStats};
```

In `tests/repoint.rs`, replace:

```rust
use replicated_rocksdb::{
    AcceptDataLoss, CfSpec, Error, HaltAction, HaltReason, LogApi, Master, ReadApi,
    RepointDivergence, RepointOptions, RepointOutcome, Slave, SlaveState, UpstreamConfig,
};
```

with:

```rust
use replicated_rocksdb::{
    AcceptDataLoss, CfSpec, Error, HaltAction, HaltReason, Master, ReadApi, RepointDivergence,
    RepointOptions, RepointOutcome, Slave, SlaveState, UpstreamConfig,
};
```

- [ ] **Step 6: Run the tests**

Run: `cargo test --test ergonomics --test write_concern && cargo test --lib admin`
Expected: PASS.

- [ ] **Step 7: Use the helpers in the examples**

Replace the hand-rolled polling loops (`while status().state != Streaming { sleep }`, the `replicas()` in-sync loops), the `PromoteRefused` `map_err`, the hand-written promotion config and the `LogApi` imports:

In `examples/failover.rs`, replace:

```rust
use replicated_rocksdb::{
    AcceptDataLoss, AllowAll, CfSpec, Error, LogApi, Master, MasterConfig, ReadApi,
    RepointDivergence, RepointOptions, ServeConfig, Slave, SlaveConfig, SlaveState, UpstreamConfig,
};
```

with:

```rust
use replicated_rocksdb::{
    AcceptDataLoss, AllowAll, CfSpec, Error, Master, MasterConfig, ReadApi, RepointDivergence,
    RepointOptions, ServeConfig, Slave, SlaveConfig, UpstreamConfig,
};
```

In `examples/failover.rs`, replace:

```rust

/// The config to promote replica `id` with: same directory, now serving as a master.
fn master_cfg(dir: &Path, id: &str) -> MasterConfig {
    let mut cfg = MasterConfig::new(id, dir.join(id));
    cfg.serve = Some(serve());
    cfg
}

async fn streaming(s: &Slave) {
    while s.status().state != SlaveState::Streaming {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}
```

with:

```rust

/// The config to promote replica `id` with: same identifier, directory, retention, tuning
/// and clock as its `SlaveConfig`, now serving as a master.
fn master_cfg(dir: &Path, id: &str) -> MasterConfig {
    let slave_cfg = replica_cfg(dir, id, ([127, 0, 0, 1], 0).into());
    MasterConfig::from_slave_config(&slave_cfg, Some(serve()))
}
```

In `examples/failover.rs`, replace:

```rust
    let c = Slave::open(replica_cfg(dir, "C", a_addr)).await?;
    streaming(&b).await;
    streaming(&c).await;
```

with:

```rust
    let c = Slave::open(replica_cfg(dir, "C", a_addr)).await?;
    b.wait_streaming(WAIT).await?;
    c.wait_streaming(WAIT).await?;
```

In `examples/failover.rs`, replace:

```rust
    for (addr, why) in &gathered.unreachable {
        println!(
            "  unreachable: {addr} ({})",
            why.lines().next().unwrap_or("")
        );
    }
```

with:

```rust
    for (addr, why) in &gathered.unreachable {
        println!("  unreachable: {addr} ({why})");
    }
```

In `examples/failover.rs`, replace:

```rust
    ];
    let new_master = c
        .promote_checked(master_cfg(dir, "C"), &peers)
        .await
        .map_err(|e| e.to_string())?;
    let new_addr = new_master.serve_addr().unwrap();
```

with:

```rust
    ];
    // `into_parts` keeps the slave of a refusal while `?` propagates the error.
    let new_master = match c.promote_checked(master_cfg(dir, "C"), &peers).await {
        Ok(master) => master,
        Err(refused) => {
            let (error, _slave) = refused.into_parts();
            return Err(error.into());
        }
    };
    let new_addr = new_master.serve_addr().unwrap();
```

In `examples/master_and_replica.rs`, replace:

```rust
use replicated_rocksdb::{
    AllowAll, CfSpec, FetchConfig, LogApi, Master, MasterConfig, ReadApi, ServeConfig, Slave,
    SlaveConfig, SlaveState, UpstreamConfig,
};
```

with:

```rust
use replicated_rocksdb::{
    AllowAll, CfSpec, FetchConfig, Master, MasterConfig, ReadApi, ServeConfig, Slave, SlaveConfig,
    SlaveState, UpstreamConfig,
};
```

In `examples/master_and_replica.rs`, replace:

```rust
    // This is the signal `WriteConcern` uses (see the `write_concern` example).
    for _ in 0..200 {
        if master.replicas().iter().any(|r| r.in_sync) {
            break;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    for r in master.replicas() {
```

with:

```rust
    // This is the signal `WriteConcern` uses (see the `write_concern` example).
    master.wait_in_sync(1, Duration::from_secs(10)).await?;
    for r in master.replicas() {
```

In `examples/master_and_replica.rs`, replace:

```rust
    // Let it finish its bootstrap and attach, so the next write travels through the log.
    while master.replicas().len() < 2 || batched_slave.status().state != SlaveState::Streaming {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    let seq = master.put("users", b"carol", b"Carol C.")?;
```

with:

```rust
    // Let it finish its bootstrap and attach, so the next write travels through the log.
    batched_slave
        .wait_streaming(Duration::from_secs(10))
        .await?;
    master.wait_in_sync(2, Duration::from_secs(10)).await?;
    let seq = master.put("users", b"carol", b"Carol C.")?;
```

In `examples/write_concern.rs`, replace:

```rust
    AllowAll, CfSpec, Error, FetchConfig, Master, MasterConfig, ReadApi, Seq, ServeConfig, Slave,
    SlaveConfig, SlaveState, UpstreamConfig, WriteConcern,
};
```

with:

```rust
    AllowAll, CfSpec, Error, FetchConfig, Master, MasterConfig, ReadApi, Seq, ServeConfig, Slave,
    SlaveConfig, UpstreamConfig, WriteConcern,
};
```

In `examples/write_concern.rs`, replace:

```rust
    let slave = Slave::open(cfg).await?;
    while slave.status().state != SlaveState::Streaming {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    Ok(slave)
```

with:

```rust
    let slave = Slave::open(cfg).await?;
    slave.wait_streaming(Duration::from_secs(10)).await?;
    Ok(slave)
```

In `examples/write_concern.rs`, replace:

```rust

    // ---- Concern in the config: applies to EVERY write on this master (put, write,
    // create_cf, ...) unless a call overrides it with the `*_with` variants.
    let mut cfg = MasterConfig::new("master", dir.path().join("master"));
```

with:

```rust

    // ---- Concern in the config: applies to EVERY data write on this master (put, write,
    // delete, ...) unless a call overrides it with the `*_with` variants. CF operations are
    // exempt: they carry no user data.
    let mut cfg = MasterConfig::new("master", dir.path().join("master"));
```

In `examples/write_concern.rs`, replace:

```rust

    // 1. No replica yet => even create_cf is refused, and nothing is written (no seq used).
    match master.create_cf(CfSpec::new("kv")) {
        Err(Error::NotEnoughReplicas { required, in_sync }) => {
            println!("create_cf refused: {in_sync} in sync, {required} required (nothing written)")
        }
```

with:

```rust

    // 1. No replica yet. Creating a CF needs none (CF operations ignore the write concern),
    // but a data write is refused, and nothing is written (no seq used).
    master.create_cf(CfSpec::new("kv"))?;
    let m = master.clone();
    match tokio::task::spawn_blocking(move || m.put("kv", b"early", b"x")).await? {
        Err(Error::NotEnoughReplicas { required, in_sync }) => {
            println!("put refused: {in_sync} in sync, {required} required (nothing written)")
        }
```

In `examples/write_concern.rs`, replace:

```rust
    let fast = open_slave(dir.path(), "fast", addr, FetchConfig::default()).await?;
    wait_in_sync(&master, 1).await;
    master.create_cf(CfSpec::new("kv"))?; // now allowed by the configured concern (1)
```

with:

```rust
    let fast = open_slave(dir.path(), "fast", addr, FetchConfig::default()).await?;
    master.wait_in_sync(1, Duration::from_secs(10)).await?;
```

In `examples/write_concern.rs`, replace:

```rust
    let slow = open_slave(dir.path(), "slow", addr, slow_fetch).await?;
    wait_in_sync(&master, 2).await;
    let tight = WriteConcern {
```

with:

```rust
    let slow = open_slave(dir.path(), "slow", addr, slow_fetch).await?;
    master.wait_in_sync(2, Duration::from_secs(10)).await?;
    let tight = WriteConcern {
```

In `examples/write_concern.rs`, replace:

```rust
}

async fn wait_in_sync(master: &Master, n: usize) {
    while master.replicas().iter().filter(|r| r.in_sync).count() < n {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}
```

with:

```rust
}
```

In `examples/secure_replication.rs`, replace:

```rust
use replicated_rocksdb::{
    CfSpec, HaltReason, Master, MasterConfig, ReadApi, ServeConfig, Slave, SlaveConfig, SlaveState,
    TlsClientConfig, TlsIdentity, TlsServerConfig, TokenAuthenticator, TokenCredentials,
```

with:

```rust
use replicated_rocksdb::{
    CfSpec, HaltReason, Master, MasterConfig, ReadApi, ServeConfig, Slave, SlaveConfig,
    TlsClientConfig, TlsIdentity, TlsServerConfig, TokenAuthenticator, TokenCredentials,
```

In `examples/secure_replication.rs`, replace:

```rust
    .await?;
    while bad.status().state != SlaveState::Halted(HaltReason::AuthRejected) {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
```

with:

```rust
    .await?;
    // `wait_streaming` reports the halt instead of waiting forever.
    match bad.wait_streaming(Duration::from_secs(10)).await {
        Err(replicated_rocksdb::Error::Halted(HaltReason::AuthRejected)) => {}
        other => panic!("expected an AuthRejected halt, got {other:?}"),
    }
```

In `examples/README.md`, replace:

```
| `embedded_master` | A `Master` as a plain local store: CFs, put/get/delete/delete_range, `Batch`, TTL, snapshot, iteration, and the **application-lock read-modify-write pattern** (the crate has no `update`/CAS/merge). | A line per operation; the locked counter ends at exactly `200`. |
| `master_and_replica` | Master serving replication, a `Slave` following it, `wait_for_seq` for read-your-writes, `Master::replicas()` / `Slave::status()`, real-time vs batched `FetchConfig`. | Replica reads, lag/in_sync numbers, and the batched replica lagging by about its `max_wait` (~500 ms). |
| `write_concern` | `WriteConcern` in config and per call; `NotEnoughReplicas` (nothing written) vs `AckTimeout` (committed, do not blindly retry); `spawn_blocking`. | A refused write, an acked write, and a timeout whose entry still reaches the slow replica. |
| `failover` | The operator runbook: stop A, `admin::gather` / `choose_promotion_candidate`, `promote_checked` (incl. `PromoteRefused`), `repoint`, then the `Diverged` / `AcceptDataLoss` path. | Two scenarios: a clean failover, then a deliberately wrong promotion showing `Diverged` and the explicit data-loss opt-in. |
| `secure_replication` | TLS (runtime `rcgen` CA and server cert) plus bearer-token auth; mTLS allow-list explained in comments. | A good-token replica replicates; a bad-token replica halts with `AuthRejected`. |
```

with:

```
| `embedded_master` | A `Master` as a plain local store: CFs, put/get/delete/delete_range, `Batch`, TTL, snapshot, iteration, and the **application-lock read-modify-write pattern** (the crate has no `update`/CAS/merge). | A line per operation; the locked counter ends at exactly `200`. |
| `master_and_replica` | Master serving replication, a `Slave` following it, `wait_streaming` / `wait_in_sync` instead of polling, `wait_for_seq` for read-your-writes, `Master::replicas()` / `Slave::status()`, real-time vs batched `FetchConfig`. | Replica reads, lag/in_sync numbers, and the batched replica lagging by about its `max_wait` (~500 ms). |
| `write_concern` | `WriteConcern` in config and per call (CF operations are exempt); `NotEnoughReplicas` (nothing written) vs `AckTimeout` (committed, do not blindly retry); `Master::wait_in_sync`; `spawn_blocking`. | A refused write, an acked write, and a timeout whose entry still reaches the slow replica. |
| `failover` | The operator runbook: stop A, `admin::gather` / `choose_promotion_candidate`, `promote_checked` (incl. `PromoteRefused::into_parts`), `MasterConfig::from_slave_config`, `repoint`, then the `Diverged` / `AcceptDataLoss` path. | Two scenarios: a clean failover, then a deliberately wrong promotion showing `Diverged` and the explicit data-loss opt-in. |
| `secure_replication` | TLS (runtime `rcgen` CA and server cert) plus bearer-token auth; mTLS allow-list explained in comments. | A good-token replica replicates; a bad-token replica halts with `AuthRejected`. |
```

Run each: `for e in embedded_master master_and_replica write_concern failover secure_replication; do cargo run -q --example $e || break; done`
Expected: all exit 0; `write_concern` prints `put refused: 0 in sync, 1 required (nothing written)`; `failover` prints `unreachable: http://127.0.0.1:<port> (Connection refused (os error 111))` on one line.

- [ ] **Step 8: Document it**

In DESIGN.md §5.2, replace the first sentence:

```markdown
`MasterConfig::write_concern: WriteConcern { min_insync_replicas, ack_timeout }` (default `0` = asynchronous, `5 s`) applies to every write method; `Master::write_with(batch, concern)` and `Master::put_with(cf, key, value, concern)` override it per call.
```

with:

```markdown
`MasterConfig::write_concern: WriteConcern { min_insync_replicas, ack_timeout }` (default `0` = asynchronous, `5 s`) applies to every data write method; `Master::write_with(batch, concern)` and `Master::put_with(cf, key, value, concern)` override it per call. **CF operations are exempt:** `create_cf` and `drop_cf` carry no user data and converge once replicated, so they always commit asynchronously (fsynced on the master, no acks awaited), whatever the configured concern; a fresh master with `min_insync_replicas = 1` can create its CFs before any replica exists. `Master::wait_in_sync(n, timeout)` waits until at least `n` direct replicas are in sync (`Err(Timeout)` otherwise).
```

In DESIGN.md §16, add before `impl Master` and extend its first lines:

```markdown
impl Master {
    pub async fn open(cfg: MasterConfig) -> Result<Self>;
    pub fn identity(&self) -> DbIdentity;
```

with:

```markdown
impl MasterConfig {
    pub fn new(identifier: impl Into<String>, path: impl Into<PathBuf>) -> Self;
    pub fn from_slave_config(slave: &SlaveConfig, serve: Option<ServeConfig>) -> Self;
                                        // to promote: same identifier, path, retention, heartbeat,
                                        // tuning and clock
}

impl Master {
    pub async fn open(cfg: MasterConfig) -> Result<Self>;   // starts a new epoch (§13)
    pub fn identity(&self) -> DbIdentity;
    pub fn last_seq(&self) -> Seq;                          // also via LogApi
```

and:

```markdown
    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq>;
    pub fn drop_cf(&self, name: &str) -> Result<Seq>;

    pub fn snapshot(&self) -> Result<ReadSnapshot>;
    pub fn replicas(&self) -> Vec<ReplicaStatus>;      // direct downstream only
```

with:

```markdown
    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq>;   // CF ops ignore the write concern (§5.2)
    pub fn drop_cf(&self, name: &str) -> Result<Seq>;

    pub fn snapshot(&self) -> Result<ReadSnapshot>;
    pub fn replicas(&self) -> Vec<ReplicaStatus>;      // direct downstream only
    pub async fn wait_in_sync(&self, n: usize, timeout: Duration) -> Result<()>;   // Err(Timeout)
```

In the `Slave` methods:

```markdown
    pub fn applied_seq(&self) -> Seq;
    pub fn serve_addr(&self) -> Option<SocketAddr>;     // when serve is configured
    pub async fn wait_for_seq(&self, seq: Seq, timeout: Duration) -> Result<()>;
```

with:

```markdown
    pub fn applied_seq(&self) -> Seq;
    pub fn last_seq(&self) -> Seq;                       // = applied_seq; also via LogApi
    pub fn serve_addr(&self) -> Option<SocketAddr>;     // when serve is configured
    pub async fn wait_for_seq(&self, seq: Seq, timeout: Duration) -> Result<()>;
    pub async fn wait_streaming(&self, timeout: Duration) -> Result<()>;   // Err(Halted(r)) | Err(Timeout)
```

In §13.1, extend the refusal bullet:

```markdown
with `into_slave()` / `into_master()`.
```

with:

```markdown
with `into_slave()` / `into_master()`, or `into_parts()` → `(Error, Option<node>)` to keep the node while propagating the error with `?`.
```

In the `admin` block:

```markdown
                                                // unreachable → Error::PeerUnreachable { peer, reason }
```

with:

```markdown
                                                // unreachable → Error::PeerUnreachable { peer, reason, source }:
                                                // `reason` is one line (the root cause), `source` the whole chain
```

In §16.2:

```markdown
`PeerUnreachable { peer, reason }` (`admin::node_status*` got no answer)
```

with:

```markdown
`PeerUnreachable { peer, reason, source }` (`admin::node_status*` got no answer; `reason` is one line)
```

- [ ] **Step 9: Run everything and commit**

Run: `cargo fmt --check && cargo test && cargo test --features failpoints && cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings && cargo doc --no-deps`
Expected: PASS, no warnings (`--all-targets` includes the examples).

```bash
git add src tests examples DESIGN.md
git commit -m "feat: API ergonomics (CF ops ignore write concern, wait helpers, into_parts, from_slave_config)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Verify core — types, range arithmetic, snapshot scans and the comparison algorithm

**Files:**
- Create: `src/verify/mod.rs`, `src/verify/range.rs`, `src/verify/scan.rs`, `src/verify/compare.rs`
- Modify: `src/store/repl.rs` (add `ReadCtx::iter_raw_from`), `src/error.rs`, `src/lib.rs`

**Interfaces:**
- Consumes: Plan 1 `codec::digest::Digest::{new, record, records, finish_range}` (and its `Default`), `codec::envelope::{decode, encode, Envelope { meta, header_len, value }}`, `tuning::{CompareFn, ComparatorRegistry::{get, register}}`, `store::test_support::{apply, open_store, open_store_with}`, `Store::{live_cf, handle, db, read_snapshot}`, `CfEntry`; Plan 2 `ReadSnapshot::{meta, phys, raw_ctx}`, `ReadCtx { db, snap, now_ms }`, `RawIter` (`Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>>`), generated `pb::{KeyRange, KeyDigest, VerifyMode}`.
- Produces:
  - Public: `VerifyMode { Quick (default), Full }`, `MismatchPolicy { Report (default), Rebootstrap }`, `VerifyConfig { interval: Option<Duration>, mode, chunk_keys: usize, chunk_bytes: usize, min_chunk_keys: usize, max_bytes_per_sec: Option<u64>, on_mismatch, capture_timeout: Duration, max_reported_keys: usize }` (+ `Default`, `pub(crate) fn validate(&self) -> Result<()>`), `CfVersion { spec: CfSpec, create_seq: Seq }`, `Mismatch { CfSpec { name, local: Option<CfVersion>, upstream: Option<CfVersion> }, Key { cf, key, local: Option<Seq>, upstream: Option<Seq> }, Range { cf, lower: Option<Vec<u8>>, upper: Option<Vec<u8>> } }` (`non_exhaustive`), `VerifyReport { seq, mode, cfs_checked: usize, keys_checked: u64, mismatches, duration }` (`non_exhaustive`, `is_clean()`), `VerifyAbort { Busy, SessionExpired, IdentityChanged, PassedSnapshot { snapshot_seq, applied_seq }, CaptureTimeout { snapshot_seq }, Upstream(String) }` (`non_exhaustive`), `Error::VerifyAborted(VerifyAbort)`.
  - `VerifyMode::{to_pb(self) -> pb::VerifyMode, from_pb(i32) -> Option<VerifyMode>}`; constants `verify::{VERIFY_WINDOW = 16, MAX_LIST_LIMIT: u32 = 1024, LIST_SOFT_BYTES = 1 MiB, MAX_BOUNDARY_KEY_BYTES = 1 MiB, MAX_RESUME_KEY_BYTES = 2 MiB}`.
  - `verify::range::{KeyOrder (bytewise(), new(&ComparatorRegistry, Option<&str>), cmp), Bounds { lower, upper } (below_upper, contains, to_pb(&self, cf) -> pb::KeyRange, from_pb(&pb::KeyRange)), payload(&[u8], VerifyMode) -> Result<&[u8]>, RangeHasher (new, add, finish), Lister (new(mode, limit, soft_bytes), offer, pub keys, pub truncated), LocalRange { bounds, count, hash }, LocalRecord { key, write_seq, payload } (new), local_range(Bounds, &[LocalRecord]) -> LocalRange, Chunker (new, push, finish), Throttle (new(Option<u64>), consume(n) -> Duration)}`.
  - `verify::scan::{scan, hash_range(&ReadSnapshot, Arc<PhysCf>, &KeyOrder, &Bounds, VerifyMode) -> Result<(u64, [u8; 32])>, list_range(…, mode, limit: usize, soft_bytes: usize) -> Result<(Vec<pb::KeyDigest>, bool)>, load_range(…, mode, &Mutex<Throttle>) -> Result<Vec<LocalRecord>>, chunk_cf(&ReadSnapshot, Arc<PhysCf>, mode, chunk_keys, chunk_bytes, &Mutex<Throttle>, emit: impl FnMut(LocalRange) -> bool) -> Result<u64>, pace(&Mutex<Throttle>, u64)}`.
  - `verify::compare::{trait Oracle { async fn hash(&mut self, cf: &str, ranges: &[LocalRange]) -> Result<Vec<bool>>; async fn list(&mut self, cf: &str, bounds: &Bounds, limit: u32) -> Result<(Vec<pb::KeyDigest>, bool)>; }, trait LocalSide { async fn load(&self, bounds: &Bounds) -> Result<Vec<LocalRecord>>; }, Budget::new(usize), CfRun { cf: &str, order: KeyOrder, min_chunk_keys: usize, budget: &mut Budget, out: &mut Vec<Mismatch> }, compare_cf(&mut O, &L, &mut CfRun, &mut mpsc::Receiver<Result<LocalRange>>) -> Result<()>, compare_specs(&[CfEntry], &[CfEntry]) -> (Vec<Mismatch>, Vec<CfEntry>)}`.
  - `ReadCtx::iter_raw_from(&self, Arc<PhysCf>, from: Option<&[u8]>) -> Result<RawIter<'a>>`.

- [ ] **Step 1: Add the module skeleton, the public types and the error variant**

Create `src/verify/mod.rs`:

```rust
//! Integrity verification (DESIGN.md §12, PROTOCOL.md §9): the public types, and the range
//! logic shared by the server (hashes and listings over a pinned snapshot) and the slave
//! verifier (contiguous ranges, bisection, listing and key-by-key diff).

pub(crate) mod compare;
pub(crate) mod range;
pub(crate) mod scan;

use std::time::Duration;

use crate::error::{Error, Result};
use crate::pb;
use crate::types::{CfSpec, Seq};

/// Range requests in flight at once on one session (PROTOCOL.md §9.2, `verify_window`).
pub(crate) const VERIFY_WINDOW: usize = 16;
/// The largest `ListRange.limit` a client may ask for, and the one this client always uses.
pub(crate) const MAX_LIST_LIMIT: u32 = 1024;
/// A `ListResult` is cut (`truncated`) once its keys reach about this many bytes, but it
/// always holds at least two keys when the range has two.
pub(crate) const LIST_SOFT_BYTES: usize = 1 << 20;
/// Keys longer than this never become range boundaries, so a range request stays far below
/// the server's 4 MiB request limit.
pub(crate) const MAX_BOUNDARY_KEY_BYTES: usize = 1 << 20;
/// A truncated listing is continued from its last key only if that key is at most this long;
/// otherwise the rest of the range is reported as [`Mismatch::Range`].
pub(crate) const MAX_RESUME_KEY_BYTES: usize = 2 << 20;

/// What a verify run compares per key (PROTOCOL.md §9.3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash)]
pub enum VerifyMode {
    /// Envelope headers only (`write_seq`, TTL): finds missing, extra and stale keys without
    /// hashing values.
    #[default]
    Quick,
    /// Whole envelopes: also finds corrupted values.
    Full,
}

impl VerifyMode {
    pub(crate) fn to_pb(self) -> pb::VerifyMode {
        match self {
            VerifyMode::Quick => pb::VerifyMode::Quick,
            VerifyMode::Full => pb::VerifyMode::Full,
        }
    }

    pub(crate) fn from_pb(v: i32) -> Option<VerifyMode> {
        match pb::VerifyMode::try_from(v).ok()? {
            pb::VerifyMode::Quick => Some(VerifyMode::Quick),
            pb::VerifyMode::Full => Some(VerifyMode::Full),
            pb::VerifyMode::Unspecified => None,
        }
    }
}

/// What a slave does when a verify run finds a difference.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum MismatchPolicy {
    /// Report it (status, listener, logs) and keep replicating.
    #[default]
    Report,
    /// Report it, then discard local data and bootstrap from the upstream.
    Rebootstrap,
}

/// Verification settings of a slave (DESIGN.md §12, §16).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct VerifyConfig {
    /// Run a verify this often. `None` (default): only on [`Slave::verify_now`](crate::Slave::verify_now).
    pub interval: Option<Duration>,
    /// Mode of the scheduled runs. Default `Quick`.
    pub mode: VerifyMode,
    /// A range is closed after this many local keys. Default 1 000.
    pub chunk_keys: usize,
    /// … or after this many local bytes (keys plus envelopes). Default 4 MiB.
    pub chunk_bytes: usize,
    /// A mismatched range is listed key by key once it holds at most this many local keys.
    /// Default 16.
    pub min_chunk_keys: usize,
    /// Local read rate limit. `None` (default): unlimited.
    pub max_bytes_per_sec: Option<u64>,
    pub on_mismatch: MismatchPolicy,
    /// How long to wait for the local applier to reach the upstream's snapshot seq. Default 60 s.
    pub capture_timeout: Duration,
    /// At most this many [`Mismatch::Key`] per run; the rest of a range is then reported as
    /// [`Mismatch::Range`]. Default 10 000.
    pub max_reported_keys: usize,
}

impl Default for VerifyConfig {
    fn default() -> Self {
        VerifyConfig {
            interval: None,
            mode: VerifyMode::Quick,
            chunk_keys: 1_000,
            chunk_bytes: 4 << 20,
            min_chunk_keys: 16,
            max_bytes_per_sec: None,
            on_mismatch: MismatchPolicy::Report,
            capture_timeout: Duration::from_secs(60),
            max_reported_keys: 10_000,
        }
    }
}

impl VerifyConfig {
    pub(crate) fn validate(&self) -> Result<()> {
        if self.chunk_keys == 0
            || self.chunk_bytes == 0
            || self.min_chunk_keys == 0
            || self.max_reported_keys == 0
        {
            return Err(Error::Config(
                "verify chunk sizes and max_reported_keys must be positive".into(),
            ));
        }
        if self.max_bytes_per_sec == Some(0) {
            return Err(Error::Config(
                "verify max_bytes_per_sec must be positive (None = unlimited)".into(),
            ));
        }
        if self.interval.is_some_and(|i| i.is_zero()) || self.capture_timeout.is_zero() {
            return Err(Error::Config(
                "verify interval and capture_timeout must be positive".into(),
            ));
        }
        Ok(())
    }
}

/// A user CF as one side of a verify sees it: its spec and the seq that created it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CfVersion {
    pub spec: CfSpec,
    pub create_seq: Seq,
}

/// One difference between a slave and its upstream at the same seq. More kinds may be added,
/// hence `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum Mismatch {
    /// The CF lists differ: a CF exists on one side only, or with another comparator or
    /// `create_seq`. Its data is compared only if both sides have it with the same comparator.
    CfSpec {
        name: String,
        local: Option<CfVersion>,
        upstream: Option<CfVersion>,
    },
    /// A key differs. `None` means the key is absent on that side; `Some` holds that side's
    /// `write_seq` (equal `write_seq`s mean the values differ).
    Key {
        cf: String,
        key: Vec<u8>,
        local: Option<Seq>,
        upstream: Option<Seq>,
    },
    /// Keys in `[lower, upper)` (`None` = unbounded) were not compared one by one: the
    /// listing could not be continued, or `max_reported_keys` was reached.
    Range {
        cf: String,
        lower: Option<Vec<u8>>,
        upper: Option<Vec<u8>>,
    },
}

/// The result of one verify run. Fields may be added, hence `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub struct VerifyReport {
    /// The seq both sides were compared at (the upstream's snapshot).
    pub seq: Seq,
    pub mode: VerifyMode,
    /// CFs whose data was compared.
    pub cfs_checked: usize,
    /// Local keys hashed.
    pub keys_checked: u64,
    pub mismatches: Vec<Mismatch>,
    pub duration: Duration,
}

impl VerifyReport {
    /// True when nothing differs.
    pub fn is_clean(&self) -> bool {
        self.mismatches.is_empty()
    }
}

/// Why a verify run was abandoned without a report.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum VerifyAbort {
    /// The upstream already serves `max_verify_sessions` sessions (`BUSY`). Retry later.
    Busy,
    /// The upstream no longer knows the session (`SESSION_NOT_FOUND`): it was idle longer
    /// than the upstream's `verify_session_timeout`.
    SessionExpired,
    /// The upstream follows another database identity than this slave.
    IdentityChanged,
    /// This slave had already applied entries past the upstream's snapshot (possible when the
    /// upstream is a slave that lost unflushed entries, DESIGN.md §6). Retry later.
    PassedSnapshot { snapshot_seq: Seq, applied_seq: Seq },
    /// This slave did not reach the upstream's snapshot seq within `capture_timeout` (it is
    /// halted, far behind, or started a bootstrap).
    CaptureTimeout { snapshot_seq: Seq },
    /// Any other failure talking to the upstream.
    Upstream(String),
}
```

In `src/error.rs`, in `enum Error`, insert before `#[error(transparent)] RocksDb(…)`:

```rust
    #[error(transparent)]
    RocksDb(#[from] rocksdb::Error),
```

insert:

```rust
    /// A verify run was abandoned before it produced a report.
    #[error("verify abandoned: {0:?}")]
    VerifyAborted(crate::verify::VerifyAbort),
```

In `src/lib.rs`, replace:

```rust
pub(crate) mod store;
pub use codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
pub use read::{ReadApi, ReadSnapshot};
pub use store::read::DbIter;
pub use tuning::{CfKind, ComparatorRegistry, Compression, RocksTuning, Tunable};
```

with:

```rust
pub(crate) mod store;
pub(crate) mod verify;
pub use codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
pub use read::{ReadApi, ReadSnapshot};
pub use store::read::DbIter;
pub use tuning::{CfKind, ComparatorRegistry, Compression, RocksTuning, Tunable};
pub use verify::{
    CfVersion, Mismatch, MismatchPolicy, VerifyAbort, VerifyConfig, VerifyMode, VerifyReport,
};
```

Create `src/verify/range.rs`, `src/verify/scan.rs` and `src/verify/compare.rs`, each with only a doc line for now, so the module compiles:

```rust
//! See Task 3.
```

(`src/verify/scan.rs`)

```rust
//! See Task 3.
```

(`src/verify/compare.rs`)

```rust
//! See Task 3.
```

Run: `cargo build`
Expected: builds (dead-code warnings for the new items are expected until Tasks 4–5 use them).

- [ ] **Step 2: Write the failing tests for range arithmetic and snapshot scans**

Replace the content of `src/verify/range.rs` with its test module:

```rust
//! See Task 3.
```

with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope::encode;

    fn hex32(s: &str) -> [u8; 32] {
        let v: Vec<u8> = (0..32)
            .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    #[test]
    fn range_hashes_match_the_v3_vectors() {
        let env = encode(1, None, b"v1");
        let mut quick = RangeHasher::new(VerifyMode::Quick);
        quick.add(b"k1", &env).unwrap();
        assert_eq!(
            quick.finish(),
            (
                1,
                hex32("2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3")
            )
        );
        let mut full = RangeHasher::new(VerifyMode::Full);
        full.add(b"k1", &env).unwrap();
        assert_eq!(
            full.finish(),
            (
                1,
                hex32("14e556f28a8301390e9be56ecb4ec7ff3ceea3cd48316ab7f7f0488117e53ecd")
            )
        );
        assert_eq!(
            RangeHasher::new(VerifyMode::Full).finish(),
            (
                0,
                hex32("af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            )
        );
        // The local side hashes the same records to the same value.
        let rec = LocalRecord::new(b"k1", &env, VerifyMode::Quick).unwrap();
        assert_eq!(
            local_range(Bounds::default(), &[rec]).hash,
            hex32("2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3")
        );
    }

    #[test]
    fn quick_payload_is_the_header_with_or_without_ttl() {
        let plain = encode(7, None, b"value");
        let ttl = encode(7, Some(99), b"value");
        assert_eq!(payload(&plain, VerifyMode::Quick).unwrap(), &plain[..10]);
        assert_eq!(payload(&ttl, VerifyMode::Quick).unwrap(), &ttl[..18]);
        assert_eq!(payload(&ttl, VerifyMode::Full).unwrap(), &ttl[..]);
        assert!(payload(b"short", VerifyMode::Quick).is_err());
    }

    #[test]
    fn empty_key_bound_is_not_unbounded() {
        let o = KeyOrder::bytewise();
        let below_empty = Bounds {
            lower: None,
            upper: Some(vec![]),
        };
        assert!(
            !below_empty.contains(&o, b""),
            "nothing sorts before the empty key"
        );
        let from_empty = Bounds {
            lower: Some(vec![]),
            upper: None,
        };
        assert!(from_empty.contains(&o, b"") && from_empty.contains(&o, b"\0"));
        let only_empty = Bounds {
            lower: Some(vec![]),
            upper: Some(vec![0]),
        };
        assert!(only_empty.contains(&o, b"") && !only_empty.contains(&o, b"\0"));
        let back = Bounds::from_pb(&only_empty.to_pb("c"));
        assert_eq!(back, only_empty);
    }

    #[test]
    fn custom_order_decides_containment() {
        let mut reg = ComparatorRegistry::default();
        reg.register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let o = KeyOrder::new(&reg, Some("reverse"));
        let b = Bounds {
            lower: Some(b"m".to_vec()),
            upper: Some(b"c".to_vec()),
        };
        assert!(b.contains(&o, b"m") && b.contains(&o, b"d"));
        assert!(!b.contains(&o, b"c") && !b.contains(&o, b"z"));
    }

    fn keys(n: usize) -> Vec<(Vec<u8>, Vec<u8>)> {
        (0..n)
            .map(|i| {
                (
                    format!("k{i:03}").into_bytes(),
                    encode(i as u64 + 1, None, &[i as u8; 10]),
                )
            })
            .collect()
    }

    fn chunk(
        records: &[(Vec<u8>, Vec<u8>)],
        chunk_keys: usize,
        chunk_bytes: usize,
    ) -> Vec<LocalRange> {
        let mut c = Chunker::new(VerifyMode::Full, chunk_keys, chunk_bytes);
        let mut out = Vec::new();
        for (k, v) in records {
            out.extend(c.push(k, v).unwrap());
        }
        out.push(c.finish());
        out
    }

    #[test]
    fn chunks_are_contiguous_and_cover_everything() {
        let recs = keys(25);
        let ranges = chunk(&recs, 10, usize::MAX);
        assert_eq!(
            ranges.iter().map(|r| r.count).collect::<Vec<_>>(),
            vec![10, 10, 5]
        );
        assert_eq!(ranges[0].bounds.lower, None);
        assert_eq!(ranges[2].bounds.upper, None);
        for w in ranges.windows(2) {
            assert_eq!(
                w[0].bounds.upper, w[1].bounds.lower,
                "no gap between ranges"
            );
        }
        assert_eq!(ranges[1].bounds.lower.as_deref(), Some(&b"k010"[..]));
        // Each range hashes exactly its own records.
        let rec: Vec<LocalRecord> = recs
            .iter()
            .map(|(k, v)| LocalRecord::new(k, v, VerifyMode::Full).unwrap())
            .collect();
        assert_eq!(
            local_range(ranges[1].bounds.clone(), &rec[10..20]),
            ranges[1]
        );
    }

    #[test]
    fn chunk_bytes_closes_ranges_and_an_empty_cf_is_one_range() {
        let recs = keys(6); // 24 bytes per record: a 4-byte key and a 20-byte envelope
        let ranges = chunk(&recs, usize::MAX, 48);
        assert_eq!(
            ranges.iter().map(|r| r.count).collect::<Vec<_>>(),
            vec![2, 2, 2]
        );
        let empty = Chunker::new(VerifyMode::Quick, 10, 10).finish();
        assert_eq!(
            empty,
            LocalRange {
                bounds: Bounds::default(),
                count: 0,
                hash: *blake3::hash(b"").as_bytes()
            }
        );
    }

    #[test]
    fn oversized_keys_never_become_boundaries() {
        let big = vec![b'b'; MAX_BOUNDARY_KEY_BYTES + 1];
        let recs = vec![
            (b"a".to_vec(), encode(1, None, b"")),
            (big.clone(), encode(2, None, b"")),
            (b"c".to_vec(), encode(3, None, b"")),
        ];
        let ranges = chunk(&recs, 1, usize::MAX);
        assert_eq!(ranges.len(), 2);
        assert_eq!(
            ranges[0].bounds.upper.as_deref(),
            Some(&b"c"[..]),
            "the long key was skipped as a boundary"
        );
        assert_eq!(ranges[0].count, 2);
        let exactly = vec![b'b'; MAX_BOUNDARY_KEY_BYTES];
        let recs = vec![
            (b"a".to_vec(), encode(1, None, b"")),
            (exactly.clone(), encode(2, None, b"")),
        ];
        assert_eq!(
            chunk(&recs, 1, usize::MAX)[0].bounds.upper,
            Some(exactly),
            "1 MiB keys are fine"
        );
    }

    #[test]
    fn listings_respect_limit_and_soft_size_but_keep_two_keys() {
        let mut l = Lister::new(VerifyMode::Quick, 3, usize::MAX);
        for (k, v) in keys(5) {
            if !l.offer(&k, &v).unwrap() {
                break;
            }
        }
        assert_eq!((l.keys.len(), l.truncated), (3, true));
        assert_eq!(l.keys[1].write_seq, 2);
        assert_eq!(
            l.keys[1].payload_hash,
            blake3::hash(&encode(2, None, &[1; 10])[..10])
                .as_bytes()
                .to_vec()
        );

        let mut l = Lister::new(VerifyMode::Full, 1024, 1);
        for (k, v) in keys(5) {
            if !l.offer(&k, &v).unwrap() {
                break;
            }
        }
        assert_eq!(
            (l.keys.len(), l.truncated),
            (2, true),
            "two keys even past the soft size"
        );

        let mut l = Lister::new(VerifyMode::Full, 1024, usize::MAX);
        for (k, v) in keys(2) {
            assert!(l.offer(&k, &v).unwrap());
        }
        assert!(!l.truncated);
    }

    #[test]
    fn throttle_paces_reads() {
        let mut t = Throttle::new(Some(1_000));
        assert_eq!(
            t.consume_at(500, Duration::ZERO),
            Duration::from_millis(500)
        );
        assert_eq!(
            t.consume_at(500, Duration::from_millis(600)),
            Duration::from_millis(400)
        );
        assert_eq!(t.consume_at(0, Duration::from_secs(5)), Duration::ZERO);
        assert_eq!(
            Throttle::new(None).consume_at(u64::MAX, Duration::ZERO),
            Duration::ZERO
        );
    }
}
```

Replace the content of `src/verify/scan.rs` with its test module:

```rust
//! See Task 3.
```

with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope;
    use crate::store::test_support::{apply, open_store, open_store_with};
    use crate::tuning::RocksTuning;
    use crate::types::{CfSpec, Op};

    fn hex32(s: &str) -> [u8; 32] {
        let v: Vec<u8> = (0..32)
            .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    fn put(cf: &str, k: &[u8], v: &[u8]) -> Op {
        Op::Put {
            cf: cf.into(),
            key: k.to_vec(),
            value: v.to_vec(),
            expire_at_ms: None,
        }
    }

    fn all() -> Bounds {
        Bounds::default()
    }

    fn b(lower: Option<&[u8]>, upper: Option<&[u8]>) -> Bounds {
        Bounds {
            lower: lower.map(<[u8]>::to_vec),
            upper: upper.map(<[u8]>::to_vec),
        }
    }

    #[test]
    fn a_snapshot_range_hashes_to_the_v3_vectors() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("users"),
            }],
        )
        .unwrap();
        // V3 stores k1 with the envelope written at seq 1; write it raw into the CF.
        let phys = store.live_cf("users").unwrap();
        store
            .db()
            .put_cf(
                &store.handle(&phys).unwrap(),
                b"k1",
                envelope::encode(1, None, b"v1"),
            )
            .unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let o = KeyOrder::bytewise();
        let cf = snap.phys("users").unwrap();
        assert_eq!(
            hash_range(&snap, cf.clone(), &o, &all(), VerifyMode::Quick).unwrap(),
            (
                1,
                hex32("2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3")
            )
        );
        assert_eq!(
            hash_range(&snap, cf.clone(), &o, &all(), VerifyMode::Full).unwrap(),
            (
                1,
                hex32("14e556f28a8301390e9be56ecb4ec7ff3ceea3cd48316ab7f7f0488117e53ecd")
            )
        );
        assert_eq!(
            hash_range(&snap, cf, &o, &b(None, Some(b"k1")), VerifyMode::Full).unwrap(),
            (
                0,
                hex32("af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            )
        );
    }

    #[test]
    fn ranges_are_half_open_and_the_empty_key_is_a_bound() {
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
            vec![
                put("c", b"", b"e"),
                put("c", b"\0", b"n"),
                put("c", b"\0\0", b"nn"),
                put("c", b"a", b"a"),
            ],
        )
        .unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let o = KeyOrder::bytewise();
        let cf = snap.phys("c").unwrap();
        let count = |bounds: Bounds| {
            hash_range(&snap, cf.clone(), &o, &bounds, VerifyMode::Quick)
                .unwrap()
                .0
        };
        assert_eq!(count(all()), 4);
        assert_eq!(
            count(b(None, Some(b""))),
            0,
            "an empty upper bound excludes everything"
        );
        assert_eq!(
            count(b(Some(b""), None)),
            4,
            "an empty lower bound includes the empty key"
        );
        assert_eq!(count(b(Some(b""), Some(b"\0"))), 1);
        assert_eq!(count(b(Some(b"\0"), Some(b"a"))), 2);
        assert_eq!(count(b(Some(b"a"), Some(b"a"))), 0);
        // Contiguous ranges partition the CF.
        let parts = [
            b(None, Some(b"\0")),
            b(Some(b"\0"), Some(b"\0\0")),
            b(Some(b"\0\0"), None),
        ];
        assert_eq!(parts.into_iter().map(count).sum::<u64>(), 4);
    }

    #[test]
    fn listings_and_loads_follow_a_custom_comparator() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = RocksTuning::new();
        tuning
            .comparators
            .register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        let store = open_store_with(dir.path(), tuning.clone());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("r").with_comparator("reverse"),
            }],
        )
        .unwrap();
        apply(
            &store,
            (b'a'..=b'f').map(|k| put("r", &[k], &[k])).collect(),
        )
        .unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let o = KeyOrder::new(&tuning.comparators, Some("reverse"));
        let cf = snap.phys("r").unwrap();
        // [e, b) under reverse order is e, d, c.
        let (keys, more) = list_range(
            &snap,
            cf.clone(),
            &o,
            &b(Some(b"e"), Some(b"b")),
            VerifyMode::Quick,
            1024,
            usize::MAX,
        )
        .unwrap();
        assert_eq!(
            keys.iter().map(|k| k.key.clone()).collect::<Vec<_>>(),
            vec![b"e".to_vec(), b"d".to_vec(), b"c".to_vec()]
        );
        assert!(!more);
        assert!(keys.iter().all(|k| k.write_seq == 2));
        let (keys, more) = list_range(
            &snap,
            cf.clone(),
            &o,
            &all(),
            VerifyMode::Quick,
            2,
            usize::MAX,
        )
        .unwrap();
        assert_eq!((keys.len(), more), (2, true));
        let t = Mutex::new(Throttle::new(None));
        let recs = load_range(
            &snap,
            cf.clone(),
            &o,
            &b(None, Some(b"d")),
            VerifyMode::Full,
            &t,
        )
        .unwrap();
        assert_eq!(
            recs.iter().map(|r| r.key.clone()).collect::<Vec<_>>(),
            vec![b"f".to_vec(), b"e".to_vec()]
        );
        let mut ranges = Vec::new();
        let n = chunk_cf(&snap, cf, VerifyMode::Quick, 4, usize::MAX, &t, |r| {
            ranges.push(r);
            true
        })
        .unwrap();
        assert_eq!(n, 6);
        assert_eq!(ranges.len(), 2);
        assert_eq!(
            ranges[0].bounds.upper.as_deref(),
            Some(&b"b"[..]),
            "boundaries follow the CF order"
        );
    }

    #[test]
    fn the_snapshot_pins_what_is_hashed() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(
            &store,
            vec![Op::CreateCf {
                spec: CfSpec::new("c"),
            }],
        )
        .unwrap();
        apply(&store, vec![put("c", b"a", b"1")]).unwrap();
        let snap = store.read_snapshot(0).unwrap();
        let before = hash_range(
            &snap,
            snap.phys("c").unwrap(),
            &KeyOrder::bytewise(),
            &all(),
            VerifyMode::Full,
        )
        .unwrap();
        apply(&store, vec![put("c", b"a", b"2"), put("c", b"b", b"3")]).unwrap();
        let after = hash_range(
            &snap,
            snap.phys("c").unwrap(),
            &KeyOrder::bytewise(),
            &all(),
            VerifyMode::Full,
        )
        .unwrap();
        assert_eq!(before, after);
        assert_eq!(after.0, 1);
    }
}
```

- [ ] **Step 3: Run them to confirm they fail**

Run: `cargo test --lib verify::`
Expected: FAIL to compile: `cannot find type RangeHasher in this scope`, `cannot find function hash_range`, `cannot find struct Bounds`, `cannot find struct Chunker`.

- [ ] **Step 4: Add `ReadCtx::iter_raw_from`**

In `src/store/repl.rs`, after the `impl<'a> ReadCtx<'a> { … iter_raw … }` block, add:

```rust
        Ok(RawIter { inner, _cf: cf })
    }
}
```

add:

```rust

impl<'a> ReadCtx<'a> {
    /// Like [`iter_raw`](Self::iter_raw), starting at the first key `>= from` under the CF's
    /// comparator (`None`: the first key).
    pub(crate) fn iter_raw_from(
        &self,
        cf: Arc<PhysCf>,
        from: Option<&[u8]>,
    ) -> Result<RawIter<'a>> {
        let handle = self.db.cf_handle(&cf.physical).ok_or_else(|| {
            Error::corrupt(format!("physical column family {} is missing", cf.physical))
        })?;
        let mode = match from {
            Some(k) => IteratorMode::From(k, rocksdb::Direction::Forward),
            None => IteratorMode::Start,
        };
        let inner = match self.snap {
            Some(s) => s.iterator_cf(&handle, mode),
            None => self.db.iterator_cf(&handle, mode),
        };
        Ok(RawIter { inner, _cf: cf })
    }
}
```

- [ ] **Step 5: Implement `src/verify/range.rs`** (above the test module)

Insert at the top of `src/verify/range.rs`, above `#[cfg(test)]`:

```rust
#[cfg(test)]
mod tests {
```

insert:

```rust
//! Range arithmetic both verify sides share: key order, half-open bounds, per-mode payloads,
//! range hashes, key listings, the chunker that cuts a CF into contiguous ranges, and the
//! read-rate throttle.

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use super::{MAX_BOUNDARY_KEY_BYTES, VerifyMode};
use crate::codec::digest::Digest;
use crate::codec::envelope;
use crate::error::Result;
use crate::pb;
use crate::tuning::{ComparatorRegistry, CompareFn};
use crate::types::Seq;

/// A CF's key order: its registered comparator, or bytewise.
#[derive(Clone, Default)]
pub(crate) struct KeyOrder(Option<Arc<CompareFn>>);

impl KeyOrder {
    pub(crate) fn bytewise() -> Self {
        KeyOrder(None)
    }

    /// `None`, or a name that is not registered, is bytewise (as in `ComparatorRegistry::compare`).
    pub(crate) fn new(reg: &ComparatorRegistry, name: Option<&str>) -> Self {
        KeyOrder(name.and_then(|n| reg.get(n)))
    }

    pub(crate) fn cmp(&self, a: &[u8], b: &[u8]) -> Ordering {
        match &self.0 {
            Some(f) => f(a, b),
            None => a.cmp(b),
        }
    }
}

/// A half-open key range `[lower, upper)` under a CF's order. `None` is unbounded; an empty
/// key is a real bound (PROTOCOL.md §9.2).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct Bounds {
    pub lower: Option<Vec<u8>>,
    pub upper: Option<Vec<u8>>,
}

impl Bounds {
    /// True if `key` sorts before `upper` (always, without an upper bound).
    pub(crate) fn below_upper(&self, order: &KeyOrder, key: &[u8]) -> bool {
        self.upper
            .as_deref()
            .is_none_or(|u| order.cmp(key, u) == Ordering::Less)
    }

    #[cfg(test)] // the tests' in-memory upstream; the scans only need `below_upper`
    pub(crate) fn contains(&self, order: &KeyOrder, key: &[u8]) -> bool {
        self.lower
            .as_deref()
            .is_none_or(|l| order.cmp(key, l) != Ordering::Less)
            && self.below_upper(order, key)
    }

    pub(crate) fn to_pb(&self, cf: &str) -> pb::KeyRange {
        pb::KeyRange {
            cf: cf.to_string(),
            lower: self.lower.clone(),
            upper: self.upper.clone(),
        }
    }

    pub(crate) fn from_pb(r: &pb::KeyRange) -> Bounds {
        Bounds {
            lower: r.lower.clone(),
            upper: r.upper.clone(),
        }
    }
}

/// The bytes hashed for one record (PROTOCOL.md §9.3): the envelope header in `Quick`, the
/// whole envelope in `Full`. `Corrupt` if the envelope does not decode.
pub(crate) fn payload(raw: &[u8], mode: VerifyMode) -> Result<&[u8]> {
    let env = envelope::decode(raw)?;
    Ok(match mode {
        VerifyMode::Quick => &raw[..env.header_len],
        VerifyMode::Full => raw,
    })
}

/// Range hash (PROTOCOL.md §5.4) of records offered in key order.
pub(crate) struct RangeHasher {
    mode: VerifyMode,
    digest: Digest,
}

impl RangeHasher {
    pub(crate) fn new(mode: VerifyMode) -> Self {
        RangeHasher {
            mode,
            digest: Digest::new(),
        }
    }

    pub(crate) fn add(&mut self, key: &[u8], raw: &[u8]) -> Result<()> {
        self.digest.record(key, payload(raw, self.mode)?);
        Ok(())
    }

    /// `(count, hash)`.
    pub(crate) fn finish(self) -> (u64, [u8; 32]) {
        self.digest.finish_range()
    }
}

/// A `ListRange` answer being built from records offered in key order.
pub(crate) struct Lister {
    mode: VerifyMode,
    limit: usize,
    soft_bytes: usize,
    bytes: usize,
    pub keys: Vec<pb::KeyDigest>,
    pub truncated: bool,
}

impl Lister {
    pub(crate) fn new(mode: VerifyMode, limit: usize, soft_bytes: usize) -> Self {
        Lister {
            mode,
            limit,
            soft_bytes,
            bytes: 0,
            keys: Vec::new(),
            truncated: false,
        }
    }

    /// Offers the next key. Returns `false` once the listing is full; that key is not added
    /// and `truncated` is set. The listing is full at `limit` keys, or at `soft_bytes` once it
    /// holds two keys (so a client that continues from the last key always makes progress).
    pub(crate) fn offer(&mut self, key: &[u8], raw: &[u8]) -> Result<bool> {
        let full = self.keys.len() >= self.limit
            || (self.keys.len() >= 2 && self.bytes >= self.soft_bytes);
        if full {
            self.truncated = true;
            return Ok(false);
        }
        let write_seq = envelope::decode(raw)?.meta.write_seq;
        let hash = blake3::hash(payload(raw, self.mode)?);
        self.bytes += key.len() + 48;
        self.keys.push(pb::KeyDigest {
            key: key.to_vec(),
            write_seq,
            payload_hash: hash.as_bytes().to_vec(),
        });
        Ok(true)
    }
}

/// One local range with its count and hash, as sent in a `HashRange`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalRange {
    pub bounds: Bounds,
    pub count: u64,
    pub hash: [u8; 32],
}

/// One local record of a range being narrowed.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct LocalRecord {
    pub key: Vec<u8>,
    pub write_seq: Seq,
    /// The mode's payload (header or whole envelope).
    pub payload: Vec<u8>,
}

impl LocalRecord {
    pub(crate) fn new(key: &[u8], raw: &[u8], mode: VerifyMode) -> Result<Self> {
        let write_seq = envelope::decode(raw)?.meta.write_seq;
        Ok(LocalRecord {
            key: key.to_vec(),
            write_seq,
            payload: payload(raw, mode)?.to_vec(),
        })
    }
}

/// `LocalRange` of `records` (already in key order) within `bounds`.
pub(crate) fn local_range(bounds: Bounds, records: &[LocalRecord]) -> LocalRange {
    let mut d = Digest::new();
    for r in records {
        d.record(&r.key, &r.payload);
    }
    let (count, hash) = d.finish_range();
    LocalRange {
        bounds,
        count,
        hash,
    }
}

/// Cuts one CF, read in key order, into contiguous ranges (PROTOCOL.md §9.2): the first
/// has no lower bound, each upper bound is the next lower bound, the last has no upper
/// bound. A range is closed when it holds `chunk_keys` keys or `chunk_bytes` bytes and the
/// next key can serve as a boundary (at most [`MAX_BOUNDARY_KEY_BYTES`]).
pub(crate) struct Chunker {
    mode: VerifyMode,
    chunk_keys: u64,
    chunk_bytes: u64,
    lower: Option<Vec<u8>>,
    digest: Digest,
    bytes: u64,
}

impl Chunker {
    pub(crate) fn new(mode: VerifyMode, chunk_keys: usize, chunk_bytes: usize) -> Self {
        Chunker {
            mode,
            chunk_keys: chunk_keys.max(1) as u64,
            chunk_bytes: chunk_bytes.max(1) as u64,
            lower: None,
            digest: Digest::new(),
            bytes: 0,
        }
    }

    /// Adds the next key. Returns the range this key closes, if any.
    pub(crate) fn push(&mut self, key: &[u8], raw: &[u8]) -> Result<Option<LocalRange>> {
        let payload = payload(raw, self.mode)?;
        let full = self.digest.records() >= self.chunk_keys || self.bytes >= self.chunk_bytes;
        let mut closed = None;
        if full && self.digest.records() > 0 && key.len() <= MAX_BOUNDARY_KEY_BYTES {
            let (count, hash) = std::mem::take(&mut self.digest).finish_range();
            let lower = self.lower.replace(key.to_vec());
            closed = Some(LocalRange {
                bounds: Bounds {
                    lower,
                    upper: Some(key.to_vec()),
                },
                count,
                hash,
            });
            self.bytes = 0;
        }
        self.digest.record(key, payload);
        self.bytes += (key.len() + raw.len()) as u64;
        Ok(closed)
    }

    /// The last range, unbounded above. An empty CF is one range `(-∞, +∞)` with no keys.
    pub(crate) fn finish(self) -> LocalRange {
        let (count, hash) = self.digest.finish_range();
        LocalRange {
            bounds: Bounds {
                lower: self.lower,
                upper: None,
            },
            count,
            hash,
        }
    }
}

/// Keeps a reader at or below `rate` bytes per second.
pub(crate) struct Throttle {
    rate: Option<u64>,
    start: Instant,
    bytes: u64,
}

impl Throttle {
    pub(crate) fn new(rate: Option<u64>) -> Self {
        Throttle {
            rate,
            start: Instant::now(),
            bytes: 0,
        }
    }

    /// Accounts for `n` more bytes read; returns how long to pause.
    pub(crate) fn consume(&mut self, n: u64) -> Duration {
        let elapsed = self.start.elapsed();
        self.consume_at(n, elapsed)
    }

    fn consume_at(&mut self, n: u64, elapsed: Duration) -> Duration {
        let Some(rate) = self.rate else {
            return Duration::ZERO;
        };
        self.bytes = self.bytes.saturating_add(n);
        Duration::from_secs_f64(self.bytes as f64 / rate as f64).saturating_sub(elapsed)
    }
}
```

- [ ] **Step 6: Implement `src/verify/scan.rs`** (above the test module)

Insert at the top of `src/verify/scan.rs`, above `#[cfg(test)]`:

```rust
#[cfg(test)]
mod tests {
```

insert:

```rust
//! Range reads through a pinned snapshot, under the CF's comparator. Used by the server for
//! `HashRange` and `ListRange`, and by the verifier for its own side.

use std::sync::{Arc, Mutex, PoisonError};

use super::VerifyMode;
use super::range::{
    Bounds, Chunker, KeyOrder, Lister, LocalRange, LocalRecord, RangeHasher, Throttle,
};
use crate::error::Result;
use crate::pb;
use crate::read::ReadSnapshot;
use crate::store::cfs::PhysCf;

/// Calls `f(key, envelope)` for each key of `cf` in `bounds`, in order, until `f` returns
/// `false`. Expired values are included: verify compares what is stored.
pub(crate) fn scan(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mut f: impl FnMut(&[u8], &[u8]) -> Result<bool>,
) -> Result<()> {
    let ctx = snap.raw_ctx();
    for item in ctx.iter_raw_from(cf, bounds.lower.as_deref())? {
        let (key, raw) = item?;
        if !bounds.below_upper(order, &key) || !f(&key, &raw)? {
            break;
        }
    }
    Ok(())
}

/// `(count, hash)` of a range (PROTOCOL.md §5.4, §9.3).
pub(crate) fn hash_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mode: VerifyMode,
) -> Result<(u64, [u8; 32])> {
    let mut h = RangeHasher::new(mode);
    scan(snap, cf, order, bounds, |k, v| h.add(k, v).map(|()| true))?;
    Ok(h.finish())
}

/// Up to `limit` keys of a range with their `write_seq` and payload hash; `true` if there are more.
pub(crate) fn list_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mode: VerifyMode,
    limit: usize,
    soft_bytes: usize,
) -> Result<(Vec<pb::KeyDigest>, bool)> {
    let mut l = Lister::new(mode, limit, soft_bytes);
    scan(snap, cf, order, bounds, |k, v| l.offer(k, v))?;
    Ok((l.keys, l.truncated))
}

/// Sleeps as long as `throttle` asks for after reading `n` bytes.
pub(crate) fn pace(throttle: &Mutex<Throttle>, n: u64) {
    let pause = throttle
        .lock()
        .unwrap_or_else(PoisonError::into_inner)
        .consume(n);
    if !pause.is_zero() {
        std::thread::sleep(pause);
    }
}

/// The local records of a range, for narrowing.
pub(crate) fn load_range(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    order: &KeyOrder,
    bounds: &Bounds,
    mode: VerifyMode,
    throttle: &Mutex<Throttle>,
) -> Result<Vec<LocalRecord>> {
    let mut out = Vec::new();
    scan(snap, cf, order, bounds, |k, v| {
        pace(throttle, (k.len() + v.len()) as u64);
        out.push(LocalRecord::new(k, v, mode)?);
        Ok(true)
    })?;
    Ok(out)
}

/// Cuts a whole CF into contiguous ranges and hands each to `emit` as soon as it is closed.
/// Stops early (without error) when `emit` returns `false`. Returns the number of keys read.
pub(crate) fn chunk_cf(
    snap: &ReadSnapshot,
    cf: Arc<PhysCf>,
    mode: VerifyMode,
    chunk_keys: usize,
    chunk_bytes: usize,
    throttle: &Mutex<Throttle>,
    mut emit: impl FnMut(LocalRange) -> bool,
) -> Result<u64> {
    let mut chunker = Chunker::new(mode, chunk_keys, chunk_bytes);
    let mut keys = 0u64;
    let mut open = true;
    scan(
        snap,
        cf,
        &KeyOrder::bytewise(),
        &Bounds::default(),
        |k, v| {
            pace(throttle, (k.len() + v.len()) as u64);
            keys += 1;
            if let Some(r) = chunker.push(k, v)? {
                open = emit(r);
            }
            Ok(open)
        },
    )?;
    if open {
        emit(chunker.finish());
    }
    Ok(keys)
}
```

- [ ] **Step 7: Run the range and scan tests**

Run: `cargo test --lib verify::`
Expected: PASS, 13 tests: 9 in `verify::range` (including `range_hashes_match_the_v3_vectors`, `empty_key_bound_is_not_unbounded`, `oversized_keys_never_become_boundaries`, `listings_respect_limit_and_soft_size_but_keep_two_keys`) and 4 in `verify::scan` (including `a_snapshot_range_hashes_to_the_v3_vectors`, `ranges_are_half_open_and_the_empty_key_is_a_bound`, `listings_and_loads_follow_a_custom_comparator`).

- [ ] **Step 8: Write the failing comparison tests**

Replace the content of `src/verify/compare.rs` with its test module:

```rust
//! See Task 3.
```

with:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope::encode;
    use crate::types::CfSpec;
    use crate::verify::VerifyMode;
    use crate::verify::range::{Chunker, Lister, RangeHasher};

    type Data = Vec<(Vec<u8>, Vec<u8>)>;

    /// An upstream over in-memory records (sorted by `order`), answering like the server.
    struct Fake {
        order: KeyOrder,
        mode: VerifyMode,
        data: Data,
        soft_bytes: usize,
        hash_batches: Vec<usize>,
        lists: Vec<Bounds>,
    }

    impl Fake {
        fn new(data: Data, mode: VerifyMode) -> Self {
            Fake {
                order: KeyOrder::bytewise(),
                mode,
                data,
                soft_bytes: usize::MAX,
                hash_batches: Vec::new(),
                lists: Vec::new(),
            }
        }

        fn in_range<'a>(
            &'a self,
            b: &'a Bounds,
        ) -> impl Iterator<Item = &'a (Vec<u8>, Vec<u8>)> + 'a {
            self.data
                .iter()
                .filter(move |(k, _)| b.contains(&self.order, k))
        }
    }

    impl Oracle for Fake {
        async fn hash(&mut self, _cf: &str, ranges: &[LocalRange]) -> Result<Vec<bool>> {
            self.hash_batches.push(ranges.len());
            Ok(ranges
                .iter()
                .map(|r| {
                    let mut h = RangeHasher::new(self.mode);
                    for (k, v) in self.in_range(&r.bounds) {
                        h.add(k, v).unwrap();
                    }
                    h.finish() == (r.count, r.hash)
                })
                .collect())
        }

        async fn list(
            &mut self,
            _cf: &str,
            b: &Bounds,
            limit: u32,
        ) -> Result<(Vec<pb::KeyDigest>, bool)> {
            self.lists.push(b.clone());
            let mut l = Lister::new(self.mode, limit as usize, self.soft_bytes);
            for (k, v) in self.in_range(b) {
                if !l.offer(k, v).unwrap() {
                    break;
                }
            }
            Ok((l.keys, l.truncated))
        }
    }

    struct Local {
        order: KeyOrder,
        mode: VerifyMode,
        data: Data,
    }

    impl LocalSide for Local {
        async fn load(&self, b: &Bounds) -> Result<Vec<LocalRecord>> {
            Ok(self
                .data
                .iter()
                .filter(|(k, _)| b.contains(&self.order, k))
                .map(|(k, v)| LocalRecord::new(k, v, self.mode).unwrap())
                .collect())
        }
    }

    fn data(n: usize) -> Data {
        (0..n)
            .map(|i| {
                (
                    format!("k{i:04}").into_bytes(),
                    encode(i as u64 + 1, None, format!("v{i}").as_bytes()),
                )
            })
            .collect()
    }

    /// Runs the comparison of one CF with `chunk_keys`, `min_chunk_keys` and a key budget.
    async fn run(
        local: Data,
        upstream: &mut Fake,
        chunk_keys: usize,
        min_chunk_keys: usize,
        budget: usize,
    ) -> Vec<Mismatch> {
        let mode = upstream.mode;
        let (tx, mut rx) = mpsc::channel(4);
        let ranges = {
            let mut c = Chunker::new(mode, chunk_keys, usize::MAX);
            let mut out = Vec::new();
            for (k, v) in &local {
                out.extend(c.push(k, v).unwrap());
            }
            out.push(c.finish());
            out
        };
        tokio::spawn(async move {
            for r in ranges {
                tx.send(Ok(r)).await.unwrap();
            }
        });
        let side = Local {
            order: KeyOrder::bytewise(),
            mode,
            data: local,
        };
        let mut budget = Budget::new(budget);
        let mut out = Vec::new();
        let mut cf = CfRun {
            cf: "c",
            order: KeyOrder::bytewise(),
            min_chunk_keys,
            budget: &mut budget,
            out: &mut out,
        };
        compare_cf(upstream, &side, &mut cf, &mut rx).await.unwrap();
        out
    }

    fn key(k: &str, local: Option<u64>, upstream: Option<u64>) -> Mismatch {
        Mismatch::Key {
            cf: "c".into(),
            key: k.as_bytes().to_vec(),
            local,
            upstream,
        }
    }

    #[tokio::test]
    async fn identical_data_needs_only_the_pipelined_hashes() {
        let mut up = Fake::new(data(1000), VerifyMode::Full);
        let out = run(data(1000), &mut up, 10, 4, 100).await;
        assert!(out.is_empty(), "{out:?}");
        assert_eq!(
            up.hash_batches,
            vec![16; 6].into_iter().chain([4]).collect::<Vec<_>>(),
            "100 ranges, 16 per window"
        );
        assert!(up.lists.is_empty());
    }

    #[tokio::test]
    async fn a_key_only_upstream_between_boundaries_is_found_by_bisection() {
        let mut upstream = data(1000);
        upstream.insert(455, (b"k0454a".to_vec(), encode(7, None, b"x")));
        let mut up = Fake::new(upstream, VerifyMode::Quick);
        let out = run(data(1000), &mut up, 100, 4, 100).await;
        assert_eq!(out, vec![key("k0454a", None, Some(7))]);
        assert_eq!(up.lists.len(), 1, "only the narrowed leaf is listed");
        let leaf = &up.lists[0];
        let local_in_leaf = data(1000)
            .iter()
            .filter(|(k, _)| leaf.contains(&KeyOrder::bytewise(), k))
            .count();
        assert!(
            local_in_leaf <= 4,
            "the listed range holds {local_in_leaf} local keys"
        );
        assert!(leaf.contains(&KeyOrder::bytewise(), b"k0454a"));
    }

    #[tokio::test]
    async fn missing_extra_and_changed_keys_including_the_open_ended_ranges() {
        let local = data(300);
        let mut upstream = data(300);
        upstream.remove(299); // last key: extra locally (range without upper bound)
        upstream.remove(150); // middle key: extra locally
        upstream.remove(0); // first key: extra locally (range without lower bound)
        upstream.push((b"k9999".to_vec(), encode(5, None, b"new"))); // only upstream, past every local key
        upstream.insert(0, (b"a".to_vec(), encode(6, None, b"new"))); // only upstream, before every local key
        upstream[100].1 = encode(100, None, b"stale"); // k0100: an older write_seq upstream
        let mut full = Fake::new(upstream.clone(), VerifyMode::Full);
        let out = run(local.clone(), &mut full, 32, 4, 100).await;
        assert_eq!(
            out,
            vec![
                key("a", None, Some(6)),
                key("k0000", Some(1), None),
                key("k0100", Some(101), Some(100)),
                key("k0150", Some(151), None),
                key("k0299", Some(300), None),
                key("k9999", None, Some(5)),
            ]
        );
        // Quick does not see the value change, only the write_seq.
        let mut quick = Fake::new(upstream, VerifyMode::Quick);
        let out = run(local, &mut quick, 32, 4, 100).await;
        assert_eq!(out.len(), 6, "{out:?}");
    }

    #[tokio::test]
    async fn equal_write_seq_with_another_value_is_only_seen_in_full_mode() {
        let mut upstream = data(50);
        upstream[20].1 = encode(21, None, b"tampered");
        let mut quick = Fake::new(upstream.clone(), VerifyMode::Quick);
        assert!(run(data(50), &mut quick, 8, 2, 100).await.is_empty());
        let mut full = Fake::new(upstream, VerifyMode::Full);
        assert_eq!(
            run(data(50), &mut full, 8, 2, 100).await,
            vec![key("k0020", Some(21), Some(21))]
        );
    }

    #[tokio::test]
    async fn listings_are_paged_without_repeats_and_the_budget_ends_in_a_range() {
        // Locally empty: one range (-inf, +inf), 3000 keys only upstream, listed in pages of 1024.
        let mut up = Fake::new(data(3000), VerifyMode::Quick);
        let out = run(Vec::new(), &mut up, 10, 4, 10_000).await;
        assert_eq!(out.len(), 3000);
        assert!(
            out.iter()
                .enumerate()
                .all(|(i, m)| *m == key(&format!("k{i:04}"), None, Some(i as u64 + 1)))
        );
        assert_eq!(up.lists.len(), 3);
        assert_eq!(
            up.lists[1].lower.as_deref(),
            Some(&b"k1023"[..]),
            "the next page starts at the last key"
        );

        let mut up = Fake::new(data(3000), VerifyMode::Quick);
        let out = run(Vec::new(), &mut up, 10, 4, 100).await;
        assert_eq!(out.len(), 101);
        assert_eq!(
            out[100],
            Mismatch::Range {
                cf: "c".into(),
                lower: Some(b"k0100".to_vec()),
                upper: None
            }
        );
    }

    #[tokio::test]
    async fn a_listing_that_cannot_progress_is_reported_as_a_range() {
        // Each page is cut after 2 keys by size; a page that only repeats the resume key
        // cannot happen with a conforming server, so paging always advances.
        let mut up = Fake::new(data(7), VerifyMode::Full);
        up.soft_bytes = 1;
        let out = run(Vec::new(), &mut up, 10, 4, 100).await;
        assert_eq!(out.len(), 7);
        assert_eq!(
            up.lists.len(),
            6,
            "2 new keys on the first page, 1 on each later page"
        );

        // A key too long to send back as a lower bound ends the listing with a range.
        let big = vec![b'k'; MAX_RESUME_KEY_BYTES + 1];
        let rows = vec![
            (b"a".to_vec(), encode(1, None, b"")),
            (big.clone(), encode(2, None, b"")),
            (b"z".to_vec(), encode(3, None, b"")),
        ];
        let mut up = Fake::new(rows, VerifyMode::Quick);
        up.soft_bytes = 1;
        let out = run(Vec::new(), &mut up, 10, 4, 100).await;
        assert_eq!(
            out,
            vec![
                key("a", None, Some(1)),
                Mismatch::Key {
                    cf: "c".into(),
                    key: big.clone(),
                    local: None,
                    upstream: Some(2)
                },
                Mismatch::Range {
                    cf: "c".into(),
                    lower: Some(big),
                    upper: None
                },
            ]
        );
    }

    #[test]
    fn split_points_avoid_long_keys_and_small_ranges() {
        let rec = |k: Vec<u8>| LocalRecord {
            key: k,
            write_seq: 1,
            payload: vec![],
        };
        let small: Vec<LocalRecord> = (0..4u8).map(|i| rec(vec![i])).collect();
        assert_eq!(split_point(&small, 4), None);
        assert_eq!(split_point(&small, 3), Some(2));
        let mut long = small.clone();
        long[2].key = vec![2; MAX_BOUNDARY_KEY_BYTES + 1];
        assert_eq!(
            split_point(&long, 1),
            Some(1),
            "nearest boundary-capable key to the median"
        );
        let all_long: Vec<LocalRecord> = (0..3u8)
            .map(|i| rec(vec![i; MAX_BOUNDARY_KEY_BYTES + 1]))
            .collect();
        assert_eq!(split_point(&all_long, 1), None);
    }

    #[test]
    fn cf_lists_are_compared_by_name_comparator_and_create_seq() {
        let e = |n: &str, cmp: Option<&str>, seq| CfEntry {
            spec: CfSpec {
                name: n.into(),
                comparator: cmp.map(str::to_string),
            },
            create_seq: seq,
        };
        let local = vec![
            e("a", None, 1),
            e("b", None, 2),
            e("c", Some("rev"), 3),
            e("only-local", None, 4),
        ];
        let upstream = vec![
            e("a", None, 1),
            e("b", None, 9),
            e("c", None, 3),
            e("only-up", None, 5),
        ];
        let (m, common) = compare_specs(&local, &upstream);
        let names: Vec<&str> = m
            .iter()
            .map(|m| match m {
                Mismatch::CfSpec { name, .. } => name.as_str(),
                _ => unreachable!(),
            })
            .collect();
        assert_eq!(names, vec!["b", "c", "only-local", "only-up"]);
        assert_eq!(
            common
                .iter()
                .map(|e| e.spec.name.as_str())
                .collect::<Vec<_>>(),
            vec!["a", "b"]
        );
        assert_eq!(
            m[2],
            Mismatch::CfSpec {
                name: "only-local".into(),
                local: Some(CfVersion {
                    spec: CfSpec::new("only-local"),
                    create_seq: 4
                }),
                upstream: None
            }
        );
    }
}
```

Run: `cargo test --lib verify::compare`
Expected: FAIL to compile: `cannot find trait Oracle in this scope`, `cannot find function compare_cf`, `cannot find struct CfRun`.

- [ ] **Step 9: Implement `src/verify/compare.rs`** (above the test module)

Insert at the top of `src/verify/compare.rs`, above `#[cfg(test)]`:

```rust
#[cfg(test)]
mod tests {
```

insert:

```rust
//! The comparison a slave runs for each CF (PROTOCOL.md §9.2, §9.4): hash every local range
//! upstream, `VERIFY_WINDOW` requests at a time; bisect each mismatched range at its local
//! median key down to `min_chunk_keys`; then list the upstream keys of that range (paging
//! through truncated answers) and diff them key by key against the local records.
//!
//! Generic over the upstream ([`Oracle`]) and the local data ([`LocalSide`]), so the logic is
//! unit-tested without a network.

use std::cmp::Ordering;

use tokio::sync::mpsc;

use super::range::{Bounds, KeyOrder, LocalRange, LocalRecord, local_range};
use super::{
    CfVersion, MAX_BOUNDARY_KEY_BYTES, MAX_LIST_LIMIT, MAX_RESUME_KEY_BYTES, Mismatch,
    VERIFY_WINDOW,
};
use crate::error::Result;
use crate::pb;
use crate::types::CfEntry;

/// The upstream side of one session.
pub(crate) trait Oracle {
    /// Sends one `HashRange` per range (all before reading any answer) and returns `matched`
    /// for each, in order.
    async fn hash(&mut self, cf: &str, ranges: &[LocalRange]) -> Result<Vec<bool>>;
    /// One `ListRange`: the keys of `bounds` (up to `limit`) and whether there are more.
    async fn list(
        &mut self,
        cf: &str,
        bounds: &Bounds,
        limit: u32,
    ) -> Result<(Vec<pb::KeyDigest>, bool)>;
}

/// The local side: the records of a range, read from the same snapshot as the ranges.
pub(crate) trait LocalSide {
    async fn load(&self, bounds: &Bounds) -> Result<Vec<LocalRecord>>;
}

/// How many more [`Mismatch::Key`] a run may report.
#[derive(Debug)]
pub(crate) struct Budget(usize);

impl Budget {
    pub(crate) fn new(max_keys: usize) -> Self {
        Budget(max_keys)
    }

    fn take(&mut self) -> bool {
        if self.0 == 0 {
            return false;
        }
        self.0 -= 1;
        true
    }
}

/// One CF being compared.
pub(crate) struct CfRun<'a> {
    pub cf: &'a str,
    pub order: KeyOrder,
    pub min_chunk_keys: usize,
    pub budget: &'a mut Budget,
    pub out: &'a mut Vec<Mismatch>,
}

impl CfRun<'_> {
    /// Reports a key difference; `false` once the budget is spent.
    fn key(&mut self, key: &[u8], local: Option<u64>, upstream: Option<u64>) -> bool {
        if !self.budget.take() {
            return false;
        }
        self.out.push(Mismatch::Key {
            cf: self.cf.to_string(),
            key: key.to_vec(),
            local,
            upstream,
        });
        true
    }

    fn range(&mut self, lower: Option<Vec<u8>>, upper: Option<Vec<u8>>) {
        self.out.push(Mismatch::Range {
            cf: self.cf.to_string(),
            lower,
            upper,
        });
    }
}

/// Compares one CF. `ranges` yields the local ranges in order (contiguous, covering the CF).
pub(crate) async fn compare_cf<O: Oracle, L: LocalSide>(
    oracle: &mut O,
    local: &L,
    run: &mut CfRun<'_>,
    ranges: &mut mpsc::Receiver<Result<LocalRange>>,
) -> Result<()> {
    let mut mismatched = Vec::new();
    let mut batch = Vec::with_capacity(VERIFY_WINDOW);
    loop {
        let done = match ranges.recv().await {
            Some(r) => {
                batch.push(r?);
                false
            }
            None => true,
        };
        if batch.len() == VERIFY_WINDOW || (done && !batch.is_empty()) {
            let matched = oracle.hash(run.cf, &batch).await?;
            for (r, ok) in batch.drain(..).zip(matched) {
                if !ok {
                    mismatched.push(r.bounds);
                }
            }
        }
        if done {
            break;
        }
    }
    for bounds in mismatched {
        let records = local.load(&bounds).await?;
        narrow(oracle, run, bounds, &records).await?;
    }
    Ok(())
}

/// The index to split `records` at: the key nearest the median that can be a boundary.
/// `None` when the range is small enough to list.
fn split_point(records: &[LocalRecord], min_chunk_keys: usize) -> Option<usize> {
    if records.len() <= min_chunk_keys.max(1) {
        return None;
    }
    let mid = records.len() / 2;
    (0..records.len())
        .flat_map(|d| [mid.checked_sub(d), mid.checked_add(d)])
        .flatten()
        .filter(|&i| i >= 1 && i < records.len())
        .find(|&i| records[i].key.len() <= MAX_BOUNDARY_KEY_BYTES)
}

/// Bisects a mismatched range at local median keys, hashing both halves, until the
/// mismatched parts are small enough to list.
async fn narrow<O: Oracle>(
    oracle: &mut O,
    run: &mut CfRun<'_>,
    bounds: Bounds,
    records: &[LocalRecord],
) -> Result<()> {
    let mut stack = vec![(bounds, 0..records.len())];
    while let Some((bounds, idx)) = stack.pop() {
        let recs = &records[idx.clone()];
        let Some(i) = split_point(recs, run.min_chunk_keys) else {
            list_and_diff(oracle, run, &bounds, recs).await?;
            continue;
        };
        let at = recs[i].key.clone();
        let left = local_range(
            Bounds {
                lower: bounds.lower.clone(),
                upper: Some(at.clone()),
            },
            &recs[..i],
        );
        let right = local_range(
            Bounds {
                lower: Some(at),
                upper: bounds.upper.clone(),
            },
            &recs[i..],
        );
        let matched = oracle.hash(run.cf, &[left.clone(), right.clone()]).await?;
        // Right first, so the left half is narrowed (and reported) first.
        if !matched[1] {
            stack.push((right.bounds, idx.start + i..idx.end));
        }
        if !matched[0] {
            stack.push((left.bounds, idx.start..idx.start + i));
        }
    }
    Ok(())
}

/// Lists the upstream keys of `bounds` and diffs them against `local` (the local records of
/// the same range, in order). A truncated listing is continued with `lower` = its last key,
/// whose repeat at the start of the next answer is skipped; this works under any comparator.
async fn list_and_diff<O: Oracle>(
    oracle: &mut O,
    run: &mut CfRun<'_>,
    bounds: &Bounds,
    local: &[LocalRecord],
) -> Result<()> {
    let mut i = 0;
    let mut page = bounds.clone();
    let mut resume: Option<Vec<u8>> = None;
    loop {
        let (keys, truncated) = oracle.list(run.cf, &page, MAX_LIST_LIMIT).await?;
        let mut last: Option<Vec<u8>> = None;
        for u in keys {
            if resume.as_deref() == Some(u.key.as_slice()) {
                continue;
            }
            // Local keys before `u` exist only locally.
            while i < local.len() && run.order.cmp(&local[i].key, &u.key) == Ordering::Less {
                if !run.key(&local[i].key, Some(local[i].write_seq), None) {
                    run.range(Some(local[i].key.clone()), bounds.upper.clone());
                    return Ok(());
                }
                i += 1;
            }
            let reported =
                if i < local.len() && run.order.cmp(&local[i].key, &u.key) == Ordering::Equal {
                    let l = &local[i];
                    i += 1;
                    let same = l.write_seq == u.write_seq
                        && blake3::hash(&l.payload).as_bytes()[..] == u.payload_hash[..];
                    same || run.key(&u.key, Some(l.write_seq), Some(u.write_seq))
                } else {
                    run.key(&u.key, None, Some(u.write_seq))
                };
            if !reported {
                run.range(Some(u.key.clone()), bounds.upper.clone());
                return Ok(());
            }
            last = Some(u.key);
        }
        if !truncated {
            break;
        }
        // Continue after the last key, unless that cannot make progress or the key is too
        // long to send back; then the rest of the range is reported as a whole.
        match last {
            Some(k) if k.len() <= MAX_RESUME_KEY_BYTES => {
                page.lower = Some(k.clone());
                resume = Some(k);
            }
            other => {
                run.range(other.or_else(|| page.lower.clone()), bounds.upper.clone());
                return Ok(());
            }
        }
    }
    while i < local.len() {
        if !run.key(&local[i].key, Some(local[i].write_seq), None) {
            run.range(Some(local[i].key.clone()), bounds.upper.clone());
            return Ok(());
        }
        i += 1;
    }
    Ok(())
}

fn version(e: &CfEntry) -> CfVersion {
    CfVersion {
        spec: e.spec.clone(),
        create_seq: e.create_seq,
    }
}

/// Compares the CF lists (both sorted by name). Returns the differences and the CFs whose
/// data can be compared: present on both sides with the same comparator.
pub(crate) fn compare_specs(
    local: &[CfEntry],
    upstream: &[CfEntry],
) -> (Vec<Mismatch>, Vec<CfEntry>) {
    let mut names: Vec<&str> = local
        .iter()
        .chain(upstream)
        .map(|e| e.spec.name.as_str())
        .collect();
    names.sort_unstable();
    names.dedup();
    let mut mismatches = Vec::new();
    let mut common = Vec::new();
    for name in names {
        let l = local.iter().find(|e| e.spec.name == name);
        let u = upstream.iter().find(|e| e.spec.name == name);
        if l != u {
            mismatches.push(Mismatch::CfSpec {
                name: name.to_string(),
                local: l.map(version),
                upstream: u.map(version),
            });
        }
        if let (Some(l), Some(u)) = (l, u) {
            if l.spec.comparator == u.spec.comparator {
                common.push(l.clone());
            }
        }
    }
    (mismatches, common)
}
```

Notes for the implementer:
- Bisection works on the local records of one mismatched range, loaded once (a range is bounded by `chunk_keys`/`chunk_bytes`, plus at most one oversized record). Sub-range hashes are computed in memory with `local_range`, so only the leaves are listed.
- A truncated listing is continued from its last key, not from "last key + `\0`": that is a successor only under bytewise order. The server repeats the resume key first; it is skipped by exact byte equality.
- `Budget` caps `Mismatch::Key` per run; once spent, the rest of the current leaf becomes one `Mismatch::Range` and every later leaf one more `Range`.

- [ ] **Step 10: Run all verify-core tests**

Run: `cargo test --lib verify::`
Expected: PASS, 21 tests (9 range, 4 scan, 8 compare: `identical_data_needs_only_the_pipelined_hashes` — 100 ranges hashed as six windows of 16 and one of 4, no listing; `a_key_only_upstream_between_boundaries_is_found_by_bisection` — one listing, of a leaf with at most 4 local keys; `missing_extra_and_changed_keys_including_the_open_ended_ranges`; `equal_write_seq_with_another_value_is_only_seen_in_full_mode`; `listings_are_paged_without_repeats_and_the_budget_ends_in_a_range`; `a_listing_that_cannot_progress_is_reported_as_a_range`; `split_points_avoid_long_keys_and_small_ranges`; `cf_lists_are_compared_by_name_comparator_and_create_seq`).

Run: `cargo test`
Expected: PASS.

- [ ] **Step 11: Commit**

```bash
cargo fmt
git add src
git commit -m "feat: verify core: range hashing, listings, chunking, bisection and key diff

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Server Verify RPCs — sessions, `HashRange`, `ListRange`, limits

**Files:**
- Create: `src/server/verify.rs`
- Modify: `src/server/mod.rs`, `src/server/tests.rs` (remove `verify_is_not_implemented_yet`), `PROTOCOL.md` (§9.2, §9.4, §12)

**Interfaces:**
- Consumes: Task 3 `verify::{VerifyMode::from_pb, LIST_SOFT_BYTES, MAX_LIST_LIMIT}`, `verify::range::{Bounds::from_pb, KeyOrder::new}`, `verify::scan::{hash_range, list_range}`; Plan 2 `ServerState { source: Weak<dyn LogSource>, cfg: ServeConfig, shutdown: watch::Receiver<bool>, replicas, … }`, `ServerState::authorize(&self, &CallInfo, Method) -> Result<Arc<dyn LogSource>, Status>`, `call_info(&Request<T>, Method) -> Result<CallInfo, Status>`, `CallInfo::check_client_id`, `store_status(Error) -> Status`, `stopped`, `end_stream(&mpsc::Sender<Result<T, Status>>, Status, &mut watch::Receiver<bool>, &mut watch::Receiver<u64>)`, `VerifyRangeStream`, `ServerHandle::{shutdown, abort}`, `sweep_leases`, `LogSource::{is_ready, resets, store, node_id, upstream_chain}`, `ReadSnapshot::{meta, phys}`, `net::{reason_status, reason_of, invalid_argument, internal_status, convert::{cf_to_pb, identity_to_pb}}`, `ServeLimits::{max_verify_sessions, verify_session_timeout}`, test helpers `server::test_client::{client, serving_master, Client}`.
- Produces:
  - `server::verify::{Sessions (new(max: usize, timeout: Duration), expire(&self, Instant, reset_gen: Option<u64>), clear(&self)), Session, begin, range, end}`; `ServerState.verify_sessions: verify::Sessions`.
  - Wire behaviour: `VerifyBegin` → `VerifyBeginResponse { session_id (16 random bytes), identity, snapshot_seq, cf_specs, session_timeout_ms }`, `RESOURCE_EXHAUSTED/BUSY` at `max_verify_sessions`, `UNAVAILABLE/NOT_READY` while bootstrapping, `INVALID_ARGUMENT` for an unspecified mode or a `client_id` that is not the node-id metadata; `VerifyRange` answers in order (`request_id` echoed), ends with `NOT_FOUND/SESSION_NOT_FOUND` (unknown/expired), `UNAVAILABLE/UPSTREAM_RESET` (reset since the session began), `UNAVAILABLE/SHUTTING_DOWN`, `INVALID_ARGUMENT`; `ListResult` cut at ~1 MiB but with at least two keys; `VerifyEnd` removes the session (`NOT_FOUND` if unknown).

- [ ] **Step 1: Write the failing server tests**

Create `src/server/verify.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::Code;

    use crate::codec::envelope;
    use crate::net::convert::identity_to_pb;
    use crate::net::reason_of;
    use crate::pb;
    use crate::pb::verify_range_request::Kind as Req;
    use crate::pb::verify_range_response::Kind as Resp;
    use crate::server::test_client::{Client, client, serving_master};
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

    /// A client that opens a session and walks away must not keep the database open.
    #[tokio::test]
    async fn open_sessions_do_not_keep_the_database_open() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let s = begin(&mut c, pb::VerifyMode::Full).await.unwrap();
        // An open VerifyRange stream with an unanswered client, too.
        let (tx, rx) = tokio::sync::mpsc::channel(4);
        let _stream = c
            .verify_range(tokio_stream::wrappers::ReceiverStream::new(rx))
            .await
            .unwrap();
        tx.send(hash(&s.session_id, 1, range(None, None), 0, vec![]))
            .await
            .unwrap();
        m.shutdown()
            .await
            .expect("shutdown releases the sessions' snapshots");
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
```

In `src/server/mod.rs`, add the module:

```rust
pub(crate) mod status;
mod tail;
```

with:

```rust
pub(crate) mod status;
mod tail;
pub(crate) mod verify;
```

In `src/server/tests.rs`, delete the test this task makes obsolete:

```rust
#[tokio::test]
async fn verify_is_not_implemented_yet() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let e = client(m.serve_addr().unwrap(), "b")
        .verify_begin(pb::VerifyBeginRequest::default())
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::Unimplemented);
    assert_eq!(
        reason_of(&e),
        Some(pb::Reason::Unspecified),
        "UNIMPLEMENTED carries ErrorInfo"
    );
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib server::verify`
Expected: the tests compile (they use only Task 3 and Plan 2 items) and FAIL at runtime, because the Verify RPCs still answer `UNIMPLEMENTED`: e.g. ``called `Result::unwrap()` on an `Err` value: Status { code: Unimplemented, message: "Verify is not implemented by this version", … }``, and `malformed_requests_are_invalid_arguments` fails with `left: Unimplemented, right: InvalidArgument`.

- [ ] **Step 3: Implement the sessions and handlers**

Insert at the top of `src/server/verify.rs`, above `#[cfg(test)]`:

```rust
#[cfg(test)]
mod tests {
```

insert:

```rust
//! Verify sessions (PROTOCOL.md §9). `VerifyBegin` pins a snapshot and reads `S`, the
//! identity and the CF list through it; `VerifyRange` answers `HashRange` and `ListRange`
//! from that snapshot, in request order; `VerifyEnd` (or `verify_session_timeout` of
//! idleness) releases it. A session never outlives a reset of the node's state, nor the
//! server's shutdown, so it can never keep a closed node's database open.

use std::collections::HashMap;
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
use crate::verify::range::{Bounds, KeyOrder};
use crate::verify::scan;
use crate::verify::{LIST_SOFT_BYTES, MAX_LIST_LIMIT, VerifyMode};

/// Answers buffered per stream (the client pipelines at most `VERIFY_WINDOW` = 16 requests).
const CHANNEL: usize = 16;

/// One open session: a snapshot at `S` and what is needed to read it.
pub(crate) struct Session {
    snap: ReadSnapshot,
    mode: VerifyMode,
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
    let call = call_info(&req, Method::Verify)?;
    let src = state.authorize(&call, Method::Verify).await?;
    let r = req.into_inner();
    call.check_client_id(&r.client_id)?;
    let mode = VerifyMode::from_pb(r.mode)
        .ok_or_else(|| net::invalid_argument("mode must be QUICK or FULL"))?;
    if !src.is_ready() {
        return Err(reason_status(
            pb::Reason::NotReady,
            "server is bootstrapping",
        ));
    }
    let reset_gen = *src.resets().borrow();
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
        cfs.insert(
            e.spec.name.clone(),
            (
                phys,
                KeyOrder::new(&comparators, e.spec.comparator.as_deref()),
            ),
        );
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
        snap,
        mode,
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
        let answer = match answer(&state, req, reset_gen).await {
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
) -> Result<pb::VerifyRangeResponse, Status> {
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
        .ok_or_else(|| net::invalid_argument("VerifyRangeRequest needs hash or list"))?;
    let s = session.clone();
    let kind = tokio::task::spawn_blocking(move || compute(&s, kind))
        .await
        .map_err(|e| internal_status(format!("verify task failed: {e}")))??;
    // Idle time counts from the end of the last request.
    session.touch();
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

/// Runs on a blocking thread.
fn compute(s: &Session, kind: ReqKind) -> Result<RespKind, Status> {
    let _span = s.span.enter();
    match kind {
        ReqKind::Hash(h) => {
            let (phys, order, bounds) = target(s, h.range.as_ref())?;
            let (count, hash) = scan::hash_range(&s.snap, phys.clone(), order, &bounds, s.mode)
                .map_err(store_status)?;
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
            let (keys, truncated) = scan::list_range(
                &s.snap,
                phys.clone(),
                order,
                &bounds,
                s.mode,
                l.limit as usize,
                LIST_SOFT_BYTES,
            )
            .map_err(store_status)?;
            Ok(RespKind::List(pb::ListResult { keys, truncated }))
        }
    }
}
```

- [ ] **Step 4: Wire the sessions into the server**

In `src/server/mod.rs`, add a field to `struct ServerState`:

```rust
    /// The node's downstream replicas (shared with the node, which reports and waits on them).
    pub(crate) replicas: Arc<ReplicaRegistry>,
}
```

with:

```rust
    /// The node's downstream replicas (shared with the node, which reports and waits on them).
    pub(crate) replicas: Arc<ReplicaRegistry>,
    /// Open verify sessions; each owns a snapshot (PROTOCOL.md §9).
    pub(crate) verify_sessions: verify::Sessions,
}
```

initialise it in `start()` (before `cfg` is moved):

```rust
        leases: Mutex::new(HashMap::new()),
        shutdown: shutdown_rx.clone(),
        cfg,
```

with:

```rust
        leases: Mutex::new(HashMap::new()),
        shutdown: shutdown_rx.clone(),
        verify_sessions: verify::Sessions::new(
            cfg.limits.max_verify_sessions,
            cfg.limits.verify_session_timeout,
        ),
        cfg,
```

replace the `UNIMPLEMENTED` stubs `verify_begin` and `verify_range`:

```rust
    async fn verify_begin(
        &self,
        _: Request<pb::VerifyBeginRequest>,
    ) -> Result<Response<pb::VerifyBeginResponse>, Status> {
        Err(net::unimplemented(
            "Verify is not implemented by this version",
        ))
    }

    type VerifyRangeStream = VerifyRangeStream;

    async fn verify_range(
        &self,
        _: Request<tonic::Streaming<pb::VerifyRangeRequest>>,
    ) -> Result<Response<VerifyRangeStream>, Status> {
        Err(net::unimplemented(
            "Verify is not implemented by this version",
        ))
    }
```

with:

```rust
    async fn verify_begin(
        &self,
        req: Request<pb::VerifyBeginRequest>,
    ) -> Result<Response<pb::VerifyBeginResponse>, Status> {
        verify::begin(&self.0, req).await
    }

    type VerifyRangeStream = VerifyRangeStream;

    async fn verify_range(
        &self,
        req: Request<tonic::Streaming<pb::VerifyRangeRequest>>,
    ) -> Result<Response<VerifyRangeStream>, Status> {
        verify::range(&self.0, req).await
    }
```

and the `verify_end` stub:

```rust
    async fn verify_end(
        &self,
        _: Request<pb::VerifyEndRequest>,
    ) -> Result<Response<pb::VerifyEndResponse>, Status> {
        Err(net::unimplemented(
            "Verify is not implemented by this version",
        ))
    }
```

with:

```rust
    async fn verify_end(
        &self,
        req: Request<pb::VerifyEndRequest>,
    ) -> Result<Response<pb::VerifyEndResponse>, Status> {
        verify::end(&self.0, req).await
    }
```

In `sweep_leases`, replace the tick arm:

```rust
            _ = tick.tick() => state.expire_leases(Instant::now()),
```

with:

```rust
            _ = tick.tick() => {
                let now = Instant::now();
                state.expire_leases(now);
                // Sessions idle too long, or opened before a reset of this node's state.
                let reset_gen = state.source.upgrade().map(|s| *s.resets().borrow());
                state.verify_sessions.expire(now, reset_gen);
            }
```

In `ServerHandle::shutdown` and in `ServerHandle::abort`, after the leases are cleared, clear the sessions too: (all 2 occurrences)

```rust
        self.state.leases().clear();
```

with:

```rust
        self.state.leases().clear();
        self.state.verify_sessions.clear();
```

`net::unimplemented` has no user left; in `src/net/mod.rs`, delete:

```rust
/// `UNIMPLEMENTED` with `ErrorInfo` (`REASON_UNSPECIFIED`).
pub(crate) fn unimplemented(message: impl Into<String>) -> Status {
    unspecified_status(Code::Unimplemented, message)
}
```

Notes for the implementer:
- A `Session` owns a `ReadSnapshot`, which holds the node's `DbCore`. That is why sessions are cleared on shutdown, expired by the sweep, and dropped when the node's reset generation moves on (epoch bump, bootstrap or repair of a serving slave): otherwise a session could keep a closed or replaced database open.
- `VerifyRange` serves one stream on its own task. It holds only `Arc<ServerState>` (whose source is weak) and, per request, an `Arc<Session>`; each request runs on a blocking thread.
- Idle time is measured from the end of the last request on the session.

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib server::`
Expected: PASS: 6 new tests in `server::verify::tests` (`a_session_answers_hashes_and_listings_from_its_snapshot`, `sessions_are_limited_and_ending_one_frees_a_slot`, `idle_sessions_expire_and_free_their_slot`, `malformed_requests_are_invalid_arguments`, `open_sessions_do_not_keep_the_database_open`, `an_epoch_bump_ends_verify_streams_and_sessions`) and every Plan 2 server test.

Run: `cargo test`
Expected: PASS.

- [ ] **Step 6: Update PROTOCOL.md**

In PROTOCOL.md §9.2, replace:

```markdown
- `ListRange`: the server returns up to `limit` keys from the range, in order, each with its `write_seq` and `payload_hash = BLAKE3(payload)`. `truncated` is set if there are more. `limit` MUST NOT exceed 1024.
```

with:

```markdown
- `ListRange`: the server returns up to `limit` keys from the range, in order, each with its `write_seq` and `payload_hash = BLAKE3(payload)`. `truncated` is set if there are more. `limit` MUST be between 1 and 1024. The server MAY return fewer than `limit` keys to bound the size of the answer (RECOMMENDED: stop once the keys add up to 1 MiB), but it MUST return at least two keys when the range holds two or more.
- To continue a truncated listing, the client sends the same range with `lower` set to the last key it received, and skips that key when the answer repeats it first. This works under any comparator (appending a byte to a key gives its successor only under bytewise order), and the two-key rule guarantees progress.
- A request whose `cf` is not in the session's snapshot, whose `range` or `kind` is missing, or whose `limit` is out of bounds fails with `INVALID_ARGUMENT`. A `VerifyBegin` with `mode = VERIFY_MODE_UNSPECIFIED` fails the same way.
- The server ends every `VerifyRange` stream, and drops every session, with `UNAVAILABLE` / `UPSTREAM_RESET` when its state is replaced (it starts bootstrapping itself, or its identity changes), as it does for `Tail` (§7.3).
```

In PROTOCOL.md §9.4, replace:

```markdown
Clients SHOULD limit their own read rate (`max_bytes_per_sec`), and servers MAY limit theirs.
```

with:

```markdown
Clients SHOULD limit their own read rate (`max_bytes_per_sec`), and servers MAY limit theirs. A client SHOULD NOT use very long keys as range boundaries, so that every request stays far below the server's request size limit; this implementation never uses a key longer than 1 MiB as a boundary, and continues a truncated listing only from a key of at most 2 MiB (otherwise it reports the rest of the range as not compared).
```

In the PROTOCOL.md §12 table, replace:

```markdown
| `ListRange.limit` | client | ≤ 1024 |
```

with:

```markdown
| `ListRange.limit` | client | ≤ 1024 |
| `ListResult` size | server | stop after ~1 MiB of keys, but never fewer than 2 keys |
```

Run: `cargo test --test proto_sync`
Expected: PASS (the protobuf block is unchanged).

- [ ] **Step 7: Commit**

```bash
cargo fmt
git add src PROTOCOL.md
git commit -m "feat: verify sessions with HashRange and ListRange on a pinned snapshot

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Slave verifier — capture at `S`, `verify_now`, reports, policy and scheduling

**Files:**
- Create: `src/slave/verifier.rs`, `tests/verify.rs`
- Modify: `src/slave/inner.rs`, `src/slave/apply.rs`, `src/slave/bootstrap.rs`, `src/slave/driver.rs`, `src/slave/client.rs`, `src/slave/config.rs`, `src/slave/state.rs`, `src/slave/mod.rs`, `src/listener.rs`, `tests/support/mod.rs`, `DESIGN.md` (§12, §16, §16.2, §18)

**Interfaces:**
- Consumes: Task 3 `verify::{VerifyMode, VerifyConfig, VerifyReport, VerifyAbort, MismatchPolicy, VERIFY_WINDOW, LIST_SOFT_BYTES}`, `verify::compare::{compare_cf, compare_specs, Budget, CfRun, Oracle, LocalSide}`, `verify::range::{Bounds, KeyOrder, LocalRange, LocalRecord, Throttle}`, `verify::scan::{chunk_cf, load_range}`; Task 4's server; Plan 2 `slave::inner::Inner { store, cfg, state_tx, shutdown_tx, max_entry_bytes, force_bootstrap: ForcedBootstrap, repoint_tx, … }` and `Inner::{new, ident, state, notify, status, upstream_uri}`, `ForcedBootstrap::{request, is_requested_for}`, `slave::client::{connect_upstream, CallMeta, Client}`, `background::blocking`, `net::{reason_of, reason_status, DECODE_SLACK, convert::{cf_from_pb, identity_from_pb}}`, the apply closure in `slave::apply::apply`, `slave::bootstrap::install`, the receive loop of `slave::driver::tail` and `unless_repointed`, `Slave { inner, driver, maintenance, server }`, `Slave::{open, shutdown, kill}`, harness `Cluster::{new, master, master_config, slave, slave_config}`, `assert_converged`, `wait_state`, `wait_until`, `RecordingListener`.
- Produces:
  - `SlaveConfig.verify: VerifyConfig` (validated in `SlaveConfig::validate`); `SlaveStatus.last_verify: Option<VerifyReport>`; `ReplicationListener::on_verify_complete(&self, &VerifyReport)` (default no-op).
  - `Slave::verify_now(&self, mode: VerifyMode) -> Result<VerifyReport>` (async).
  - `slave::inner::{Capture { Idle, Hold, At { seq, tx }, Cancelled }, CaptureHold (target(&self, Seq) -> oneshot::Receiver<CaptureReply>; dropping it releases the applier), CaptureMiss { Passed(Seq), Cancelled }, CaptureReply}`, `Inner::{hold_applier(self: &Arc<Self>) -> CaptureHold, wait_while_held(&self) (async), cancel_capture(&self), capture_after_commit(&self, Seq), set_last_verify(&self, VerifyReport), last_verify(&self) -> Option<VerifyReport>}`, fields `Inner.{verify_lock: tokio::sync::Mutex<()>, last_applied_ts: AtomicU64, rebootstrap: Notify}`.
  - `slave::verifier::{verify(&Arc<Inner>, VerifyMode) -> Result<VerifyReport>, spawn(&Arc<Inner>) -> Option<JoinHandle<()>>}`; `Slave.verifier: Option<JoinHandle<()>>`; `slave::client::connect_now(&Inner) -> Result<Client>` (async, eager).
  - Harness: `RecordingListener::verifies(&self) -> Vec<VerifyReport>` (field `pub verifies: Mutex<Vec<VerifyReport>>`).

- [ ] **Step 1: Write the failing end-to-end tests**

In `tests/support/mod.rs`, import `VerifyReport`:

```rust
    ReadApi, ReplicationListener, Seq, ServeConfig, Slave, SlaveConfig, SlaveState, UpstreamConfig,
    ValueMeta,
};
```

with:

```rust
    ReadApi, ReplicationListener, Seq, ServeConfig, Slave, SlaveConfig, SlaveState, UpstreamConfig,
    ValueMeta, VerifyReport,
};
```

add a field to `RecordingListener`:

```rust
    pub states: Mutex<Vec<SlaveState>>,
    pub violations: Mutex<Vec<String>>,
```

with:

```rust
    pub states: Mutex<Vec<SlaveState>>,
    pub verifies: Mutex<Vec<VerifyReport>>,
    pub violations: Mutex<Vec<String>>,
```

add to `impl RecordingListener`, before `assert_no_violations`:

```rust
    pub fn assert_no_violations(&self) {
```

insert:

```rust
    pub fn verifies(&self) -> Vec<VerifyReport> {
        self.verifies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
```

and to `impl ReplicationListener for RecordingListener`, after `on_state_change`:

```rust
    fn on_state_change(&self, state: &SlaveState) {
        self.states
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(state.clone());
    }
```

add:

```rust

    fn on_verify_complete(&self, report: &VerifyReport) {
        self.verifies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(report.clone());
    }
```

Create `tests/verify.rs`:

```rust
//! Verify end to end: a slave compares itself with its upstream over real gRPC.

mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, Error, HaltReason, Master, ReadApi, Slave, SlaveState, VerifyAbort, VerifyMode,
};
use support::{Cluster, assert_converged, wait_state, wait_until};

/// Values expired but not reaped, a DeleteRange, a CF dropped and re-created under the same
/// name, and an empty CF: all identical on disk, so both modes are clean.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_verifies_clean_in_both_modes() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("data")).unwrap();
    for i in 0..300u32 {
        m.put(
            "data",
            format!("k{i:03}").as_bytes(),
            format!("value-{i}").as_bytes(),
        )
        .unwrap();
    }
    m.put_with_ttl("data", b"ttl", b"soon gone", Duration::from_secs(5))
        .unwrap();
    m.delete_range("data", b"k100", b"k150").unwrap();
    m.create_cf(CfSpec::new("again")).unwrap();
    m.put("again", b"old", b"1").unwrap();
    m.drop_cf("again").unwrap();
    m.create_cf(CfSpec::new("again")).unwrap();
    m.put("again", b"new", b"2").unwrap();
    m.create_cf(CfSpec::new("empty")).unwrap();
    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.verify.chunk_keys = 16;
    cfg.verify.min_chunk_keys = 2;
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    c.clock.advance(Duration::from_secs(10));
    assert_eq!(
        b.get("data", b"ttl").unwrap(),
        None,
        "expired on reads, not reaped"
    );

    for mode in [VerifyMode::Quick, VerifyMode::Full] {
        let r = b.verify_now(mode).await.unwrap();
        assert!(r.is_clean(), "{mode:?}: {:?}", r.mismatches);
        assert_eq!((r.seq, r.mode, r.cfs_checked), (m.last_seq(), mode, 3));
        assert_eq!(
            r.keys_checked,
            250 + 1 + 1,
            "raw keys, the expired one included"
        );
    }
    assert_eq!(
        b.status().last_verify.map(|r| r.mode),
        Some(VerifyMode::Full)
    );
    assert_eq!(listener.verifies().len(), 2);
}

/// A→B convergence under concurrent writes, with Full verifies while the writes go on: each
/// run compares both sides at the upstream's snapshot seq, so it is clean however far the
/// slave has moved on.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_pins_the_snapshot_while_concurrent_writes_continue() {
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("data")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_state(&b, "bootstrapped and streaming", |s| {
        *s == SlaveState::Streaming
    })
    .await;
    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4u32)
        .map(|w| {
            let (m, stop) = (m.clone(), stop.clone());
            tokio::task::spawn_blocking(move || {
                let mut n = 0u32;
                while !stop.load(Ordering::SeqCst) {
                    // Overwrites too: write_seqs change under the verifier's feet.
                    m.put(
                        "data",
                        format!("w{w}-{:03}", n % 300).as_bytes(),
                        &n.to_be_bytes(),
                    )
                    .unwrap();
                    n += 1;
                }
                n
            })
        })
        .collect();
    let mut last = 0;
    for _ in 0..5 {
        let r = b.verify_now(VerifyMode::Full).await.unwrap();
        assert!(r.is_clean(), "{:?}", r.mismatches);
        assert!(r.seq >= last, "snapshot seqs never go back");
        last = r.seq;
    }
    stop.store(true, Ordering::SeqCst);
    for w in writers {
        assert!(w.await.unwrap() > 0);
    }
    assert!(
        m.last_seq() > last,
        "the writes went on during the verifies"
    );
    assert_converged(&*m, &[("b", &b)]).await;
    let r = b.verify_now(VerifyMode::Full).await.unwrap();
    assert!(r.is_clean() && r.seq == m.last_seq());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn verify_needs_data_and_the_same_identity() {
    let c = Cluster::new();
    let nowhere = std::net::TcpListener::bind("127.0.0.1:0")
        .unwrap()
        .local_addr()
        .unwrap();
    let (lonely, _) = c.slave("lonely", nowhere).await;
    assert!(matches!(
        lonely.verify_now(VerifyMode::Quick).await,
        Err(Error::NotReady)
    ));

    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let port = m.serve_addr().unwrap().port();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    // Another database takes over the upstream's address.
    m.shutdown().await.unwrap();
    let mut other = c.master_config("other");
    other.serve.as_mut().unwrap().port = port;
    let _other = Master::open(other).await.unwrap();
    wait_state(&b, "IdentityChanged halt", |s| {
        *s == SlaveState::Halted(HaltReason::IdentityChanged)
    })
    .await;
    let e = b.verify_now(VerifyMode::Quick).await.unwrap_err();
    assert!(
        matches!(e, Error::VerifyAborted(VerifyAbort::IdentityChanged)),
        "{e:?}"
    );
    assert!(
        b.status().last_verify.is_none(),
        "an abandoned run leaves no report"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn scheduled_verifies_run_on_their_interval() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        m.put("c", &[i], &[i]).unwrap();
    }
    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.verify.interval = Some(Duration::from_millis(200));
    cfg.verify.mode = VerifyMode::Full;
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    wait_until("two scheduled verifies", || listener.verifies().len() >= 2).await;
    assert!(
        listener
            .verifies()
            .iter()
            .all(|r| r.is_clean() && r.mode == VerifyMode::Full)
    );
    assert!(b.status().last_verify.unwrap().is_clean());
    // The scheduler holds the slave only during a run: shutdown still closes the database.
    b.shutdown().await.unwrap();
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --test verify`
Expected: FAIL to compile: `method on_verify_complete is not a member of trait ReplicationListener`, `no method named verify_now found for struct Slave`, `no field verify on type SlaveConfig`, `no field last_verify on type SlaveStatus`.

- [ ] **Step 3: Configuration, status and listener**

In `src/slave/config.rs`, import `VerifyConfig`:

```rust
use crate::tuning::RocksTuning;
```

with:

```rust
use crate::tuning::RocksTuning;
use crate::verify::VerifyConfig;
```

add the field to `SlaveConfig`:

```rust
    pub retention: Retention,
    pub listener: Option<Arc<dyn ReplicationListener>>,
```

with:

```rust
    pub retention: Retention,
    /// Integrity verification against the upstream (DESIGN.md §12). Scheduled runs are off
    /// unless `interval` is set; `Slave::verify_now` always works.
    pub verify: VerifyConfig,
    pub listener: Option<Arc<dyn ReplicationListener>>,
```

its default in `SlaveConfig::new`:

```rust
            retention: Retention::default(),
            listener: None,
```

with:

```rust
            retention: Retention::default(),
            verify: VerifyConfig::default(),
            listener: None,
```

and validate it in `SlaveConfig::validate`:

```rust
        if let Some(serve) = &self.serve {
            serve.validate()?;
            serve.check_heartbeat(self.heartbeat)?;
        }
```

with:

```rust
        self.verify.validate()?;
        if let Some(serve) = &self.serve {
            serve.validate()?;
            serve.check_heartbeat(self.heartbeat)?;
        }
```

In `src/slave/state.rs`, import `VerifyReport`:

```rust
use crate::types::{DbIdentity, Seq};
```

with:

```rust
use crate::types::{DbIdentity, Seq};
use crate::verify::VerifyReport;
```

and add the field to `SlaveStatus`:

```rust
    /// True after the listener panicked; it is not called again until restart.
    pub listener_failed: bool,
}
```

with:

```rust
    /// True after the listener panicked; it is not called again until restart.
    pub listener_failed: bool,
    /// The last verify report (scheduled or `verify_now`), if any run produced one.
    pub last_verify: Option<VerifyReport>,
}
```

In `src/listener.rs`, import `VerifyReport`:

```rust
use crate::types::{LogEntry, Seq};
```

with:

```rust
use crate::types::{LogEntry, Seq};
use crate::verify::VerifyReport;
```

and add the callback:

```rust
    fn on_state_change(&self, _state: &SlaveState) {}
```

with:

```rust
    fn on_state_change(&self, _state: &SlaveState) {}
    /// After every verify run that produced a report (scheduled or `Slave::verify_now`).
    /// Mismatches are in `report.mismatches`; under `MismatchPolicy::Rebootstrap` the
    /// re-bootstrap has already been requested.
    fn on_verify_complete(&self, _report: &VerifyReport) {}
```

- [ ] **Step 4: The capture slot and verify state in `Inner`**

In `src/slave/inner.rs`, replace the imports:

```rust
use tokio::sync::{Notify, watch};
```

with:

```rust
use tokio::sync::{Notify, oneshot, watch};
```

and:

```rust
use crate::listener::ReplicationListener;
use crate::store::Store;
```

with:

```rust
use crate::listener::ReplicationListener;
use crate::read::ReadSnapshot;
use crate::store::Store;
```

and:

```rust
use crate::types::{DbIdentity, EpochStart, Seq, newest_records};
```

with:

```rust
use crate::types::{DbIdentity, EpochStart, Seq, newest_records};
use crate::verify::VerifyReport;
```

Add the fields at the end of `struct Inner`:

```rust
    pub diverged_on_stream: AtomicBool,
}
```

with:

```rust
    pub diverged_on_stream: AtomicBool,
    /// What a verify asked of the applier: hold, or snapshot at a seq.
    capture: Mutex<Capture>,
    /// Wakes an applier waiting on a hold.
    capture_changed: Notify,
    /// One verify run at a time.
    pub verify_lock: tokio::sync::Mutex<()>,
    last_verify: Mutex<Option<VerifyReport>>,
    /// `ts_ms` of the last applied entry; 0 = unknown (right after a bootstrap).
    pub last_applied_ts: AtomicU64,
    /// Wakes the tail loop when a verify asks for a re-bootstrap.
    pub rebootstrap: Notify,
}
```

In `Inner::new`, compute the last entry's timestamp:

```rust
        let last_seq = store.position().last_seq;
        Ok(Inner {
```

with:

```rust
        let last_seq = store.position().last_seq;
        // The entry at last_seq, if still in the log, gives the lag's time base after a restart.
        let last_applied_ts = match last_seq {
            0 => 0,
            s => store
                .read_entries(s, 1, usize::MAX)
                .ok()
                .and_then(|v| v.first().map(|e| e.ts_ms))
                .unwrap_or(0),
        };
        Ok(Inner {
```

and initialise the new fields:

```rust
            diverged_on_stream: AtomicBool::new(false),
        })
```

with:

```rust
            diverged_on_stream: AtomicBool::new(false),
            capture: Mutex::new(Capture::Idle),
            capture_changed: Notify::new(),
            verify_lock: tokio::sync::Mutex::new(()),
            last_verify: Mutex::new(None),
            last_applied_ts: AtomicU64::new(last_applied_ts),
            rebootstrap: Notify::new(),
        })
```

In `Inner::status`, report the last verify:

```rust
            listener_failed: self.listener_failed.load(Ordering::SeqCst),
        }
```

with:

```rust
            listener_failed: self.listener_failed.load(Ordering::SeqCst),
            last_verify: self.last_verify(),
        }
```

Insert above the test module of `src/slave/inner.rs`:

```rust
#[cfg(test)]
mod tests {
    use super::ForcedBootstrap;
```

insert:

```rust
/// Why a verify waiting for seq `S` got no snapshot from the applier.
pub(crate) enum CaptureMiss {
    /// The applier committed this later seq without ever being at `S` while asked.
    Passed(Seq),
    /// Cancelled (a bootstrap began), or the snapshot could not be taken.
    Cancelled,
}

pub(crate) type CaptureReply = std::result::Result<ReadSnapshot, CaptureMiss>;

/// What a verify asked of the applier (DESIGN.md §12).
#[derive(Default)]
pub(crate) enum Capture {
    #[default]
    Idle,
    /// Commit nothing more until the upstream's snapshot seq is known.
    Hold,
    /// Take a snapshot right after committing `seq`.
    At {
        seq: Seq,
        tx: oneshot::Sender<CaptureReply>,
    },
    /// The state a held verify was waiting for is gone (a bootstrap began): its target is
    /// answered `Cancelled`, and the applier is not held.
    Cancelled,
}

/// A verify's hold on the applier. While it exists the applier commits nothing past the
/// target set with [`CaptureHold::target`]; dropping it releases the applier.
pub(crate) struct CaptureHold {
    inner: Arc<Inner>,
}

impl CaptureHold {
    /// Lets the applier run up to `seq`, snapshot right after committing it, and continue.
    ///
    /// Answered at once when the slave is already at or past `seq`: a commit that was under
    /// way when the hold was taken may have landed since. The applier is held, so a snapshot
    /// taken now is exactly the state at `seq`.
    pub(crate) fn target(&self, seq: Seq) -> oneshot::Receiver<CaptureReply> {
        let (tx, rx) = oneshot::channel();
        let mut slot = self.inner.capture();
        if matches!(*slot, Capture::Cancelled) {
            let _ = tx.send(Err(CaptureMiss::Cancelled));
            return rx;
        }
        let applied = self.inner.store.position().last_seq;
        if applied >= seq {
            let reply = if applied == seq {
                self.inner.store.read_snapshot(0).map_err(|e| {
                    tracing::warn!(node = %self.inner.cfg.identifier, seq, error = %e, "verify snapshot failed");
                    CaptureMiss::Cancelled
                })
            } else {
                Err(CaptureMiss::Passed(applied))
            };
            let _ = tx.send(reply);
            return rx;
        }
        *slot = Capture::At { seq, tx };
        drop(slot);
        self.inner.capture_changed.notify_waiters();
        rx
    }
}

impl Drop for CaptureHold {
    fn drop(&mut self) {
        *self.inner.capture() = Capture::Idle;
        self.inner.capture_changed.notify_waiters();
    }
}

impl Inner {
    fn capture(&self) -> std::sync::MutexGuard<'_, Capture> {
        self.capture.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Pauses the applier before its next commit, until the hold sets a target or is dropped.
    /// A verify takes it before asking the upstream for its snapshot, so this slave cannot pass
    /// the snapshot's seq meanwhile.
    pub(crate) fn hold_applier(self: &Arc<Self>) -> CaptureHold {
        *self.capture() = Capture::Hold;
        CaptureHold {
            inner: self.clone(),
        }
    }

    /// Called by the applier before each commit: waits while a verify holds it.
    pub(crate) async fn wait_while_held(&self) {
        loop {
            let notified = self.capture_changed.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if !matches!(*self.capture(), Capture::Hold) {
                return;
            }
            notified.await;
        }
    }

    /// Ends any capture (a bootstrap replaces the state it was waiting for): a target already
    /// set is answered `Cancelled`, and so is one a held verify sets later.
    pub(crate) fn cancel_capture(&self) {
        let mut slot = self.capture();
        match std::mem::take(&mut *slot) {
            Capture::At { tx, .. } => {
                let _ = tx.send(Err(CaptureMiss::Cancelled));
                *slot = Capture::Cancelled;
            }
            Capture::Hold | Capture::Cancelled => *slot = Capture::Cancelled,
            Capture::Idle => {}
        }
        drop(slot);
        self.capture_changed.notify_waiters();
    }

    /// Called by the applier on its blocking thread right after committing `seq`, before the
    /// next entry can commit: the snapshot it takes is exactly the state at `seq`.
    pub(crate) fn capture_after_commit(&self, seq: Seq) {
        let tx = {
            let mut slot = self.capture();
            match &*slot {
                Capture::At { seq: want, .. } if *want <= seq => match std::mem::take(&mut *slot) {
                    Capture::At { seq: want, tx } => Some((want, tx)),
                    _ => None,
                },
                _ => None,
            }
        };
        let Some((want, tx)) = tx else { return };
        let reply = if want == seq {
            self.store.read_snapshot(0).map_err(|e| {
                tracing::warn!(node = %self.cfg.identifier, seq, error = %e, "verify snapshot failed");
                CaptureMiss::Cancelled
            })
        } else {
            Err(CaptureMiss::Passed(seq))
        };
        let _ = tx.send(reply);
    }

    pub(crate) fn set_last_verify(&self, report: VerifyReport) {
        *self
            .last_verify
            .lock()
            .unwrap_or_else(PoisonError::into_inner) = Some(report);
    }

    pub(crate) fn last_verify(&self) -> Option<VerifyReport> {
        self.last_verify
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone()
    }
}
```

- [ ] **Step 5: Hold and capture in the applier; cancel on bootstrap; wake the tail loop**

In `src/slave/apply.rs`, before the blocking commit, wait while a verify holds the applier:

```rust
    let i = inner.clone();
    let e = entry.clone();
    let result = tokio::task::spawn_blocking(move || {
```

with:

```rust
    // A verify holds the applier while the upstream pins its snapshot (DESIGN.md §12).
    inner.wait_while_held().await;
    let i = inner.clone();
    let e = entry.clone();
    let result = tokio::task::spawn_blocking(move || {
```

and in the blocking closure, replace:

```rust
        i.store
            .commit(prepared, i.cfg.sync_writes)
            .map_err(Failure::Commit)
    })
```

with:

```rust
        i.store
            .commit(prepared, i.cfg.sync_writes)
            .map_err(Failure::Commit)?;
        // Still on the applier's thread, before the next entry can commit: a verify waiting
        // for this seq gets a snapshot of exactly this state (DESIGN.md §12).
        i.capture_after_commit(e.seq);
        Ok::<(), Failure>(())
    })
```

In `src/slave/bootstrap.rs`, at the start of `install`:

```rust
    inner.force_bootstrap.clear(generation);
    inner.set_ident(None);
```

with:

```rust
    inner.force_bootstrap.clear(generation);
    // A verify waiting for a seq of the old state can never be served now.
    inner.cancel_capture();
    inner.set_ident(None);
```

In `src/slave/driver.rs`, in `tail`, replace the receive at the top of the loop:

```rust
        let next = match unless_repointed(
            inner,
            generation,
            tokio::time::timeout(idle, stream.message()),
        )
        .await
        {
            Ok(next) => next,
            Err(end) => return end,
        };
```

with:

```rust
        let received = tokio::select! {
            r = unless_repointed(inner, generation, tokio::time::timeout(idle, stream.message())) => r,
            // A verify asked for a re-bootstrap (`MismatchPolicy::Rebootstrap`): handshake again,
            // which bootstraps because the request is set. A stale wake-up is ignored.
            _ = inner.rebootstrap.notified() => {
                if inner.force_bootstrap.is_requested_for(generation) {
                    return SessionEnd::Again;
                }
                continue;
            }
        };
        let next = match received {
            Ok(next) => next,
            Err(end) => return end,
        };
```

In `src/slave/client.rs`, add an eager connect (the verifier connects before it holds the applier) and split the endpoint setup out of `connect_upstream`:

```rust
/// A client for `up`, identifying itself as `identifier`.
pub(crate) fn connect_upstream(
    up: &UpstreamConfig,
    identifier: &str,
    connect_timeout: Duration,
) -> Result<Client> {
    let mut endpoint = Endpoint::from_shared(up.uri())
```

with:

```rust
/// A client for the current upstream whose connection is already established (bounded by
/// `connect_timeout`). A verify connects this way before it holds the applier.
pub(crate) async fn connect_now(inner: &Inner) -> Result<Client> {
    let (endpoint, meta) = endpoint_for(
        &inner.upstream(),
        &inner.cfg.identifier,
        inner.cfg.connect_timeout,
    )?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
    Ok(ReplicationClient::with_interceptor(channel, meta))
}

/// A client for `up`, identifying itself as `identifier`.
pub(crate) fn connect_upstream(
    up: &UpstreamConfig,
    identifier: &str,
    connect_timeout: Duration,
) -> Result<Client> {
    let (endpoint, meta) = endpoint_for(up, identifier, connect_timeout)?;
    Ok(ReplicationClient::with_interceptor(
        endpoint.connect_lazy(),
        meta,
    ))
}

fn endpoint_for(
    up: &UpstreamConfig,
    identifier: &str,
    connect_timeout: Duration,
) -> Result<(Endpoint, CallMeta)> {
    let mut endpoint = Endpoint::from_shared(up.uri())
```

and at the end of the old body (now `endpoint_for`):

```rust
    let meta = CallMeta {
        node_id,
        credentials: up.credentials.clone(),
    };
    Ok(ReplicationClient::with_interceptor(
        endpoint.connect_lazy(),
        meta,
    ))
}
```

with:

```rust
    let meta = CallMeta {
        node_id,
        credentials: up.credentials.clone(),
    };
    Ok((endpoint, meta))
}
```

- [ ] **Step 6: Implement the verifier**

Create `src/slave/verifier.rs`:

```rust
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
        Some(pb::Reason::SessionNotFound) => VerifyAbort::SessionExpired,
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
        );
        let (range_tx, mut ranges) = mpsc::channel(VERIFY_WINDOW);
        let producer = {
            let (snap, phys, throttle) = (snap.clone(), phys.clone(), throttle.clone());
            let (chunk_keys, chunk_bytes) = (cfg.chunk_keys, cfg.chunk_bytes);
            tokio::task::spawn_blocking(move || {
                let emit = |r: LocalRange| range_tx.blocking_send(Ok(r)).is_ok();
                match chunk_cf(&snap, phys, mode, chunk_keys, chunk_bytes, &throttle, emit) {
                    Ok(keys) => keys,
                    Err(e) => {
                        let _ = range_tx.blocking_send(Err(e));
                        0
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
    let reply = hold.target(s);
    let mut states = inner.state_tx.subscribe();
    let timeout = inner.cfg.verify.capture_timeout;
    let outcome = tokio::select! {
        r = tokio::time::timeout(timeout, reply) => r.ok().and_then(|r| r.ok()),
        _ = states.wait_for(|st| matches!(st, SlaveState::Halted(_))) => None,
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
```

Notes for the implementer:
- The verifier **holds the applier before `VerifyBegin`**: the upstream's snapshot `S` is then never behind this slave (unless the upstream is a slave that is itself behind, DESIGN.md §6). With the target set, the applier runs up to `S`, snapshots right after committing it and continues; the run gets a snapshot whose `seq()` is exactly `S`, or learns that the slave is past `S` (`PassedSnapshot`). It gives up at once if the slave is halted (it can never reach `S`) and otherwise after `capture_timeout`. Without the hold, a slave that follows a busy master passes `S` while the `VerifyBegin` answer travels back, and verifies under writes would always abandon.
- The local side runs on blocking threads: the chunker streams ranges through a channel (capacity `VERIFY_WINDOW`), and `compare_cf` hashes them upstream sixteen at a time. Both share one `Throttle`, so `max_bytes_per_sec` bounds every local read of the run.
- `VerifyEnd` is sent on every path after `VerifyBegin` succeeded; errors from it are ignored (the upstream expires idle sessions anyway).
- `client::connect_now(inner)` connects to the **current** upstream (so a verify after `Slave::repoint` compares with the new upstream) and establishes the connection **before** the applier is held; `VerifyBegin` is then bounded by `min(10 s, capture_timeout)`. While held, the slave applies and acknowledges nothing, so the pause must stay short (a write concern on the master could otherwise time out). Every later answer on the stream is bounded by 60 s.
- `CaptureHold::target` answers at once when the slave is already at or past `S` (a commit under way when the hold was taken may have landed meanwhile); `cancel_capture` also marks a held verify `Cancelled` so its later target is refused (a bootstrap replaced the state).
- `finish` stores and reports every report, but applies `on_mismatch` only when the slave is not halted: a halted slave may be on another history than its upstream (a refused divergence), and a policy must not discard data on that basis.

- [ ] **Step 7: Wire it into `Slave`**

In `src/slave/mod.rs`, add the module:

```rust
mod source;
mod state;
```

with:

```rust
mod source;
mod state;
mod verifier;
```

import the verify types:

```rust
use crate::types::{CfSpec, Direction, IterMode, Role, Seq, ValueMeta};
```

with:

```rust
use crate::types::{CfSpec, Direction, IterMode, Role, Seq, ValueMeta};
use crate::verify::{VerifyMode, VerifyReport};
```

add the scheduled verifier's task to `struct Slave`:

```rust
    maintenance: Option<JoinHandle<()>>,
    server: Option<ServerHandle>,
}
```

with:

```rust
    maintenance: Option<JoinHandle<()>>,
    verifier: Option<JoinHandle<()>>,
    server: Option<ServerHandle>,
}
```

start it in `Slave::open`:

```rust
        let maintenance = maintenance::spawn(&inner);
        Ok(Slave {
            inner,
            driver: Some(driver),
            maintenance: Some(maintenance),
            server,
        })
```

with:

```rust
        let maintenance = maintenance::spawn(&inner);
        let verifier = verifier::spawn(&inner);
        Ok(Slave {
            inner,
            driver: Some(driver),
            maintenance: Some(maintenance),
            verifier,
            server,
        })
```

and stop it in `Slave::shutdown` and `Slave::kill`: (all 2 occurrences)

```rust
        for task in [self.driver.take(), self.maintenance.take()]
            .into_iter()
            .flatten()
```

with:

```rust
        for task in [
            self.driver.take(),
            self.maintenance.take(),
            self.verifier.take(),
        ]
        .into_iter()
        .flatten()
```

and add, before `watch_state`:

```rust
    pub fn watch_state(&self) -> watch::Receiver<SlaveState> {
```

insert:

```rust
    /// Compares this slave's data with its upstream's now (DESIGN.md §12) and returns the
    /// report. The report is also stored in [`SlaveStatus::last_verify`] and passed to the
    /// listener's `on_verify_complete`; under `MismatchPolicy::Rebootstrap` a report with
    /// mismatches makes the slave bootstrap. `cfg.verify` sets range sizes, the rate limit and
    /// the policy; `mode` overrides `cfg.verify.mode`. One run at a time: a concurrent call
    /// waits for the first.
    ///
    /// Errors: `NotReady` without data; `VerifyAborted(_)` when no report could be made
    /// (`Busy`, `SessionExpired`, `IdentityChanged`, `PassedSnapshot`, `CaptureTimeout`,
    /// `Upstream`); these leave `last_verify` unchanged.
    pub async fn verify_now(&self, mode: VerifyMode) -> Result<VerifyReport> {
        verifier::verify(&self.inner, mode).await
    }
```

- [ ] **Step 8: Run the tests**

Run: `cargo test --lib slave::verifier && cargo test --test verify`
Expected: PASS: `answers_are_sized_for_two_maximal_keys`, and 4 tests in `verify` (`a_replica_verifies_clean_in_both_modes`, `verify_pins_the_snapshot_while_concurrent_writes_continue`, `verify_needs_data_and_the_same_identity`, `scheduled_verifies_run_on_their_interval`).

Run: `cargo test && cargo test --features failpoints`
Expected: PASS.

- [ ] **Step 9: Update DESIGN.md**

In DESIGN.md §12 **Comparing at the same seq**, replace step 2:

```markdown
2. The slave's applier captures a snapshot **exactly at `S`**: it applies up to `S` and snapshots before `S + 1`. Normally the slave is at or behind `S`, since it can only have received what the upstream had. The exception is the case in §6, where it can be ahead. If it is already past `S`, the session is abandoned and retried later.
```

with:

```markdown
2. The slave's applier captures a snapshot **exactly at `S`**. The slave connects first, then **holds its applier before sending `VerifyBegin`**, so it cannot run past `S` while the answer travels back (a slave following a busy master otherwise always would). Once `S` is known the applier runs up to `S`, snapshots right after committing it, and continues; a slave already at `S` snapshots at once. The pause is bounded: `VerifyBegin` must answer within `min(10 s, capture_timeout)`, because a held slave applies and acknowledges nothing (a write concern on the master would time out). Normally the slave is at or behind `S`, since it can only have received what the upstream had; the exception is the case in §6, where it can be ahead: then the run is abandoned (`PassedSnapshot`) and retried later. A slave that cannot reach `S` gives up after `capture_timeout`, and at once when it is halted.
```

In DESIGN.md §12 **Ranges**, replace:

```markdown
- On a mismatch the slave splits the range at its median key
```

with:

```markdown
- Keys longer than 1 MiB are never used as boundaries, so range requests stay small; a range simply grows past them.
- On a mismatch the slave splits the range at its median key
```

In DESIGN.md §12 **Result**, replace:

```markdown
- `VerifyReport { seq, mode, cfs_checked, keys_checked, mismatches: Vec<Mismatch>, duration }`, where `Mismatch` is `CfSpec { .. }` or `Key { cf, key, local: Option<write_seq>, upstream: Option<write_seq> }`, or `Range { cf, lower, upper }` if listing was cut short.
```

with:

```markdown
- `VerifyReport { seq, mode, cfs_checked, keys_checked, mismatches: Vec<Mismatch>, duration }` (`is_clean()`), where `Mismatch` is `CfSpec { name, local: Option<CfVersion>, upstream: Option<CfVersion> }` (`CfVersion { spec, create_seq }`), `Key { cf, key, local: Option<write_seq>, upstream: Option<write_seq> }` (equal `write_seq`s mean the values differ), or `Range { cf, lower, upper }` for keys that were not compared one by one: the listing could not be continued (its last key is longer than 2 MiB), or the run had already reported `max_reported_keys` keys.
- A run that cannot produce a report fails with `Error::VerifyAborted(VerifyAbort)`: `Busy` and `SessionExpired` (from the upstream), `IdentityChanged`, `PassedSnapshot { snapshot_seq, applied_seq }` (the slave is already past `S`; retry later), `CaptureTimeout { snapshot_seq }` (the slave did not reach `S` within `capture_timeout`, or is halted), or `Upstream(..)`. Without data, `verify_now` returns `NotReady`.
- Every report is stored in `SlaveStatus.last_verify` and passed to `on_verify_complete`. Under `MismatchPolicy::Rebootstrap`, a report with mismatches ends the current Tail stream and the slave bootstraps at once (a halted slave stays halted).
```

In DESIGN.md §16, in `SlaveConfig`, replace:

```
    pub verify: VerifyConfig,                // { interval: Option<Duration>, mode, chunk_keys, chunk_bytes,
                                             //   min_chunk_keys, max_bytes_per_sec, on_mismatch }
```

with:

```
    pub verify: VerifyConfig,                // { interval: None, mode: Quick, chunk_keys: 1 000, chunk_bytes: 4 MiB,
                                             //   min_chunk_keys: 16, max_bytes_per_sec: None, on_mismatch: Report,
                                             //   capture_timeout: 60 s, max_reported_keys: 10 000 }
```

In DESIGN.md §16.2, replace:

```markdown
`PeerUnreachable { peer, reason, source }` (`admin::node_status*` got no answer; `reason` is one line), `Io`, `RocksDb`.
```

with:

```markdown
`PeerUnreachable { peer, reason, source }` (`admin::node_status*` got no answer; `reason` is one line), `VerifyAborted(VerifyAbort)` (`verify_now` could not produce a report), `Io`, `RocksDb`.
```

In the DESIGN.md §18 table, replace:

```markdown
| Verify | off by default; `Quick` when enabled |
```

with:

```markdown
| Verify | off by default; `Quick` when enabled; ranges of 1 000 keys or 4 MiB, listed at ≤ 16 keys; capture timeout 60 s; at most 10 000 key mismatches per report |
```

- [ ] **Step 10: Commit**

```bash
cargo fmt
git add src tests DESIGN.md
git commit -m "feat: slave verifier with capture at S, verify_now, reports, policy and scheduling

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

