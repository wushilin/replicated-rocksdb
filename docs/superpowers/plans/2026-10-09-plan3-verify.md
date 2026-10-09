# Plan 3 of 3 — Integrity Verification, Repair, Observability and the Full Test Sweep Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement Verify end to end — server sessions pinned to a snapshot with `HashRange`/`ListRange`, and a slave verifier that captures its own snapshot at exactly the upstream's seq, compares contiguous ranges, bisects and lists mismatches down to the key, and reports, repairs or re-bootstraps — plus divergence repair (addendum §5), `lag_ms`, tracing spans, and a sweep that gives every DESIGN.md §21 test item a real test. Task 0 first fixes four findings carried over from Plan 2's final review.

**Architecture:** A new crate-private `src/verify/` module holds everything both sides share: key order, half-open bounds, per-mode payloads, range hashes, listings, the chunker, the throttle (`range.rs`), range reads through a `ReadSnapshot` (`scan.rs`), and the comparison algorithm (`compare.rs`) written against two small traits (`Oracle` = the upstream, `LocalSide` = local records) so bisection, paging and the key-by-key diff are unit-tested without a network. The server (`src/server/verify.rs`) keeps sessions that own a `ReadSnapshot`; the slave (`src/slave/verifier.rs`) implements `Oracle` over the `VerifyRange` bidi stream and gets its snapshot from the applier right after it commits seq `S`; it pauses the applier before asking for the session, so it cannot run past `S` meanwhile. Repair reuses the session: three more request kinds (`EntryCrcs` to find a fork point, `FetchKeys`/`FetchRange` to read envelopes at `S`) let a diverged slave — or one a verify found damaged — rewind to a common seq in one synced batch with only the affected keys taken from the upstream, then replay (`src/slave/repair.rs`, `src/store/repair.rs`). Observability adds `lag_ms` from the last applied entry's `ts_ms` and `tracing` spans on both ends of every handshake, bootstrap and verify session and on each reaper round. Every integrity test is real: nodes in one process over gRPC on 127.0.0.1, corruption injected by opening a stopped slave's directory with plain RocksDB.

**Tech Stack:** Plan 1 and Plan 2's stack (Rust 1.99 toolchain, edition 2024, MSRV 1.85, rocksdb 0.25.0 with `multi-threaded-cf`, prost 0.14.4, tokio 1.53, tonic 0.14.6, tonic-types, tokio-stream 0.1.19, blake3 1.8, tracing 0.1.44, …). No new dependencies. Tests use `rocksdb` (a normal dependency) to tamper with stopped node directories, and implement a tiny `tracing::Subscriber` to capture spans.

**Spec:** [DESIGN.md](../../../DESIGN.md) §12, §15, §16, §18, §19, §21; [PROTOCOL.md](../../../PROTOCOL.md) §5.4, §9, §10, §11.2, §12, §13, §14, §15; the "Plan 3: Verify" section of [the Plan 2/3 design notes](../specs/2026-10-09-plan2-3-design-notes.md); §5 (and the §3–§5 tests) of [the acks/failover/repair addendum](../specs/2026-10-09-failover-acks-repair.md), which wins over DESIGN/PROTOCOL until merged (Task 8 merges §5). Read DESIGN.md §12 and PROTOCOL.md §9 before starting. This plan builds on [Plan 1](2026-10-09-plan1-storage-and-master.md) and [Plan 2](2026-10-09-plan2-replication.md) (with its addendum §1–§4 amendment: acks over a bidirectional Tail, `ReplicaStatus` / `replicas()`, write concern, `repoint`, `Status`, `promote_checked`, `demote`), **both fully implemented: the code at commit `fae88e4` (branch `feat/storage-and-master`) is the source of truth.** Every snippet below that is quoted for replacement is quoted from that tree with this plan's earlier tasks applied, and was applied and tested in that order: each task compiles, passes its tests and leaves `cargo fmt --check` and `cargo clippy --all-targets -- -D warnings` (with and without `--features failpoints`) clean. If an edit does not apply verbatim, the tree has moved on since `fae88e4`: apply the same change to the equivalent lines.

**Series:**
- Plan 1: storage engine and master (done).
- Plan 2: replication server, auth/TLS, `Slave`, chaining, promotion, failure/resume (done before this plan).
- **Plan 3 (this):** carried review fixes, Verify, repair (divergence and verify mismatches; the `repoint` `Repair` arm), `lag_ms`, `last_verify`, tracing spans, DESIGN.md §21 sweep. (`replicas()` / `ReplicaStatus` and the addendum §3–§4 failover tests — `promote_checked`, `demote`, the chooser, `repoint` refusal — already exist from Plan 2: `tests/promotion.rs`, `tests/repoint.rs`, `src/admin.rs`.)

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
- **RocksDB is never read on an async worker** in new or touched server code: reads go through `tokio::task::spawn_blocking` or `background::blocking` (Task 6 moves the remaining Plan 2 cases).
- A verify session (server) or verify run (slave) must never keep a node's database open after `shutdown()`: sessions are cleared on shutdown and on reset, and background tasks hold `Weak<Inner>` between runs (Plan 2's rule).
- Tests that matter are **real**: nodes in one process, gRPC over `127.0.0.1` port 0, corruption injected by opening a **stopped** node's directory with plain `rocksdb::DB`. Failpoint tests live only in `tests/failpoints.rs` and need `--features failpoints`.
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Capturing exactly `S`.** The verifier pauses the applier (`Inner::hold_applier`) *before* `VerifyBegin`, then lets it run to `S` and snapshot right after that commit. Without the hold, a slave following a busy master is past `S` by the time the answer arrives and every run aborts. A slave genuinely ahead of its upstream's snapshot (C ahead of B after B lost unflushed entries, DESIGN.md §6) must still abandon the run with `VerifyAbort::PassedSnapshot`, never compare different seqs and report phantom mismatches. (Tests `verify_pins_the_snapshot_while_concurrent_writes_continue` in Task 3, `a_slave_ahead_of_the_upstream_snapshot_abandons_the_run` in Task 4.)
2. **A halted slave behind `S`** can never reach the snapshot seq: `verify_now` must return `CaptureTimeout` as soon as the slave is halted, not hang for `capture_timeout` (60 s). (Test `a_halted_slave_behind_the_snapshot_gives_up_at_once` in Task 4.)
3. **Very long keys** (the protocol allows keys up to `max_entry_bytes`, but the server accepts 4 MiB requests): keys longer than 1 MiB never become range bounds, a listing whose last key is over 2 MiB is reported as `Mismatch::Range` instead of sending an oversized request, and the server always returns at least two keys per `ListResult` so paging never stalls. (Tests `oversized_keys_never_become_boundaries` and `split_points_avoid_long_keys_and_small_ranges`, `a_listing_that_cannot_progress_is_reported_as_a_range` in Task 1; the 1 MiB edge-case key in `edge_case_data_verifies_and_its_corruption_is_found` in Task 4.)
4. **Open verify sessions must not pin the database**: a client that opens a session and walks away must not make `Master::shutdown` time out, and an epoch bump or re-bootstrap must end verify streams with `UPSTREAM_RESET` and drop their snapshots. (Tests `open_sessions_do_not_keep_the_database_open`, `an_epoch_bump_ends_verify_streams_and_sessions` in Task 2.)
5. **A crash between a repair's rewind and replay reaching `S`** leaves keys at `S` next to keys at `F − 1`: that mixed state must never be read or served, before or after a restart, and the slave must converge without a bootstrap. Repair must also refuse (fall back to a bootstrap) whenever entry `F − 1` is not held by both sides with the same CRC, since nothing else proves the histories agree up to it. (Tests `crash_mid_repair_then_converge`, `a_trimmed_local_suffix_falls_back_to_a_bootstrap`, `an_upstream_without_the_fork_point_falls_back_to_a_bootstrap` in Task 8.)

## Design deltas this plan introduces

Each owning task updates DESIGN.md or PROTOCOL.md with the exact text given there.

- **The verifier holds the applier before `VerifyBegin`** (Task 3, DESIGN.md §12 step 2): the design notes had the applier simply "capture at `S`", which a slave keeping up with a busy master never manages (it is already past `S` when the answer arrives). The applier waits while held, then runs exactly to `S`.
- **`ListRange` paging and size** (Task 2, PROTOCOL.md §9.2, §9.4, §12): `limit` must be 1..=1024; the server may stop early to bound the answer (~1 MiB) but returns at least two keys when the range has two; a client continues a truncated listing with `lower` = the last key received and skips that key when it is repeated. This replaces the design notes' "`lower = last key + \0`", which is only a successor under bytewise order (under a reverse comparator it would re-list or skip keys).
- **Long keys are never bounds** (Task 2, PROTOCOL.md §9.4; DESIGN.md §12): keys over 1 MiB are not used as range boundaries; listing resumes only from keys of at most 2 MiB.
- **Result and failure types** (Task 3, DESIGN.md §12, §16, §16.2): `Mismatch::CfSpec { name, local: Option<CfVersion>, upstream: Option<CfVersion> }` (`CfVersion { spec, create_seq }`, so a `create_seq` difference is visible; the notes had `Option<CfSpec>`); `Mismatch::Range` is also used once `max_reported_keys` is reached; `Error::VerifyAborted(VerifyAbort)` with `Busy`, `SessionExpired`, `IdentityChanged`, `PassedSnapshot`, `CaptureTimeout`, `Upstream`; `VerifyConfig` gains `capture_timeout` and `max_reported_keys`; `VerifyReport::is_clean()`.
- **`replicas()` / `ReplicaStatus` move to Plan 2's acks amendment** (addendum §1 defines them with real acks); the design notes' Plan 3 version (`acked_seq = last sent`) is dropped.
- **Repair** (Tasks 7–8, PROTOCOL.md §6.5, §9.2, §11.2, §13, §14; DESIGN.md §3.2, §12.1 new, §13, §13.1, §15, §16, §16.3, §21, §22): `FetchKeys`, `FetchRange`, `EntryCrcs` request kinds on `VerifyRange`; `DivergencePolicy::Repair`, `MismatchPolicy::Repair`, `SlaveConfig.repair_max_bytes` (64 MiB), `Slave::diverged_entries` (fork point persisted as `__meta.diverged_at` when the slave halts `Diverged`), listener `on_rewind`, `node_state = repairing:<seq>` (also in `Status`). The fork point is found by binary search over entry CRCs and needs entry `F − 1` on both sides; verify repair rewinds to `min(L, S)` so it can reuse the same replay argument. `repoint(.., RepointDivergence::Repair { accept_loss })` now returns `Repaired { discarded }` (Plan 2 returned `Error::Unsupported`) and `repoint::mode_error` goes away. After a repair the driver handshakes again rather than tailing on the `DIVERGED` answer.
- **`lag_ms`** (Task 5, DESIGN.md §16): `Some(0)` when `applied_seq >= origin_last_seq`; while behind, `now − ts_ms` of the last applied entry; `None` while that `ts_ms` is unknown (after a bootstrap, until the next entry).
- **Span names and fields** (Task 6, DESIGN.md §19): `handshake`, `bootstrap`, `verify`, `reaper`, each with `node_id`, `upstream` (absent on the master), `seq`, `epoch`; server-side spans add `client`, `verify` adds `mode`.
- **Plan 2 review items** (Task 0, Task 6): the Tail junction race, an orphan CF of another name, BUSY peers in `check_promotion` (test only), the `RepointDivergence` rustdoc; RocksDB reads off async workers and `ReplicaRegistry::status` outside its locks. No spec change.
- **`MismatchPolicy::Rebootstrap`** ends the current Tail stream (a `Notify` the tail loop selects on) so the bootstrap starts at once, not at the next disconnect (Task 3). A halted slave stays halted.

## File Structure

```
src/server/tail.rs            junction race (Task 0); validation and junction reads on blocking threads (Task 6)
src/store/apply.rs            orphan CF of another name is dropped before its intent is replaced (Task 0)
src/admin.rs                  + test: a BUSY Status answer is not a transport failure (Task 0)
src/slave/repoint.rs          RepointDivergence docs (Task 0); Repair arm, mode_error removed (Task 8)
src/verify/mod.rs             public types (VerifyMode, VerifyConfig, MismatchPolicy, CfVersion, Mismatch,
                              VerifyReport, VerifyAbort), protocol constants (Task 1); MismatchPolicy::Repair (Task 8)
src/verify/range.rs           KeyOrder, Bounds, payload, RangeHasher, Lister, LocalRange, LocalRecord,
                              Chunker, Throttle (Task 1)
src/verify/scan.rs            range reads through a ReadSnapshot: scan, hash_range, list_range, load_range,
                              chunk_cf, pace (Task 1); fetch_range (Task 7)
src/verify/compare.rs         Oracle / LocalSide traits, compare_cf (pipelined hashes, bisection, paged
                              listing, key diff), compare_specs, Budget, CfRun (Task 1)
src/store/repl.rs             + ReadCtx::iter_raw_from (Task 1)
src/store/repair.rs           Rewind, Store::{rewind, set_diverged_at, diverged_at} (Task 8)
src/store/{mod,meta,bootstrap}.rs  repair module, __meta.diverged_at, cleared by a bootstrap reset (Task 8)
src/error.rs                  + Error::VerifyAborted (Task 1)
src/lib.rs                    module + re-exports (Task 1)
src/server/verify.rs          Sessions, VerifyBegin / VerifyRange / VerifyEnd handlers, server tests (Task 2);
                              FetchKeys / FetchRange / EntryCrcs (Task 7)
src/server/mod.rs             ServerState field, Svc delegates, sweep, shutdown clears sessions (Task 2);
                              handshake and Status spans and blocking reads (Task 6)
src/server/tests.rs           - verify_is_not_implemented_yet (Task 2)
src/server/{bootstrap,replicas}.rs  bootstrap span; ReplicaRegistry::status computes outside its locks (Task 6)
src/slave/verifier.rs         verify run, applier hold and capture, Upstream oracle, scheduling (Task 3);
                              UpstreamSession and fetches (Task 7); MismatchPolicy::Repair (Task 8)
src/slave/repair.rs           AffectedSet, fork search, divergence and verify repair, record_fork (Task 8)
src/slave/inner.rs            + capture slot and applier hold, verify lock, last_verify, last_applied_ts,
                              rebootstrap notify (Task 3); status() with lag_ms (Task 5); repair state,
                              is_ready false while repairing (Task 8)
src/slave/apply.rs            + wait while held, capture right after commit (Task 3), last_applied_ts (Task 5),
                              end of repair (Task 8)
src/slave/bootstrap.rs        + cancel capture (Task 3), reset ts (Task 5), clear repair state (Task 8)
src/slave/driver.rs           + tail loop wakes for verify actions (Task 3); spans (Task 6); repair actions (Task 8)
src/slave/{policy,source}.rs  + Action::Repair; node_state "repairing:<seq>" (Task 8)
src/slave/config.rs           + SlaveConfig.verify (Task 3); DivergencePolicy::Repair, repair_max_bytes (Task 8)
src/slave/state.rs            + SlaveStatus.last_verify (Task 3), lag_ms (Task 5); NodeState::Repairing (Task 8)
src/slave/mod.rs              + verify_now, verifier task (Task 3); diverged_entries (Task 8)
src/listener.rs               + on_verify_complete (Task 3), on_rewind (Task 8)
src/master/reaper.rs          + reaper span (Task 6)
proto/replication.proto       + FetchKeys, FetchRange, EntryCrcs and their results (Task 7); node_state comment (Task 8)
tests/support/mod.rs          + RecordingListener::{verifies, rewinds}, RawDb (Tasks 3, 4, 8), RawDb::last_seq (Task 9)
tests/verify.rs               verify end to end (Task 3)
tests/integrity.rs            on-disk corruption, chains, policies, limits (Task 4)
tests/observability.rs        lag_ms (Task 5)
tests/spans.rs                tracing spans, with a recording subscriber (Task 6)
tests/repair.rs               divergence and verify repair, fallbacks, diverged_entries (Task 8)
tests/repoint.rs              Repair arm now repairs (Task 8)
tests/failure_resume.rs       + quiet master with an empty log (Task 9)
tests/replication.rs          + CF drop while a slave snapshot is open (Task 9)
tests/failpoints.rs           + crash mid-repair (Task 8), random kills while writing (Task 9)
tests/promotion.rs            + a throttled peer blocks a checked promotion (Task 0); Full verify after promotion (Task 9)
DESIGN.md, PROTOCOL.md        amendments (Tasks 2, 3, 5, 6, 7, 8)
```

## Tasks

| # | Task | Main output |
|---|---|---|
| 0 | Carried fixes from Plan 2's final review | tail junction, orphan CF, BUSY peers, repoint docs |
| 1 | Verify core | `src/verify/` |
| 2 | Server Verify RPCs | `src/server/verify.rs` |
| 3 | Slave verifier | `src/slave/verifier.rs`, `verify_now` |
| 4 | Integrity tests with on-disk damage | `tests/integrity.rs` |
| 5 | `lag_ms` | `SlaveStatus.lag_ms` |
| 6 | Spans and blocking reads | `tests/spans.rs` |
| 7 | Repair fetches on the verify session | `FetchKeys`, `FetchRange`, `EntryCrcs` |
| 8 | Divergence repair and `MismatchPolicy::Repair` | `src/slave/repair.rs`, `tests/repair.rs` |
| 9 | DESIGN.md §21 sweep | coverage map and four new tests |

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

### Task 1: Verify core — types, range arithmetic, snapshot scans and the comparison algorithm

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
//! See Task 1.
```

(`src/verify/scan.rs`)

```rust
//! See Task 1.
```

(`src/verify/compare.rs`)

```rust
//! See Task 1.
```

Run: `cargo build`
Expected: builds (dead-code warnings for the new items are expected until Tasks 2–3 use them).

- [ ] **Step 2: Write the failing tests for range arithmetic and snapshot scans**

Replace the content of `src/verify/range.rs` with its test module:

```rust
//! See Task 1.
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
//! See Task 1.
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
//! See Task 1.
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

### Task 2: Server Verify RPCs — sessions, `HashRange`, `ListRange`, limits

**Files:**
- Create: `src/server/verify.rs`
- Modify: `src/server/mod.rs`, `src/server/tests.rs` (remove `verify_is_not_implemented_yet`), `PROTOCOL.md` (§9.2, §9.4, §12)

**Interfaces:**
- Consumes: Task 1 `verify::{VerifyMode::from_pb, LIST_SOFT_BYTES, MAX_LIST_LIMIT}`, `verify::range::{Bounds::from_pb, KeyOrder::new}`, `verify::scan::{hash_range, list_range}`; Plan 2 `ServerState { source: Weak<dyn LogSource>, cfg: ServeConfig, shutdown: watch::Receiver<bool>, replicas, … }`, `ServerState::authorize(&self, &CallInfo, Method) -> Result<Arc<dyn LogSource>, Status>`, `call_info(&Request<T>, Method) -> Result<CallInfo, Status>`, `CallInfo::check_client_id`, `store_status(Error) -> Status`, `stopped`, `end_stream(&mpsc::Sender<Result<T, Status>>, Status, &mut watch::Receiver<bool>, &mut watch::Receiver<u64>)`, `VerifyRangeStream`, `ServerHandle::{shutdown, abort}`, `sweep_leases`, `LogSource::{is_ready, resets, store, node_id, upstream_chain}`, `ReadSnapshot::{meta, phys}`, `net::{reason_status, reason_of, invalid_argument, internal_status, convert::{cf_to_pb, identity_to_pb}}`, `ServeLimits::{max_verify_sessions, verify_session_timeout}`, test helpers `server::test_client::{client, serving_master, Client}`.
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
Expected: the tests compile (they use only Task 1 and Plan 2 items) and FAIL at runtime, because the Verify RPCs still answer `UNIMPLEMENTED`: e.g. ``called `Result::unwrap()` on an `Err` value: Status { code: Unimplemented, message: "Verify is not implemented by this version", … }``, and `malformed_requests_are_invalid_arguments` fails with `left: Unimplemented, right: InvalidArgument`.

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

### Task 3: Slave verifier — capture at `S`, `verify_now`, reports, policy and scheduling

**Files:**
- Create: `src/slave/verifier.rs`, `tests/verify.rs`
- Modify: `src/slave/inner.rs`, `src/slave/apply.rs`, `src/slave/bootstrap.rs`, `src/slave/driver.rs`, `src/slave/config.rs`, `src/slave/state.rs`, `src/slave/mod.rs`, `src/listener.rs`, `tests/support/mod.rs`, `DESIGN.md` (§12, §16, §16.2, §18)

**Interfaces:**
- Consumes: Task 1 `verify::{VerifyMode, VerifyConfig, VerifyReport, VerifyAbort, MismatchPolicy, VERIFY_WINDOW, LIST_SOFT_BYTES}`, `verify::compare::{compare_cf, compare_specs, Budget, CfRun, Oracle, LocalSide}`, `verify::range::{Bounds, KeyOrder, LocalRange, LocalRecord, Throttle}`, `verify::scan::{chunk_cf, load_range}`; Task 2's server; Plan 2 `slave::inner::Inner { store, cfg, state_tx, shutdown_tx, max_entry_bytes, force_bootstrap: ForcedBootstrap, repoint_tx, … }` and `Inner::{new, ident, state, notify, status, upstream_uri}`, `ForcedBootstrap::{request, is_requested_for}`, `slave::client::{connect(&Inner) -> Result<Client>, Client}` (connects to the current upstream, so verify follows `repoint`), `background::blocking`, `net::{reason_of, reason_status, DECODE_SLACK, convert::{cf_from_pb, identity_from_pb}}`, the apply closure in `slave::apply::apply`, `slave::bootstrap::install`, the receive loop of `slave::driver::tail` and `unless_repointed`, `Slave { inner, driver, maintenance, server }`, `Slave::{open, shutdown, kill}`, harness `Cluster::{new, master, master_config, slave, slave_config}`, `assert_converged`, `wait_state`, `wait_until`, `RecordingListener`.
- Produces:
  - `SlaveConfig.verify: VerifyConfig` (validated in `SlaveConfig::validate`); `SlaveStatus.last_verify: Option<VerifyReport>`; `ReplicationListener::on_verify_complete(&self, &VerifyReport)` (default no-op).
  - `Slave::verify_now(&self, mode: VerifyMode) -> Result<VerifyReport>` (async).
  - `slave::inner::{Capture { Idle, Hold, At { seq, tx } }, CaptureHold (target(&self, Seq) -> oneshot::Receiver<CaptureReply>; dropping it releases the applier), CaptureMiss { Passed(Seq), Cancelled }, CaptureReply}`, `Inner::{hold_applier(self: &Arc<Self>) -> CaptureHold, wait_while_held(&self) (async), cancel_capture(&self), capture_after_commit(&self, Seq), set_last_verify(&self, VerifyReport), last_verify(&self) -> Option<VerifyReport>}`, fields `Inner.{verify_lock: tokio::sync::Mutex<()>, last_applied_ts: AtomicU64, rebootstrap: Notify}`.
  - `slave::verifier::{verify(&Arc<Inner>, VerifyMode) -> Result<VerifyReport>, spawn(&Arc<Inner>) -> Option<JoinHandle<()>>}`; `Slave.verifier: Option<JoinHandle<()>>`.
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
    CfSpec, Error, HaltReason, LogApi, Master, ReadApi, Slave, SlaveState, VerifyAbort, VerifyMode,
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
use crate::types::{DbIdentity, EpochStart, Seq};
```

with:

```rust
use crate::types::{DbIdentity, EpochStart, Seq};
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
}

/// A verify's hold on the applier. While it exists the applier commits nothing past the
/// target set with [`CaptureHold::target`]; dropping it releases the applier.
pub(crate) struct CaptureHold {
    inner: Arc<Inner>,
}

impl CaptureHold {
    /// Lets the applier run up to `seq`, snapshot right after committing it, and continue.
    pub(crate) fn target(&self, seq: Seq) -> oneshot::Receiver<CaptureReply> {
        let (tx, rx) = oneshot::channel();
        *self.inner.capture() = Capture::At { seq, tx };
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

    /// Ends any capture (a bootstrap replaces the state it was waiting for).
    pub(crate) fn cancel_capture(&self) {
        if let Capture::At { tx, .. } = std::mem::take(&mut *self.capture()) {
            let _ = tx.send(Err(CaptureMiss::Cancelled));
        }
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

- [ ] **Step 6: Implement the verifier**

Create `src/slave/verifier.rs`:

```rust
//! The slave side of Verify (DESIGN.md §12, PROTOCOL.md §9): open a session upstream,
//! capture a local snapshot at exactly the upstream's seq `S` (taken by the applier right
//! after it commits `S`), compare the CF lists, then compare each common CF range by range.

use std::cmp::Ordering as SeqOrder;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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
        if inner.cfg.verify.on_mismatch == MismatchPolicy::Rebootstrap {
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
    let mut client = client::connect(inner)?.max_decoding_message_size(limit);
    let req = pb::VerifyBeginRequest {
        client_id: inner.cfg.identifier.clone(),
        mode: mode.to_pb() as i32,
    };
    // The applier commits nothing while the upstream pins its snapshot, so this slave cannot
    // pass the snapshot's seq before the capture is set up.
    let hold = inner.hold_applier();
    let begin = client
        .verify_begin(req)
        .await
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
/// now if the slave is at `s`, otherwise by the applier right after it commits `s`.
async fn capture(inner: &Arc<Inner>, hold: CaptureHold, s: Seq) -> Result<ReadSnapshot> {
    let applied = inner.store.position().last_seq;
    match applied.cmp(&s) {
        // Only possible with an upstream that is behind this slave (DESIGN.md §6).
        SeqOrder::Greater => {
            return Err(abort(VerifyAbort::PassedSnapshot {
                snapshot_seq: s,
                applied_seq: applied,
            }));
        }
        SeqOrder::Equal => {
            let i = inner.clone();
            let snap = blocking(move || i.store.read_snapshot(0)).await?;
            // A commit that was already under way when the hold was taken may have landed.
            if snap.seq() != s {
                return Err(abort(VerifyAbort::PassedSnapshot {
                    snapshot_seq: s,
                    applied_seq: snap.seq(),
                }));
            }
            return Ok(snap);
        }
        SeqOrder::Less => {}
    }
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
        match self.answers.message().await {
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
- `client::connect(inner)` connects to the **current** upstream, so a verify after `Slave::repoint` compares with the new upstream.

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
`PeerUnreachable { peer, reason }` (`admin::node_status*` got no answer), `Io`, `RocksDb`.
```

with:

```markdown
`PeerUnreachable { peer, reason }` (`admin::node_status*` got no answer), `VerifyAborted(VerifyAbort)` (`verify_now` could not produce a report), `Io`, `RocksDb`.
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

### Task 4: Integrity scans with real on-disk damage, chains, policies and limits

**Files:**
- Create: `tests/integrity.rs`
- Modify: `tests/support/mod.rs` (add `RawDb`, `KeyCmp`)

**Interfaces:**
- Consumes: Task 3 `Slave::verify_now`, `SlaveConfig.verify`, `SlaveStatus.last_verify`, `RecordingListener::verifies`; Task 1 public `Mismatch`, `MismatchPolicy`, `VerifyAbort`, `VerifyMode`; Plan 1 `replicated_rocksdb::codec::envelope::encode` (public, doc-hidden), `ComparatorRegistry::register`; Plan 2 harness `Cluster::{new, master, master_config, slave_config, path}`, `assert_converged`, `wait_state`, `wait_until`, `write_edge_cases`, `edge_cases`, `copy_dir`, `restore_dir`, `RecordingListener::{bootstraps, assert_no_violations}`, `Slave::{shutdown, serve_addr, last_seq}`, `ServeLimits::{verify_session_timeout, max_verify_sessions}`; the `rocksdb` crate (a normal dependency, so integration tests can use it).
- Produces: harness `RawDb { open(&Path), open_with(&Path, &[(&str /* logical CF */, &str /* comparator name */, KeyCmp)]), get(cf, key) -> Option<Vec<u8>>, put(cf, key, envelope), delete(cf, key), corrupt_value(cf, key) }`, `type KeyCmp = fn(&[u8], &[u8]) -> std::cmp::Ordering`.

This task adds no production code: it proves Tasks 1–3 against real damage. If a test fails, fix the code it points at (`src/verify/compare.rs` for wrong or missing mismatches, `src/slave/verifier.rs` for aborts and policy, `src/server/verify.rs` for limits).

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
    CfSpec, Error, HaltReason, LogApi, Master, Mismatch, MismatchPolicy, ReadApi, Slave,
    SlaveConfig, SlaveState, VerifyAbort, VerifyMode,
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

### Task 5: Replication lag (`SlaveStatus.lag_ms`)

**Files:**
- Create: `tests/observability.rs`
- Modify: `src/slave/state.rs`, `src/slave/inner.rs`, `src/slave/apply.rs`, `src/slave/bootstrap.rs`, `DESIGN.md` (§16)

`Master::replicas()` / `Slave::replicas()` (`ReplicaStatus` with acks, `lag_entries`, `lag_ms`, `in_sync`) already exist from Plan 2's acks work; this task adds the slave's own lag behind the origin.

**Interfaces:**
- Consumes: Task 3 `Inner.last_applied_ts`, `Inner::last_verify`; Plan 2 `Inner::{status, now_ms}`, `slave::apply::apply`, `slave::bootstrap::install`, harness `Cluster`, `assert_converged`, `wait_state`, `Slave::clear_halt`.
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

use replicated_rocksdb::{CfSpec, HaltAction, HaltReason, LogApi, Master, Slave, SlaveState};
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

### Task 6: Tracing spans, and RocksDB reads off the async workers

**Files:**
- Create: `tests/spans.rs`
- Modify: `src/server/mod.rs` (Handshake, Status), `src/server/tail.rs`, `src/server/bootstrap.rs`, `src/server/replicas.rs`, `src/slave/driver.rs`, `src/master/reaper.rs`, `DESIGN.md` (§19)

Two parts. **Spans** (DESIGN.md §19): `handshake`, `bootstrap`, `verify` and `reaper`, with `node_id`, `upstream` (absent on the master), `seq` and `epoch`; server-side spans add `client`, `verify` adds `mode`. The two `verify` spans exist since Tasks 2–3. **Deferred items from Plan 2's review:** RocksDB reads that ran on async workers move to blocking threads (the handshake's server view, Tail validation and the junction check, the `Status` RPC), and `ReplicaRegistry::status` no longer reads the log while holding the registry's locks.

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

### Task 7: Repair fetches on the verify session — `FetchKeys`, `FetchRange`, `EntryCrcs`

**Files:**
- Modify: `proto/replication.proto`, `PROTOCOL.md` (§9.2, §13, §14), `src/server/verify.rs`, `src/verify/scan.rs`, `src/slave/verifier.rs`

This task adds the wire half of divergence repair (spec addendum `docs/superpowers/specs/2026-10-09-failover-acks-repair.md` §5): three request kinds on the `VerifyRange` stream of a session pinned to the upstream's snapshot at `S`. `FetchKeys` returns the envelopes (or absence) of given keys, `FetchRange` every key of a range with its envelope (paged like `ListRange`), and `EntryCrcs` the `crc32c` of retained log entries up to `S` (to locate a fork point). The client side becomes a reusable `UpstreamSession`, used by verify and, in Task 8, by repair.

**Interfaces:**
- Consumes: Task 2 `server::verify::{Session, begin, answer, compute, target}`, Task 3 `slave::verifier::{verify, session, compare, capture, Upstream}`, Task 1 `verify::scan::scan`, `verify::{LIST_SOFT_BYTES, MAX_LIST_LIMIT, MAX_RESUME_KEY_BYTES}`; Plan 2 `LogSource::store`, `Store::entry_crc`, `ReadCtx::get_raw`, `ServerState.source`, `slave::client::connect`.
- Produces:
  - Generated `pb::{FetchKeys { cf, keys }, FetchRange { range, limit }, EntryCrcs { from_seq, to_seq }, FetchResult { records, truncated }, FetchedRecord { key, envelope: Option<Vec<u8>> }, EntryCrcResult { entries }, SeqCrc { seq, crc32c }}`, `pb::verify_range_request::Kind::{FetchKeys, FetchRange, EntryCrcs}`, `pb::verify_range_response::Kind::{Fetch, EntryCrcs}`.
  - `verify::scan::fetch_range(&ReadSnapshot, Arc<PhysCf>, &KeyOrder, &Bounds, limit: usize, soft_bytes: usize) -> Result<(Vec<pb::FetchedRecord>, bool)>`.
  - `slave::verifier::{UpstreamSession { pub begin: pb::VerifyBeginResponse, pub upstream: Upstream } (open(&Inner, &Client, VerifyMode) -> Result<Self>, close(self)), Upstream::{fetch_keys(&mut self, cf: &str, keys: &[Vec<u8>]) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>>, fetch_range(&mut self, cf: &str, &Bounds) -> Result<Vec<(Vec<u8>, Vec<u8>)>>, entry_crc(&mut self, Seq) -> Result<Option<u32>>}}`.

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

In PROTOCOL.md §9.2, after the bullet about `UPSTREAM_RESET` (added in Task 2), add the new kinds:

```markdown
- The server ends every `VerifyRange` stream, and drops every session, with `UNAVAILABLE` / `UPSTREAM_RESET` when its state is replaced (it starts bootstrapping itself, or its identity changes), as it does for `Tail` (§7.3).
```

with:

```markdown
- The server ends every `VerifyRange` stream, and drops every session, with `UNAVAILABLE` / `UPSTREAM_RESET` when its state is replaced (it starts bootstrapping itself, or its identity changes), as it does for `Tail` (§7.3).
- `FetchKeys` (repair): the server returns, in the order asked, a `FetchedRecord` per key with its whole envelope as of the session's snapshot, or without `envelope` when the key does not exist. `keys` MUST NOT hold more than 1024 keys. The server MAY answer only a prefix of the keys (setting `truncated`) to bound the answer (RECOMMENDED: stop once ~1 MiB of envelopes is reached), but answers at least one; the client asks again for the rest.
- `FetchRange` (repair): like `ListRange`, but each record carries the whole envelope. The same limits, paging rule and two-record minimum apply.
- `EntryCrcs` (repair): the server returns `(seq, crc32c)` for every entry with `from_seq ≤ seq ≤ min(to_seq, snapshot_seq, from_seq + 1023)` that it still holds (entry `snapshot_seq` is always known). Entries up to the snapshot seq never change, so this locates the first seq where a client's log differs from the server's.
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

In `src/server/verify.rs`, add imports:

```rust
use crate::read::ReadSnapshot;
use crate::store::cfs::PhysCf;
```

with:

```rust
use crate::read::ReadSnapshot;
use crate::source::LogSource;
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

pass the node to `compute` (in `answer`):

```rust
    let s = session.clone();
    let kind = tokio::task::spawn_blocking(move || compute(&s, kind))
```

with:

```rust
    let s = session.clone();
    // Entry CRCs come from the node's log; held only for this request.
    let src = state.source.upgrade();
    let kind = tokio::task::spawn_blocking(move || compute(&s, kind, src.as_deref()))
```

and in `compute`, replace the signature:

```rust
fn compute(s: &Session, kind: ReqKind) -> Result<RespKind, Status> {
```

with:

```rust
fn compute(s: &Session, kind: ReqKind, src: Option<&dyn LogSource>) -> Result<RespKind, Status> {
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
            let src = src
                .ok_or_else(|| reason_status(pb::Reason::ShuttingDown, "node is shutting down"))?;
            // Entries up to S never change; later ones are not part of this session.
            let last = e
                .to_seq
                .min(s.seq)
                .min(e.from_seq.saturating_add(u64::from(MAX_LIST_LIMIT) - 1));
            let mut entries = Vec::new();
            for seq in e.from_seq.max(1)..=last {
                let crc = match src.store().entry_crc(seq).map_err(store_status)? {
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

Replace the whole of `src/slave/verifier.rs` with:

```rust
//! The slave side of Verify (DESIGN.md §12, PROTOCOL.md §9): open a session upstream,
//! capture a local snapshot at exactly the upstream's seq `S` (taken by the applier right
//! after it commits `S`), compare the CF lists, then compare each common CF range by range.

use std::cmp::Ordering as SeqOrder;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Mutex};
use std::time::Instant;

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
    LIST_SOFT_BYTES, MAX_LIST_LIMIT, MAX_RESUME_KEY_BYTES, MismatchPolicy, VERIFY_WINDOW,
    VerifyAbort, VerifyMode, VerifyReport,
};

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
        if inner.cfg.verify.on_mismatch == MismatchPolicy::Rebootstrap {
            // For the upstream the slave follows now; a later repoint makes it inert.
            inner.force_bootstrap.request(*inner.repoint_tx.borrow());
            inner.rebootstrap.notify_one();
        }
    }
    inner.set_last_verify(report.clone());
    inner.notify(|l| l.on_verify_complete(report));
}

/// An open session on the upstream (PROTOCOL.md §9): its `VerifyBegin` answer and the
/// `VerifyRange` stream used for hashes, listings, fetches and entry CRCs.
pub(crate) struct UpstreamSession {
    client: Client,
    pub begin: pb::VerifyBeginResponse,
    pub upstream: Upstream,
}

impl UpstreamSession {
    pub(crate) async fn open(
        inner: &Inner,
        client: &Client,
        mode: VerifyMode,
    ) -> Result<UpstreamSession> {
        let limit = decode_limit(inner.max_entry_bytes.load(Ordering::SeqCst));
        let mut client = client.clone().max_decoding_message_size(limit);
        let req = pb::VerifyBeginRequest {
            client_id: inner.cfg.identifier.clone(),
            mode: mode.to_pb() as i32,
        };
        let begin = client
            .verify_begin(req)
            .await
            .map_err(|s| status_abort(&s))?
            .into_inner();
        let (tx, rx) = mpsc::channel(2 * VERIFY_WINDOW);
        let answers = match client.verify_range(ReceiverStream::new(rx)).await {
            Ok(r) => r.into_inner(),
            Err(s) => {
                let end = pb::VerifyEndRequest {
                    session_id: begin.session_id.clone(),
                };
                let _ = client.verify_end(end).await;
                return Err(status_abort(&s));
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
    inner: &Arc<Inner>,
    identity: DbIdentity,
    mode: VerifyMode,
    span: &tracing::Span,
) -> Result<VerifyReport> {
    let started = Instant::now();
    let client = client::connect(inner)?;
    // The applier commits nothing while the upstream pins its snapshot, so this slave cannot
    // pass the snapshot's seq before the capture is set up.
    let hold = inner.hold_applier();
    let mut session = UpstreamSession::open(inner, &client, mode).await?;
    span.record("seq", session.begin.snapshot_seq);
    let result = compare(inner, &mut session, hold, identity, mode, started).await;
    session.close().await;
    result
}

async fn compare(
    inner: &Arc<Inner>,
    session: &mut UpstreamSession,
    hold: CaptureHold,
    identity: DbIdentity,
    mode: VerifyMode,
    started: Instant,
) -> Result<VerifyReport> {
    let begin = &session.begin;
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
        let compared = compare_cf(upstream, &local, &mut run, &mut ranges).await;
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
/// now if the slave is at `s`, otherwise by the applier right after it commits `s`.
async fn capture(inner: &Arc<Inner>, hold: CaptureHold, s: Seq) -> Result<ReadSnapshot> {
    let applied = inner.store.position().last_seq;
    match applied.cmp(&s) {
        // Only possible with an upstream that is behind this slave (DESIGN.md §6).
        SeqOrder::Greater => {
            return Err(abort(VerifyAbort::PassedSnapshot {
                snapshot_seq: s,
                applied_seq: applied,
            }));
        }
        SeqOrder::Equal => {
            let i = inner.clone();
            let snap = blocking(move || i.store.read_snapshot(0)).await?;
            // A commit that was already under way when the hold was taken may have landed.
            if snap.seq() != s {
                return Err(abort(VerifyAbort::PassedSnapshot {
                    snapshot_seq: s,
                    applied_seq: snap.seq(),
                }));
            }
            return Ok(snap);
        }
        SeqOrder::Less => {}
    }
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
pub(crate) struct Upstream {
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
        match self.answers.message().await {
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
    /// in the order asked.
    pub(crate) async fn fetch_keys(
        &mut self,
        cf: &str,
        keys: &[Vec<u8>],
    ) -> Result<Vec<(Vec<u8>, Option<Vec<u8>>)>> {
        let mut out = Vec::with_capacity(keys.len());
        let mut rest = keys;
        while !rest.is_empty() {
            let ask = &rest[..rest.len().min(MAX_LIST_LIMIT as usize)];
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
            out.extend(f.records.into_iter().map(|r| (r.key, r.envelope)));
        }
        Ok(out)
    }

    /// Every key of `bounds` with its envelope at the session's snapshot, paging through
    /// truncated answers like a listing.
    pub(crate) async fn fetch_range(
        &mut self,
        cf: &str,
        bounds: &Bounds,
    ) -> Result<Vec<(Vec<u8>, Vec<u8>)>> {
        let mut out: Vec<(Vec<u8>, Vec<u8>)> = Vec::new();
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
            let before = out.len();
            for r in f.records {
                if resume.as_deref() == Some(r.key.as_slice()) {
                    continue;
                }
                let env = r.envelope.ok_or_else(|| {
                    abort(VerifyAbort::Upstream(
                        "FetchRange record without envelope".into(),
                    ))
                })?;
                out.push((r.key, env));
            }
            if !f.truncated {
                return Ok(out);
            }
            match out.last() {
                Some((k, _)) if out.len() > before && k.len() <= MAX_RESUME_KEY_BYTES => {
                    page.lower = Some(k.clone());
                    resume = Some(k.clone());
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

(Changes from Task 3: `UpstreamSession` opens and closes a session for any user; `Upstream` is `pub(crate)` and gains `fetch_keys`, `fetch_range` and `entry_crc`; answers of an unexpected kind are errors.)

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

### Task 8: Divergence repair and `MismatchPolicy::Repair`

**Files:**
- Create: `src/store/repair.rs`, `src/slave/repair.rs`, `tests/repair.rs`
- Modify: `src/store/mod.rs`, `src/store/meta.rs`, `src/store/bootstrap.rs`, `src/slave/{mod,inner,state,config,policy,driver,repoint,apply,bootstrap,source,verifier}.rs`, `src/verify/mod.rs`, `src/listener.rs`, `proto/replication.proto`, `tests/support/mod.rs`, `tests/repoint.rs`, `tests/failpoints.rs`, `DESIGN.md` (§3.2, §12, §12.1 new, §13, §13.1, §15, §16, §16.3, §18, §21, §22), `PROTOCOL.md` (§6.5, §11.2, §14)

Spec addendum §5. A slave whose log holds entries `F..=L` its upstream's history does not have (`DIVERGED`) can, instead of a full bootstrap, take **only the keys those entries touched** from the upstream's snapshot at `S`, drop `F..=L`, set `last_seq = F − 1` in the same synced batch, and tail from `F`. Replaying `F..=S` (absolute ops only) over the repaired keys gives the upstream's exact state at `S`, so the node answers `NotReady` (reads, downstream handshakes, `node_state = repairing:<S>`, persisted) until replay reaches `S`. The fork `F` is located by a binary search over `EntryCrcs` (Task 7). The same machinery repairs the keys a verify reports (`MismatchPolicy::Repair`). Anything it cannot prove falls back to a full bootstrap: a CF operation in the suffix, a suffix or fork entry no longer held by both sides, a CF that differs, or more than `repair_max_bytes` to fetch.

It runs in the driver between sessions, so no entry is applied concurrently; it cancels a pending verify capture and resets downstream streams before the rewind. `repoint(.., RepointDivergence::Repair { accept_loss })` now works (it returned `Error::Unsupported` in Plan 2) and settles `Repaired { discarded }`; `DivergencePolicy::Repair` does the same on reconnects. A slave halted `Diverged` records its fork point (best effort, 30 s), and `Slave::diverged_entries()` returns the local suffix for salvage.

**Interfaces:**
- Consumes: Task 7 `UpstreamSession::{open, close}`, `Upstream::{fetch_keys, fetch_range, entry_crc}`; Task 3 `Inner::{cancel_capture, capture_after_commit, rebootstrap}`, `verifier::finish`; Task 1 `verify::range::{Bounds, KeyOrder}`, `Mismatch`; Plan 2 `Store::{entry_crc, read_entries(from, max, max_bytes), live_ctx, iter_raw_from, handle, cf, write_meta, set_position, set_first_retained_seq, first_changelog_seq, tuning}`, `meta::{encode_identity, encode_epoch_history, be64}`, `net::convert::{cf_from_pb, checked_history, identity_from_pb}`, `driver::{session, end_for, status_end, unless_repointed, commit_halt_clear}`, `Inner::{settle_repoint, fail_repoint_on_halt, force_bootstrap, halt_clear_pending, reset_downstream, set_ident, seq_tx, notify}`, `repoint::divergence`, `background::blocking`, `failpoint::fire`; harness `Cluster::{master, master_config, slave_config, restart_master_after_backup, path}`, `restore_dir`, `RawDb`, `RecordingListener`, `wait_state`, `wait_until`, `assert_converged`.
- Produces:
  - `store::repair::Rewind { keep_through, keep_crc, drop_through, identity, epoch_history, node_state, clear: Vec<(Arc<PhysCf>, Bounds)>, writes: Vec<(Arc<PhysCf>, Vec<u8>, Option<Vec<u8>>)> }`, `Store::{rewind(&Rewind) -> Result<()>, set_diverged_at(Option<Seq>) -> Result<()>, diverged_at() -> Result<Option<Seq>>}`, `meta::key::DIVERGED_AT`.
  - `slave::repair::{AffectedSet { keys, ranges } (from_entries, from_mismatches, merge), RepairError::{Fallback, Retry}, Repaired { discarded, rewound_to, ready_at }, record_fork, repair_divergence, repair_pending}`; failpoint `slave-repair-after-rewind`.
  - `NodeState::Repairing(Seq)` (`repairing:<seq>`); `Inner::{repairing, begin_repair, finish_repair_at, request_repair, take_pending_repair, has_pending_repair}`; `Inner::is_ready` is false while repairing.
  - Public: `DivergencePolicy::Repair`, `MismatchPolicy::Repair`, `SlaveConfig::repair_max_bytes: u64` (default 64 MiB), `Slave::diverged_entries(&self) -> Result<Vec<LogEntry>>`, `ReplicationListener::on_rewind(&self, last_seq: Seq)`, `RepointOutcome::Repaired { discarded }` is now returned; `policy::Action::Repair`; `repoint::mode_error` is removed.
  - Harness: `RecordingListener::rewinds() -> usize`.

- [ ] **Step 1: The store side — an atomic rewind**

In `src/store/meta.rs`, after:

```rust
    pub const MAX_BURNED_EPOCH: &[u8] = b"max_burned_epoch";
```

add:

```rust
    /// The first seq of the local divergent suffix, recorded when a slave halts `Diverged`
    /// (`Slave::diverged_entries`). Cleared by a repair or a bootstrap.
    pub const DIVERGED_AT: &[u8] = b"diverged_at";
```

In `src/store/bootstrap.rs`, `reset_for_bootstrap` forgets it too:

```rust
                key::MAX_ENTRY_BYTES,
            ] {
                batch.delete_cf(&meta_cf, k);
```

with:

```rust
                key::MAX_ENTRY_BYTES,
                key::DIVERGED_AT,
            ] {
                batch.delete_cf(&meta_cf, k);
```

In `src/store/mod.rs`, register the module:

```rust
pub(crate) mod repl;
```

with:

```rust
pub(crate) mod repair;
pub(crate) mod repl;
```

Create `src/store/repair.rs`:

```rust
//! Store side of repair (DESIGN.md §12.1): rewinding a slave to a common seq in one synced
//! batch, with the affected keys set to the upstream's state, and the recorded fork point of
//! a divergence.

use std::sync::Arc;

use rocksdb::{WriteBatch, WriteOptions};

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
    /// The `node_state` to persist (`repairing:<seq>` or `ready`).
    pub node_state: Vec<u8>,
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
        batch.put_cf(&meta_cf, key::NODE_STATE, &r.node_state);
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

    /// Records (or, with `None`, forgets) where the local divergent suffix starts. Synced.
    pub(crate) fn set_diverged_at(&self, fork: Option<Seq>) -> Result<()> {
        match fork {
            Some(seq) => self.write_meta(&[(key::DIVERGED_AT, &seq.to_be_bytes())], true),
            None => {
                let mut batch = WriteBatch::default();
                batch.delete_cf(&self.cf(CF_META), key::DIVERGED_AT);
                let mut wo = WriteOptions::default();
                wo.set_sync(true);
                Ok(self.db().write_opt(batch, &wo)?)
            }
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
            },
            EpochStart {
                epoch: 2,
                start_seq: 3,
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
                node_state: b"repairing:9".to_vec(),
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
        assert_eq!(m.node_state.as_deref(), Some(&b"repairing:9"[..]));
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

- [ ] **Step 2: `repairing:<seq>`, the new policies and settings**

In `src/slave/state.rs`, replace:

```rust
/// `__meta.node_state`: `ready`, `bootstrapping` or `halted:<Reason>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeState {
    Ready,
    Bootstrapping,
    Halted(HaltReason),
}
```

with:

```rust
/// `__meta.node_state`: `ready`, `bootstrapping`, `halted:<Reason>` or `repairing:<seq>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeState {
    Ready,
    Bootstrapping,
    Halted(HaltReason),
    /// A repair committed; reads answer `NotReady` until `last_seq` reaches this seq again
    /// (DESIGN.md §12.1).
    Repairing(Seq),
}
```

in `encode`:

```rust
                format!("halted:{}", r.name()).into_bytes()
            }
        }
    }
```

with:

```rust
                format!("halted:{}", r.name()).into_bytes()
            }
            NodeState::Repairing(seq) => format!("repairing:{seq}").into_bytes(),
        }
    }
```

in `decode`:

```rust
        if raw == NODE_BOOTSTRAPPING {
            return Ok(NodeState::Bootstrapping);
        }
        std::str::from_utf8(raw)
```

with:

```rust
        if raw == NODE_BOOTSTRAPPING {
            return Ok(NodeState::Bootstrapping);
        }
        if let Some(seq) = std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.strip_prefix("repairing:"))
            .and_then(|s| s.parse().ok())
        {
            return Ok(NodeState::Repairing(seq));
        }
        std::str::from_utf8(raw)
```

and in the test `node_state_round_trips_and_only_data_halts_persist`:

```rust
        for s in [NodeState::Ready, NodeState::Bootstrapping] {
```

with:

```rust
        for s in [
            NodeState::Ready,
            NodeState::Bootstrapping,
            NodeState::Repairing(42),
        ] {
```

In `src/slave/state.rs`, replace:

```rust
        assert!(NodeState::decode(Some(b"halted:Nope")).is_err());
```

with:

```rust
        assert!(NodeState::decode(Some(b"halted:Nope")).is_err());
        assert_eq!(NodeState::Repairing(7).encode(), b"repairing:7");
        assert!(NodeState::decode(Some(b"repairing:x")).is_err());
```

In `src/slave/config.rs`, replace:

```rust
/// What to do when the upstream reports `DIVERGED`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DivergencePolicy {
    /// Halt with `Diverged` (persisted).
    #[default]
    Halt,
    Rebootstrap,
}
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
}
```

add the setting to `SlaveConfig`, before `sync_writes`:

```rust
    /// Fsync every applied entry before acknowledging it.
```

with:

```rust
    /// A repair (DESIGN.md §12.1) that would fetch more than this many bytes of keys and
    /// envelopes falls back to a bootstrap. Default 64 MiB.
    pub repair_max_bytes: u64,
    /// Fsync every applied entry before acknowledging it.
```

and to `SlaveConfig::new`:

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

In `src/verify/mod.rs`, replace:

```rust
    /// Report it, then discard local data and bootstrap from the upstream.
    Rebootstrap,
}
```

with:

```rust
    /// Report it, then discard local data and bootstrap from the upstream.
    Rebootstrap,
    /// Report it, then take the reported keys and ranges from the upstream (DESIGN.md
    /// §12.1), without a bootstrap. A `CfSpec` mismatch, or a repair that cannot be done,
    /// bootstraps instead.
    Repair,
}
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

and in the trait docs:

```rust
/// Delivery is at-most-once across crashes. Within a process, `on_entry` seqs are strictly
/// increasing between bootstraps; after `on_bootstrap_complete(S)` delivery continues at
/// `S + 1` (state was replaced wholesale, so rebuild derived state).
```

with:

```rust
/// Delivery is at-most-once across crashes. Within a process, `on_entry` seqs are strictly
/// increasing between bootstraps and rewinds; after `on_bootstrap_complete(S)` or
/// `on_rewind(S)` delivery continues at `S + 1` (state was replaced wholesale, or in part, so
/// rebuild derived state).
```

Run: `cargo build`
Expected: two `E0004` errors for the new variants: `NodeState::Repairing` not covered in `src/slave/inner.rs` (Step 3) and `DivergencePolicy::Repair` not covered in `src/slave/policy.rs` (Step 5).

- [ ] **Step 3: The slave's repair state**

In `src/slave/inner.rs`, replace:

```rust
use super::repoint::PendingRepoint;
```

with:

```rust
use super::repair::AffectedSet;
use super::repoint::PendingRepoint;
```

add the fields:

```rust
    /// Wakes the tail loop when a verify asks for a re-bootstrap.
    pub rebootstrap: Notify,
}
```

with:

```rust
    /// Wakes the tail loop when a verify asks for a re-bootstrap or a repair.
    pub rebootstrap: Notify,
    /// Non-zero while a repair waits for replay to reach this seq: reads answer `NotReady`.
    repair_until: AtomicU64,
    /// Keys a verify asked to repair (`MismatchPolicy::Repair`), done at the next session.
    pending_repair: Mutex<Option<AffectedSet>>,
}
```

in `Inner::new`, replace:

```rust
        let state = match node_state {
            NodeState::Halted(r) => SlaveState::Halted(r),
            NodeState::Ready | NodeState::Bootstrapping => SlaveState::Connecting,
        };
        let last_seq = store.position().last_seq;
```

with:

```rust
        let state = match node_state {
            NodeState::Halted(r) => SlaveState::Halted(r),
            NodeState::Ready | NodeState::Bootstrapping | NodeState::Repairing(_) => {
                SlaveState::Connecting
            }
        };
        let last_seq = store.position().last_seq;
        // A repair interrupted before replay reached its target stays not ready.
        let repair_until = match node_state {
            NodeState::Repairing(until) if until > last_seq => until,
            _ => 0,
        };
```

and initialise them:

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

Replace `is_ready`:

```rust
    /// Readable and servable: the slave holds a consistent state at some seq.
    pub(crate) fn is_ready(&self) -> bool {
        self.ident
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
    }
```

with:

```rust
    /// Readable and servable: the slave holds a consistent state at some seq (not during a
    /// bootstrap, nor while a repair replays up to its target).
    pub(crate) fn is_ready(&self) -> bool {
        self.ident
            .read()
            .unwrap_or_else(PoisonError::into_inner)
            .is_some()
            && self.repair_until.load(Ordering::SeqCst) == 0
    }
```

In `src/slave/inner.rs`, insert before the test module:

```rust
#[cfg(test)]
mod tests {
```

insert:

```rust
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
        if let Err(e) = self
            .store
            .write_meta(&[(key::NODE_STATE, &NodeState::Ready.encode())], true)
        {
            // On restart, `repairing:<until>` with last_seq >= until is treated as ready.
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

In `src/slave/source.rs`, `node_state` reports it:

```rust
            SlaveState::Connecting | SlaveState::Streaming => {
                if Inner::is_ready(self) {
                    "ready".into()
                } else {
                    "bootstrapping".into()
                }
            }
```

with:

```rust
            SlaveState::Connecting | SlaveState::Streaming => {
                if Inner::is_ready(self) {
                    "ready".into()
                } else if let Some(seq) = self.repairing() {
                    format!("repairing:{seq}")
                } else {
                    "bootstrapping".into()
                }
            }
```

In `src/slave/apply.rs`, replace:

```rust
        i.capture_after_commit(e.seq);
        Ok::<(), Failure>(())
```

with:

```rust
        i.capture_after_commit(e.seq);
        // A repair is complete once replay reaches its target (DESIGN.md §12.1).
        i.finish_repair_at(e.seq);
        Ok::<(), Failure>(())
```

In `src/slave/bootstrap.rs`, in `install`, replace:

```rust
    // A verify waiting for a seq of the old state can never be served now.
    inner.cancel_capture();
```

with:

```rust
    // A verify waiting for a seq of the old state can never be served now.
    inner.cancel_capture();
    // A bootstrap replaces everything a repair would have fixed.
    inner.take_pending_repair();
    inner.begin_repair(0);
```

- [ ] **Step 4: The repair itself**

In `src/slave/mod.rs`, register the module:

```rust
mod repoint;
mod source;
```

with:

```rust
mod repair;
mod repoint;
mod source;
```

Create `src/slave/repair.rs`:

```rust
//! Repair (DESIGN.md §12.1): bring a slave back in line with its upstream by rewriting only
//! the keys that differ, instead of a full bootstrap.
//!
//! Both kinds run in the driver, while no entry is being applied:
//! - **Divergence repair**: the local log has entries `F..=L` the upstream's history does not
//!   have. The keys those entries touched are set to the upstream's state at its snapshot
//!   `S`, the suffix is dropped, `last_seq` becomes `F - 1`, and the slave tails from `F`.
//!   Replaying `F..=S` (absolute ops) over the repaired keys gives exactly the upstream's
//!   state at `S`; until then reads answer `NotReady`.
//! - **Verify repair** (`MismatchPolicy::Repair`): the keys a verify reported are set to the
//!   upstream's state at a fresh snapshot `S`; local entries after `S` are dropped and
//!   fetched again, so the same replay argument holds.
//!
//! Anything this cannot handle falls back to a full bootstrap: a CF operation in the
//! suffix, a suffix or fork entry that is no longer retained on either side, a CF that
//! differs between the sides, or more than `repair_max_bytes` to fetch.

use std::collections::{BTreeMap, BTreeSet};
use std::ops::RangeInclusive;
use std::sync::Arc;

use super::client::Client;
use super::inner::Inner;
use super::state::NodeState;
use super::verifier::{Upstream, UpstreamSession};
use crate::background::blocking;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::failpoint;
use crate::net::convert::{cf_from_pb, checked_history, identity_from_pb};
use crate::pb;
use crate::store::repair::Rewind;
use crate::types::{CfEntry, LogEntry, Op, Seq};
use crate::verify::range::Bounds;
use crate::verify::{Mismatch, VerifyMode};

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

/// The first seq where the local log and the upstream's (up to its snapshot `s`) differ,
/// found by binary search over entry CRCs. `last + 1` if they agree up to
/// `min(local last, s)`. Fails over to bootstrap when the answer cannot be proven from
/// entries both sides still hold.
async fn locate_fork(
    inner: &Inner,
    up: &mut Upstream,
    s: Seq,
) -> std::result::Result<Seq, RepairError> {
    let upper = inner.store.position().last_seq.min(s);
    if upper == 0 {
        return Ok(1);
    }
    let lo = inner.store.first_retained_seq().clamp(1, upper);
    // Some(true): entry `seq` is the same on both sides; None: one side no longer has it.
    let mut same = async |seq: Seq| -> std::result::Result<Option<bool>, RepairError> {
        let local = inner.store.entry_crc(seq)?;
        let upstream = up.entry_crc(seq).await?;
        Ok(local.zip(upstream).map(|(a, b)| a == b))
    };
    match same(upper).await? {
        Some(true) => return Ok(upper + 1),
        Some(false) => {}
        None => return Err(fallback(format!("entry {upper} is not held by both sides"))),
    }
    // Invariant: entry `hi` differs; the first difference is in `low..=hi`.
    let (mut low, mut hi) = (lo, upper);
    while low < hi {
        let mid = low + (hi - low) / 2;
        match same(mid).await? {
            Some(true) => low = mid + 1,
            Some(false) => hi = mid,
            None => return Err(fallback(format!("entry {mid} is not held by both sides"))),
        }
    }
    if hi > 1 && same(hi - 1).await? != Some(true) {
        return Err(fallback(format!(
            "the last common entry {} is no longer held by both sides",
            hi - 1
        )));
    }
    Ok(hi)
}

/// Opens a session and checks it follows the identity the handshake announced.
async fn open(
    inner: &Inner,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<UpstreamSession, RepairError> {
    let session = UpstreamSession::open(inner, client, VerifyMode::Quick).await?;
    if session.begin.identity != hs.identity {
        session.close().await;
        return Err(RepairError::Retry(
            "the upstream's identity changed since the handshake".into(),
        ));
    }
    Ok(session)
}

/// How long a halting slave asks its upstream for the fork point.
const FORK_LOOKUP_TIMEOUT: std::time::Duration = std::time::Duration::from_secs(30);

/// Finds the fork point of a divergence and records it for `Slave::diverged_entries`.
/// Best effort: on failure the fork point is recorded as unknown.
pub(crate) async fn record_fork(
    inner: &Arc<Inner>,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> Option<Seq> {
    let lookup = async {
        let mut session = open(inner, client, hs).await?;
        let s = session.begin.snapshot_seq;
        let fork = locate_fork(inner, &mut session.upstream, s).await;
        session.close().await;
        fork
    };
    let fork = match tokio::time::timeout(FORK_LOOKUP_TIMEOUT, lookup).await {
        Ok(Ok(fork)) => Some(fork),
        Ok(Err(e)) => {
            tracing::warn!(node = %inner.cfg.identifier, error = ?e, "fork point unknown");
            None
        }
        Err(_) => {
            tracing::warn!(node = %inner.cfg.identifier, "fork point unknown: the upstream did not answer in time");
            None
        }
    };
    let i = inner.clone();
    if let Err(e) = blocking(move || i.store.set_diverged_at(fork)).await {
        tracing::warn!(node = %inner.cfg.identifier, error = %e, "could not record the fork point");
    }
    fork
}

/// Repairs a divergence from the upstream that answered `hs` (`REJECT / DIVERGED`).
pub(crate) async fn repair_divergence(
    inner: &Arc<Inner>,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<Repaired, RepairError> {
    let mut session = open(inner, client, hs).await?;
    let result = async {
        let s = session.begin.snapshot_seq;
        let fork = locate_fork(inner, &mut session.upstream, s).await?;
        if fork < hs.first_retained_seq {
            return Err(fallback(format!(
                "the upstream no longer holds entry {fork}"
            )));
        }
        let last = inner.store.position().last_seq;
        let i = inner.clone();
        let suffix = blocking(move || i.store.read_entries(fork, usize::MAX, usize::MAX))
            .await
            .map_err(|e| match e {
                Error::Gap { .. } => fallback(format!(
                    "the local suffix from {fork} is no longer retained"
                )),
                other => RepairError::Retry(other.to_string()),
            })?;
        let Some(set) = AffectedSet::from_entries(&suffix)? else {
            return Err(fallback(
                "the divergent suffix creates or drops a column family",
            ));
        };
        let plan = Plan {
            keep_through: fork - 1,
            ready_at: s,
            set,
        };
        apply(inner, hs, &mut session, plan, last).await
    }
    .await;
    session.close().await;
    result
}

/// Repairs the keys a verify reported (`MismatchPolicy::Repair`), taking them from the
/// upstream that answered `hs` (`RESUME`).
pub(crate) async fn repair_pending(
    inner: &Arc<Inner>,
    client: &Client,
    hs: &pb::HandshakeResponse,
) -> std::result::Result<Repaired, RepairError> {
    let Some(set) = inner.take_pending_repair() else {
        let last = inner.store.position().last_seq;
        return Ok(Repaired {
            discarded: last + 1..=last,
            rewound_to: last,
            ready_at: last,
        });
    };
    let mut session = match open(inner, client, hs).await {
        Ok(s) => s,
        Err(e) => {
            inner.request_repair(set);
            return Err(e);
        }
    };
    let result = async {
        let s = session.begin.snapshot_seq;
        let last = inner.store.position().last_seq;
        if last > s && inner.store.entry_crc(s)? != session.upstream.entry_crc(s).await? {
            return Err(fallback(format!("entry {s} differs from the upstream's")));
        }
        let plan = Plan {
            keep_through: last.min(s),
            ready_at: last.max(s),
            set: set.clone(),
        };
        apply(inner, hs, &mut session, plan, last).await
    }
    .await;
    session.close().await;
    if let Err(RepairError::Retry(_)) = &result {
        inner.request_repair(set);
    }
    result
}

/// A repair to carry out: rewind to `keep_through`, take `set` from the upstream's snapshot,
/// and refuse reads until `ready_at` is applied again.
struct Plan {
    keep_through: Seq,
    ready_at: Seq,
    set: AffectedSet,
}

async fn apply(
    inner: &Arc<Inner>,
    hs: &pb::HandshakeResponse,
    session: &mut UpstreamSession,
    plan: Plan,
    last: Seq,
) -> std::result::Result<Repaired, RepairError> {
    let identity = hs
        .identity
        .as_ref()
        .ok_or_else(|| RepairError::Retry("handshake without identity".into()))?;
    let identity = identity_from_pb(identity)?;
    let history = checked_history(&identity, &hs.epoch_history)?;
    let upstream_cfs: Vec<CfEntry> = session.begin.cf_specs.iter().map(cf_from_pb).collect();
    let max_bytes = inner.cfg.repair_max_bytes;
    let mut fetched_bytes = 0u64;
    let mut clear = Vec::new();
    let mut writes = Vec::new();
    for cf in plan.set.cfs() {
        let local = inner
            .store
            .live_cf(cf)
            .map_err(|_| fallback(format!("CF {cf} does not exist locally")))?;
        if !upstream_cfs.contains(&local.entry) {
            return Err(fallback(format!(
                "CF {cf} differs between this slave and its upstream"
            )));
        }
        let mut take =
            |key: Vec<u8>, env: Option<Vec<u8>>| -> std::result::Result<(), RepairError> {
                if let Some(v) = &env {
                    envelope::decode(v).map_err(|e| {
                        RepairError::Retry(format!("upstream sent a bad envelope: {e}"))
                    })?;
                    fetched_bytes += (key.len() + v.len()) as u64;
                    if fetched_bytes > max_bytes {
                        return Err(fallback(format!(
                            "more than repair_max_bytes ({max_bytes}) to fetch"
                        )));
                    }
                }
                writes.push((local.clone(), key, env));
                Ok(())
            };
        if let Some(keys) = plan.set.keys.get(cf) {
            let keys: Vec<Vec<u8>> = keys.iter().cloned().collect();
            for (key, env) in session.upstream.fetch_keys(cf, &keys).await? {
                take(key, env)?;
            }
        }
        for bounds in plan.set.ranges.get(cf).into_iter().flatten() {
            for (key, env) in session.upstream.fetch_range(cf, bounds).await? {
                take(key, Some(env))?;
            }
            clear.push((local.clone(), bounds.clone()));
        }
    }
    let keep_crc = match plan.keep_through {
        0 => None,
        k => Some(
            inner
                .store
                .entry_crc(k)?
                .ok_or_else(|| fallback(format!("local entry {k} is no longer retained")))?,
        ),
    };
    let repairing = plan.ready_at > plan.keep_through;
    let node_state = if repairing {
        NodeState::Repairing(plan.ready_at)
    } else {
        NodeState::Ready
    };
    let rewind = Rewind {
        keep_through: plan.keep_through,
        keep_crc,
        drop_through: last,
        identity,
        epoch_history: history.clone(),
        node_state: node_state.encode(),
        clear,
        writes,
    };
    // Readers and downstream replicas must not see the mixed state, and a verify waiting for
    // a seq of the old state can never be served now.
    inner.begin_repair(if repairing { plan.ready_at } else { 0 });
    inner.reset_downstream();
    inner.cancel_capture();
    let i = inner.clone();
    if let Err(e) = blocking(move || i.store.rewind(&rewind)).await {
        // The batch is atomic: nothing was written, the old state stands.
        inner.begin_repair(0);
        return Err(e.into());
    }
    inner.set_ident(Some((identity, history)));
    inner.seq_tx.send_replace(plan.keep_through);
    tracing::info!(
        node = %inner.cfg.identifier,
        rewound_to = plan.keep_through,
        discarded = last.saturating_sub(plan.keep_through),
        ready_at = plan.ready_at,
        "repaired from the upstream"
    );
    inner.notify(|l| l.on_rewind(plan.keep_through));
    if failpoint::fire("slave-repair-after-rewind") {
        // Emulated crash right after the rewind: freeze until the test kills the node.
        std::future::pending::<()>().await;
    }
    Ok(Repaired {
        discarded: plan.keep_through + 1..=last,
        rewound_to: plan.keep_through,
        ready_at: plan.ready_at,
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::encode_ops;
    use crate::types::CfSpec;

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

Run: `cargo build`
Expected: only the `DivergencePolicy::Repair` error in `src/slave/policy.rs` remains; Step 5 wires the module in.

- [ ] **Step 5: Wire it into the policies, the driver and the verifier**

In `src/slave/policy.rs`, add the action:

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

and map the policy:

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

In the test `reject_rows`, replace:

```rust
        assert_eq!(
            act(Decision::Reject, Reason::Diverged, &OPPOSITE),
            Action::Bootstrap
        );
```

with:

```rust
        assert_eq!(
            act(Decision::Reject, Reason::Diverged, &OPPOSITE),
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
```

and in `status_rows`:

```rust
        assert_eq!(
            after_status(&s(Reason::Diverged), &OPPOSITE),
            Action::Bootstrap
        );
```

with:

```rust
        assert_eq!(
            after_status(&s(Reason::Diverged), &OPPOSITE),
            Action::Bootstrap
        );
        let repair = Policies {
            divergence: DivergencePolicy::Repair,
            ..DEFAULTS
        };
        assert_eq!(after_status(&s(Reason::Diverged), &repair), Action::Repair);
```

In `src/slave/driver.rs`, replace the imports:

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

In `end_for`, replace the last arm, and add a helper after the function:

```rust
        Action::Again | Action::Tail | Action::AdoptEpochThenTail => SessionEnd::Again,
    }
}
```

with:

```rust
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
}
```

In `session`, a repoint's `Repair` mode now repairs:

```rust
            action = match mode {
                RepointDivergence::Rebootstrap { .. } => Action::Bootstrap,
                RepointDivergence::Refuse | RepointDivergence::Repair { .. } => {
                    let local_epoch = inner.ident().map_or(0, |(id, _)| id.epoch);
                    let err = repoint::mode_error(mode, || {
                        repoint::divergence(local_epoch, inner.store.position().last_seq, &hs)
                    });
                    tracing::warn!(node = %inner.cfg.identifier, error = %err, "repoint refused: diverged");
```

with:

```rust
            action = match mode {
                RepointDivergence::Rebootstrap { .. } => Action::Bootstrap,
                RepointDivergence::Repair { .. } => Action::Repair,
                RepointDivergence::Refuse => {
                    let local_epoch = inner.ident().map_or(0, |(id, _)| id.epoch);
                    let err =
                        repoint::divergence(local_epoch, inner.store.position().last_seq, &hs);
                    tracing::warn!(node = %inner.cfg.identifier, error = %err, "repoint refused: diverged");
```

and in the `match action`, replace the `Tail` arm:

```rust
        Action::Tail => {
            commit_halt_clear(inner).await;
            inner.settle_repoint(generation, Ok(RepointOutcome::Resumed));
            tail(inner, generation, &c, &hs, None, backoff).await
        }
```

with:

```rust
        Action::Tail => {
            commit_halt_clear(inner).await;
            if inner.has_pending_repair() {
                // A verify reported keys to repair (`MismatchPolicy::Repair`): fix them, then
                // handshake again from the new position.
                let span = repair_span(inner);
                return match repair::repair_pending(inner, &c, &hs)
                    .instrument(span)
                    .await
                {
                    Ok(_) => SessionEnd::Again,
                    Err(e) => repair_failed(inner, generation, e),
                };
            }
            inner.settle_repoint(generation, Ok(RepointOutcome::Resumed));
            tail(inner, generation, &c, &hs, None, backoff).await
        }
```

add the `Repair` arm and record the fork point on a `Diverged` halt:

```rust
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Again => SessionEnd::Again,
    }
}
```

with:

```rust
        Action::Repair => {
            let span = repair_span(inner);
            match repair::repair_divergence(inner, &c, &hs)
                .instrument(span)
                .await
            {
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
                Err(e) => repair_failed(inner, generation, e),
            }
        }
        Action::Halt(HaltReason::Diverged) => {
            // Where the local suffix starts, for `Slave::diverged_entries` (best effort).
            repair::record_fork(inner, &c, &hs).await;
            SessionEnd::Halt(HaltReason::Diverged)
        }
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Again => SessionEnd::Again,
    }
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

In `tail`, a verify's repair request also ends the stream:

```rust
            // A verify asked for a re-bootstrap (`MismatchPolicy::Rebootstrap`): handshake again,
            // which bootstraps because the request is set. A stale wake-up is ignored.
            _ = inner.rebootstrap.notified() => {
                if inner.force_bootstrap.is_requested_for(generation) {
                    return SessionEnd::Again;
                }
                continue;
            }
```

with:

```rust
            // A verify asked for a re-bootstrap (`MismatchPolicy::Rebootstrap`) or a repair
            // (`MismatchPolicy::Repair`): handshake again, which bootstraps or repairs because
            // the request is set. A stale wake-up is ignored.
            _ = inner.rebootstrap.notified() => {
                if inner.force_bootstrap.is_requested_for(generation) || inner.has_pending_repair() {
                    return SessionEnd::Again;
                }
                continue;
            }
```

In `src/slave/verifier.rs`, in `finish`, replace:

```rust
        if inner.cfg.verify.on_mismatch == MismatchPolicy::Rebootstrap {
            // For the upstream the slave follows now; a later repoint makes it inert.
            inner.force_bootstrap.request(*inner.repoint_tx.borrow());
            inner.rebootstrap.notify_one();
        }
```

with:

```rust
        let set = match inner.cfg.verify.on_mismatch {
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

with the import:

```rust
use super::inner::{CaptureHold, CaptureMiss, Inner};
```

with:

```rust
use super::inner::{CaptureHold, CaptureMiss, Inner};
use super::repair::AffectedSet;
```

In `src/slave/repoint.rs`, delete `mode_error` (no longer used):

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
```

with:

```rust

```

and update the docs of `Repair`:

```rust
    /// Discard only the divergent suffix and resume (DESIGN.md §22, Plan 3). **Not available in
    /// this version:** `repoint` returns `Error::Unsupported` and the slave halts
    /// `Diverged` with its data intact. It needs verify sessions (Plan 3).
    Repair { accept_loss: AcceptDataLoss },
```

with:

```rust
    /// Discard only the divergent suffix (DESIGN.md §12.1): the keys it touched are taken from
    /// the new upstream, the suffix is dropped, and the slave replays the upstream's history
    /// (`Repaired { discarded }`; reads answer `NotReady` until replay reaches the upstream's
    /// snapshot seq). When repair cannot be proven safe (a CF operation in the suffix, entries
    /// no longer retained, too much to fetch), the slave bootstraps instead (`Bootstrapped`).
    Repair { accept_loss: AcceptDataLoss },
```

In `src/slave/repoint.rs`, replace:

```rust
    /// The divergent suffix `discarded` was rolled back (Plan 3; never returned by this
    /// version).
    Repaired { discarded: RangeInclusive<Seq> },
```

with:

```rust
    /// The divergent suffix `discarded` was rolled back and the keys it touched were taken
    /// from the new upstream (`RepointDivergence::Repair`). Reads answer `NotReady` until the
    /// slave has replayed up to the upstream's snapshot seq.
    Repaired { discarded: RangeInclusive<Seq> },
```

and in the docs of `Slave::repoint`:

```rust
    ///   `RepointDivergence::Repair` returns `Err(Unsupported)` in this version (it needs
    ///   Plan 3's verify sessions) and also leaves the slave halted with its data intact.
```

with:

```rust
    ///   `RepointDivergence::Repair { accept_loss: AcceptDataLoss }` discards only the
    ///   divergent suffix and returns `Ok(Repaired { discarded })` (or `Ok(Bootstrapped)` when
    ///   repair is not possible).
```

In `src/slave/mod.rs`, add before `watch_state`:

```rust
    pub fn watch_state(&self) -> watch::Receiver<SlaveState> {
```

with:

```rust
    /// On a slave halted `Diverged`: its local entries past the fork point, for salvage (DESIGN.md
    /// §12.1). The fork point is located when the slave halts, by comparing entry CRCs with the
    /// upstream. `Err(Config)` if the slave is not halted `Diverged`, or the fork point is not
    /// known (the upstream could not be asked); `Err(Gap)` if local retention already removed
    /// part of the suffix.
    pub fn diverged_entries(&self) -> Result<Vec<LogEntry>> {
        if self.inner.state() != SlaveState::Halted(HaltReason::Diverged) {
            return Err(Error::Config("the slave is not halted Diverged".into()));
        }
        let fork = self.inner.store.diverged_at()?.ok_or_else(|| {
            Error::Config(
                "the fork point is not known: the upstream could not be asked when the slave halted"
                    .into(),
            )
        })?;
        self.inner.store.read_entries(fork, usize::MAX, usize::MAX)
    }

    pub fn watch_state(&self) -> watch::Receiver<SlaveState> {
```

with the import:

```rust
use crate::types::{CfSpec, Direction, IterMode, Role, Seq, ValueMeta};
```

with:

```rust
use crate::types::{CfSpec, Direction, IterMode, LogEntry, Role, Seq, ValueMeta};
```

and in the docs of `verify_now`:

```rust
    /// listener's `on_verify_complete`; under `MismatchPolicy::Rebootstrap` a report with
    /// mismatches makes the slave bootstrap.
```

with:

```rust
    /// listener's `on_verify_complete`; under `MismatchPolicy::Rebootstrap` a report with
    /// mismatches makes the slave bootstrap, under `MismatchPolicy::Repair` it takes the
    /// reported keys from the upstream.
```

In `proto/replication.proto` **and** the PROTOCOL.md §14 block (byte-identical), replace:

```proto
  string   node_state               = 8;   // "ready", "bootstrapping" or "halted:<Reason>"
```

with:

```proto
  string   node_state               = 8;   // "ready", "bootstrapping", "halted:<Reason>" or "repairing:<seq>"
```

In PROTOCOL.md §14:

```proto
  string   node_state               = 8;   // "ready", "bootstrapping" or "halted:<Reason>"
```

with:

```proto
  string   node_state               = 8;   // "ready", "bootstrapping", "halted:<Reason>" or "repairing:<seq>"
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
| `REJECT` / `DIVERGED` | Divergence policy *Halt* (default) → halt `Diverged`. *Repair* → repair from a verify session (`EntryCrcs` to find the fork, `FetchKeys`/`FetchRange` for the keys the suffix touched; §9.2), falling back to a bootstrap. *Rebootstrap* → bootstrap. |
```

Run: `cargo build && cargo test --lib`
Expected: builds without warnings; all unit tests PASS (224), including `store::repair`, `slave::repair` (2 each), `reject_rows`, `status_rows` and `node_state_round_trips_and_only_data_halts_persist`.

- [ ] **Step 6: Write the end-to-end tests**

The recording listener counts rewinds and restarts its seq-order check there.

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

add:

```rust
    pub fn entries(&self) -> Vec<Seq> {
```

with:

```rust
    pub fn rewinds(&self) -> usize {
        self.rewinds.load(Ordering::SeqCst)
    }

    pub fn entries(&self) -> Vec<Seq> {
```

and in `impl ReplicationListener for RecordingListener`:

```rust
    fn on_verify_complete(&self, report: &VerifyReport) {
        self.verifies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(report.clone());
    }
```

with:

```rust
    fn on_verify_complete(&self, report: &VerifyReport) {
        self.verifies
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .push(report.clone());
    }

    fn on_rewind(&self, last_seq: Seq) {
        self.last.store(last_seq, Ordering::SeqCst);
        self.rewinds.fetch_add(1, Ordering::SeqCst);
    }
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

/// A master with 50 keys and a converged slave `d` that received them through Tail, so its
/// own log holds the entries before any later fork point (repair needs entry `F - 1` on both
/// sides).
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
    let (mut cfg, listener) = c.slave_config("d", a.serve_addr().unwrap());
    tweak(&mut cfg);
    let d = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    for i in 0..50u32 {
        a.put("c", &key(i), b"base").unwrap();
    }
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
```

In `tests/repoint.rs`, in `divergence_is_refused_unless_data_loss_is_accepted`, replace:

```rust
    let (d, ld) = c.slave("d", a.serve_addr().unwrap()).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d)]).await;
    let b_cfg = c.slave_config("b", a.serve_addr().unwrap()).0;
```

with:

```rust
    let (d, ld) = c.slave("d", a.serve_addr().unwrap()).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    // D tails entry 2 rather than bootstrapping past it: repair needs the entry before the
    // fork point on both sides.
    wait_state(&d, "d streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d)]).await;
    let b_cfg = c.slave_config("b", a.serve_addr().unwrap()).0;
```

Further down in the same test, `RepointDivergence::Repair` now repairs (the Plan 2 test asserted `Unsupported`; the `Rebootstrap` path stays covered by `consent_to_lose_data_ends_when_its_repoint_settles`). Replace the end of the test:

Replace:

```rust
    let repair = RepointOptions {
        on_divergence: RepointDivergence::Repair {
            accept_loss: AcceptDataLoss,
        },
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
    b.put("c", b"after", b"4").unwrap();
    assert_converged(&b, &[("d", &d)]).await;
    assert_eq!(
        d.get("c", b"only on d").unwrap(),
        None,
        "the divergent entries were discarded"
    );
    assert_eq!(ld.bootstraps(), 2);
}
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
        on_divergence: RepointDivergence::Repair {
            accept_loss: AcceptDataLoss,
        },
    };
    assert_eq!(
        repoint(&d, b_up, repair).await.unwrap(),
        RepointOutcome::Repaired {
            discarded: fork..=d_last
        }
    );
    b.put("c", b"after", b"4").unwrap();
    assert_converged(&b, &[("d", &d)]).await;
    assert_eq!(
        d.get("c", b"only on d").unwrap(),
        None,
        "the divergent entries were discarded"
    );
    assert_eq!(d.get("c", b"k").unwrap(), Some(b"1".to_vec()));
    assert_eq!(ld.bootstraps(), 1, "repaired without a bootstrap");
    assert_eq!(ld.rewinds(), 1);
}
```

Append to `tests/failpoints.rs` (a crash right after the rewind):

```rust
/// A slave killed right after its repair committed: it stays `NotReady` until replay passes
/// the upstream's snapshot seq (also across the restart), then converges, without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_repair_then_converge() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, first) = c.slave_config("d", a.serve_addr().unwrap());
    cfg.on_divergence = replicated_rocksdb::DivergencePolicy::Repair;
    let d = Slave::open(cfg.clone()).await.unwrap();
    // Tailed, not bootstrapped: D's log holds the entries before the fork point.
    assert_converged(&a, &[("d", &d)]).await;
    for i in 0..20u8 {
        a.put("c", &[i], b"base").unwrap();
    }
    assert_converged(&a, &[("d", &d)]).await;
    wait_until("streaming", || d.status().state == SlaveState::Streaming).await;

    // D receives entries the restored master will not have.
    let a = c.restart_master_after_backup(a, "a", "a-backup").await;
    a.put("c", &[1], b"lost").unwrap();
    a.delete("c", &[2]).unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    d.shutdown().await.unwrap();
    let port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    support::restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    mcfg.bump_epoch_on_open = true;
    let a = Master::open(mcfg).await.unwrap();
    for i in 0..5u8 {
        a.put("c", &[100 + i], b"new").unwrap(); // so replay must run past the rewind point
    }

    fail::cfg("slave-repair-after-rewind", "1*return").unwrap();
    let listener = std::sync::Arc::new(support::RecordingListener::default());
    let mut frozen_cfg = cfg.clone();
    frozen_cfg.listener = Some(listener.clone());
    let d = Slave::open(frozen_cfg).await.unwrap();
    wait_until("frozen right after the rewind", || listener.rewinds() == 1).await;
    assert!(
        matches!(d.get("c", &[0]), Err(replicated_rocksdb::Error::NotReady)),
        "mixed state is never served"
    );
    d.kill().await.unwrap();
    fail::remove("slave-repair-after-rewind");

    // The persisted `repairing:<S>` keeps it NotReady after the restart until replay reaches S.
    let d = Slave::open(cfg).await.unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    assert_eq!(d.get("c", &[1]).unwrap(), Some(b"base".to_vec()));
    assert_eq!(first.bootstraps(), 1, "no bootstrap at any point");
    assert!(
        d.verify_now(replicated_rocksdb::VerifyMode::Full)
            .await
            .unwrap()
            .is_clean()
    );
    scenario.teardown();
}
```

- [ ] **Step 7: Run the tests**

Run: `cargo test --test repair --test repoint`
Expected: PASS (6 tests in `repair`, 9 in `repoint`).

Run: `cargo test --features failpoints --test failpoints crash_mid_repair_then_converge`
Expected: PASS.

- [ ] **Step 8: Document it**

In DESIGN.md §3.2, replace the `node_state` row:

```markdown
| `node_state` | slave | text: `ready`, `bootstrapping` or `halted:<Reason>` (data-related reasons only); absent means `ready`. Persisted, see §16.3 |
```

with:

```markdown
| `node_state` | slave | text: `ready`, `bootstrapping`, `halted:<Reason>` (data-related reasons only) or `repairing:<seq>` (§12.1); absent means `ready`. Persisted, see §16.3 |
| `diverged_at` | slave | first seq of the local divergent suffix, recorded when the slave halts `Diverged` (`Slave::diverged_entries`); cleared by a repair or a bootstrap |
```

In DESIGN.md §12, replace:

```markdown
- Every report is stored in `SlaveStatus.last_verify` and passed to `on_verify_complete`. Under `MismatchPolicy::Rebootstrap`, a report with mismatches ends the current Tail stream and the slave bootstraps at once (a halted slave stays halted).
- `MismatchPolicy::Report` (default) or `MismatchPolicy::Rebootstrap`. There is no automatic range repair in v1.
```

with:

```markdown
- Every report is stored in `SlaveStatus.last_verify` and passed to `on_verify_complete`. Under `MismatchPolicy::Rebootstrap`, a report with mismatches ends the current Tail stream and the slave bootstraps at once (a halted slave stays halted). Under `MismatchPolicy::Repair`, it ends the stream and the reported keys and ranges are repaired from the upstream (§12.1); a `CfSpec` mismatch bootstraps instead.
- `MismatchPolicy::Report` (default), `MismatchPolicy::Rebootstrap` or `MismatchPolicy::Repair`.
```

In DESIGN.md, insert before `## 13. Identity, epoch, versioning`:

```
## 13. Identity, epoch, versioning
```

insert:

```
### 12.1 Repair

A slave can take **only the keys that differ** from its upstream instead of bootstrapping. Repair runs in the driver between sessions (no entry is applied meanwhile), over a verify session (PROTOCOL.md §9.2) pinned to the upstream's snapshot at `S`.

- **Divergence** (`REJECT` / `DIVERGED` under `DivergencePolicy::Repair`, or a `repoint` with `RepointDivergence::Repair { accept_loss }`):
  1. Find the fork `F`, the first seq whose entry differs, by binary search over `EntryCrcs` between the local `first_retained_seq` and `min(last_seq, S)`. Entry `F − 1` must be held by both sides with the same `crc32c`.
  2. Read the local suffix `F..=L` and collect what it touched: keys of `Put` and `Delete`, ranges of `DeleteRange`.
  3. Fetch those keys (`FetchKeys`) and every key of those ranges (`FetchRange`) as the upstream's whole envelopes at `S`.
  4. One synced `WriteBatch`: clear the ranges, write or delete the keys with the fetched envelopes, delete changelog entries `F..=L`, set `last_seq = F − 1` and `last_entry_crc` = CRC of local entry `F − 1`, the upstream's identity and epoch history, and `node_state = repairing:<S>` (`ready` when `S < F`). Downstream streams are reset and a pending verify capture is cancelled.
  5. Tail from `F`. Replaying `F..=S` (absolute ops only) over the repaired keys gives exactly the upstream's state at `S`; every other key was never touched by the suffix and already held its state at `F − 1`. Until `last_seq` reaches `S`, reads, snapshots and downstream handshakes answer `NotReady`, also after a restart (`repairing:<S>` is persisted). Then `node_state = ready`.
- **Verify mismatches** (`MismatchPolicy::Repair`): the reported keys and ranges are fetched at a fresh snapshot `S`. When `last_seq > S`, entry `S` must match and the local entries after `S` are dropped (they came from this upstream and are fetched again); the slave is `NotReady` until it is back at `max(last_seq, S)`.
- **Fallback to a full bootstrap:** a CF operation in the suffix, a suffix entry this slave no longer retains, a fork entry the upstream no longer holds, a CF that differs between the sides (or a `CfSpec` mismatch), or more than `SlaveConfig::repair_max_bytes` (default 64 MiB) of keys and envelopes to fetch. A transient failure (upstream unreachable, session lost) retries with backoff.
- **Salvage.** A slave that halts `Diverged` locates and persists its fork point (`__meta.diverged_at`, best effort); `Slave::diverged_entries()` returns the local entries `F..=L`.
- The listener's `on_rewind(F − 1)` is called after the batch; `on_entry` then continues at `F`. `RepointOutcome::Repaired { discarded: F..=L }` is what `repoint` returns.
```

In DESIGN.md §13, replace:

```markdown
  - `on_divergence: DivergencePolicy` — `Halt` (default) halts with `Diverged`; `Rebootstrap` bootstraps. Applies to `DIVERGED` on reconnects; an explicit `Slave::repoint` uses `RepointOptions::on_divergence` instead (§13.1).
```

with:

```markdown
  - `on_divergence: DivergencePolicy` — `Halt` (default) halts with `Diverged`; `Repair` repairs only the divergent suffix (§12.1); `Rebootstrap` bootstraps. Applies to `DIVERGED` on reconnects; an explicit `Slave::repoint` uses `RepointOptions::on_divergence` instead (§13.1).
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

````markdown
    fn on_verify_complete(&self, report: &VerifyReport) {}
}
```
````

with:

````markdown
    fn on_verify_complete(&self, report: &VerifyReport) {}
    fn on_rewind(&self, last_seq: Seq) {}  // a repair (§12.1) moved the position back; on_entry continues at last_seq + 1
}
```
````

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

In DESIGN.md §16.3, add a row:

```markdown
| `halted:<Reason>`, data-related reasons only | `Halted(reason)`. The slave does not contact its upstream until `clear_halt`. |
```

with:

```markdown
| `halted:<Reason>`, data-related reasons only | `Halted(reason)`. The slave does not contact its upstream until `clear_halt`. |
| `repairing:<seq>` | `Connecting` or `Streaming`, reads `NotReady` until `applied_seq` reaches `seq` (§12.1), also after a restart. |
```

In DESIGN.md §18, add a row:

```markdown
| Verify session timeout | 5 min |
```

with:

```markdown
| Verify session timeout | 5 min |
| Repair fetch bound (`repair_max_bytes`) | 64 MiB; a larger repair bootstraps |
```

In DESIGN.md §21, add after the corruption bullet:

```markdown
- Injected corruption on a slave is detected by `Full` verify and localised to the key. A key that exists only on the upstream, between the slave's chunk boundaries, is found.
```

with:

```markdown
- Injected corruption on a slave is detected by `Full` verify and localised to the key. A key that exists only on the upstream, between the slave's chunk boundaries, is found.
- **Repair:** a diverged slave with puts, deletes and range deletes in its suffix repairs without a bootstrap and matches under `Full` verify; `repoint(.., Repair { AcceptDataLoss })` returns `Repaired`; a CF op in the suffix, a trimmed local suffix and an upstream without the fork point fall back to a bootstrap; a crash right after the rewind stays `NotReady` until replay passes `S`, then converges; `MismatchPolicy::Repair` fixes corrupted, missing and extra keys; `diverged_entries` returns the suffix of a halted slave.
```

In DESIGN.md §22, replace:

```markdown
- Divergence is detected by CRC comparison on connect and by rejecting slaves ahead of the master. It is not repaired automatically: an explicit `repoint` refuses by default and discards entries only with `AcceptDataLoss`.
```

with:

```markdown
- Divergence is detected by CRC comparison on connect and by rejecting slaves ahead of the master. It is resolved only by policy (`DivergencePolicy::Repair` or `Rebootstrap`) or with explicit consent: a `repoint` refuses by default and discards entries only with `AcceptDataLoss`. Repair rewrites only the keys the divergent suffix touched (§12.1) and falls back to a bootstrap whenever it cannot prove the result.
```

and delete the future-work item (it is `MismatchPolicy::Repair` now):

```markdown
- Automatic range repair after a verify mismatch.
```

with:

```markdown

```

- [ ] **Step 9: Run everything and commit**

Run: `cargo test && cargo test --features failpoints && cargo clippy --all-targets -- -D warnings`
Expected: PASS, no warnings.

```bash
git add src tests proto DESIGN.md PROTOCOL.md
git commit -m "feat: divergence repair, MismatchPolicy::Repair and diverged_entries

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: DESIGN.md §21 sweep — every listed test exists, and the missing ones are added

**Files:**
- Modify: `tests/failure_resume.rs`, `tests/replication.rs`, `tests/failpoints.rs`, `tests/promotion.rs`, `tests/support/mod.rs`

**Interfaces:**
- Consumes: Task 3 `Slave::verify_now`, `VerifyMode`; Plan 2 harness (`Cluster::{new, master, master_config, slave, slave_config, restart_master, path, clock}`, `assert_converged`, `wait_state`, `wait_until`, `RecordingListener`, `RawDb`, `WAIT`), `Slave::{snapshot, wait_for_seq, kill (feature failpoints), shutdown}`, `serial()` in `tests/failpoints.rs`, the locals `b`, `cn`, `e`, `f` at the end of `promotion_end_to_end` in `tests/promotion.rs`; `LogApi::{last_seq, first_retained_seq, entries_since}`.
- Produces: tests only, plus `RawDb::last_seq() -> u64` in the harness.

The coverage map below is the sweep. Every DESIGN.md §21 item (including the repair bullet Task 8 adds) names the test(s) that cover it; the four marked **new** are added in this task.

| DESIGN.md §21 item | Test(s) |
|---|---|
| A→B convergence under concurrent writes; verify `Full` matches | `tests/verify.rs::verify_pins_the_snapshot_while_concurrent_writes_continue` (Task 3) |
| A→B→C chain, C bootstrapping from B while A writes and creates/drops CFs; A, B, C identical | `tests/chain.rs::a_chain_replicates_everything_and_bootstraps_from_the_middle` (Plan 2); `tests/integrity.rs::every_link_of_a_chain_verifies_against_its_own_upstream` (Task 4) |
| Acks and write concern: `replicas()` follows acks and lag; bad acks end the stream; `NotEnoughReplicas`; `AckTimeout`; promote the in-sync replica after losing the master | `src/server/tests.rs::{acks_drive_replica_status_and_in_sync, ack_violations_end_the_stream_with_invalid_argument}`, `src/server/replicas.rs::acks_must_be_monotonic_and_not_above_what_was_sent`, `tests/write_concern.rs` (all four), `tests/failpoints.rs::a_replica_that_stops_acking_times_the_write_out` (Plan 2) |
| Slave crash and restart at random points: no gaps, no duplicates, listener order preserved | `tests/failpoints.rs::crash_after_apply_resumes_without_gap_or_duplicate`, `crash_mid_bootstrap_restarts_the_bootstrap` (Plan 2); **new** `tests/failpoints.rs::random_kills_while_writing_never_lose_or_repeat_an_entry` |
| C ahead of B after B crashes unsynced: C waits, then converges | `tests/failure_resume.rs::a_downstream_ahead_of_its_upstream_waits_and_converges` (Plan 2); `tests/integrity.rs::a_slave_ahead_of_the_upstream_snapshot_abandons_the_run` (Task 4) |
| Retention trims past a stopped slave; it bootstraps again on restart | `tests/rebootstrap.rs::retention_gap_triggers_a_rebootstrap_that_converges` (Plan 2) |
| Bootstrap lease under `max_entries = 1` | `tests/failpoints.rs::bootstrap_lease_survives_aggressive_retention`, `src/server/tests.rs::bootstrap_lease_holds_retention_until_tail_presents_it` (Plan 2) |
| Quiet master with every changelog entry expired: `last_seq` kept, slaves resume | `src/store/retention.rs::quiet_store_can_trim_everything_but_keeps_position` (Plan 1); **new** `tests/failure_resume.rs::a_quiet_master_with_an_empty_log_keeps_last_seq_and_slaves_resume` |
| TTL: visible expiry before reaping, reaper `Delete` replicates, overwrite clears TTL, stale index cleaned, several entries past `max_deletes_per_entry` | `tests/master_ttl.rs` (Plan 1: `expired_values_are_hidden_then_reaped_by_a_logged_delete`, `overwrite_clears_ttl_and_leaves_a_stale_index_entry`, `range_deleted_and_dropped_cf_entries_are_stale`, `large_rounds_split_into_bounded_entries`, `ttl_on_empty_and_nul_keys_is_reaped`); `tests/replication.rs::ttl_values_replicate_and_the_reapers_delete_replicates` (Plan 2) |
| Divergence: restore without bump → `Diverged`; with `bump_epoch_on_open` → `EPOCH_ADVANCED` / `DIVERGED` per position | `tests/rebootstrap.rs::master_restored_without_epoch_bump_makes_ahead_slaves_diverge`, `restored_master_with_epoch_bump_keeps_behind_slaves_and_rejects_ahead_ones` (Plan 2) |
| Promotion: siblings continue or diverge per policy, old master `STALE_EPOCH`, writes and TTLs on the new master, **`Full` verify on every link** | `tests/promotion.rs::promotion_end_to_end`, `the_old_master_is_refused_with_stale_epoch_and_the_client_retries`, `promote_checked_refuses_when_a_reachable_peer_is_later`, `demote_undoes_a_wrong_promotion_with_zero_loss`, `tests/repoint.rs::failover_runbook_end_to_end` (Plan 2); **new** verify block appended to `promotion_end_to_end` |
| Failure and resume: master, mid-chain and leaf restarts; crash mid-bootstrap and mid-apply; dropped connections | `tests/failure_resume.rs::restarts_of_master_mid_chain_and_leaf_resume_without_bootstrap`, `tests/failpoints.rs::{crash_mid_bootstrap_restarts_the_bootstrap, crash_after_apply_resumes_without_gap_or_duplicate, connection_drop_mid_tail_resumes}` (Plan 2) |
| Edge-case data on every path (write, reopen, tail, bootstrap, verify, promotion) | `tests/master_write.rs::edge_case_keys_and_values_through_the_master`, `src/store/apply.rs::nul_empty_and_large_keys_and_values_survive_reopen` (Plan 1); `tests/replication.rs::edge_case_data_through_tail_and_bootstrap`, `tests/chain.rs::a_chain_replicates_everything_and_bootstraps_from_the_middle`, `tests/promotion.rs::promotion_end_to_end` (Plan 2); `tests/integrity.rs::edge_case_data_verifies_and_its_corruption_is_found` (Task 4) |
| Epoch bump: streams end `UPSTREAM_RESET`; seq continues; a `Rebootstrap` slave converges | `tests/rebootstrap.rs::live_epoch_bump_resets_streams_and_slaves_continue`, `src/server/tests.rs::bump_epoch_ends_streams_with_upstream_reset` (Plan 2); `src/server/verify.rs::an_epoch_bump_ends_verify_streams_and_sessions` (Task 2) |
| Identity change → `Halted`; protocol mismatch → `REJECT`; loop → `LoopDetected` | `tests/rebootstrap.rs::identity_change_is_refused_persistently_until_clear_halt_rebootstraps`, `src/server/tests.rs::version_mismatch_is_answered_in_the_handshake_body`, `tests/chain.rs::{a_loop_is_detected_and_the_halt_is_not_persisted, a_slave_pointed_at_itself_halts_loop_detected}` (Plan 2) |
| Halt persistence; `clear_halt(Rebootstrap)` recovers | `tests/failure_resume.rs::a_log_gap_halt_persists_across_restart_until_cleared`, `tests/rebootstrap.rs::identity_change_is_refused_persistently_until_clear_halt_rebootstraps` (Plan 2) |
| CF drop while a `ReadSnapshot` is open: still readable; physical drop after release | `src/store/read.rs::snapshot_keeps_dropped_cf_readable_until_released`, `tests/master_retention.rs::maintenance_drops_released_cfs` (Plan 1); `tests/replication.rs::a_cf_held_by_a_snapshot_is_retried_not_halted` (Plan 2); **new** `tests/replication.rs::a_replicated_cf_drop_waits_for_a_slave_snapshot` |
| Unregistered comparator: master rejects; slave halts `SpecMismatch`, recovers once registered | `tests/rebootstrap.rs::missing_comparator_halts_until_registered_and_cleared` (Plan 2) |
| Token auth accept/reject; per-method authorisation; token without TLS refused | `tests/security.rs` (Plan 2: `tokens_over_tls_accept_any_valid_secret_and_reject_others`, `bootstrap_can_be_denied_per_method`, `token_authentication_on_plaintext_is_refused_unless_allowed`, `mutual_tls_allow_list_by_certificate_subject`) |
| Entry over `max_entry_bytes` rejected; a large valid entry passes through the chain | `tests/master_write.rs::max_entry_bytes_is_enforced_at_the_boundary` (Plan 1); `tests/chain.rs::a_chain_replicates_everything_and_bootstraps_from_the_middle` (the ~16 MiB edge-case value through A→B→C, Plan 2) |
| Injected corruption found by `Full` verify and localised to the key; an upstream-only key between chunk boundaries is found | `tests/integrity.rs::{a_corrupted_value_is_found_by_full_verify_only, a_key_only_upstream_between_two_chunk_boundaries_is_found, keys_deleted_on_disk_are_reported_in_both_modes, extra_keys_on_disk_are_reported_with_no_upstream_value, a_custom_comparator_cf_verifies_and_localises_damage}` (Task 4) |
| Repair: divergence repaired without bootstrap; fallbacks; crash after the rewind; verify-mismatch repair; `diverged_entries` | `tests/repair.rs` (all six), `tests/failpoints.rs::crash_mid_repair_then_converge`, `tests/repoint.rs::divergence_is_refused_unless_data_loss_is_accepted` (Task 8) |
| Wire codec matches PROTOCOL.md §15 vectors | `src/codec/entry.rs::vector_v1_ops_bytes_and_crc`, `src/codec/envelope.rs::vector_v2_envelopes`, `src/codec/digest.rs::vector_v3_digests` (Plan 1); `src/verify/range.rs::range_hashes_match_the_v3_vectors`, `src/verify/scan.rs::a_snapshot_range_hashes_to_the_v3_vectors` (Task 1) |

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
Expected: PASS (it exercises Plan 1 retention and Plan 2 handshake rule 13; if it fails, the bug is there).

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

The harness reads a stopped node's durable position, so the test knows exactly where each restart must resume.

In `tests/support/mod.rs`, in `impl RawDb`, insert before `get`:

```rust
    /// The stored envelope.
    pub fn get(&self, cf: &str, key: &[u8]) -> Option<Vec<u8>> {
```

insert:

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
```

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

- [ ] **Step 5: Run everything and lint**

Run: `cargo fmt --check && cargo test && cargo test --features failpoints`
Expected: PASS: every unit, integration and doc test, with and without the feature.

Run: `cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings && cargo doc --no-deps`
Expected: no warnings; docs build.

Run: `cargo test --test proto_sync`
Expected: PASS (`proto/replication.proto` still matches PROTOCOL.md §14 after Tasks 7 and 8).

- [ ] **Step 6: Commit**

```bash
git add tests
git commit -m "test: complete the DESIGN.md §21 sweep (quiet master, slave snapshot CF drop, random kills, verify after promotion)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

