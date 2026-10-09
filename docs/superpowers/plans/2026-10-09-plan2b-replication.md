# Plan 2 (part 2: Tasks 9–15)

> **Split note:** Part 2 of Plan 2 (Tasks 9–15). Tasks 1–8 are in [2026-10-09-plan2-replication.md](2026-10-09-plan2-replication.md); its code is assumed implemented. The header below is copied verbatim from part 1.

# Plan 2 of 3 — Replication, Re-bootstrap, Failure/Resume and Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Plan 1's single-node master into a replicating system: a gRPC replication server (Handshake, a bidirectional Tail with Kafka-style fetch batching and replica acknowledgements, Bootstrap with retention leases, a read-only Status RPC) with token/mTLS auth and TLS, read-only `Slave`s that bootstrap, tail, acknowledge, chain, halt and recover by policy, an optional write concern on the master (in-sync replicas), `Slave::repoint`, and promotion with safety tooling (`promote_checked`, `admin::choose_promotion_candidate`, `Master::demote`) — every path proven by real multi-node tests over 127.0.0.1.

**Architecture:** One replication server (`src/server/`) runs over a `LogSource` trait that both `master::Inner` and `slave::Inner` implement; it holds the source weakly and every producer gives up on shutdown or reset, so a server never keeps a closed node's database open. Each Tail stream registers in a per-node replica registry (`server::replicas`) that records what was sent and what the replica acknowledged; the node reports it (`replicas()`, `Status`) and the master's writer waits on it for a write concern. A slave is a Plan 1 `Store` plus one driver task: connect → handshake → act on a pure policy table → bootstrap (a crash-safe install into fresh physical CFs) and/or tail (each entry applied through the same `Store::prepare`/`commit` as the master, unsynced unless `sync_writes`), acknowledging what it committed. Decision tables (handshake rules, slave policies) are pure functions with one unit test per row; everything else is tested end to end with nodes talking real gRPC on 127.0.0.1.

**Tech Stack:** Plan 1's stack (Rust 1.99, edition 2024, rocksdb 0.25.0, prost 0.14.4, tokio 1.53, …) plus tonic 0.14.6 (`tls-ring`), tonic-prost 0.14.6, tonic-types 0.14.6 (`ErrorInfo`), tokio-stream 0.1.19, subtle 2.6.1, async-trait 0.1.92, optional fail 0.5.1 (feature `failpoints`); dev rcgen 0.14.10.

**Spec:** [DESIGN.md](../../../DESIGN.md), [PROTOCOL.md](../../../PROTOCOL.md) (as of commit `f960d4e`, which added Tail fetch batching, PROTOCOL.md §7.1, §7.3–§7.5), [the Plan 2/3 design notes](../specs/2026-10-09-plan2-3-design-notes.md) (all of it except the "Plan 3: Verify" section) and §1–§4 and §6 of [the acks/failover/repair addendum](../specs/2026-10-09-failover-acks-repair.md) (commit `3b4b07e`), which wins over DESIGN/PROTOCOL until Tasks 6, 7, 11, 12 and 13 merge it. Read DESIGN.md §6, §10–§16 and PROTOCOL.md §3, §6–§8, §10–§12 before starting. This plan builds on [Plan 1](2026-10-09-plan1-storage-and-master.md). **The Plan 1 code under `src/` is the source of truth, not the Plan 1 document**: in particular `Store.db` is `Arc<DbCore>` (`Deref<Target = DB>`, never wrap the DB in another `Arc`), runtime-created CFs keep their options alive through `Store::retain_cf_options`, `OwnedSnapshot` reads only through `get_cf`/`iterator_cf` bound to `&self`, background tasks hold `Weak<Inner>`, `Store::commit` re-checks the seq, and `Store::ttl_due` takes a cursor. Every type and function the Plan 1 Interfaces blocks name exists, as implemented (`Store`, `Store::retain_cf_options`, `DbCore`, `prepare`/`commit`, `MetaSnapshot`, `CfRegistry`, `PhysCf`, `ReadCtx`, `ReadSnapshot`, `OwnedSnapshot`, `entries_after`, `guard`/`RetentionGuard`, `trim_changelog`, `drop_unreferenced_cfs`, `master::writer::Inner`, `Master`, `LogApi`, `Digest`, `envelope`, …) exists exactly as written there.

**Amendment (addendum §1–§4).** Tasks 1–5 were implemented before the addendum and are unchanged; **from Task 6 on, the code under `src/` written by Tasks 1–5 (commit `7bdd8fc`) is the source of truth**, and where Task 6 replaces a file it gives the whole file. Task 6 turns Tail into a bidirectional stream with acknowledgements and adds the `Status` RPC; Tasks 7–10 are the slave tasks (now acknowledging and with `sync_writes`); Task 11 adds the promotion tooling next to `promote`; Task 12 adds the write concern; Task 13 adds `repoint`; Tasks 14–15 are the security and failure/resume tasks.

**Series:**
- Plan 1: storage engine and master (done before this plan).
- **Plan 2 (this):** replication server with acks and Status, auth/TLS, `Slave` (bootstrap, tail, acks, halts, policies, listener), chaining, `replicas()`, write concern, promotion with safety tooling, `repoint`, failure/resume tests.
- Plan 3: Verify (sessions, range hashing, `ListRange`), divergence repair (the `repoint` `Repair` arm), `SlaveStatus::{lag_ms, last_verify}`, observability spans.

## Global Constraints

- Everything in Plan 1's Global Constraints still holds; in particular: absolute ops only (`Put`, `Delete`, `DeleteRange`, `CreateCf`, `DropCf`), no merge; mutation + `__changelog[seq]` + `__meta.last_seq` in one `WriteBatch`; `__meta.last_seq` is the source of truth; the master always fsyncs; `LogEntry.ops` is never re-encoded.
- **No read-modify-write API** (no `update`, CAS or merge). Nothing in this plan adds one; the rustdoc of `Master::put`, `Master::write`, `Master::get` and the crate docs keep the DESIGN.md §5.1 warning, and `Master::write_with` refers to it.
- **Data-loss-capable operations are explicit and refuse by default.** `repoint` never discards divergent entries unless the caller passes `RepointDivergence::{Repair, Rebootstrap} { accept_loss: AcceptDataLoss }`; `demote` refuses once the new epoch has entries and `promote_checked` when a reachable peer is later — and a refusal hands the untouched, still-running node back (`Err(DemoteRefused { error, master })`, `Err(PromoteRefused { error, slave })`), losing it only on a failure after the irreversible shutdown began, as their rustdoc says; `Error::AckTimeout` says (in its rustdoc and `WriteConcern`'s) that the write is committed and must not be retried blindly.
- **Slaves forward entries unchanged** (`seq`, `epoch`, `ts_ms`, `ops` bytes, `crc32c`). Identity always refers to the origin master.
- Slaves apply each entry with `Store::prepare` + `Store::commit(sync = SlaveConfig::sync_writes, default false)`; one entry is one `WriteBatch`. Bootstrap records are written unsynced; the final bootstrap batch is synced.
- `PROTOCOL_VERSION = 1` and `ENVELOPE_VERSION = 1` must match exactly. Handshake reports a mismatch in its body; every other RPC requires `x-rrdb-protocol-version` and fails with `FAILED_PRECONDITION / PROTOCOL_MISMATCH`.
- Request metadata on every call: `x-rrdb-protocol-version`, `x-rrdb-node-id`, and `authorization: Bearer <token>` with token auth.
- Every protocol failure is a gRPC status with `google.rpc.ErrorInfo { domain: "rrdb.replication.v1", reason: <Reason name> }`, including unexpected server failures (`INTERNAL` with reason `REASON_UNSPECIFIED`, `net::internal_status`); authentication failures (`UNAUTHENTICATED`, `PERMISSION_DENIED`) carry no `ErrorInfo`; malformed requests (including a Tail whose first message is not `start`, or an ack that goes backwards or past the last sent seq) are `INVALID_ARGUMENT` and the Verify stubs `UNIMPLEMENTED`.
- Tail is `rpc Tail(stream TailClientMessage) returns (stream TailMessage)`: the client sends `start` (a `TailRequest`) and then `ack { applied_seq }` after each applied batch (coalesced through a `watch`) and at least once per heartbeat; a stream that ends with no status is transient (back off, handshake again). PROTOCOL.md §14 stays byte-identical to `proto/replication.proto` (`tests/proto_sync.rs`).
- Tail fetch batching (PROTOCOL.md §7.5): `TailMessage` carries `EntryBatch { repeated LogEntry entries }` or `Heartbeat`; the server sends a pending batch when `min_batch_bytes = 0` and it is non-empty, when it reaches `min_batch_bytes`, when `max_wait_ms` has passed since its oldest entry became available, or when the next entry would exceed `max_batch_bytes` (a single larger entry goes alone); `max_wait_ms` ≤ `heartbeat_ms`, `max_batch_bytes` ≤ `ServeLimits::max_tail_batch_bytes` (default 4 MiB), 0 → 1 MiB. Catch-up is never slowed by `max_wait_ms`. Slaves apply each entry of a batch as its own atomic write. `SlaveConfig::fetch` defaults to `{ min_batch_bytes: 0, max_wait: 100 ms, max_batch_bytes: 1 MiB }`.
- Decode limits: server requests 4 MiB; client Tail `max(max_entry_bytes, max_batch_bytes) + 64 KiB`; client Bootstrap `max(max_entry_bytes, 16 MiB) + 64 KiB`; `bootstrap_chunk_bytes` ≤ 16 MiB.
- Defaults (DESIGN.md §18): heartbeat 5 s, idle timeout 3 × heartbeat, reconnect 100 ms → 30 s ×2 with full jitter (reset after 60 s of streaming), bootstrap lease TTL 15 min, 2 concurrent bootstraps, 4 verify sessions, chunk 1 MiB, 10 failed authentications per minute per address, `replica_lag_time_max` 10 s, write concern `{ min_insync_replicas: 0, ack_timeout: 5 s }`, `sync_writes` false, `IdentityPolicy::Refuse`, `DivergencePolicy::Halt`, `GapPolicy::Rebootstrap`, `RepointDivergence::Refuse`.
- Token authentication on a plaintext listener is refused unless `allow_plaintext_token`. Tokens are compared in constant time (`subtle`) and never logged or printed.
- Data-related halts (`IdentityChanged`, `Diverged`, `LogGap`, `Corrupt`, `SpecMismatch`) are persisted in `__meta.node_state` and survive restart; configuration halts (`ProtocolMismatch`, `EnvelopeMismatch`, `AuthRejected`, `LoopDetected`) are not.
- TTL stays master-driven: slaves never reap and have no `__ttl_index`; promotion rebuilds it.
- Plan 1 code conventions apply to new code: every `create_cf` is followed by `Store::retain_cf_options(opts)`; snapshot iterators are created and consumed where the `ReadSnapshot` is owned (the bootstrap job iterates inside the blocking closure that owns it); periodic background tasks (master and slave maintenance) hold `Weak<Inner>` and upgrade only for one tick; the slave driver holds `Weak<Inner>` while it waits (halted, backing off) and a strong reference only for one session, and Tail producers hold the source for one stream — sessions and streams are cancelled by the shutdown signal that `shutdown()` and `Drop` send, so dropping a node releases its database. RocksDB I/O that may fsync (halts, epoch adoption, meta writes) runs on blocking threads (`background::blocking`), never on async workers. A write waiting for acknowledgements blocks its own thread on a `Condvar` (`ReplicaRegistry::wait_until`), never an async worker and never while holding the writer lock. A repoint cancels a session only at network awaits (`driver::unless_repointed`), never in the middle of a store write.
- Every epoch history received from upstream (handshake `EPOCH_ADVANCED`, bootstrap header) is validated: non-empty, first record `{1, 1}`, epochs strictly increasing, start seqs non-decreasing, last epoch equal to the identity's epoch; otherwise the slave halts `Corrupt`.
- On a slave, a failed `Store::prepare` or `Store::commit` halts the slave (`SpecMismatch` for an unknown comparator, otherwise `Corrupt`); the same entry is never retried in-process.
- The Verify RPCs exist (generated service) but return `UNIMPLEMENTED` until Plan 3.
- Tests that matter are **real**: nodes in one process, real gRPC over `127.0.0.1` with port 0, data compared exactly with the harness `dump`, bootstraps counted with the `RecordingListener`. Failpoint tests live only in `tests/failpoints.rs`, run serially, and need `--features failpoints`. Tests never use sleep as synchronisation, and every wait is bounded (server unit tests 10 s through `test_client::{within, eventually}`; the integration harness `support::WAIT`); `repoint` calls in tests are wrapped in a bounded timeout.
- `cargo clippy --all-targets -- -D warnings` and `cargo clippy --all-targets --features failpoints -- -D warnings` pass at the end of Task 6 and of every task from Task 8 on. (Task 7 adds slave building blocks that only Task 8 uses, so dead-code warnings are expected in between.)
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A client that stops reading must not pin the database.** Tonic serves each connection on its own spawned task, so a Tail or Bootstrap producer blocked on a full channel would keep `Arc<dyn LogSource>` (and the RocksDB handle) alive forever and make `shutdown()` time out. Every producer send also waits on shutdown and reset; after a reset the bootstrap job drops its snapshot and permit **before** delivering its final status to a client that may never read it. (Tests `shutdown_with_unread_streams_releases_the_database` in Task 5, `a_reset_releases_a_stalled_bootstraps_snapshot_and_slot` in Task 6.)
2. **Acks are untrusted input.** An ack that goes backwards or above the last sent seq ends the stream with `INVALID_ARGUMENT`; the ack reader is its own task, so a client that never acks cannot stall the producer, and closed streams leave the registry. (Tests `ack_violations_end_the_stream_with_invalid_argument`, `the_first_tail_message_must_be_start` in Task 6.)
3. **Write concern semantics.** `NotEnoughReplicas` consumes no seq; `AckTimeout` means *committed but not acknowledged in time*; the writer lock is never held while waiting. (Tests in `master::write_concern` and `tests/write_concern.rs`, Task 12; `a_replica_that_stops_acking_times_the_write_out`, Task 15.)
4. **Repoint never discards data implicitly** and never cancels a store write half-way: a diverged slave stays halted with its data readable unless the caller passes `AcceptDataLoss`; a bootstrap abandoned by a repoint restarts from scratch against the new upstream. (Tests in `tests/repoint.rs`, Task 13; `repoint_during_bootstrap_restarts_it_against_the_new_upstream`, Task 15.)
5. **A slave pointed at its own address** (or repointed at itself or its downstream) must halt with `LoopDetected` (rule 3), not loop or deadlock. (Tests `a_slave_pointed_at_itself_halts_loop_detected` in Task 10, `repoint_refuses_loops` in Task 13.)
6. **Re-bootstrap while a reader holds a snapshot of a CF with the same physical name** (`u:{name}:{create_seq}` repeats when re-bootstrapping from the same origin): the snapshot keeps its old view, the new data is correct, and the live CF is never physically dropped afterwards. (Test `a_cf_held_by_a_snapshot_is_taken_over_without_disturbing_the_snapshot` in Task 7.)
7. **Promotion or demotion while a `ReadSnapshot` is alive** keeps the DB open: it must fail cleanly and leave a directory that reopens. A refused plain `promote` shuts the slave down (not leak tasks holding the DB); a refused `promote_checked` or `demote` returns the node still running, and only failures after its shutdown began lose the handle. (Tests `promotion_is_refused_without_data_and_needs_released_snapshots`, `demote_is_refused_once_the_new_epoch_has_entries` in Task 11.)
8. **A restarted slave whose bootstrap was interrupted** must never serve the partial data: reads answer `NotReady` until the bootstrap is redone from scratch, and the leftover physical CFs (whose comparators must be known at open) are dropped. (Test `crash_mid_bootstrap_restarts_the_bootstrap` in Task 15, store-level `crash_mid_bootstrap_reopens_and_the_next_reset_drops_leftovers` in Task 7.)

## Design deltas this plan introduces

Each owning task updates DESIGN.md or PROTOCOL.md with the exact text given there.

- **`MasterConfig.serve: Option<ServeConfig>`** (Task 5, amends DESIGN.md §16): `None` is an embedded, non-replicating master. `Master::serve_addr()` and `Slave::serve_addr()` return the bound address (tests bind port 0).
- **`__meta.node_state` is text** `ready` | `bootstrapping` | `halted:<Reason>`, and **`__meta.bootstrap_cfs`** lists physical CFs created by an unfinished bootstrap so the next open knows their comparators (Task 7, DESIGN.md §3.2, §10, §16.3).
- **A server treats its own `last_entry_crc` as the CRC of its entry `last_seq`** even when its log no longer holds that entry (right after a bootstrap, or after retention emptied the log), for handshake rules 9 and 12 and Tail's `prev_crc32c` check (Task 2, PROTOCOL.md §6.3 notes).
- **`LogSource`** is the trait from the design notes (Task 2, replaces the sketch in DESIGN.md §11); Task 6 adds `node_state()` and `upstream()` with defaults for the `Status` RPC.
- **Bootstrap decode limit** is `max(max_entry_bytes, 16 MiB) + 64 KiB` and servers keep `bootstrap_chunk_bytes` ≤ 16 MiB, because a client cannot know the server's chunk size (Task 4, PROTOCOL.md §11.4 and §12).
- **Auth additions** (Task 4, DESIGN.md §14): `AllowMethods` (per-method restriction around any authenticator), `NoCredentials`, `ServeLimits::max_auth_failures_per_minute`; the mTLS peer id is the client certificate's subject CommonName. Task 6 adds `Method::Status` (in `allow_all_methods`).
- **Slave config and status additions** (Task 7, DESIGN.md §16): `SlaveConfig::{heartbeat, connect_timeout, clock, sync_writes}`, `SlaveStatus::listener_failed`. `VerifyConfig`, `lag_ms` and `last_verify` come with Plan 3.
- **Bootstrap lease hand-over** (Task 5, PROTOCOL.md §7.2): the Tail that presents the lease takes it over and releases it once it has read the entries after the snapshot (or when it ends), instead of releasing it the moment the stream is established — otherwise `max_entries = 1` retention can trim `S + 1` before the producer's first read.
- **Bidirectional Tail with acks, replica status, `Status` RPC** (Task 6, addendum §1 and §4; PROTOCOL.md §2, §3.3, §6.5, §7, §7.1, §7.3, §7.6, §10, §11.2, §11.4, §12, §14; DESIGN.md §11, §14, §16, §17, §18, §19). Decisions: `ReplicaStatus::lag_ms` is `Option<u64>` — `None` when the first unacknowledged entry is no longer retained on the serving node, so no timestamp is known; `in_sync` also requires the replica to follow the node's current identity; `ServeLimits::replica_lag_time_max` (default 10 s) lives with the other server limits; the replica registry is owned by the node (`Inner.replicas`) and handed to `server::start`, so a node can report and wait on it; `Status.role` is the text `"master"`/`"slave"`, `node_state` is `__meta`'s text form, and `upstream` is empty on the master.
- **Clean end of a Tail stream is transient** (Task 6, PROTOCOL.md §7.3, §11.2, §11.4): a stream that ends with no status is treated like a lost connection.
- **Bootstrap jobs release their snapshot and slot before their final status** (Task 6 fix of Task 5's `server::bootstrap`): after a reset the job used to wait in `end_stream` for a stalled client while still holding its `ReadSnapshot` and permit.
- **Listener ordering** (Task 8, DESIGN.md §15): seqs are strictly increasing *between bootstraps*; after `on_bootstrap_complete(S)` delivery continues at `S + 1`. The listener is called before the new seq is published to `wait_for_seq`/`subscribe`, so whoever waits for a seq also sees the listener's effects.
- **Refused promotion shuts the slave down** (Task 11, DESIGN.md §13.1): `promote(self)` consumes the slave, so on refusal it is shut down cleanly and its directory is unchanged. An outstanding `ReadSnapshot` is detected after shutdown (it still holds the shared `DbCore`) and reported as `Error::Config` naming the snapshot. **Controller ruling (refuse by default, explicit API):** `promote_checked` and `Master::demote` do not shut down on refusal — they return `Err(PromoteRefused { error, slave: Option<Box<Slave>> })` / `Err(DemoteRefused { error, master: Option<Box<Master>> })` (`Debug`, `Display`, `std::error::Error`, `into_slave()` / `into_master()`) carrying the untouched running node; the handle is `None` only for a failure after the node started shutting down for the change (I/O error during shutdown, an open `ReadSnapshot`, a failure rewriting `__meta` or opening the new role), documented in their rustdoc. The handle is an `Option` because those late failures have no node to return, and boxed for clippy's `result_large_err`. The non-consuming `Slave::check_promotion` and `Master::can_demote` remain as pre-checks.
- **Promotion tooling** (Task 11, addendum §4, DESIGN.md §13.1, §16, §20, §22): `admin::{NodeInfo, ReplicaSummary, compare, Comparison, Side, choose_promotion_candidate, Choice, node_status}`, `Slave::{promotion_info, check_promotion, promote_checked}`, `PromotionCheck`, `Master::{can_demote, demote}`, `Error::NotMostAdvanced`. Decisions: `compare` treats a node without data as behind every node with data; `Choice::diverged` is `Vec<(node_id, fork_seq)>`, `foreign` is node ids; unreachable peers do not block `promote_checked` (they are logged and listed in `PromotionCheck`).
- **Write concern** (Task 12, addendum §2, DESIGN.md §1, §5.2, §16, §16.2, §18, §21, §22): `WriteConcern { min_insync_replicas, ack_timeout }` (`MasterConfig::write_concern`, `Master::{write_with, put_with}`), `Error::{NotEnoughReplicas, AckTimeout}`.
- **Repoint** (Task 13, addendum §3, DESIGN.md §13, §13.1 including the failover runbook, §16, §16.2, §16.3, §22; PROTOCOL.md §6.4): `Slave::repoint`, `RepointOptions { on_divergence }`, `RepointDivergence`, `AcceptDataLoss`, `RepointOutcome`, `Error::{Diverged, Unsupported}`. Decisions: `RepointOutcome` adds `Retrying { last_error }` for a new upstream that cannot be used yet (unreachable, `STALE_EPOCH`, `NOT_READY`) — `repoint` returns at once and the slave keeps retrying it; `Repaired { discarded }` exists for Plan 3 and is never returned here; `Repair` returns `Error::Unsupported` and leaves the slave halted `Diverged`; `repoint` clears any halt first, and a loop halts `LoopDetected` (`Err(Halted(LoopDetected))`) until the next repoint; `RepointOptions` has no timeout — callers bound the wait with `tokio::time::timeout`, which does not undo the repoint; `fork_seq` for a same-epoch CRC mismatch is the mismatching (last) seq, since the true fork point needs Plan 3's repair.
- **Rulings from the Plan 1 final review:** upstream epoch histories are validated (Global Constraints); a failed commit on a slave halts it `Corrupt` instead of retrying; a `failpoints`-gated test covers the master's `bump_epoch` fence (point `master-bump-epoch-write`, Task 15).
- **Test-only `Slave::kill`** behind feature `failpoints` (Task 15, DESIGN.md §16). The feature enables `fail/failpoints` as well (the design notes listed only `dep:fail`, but `fail` compiles every point to a no-op without its own feature). `futures-util` from the design notes is not added: nothing needs it.
- **Pre-flight rulings:** shared helpers instead of duplicated blocks (`server::{start_for, end_stream}`, `background::{every, run_logged, blocking}`, `Store::delete_all_keys`); bootstrap jobs drop their snapshot before returning their permit and `ServerHandle::shutdown` waits (≤ 5 s) for all permits; the stream-ending send also gives up on reset; tests never use sleep as synchronisation and bound every wait (10 s); Plan 1 placeholder `allow`s are removed as their items get used, and the items Plan 2 does not use are deleted (Task 15). The server-test helpers (`test_client::{eventually, within, T0}`) duplicate the integration harness on purpose: unit tests cannot use `tests/support`.

## File Structure

```
Cargo.toml                    + tonic, tonic-prost, tonic-types, tokio-stream, subtle, async-trait, fail; rcgen (Task 1); [[test]] failpoints (Task 15)
build.rs                      server + client codegen (Task 1)
proto/replication.proto       bidirectional Tail (TailClientMessage, TailAck), Status RPC (Task 6)
src/lib.rs                    module tree and re-exports (Tasks 1–7, 11, 12, 13)
src/net/mod.rs                metadata keys, Reason <-> status with ErrorInfo, internal_status, decode limits (Task 1)
src/net/convert.rs            DbIdentity / CfEntry / EpochStart / LogEntry <-> pb, checked_history (Task 1)
src/store/meta.rs             + node_state, max_entry_bytes, bootstrap_cfs keys and fields (Task 2)
src/store/mod.rs              + bootstrap_cfs in the known set at open; new modules (Tasks 2, 7, 11); drop unused Store::role (Task 15)
src/store/repl.rs             entry_crc, read_entries, ReadSnapshot::{meta, phys, raw_ctx}, ReadCtx::iter_raw (Task 2)
src/source.rs                 LogSource trait (Task 2); node_state, upstream (Task 6)
src/server/mod.rs             module list (Task 3); full server (Task 5); replica registry, bidi Tail, Status (Task 6)
src/server/handshake.rs       pure decision table, one test per rule (Task 3)
src/types.rs                  drop the stale allow on fork_after (Task 3) and epoch_for (Task 8)
src/auth.rs                   Method, PeerInfo, Authenticator, tokens, mTLS allow-list, AllowMethods, limiter, CN parser (Task 4); Method::Status (Task 6)
src/config.rs                 + TLS configs, ServeLimits, ServeConfig (Task 4); MasterConfig.serve (Task 5); replica_lag_time_max (Task 6); WriteConcern (Task 12)
src/failpoint.rs              `fire(name)`: fail::eval behind feature `failpoints` (Task 5); `io_error(name)` (Task 15)
src/server/tail.rs            Tail validation, fetch batching (§7.5) and producer (Task 5); start/ack messages, ack reader (Task 6)
src/server/bootstrap.rs       Bootstrap producer, digest, lease hand-over (Task 5); release before the final status (Task 6)
src/server/replicas.rs        replica registry: per-stream sent/acked seqs, in-sync, ReplicaStatus (Task 6; acks/wait_until used from Task 12)
src/server/status.rs          the Status RPC's report (Task 6)
src/server/test_client.rs     bare protocol client and bounded-wait helpers for server unit tests (Task 5); TailExt (Task 6)
src/server/tests.rs           server unit tests against a real master (Tasks 5, 6)
src/store/retention.rs        drop the stale allows on GuardSet, RetentionGuard, guard (Task 5)
src/master/source.rs          LogSource for the master; slow-reader test (Task 5)
src/master/writer.rs          + reset_tx, identity_and_history (Task 5); replicas (Task 6); failpoint in bump_epoch (Task 15)
src/master/mod.rs             + server start (start_for), serve_addr, shutdown waits for the DB (Task 5); replicas (Task 6); can_demote, demote (Task 11); write_with, put_with (Task 12)
src/master/write_concern.rs   commit_with: in-sync check, commit, wait for acks (Task 12)
src/admin.rs                  NodeInfo (Task 6); compare, choose_promotion_candidate, node_status (Task 11)
src/error.rs                  + Halted (Task 7); NotMostAdvanced (Task 11); NotEnoughReplicas, AckTimeout (Task 12); Diverged, Unsupported (Task 13)
src/listener.rs               ReplicationListener (Task 7)
src/store/bootstrap.rs        reset_for_bootstrap, begin_bootstrap_cf, write_bootstrap_records, finish_bootstrap, adopt_identity, delete_all_keys (Task 7)
src/slave/mod.rs              module list (Task 7); Slave API, ReadApi, LogApi (Task 8); clear_halt (Task 9); serving, replicas (Task 10); promote module (Task 11); repoint module (Task 13); kill (Task 15)
src/slave/config.rs           SlaveConfig, UpstreamConfig, FetchConfig, policies, Backoff, sync_writes (Task 7)
src/slave/state.rs            HaltReason, SlaveState, HaltAction, NodeState, SlaveStatus (Task 7)
src/slave/policy.rs           pure handshake/status → action table (Task 7)
src/background.rs             every, run_logged, blocking: helpers for background and blocking work (Task 8)
src/master/maintenance.rs     uses the background helpers (Task 8)
src/slave/inner.rs            shared slave state (Task 8); replicas, upstream_uri (Task 10); current upstream, repoint state (Task 13)
src/slave/client.rs           upstream channel + metadata interceptor (Task 8); connect_upstream used by admin (Task 11); connect to the current upstream (Task 13)
src/slave/driver.rs           connect → handshake → act loop, tail with acks (Task 8); repoint cancellation and outcomes (Task 13)
src/slave/apply.rs            per-entry checks and apply (Task 8)
src/slave/bootstrap.rs        bootstrap install (Task 8); cancellable by a repoint (Task 13)
src/slave/maintenance.rs      retention and CF drops on slaves (Task 8)
src/slave/source.rs           LogSource for slaves (Task 10)
src/store/promote.rs          rebuild_ttl_index, promote_to_master, demote_to_slave (Task 11)
src/slave/promote.rs          promote, promotion_info, check_promotion, promote_checked (Task 11)
src/slave/repoint.rs          repoint, RepointOptions, RepointOutcome, AcceptDataLoss (Task 13)
src/store/cfs.rs              drop unused pending_entries, push_pending (Task 15)
src/codec/mod.rs              drop the unused decode_ops re-export (Task 15)
tests/support/mod.rs          Cluster harness, dump, edge cases, RecordingListener (Task 8; serving in Task 10)
tests/replication.rs          core replication, acks, sync_writes (Task 8)
tests/rebootstrap.rs          policies, re-bootstrap, restore, epoch bump (Task 9)
tests/chain.rs                chains, loops, reset propagation, replicas per hop (Task 10)
tests/promotion.rs            promotion, Status, promote_checked, demote (Task 11)
tests/write_concern.rs        write concern end to end (Task 12)
tests/repoint.rs              repoint, failover runbook (Task 13)
tests/security.rs             TLS, mTLS, tokens (Task 14)
tests/failure_resume.rs       restarts, downstream ahead, persisted LogGap (Task 15)
tests/failpoints.rs           fault injection, serial, --features failpoints (Task 15)
```

---

### Task 9: Halt recovery and re-bootstrap policies end to end

**Files:**
- Modify: `src/slave/mod.rs` (`clear_halt`)
- Create: `tests/rebootstrap.rs`

**Interfaces:**
- Consumes: Task 8 `Slave`, `Inner::{state, set_state, set_last_error, force_bootstrap, halt_cleared}`, harness; Task 7 `NodeState`, `HaltAction`; Task 2 `meta::key::NODE_STATE`.
- Produces: `Slave::clear_halt(&self, action: HaltAction) -> Result<()>` (async): persists `node_state = ready`, sets `force_bootstrap` for `Rebootstrap`, returns the slave to `Connecting` and wakes the driver; `Err(Config)` if not halted.

- [ ] **Step 1: Write the failing tests**

Create `tests/rebootstrap.rs`:

```rust
//! Policies and re-bootstrap, end to end: retention gaps, identity changes, divergence,
//! restores from backup and epoch bumps.

mod support;

use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, DivergencePolicy, Error, HaltAction, HaltReason, IdentityPolicy, LogApi, Master, ReadApi, RocksTuning,
    Slave, SlaveState,
};
use support::{assert_converged, restore_dir, wait_state, wait_until, Cluster, WAIT};

fn halted(r: HaltReason) -> impl Fn(&SlaveState) -> bool {
    move |s| *s == SlaveState::Halted(r)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn retention_gap_triggers_a_rebootstrap_that_converges() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_entries = Some(2);
    mcfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    // Wait for the Tail that presents the bootstrap lease; until then the lease holds retention.
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    let behind = b.last_seq();
    b.shutdown().await.unwrap();

    for i in 0..20u8 {
        m.put("c", &[i], &[i]).unwrap();
    }
    wait_until("master retention", || m.first_retained_seq() > behind + 1).await;
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1, "LOG_GAP with GapPolicy::Rebootstrap");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_change_is_refused_persistently_until_clear_halt_rebootstraps() {
    let c = Cluster::new();
    let m1 = c.master("a1").await;
    let port = m1.serve_addr().unwrap().port();
    m1.create_cf(CfSpec::new("old")).unwrap();
    let (cfg, _) = c.slave_config("b", m1.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m1, &[("b", &b)]).await;
    m1.shutdown().await.unwrap();

    // A different database appears at the same address.
    let mut cfg2 = c.master_config("a2");
    cfg2.serve.as_mut().unwrap().port = port;
    let m2 = Master::open(cfg2).await.unwrap();
    m2.create_cf(CfSpec::new("new")).unwrap();
    for i in 0..3u8 {
        m2.put("new", &[i], b"v").unwrap();
    }
    wait_state(&b, "IdentityChanged halt", halted(HaltReason::IdentityChanged)).await;
    assert!(matches!(b.wait_for_seq(99, WAIT).await, Err(Error::Halted(HaltReason::IdentityChanged))));
    assert_eq!(b.get("old", b"k").unwrap(), None, "reads keep working while halted");
    assert_eq!(b.list_cfs(), vec![CfSpec::new("old")]);

    // The halt survives a restart: the reopened slave does not contact its upstream.
    let (cfg, listener) = c.slave_config("b", m2.serve_addr().unwrap());
    b.shutdown().await.unwrap();
    let b = Slave::open(cfg).await.unwrap();
    assert_eq!(b.status().state, SlaveState::Halted(HaltReason::IdentityChanged));
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(b.status().state, SlaveState::Halted(HaltReason::IdentityChanged));
    assert_eq!(listener.bootstraps(), 0);

    b.clear_halt(HaltAction::Rebootstrap).await.unwrap();
    // Seqs of different databases can coincide: wait for the bootstrap before comparing.
    wait_until("the bootstrap from the new database", || listener.bootstraps() == 1).await;
    assert_converged(&m2, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1);
    assert_eq!(b.status().origin_identity, Some(m2.identity()));
    assert!(matches!(b.clear_halt(HaltAction::Retry).await, Err(Error::Config(_))), "not halted any more");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn identity_change_with_rebootstrap_policy_converges_directly() {
    let c = Cluster::new();
    let m1 = c.master("a1").await;
    let port = m1.serve_addr().unwrap().port();
    m1.create_cf(CfSpec::new("old")).unwrap();
    let (mut cfg, listener) = c.slave_config("b", m1.serve_addr().unwrap());
    cfg.on_identity_change = IdentityPolicy::Rebootstrap;
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m1, &[("b", &b)]).await;
    m1.shutdown().await.unwrap();

    let mut cfg2 = c.master_config("a2");
    cfg2.serve.as_mut().unwrap().port = port;
    let m2 = Master::open(cfg2).await.unwrap();
    m2.create_cf(CfSpec::new("new")).unwrap();
    m2.put("new", b"k", b"v").unwrap();
    wait_until("the bootstrap from the new database", || listener.bootstraps() == 2).await;
    assert_converged(&m2, &[("b", &b)]).await;
    assert!(b.list_cfs().iter().all(|s| s.name != "old"));
}

/// A master restored from an older backup without an epoch bump: a slave that is ahead is
/// DIVERGED (rule 11) and halts by default, or re-bootstraps with `DivergencePolicy::Rebootstrap`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn master_restored_without_epoch_bump_makes_ahead_slaves_diverge() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"1").unwrap();
    let m = c.restart_master_after_backup(m, "a", "a-backup").await;
    for i in 0..3u8 {
        m.put("c", &[i], b"after backup").unwrap();
    }
    let (halting, _) = c.slave("halting", m.serve_addr().unwrap()).await;
    let (mut cfg, rebooted_listener) = c.slave_config("rebooting", m.serve_addr().unwrap());
    cfg.on_divergence = DivergencePolicy::Rebootstrap;
    let rebooting = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("halting", &halting), ("rebooting", &rebooting)]).await;
    let ahead = m.last_seq();

    let port = m.serve_addr().unwrap().port();
    m.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    let m = Master::open(mcfg).await.unwrap();
    assert!(m.last_seq() < ahead);
    m.put("c", b"new history", b"x").unwrap();

    wait_state(&halting, "Diverged halt", halted(HaltReason::Diverged)).await;
    assert_eq!(halting.last_seq(), ahead, "a halted slave keeps its data");
    wait_until("the re-bootstrap after DIVERGED", || rebooted_listener.bootstraps() == 2).await;
    assert_converged(&m, &[("rebooting", &rebooting)]).await;
}

/// Restoring a backup with `bump_epoch_on_open`: a slave at or behind the restore point with
/// matching entries continues without a bootstrap (EPOCH_ADVANCED); one past it is DIVERGED.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restored_master_with_epoch_bump_keeps_behind_slaves_and_rejects_ahead_ones() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"1").unwrap();
    let (behind_cfg, _) = c.slave_config("behind", m.serve_addr().unwrap());
    let behind = Slave::open(behind_cfg).await.unwrap();
    assert_converged(&m, &[("behind", &behind)]).await;
    behind.shutdown().await.unwrap();

    let m = c.restart_master_after_backup(m, "a", "a-backup").await;
    m.put("c", b"lost", b"x").unwrap();
    let (ahead, _) = c.slave("ahead", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("ahead", &ahead)]).await;

    let port = m.serve_addr().unwrap().port();
    m.shutdown().await.unwrap();
    restore_dir(&c.path("a-backup"), &c.path("a"));
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().port = port;
    mcfg.bump_epoch_on_open = true;
    let m = Master::open(mcfg).await.unwrap();
    assert_eq!(m.identity().epoch, 2);
    m.put("c", b"after restore", b"y").unwrap();

    let (cfg, listener) = c.slave_config("behind", m.serve_addr().unwrap());
    let behind = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("behind", &behind)]).await;
    assert_eq!(listener.bootstraps(), 0, "EPOCH_ADVANCED: no bootstrap");
    assert_eq!(behind.status().origin_identity.unwrap().epoch, 2);
    wait_state(&ahead, "Diverged halt", halted(HaltReason::Diverged)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn live_epoch_bump_resets_streams_and_slaves_continue() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    let id = m.bump_epoch().unwrap();
    m.put("c", b"k", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(b.status().origin_identity, Some(id));
    assert_eq!(listener.bootstraps(), 1, "only the initial bootstrap");
    let states = listener.states();
    let streaming = states.iter().filter(|s| **s == SlaveState::Streaming).count();
    assert!(streaming >= 2, "the stream was reset and re-established: {states:?}");
}

/// A slave without a CF's comparator halts with SpecMismatch (persisted); after the
/// comparator is registered and the halt cleared with Retry, it applies the entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn missing_comparator_halts_until_registered_and_cleared() {
    let reverse = |t: &mut RocksTuning| {
        t.comparators.register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
    };
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    reverse(&mut mcfg.rocksdb);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("plain")).unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    m.create_cf(CfSpec::new("rev").with_comparator("reverse")).unwrap();
    m.put("rev", b"a", b"1").unwrap();
    m.put("rev", b"b", b"2").unwrap();
    wait_state(&b, "SpecMismatch halt", halted(HaltReason::SpecMismatch)).await;

    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    reverse(&mut cfg.rocksdb);
    b.shutdown().await.unwrap();
    let b = Slave::open(cfg).await.unwrap();
    assert_eq!(b.status().state, SlaveState::Halted(HaltReason::SpecMismatch), "persisted");
    b.clear_halt(HaltAction::Retry).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 0);
    let keys: Vec<Vec<u8>> = b.iter("rev", replicated_rocksdb::IterMode::Start).unwrap().map(|r| r.unwrap().0).collect();
    assert_eq!(keys, vec![b"b".to_vec(), b"a".to_vec()], "reverse order on the slave too");
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --test rebootstrap`
Expected: FAIL to compile: `no method named clear_halt found for struct Slave`.

- [ ] **Step 3: Implement `clear_halt`**

In `src/slave/mod.rs`, add these imports:

```rust
use std::sync::atomic::Ordering;

use crate::store::meta::key;
use state::NodeState;
```

and add to `impl Slave`, before `pub fn snapshot`:

```rust
    /// Operator action: leaves a halt. `Retry` handshakes again with the current data (after
    /// fixing the cause, for example registering a missing comparator and reopening);
    /// `Rebootstrap` discards local data and bootstraps — the only way out of `Diverged`, or
    /// of `IdentityChanged` under `IdentityPolicy::Refuse`. `Err(Config)` if not halted.
    pub async fn clear_halt(&self, action: HaltAction) -> Result<()> {
        let SlaveState::Halted(reason) = self.inner.state() else {
            return Err(Error::Config("the slave is not halted".into()));
        };
        let i = self.inner.clone();
        crate::background::blocking(move || i.store.write_meta(&[(key::NODE_STATE, &NodeState::Ready.encode())], true))
            .await?;
        if action == HaltAction::Rebootstrap {
            self.inner.force_bootstrap.store(true, Ordering::SeqCst);
        }
        tracing::info!(node = %self.inner.cfg.identifier, reason = reason.name(), ?action, "halt cleared");
        self.inner.set_last_error(None);
        self.inner.set_state(SlaveState::Connecting);
        self.inner.halt_cleared.notify_one();
        Ok(())
    }
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --test rebootstrap`
Expected: PASS, 7 tests:
- `retention_gap_triggers_a_rebootstrap_that_converges` (LOG_GAP, default `GapPolicy::Rebootstrap`, one new bootstrap, exact dump equality);
- `identity_change_is_refused_persistently_until_clear_halt_rebootstraps` (`IdentityChanged` halt survives restart, the slave does not contact its upstream, `clear_halt(Rebootstrap)` converges);
- `identity_change_with_rebootstrap_policy_converges_directly`;
- `master_restored_without_epoch_bump_makes_ahead_slaves_diverge` (rule 11; `Halt` keeps data, `Rebootstrap` converges);
- `restored_master_with_epoch_bump_keeps_behind_slaves_and_rejects_ahead_ones` (`EPOCH_ADVANCED` without bootstrap; past the fork `DIVERGED`);
- `live_epoch_bump_resets_streams_and_slaves_continue`;
- `missing_comparator_halts_until_registered_and_cleared` (`SpecMismatch` persisted; `clear_halt(Retry)` after registering the comparator).

- [ ] **Step 5: Commit**

```bash
git add src tests
git commit -m "feat: clear_halt and end-to-end re-bootstrap, divergence and epoch tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Chained replication — slaves serve downstream replicas

**Files:**
- Create: `src/slave/source.rs`, `tests/chain.rs`
- Modify: `src/slave/mod.rs` (serve, `serve_addr`, `replicas`), `src/slave/inner.rs` (`replicas`, `upstream_uri`), `tests/support/mod.rs` (serving slaves, `restart_slave`), `DESIGN.md` (§16)

**Interfaces:**
- Consumes: Task 5 `server::start_for`, `ServerHandle`; Task 6 `ReplicaRegistry`, `ReplicaStatus`, `ServerHandle::replica_lag_time_max`, `LogSource::{node_state, upstream}`; Task 2 `LogSource`; Task 8 `Inner::{ident, is_ready, state, upstream_chain, origin_last_seq, max_entry_bytes, seq_tx, reset_tx}`.
- Produces: `impl LogSource for slave::inner::Inner` (with `node_state()` = `bootstrapping` / `halted:<Reason>` / `ready`, and `upstream()` = the upstream URI); `Inner.replicas: Arc<ReplicaRegistry>`, `Inner::upstream_uri(&self) -> String`; `Slave::open` starts a server when `cfg.serve` is `Some`; `Slave::serve_addr(&self) -> Option<SocketAddr>`; `Slave::replicas(&self) -> Vec<ReplicaStatus>` (direct downstream replicas; empty without `serve`); harness `Cluster::restart_slave(&self, Slave, SlaveConfig) -> Slave` and serving slaves by default.

- [ ] **Step 1: Make harness slaves serve**

In `tests/support/mod.rs`, in `Cluster::slave_config`, replace:

```rust
        cfg.listener = Some(listener.clone());
```

with:

```rust
        cfg.serve = Some(self.serve());
        cfg.listener = Some(listener.clone());
```

and add to `impl Cluster`:

```rust
    /// Reopens a slave on the port it served before, so its downstream finds it again.
    pub async fn restart_slave(&self, s: Slave, cfg: SlaveConfig) -> Slave {
        let port = s.serve_addr().map(|a| a.port());
        s.shutdown().await.unwrap();
        let mut cfg = cfg;
        if let (Some(port), Some(serve)) = (port, cfg.serve.as_mut()) {
            serve.port = port;
        }
        Slave::open(cfg).await.unwrap()
    }
}
```

(Insert it before the closing `}` of `impl Cluster`.)

- [ ] **Step 2: Write the failing tests**

Create `tests/chain.rs`:

```rust
//! Chained replication, end to end: A (master) → B (slave serving) → C.

mod support;

use std::time::Duration;

use replicated_rocksdb::{CfSpec, HaltReason, IdentityPolicy, LogApi, Master, ReadApi, Slave, SlaveState};
use support::{assert_converged, check_edge_cases, wait_state, wait_until, write_edge_cases, Cluster};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_chain_replicates_everything_and_bootstraps_from_the_middle() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..50u8 {
        a.put("c", &[i], &[i; 100]).unwrap();
    }
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b)]).await;

    // C bootstraps from B while A keeps writing.
    let writer = {
        let a = &a;
        async move {
            for i in 0..50u8 {
                a.put("c", &[b'w', i], b"during").unwrap();
                tokio::time::sleep(Duration::from_millis(2)).await;
            }
        }
    };
    let ((cn, lc), ()) = tokio::join!(c.slave("c", b.serve_addr().unwrap()), writer);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 1);

    a.create_cf(CfSpec::new("tmp")).unwrap();
    a.put_with_ttl("tmp", b"t", b"v", Duration::from_secs(5)).unwrap();
    a.drop_cf("c").unwrap();
    write_edge_cases(&a, "edge");
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    check_edge_cases(&cn, "edge");
    lc.assert_no_violations();

    let s = cn.status();
    assert_eq!(s.upstream_chain, vec!["b".to_string(), "a".to_string()]);
    assert_eq!(s.origin_identity, Some(a.identity()));
    wait_until("origin position reaches the leaf", || cn.status().origin_last_seq == a.last_seq()).await;
    // C's log is A's log, byte for byte.
    let from = cn.first_retained_seq() - 1;
    let ours: Vec<_> = cn.entries_since(from).unwrap().map(Result::unwrap).collect();
    let origin: Vec<_> = a.entries_since(from).unwrap().map(Result::unwrap).collect();
    assert_eq!(ours, origin);
    // Each node reports only its direct replicas.
    wait_until("c acknowledged on b", || b.replicas().iter().any(|r| r.node_id == "c" && r.applied_seq == a.last_seq())).await;
    assert_eq!(a.replicas().iter().map(|r| r.node_id.clone()).collect::<Vec<_>>(), vec!["b".to_string()]);
    assert_eq!(b.replicas().len(), 1);
    assert!(cn.replicas().is_empty());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_loop_is_detected_and_the_halt_is_not_persisted() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    let (cn, _) = c.slave("c", b.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    // Point B at C, which is downstream of B.
    let mut looped = b_cfg.clone();
    looped.upstream = replicated_rocksdb::UpstreamConfig::from_addr(cn.serve_addr().unwrap());
    let b = c.restart_slave(b, looped).await;
    wait_state(&b, "LoopDetected halt", |s| *s == SlaveState::Halted(HaltReason::LoopDetected)).await;

    // Configuration halts are re-evaluated on restart: pointed back at A, B resumes.
    let b = c.restart_slave(b, b_cfg).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
}

/// Rule 3 also catches a slave pointed at its own address (`client_id == server_id`).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slave_pointed_at_itself_halts_loop_detected() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    let mut own = b_cfg;
    own.upstream = replicated_rocksdb::UpstreamConfig::from_addr(b.serve_addr().unwrap());
    let b = c.restart_slave(b, own).await;
    wait_state(&b, "LoopDetected halt", |s| *s == SlaveState::Halted(HaltReason::LoopDetected)).await;
    assert_eq!(b.get("c", b"k").unwrap(), None, "still readable while halted");
}

/// When B re-bootstraps, it ends C's streams with UPSTREAM_RESET; C reconnects and follows.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_upstream_rebootstrap_resets_downstream_streams() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let port = a.serve_addr().unwrap().port();
    a.create_cf(CfSpec::new("old")).unwrap();
    let (mut b_cfg, lb) = c.slave_config("b", a.serve_addr().unwrap());
    b_cfg.on_identity_change = IdentityPolicy::Rebootstrap;
    let b = Slave::open(b_cfg).await.unwrap();
    let (mut c_cfg, lc) = c.slave_config("c", b.serve_addr().unwrap());
    c_cfg.on_identity_change = IdentityPolicy::Rebootstrap;
    let cn = Slave::open(c_cfg).await.unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    wait_state(&cn, "C streaming", |s| *s == SlaveState::Streaming).await;
    let streaming = || lc.states().iter().filter(|s| **s == SlaveState::Streaming).count();
    let streams_before = streaming();

    // B's upstream is replaced by a different database: B re-bootstraps while serving C.
    a.shutdown().await.unwrap();
    let mut cfg2 = c.master_config("a2");
    cfg2.serve.as_mut().unwrap().port = port;
    let a2 = Master::open(cfg2).await.unwrap();
    a2.create_cf(CfSpec::new("new")).unwrap();
    a2.put("new", b"k", b"v").unwrap();

    assert_converged(&a2, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(lb.bootstraps(), 2);
    assert_eq!(lc.bootstraps(), 2, "C followed B's new identity");
    wait_until("C's stream reset and re-established", || streaming() > streams_before).await;
}
```

- [ ] **Step 3: Run them to confirm they fail**

Run: `cargo test --test chain`
Expected: FAIL to compile: `no method named serve_addr found for struct Slave`.

- [ ] **Step 4: Implement serving from a slave**

In `src/slave/inner.rs`, add to `Inner` after `listener_failed: AtomicBool,`:

```rust
    /// Downstream replicas served by this slave.
    pub(crate) replicas: Arc<crate::server::replicas::ReplicaRegistry>,
```

initialise it in `Inner::new` after `listener_failed: AtomicBool::new(false),`:

```rust
            replicas: Arc::default(),
```

and add after `Inner::new`:

```rust
    /// The upstream's address.
    pub(crate) fn upstream_uri(&self) -> String {
        self.cfg.upstream.uri()
    }
```

Create `src/slave/source.rs`:

```rust
//! A slave as a replication source for downstream replicas (chaining, DESIGN.md §11).

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;

use super::inner::Inner;
use super::state::SlaveState;
use crate::source::LogSource;
use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

impl LogSource for Inner {
    fn node_id(&self) -> &str {
        &self.cfg.identifier
    }

    fn role(&self) -> Role {
        Role::Slave
    }

    fn is_ready(&self) -> bool {
        Inner::is_ready(self)
    }

    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)> {
        self.ident()
    }

    fn store(&self) -> &Store {
        &self.store
    }

    fn origin_last_seq(&self) -> Seq {
        self.origin_last_seq.load(Ordering::SeqCst).max(self.store.position().last_seq)
    }

    fn upstream_chain(&self) -> Vec<String> {
        Inner::upstream_chain(self)
    }

    fn max_entry_bytes(&self) -> u64 {
        self.max_entry_bytes.load(Ordering::SeqCst)
    }

    fn heartbeat(&self) -> Duration {
        self.cfg.heartbeat
    }

    fn subscribe(&self) -> watch::Receiver<Seq> {
        self.seq_tx.subscribe()
    }

    fn resets(&self) -> watch::Receiver<u64> {
        self.reset_tx.subscribe()
    }

    fn node_state(&self) -> String {
        match self.state() {
            SlaveState::Bootstrapping { .. } => "bootstrapping".into(),
            SlaveState::Halted(r) => format!("halted:{}", r.name()),
            SlaveState::Connecting | SlaveState::Streaming => {
                if Inner::is_ready(self) { "ready".into() } else { "bootstrapping".into() }
            }
        }
    }

    fn upstream(&self) -> Option<String> {
        Some(self.upstream_uri())
    }
}
```

In `src/slave/mod.rs`, add `mod source;` to the module list and `use std::net::SocketAddr;` and `use crate::server::replicas::ReplicaStatus;` to the imports. In `Slave::open`, replace these lines (as Task 8 wrote them):

```rust
        let driver = driver::spawn(&inner);
        let maintenance = maintenance::spawn(&inner);
        Ok(Slave { inner, driver: Some(driver), maintenance: Some(maintenance), server: None })
    }
```

with:

```rust
        let server = crate::server::start_for(&inner, inner.cfg.serve.clone(), inner.replicas.clone()).await?;
        let driver = driver::spawn(&inner);
        let maintenance = maintenance::spawn(&inner);
        Ok(Slave { inner, driver: Some(driver), maintenance: Some(maintenance), server })
    }

    /// Direct downstream replicas of this slave (see `Master::replicas`). Empty when it does
    /// not serve replication.
    pub fn replicas(&self) -> Vec<ReplicaStatus> {
        match &self.server {
            Some(s) => self.inner.replicas.status(&*self.inner, self.inner.now_ms(), s.replica_lag_time_max()),
            None => Vec::new(),
        }
    }

    /// The address this slave serves downstream replicas on, if `serve` is configured.
    pub fn serve_addr(&self) -> Option<SocketAddr> {
        self.server.as_ref().map(ServerHandle::addr)
    }
```

- [ ] **Step 5: Run the tests**

Run: `cargo test --test chain`
Expected: PASS, 4 tests: `a_chain_replicates_everything_and_bootstraps_from_the_middle` (C bootstraps from B while A writes; CF create/drop, TTL and the edge-case dataset reach C; C's log equals A's byte for byte; `upstream_chain == ["b", "a"]`), `a_loop_is_detected_and_the_halt_is_not_persisted`, `a_slave_pointed_at_itself_halts_loop_detected`, `an_upstream_rebootstrap_resets_downstream_streams`. The chain test also checks that each node reports only its direct replicas (`a.replicas()` is B, `b.replicas()` is C, `cn.replicas()` is empty).

Run: `cargo test`
Expected: PASS (the earlier suites now run with serving slaves).

- [ ] **Step 6: Update DESIGN.md §16**

In `impl Slave`, after `pub fn applied_seq(&self) -> Seq;` add:

```
    pub fn serve_addr(&self) -> Option<SocketAddr>;     // when serve is configured
```

Run: `cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Commit**

```bash
git add src tests DESIGN.md
git commit -m "feat: slaves serve downstream replicas; chain, loop and reset tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: Promotion and promotion tooling

**Files:**
- Create: `src/store/promote.rs`, `src/slave/promote.rs`, `tests/promotion.rs`
- Modify: `src/admin.rs` (replace), `src/store/mod.rs`, `src/slave/mod.rs`, `src/master/mod.rs` (`can_demote`, `demote`), `src/error.rs`, `src/lib.rs`, `DESIGN.md` (§13.1, §16, §16.2, §20, §22)

**Interfaces:**
- Consumes: Task 7 `Store::delete_all_keys`, `store::bootstrap::NODE_READY`; Task 8 `background::blocking`, `slave::client::connect_upstream`; Task 6 `admin::{NodeInfo, ReplicaSummary}`, `server::status::node_info`, `pb::{StatusRequest, StatusResponse}`, `ServerHandle::replica_lag_time_max`, `Inner.replicas`; Plan 1 `ttl::index_key`, `Store::{ttl_due, handle, cfs, load_meta}`, `Master::open`, `MasterConfig::validate`; Task 7 `Store::finish_bootstrap` (tests); Task 8 `Slave::shutdown`, `Inner::{state, is_ready, reset_downstream}`.
- Produces:
  - `Store::rebuild_ttl_index(&self) -> Result<u64>`, `Store::promote_to_master(&self) -> Result<DbIdentity>`, `Store::demote_to_slave(&self) -> Result<DbIdentity>` (refused with `Error::Config` while the newest epoch has entries or when there is no earlier epoch).
  - `Slave::promote(self, cfg: MasterConfig) -> Result<Master>` (async; `Err(Config)` naming the `ReadSnapshot` if one still holds the database after the slave shut down).
  - `Slave::promotion_info(&self) -> NodeInfo`; `Slave::check_promotion(&self, peers: &[UpstreamConfig]) -> Result<PromotionCheck>` (async; `Err(NotMostAdvanced { leader, seq })` if a reachable peer is later); `pub struct PromotionCheck { unreachable: Vec<(String, String)>, diverged: Vec<(String, Seq)>, foreign: Vec<String> }`; `Slave::promote_checked(self, cfg, peers) -> Result<Master, PromoteRefused>` (async); `pub struct PromoteRefused { pub error: Error, pub slave: Option<Box<Slave>> }` (`Debug`, `Display`, `std::error::Error`, `into_slave() -> Option<Slave>`): every refusal (promote's own checks, `NotMostAdvanced`, a failed peer check) returns the untouched running slave; `None` only for a failure after the shutdown for the promotion began.
  - `Master::can_demote(&self) -> Result<()>`; `Master::demote(self, cfg: SlaveConfig) -> Result<Slave, DemoteRefused>` (async); `pub struct DemoteRefused { pub error: Error, pub master: Option<Box<Master>> }` (same traits, `into_master() -> Option<Master>`): a refusal (`can_demote` fails, wrong `cfg.path`) returns the untouched running master; `None` only for a failure after its shutdown began.
  - Private `Slave::{promote_refusal(&self, &MasterConfig) -> Option<String>, promote_now(self, MasterConfig)}`: `promote` = check (refusal shuts down, as before) + `promote_now`; `promote_checked` = check + peer check (refusals return the slave) + `promote_now`.
  - `admin::{NodeInfo::from_pb (crate), Side { A, B }, Comparison { Later(Side), Equal, Diverged { fork }, DifferentDatabase }, compare(&NodeInfo, &NodeInfo) -> Comparison, Choice { candidate, compatible: Vec<String>, diverged: Vec<(String, Seq)>, foreign: Vec<String> }, choose_promotion_candidate(&[NodeInfo]) -> Result<Choice>, node_status(&UpstreamConfig, client_id) -> Result<NodeInfo> (async)}`.
  - `Error::NotMostAdvanced { leader: String, seq: Seq }`; crate-root re-exports `PromotionCheck`, `PromoteRefused`, `DemoteRefused`.

- [ ] **Step 1: Write the failing tests**

Create `src/store/promote.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::bootstrap::BootstrapFinish;
    use crate::store::test_support::apply;
    use crate::types::{CfEntry, CfSpec, Op};

    #[test]
    fn promotion_rewrites_meta_and_rebuilds_the_ttl_index() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path(), Role::Slave, Default::default()).unwrap().0;
            let id = DbIdentity { db_uuid: uuid::Uuid::from_u128(5), epoch: 2 };
            let history = vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 1 }];
            store
                .finish_bootstrap(&BootstrapFinish {
                    identity: id,
                    epoch_history: history,
                    cf_specs: vec![],
                    snapshot_seq: 0,
                    snapshot_crc: None,
                    max_entry_bytes: 1 << 20,
                })
                .unwrap();
            apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
            let put = |k: &[u8], at| Op::Put { cf: "c".into(), key: k.to_vec(), value: vec![], expire_at_ms: at };
            apply(&store, vec![put(b"a", Some(50)), put(b"", None), put(b"\0", Some(70))]).unwrap();
            assert_eq!(store.rebuild_ttl_index().unwrap(), 2);
            assert_eq!(store.rebuild_ttl_index().unwrap(), 2, "idempotent");
            let due = store.ttl_due(60, 10, None).unwrap();
            assert_eq!(due.iter().map(|e| e.key.clone()).collect::<Vec<_>>(), vec![b"a".to_vec()]);
            let c = CfEntry { spec: CfSpec::new("c"), create_seq: 1 };
            assert!(due.iter().all(|e| e.cf_name == c.spec.name && e.create_seq == c.create_seq));

            let promoted = store.promote_to_master().unwrap();
            assert_eq!(promoted, DbIdentity { db_uuid: id.db_uuid, epoch: 3 });
        }
        let (_, m) = Store::open(dir.path(), Role::Master, Default::default()).unwrap();
        assert_eq!(m.role, Role::Master);
        assert_eq!(m.identity.unwrap().epoch, 3);
        assert_eq!(m.epoch_history.last(), Some(&EpochStart { epoch: 3, start_seq: 3 }));
        assert_eq!(m.last_ts, 2_000, "ts of entry 2 (test_support stamps 1000 × seq)");
        assert_eq!((m.node_state, m.max_entry_bytes), (None, None));
    }

    #[test]
    fn demotion_undoes_an_unused_promotion_only() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path(), Role::Slave, Default::default()).unwrap().0;
            let id = DbIdentity { db_uuid: uuid::Uuid::from_u128(5), epoch: 1 };
            store
                .finish_bootstrap(&BootstrapFinish {
                    identity: id,
                    epoch_history: vec![EpochStart { epoch: 1, start_seq: 1 }],
                    cf_specs: vec![],
                    snapshot_seq: 0,
                    snapshot_crc: None,
                    max_entry_bytes: 1 << 20,
                })
                .unwrap();
            apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
            store.promote_to_master().unwrap();
        }
        {
            let store = Store::open(dir.path(), Role::Master, Default::default()).unwrap().0;
            assert_eq!(store.demote_to_slave().unwrap(), DbIdentity { db_uuid: uuid::Uuid::from_u128(5), epoch: 1 });
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(m.epoch_history, vec![EpochStart { epoch: 1, start_seq: 1 }]);
        assert_eq!(m.last_seq, 1);
        assert!(matches!(store.demote_to_slave(), Err(Error::Config(_))), "first epoch: nothing to undo");
    }

    #[test]
    fn a_slave_without_data_cannot_be_promoted() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Role::Slave, Default::default()).unwrap().0;
        assert!(matches!(store.promote_to_master(), Err(Error::Config(_))));
    }
}
```

Add `pub(crate) mod promote;` to the `src/store/mod.rs` module list.

Create `tests/promotion.rs`:

```rust
//! Promotion of a slave to master, end to end (DESIGN.md §13.1, PROTOCOL.md §6.4).

mod support;

use std::net::SocketAddr;
use std::time::Duration;

use replicated_rocksdb::admin::{self, Comparison};
use replicated_rocksdb::{
    CfSpec, DivergencePolicy, EpochStart, Error, HaltReason, LogApi, Master, Op, ReadApi, Role, Slave, SlaveState, UpstreamConfig,
};
use support::{assert_converged, check_edge_cases, wait_state, wait_until, write_edge_cases, Cluster};

/// Reopens slave `name` (shut down by the caller) pointed at `upstream`.
async fn reopen(c: &Cluster, name: &str, upstream: SocketAddr, tweak: impl FnOnce(&mut replicated_rocksdb::SlaveConfig)) -> (Slave, std::sync::Arc<support::RecordingListener>) {
    let (mut cfg, listener) = c.slave_config(name, upstream);
    tweak(&mut cfg);
    (Slave::open(cfg).await.unwrap(), listener)
}

/// A → {B, C, D, E}. C stops early (behind B), D and E keep following A past B.
/// A stops; B is promoted. C continues without a bootstrap (EPOCH_ADVANCED); D (default
/// policy) halts Diverged; E (Rebootstrap) re-bootstraps and converges. Writes, TTL reaping
/// and the edge-case dataset all replicate from the new master, through tail and bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_end_to_end() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    // The replicas bootstrap now, so every later entry reaches them through Tail and stays in
    // their logs: a promoted node proves continuity with the CRC of an entry it holds.
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    let (d, _) = c.slave("d", a_addr).await;
    let (e, _) = c.slave("e", a_addr).await;
    for (name, s) in [("b", &b), ("c", &cn), ("d", &d), ("e", &e)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    a.put_with_ttl("c", b"session", b"s", Duration::from_secs(60)).unwrap();
    for i in 0..5u8 {
        a.put("c", &[i], &[i]).unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn), ("d", &d), ("e", &e)]).await;
    cn.shutdown().await.unwrap(); // C stays behind
    a.put("c", b"seen by b", b"1").unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d), ("e", &e)]).await;
    b.shutdown().await.unwrap(); // B stops at the fork point
    a.put("c", b"never seen by b", b"2").unwrap();
    assert_converged(&a, &[("d", &d), ("e", &e)]).await;
    let ahead = a.last_seq();
    a.shutdown().await.unwrap(); // the operator stops the old master

    // Promote B. It must be a running slave: reopen it (its upstream is gone) and promote.
    let (b, _) = reopen(&c, "b", a_addr, |_| {}).await;
    let fork = b.last_seq() + 1;
    let mut mcfg = c.master_config("b");
    mcfg.identifier = "b".into();
    let b = b.promote(mcfg).await.unwrap();
    assert_eq!(b.identity().epoch, 2);
    assert_eq!(b.epoch_history(), vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: fork }]);
    let b_addr = b.serve_addr().unwrap();

    // C (behind the fork) continues without a bootstrap.
    let (cn, lc) = reopen(&c, "c", b_addr, |_| {}).await;
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 0);
    assert_eq!(cn.status().origin_identity, Some(b.identity()));

    // D and E (past the fork) are DIVERGED; D halts, E re-bootstraps.
    d.shutdown().await.unwrap();
    e.shutdown().await.unwrap();
    let (d, _) = reopen(&c, "d", b_addr, |_| {}).await;
    let (e, le) = reopen(&c, "e", b_addr, |cfg| cfg.on_divergence = DivergencePolicy::Rebootstrap).await;
    wait_state(&d, "Diverged halt", |s| *s == SlaveState::Halted(HaltReason::Diverged)).await;
    assert_eq!(d.last_seq(), ahead, "D keeps its data");
    wait_until("E re-bootstraps after DIVERGED", || le.bootstraps() == 1).await;
    assert_converged(&b, &[("c", &cn), ("e", &e)]).await;

    // Writes on the new master replicate; the TTL written on A before promotion is reaped by B.
    b.put("c", b"after promotion", b"3").unwrap();
    c.clock.advance(Duration::from_secs(60));
    let before_reap = b.last_seq();
    assert_eq!(b.reap_expired().unwrap().deleted, 1);
    write_edge_cases(&b, "edge");
    assert_converged(&b, &[("c", &cn), ("e", &e)]).await;
    // The new master's reaper Delete itself reached C through the log.
    let reaped: Vec<Op> = cn.entries_since(before_reap).unwrap().flat_map(|e| e.unwrap().decode_ops().unwrap()).collect();
    assert!(reaped.contains(&Op::Delete { cf: "c".into(), key: b"session".to_vec() }), "{reaped:?}");
    check_edge_cases(&cn, "edge");
    check_edge_cases(&e, "edge");

    // A fresh replica of the new master gets the edge cases through Bootstrap.
    let (f, lf) = c.slave("f", b_addr).await;
    assert_converged(&b, &[("f", &f)]).await;
    assert_eq!(lf.bootstraps(), 1);
    check_edge_cases(&f, "edge");
}

/// The old master, restarted, is refused by new-epoch replicas with STALE_EPOCH; they keep
/// retrying (not halted) and follow the new master again once re-pointed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_old_master_is_refused_with_stale_epoch_and_the_client_retries() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    // Bootstrapped and tailing before the write, so entry 2 reaches both through Tail.
    wait_state(&b, "b streaming", |st| *st == SlaveState::Streaming).await;
    wait_state(&cn, "c streaming", |st| *st == SlaveState::Streaming).await;
    a.put("c", b"through tail", b"0").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let a_cfg = c.master_config("a");
    a.shutdown().await.unwrap();

    let b = b.promote(c.master_config("b")).await.unwrap();
    cn.shutdown().await.unwrap();
    let (cn, _) = reopen(&c, "c", b.serve_addr().unwrap(), |_| {}).await;
    b.put("c", b"k", b"v").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(cn.status().origin_identity.unwrap().epoch, 2);

    // The old master comes back at its old address; C is (wrongly) pointed at it.
    let mut a_cfg = a_cfg;
    a_cfg.serve.as_mut().unwrap().port = a_addr.port();
    let old = Master::open(a_cfg).await.unwrap();
    old.put("c", b"split brain", b"x").unwrap();
    cn.shutdown().await.unwrap();
    let (cn, _) = reopen(&c, "c", a_addr, |_| {}).await;
    wait_until("STALE_EPOCH reported", || cn.status().last_error.as_deref().is_some_and(|e| e.contains("STALE_EPOCH"))).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    let s = cn.status();
    assert_eq!(s.state, SlaveState::Connecting, "retrying, not halted");
    assert!(s.last_error.unwrap().contains("STALE_EPOCH"));
    assert_eq!(cn.get("c", b"split brain").unwrap(), None);

    // Re-pointed at the new master, it continues.
    cn.shutdown().await.unwrap();
    let (cn, lc) = reopen(&c, "c", b.serve_addr().unwrap(), |_| {}).await;
    b.put("c", b"k2", b"v2").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_is_refused_without_data_and_needs_released_snapshots() {
    let c = Cluster::new();
    let nobody = std::net::TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap();
    let (empty, _) = c.slave("empty", nobody).await;
    assert!(matches!(empty.promote(c.master_config("empty")).await, Err(Error::Config(_))));

    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    assert_converged(&a, &[("b", &b)]).await;
    assert!(matches!(b.promote(c.master_config("elsewhere")).await, Err(Error::Config(_))), "path must match");

    let (b, _) = reopen(&c, "b", a.serve_addr().unwrap(), |_| {}).await;
    b.wait_for_seq(1, support::WAIT).await.unwrap();
    let snap = b.snapshot().unwrap();
    let err = b.promote(c.master_config("b")).await.err().expect("an open snapshot keeps the database open");
    assert!(matches!(&err, Error::Config(m) if m.contains("ReadSnapshot")), "{err}");
    drop(snap);
    // Nothing was changed: the directory is still a slave that resumes.
    let (b, lb) = reopen(&c, "b", a.serve_addr().unwrap(), |_| {}).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    assert_eq!(lb.bootstraps(), 0);
}

// ---------------- Promotion tooling (addendum §4) ----------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_status_rpc_and_promotion_info_report_positions() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    wait_until("b in sync on a", || a.replicas().iter().any(|r| r.node_id == "b" && r.in_sync)).await;

    let of_a = admin::node_status(&UpstreamConfig::from_addr(a.serve_addr().unwrap()), "operator").await.unwrap();
    assert_eq!((of_a.node_id.as_str(), of_a.role, of_a.last_seq), ("a", Role::Master, 2));
    assert_eq!(of_a.identity, Some(a.identity()));
    assert_eq!(of_a.upstream, None);
    assert_eq!(of_a.replicas.iter().map(|r| (r.node_id.as_str(), r.applied_seq, r.in_sync)).collect::<Vec<_>>(), vec![("b", 2, true)]);
    assert!(of_a.last_ts_ms > 0);

    let of_b = admin::node_status(&UpstreamConfig::from_addr(b.serve_addr().unwrap()), "operator").await.unwrap();
    assert_eq!(of_b, b.promotion_info(), "the RPC and the in-process call agree");
    assert_eq!((of_b.role, of_b.last_seq, of_b.node_state.as_str()), (Role::Slave, 2, "ready"));
    assert_eq!(of_b.last_entry_crc, of_a.last_entry_crc);
    assert_eq!(of_b.upstream, Some(format!("http://{}", a.serve_addr().unwrap())));
    assert_eq!(admin::compare(&of_a, &of_b), Comparison::Equal);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promote_checked_refuses_when_a_reachable_peer_is_later() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    b.shutdown().await.unwrap(); // b falls behind
    a.put("c", b"only c has it", b"v").unwrap();
    assert_converged(&a, &[("c", &cn)]).await;
    a.shutdown().await.unwrap();

    let (b, _) = reopen(&c, "b", a_addr, |_| {}).await;
    let c_addr = UpstreamConfig::from_addr(cn.serve_addr().unwrap());
    let refused = b.promote_checked(c.master_config("b"), std::slice::from_ref(&c_addr)).await.err().expect("c is later");
    assert!(matches!(&refused.error, Error::NotMostAdvanced { leader, seq: 2 } if leader == "c"), "{refused}");
    // Refused by default: b comes back untouched and still running.
    let b = refused.into_slave().expect("a refusal returns the slave");
    assert_eq!(b.get("c", b"only c has it").unwrap(), None, "b still serves its own data");
    assert_eq!(b.last_seq(), 1);
    // A refusal from promote's own checks returns it too.
    let refused = b.promote_checked(c.master_config("elsewhere"), &[]).await.err().expect("wrong path");
    assert!(matches!(refused.error, Error::Config(_)));
    let b = refused.into_slave().expect("a refusal returns the slave");

    // The most advanced node passes the check; the unreachable old master does not block it.
    let check = cn.check_promotion(&[UpstreamConfig::from_addr(b.serve_addr().unwrap()), UpstreamConfig::from_addr(a_addr)]).await.unwrap();
    assert_eq!(check.unreachable.len(), 1);
    assert!(check.diverged.is_empty() && check.foreign.is_empty());
    let new_master = cn.promote_checked(c.master_config("c"), &[UpstreamConfig::from_addr(a_addr)]).await.unwrap();
    assert_eq!(new_master.get("c", b"only c has it").unwrap(), Some(b"v".to_vec()));
    // The returned slave is fully alive: it still serves a downstream replica.
    let (e, _) = c.slave("e", b.serve_addr().unwrap()).await;
    assert_converged(&b, &[("e", &e)]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demote_undoes_a_wrong_promotion_with_zero_loss() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    for (name, s) in [("b", &b), ("c", &cn)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    for i in 0..5u8 {
        a.put("c", &[i], &[i]).unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let before = support::dump(&b);
    a.shutdown().await.unwrap();

    // The operator promotes b by mistake, then undoes it before any write.
    let b_master = b.promote(c.master_config("b")).await.unwrap();
    b_master.can_demote().unwrap();
    let (b_cfg, _) = c.slave_config("b", a_addr);
    let b = b_master.demote(b_cfg).await.unwrap();
    assert_eq!(b.status().origin_identity.unwrap().epoch, 1, "the old identity is back");
    support::assert_same("b after demote", &before, &support::dump(&b));

    // c is promoted instead; b follows it without a bootstrap: nothing was lost.
    let c_master = cn.promote(c.master_config("c")).await.unwrap();
    b.shutdown().await.unwrap();
    let (b, lb) = reopen(&c, "b", c_master.serve_addr().unwrap(), |_| {}).await;
    c_master.put("c", b"after", b"v").unwrap();
    assert_converged(&c_master, &[("b", &b)]).await;
    assert_eq!(lb.bootstraps(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demote_is_refused_once_the_new_epoch_has_entries() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    assert_converged(&a, &[("b", &b)]).await;
    a.shutdown().await.unwrap();
    let b_master = b.promote(c.master_config("b")).await.unwrap();
    let seq = b_master.put("c", b"new epoch", b"v").unwrap();
    assert!(matches!(b_master.can_demote(), Err(Error::Config(_))));
    let (b_cfg, _) = c.slave_config("b", a_addr);
    let refused = b_master.demote(b_cfg).await.err().expect("the new epoch has entries");
    assert!(matches!(refused.error, Error::Config(_)), "{refused}");
    // Refused by default: the master comes back untouched and still running.
    let b_master = refused.into_master().expect("a refusal returns the master");
    assert_eq!((b_master.last_seq(), b_master.get("c", b"new epoch").unwrap()), (seq, Some(b"v".to_vec())));
    let (d, _) = c.slave("d", b_master.serve_addr().unwrap()).await;
    b_master.put("c", b"still writable", b"v").unwrap();
    assert_converged(&b_master, &[("d", &d)]).await;
}
```

In `src/admin.rs`, append the comparison tests (they fail until Step 5):

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, uuid: u128, history: &[(u64, Seq)], last: Seq, crc: Option<u32>) -> NodeInfo {
        let epoch_history: Vec<EpochStart> = history.iter().map(|&(epoch, start_seq)| EpochStart { epoch, start_seq }).collect();
        NodeInfo {
            node_id: id.into(),
            role: Role::Slave,
            identity: epoch_history.last().map(|e| DbIdentity { db_uuid: uuid::Uuid::from_u128(uuid), epoch: e.epoch }),
            epoch_history,
            last_seq: last,
            last_entry_crc: crc,
            last_ts_ms: 0,
            node_state: "ready".into(),
            upstream: None,
            replicas: vec![],
        }
    }

    #[test]
    fn compare_orders_compatible_nodes_and_detects_divergence() {
        let a = node("a", 1, &[(1, 1)], 10, Some(1));
        let b = node("b", 1, &[(1, 1)], 8, Some(2));
        assert_eq!(compare(&a, &b), Comparison::Later(Side::A));
        assert_eq!(compare(&b, &a), Comparison::Later(Side::B));
        assert_eq!(compare(&a, &a.clone()), Comparison::Equal);
        assert_eq!(compare(&a, &node("a2", 1, &[(1, 1)], 10, Some(9))), Comparison::Diverged { fork: 10 });
        // Promoted at 11: the old-epoch node behind the fork is behind; past it, diverged.
        let promoted = node("p", 1, &[(1, 1), (2, 11)], 12, Some(3));
        assert_eq!(compare(&node("c", 1, &[(1, 1)], 10, None), &promoted), Comparison::Later(Side::B));
        assert_eq!(compare(&node("d", 1, &[(1, 1)], 11, None), &promoted), Comparison::Diverged { fork: 11 });
        // Two promotions of the same epoch at different points.
        let other = node("q", 1, &[(1, 1), (2, 15)], 16, None);
        assert_eq!(compare(&promoted, &other), Comparison::Diverged { fork: 11 });
        assert_eq!(compare(&a, &node("x", 2, &[(1, 1)], 10, None)), Comparison::DifferentDatabase);
        let empty = NodeInfo { identity: None, epoch_history: vec![], ..node("e", 1, &[(1, 1)], 0, None) };
        assert_eq!(compare(&empty, &a), Comparison::Later(Side::B));
    }

    #[test]
    fn the_chooser_picks_the_most_advanced_and_flags_the_rest() {
        let nodes = vec![
            node("b", 1, &[(1, 1)], 10, Some(1)),
            node("c", 1, &[(1, 1)], 12, Some(2)),
            node("d", 1, &[(1, 1), (2, 11)], 11, Some(3)),
            node("x", 9, &[(1, 1)], 50, None),
        ];
        // d has the newest epoch: it wins; c holds entries past d's fork (11, 12): diverged.
        let choice = choose_promotion_candidate(&nodes).unwrap();
        assert_eq!(choice.candidate.node_id, "d");
        assert_eq!(choice.compatible, vec!["b".to_string()]);
        assert_eq!(choice.diverged, vec![("c".to_string(), 11)]);
        assert_eq!(choice.foreign, vec!["x".to_string()]);

        let same_epoch = vec![node("b", 1, &[(1, 1)], 10, None), node("c", 1, &[(1, 1)], 12, None)];
        assert_eq!(choose_promotion_candidate(&same_epoch).unwrap().candidate.node_id, "c", "highest seq");
        let tie = vec![node("c", 1, &[(1, 1)], 12, Some(1)), node("b", 1, &[(1, 1)], 12, Some(1))];
        assert_eq!(choose_promotion_candidate(&tie).unwrap().candidate.node_id, "b", "lowest id on a tie");
        let empty = NodeInfo { identity: None, epoch_history: vec![], ..node("e", 1, &[(1, 1)], 0, None) };
        assert!(matches!(choose_promotion_candidate(&[empty]), Err(Error::Config(_))));
        assert!(matches!(choose_promotion_candidate(&[]), Err(Error::Config(_))));
    }
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib store::promote`
Expected: FAIL to compile: `no method named rebuild_ttl_index`, `no method named promote_to_master`, `no method named demote_to_slave`, `cannot find function compare in this scope`.

- [ ] **Step 3: Implement the store side** (above the test module in `src/store/promote.rs`)

```rust
//! Store side of promoting a slave to master (DESIGN.md §13.1).

use rocksdb::{IteratorMode, WriteBatch, WriteOptions};

use super::meta::{self, key, CF_CHANGELOG, CF_META, CF_TTL_INDEX};
use super::ttl::index_key;
use super::Store;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::types::{DbIdentity, EpochStart, LogEntry, Role};

const BATCH: usize = 1_000;

impl Store {
    /// Rebuilds the derived `__ttl_index` from a full scan of the envelopes: cleared, then one
    /// entry per stored value that has an expiry. Returns how many entries were added.
    pub(crate) fn rebuild_ttl_index(&self) -> Result<u64> {
        let ttl = self.cf(CF_TTL_INDEX);
        self.delete_all_keys(&ttl)?;
        let mut batch = WriteBatch::default();
        let mut added = 0u64;
        let live: Vec<_> = self.cfs.read().values().cloned().collect();
        for phys in live {
            let handle = self.handle(&phys)?;
            for item in self.db().iterator_cf(&handle, IteratorMode::Start) {
                let (k, v) = item?;
                if let Some(at) = envelope::decode(&v)?.meta.expire_at_ms {
                    batch.put_cf(&ttl, index_key(at, &phys.entry, &k), []);
                    added += 1;
                    if batch.len() >= BATCH {
                        self.db().write(std::mem::take(&mut batch))?;
                    }
                }
            }
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        Ok(added)
    }

    /// One synced batch: role = master, a new epoch starting at `last_seq + 1`, `last_ts`
    /// from the entry at `last_seq` (0 if it is no longer retained), slave-only keys removed.
    pub(crate) fn promote_to_master(&self) -> Result<DbIdentity> {
        let m = self.load_meta()?.ok_or_else(|| Error::corrupt("__meta is empty"))?;
        let old = m.identity.ok_or_else(|| Error::Config("this slave has no data to promote".into()))?;
        let mut history = m.epoch_history;
        let epoch = history.iter().map(|e| e.epoch).max().unwrap_or(old.epoch).max(old.epoch) + 1;
        history.push(EpochStart { epoch, start_seq: m.last_seq + 1 });
        let identity = DbIdentity { db_uuid: old.db_uuid, epoch };
        let last_ts = match self.db().get_cf(&self.cf(CF_CHANGELOG), m.last_seq.to_be_bytes())? {
            Some(v) => LogEntry::decode(&v)?.ts_ms,
            None => 0,
        };
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ROLE, Role::Master.as_bytes());
        batch.put_cf(&meta_cf, key::ORIGIN_IDENTITY, meta::encode_identity(&identity));
        batch.put_cf(&meta_cf, key::EPOCH_HISTORY, meta::encode_epoch_history(&history));
        batch.put_cf(&meta_cf, key::LAST_TS, last_ts.to_be_bytes());
        for k in [key::NODE_STATE, key::MAX_ENTRY_BYTES, key::BOOTSTRAP_CFS] {
            batch.delete_cf(&meta_cf, k);
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        Ok(identity)
    }
}

impl Store {
    /// Undoes a promotion that has not taken writes (addendum §4): one synced batch sets
    /// role = slave and restores the identity and history from before the newest epoch,
    /// whose record is removed. Refused (`Error::Config`, nothing written) if the newest epoch
    /// already has entries, or if there is no earlier epoch to return to.
    pub(crate) fn demote_to_slave(&self) -> Result<DbIdentity> {
        let m = self.load_meta()?.ok_or_else(|| Error::corrupt("__meta is empty"))?;
        let id = m.identity.ok_or_else(|| Error::corrupt("a master without identity"))?;
        let mut history = m.epoch_history;
        let newest = *history.last().ok_or_else(|| Error::corrupt("a master without epoch history"))?;
        if history.len() < 2 {
            return Err(Error::Config("nothing to undo: this is the database's first epoch".into()));
        }
        if m.last_seq >= newest.start_seq {
            return Err(Error::Config(format!(
                "epoch {} already has entries ({}..={}); demoting would discard them",
                newest.epoch, newest.start_seq, m.last_seq
            )));
        }
        history.pop();
        let previous = DbIdentity { db_uuid: id.db_uuid, epoch: history.last().map_or(1, |e| e.epoch) };
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ROLE, Role::Slave.as_bytes());
        batch.put_cf(&meta_cf, key::ORIGIN_IDENTITY, meta::encode_identity(&previous));
        batch.put_cf(&meta_cf, key::EPOCH_HISTORY, meta::encode_epoch_history(&history));
        batch.put_cf(&meta_cf, key::NODE_STATE, crate::store::bootstrap::NODE_READY);
        batch.delete_cf(&meta_cf, key::LAST_TS);
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        // Slaves keep no TTL index. Cleared after the role switch: a crash in between leaves a
        // slave with a stale index, which nothing reads and the next promotion rebuilds.
        self.delete_all_keys(&self.cf(CF_TTL_INDEX))?;
        Ok(previous)
    }
}
```

`demote_to_slave` writes its synced batch first and clears the TTL index afterwards: a crash in between leaves a slave with a stale index, which nothing reads and the next promotion rebuilds.

Run: `cargo test --lib store::promote`
Expected: PASS, 3 tests: `promotion_rewrites_meta_and_rebuilds_the_ttl_index`, `demotion_undoes_an_unused_promotion_only`, `a_slave_without_data_cannot_be_promoted`.

- [ ] **Step 4: Add `Error::NotMostAdvanced`**

In `src/error.rs`, after the `Halted` variant add:

```rust
    /// `promote_checked` found a reachable peer that holds more of the log.
    #[error("{leader} is more advanced (last_seq {seq}); promote it instead")]
    NotMostAdvanced { leader: String, seq: Seq },
```

- [ ] **Step 5: Implement the promotion tooling**

Replace `src/admin.rs` with (the module doc carries the failover runbook; the tests from Step 1 stay at the end):

```rust
//! Operator tooling for failover (addendum §4): node status, comparing nodes, and choosing
//! which replica to promote.
//!
//! # Failover runbook
//!
//! 1. Fence the old master (stop it, or cut it off from clients and replicas).
//! 2. Gather the status of every replica: [`node_status`] over the network, or
//!    [`Slave::promotion_info`](crate::Slave::promotion_info) in-process.
//! 3. Promote the most advanced compatible one: [`choose_promotion_candidate`], then
//!    [`Slave::promote_checked`](crate::Slave::promote_checked), which refuses if a reachable
//!    peer is later.
//! 4. `Slave::repoint` every other replica at the new master.
//!
//! With `WriteConcern::min_insync_replicas >= 1`, the most advanced in-sync replica holds
//! every acknowledged write.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::net::convert::{epochs_from_pb, identity_from_pb};
use crate::pb;
use crate::slave::UpstreamConfig;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

/// What a node reports about itself (`Status` RPC, `Slave::promotion_info`).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeInfo {
    pub node_id: String,
    pub role: Role,
    /// `None` while the node has no data (fresh, or bootstrapping).
    pub identity: Option<DbIdentity>,
    pub epoch_history: Vec<EpochStart>,
    pub last_seq: Seq,
    pub last_entry_crc: Option<u32>,
    /// `ts_ms` of the entry at `last_seq`; 0 if it is no longer retained.
    pub last_ts_ms: u64,
    /// `ready`, `bootstrapping` or `halted:<Reason>`.
    pub node_state: String,
    /// The upstream address; `None` on the master.
    pub upstream: Option<String>,
    pub replicas: Vec<ReplicaSummary>,
}

/// One direct downstream replica, as reported by `Status`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaSummary {
    pub node_id: String,
    pub applied_seq: Seq,
    pub in_sync: bool,
}

impl NodeInfo {
    pub(crate) fn from_pb(p: pb::StatusResponse) -> Result<NodeInfo> {
        let role = match p.role.as_str() {
            "master" => Role::Master,
            "slave" => Role::Slave,
            other => return Err(Error::corrupt(format!("unknown role {other:?} in Status"))),
        };
        Ok(NodeInfo {
            node_id: p.node_id,
            role,
            identity: p.identity.as_ref().map(identity_from_pb).transpose()?,
            epoch_history: epochs_from_pb(&p.epoch_history),
            last_seq: p.last_seq,
            last_entry_crc: p.last_entry_crc,
            last_ts_ms: p.last_ts_ms,
            node_state: p.node_state,
            upstream: Some(p.upstream).filter(|u| !u.is_empty()),
            replicas: p
                .replicas
                .into_iter()
                .map(|r| ReplicaSummary { node_id: r.node_id, applied_seq: r.applied_seq, in_sync: r.in_sync })
                .collect(),
        })
    }

    fn newest(&self) -> (u64, Seq) {
        (self.epoch_history.last().map_or(0, |e| e.epoch), self.last_seq)
    }
}

/// Which side of a comparison.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Side {
    A,
    B,
}

/// How two nodes' data relate.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Comparison {
    /// Compatible histories; this side holds everything the other holds, and more.
    Later(Side),
    Equal,
    /// Both hold different entries from `fork` on.
    Diverged { fork: Seq },
    /// Different databases (different `db_uuid`).
    DifferentDatabase,
}

/// The first seq whose epoch differs between two histories, if any.
fn fork_between(a: &[EpochStart], b: &[EpochStart]) -> Option<Seq> {
    for i in 0..a.len().max(b.len()) {
        match (a.get(i), b.get(i)) {
            (Some(x), Some(y)) if x == y => continue,
            (Some(x), Some(y)) => return Some(x.start_seq.min(y.start_seq)),
            (Some(x), None) | (None, Some(x)) => return Some(x.start_seq),
            (None, None) => break,
        }
    }
    None
}

/// Compares two nodes of a topology. A node without data is behind any node with data.
/// Two histories are compatible when every seq both nodes hold carries the same epoch in
/// both; then the later node is the one with the newer epoch, then the higher `last_seq`.
/// Equal positions with different `last_entry_crc` are `Diverged`.
pub fn compare(a: &NodeInfo, b: &NodeInfo) -> Comparison {
    let (ia, ib) = match (a.identity, b.identity) {
        (None, None) => return Comparison::Equal,
        (None, Some(_)) => return Comparison::Later(Side::B),
        (Some(_), None) => return Comparison::Later(Side::A),
        (Some(ia), Some(ib)) => (ia, ib),
    };
    if ia.db_uuid != ib.db_uuid {
        return Comparison::DifferentDatabase;
    }
    if let Some(fork) = fork_between(&a.epoch_history, &b.epoch_history) {
        if a.last_seq.min(b.last_seq) >= fork {
            return Comparison::Diverged { fork };
        }
    }
    match a.newest().cmp(&b.newest()) {
        std::cmp::Ordering::Greater => Comparison::Later(Side::A),
        std::cmp::Ordering::Less => Comparison::Later(Side::B),
        std::cmp::Ordering::Equal => match (a.last_entry_crc, b.last_entry_crc) {
            (Some(x), Some(y)) if x != y => Comparison::Diverged { fork: a.last_seq },
            _ => Comparison::Equal,
        },
    }
}

/// The node to promote, and how the others relate to it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Choice {
    pub candidate: NodeInfo,
    /// Compatible nodes: behind the candidate or equal to it. They can follow it.
    pub compatible: Vec<String>,
    /// Nodes holding entries the candidate does not (`node_id`, fork seq): repointing them
    /// needs an explicit decision to discard those entries.
    pub diverged: Vec<(String, Seq)>,
    /// Nodes of another database.
    pub foreign: Vec<String>,
}

/// Picks the most advanced node holding data (newest epoch, then highest `last_seq`, then
/// lowest `node_id`) and classifies the others against it. `Err(Config)` if no node holds data.
pub fn choose_promotion_candidate(nodes: &[NodeInfo]) -> Result<Choice> {
    let candidate = nodes
        .iter()
        .filter(|n| n.identity.is_some())
        .max_by(|x, y| x.newest().cmp(&y.newest()).then_with(|| y.node_id.cmp(&x.node_id)))
        .ok_or_else(|| Error::Config("no node holds data".into()))?
        .clone();
    let mut choice = Choice { candidate, compatible: Vec::new(), diverged: Vec::new(), foreign: Vec::new() };
    for n in nodes.iter().filter(|n| n.node_id != choice.candidate.node_id) {
        match compare(&choice.candidate, n) {
            Comparison::Later(Side::A) | Comparison::Equal => choice.compatible.push(n.node_id.clone()),
            // Not reachable for the most advanced node, but never lose a node silently.
            Comparison::Later(Side::B) => choice.compatible.push(n.node_id.clone()),
            Comparison::Diverged { fork } => choice.diverged.push((n.node_id.clone(), fork)),
            Comparison::DifferentDatabase => choice.foreign.push(n.node_id.clone()),
        }
    }
    Ok(choice)
}

/// Queries a node's `Status` over the network. `client_id` identifies the caller (it must be
/// a valid node identifier); `upstream` carries the address, TLS and credentials.
pub async fn node_status(upstream: &UpstreamConfig, client_id: &str) -> Result<NodeInfo> {
    let mut c = crate::slave::connect_upstream(upstream, client_id, Duration::from_secs(5))?;
    let r = c
        .status(pb::StatusRequest { client_id: client_id.to_string() })
        .await
        .map_err(|s| Error::Config(format!("Status from {}: {}: {}", upstream.uri(), s.code(), s.message())))?;
    NodeInfo::from_pb(r.into_inner())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn node(id: &str, uuid: u128, history: &[(u64, Seq)], last: Seq, crc: Option<u32>) -> NodeInfo {
        let epoch_history: Vec<EpochStart> = history.iter().map(|&(epoch, start_seq)| EpochStart { epoch, start_seq }).collect();
        NodeInfo {
            node_id: id.into(),
            role: Role::Slave,
            identity: epoch_history.last().map(|e| DbIdentity { db_uuid: uuid::Uuid::from_u128(uuid), epoch: e.epoch }),
            epoch_history,
            last_seq: last,
            last_entry_crc: crc,
            last_ts_ms: 0,
            node_state: "ready".into(),
            upstream: None,
            replicas: vec![],
        }
    }

    #[test]
    fn compare_orders_compatible_nodes_and_detects_divergence() {
        let a = node("a", 1, &[(1, 1)], 10, Some(1));
        let b = node("b", 1, &[(1, 1)], 8, Some(2));
        assert_eq!(compare(&a, &b), Comparison::Later(Side::A));
        assert_eq!(compare(&b, &a), Comparison::Later(Side::B));
        assert_eq!(compare(&a, &a.clone()), Comparison::Equal);
        assert_eq!(compare(&a, &node("a2", 1, &[(1, 1)], 10, Some(9))), Comparison::Diverged { fork: 10 });
        // Promoted at 11: the old-epoch node behind the fork is behind; past it, diverged.
        let promoted = node("p", 1, &[(1, 1), (2, 11)], 12, Some(3));
        assert_eq!(compare(&node("c", 1, &[(1, 1)], 10, None), &promoted), Comparison::Later(Side::B));
        assert_eq!(compare(&node("d", 1, &[(1, 1)], 11, None), &promoted), Comparison::Diverged { fork: 11 });
        // Two promotions of the same epoch at different points.
        let other = node("q", 1, &[(1, 1), (2, 15)], 16, None);
        assert_eq!(compare(&promoted, &other), Comparison::Diverged { fork: 11 });
        assert_eq!(compare(&a, &node("x", 2, &[(1, 1)], 10, None)), Comparison::DifferentDatabase);
        let empty = NodeInfo { identity: None, epoch_history: vec![], ..node("e", 1, &[(1, 1)], 0, None) };
        assert_eq!(compare(&empty, &a), Comparison::Later(Side::B));
    }

    #[test]
    fn the_chooser_picks_the_most_advanced_and_flags_the_rest() {
        let nodes = vec![
            node("b", 1, &[(1, 1)], 10, Some(1)),
            node("c", 1, &[(1, 1)], 12, Some(2)),
            node("d", 1, &[(1, 1), (2, 11)], 11, Some(3)),
            node("x", 9, &[(1, 1)], 50, None),
        ];
        // d has the newest epoch: it wins; c holds entries past d's fork (11, 12): diverged.
        let choice = choose_promotion_candidate(&nodes).unwrap();
        assert_eq!(choice.candidate.node_id, "d");
        assert_eq!(choice.compatible, vec!["b".to_string()]);
        assert_eq!(choice.diverged, vec![("c".to_string(), 11)]);
        assert_eq!(choice.foreign, vec!["x".to_string()]);

        let same_epoch = vec![node("b", 1, &[(1, 1)], 10, None), node("c", 1, &[(1, 1)], 12, None)];
        assert_eq!(choose_promotion_candidate(&same_epoch).unwrap().candidate.node_id, "c", "highest seq");
        let tie = vec![node("c", 1, &[(1, 1)], 12, Some(1)), node("b", 1, &[(1, 1)], 12, Some(1))];
        assert_eq!(choose_promotion_candidate(&tie).unwrap().candidate.node_id, "b", "lowest id on a tie");
        let empty = NodeInfo { identity: None, epoch_history: vec![], ..node("e", 1, &[(1, 1)], 0, None) };
        assert!(matches!(choose_promotion_candidate(&[empty]), Err(Error::Config(_))));
        assert!(matches!(choose_promotion_candidate(&[]), Err(Error::Config(_))));
    }
}
```

Create `src/slave/promote.rs`:

```rust
//! Promotion of a slave to master (DESIGN.md §13.1).

use std::sync::Arc;
use std::time::Duration;

use super::state::{HaltReason, SlaveState};
use super::{Slave, UpstreamConfig};
use crate::admin::{self, compare, Comparison, NodeInfo, Side};
use crate::config::MasterConfig;
use crate::error::{Error, Result};
use crate::master::Master;
use crate::server::status::node_info;
use crate::server::ServerHandle;
use crate::store::Store;
use crate::types::{Role, Seq};

impl Slave {
    /// Turns this slave into the master. **Operator action: stop or fence the old master
    /// first** — the crate cannot detect two live masters, only the divergence they cause.
    ///
    /// Refused with `Error::Config` while bootstrapping, without data, when halted with
    /// `Corrupt` or `SpecMismatch`, or if `cfg.path` is not this slave's path. Every
    /// `ReadSnapshot` taken from this slave must be dropped first: one that is alive keeps
    /// the database open and the promotion fails. In every failure case the slave has been
    /// shut down and its directory is unchanged, so it can be reopened as a slave (after the
    /// last snapshot is dropped).
    ///
    /// Downstream streams end with `UPSTREAM_RESET`. The new master starts epoch
    /// `1 + max(epoch history)` at `applied_seq() + 1` and rebuilds its TTL index, so values
    /// written with a TTL before the promotion are reaped by it.
    pub async fn promote(self, cfg: MasterConfig) -> Result<Master> {
        if let Some(msg) = self.promote_refusal(&cfg) {
            self.shutdown().await?;
            return Err(Error::Config(msg));
        }
        self.promote_now(cfg).await
    }

    /// Why `promote(cfg)` would be refused before it changes anything, if it would.
    fn promote_refusal(&self, cfg: &MasterConfig) -> Option<String> {
        match self.inner.state() {
            SlaveState::Bootstrapping { .. } => Some("cannot promote while bootstrapping".to_string()),
            SlaveState::Halted(r @ (HaltReason::Corrupt | HaltReason::SpecMismatch)) => {
                Some(format!("cannot promote a slave halted with {r:?}"))
            }
            _ if !self.inner.is_ready() => Some("this slave has no consistent data to promote".to_string()),
            _ if cfg.path != self.inner.cfg.path => Some("MasterConfig.path must be the slave's path".to_string()),
            _ => cfg.validate().err().map(|e| e.to_string()),
        }
    }

    /// The irreversible part of `promote`: from here on a failure loses the slave handle.
    async fn promote_now(self, cfg: MasterConfig) -> Result<Master> {
        let path = cfg.path.clone();
        let tuning = cfg.rocksdb.clone();
        // Snapshots share the database handle; one that outlives the slave keeps the DB open.
        let db = Arc::downgrade(self.inner.store.db());
        self.inner.reset_downstream();
        self.shutdown().await?;
        if db.strong_count() > 0 {
            return Err(Error::Config(
                "a ReadSnapshot of this slave is still open and keeps the database open; drop it and reopen the slave"
                    .into(),
            ));
        }
        crate::background::blocking(move || {
            let (store, _) = Store::open(&path, Role::Slave, tuning)?;
            let indexed = store.rebuild_ttl_index()?;
            let identity = store.promote_to_master()?;
            tracing::info!(epoch = identity.epoch, ttl_entries = indexed, "slave promoted to master");
            Ok(())
        })
        .await?;
        Master::open(cfg).await
    }
}

/// What `check_promotion` found among the peers (none of them is later than this slave).
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct PromotionCheck {
    /// Peers whose `Status` could not be read (address and error). They do not block.
    pub unreachable: Vec<(String, String)>,
    /// Peers that hold entries this slave does not (`node_id`, fork seq).
    pub diverged: Vec<(String, Seq)>,
    /// Peers of another database.
    pub foreign: Vec<String>,
}

impl Slave {
    /// What this slave reports about itself; the same fields as the `Status` RPC.
    pub fn promotion_info(&self) -> NodeInfo {
        let lag_max = self.server.as_ref().map_or(Duration::from_secs(10), ServerHandle::replica_lag_time_max);
        node_info(&*self.inner, &self.inner.replicas, lag_max)
    }

    /// Asks every reachable peer for its `Status` and refuses with `Error::NotMostAdvanced`
    /// if one holds more of the log than this slave (addendum §4). Unreachable, diverged and
    /// foreign peers are reported, not refused: the operator decides.
    pub async fn check_promotion(&self, peers: &[UpstreamConfig]) -> Result<PromotionCheck> {
        let me = self.promotion_info();
        let mut check = PromotionCheck::default();
        for peer in peers {
            let info = match admin::node_status(peer, &me.node_id).await {
                Ok(info) => info,
                Err(e) => {
                    tracing::warn!(peer = %peer.uri(), error = %e, "promotion check: peer unreachable");
                    check.unreachable.push((peer.uri(), e.to_string()));
                    continue;
                }
            };
            match compare(&me, &info) {
                Comparison::Later(Side::B) => {
                    return Err(Error::NotMostAdvanced { leader: info.node_id, seq: info.last_seq });
                }
                Comparison::Diverged { fork } => check.diverged.push((info.node_id, fork)),
                Comparison::DifferentDatabase => check.foreign.push(info.node_id),
                Comparison::Later(Side::A) | Comparison::Equal => {}
            }
        }
        Ok(check)
    }

    /// [`check_promotion`](Self::check_promotion), then [`promote`](Self::promote), refusing
    /// by default. **A refusal hands the slave back untouched and still running**
    /// ([`PromoteRefused::into_slave`]): the checks of `promote` (bootstrapping, no data,
    /// halted `Corrupt`/`SpecMismatch`, wrong `cfg.path`, invalid `cfg`), a later reachable peer
    /// (`Error::NotMostAdvanced`), or a peer check that could not run.
    ///
    /// Only a failure after the slave has started shutting down for the promotion loses the
    /// handle (`PromoteRefused::slave` is `None`): an I/O error during shutdown, a
    /// `ReadSnapshot` that still holds the database (`Error::Config`; drop it and reopen the
    /// directory as a slave, which is unchanged), or a failure while rewriting it.
    pub async fn promote_checked(self, cfg: MasterConfig, peers: &[UpstreamConfig]) -> Result<Master, PromoteRefused> {
        if let Some(msg) = self.promote_refusal(&cfg) {
            return Err(PromoteRefused { error: Error::Config(msg), slave: Some(Box::new(self)) });
        }
        match self.check_promotion(peers).await {
            Ok(check) => {
                if !check.unreachable.is_empty() || !check.diverged.is_empty() || !check.foreign.is_empty() {
                    tracing::warn!(?check, "promoting despite unreachable, diverged or foreign peers");
                }
                self.promote_now(cfg).await.map_err(|error| PromoteRefused { error, slave: None })
            }
            Err(error) => Err(PromoteRefused { error, slave: Some(Box::new(self)) }),
        }
    }
}

/// A refused or failed [`Slave::promote_checked`]. `slave` is the untouched, still-running
/// slave for every refusal; it is `None` only when the promotion failed after the slave had
/// started shutting down.
pub struct PromoteRefused {
    pub error: Error,
    /// Boxed to keep the `Err` variant small.
    pub slave: Option<Box<Slave>>,
}

impl PromoteRefused {
    /// The slave, still running, if the promotion was refused before it changed anything.
    pub fn into_slave(self) -> Option<Slave> {
        self.slave.map(|s| *s)
    }
}

impl std::fmt::Debug for PromoteRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PromoteRefused").field("error", &self.error).field("slave_returned", &self.slave.is_some()).finish()
    }
}

impl std::fmt::Display for PromoteRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "promotion refused: {}", self.error)
    }
}

impl std::error::Error for PromoteRefused {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
```

In `src/slave/mod.rs`, add `mod promote;` to the module list, make the client constructor visible to `admin` (after `mod client;`):

```rust
pub(crate) use client::connect_upstream;
```

and add after the `pub use config::{…};` line:

```rust
pub use promote::{PromoteRefused, PromotionCheck};
```

In `src/lib.rs`, add `PromoteRefused` and `PromotionCheck` to the `pub use slave::{…}` list, and replace `pub use master::{Master, ReapStats};` with `pub use master::{DemoteRefused, Master, ReapStats};`.

In `src/master/mod.rs`, add to `impl Master` after `serve_addr`:

```rust
    /// Whether [`demote`](Self::demote) would be allowed now: the newest epoch has no entries
    /// yet (`last_seq < its start_seq`) and there is an earlier epoch to return to.
    /// `Err(Config)` says why not. The master keeps running either way.
    pub fn can_demote(&self) -> Result<()> {
        let history = self.epoch_history();
        let last = self.last_seq();
        match history.last() {
            _ if history.len() < 2 => Err(crate::Error::Config("nothing to undo: this is the database's first epoch".into())),
            Some(newest) if last >= newest.start_seq => Err(crate::Error::Config(format!(
                "epoch {} already has entries ({}..={last}); demoting would discard them",
                newest.epoch, newest.start_seq
            ))),
            _ => Ok(()),
        }
    }

    /// Undoes a promotion of the wrong node (addendum §4): allowed only while the newest epoch
    /// has no entries, so nothing is lost. Persists role = slave, restores the identity and
    /// epoch history from before the promotion, and opens the directory as a slave with `cfg`
    /// (`cfg.path` must be this master's path).
    ///
    /// **Refused by default:** when [`can_demote`](Self::can_demote) fails or `cfg.path`
    /// differs, the master is handed back untouched and still running
    /// ([`DemoteRefused::into_master`]). Only a failure after the master has started shutting
    /// down for the demotion loses the handle (`DemoteRefused::master` is `None`): an I/O
    /// error during shutdown, a `ReadSnapshot` that still holds the database (`Error::Config`;
    /// drop it and reopen the directory, which is unchanged, as the master), or a failure
    /// while rewriting it or opening the slave.
    pub async fn demote(self, cfg: crate::slave::SlaveConfig) -> Result<crate::slave::Slave, DemoteRefused> {
        let refusal = match self.can_demote() {
            Err(e) => Some(e),
            Ok(()) if cfg.path != self.inner.cfg.path => {
                Some(crate::Error::Config("SlaveConfig.path must be the master's path".into()))
            }
            Ok(()) => None,
        };
        if let Some(error) = refusal {
            return Err(DemoteRefused { error, master: Some(Box::new(self)) });
        }
        let lost = |error| DemoteRefused { error, master: None };
        let (path, tuning) = (cfg.path.clone(), cfg.rocksdb.clone());
        let db = Arc::downgrade(self.inner.store.db());
        self.shutdown().await.map_err(lost)?;
        if db.strong_count() > 0 {
            return Err(lost(crate::Error::Config(
                "a ReadSnapshot of this master is still open and keeps the database open; drop it first".into(),
            )));
        }
        crate::background::blocking(move || {
            let (store, _) = Store::open(&path, Role::Master, tuning)?;
            let identity = store.demote_to_slave()?;
            tracing::info!(epoch = identity.epoch, "master demoted to slave");
            Ok(())
        })
        .await
        .map_err(lost)?;
        crate::slave::Slave::open(cfg).await.map_err(lost)
    }
```

and before `impl ReadApi for Master`:

```rust
/// A refused or failed [`Master::demote`]. `master` is the untouched, still-running master
/// for every refusal; it is `None` only when the demotion failed after the master had started
/// shutting down.
pub struct DemoteRefused {
    pub error: crate::Error,
    /// Boxed to keep the `Err` variant small.
    pub master: Option<Box<Master>>,
}

impl DemoteRefused {
    /// The master, still running, if the demotion was refused before it changed anything.
    pub fn into_master(self) -> Option<Master> {
        self.master.map(|m| *m)
    }
}

impl std::fmt::Debug for DemoteRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DemoteRefused").field("error", &self.error).field("master_returned", &self.master.is_some()).finish()
    }
}

impl std::fmt::Display for DemoteRefused {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "demotion refused: {}", self.error)
    }
}

impl std::error::Error for DemoteRefused {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        Some(&self.error)
    }
}
```

- [ ] **Step 6: Run the tests**

Run: `cargo test --lib admin && cargo test --test promotion`
Expected: PASS: `admin::tests::{compare_orders_compatible_nodes_and_detects_divergence, the_chooser_picks_the_most_advanced_and_flags_the_rest}`, and 7 tests in `promotion`: `promotion_end_to_end` (including that the new master's reaper `Delete` reaches C through `entries_since`), `the_old_master_is_refused_with_stale_epoch_and_the_client_retries`, `promotion_is_refused_without_data_and_needs_released_snapshots` (`Error::Config` naming the open `ReadSnapshot`; the directory reopens as a slave afterwards), `the_status_rpc_and_promotion_info_report_positions` (the `Status` RPC and `promotion_info` agree; the master lists its in-sync replica), `promote_checked_refuses_when_a_reachable_peer_is_later` (the refusal returns B still running — it serves reads, a second refusal for a wrong path returns it too, and it then serves a new downstream replica to convergence; the most advanced node passes with an unreachable peer listed), `demote_undoes_a_wrong_promotion_with_zero_loss` (the old identity is back, data identical, a sibling promoted instead is followed without a bootstrap), `demote_is_refused_once_the_new_epoch_has_entries` (the refusal returns the master still running: its write is readable, it takes another write and a new replica converges).

Run: `cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings`
Expected: no warnings.

- [ ] **Step 7: Update DESIGN.md**

After step 5 of §13.1 (`5. Open with Master::open(cfg). …`), add:

```
`promote(self)` consumes the slave. If it is refused (bootstrapping, no data, halted `Corrupt`/`SpecMismatch`, wrong `cfg.path`, invalid `cfg`) or fails (for example because a `ReadSnapshot` of the slave is still alive and keeps the database open), the slave has been shut down cleanly and its directory is unchanged, so it can be reopened as a slave.
```

In DESIGN.md §13.1, after:

```
`promote(self)` consumes the slave. If it is refused (bootstrapping, no data, halted `Corrupt`/`SpecMismatch`, wrong `cfg.path`, invalid `cfg`) or fails (for example because a `ReadSnapshot` of the slave is still alive and keeps the database open), the slave has been shut down cleanly and its directory is unchanged, so it can be reopened as a slave.
```

add:

```
**Promotion tooling.**
- `Slave::promotion_info()` returns the same `NodeInfo` as the `Status` RPC (PROTOCOL.md §6.5); `admin::node_status(upstream, client_id)` fetches it over the network.
- `admin::compare(a, b)` → `Later(A | B)`, `Equal`, `Diverged { fork }` or `DifferentDatabase`. Two histories are compatible when every seq both nodes hold carries the same epoch in both; then the node with the newer epoch is later, then the one with the higher `last_seq`. Equal positions with different `last_entry_crc` have diverged. A node without data is behind every node with data.
- `admin::choose_promotion_candidate(&[NodeInfo])` picks the most advanced node holding data (newest epoch, then highest `last_seq`, then lowest node id) and lists the others as compatible, diverged (`(node_id, fork)`) or foreign.
- `Slave::promote_checked(cfg, peers)` reads every peer's `Status` first and refuses with `Error::NotMostAdvanced { leader, seq }` if a reachable peer is later. Unreachable, diverged and foreign peers are logged and do not block: that is the operator's call. `Slave::check_promotion(peers)` runs the same check without promoting.
- `Master::demote(cfg: SlaveConfig)` undoes a promotion of the wrong node. It is allowed only while the newest epoch has no entries (`last_seq < its start_seq`), so nothing is lost: it removes that epoch record, restores the previous identity, persists `role = slave` and opens the directory as a slave. `Master::can_demote()` checks this without consuming the master.
- `promote_checked` and `demote` refuse by default and **hand the untouched, still-running node back**: `Err(PromoteRefused { error, slave })` / `Err(DemoteRefused { error, master })`, with `into_slave()` / `into_master()`. Only a failure after the node has started shutting down for the change (an I/O error during shutdown, a `ReadSnapshot` that still holds the database, a failure while rewriting `__meta`) loses the handle (`None`); the directory is then reopened by the application. Plain `promote` keeps its behaviour: a refusal shuts the slave down with its directory unchanged.
```

In DESIGN.md §16 (impl Master), after:

```
    pub fn bump_epoch(&self) -> Result<DbIdentity>;
```

add:

```
    pub fn can_demote(&self) -> Result<()>;           // Err(Config) says why demote would be refused
    pub async fn demote(self, cfg: SlaveConfig) -> Result<Slave, DemoteRefused>;  // undo an unused promotion (§13.1);
                                                      // a refusal returns the running master
```

In DESIGN.md §16 (impl Slave), after:

```
    pub async fn promote(self, cfg: MasterConfig) -> Result<Master>;    // operator action (§13.1)
```

add:

```
    pub async fn promote_checked(self, cfg: MasterConfig, peers: &[UpstreamConfig]) -> Result<Master, PromoteRefused>;
                                             // refuses if a reachable peer is later; a refusal returns
                                             // the running slave (§13.1)
    pub async fn check_promotion(&self, peers: &[UpstreamConfig]) -> Result<PromotionCheck>;
    pub fn promotion_info(&self) -> NodeInfo;   // what the Status RPC reports
```

In DESIGN.md §16 (admin), after:

```
// A Slave has no write methods. Writing to it is a compile error.
```

add:

```
// ---------------- admin: failover tooling (§13.1) ----------------
pub struct NodeInfo {
    pub node_id: String, pub role: Role, pub identity: Option<DbIdentity>,
    pub epoch_history: Vec<EpochStart>, pub last_seq: Seq, pub last_entry_crc: Option<u32>,
    pub last_ts_ms: u64, pub node_state: String, pub upstream: Option<String>,
    pub replicas: Vec<ReplicaSummary>,          // { node_id, applied_seq, in_sync }
}
pub fn compare(a: &NodeInfo, b: &NodeInfo) -> Comparison;  // Later(Side) | Equal | Diverged { fork } | DifferentDatabase
pub fn choose_promotion_candidate(nodes: &[NodeInfo]) -> Result<Choice>; // { candidate, compatible, diverged, foreign }
pub async fn node_status(upstream: &UpstreamConfig, client_id: &str) -> Result<NodeInfo>;
```

In DESIGN.md §16.2, replace:

```
`Halted(HaltReason)` (from `wait_for_seq`), `Timeout`, `Io`, `RocksDb`.
```

with:

```
`Halted(HaltReason)` (from `wait_for_seq`), `Timeout`, `NotMostAdvanced { leader, seq }` (`promote_checked`), `Io`, `RocksDb`.
```

In DESIGN.md §20, after:

```
    auth.rs           traits + Token/AllowAll/Mtls impls, auth-failure rate limit
```

add:

```
    admin.rs          failover tooling: NodeInfo, compare, choose_promotion_candidate, node_status
```

In DESIGN.md §22, replace:

```
- Promotion is manual (`Slave::promote`). Fencing the old master is the operator's job; `STALE_EPOCH` and the CRC checks are backstops.
```

with:

```
- Promotion is manual (`Slave::promote`). Fencing the old master is the operator's job; `STALE_EPOCH` and the CRC checks are backstops. `promote_checked`, `admin::choose_promotion_candidate` and `Master::demote` help the operator pick the right node; they never act on their own.
```

- [ ] **Step 8: Commit**

```bash
git add src tests DESIGN.md
git commit -m "feat: promotion with checked promotion, candidate choice, Status-based comparison and demote

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 12: In-sync replicas and write concern

**Files:**
- Create: `src/master/write_concern.rs`, `tests/write_concern.rs`
- Modify: `src/config.rs` (`WriteConcern`, `MasterConfig::write_concern`), `src/master/mod.rs` (every write method, `write_with`, `put_with`), `src/server/replicas.rs` (drop two `allow`s), `src/error.rs`, `src/lib.rs`, `DESIGN.md` (§1, §5.2, §16, §16.2, §18, §21, §22)

**Interfaces:**
- Consumes: Task 6 `ReplicaRegistry::{acks, wait_until}`, `Inner.replicas`, `ServeLimits::replica_lag_time_max`, `test_client::{ack, TailExt}`; Task 11 `Slave::promote_checked` (tests); Plan 1 `master::writer::Inner::{commit_ops, identity}`.
- Produces:
  - `pub struct WriteConcern { pub min_insync_replicas: usize, pub ack_timeout: Duration }` (`Default` = `0` / 5 s; `WriteConcern::in_sync(n)`), `MasterConfig::write_concern` (validated: a positive `ack_timeout` when `min_insync_replicas > 0`).
  - `master::writer::Inner::commit_with(&self, &WriteConcern, build) -> Result<Seq>`: `NotEnoughReplicas` before the writer lock (nothing written, no seq consumed), then `commit_ops`, then `ReplicaRegistry::wait_until` without the writer lock; `AckTimeout` if N in-sync acks are not seen within `ack_timeout`.
  - `Master::write_with(&self, Batch, WriteConcern) -> Result<Seq>`, `Master::put_with(&self, cf, key, value, WriteConcern) -> Result<Seq>`; `put`, `put_with_ttl`, `delete`, `delete_range`, `write`, `create_cf`, `drop_cf` use `MasterConfig::write_concern`.
  - `Error::NotEnoughReplicas { required: usize, in_sync: usize }`, `Error::AckTimeout { seq: Seq, acked: usize, required: usize }`.

- [ ] **Step 1: Write the failing tests**

Create `src/master/write_concern.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::config::WriteConcern;
    use crate::error::Error;
    use crate::log::LogApi;
    use crate::server::test_client::{ack, client, eventually, next_batch, serving_master, tail_req, TailExt};
    use crate::types::CfSpec;

    fn concern(n: usize, timeout_ms: u64) -> WriteConcern {
        WriteConcern { min_insync_replicas: n, ack_timeout: Duration::from_millis(timeout_ms) }
    }

    /// A replica that stops acknowledging: the write is committed but the caller gets
    /// `AckTimeout`; the late ack still lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ack_timeout_leaves_the_write_committed() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        let m = Arc::new(m);
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
        next_batch(&mut s).await;
        acks.send(ack(1)).await.unwrap();
        eventually("in sync", || m.replicas().iter().any(|r| r.in_sync)).await;

        let m2 = m.clone();
        let r = tokio::task::spawn_blocking(move || m2.put_with("c", b"k", b"v", concern(1, 300))).await.unwrap();
        assert!(matches!(r, Err(Error::AckTimeout { seq: 2, acked: 0, required: 1 })), "{r:?}");
        assert_eq!(m.last_seq(), 2, "committed despite the timeout");
        assert_eq!(next_batch(&mut s).await[0].seq, 2, "and still replicated");
        acks.send(ack(2)).await.unwrap();
        eventually("the late ack", || m.replicas()[0].applied_seq == 2).await;
    }

    /// A write with N = 1 returns as soon as the in-sync replica acknowledges it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_returns_after_the_in_sync_ack() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        let m = Arc::new(m);
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
        next_batch(&mut s).await;
        acks.send(ack(1)).await.unwrap();
        eventually("in sync", || m.replicas().iter().any(|r| r.in_sync)).await;

        let m2 = m.clone();
        let write = tokio::task::spawn_blocking(move || m2.put_with("c", b"k", b"v", concern(1, 10_000)));
        let batch = next_batch(&mut s).await;
        assert!(!write.is_finished(), "waits for the ack");
        acks.send(ack(batch[0].seq)).await.unwrap();
        assert_eq!(write.await.unwrap().unwrap(), 2);
    }

    /// A replica that stops being caught up leaves the ISR after `replica_lag_time_max`,
    /// then `NotEnoughReplicas` refuses writes without consuming a seq; catching up rejoins.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lagging_replica_leaves_the_isr_and_rejoins() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| {
            cfg.serve.as_mut().unwrap().limits.replica_lag_time_max = Duration::from_millis(300);
        })
        .await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
        next_batch(&mut s).await;
        acks.send(ack(1)).await.unwrap();
        eventually("in sync", || m.replicas().iter().any(|r| r.in_sync)).await;

        m.put("c", b"unacked", b"v").unwrap();
        eventually("out of sync after 300 ms behind", || !m.replicas()[0].in_sync).await;
        let last = m.last_seq();
        let r = m.put_with("c", b"refused", b"v", concern(1, 1_000));
        assert!(matches!(r, Err(Error::NotEnoughReplicas { required: 1, in_sync: 0 })), "{r:?}");
        assert_eq!(m.last_seq(), last, "no seq consumed");

        assert_eq!(next_batch(&mut s).await[0].seq, last);
        acks.send(ack(last)).await.unwrap();
        eventually("back in sync", || m.replicas()[0].in_sync).await;
    }

    #[tokio::test]
    async fn without_replicas_a_write_concern_is_refused_and_consumes_no_seq() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let r = m.put_with("c", b"k", b"v", WriteConcern::in_sync(1));
        assert!(matches!(r, Err(Error::NotEnoughReplicas { required: 1, in_sync: 0 })), "{r:?}");
        assert_eq!(m.last_seq(), 1);
        let (embedded, _) = serving_master(tempfile::tempdir().unwrap().path(), |cfg| cfg.serve = None).await;
        embedded.create_cf(CfSpec::new("c")).unwrap();
        assert!(matches!(embedded.put_with("c", b"k", b"v", WriteConcern::in_sync(1)), Err(Error::NotEnoughReplicas { .. })));
        assert_eq!(embedded.put_with("c", b"k", b"v", WriteConcern::default()).unwrap(), 2, "N = 0 is asynchronous");
    }
}
```

Add `mod write_concern;` to the module list in `src/master/mod.rs`.

Create `tests/write_concern.rs`:

```rust
//! Write concern end to end (addendum §2, §6): real slaves acknowledge over gRPC.

mod support;

use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{CfSpec, Error, LogApi, Master, SlaveState, UpstreamConfig, WriteConcern};
use support::{assert_converged, wait_state, wait_until, Cluster};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_write_returns_after_a_real_slave_acknowledged_it() {
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_until("b in sync", || m.replicas().iter().any(|r| r.node_id == "b" && r.in_sync)).await;
    for i in 0..20u8 {
        let m2 = m.clone();
        let seq = tokio::task::spawn_blocking(move || m2.put_with("c", &[i], b"v", WriteConcern::in_sync(1)))
            .await
            .unwrap()
            .unwrap();
        assert!(b.last_seq() >= seq, "entry {seq} is committed on the slave when the write returns");
    }
    let r = m.replicas();
    assert_eq!((r.len(), r[0].applied_seq), (1, m.last_seq()));
}

/// The master is lost right after a write acknowledged by both in-sync replicas;
/// `promote_checked` promotes one of them and the write is there.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_acknowledged_write_survives_losing_the_master() {
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    let a_addr = m.serve_addr().unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, _) = c.slave("c", a_addr).await;
    for (name, s) in [("b", &b), ("c", &cn)] {
        wait_state(s, name, |st| *st == SlaveState::Streaming).await;
    }
    wait_until("both in sync", || m.replicas().iter().filter(|r| r.in_sync).count() == 2).await;
    let m2 = m.clone();
    let seq = tokio::task::spawn_blocking(move || m2.put_with("c", b"acked", b"v", WriteConcern::in_sync(2)))
        .await
        .unwrap()
        .unwrap();
    let m = Arc::try_unwrap(m).ok().expect("the writer task is done");
    m.shutdown().await.unwrap();

    let peers = [UpstreamConfig::from_addr(cn.serve_addr().unwrap()), UpstreamConfig::from_addr(a_addr)];
    let new_master = b.promote_checked(c.master_config("b"), &peers).await.unwrap();
    assert!(new_master.last_seq() >= seq);
    assert_eq!(new_master.get("c", b"acked").unwrap(), Some(b"v".to_vec()));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_configured_write_concern_applies_to_every_write() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    cfg.write_concern = WriteConcern { min_insync_replicas: 1, ack_timeout: Duration::from_secs(10) };
    let m = Master::open(cfg).await.unwrap();
    let r = m.create_cf(CfSpec::new("c"));
    assert!(matches!(r, Err(Error::NotEnoughReplicas { required: 1, in_sync: 0 })), "{r:?}");
    assert_eq!(m.last_seq(), 0);
    // An explicit asynchronous write needs no replica.
    assert_eq!(m.write_with(replicated_rocksdb::Batch::new(), WriteConcern::default()).err().map(|e| e.to_string()), Some("batch is empty".into()));
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    // The slave bootstraps the empty database (S = 0) and tails; then it is in sync.
    wait_until("b in sync", || m.replicas().iter().any(|r| r.in_sync)).await;
    let m = Arc::new(m);
    let m2 = m.clone();
    tokio::task::spawn_blocking(move || m2.create_cf(CfSpec::new("c"))).await.unwrap().unwrap();
    assert_converged(&*m, &[("b", &b)]).await;
}
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib master::write_concern`
Expected: FAIL to compile: `unresolved import crate::config::WriteConcern`, `no method named put_with`, `no variant named NotEnoughReplicas`.

- [ ] **Step 3: Configuration and errors**

In `src/config.rs`, before `pub struct MasterConfig` (and its `#[derive(Clone)]`) add:

```rust
/// How many in-sync replicas must acknowledge a write before it returns (addendum §2).
///
/// A direct replica of the master is *in sync* while its Tail stream is open, it follows the
/// master's current identity, and it has been fully caught up within
/// `ServeLimits::replica_lag_time_max` (default 10 s).
///
/// With `min_insync_replicas = N > 0` a write is refused with `Error::NotEnoughReplicas`
/// (nothing written, no seq consumed) if fewer than N replicas are in sync. Otherwise it is
/// committed and fsynced on the master as usual, and the call then waits for N in-sync replicas
/// to acknowledge it. **`Error::AckTimeout` does not undo the write**: the entry is committed
/// on the master and keeps replicating; the error only says that N acknowledgements were not
/// seen within `ack_timeout` (Kafka semantics).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct WriteConcern {
    /// 0 = asynchronous replication (the default).
    pub min_insync_replicas: usize,
    pub ack_timeout: Duration,
}

impl Default for WriteConcern {
    fn default() -> Self {
        WriteConcern { min_insync_replicas: 0, ack_timeout: Duration::from_secs(5) }
    }
}

impl WriteConcern {
    /// Wait for `n` in-sync replicas, with the default 5 s timeout.
    pub fn in_sync(n: usize) -> Self {
        WriteConcern { min_insync_replicas: n, ..Self::default() }
    }
}
```

In `MasterConfig`, after `pub serve: Option<ServeConfig>,` add:

```rust
    /// Default write concern of every write method. Default: asynchronous (no acks awaited).
    pub write_concern: WriteConcern,
```

in `MasterConfig::new`, after `serve: None,` add `write_concern: WriteConcern::default(),`, and in `MasterConfig::validate`, after the `serve.validate()` block add:

```rust
        if self.write_concern.min_insync_replicas > 0 && self.write_concern.ack_timeout.is_zero() {
            return Err(Error::Config("write_concern.ack_timeout must be positive".into()));
        }
```

In `src/lib.rs`, add `WriteConcern` to the `pub use config::{…}` list.

In `src/error.rs`, after the `Halted` variant add:

```rust
    /// Fewer replicas are in sync than the write concern requires; nothing was written.
    #[error("{in_sync} replicas in sync, the write concern requires {required}; nothing was written")]
    NotEnoughReplicas { required: usize, in_sync: usize },
    /// The write is committed on the master (and keeps replicating), but `required` in-sync
    /// acknowledgements were not seen within the write concern's `ack_timeout`. Retrying the
    /// write would apply it a second time.
    #[error("entry {seq} is committed but only {acked} of {required} in-sync replicas acknowledged it in time")]
    AckTimeout { seq: Seq, acked: usize, required: usize },
```

- [ ] **Step 4: Implement `commit_with`** (above the test module in `src/master/write_concern.rs`)

```rust
//! Write concern: waiting for in-sync replicas to acknowledge a write (addendum §2).

use std::time::Duration;

use super::writer::Inner;
use crate::config::WriteConcern;
use crate::error::{Error, Result};
use crate::types::{Op, Seq};

impl Inner {
    /// How long a replica may go without being fully caught up and still count as in sync;
    /// zero (no replica is ever in sync) when this master does not serve replication.
    fn replica_lag_time_max(&self) -> Duration {
        self.cfg.serve.as_ref().map_or(Duration::ZERO, |s| s.limits.replica_lag_time_max)
    }

    /// In-sync replicas of the current epoch, and how many of them acknowledged `seq`.
    fn in_sync(&self, seq: Seq) -> (usize, usize) {
        let acks = self.replicas.acks(self.identity().epoch, self.replica_lag_time_max());
        let in_sync = acks.iter().filter(|(_, ins)| *ins).count();
        let acked = acks.iter().filter(|(applied, ins)| *ins && *applied >= seq).count();
        (in_sync, acked)
    }

    /// Commits one entry under `wc` (addendum §2). With `min_insync_replicas = N > 0`:
    /// refuses with `NotEnoughReplicas` (nothing written, no seq consumed) if fewer than N
    /// replicas are in sync; otherwise commits as usual (fsynced, writer lock released) and
    /// then waits for N in-sync replicas to acknowledge the seq, or fails with `AckTimeout`
    /// — the entry stays committed and keeps replicating.
    pub(crate) fn commit_with(&self, wc: &WriteConcern, build: impl FnOnce(u64) -> Result<Vec<Op>>) -> Result<Seq> {
        let required = wc.min_insync_replicas;
        if required == 0 {
            return self.commit_ops(build);
        }
        let (in_sync, _) = self.in_sync(Seq::MAX);
        if in_sync < required {
            return Err(Error::NotEnoughReplicas { required, in_sync });
        }
        let seq = self.commit_ops(build)?;
        if self.replicas.wait_until(wc.ack_timeout, |_| self.in_sync(seq).1 >= required) {
            Ok(seq)
        } else {
            Err(Error::AckTimeout { seq, acked: self.in_sync(seq).1, required })
        }
    }
}
```

In `src/server/replicas.rs`, delete both lines `#[cfg_attr(not(test), allow(dead_code))] // used by the write concern (later Plan 2 task)` (above `acks` and `wait_until`).

- [ ] **Step 5: Route every write through the write concern**

In `src/master/mod.rs`, import `WriteConcern` (`use crate::config::{MasterConfig, WriteConcern};`), and in `put`, `put_with_ttl`, `delete`, `delete_range`, `write`, `create_cf` and `drop_cf` replace `self.inner.commit_ops(` with `self.inner.commit_with(&self.inner.cfg.write_concern, ` (the closure argument stays as it is). Then add after `write`:

```rust
    /// Like [`write`](Self::write), with an explicit write concern instead of
    /// `MasterConfig::write_concern`. With `min_insync_replicas = N > 0` it refuses with
    /// `NotEnoughReplicas` when fewer than N replicas are in sync (nothing written), and
    /// otherwise returns once N in-sync replicas acknowledged the entry. **On
    /// `Error::AckTimeout` the entry is committed and keeps replicating**; see
    /// [`WriteConcern`](crate::WriteConcern).
    ///
    /// The read-modify-write caveat of [`write`](Self::write) applies unchanged. Blocks while
    /// waiting for acknowledgements (without holding the writer lock); async callers should
    /// use `spawn_blocking`.
    pub fn write_with(&self, batch: Batch, concern: WriteConcern) -> Result<Seq> {
        self.inner.commit_with(&concern, |ts| Ok(batch.into_ops(ts)))
    }

    /// Like [`put`](Self::put), with an explicit write concern; see [`write_with`](Self::write_with).
    pub fn put_with(&self, cf: &str, key: &[u8], value: &[u8], concern: WriteConcern) -> Result<Seq> {
        let op = Op::Put { cf: cf.into(), key: key.into(), value: value.into(), expire_at_ms: None };
        self.inner.commit_with(&concern, |_| Ok(vec![op]))
    }
```

- [ ] **Step 6: Run the tests**

Run: `cargo test --lib master::write_concern && cargo test --test write_concern`
Expected: PASS: 4 unit tests (`ack_timeout_leaves_the_write_committed`, `a_write_returns_after_the_in_sync_ack`, `a_lagging_replica_leaves_the_isr_and_rejoins` (a replica that stops being caught up leaves the ISR after `replica_lag_time_max` and `NotEnoughReplicas` consumes no seq; catching up rejoins), `without_replicas_a_write_concern_is_refused_and_consumes_no_seq`) and 3 end-to-end tests (`a_write_returns_after_a_real_slave_acknowledged_it`, `an_acknowledged_write_survives_losing_the_master` (`promote_checked` on an in-sync replica after the master is gone: the acknowledged write is there), `the_configured_write_concern_applies_to_every_write`). The failpoint test `a_replica_that_stops_acking_times_the_write_out` (a real slave that commits but never acknowledges → `AckTimeout`, entry committed) is added with the other failpoint tests in Task 15.

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings`
Expected: PASS; no warnings.

- [ ] **Step 7: Update DESIGN.md**

In DESIGN.md §1, replace:

```
- Strong / synchronous consistency. Replication is **async**, designed for **low update frequency**.
```

with:

```
- Strong consistency or consensus. Replication is **async** by default, designed for **low update frequency**. A write concern can make a write wait for acknowledgements from in-sync replicas (§5.2), but there is no quorum commit and no automatic failover.
```

In DESIGN.md §5.1 (end), after:

```
The same rules are in the rustdoc for `Master::put`, `Master::write` and `Master::get`, and in the crate-level docs.
```

add:

```
### 5.2 Write concern (in-sync replicas)

`MasterConfig::write_concern: WriteConcern { min_insync_replicas, ack_timeout }` (default `0` = asynchronous, `5 s`) applies to every write method; `Master::write_with(batch, concern)` and `Master::put_with(cf, key, value, concern)` override it per call. A direct replica of the master is **in sync** while its Tail stream is open, it follows the master's current identity, and its acknowledged seq reached the master's `last_seq` within `ServeLimits::replica_lag_time_max` (default 10 s; PROTOCOL.md §7.6). Replicas of replicas do not count.

With `min_insync_replicas = N > 0`:
1. Before taking the writer lock: if fewer than N replicas are in sync → `Error::NotEnoughReplicas { required, in_sync }`. Nothing is written and no seq is consumed.
2. Commit and fsync as usual (§5), then release the writer lock.
3. Wait, without the writer lock, until N in-sync replicas have acknowledged `seq` → `Ok(seq)`.
4. After `ack_timeout` → `Error::AckTimeout { seq, acked, required }`. **The entry is committed on the master and keeps replicating**; the error only says that N acknowledgements were not seen in time (Kafka semantics). Retrying the write would write it twice.

Write methods stay synchronous; async callers use `spawn_blocking`. Concurrent writers pipeline: each waits for its own acknowledgements while the next commits. With `N ≥ 1`, the most advanced in-sync replica holds every acknowledged write, which is what the failover runbook (§13.1) relies on.
```

In DESIGN.md §16 (MasterConfig), replace:

```
                                        // `Tunable` (no merge/filter/TTL setters exist)
}
```

with:

```
                                        // `Tunable` (no merge/filter/TTL setters exist)
    pub write_concern: WriteConcern,    // { min_insync_replicas: 0 (async), ack_timeout: 5 s } (§5.2)
}
```

In DESIGN.md §16 (impl Master), after:

```
    pub fn write(&self, batch: Batch) -> Result<Seq>;
```

add:

```
    pub fn write_with(&self, batch: Batch, concern: WriteConcern) -> Result<Seq>;   // §5.2
    pub fn put_with(&self, cf: &str, k: &[u8], v: &[u8], concern: WriteConcern) -> Result<Seq>;
```

In DESIGN.md §16.2, replace:

```
`NotMostAdvanced { leader, seq }` (`promote_checked`), 
```

with:

```
`NotMostAdvanced { leader, seq }` (`promote_checked`), `NotEnoughReplicas { required, in_sync }` (nothing written) and `AckTimeout { seq, acked, required }` (written, not acknowledged in time; §5.2), 
```

In DESIGN.md §18 (table), after:

```
| Master fsync | always (not configurable) |
```

add:

```
| Write concern | `min_insync_replicas` 0 (asynchronous), `ack_timeout` 5 s |
```

In DESIGN.md §21, after:

```
- **Bootstrap lease:** aggressive retention (`max_entries = 1`) during a slow bootstrap; the slave still tails from `S + 1` without bootstrapping again.
```

add:

```
- **Acks and write concern:** `replicas()` follows a replica's acknowledged seq and lag; an ack that goes backwards or past what was sent ends the stream. `min_insync_replicas = 1` without a replica → `NotEnoughReplicas` (no seq consumed); with one → returns after its ack; a replica that stops acknowledging → `AckTimeout`, the entry still replicates; master lost after an acknowledged write → `promote_checked` on the in-sync replica → the write is there.
```

In DESIGN.md §22, replace:

```
**Future**
- Automatic range repair after a verify mismatch.
- Semi-sync writes (wait for N replica acks).
```

with:

```
- Optional write concern (`min_insync_replicas`, `ack_timeout`) with Kafka semantics: `AckTimeout` never undoes the write.

**Future**
- Automatic range repair after a verify mismatch.
```

- [ ] **Step 8: Commit**

```bash
git add src tests DESIGN.md
git commit -m "feat: write concern with in-sync replicas (NotEnoughReplicas, AckTimeout)

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 13: `Slave::repoint` — switching upstream at runtime

**Files:**
- Create: `src/slave/repoint.rs`, `tests/repoint.rs`
- Modify: `src/slave/inner.rs` (replace), `src/slave/driver.rs` (replace), `src/slave/bootstrap.rs` (replace), `src/slave/client.rs`, `src/slave/mod.rs`, `src/error.rs`, `src/lib.rs`, `DESIGN.md` (§13, §13.1, §16, §16.2, §16.3, §22), `PROTOCOL.md` (§6.4)

**Interfaces:**
- Consumes: Task 8 `slave::{inner::Inner, driver::{run, session, tail, SessionEnd, status_end}, bootstrap::install, client::connect_upstream}`, `Inner::{halt_cleared, set_state, set_last_error, reset_downstream, ident}`; Task 7 `NodeState`, `policy::after_handshake`; Task 11 `admin::choose_promotion_candidate`, `Slave::{promote, promote_checked, promotion_info}` (tests); Plan 1 `fork_after`.
- Produces:
  - `pub struct AcceptDataLoss;` `pub enum RepointDivergence { Refuse (default), Repair { accept_loss: AcceptDataLoss }, Rebootstrap { accept_loss: AcceptDataLoss } }`, `pub struct RepointOptions { pub on_divergence: RepointDivergence }` (`Default`), `#[non_exhaustive] pub enum RepointOutcome { Resumed, EpochAdvanced, Bootstrapped, Repaired { discarded: RangeInclusive<Seq> }, Retrying { last_error: String } }` — all re-exported from the crate root.
  - `Slave::repoint(&self, upstream: UpstreamConfig, opts: RepointOptions) -> Result<RepointOutcome>` (async).
  - `Error::Diverged { fork_seq: Seq, local_last_seq: Seq, divergent_entries: u64 }`, `Error::Unsupported(String)`.
  - `Inner::{upstream() -> UpstreamConfig, set_upstream(UpstreamConfig), upstream_uri() (now the current upstream), repoint_tx: watch::Sender<u64>, pending_repoint(), repoint_mode(generation), settle_repoint(generation, Result<RepointOutcome>), fail_repoint_on_halt(generation, Error), settle_repoint_end(generation, &SessionEnd)}`, `repoint::{PendingRepoint, divergence(local_epoch, local_last, &pb::HandshakeResponse) -> Error}`; `driver::unless_repointed(&Inner, generation, impl Future) -> Result<T, SessionEnd>`; `bootstrap::install(&Arc<Inner>, generation, &Client, &pb::HandshakeResponse)`; `client::connect(&Inner)`.

**How it works.** `repoint` validates the address, clears a halt (persisting `node_state = ready`), stores the new upstream and a pending reply tagged with a new *generation*, and bumps `Inner::repoint_tx`. The driver reads the generation at the start of each session and rebuilds its client when it changed; every network await of a session (handshake, opening Tail or Bootstrap, each stream message, and the mid-bootstrap failpoint freeze) goes through `unless_repointed`, which ends the session with `Again` as soon as the generation moves on. Store writes are never cancelled: the driver only stops between them. A bootstrap abandoned this way left the slave `bootstrapping` without identity, so the next session bootstraps from scratch. The session of the new generation settles the reply after its handshake: `Resumed`, `EpochAdvanced` (after adopting the epoch) or `Bootstrapped` (after the install), `Retrying` when the session ends in a retry, an error when it halts (sent only once the halt is persisted). A `DIVERGED` answer is decided by the pending `RepointDivergence`, not by `SlaveConfig::on_divergence`: `Rebootstrap` bootstraps; `Refuse` halts `Diverged` and reports `Error::Diverged` (fork from the server's epoch history, rule 8; from the master's `last_seq + 1`, rule 11; or the last entry, rule 12); `Repair` halts `Diverged` and reports `Error::Unsupported` until Plan 3 replaces that arm. Success resets downstream streams, so they learn the new upstream chain.

- [ ] **Step 1: Write the failing tests**

Create `tests/repoint.rs`:

```rust
//! Switching upstream at runtime (DESIGN.md §13.1–§13.2, addendum §3).

mod support;

use replicated_rocksdb::admin;
use replicated_rocksdb::{
    AcceptDataLoss, CfSpec, Error, HaltReason, LogApi, Master, ReadApi, RepointDivergence, RepointOptions, RepointOutcome,
    Slave, SlaveState, UpstreamConfig,
};
use support::{assert_converged, wait_state, wait_until, Cluster};

fn up(addr: std::net::SocketAddr) -> UpstreamConfig {
    UpstreamConfig::from_addr(addr)
}

/// `Slave::repoint`, bounded by the harness wait.
async fn repoint(s: &Slave, upstream: UpstreamConfig, opts: RepointOptions) -> Result<RepointOutcome, Error> {
    tokio::time::timeout(support::WAIT, s.repoint(upstream, opts)).await.expect("repoint settles")
}

/// The failover runbook (DESIGN.md §13.1): the master is lost; the operator compares the
/// replicas, promotes the most advanced one with `promote_checked`, and repoints the others
/// at it without reopening them or bootstrapping.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn failover_runbook_end_to_end() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    let (cn, lc) = c.slave("c", a.serve_addr().unwrap()).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    wait_state(&cn, "c streaming", |s| *s == SlaveState::Streaming).await;
    for i in 0..5u8 {
        a.put("c", &[i], &[i]).unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    a.shutdown().await.unwrap(); // the master is lost

    // 1. Compare. 2. Promote the candidate, checking its peers once more.
    let choice = admin::choose_promotion_candidate(&[b.promotion_info(), cn.promotion_info()]).unwrap();
    assert_eq!(choice.candidate.node_id, "b", "equal positions: lowest node id");
    assert_eq!(choice.compatible, vec!["c".to_string()]);
    assert!(choice.diverged.is_empty());
    let b = b.promote_checked(c.master_config("b"), &[up(cn.serve_addr().unwrap())]).await.unwrap();
    // 3. Repoint the others. Reads keep working throughout.
    let outcome = repoint(&cn, up(b.serve_addr().unwrap()), RepointOptions::default()).await.unwrap();
    assert_eq!(outcome, RepointOutcome::EpochAdvanced);
    assert_eq!(cn.get("c", &[4]).unwrap(), Some(vec![4]));
    b.put("c", b"after failover", b"1").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 1, "only the initial bootstrap");
    assert_eq!(cn.status().upstream_chain, vec!["b".to_string()]);
    wait_until("c in sync on b", || b.replicas().iter().any(|r| r.node_id == "c" && r.in_sync)).await;
    // Repointing to the upstream it already follows resumes.
    assert_eq!(repoint(&cn, up(b.serve_addr().unwrap()), RepointOptions::default()).await.unwrap(), RepointOutcome::Resumed);
}

/// D holds entries the new master never saw. Repoint refuses by default and keeps D's data
/// readable; `Repair` is not available yet; `Rebootstrap` with `AcceptDataLoss` converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn divergence_is_refused_unless_data_loss_is_accepted() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a.serve_addr().unwrap()).await;
    let (d, ld) = c.slave("d", a.serve_addr().unwrap()).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b), ("d", &d)]).await;
    let b_cfg = c.slave_config("b", a.serve_addr().unwrap()).0;
    b.shutdown().await.unwrap();
    a.put("c", b"only on d", b"2").unwrap();
    a.put("c", b"also only on d", b"3").unwrap();
    assert_converged(&a, &[("d", &d)]).await;
    a.shutdown().await.unwrap();
    let b = Slave::open(b_cfg).await.unwrap();
    let fork = b.last_seq() + 1;
    let b = b.promote(c.master_config("b")).await.unwrap();
    let b_up = up(b.serve_addr().unwrap());

    let err = repoint(&d, b_up.clone(), RepointOptions::default()).await.unwrap_err();
    match err {
        Error::Diverged { fork_seq, local_last_seq, divergent_entries } => {
            assert_eq!((fork_seq, local_last_seq, divergent_entries), (fork, d.last_seq(), 2));
        }
        e => panic!("expected Diverged, got {e}"),
    }
    assert_eq!(d.status().state, SlaveState::Halted(HaltReason::Diverged));
    assert_eq!(d.get("c", b"only on d").unwrap(), Some(b"2".to_vec()), "data intact and readable");

    let repair = RepointOptions { on_divergence: RepointDivergence::Repair { accept_loss: AcceptDataLoss } };
    assert!(matches!(repoint(&d, b_up.clone(), repair).await, Err(Error::Unsupported(_))));
    assert_eq!(d.status().state, SlaveState::Halted(HaltReason::Diverged));
    assert_eq!(d.get("c", b"also only on d").unwrap(), Some(b"3".to_vec()));

    let rebootstrap =
        RepointOptions { on_divergence: RepointDivergence::Rebootstrap { accept_loss: AcceptDataLoss } };
    assert_eq!(repoint(&d, b_up, rebootstrap).await.unwrap(), RepointOutcome::Bootstrapped);
    b.put("c", b"after", b"4").unwrap();
    assert_converged(&b, &[("d", &d)]).await;
    assert_eq!(d.get("c", b"only on d").unwrap(), None, "the divergent entries were discarded");
    assert_eq!(ld.bootstraps(), 2);
}

/// Repointing at itself or at its own downstream halts `LoopDetected` (data intact); a
/// further repoint to a valid upstream recovers.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repoint_refuses_loops() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, lb) = c.slave("b", a.serve_addr().unwrap()).await;
    let (cn, _) = c.slave("c", b.serve_addr().unwrap()).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let to_self = repoint(&b, up(b.serve_addr().unwrap()), RepointOptions::default()).await;
    assert!(matches!(to_self, Err(Error::Halted(HaltReason::LoopDetected))), "{to_self:?}");
    let to_downstream = repoint(&b, up(cn.serve_addr().unwrap()), RepointOptions::default()).await;
    assert!(matches!(to_downstream, Err(Error::Halted(HaltReason::LoopDetected))), "{to_downstream:?}");
    assert_eq!(b.get("c", b"k").unwrap(), Some(b"v".to_vec()));

    assert_eq!(repoint(&b, up(a.serve_addr().unwrap()), RepointOptions::default()).await.unwrap(), RepointOutcome::Resumed);
    a.put("c", b"k2", b"v2").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(lb.bootstraps(), 1);
}

/// Repointing at a restarted old master reports `Retrying` with STALE_EPOCH (not a halt);
/// repointing back resumes without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repoint_to_a_stale_master_keeps_retrying() {
    let c = Cluster::new();
    let a = c.master("a").await;
    let a_addr = a.serve_addr().unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    let (cn, lc) = c.slave("c", a_addr).await;
    wait_state(&b, "b streaming", |s| *s == SlaveState::Streaming).await;
    wait_state(&cn, "c streaming", |s| *s == SlaveState::Streaming).await;
    a.put("c", b"k", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let a_cfg = c.master_config("a");
    a.shutdown().await.unwrap();
    let b = b.promote(c.master_config("b")).await.unwrap();
    let b_up = up(b.serve_addr().unwrap());
    assert_eq!(repoint(&cn, b_up.clone(), RepointOptions::default()).await.unwrap(), RepointOutcome::EpochAdvanced);
    b.put("c", b"k2", b"v2").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;

    let mut a_cfg = a_cfg;
    a_cfg.serve.as_mut().unwrap().port = a_addr.port();
    let old = Master::open(a_cfg).await.unwrap();
    old.put("c", b"split brain", b"x").unwrap();
    match repoint(&cn, up(a_addr), RepointOptions::default()).await.unwrap() {
        RepointOutcome::Retrying { last_error } => assert!(last_error.contains("STALE_EPOCH"), "{last_error}"),
        o => panic!("expected Retrying, got {o:?}"),
    }
    assert!(!matches!(cn.status().state, SlaveState::Halted(_)));
    assert_eq!(cn.get("c", b"split brain").unwrap(), None);

    assert_eq!(repoint(&cn, b_up, RepointOptions::default()).await.unwrap(), RepointOutcome::Resumed);
    b.put("c", b"k3", b"v3").unwrap();
    assert_converged(&b, &[("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 1);
}
```

The repoint-during-bootstrap test needs a failpoint, so it is added to `tests/failpoints.rs` in Task 15 (`repoint_during_bootstrap_restarts_it_against_the_new_upstream`).

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --test repoint`
Expected: FAIL to compile: `unresolved imports replicated_rocksdb::AcceptDataLoss, RepointDivergence, RepointOptions, RepointOutcome`, `no method named repoint found for struct Slave`.

- [ ] **Step 3: Errors and public types**

In `src/error.rs`, after the `NotMostAdvanced` variant add:

```rust
    /// The local log holds entries the upstream does not: `divergent_entries` entries from
    /// `fork_seq` to `local_last_seq` (for a mismatch at the same epoch, `fork_seq` is the
    /// last entry; earlier ones may differ too). Local data is intact.
    #[error("local log diverged from the upstream at seq {fork_seq} ({divergent_entries} entries up to {local_last_seq})")]
    Diverged { fork_seq: Seq, local_last_seq: Seq, divergent_entries: u64 },
    /// The operation is not available in this version of the crate.
    #[error("not supported by this version: {0}")]
    Unsupported(String),
```

Create `src/slave/repoint.rs`:

```rust
//! Switching upstream at runtime (DESIGN.md §13.2).

use std::ops::RangeInclusive;

use tokio::sync::oneshot;

use super::client::connect_upstream;
use super::driver::SessionEnd;
use super::inner::Inner;
use super::state::{NodeState, SlaveState};
use super::{Slave, UpstreamConfig};
use crate::error::{Error, Result};
use crate::pb;
use crate::store::meta::key;
use crate::types::{fork_after, EpochStart, Seq};

/// Acknowledges that an operation discards data. A caller constructs it explicitly
/// (`AcceptDataLoss`) to opt in; nothing in the crate supplies one by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptDataLoss;

/// What a repoint does when the new upstream answers `DIVERGED`. The slave's configured
/// `on_divergence` policy is not used for the repoint's own handshake.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum RepointDivergence {
    /// `repoint` returns `Error::Diverged`; the slave halts `Diverged` with its data intact
    /// and readable.
    #[default]
    Refuse,
    /// Discard only the divergent suffix and resume (DESIGN.md §13.3). **Not available in
    /// this version:** `repoint` returns `Error::Unsupported` and the slave halts
    /// `Diverged` with its data intact. It needs verify sessions (Plan 3).
    Repair { accept_loss: AcceptDataLoss },
    /// Discard all local data and bootstrap from the new upstream.
    Rebootstrap { accept_loss: AcceptDataLoss },
}

/// Options of [`Slave::repoint`]. The default refuses divergence.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RepointOptions {
    pub on_divergence: RepointDivergence,
}

/// How the first handshake with the new upstream settled.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum RepointOutcome {
    /// Same epoch: tailing from `applied_seq() + 1`.
    Resumed,
    /// The new upstream is at a later epoch and continuity was proven; tailing.
    EpochAdvanced,
    /// Local data was replaced by a bootstrap from the new upstream (no local data, an
    /// unprovable epoch change or log gap under the configured policies, or
    /// `RepointDivergence::Rebootstrap`).
    Bootstrapped,
    /// The divergent suffix `discarded` was rolled back (Plan 3; never returned by this
    /// version).
    Repaired { discarded: RangeInclusive<Seq> },
    /// The new upstream could not be used yet (unreachable, `STALE_EPOCH`, `NOT_READY`, …).
    /// The slave keeps retrying it with backoff; `last_error` says why.
    Retrying { last_error: String },
}

pub(crate) struct PendingRepoint {
    pub generation: u64,
    pub on_divergence: RepointDivergence,
    /// The error to report when the session ends in a halt, instead of `Halted(reason)`.
    pub halt_error: Option<Error>,
    pub reply: oneshot::Sender<Result<RepointOutcome>>,
}

impl Inner {
    /// The divergence mode of the repoint that session `generation` settles, if any.
    pub(crate) fn repoint_mode(&self, generation: u64) -> Option<RepointDivergence> {
        self.pending_repoint().as_ref().filter(|p| p.generation == generation).map(|p| p.on_divergence)
    }

    /// Settles the repoint that session `generation` was started for (no-op if there is none).
    pub(crate) fn settle_repoint(&self, generation: u64, result: Result<RepointOutcome>) {
        let mut pending = self.pending_repoint();
        if pending.as_ref().is_some_and(|p| p.generation == generation) {
            let p = pending.take().expect("checked");
            if result.is_ok() {
                // Downstream nodes learn the new upstream chain from a fresh handshake.
                self.reset_downstream();
            }
            let _ = p.reply.send(result);
        }
    }

    /// Makes the repoint of session `generation` fail with `e` once the session's halt is in
    /// place.
    pub(crate) fn fail_repoint_on_halt(&self, generation: u64, e: Error) {
        if let Some(p) = self.pending_repoint().as_mut().filter(|p| p.generation == generation) {
            p.halt_error = Some(e);
        }
    }

    /// Settles a repoint from the way its session ended: a halt (called once it is in
    /// place) is an error, a retry is `Retrying`, and `Again` waits for the next handshake.
    pub(crate) fn settle_repoint_end(&self, generation: u64, end: &SessionEnd) {
        match end {
            SessionEnd::Halt(r) => {
                let e = self.pending_repoint().as_mut().and_then(|p| p.halt_error.take());
                self.settle_repoint(generation, Err(e.unwrap_or(Error::Halted(*r))));
            }
            SessionEnd::Retry(m) => self.settle_repoint(generation, Ok(RepointOutcome::Retrying { last_error: m.clone() })),
            SessionEnd::Again => {}
        }
    }
}

/// `Error::Diverged` for a `DIVERGED` answer: where the local log leaves the upstream's.
pub(crate) fn divergence(local_epoch: u64, local_last: Seq, hs: &pb::HandshakeResponse) -> Error {
    let history: Vec<EpochStart> =
        hs.epoch_history.iter().map(|e| EpochStart { epoch: e.epoch, start_seq: e.start_seq }).collect();
    let fork_seq = match fork_after(&history, local_epoch) {
        // Rule 8: everything from the next epoch's start is not on the upstream's history.
        Some(fork) if fork <= local_last => fork,
        // Rule 11: the local log runs past the master's.
        _ if local_last > hs.last_seq => hs.last_seq + 1,
        // Rule 12: the last entry differs; earlier ones may too (repair finds out).
        _ => local_last,
    };
    Error::Diverged { fork_seq, local_last_seq: local_last, divergent_entries: local_last + 1 - fork_seq.min(local_last + 1) }
}

impl Slave {
    /// Switches this slave to another upstream at runtime, without reopening; reads keep
    /// being served throughout. The new upstream is not persisted: after a restart, open
    /// the slave with it.
    ///
    /// The current upstream stream is closed (a bootstrap in progress is abandoned and
    /// starts from scratch against the new upstream), a halt is cleared, and the new
    /// upstream is handshaken under all the normal rules (loop detection, `STALE_EPOCH`,
    /// epoch continuity). `repoint` returns once that first handshake settles:
    ///
    /// - `Ok(Resumed | EpochAdvanced | Bootstrapped)`: replicating from the new upstream.
    /// - `Ok(Retrying { .. })`: the new upstream cannot be used yet (unreachable,
    ///   `STALE_EPOCH`, `NOT_READY`); the slave keeps retrying it.
    /// - `Err(Diverged { .. })` (with `RepointDivergence::Refuse`, the default): the local log
    ///   holds entries the new upstream does not. **Divergence is never resolved
    ///   implicitly:** the slave halts `Diverged`, its data intact and readable. Discarding
    ///   the divergent entries takes an explicit [`AcceptDataLoss`]:
    ///   `RepointDivergence::Rebootstrap { accept_loss: AcceptDataLoss }`.
    ///   `RepointDivergence::Repair` returns `Err(Unsupported)` in this version (it needs
    ///   Plan 3's verify sessions) and also leaves the slave halted with its data intact.
    /// - `Err(Halted(LoopDetected))`: the new upstream is this slave or replicates from it;
    ///   the slave halts until it is repointed again.
    /// - `Err(Halted(reason))` for any other halt, `Err(Config)` for an invalid address.
    ///
    /// The wait includes a whole bootstrap when one is needed. To bound it, wrap the call in
    /// `tokio::time::timeout`: dropping the future does not undo the repoint, the slave keeps
    /// following the new upstream.
    pub async fn repoint(&self, upstream: UpstreamConfig, opts: RepointOptions) -> Result<RepointOutcome> {
        if upstream.server.is_empty() || upstream.port == 0 {
            return Err(Error::Config("upstream server and port are required".into()));
        }
        connect_upstream(&upstream, &self.inner.cfg.identifier, self.inner.cfg.connect_timeout)?;
        if let SlaveState::Halted(reason) = self.inner.state() {
            let i = self.inner.clone();
            crate::background::blocking(move || i.store.write_meta(&[(key::NODE_STATE, &NodeState::Ready.encode())], true))
                .await?;
            tracing::info!(node = %self.inner.cfg.identifier, reason = reason.name(), "halt cleared by repoint");
        }
        let (reply, settled) = oneshot::channel();
        {
            let mut pending = self.inner.pending_repoint();
            tracing::info!(node = %self.inner.cfg.identifier, upstream = %upstream.uri(), "repointing");
            self.inner.set_upstream(upstream);
            let generation = *self.inner.repoint_tx.borrow() + 1;
            *pending = Some(PendingRepoint { generation, on_divergence: opts.on_divergence, halt_error: None, reply });
            self.inner.repoint_tx.send_replace(generation);
        }
        self.inner.set_last_error(None);
        if matches!(self.inner.state(), SlaveState::Halted(_)) {
            self.inner.set_state(SlaveState::Connecting);
            self.inner.halt_cleared.notify_one();
        }
        // The sender is dropped when a later repoint supersedes this one or the slave shuts down.
        settled.await.unwrap_or_else(|_| Err(Error::Config("the repoint was superseded or the slave shut down".into())))
    }
}
```

In `src/slave/mod.rs`, add `mod repoint;` to the module list and, after `pub use promote::PromotionCheck;`:

```rust
pub use repoint::{AcceptDataLoss, RepointDivergence, RepointOptions, RepointOutcome};
```

In `src/lib.rs`, replace the `pub use slave::{…};` block with:

```rust
pub use slave::{
    AcceptDataLoss, Backoff, DivergencePolicy, FetchConfig, GapPolicy, HaltAction, HaltReason, IdentityPolicy, PromoteRefused, PromotionCheck,
    RepointDivergence, RepointOptions, RepointOutcome, Slave, SlaveConfig, SlaveState, SlaveStatus, UpstreamConfig,
};
```

- [ ] **Step 4: The current upstream and the repoint state**

Replace `src/slave/inner.rs` with (adds `upstream`, `repoint_tx`, `pending_repoint` and their accessors; `upstream_uri` now reports the current upstream):

```rust
//! Shared state of one slave: the store plus everything the driver, the API and the
//! replication server (when serving) read.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::SystemTime;

use tokio::sync::{watch, Notify};

use super::config::{SlaveConfig, UpstreamConfig};
use super::policy::Policies;
use super::repoint::PendingRepoint;
use super::state::{HaltReason, NodeState, SlaveState, SlaveStatus};
use crate::error::{Error, Result};
use crate::listener::ReplicationListener;
use crate::store::meta::{key, MetaSnapshot};
use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Seq};

pub(crate) struct Inner {
    pub store: Store,
    pub cfg: SlaveConfig,
    /// `None` until the first bootstrap completes, and during a bootstrap.
    ident: RwLock<Option<(DbIdentity, Vec<EpochStart>)>>,
    pub state_tx: watch::Sender<SlaveState>,
    /// Last applied seq, published after each commit.
    pub seq_tx: watch::Sender<Seq>,
    /// Bumped to end every stream this slave serves with `UPSTREAM_RESET`.
    pub reset_tx: watch::Sender<u64>,
    pub shutdown_tx: watch::Sender<bool>,
    pub origin_last_seq: AtomicU64,
    pub upstream_last_seq: AtomicU64,
    upstream_chain: RwLock<Vec<String>>,
    pub max_entry_bytes: AtomicU64,
    last_contact: Mutex<Option<SystemTime>>,
    last_error: Mutex<Option<String>>,
    /// Set by `clear_halt(Rebootstrap)` and by a gap seen mid-stream.
    pub force_bootstrap: AtomicBool,
    /// Shared with the driver, which waits on it without holding the `Inner`.
    pub halt_cleared: Arc<Notify>,
    listener_failed: AtomicBool,
    /// Downstream replicas served by this slave.
    pub(crate) replicas: Arc<crate::server::replicas::ReplicaRegistry>,
    /// The current upstream: `cfg.upstream` until `Slave::repoint` replaces it.
    upstream: RwLock<UpstreamConfig>,
    /// Bumped by every repoint; the driver ends its session and reconnects.
    pub repoint_tx: watch::Sender<u64>,
    pending_repoint: Mutex<Option<PendingRepoint>>,
}

impl Inner {
    pub(crate) fn new(store: Store, cfg: SlaveConfig, meta: &MetaSnapshot) -> Result<Inner> {
        let node_state = NodeState::decode(meta.node_state.as_deref())?;
        let ident = match (meta.identity, node_state) {
            (_, NodeState::Bootstrapping) | (None, _) => None,
            (Some(_), _) if meta.epoch_history.is_empty() => {
                return Err(Error::corrupt("origin_identity without epoch_history in __meta"));
            }
            (Some(id), _) => Some((id, meta.epoch_history.clone())),
        };
        let state = match node_state {
            NodeState::Halted(r) => SlaveState::Halted(r),
            NodeState::Ready | NodeState::Bootstrapping => SlaveState::Connecting,
        };
        let last_seq = store.position().last_seq;
        Ok(Inner {
            store,
            ident: RwLock::new(ident),
            state_tx: watch::channel(state).0,
            seq_tx: watch::channel(last_seq).0,
            reset_tx: watch::channel(0).0,
            shutdown_tx: watch::channel(false).0,
            origin_last_seq: AtomicU64::new(last_seq),
            upstream_last_seq: AtomicU64::new(0),
            upstream_chain: RwLock::new(Vec::new()),
            max_entry_bytes: AtomicU64::new(meta.max_entry_bytes.unwrap_or(0)),
            last_contact: Mutex::new(None),
            last_error: Mutex::new(None),
            force_bootstrap: AtomicBool::new(false),
            halt_cleared: Arc::new(Notify::new()),
            listener_failed: AtomicBool::new(false),
            replicas: Arc::default(),
            upstream: RwLock::new(cfg.upstream.clone()),
            cfg,
            repoint_tx: watch::channel(0).0,
            pending_repoint: Mutex::new(None),
        })
    }

    /// The current upstream's address.
    pub(crate) fn upstream_uri(&self) -> String {
        self.upstream().uri()
    }

    pub(crate) fn upstream(&self) -> UpstreamConfig {
        self.upstream.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(crate) fn set_upstream(&self, up: UpstreamConfig) {
        *self.upstream.write().unwrap_or_else(PoisonError::into_inner) = up;
    }

    pub(crate) fn pending_repoint(&self) -> std::sync::MutexGuard<'_, Option<PendingRepoint>> {
        self.pending_repoint.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn policies(&self) -> Policies {
        Policies { identity: self.cfg.on_identity_change, divergence: self.cfg.on_divergence, gap: self.cfg.on_log_gap }
    }

    pub(crate) fn ident(&self) -> Option<(DbIdentity, Vec<EpochStart>)> {
        self.ident.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub(crate) fn set_ident(&self, v: Option<(DbIdentity, Vec<EpochStart>)>) {
        *self.ident.write().unwrap_or_else(PoisonError::into_inner) = v;
    }

    /// Readable and servable: the slave holds a consistent state at some seq.
    pub(crate) fn is_ready(&self) -> bool {
        self.ident.read().unwrap_or_else(PoisonError::into_inner).is_some()
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.cfg.clock.now_ms()
    }

    pub(crate) fn state(&self) -> SlaveState {
        self.state_tx.borrow().clone()
    }

    /// Publishes a state change and tells the listener (no-op if unchanged).
    pub(crate) fn set_state(&self, s: SlaveState) {
        let changed = self.state_tx.send_if_modified(|cur| {
            if *cur == s {
                false
            } else {
                *cur = s.clone();
                true
            }
        });
        if changed {
            self.notify(|l| l.on_state_change(&s));
        }
    }

    /// Stops advancing. Data-related reasons are persisted first (a synced write: call it on a
    /// blocking thread), so they survive restart.
    pub(crate) fn halt(&self, reason: HaltReason) {
        if reason.is_persisted() {
            let state = NodeState::Halted(reason).encode();
            if let Err(e) = self.store.write_meta(&[(key::NODE_STATE, &state)], true) {
                tracing::error!(node = %self.cfg.identifier, error = %e, "could not persist the halt");
            }
        }
        tracing::error!(
            node = %self.cfg.identifier,
            reason = reason.name(),
            applied_seq = self.store.position().last_seq,
            upstream_last_seq = self.upstream_last_seq.load(Ordering::SeqCst),
            "replication halted"
        );
        self.set_state(SlaveState::Halted(reason));
    }

    /// Ends every stream this slave serves with `UPSTREAM_RESET`.
    pub(crate) fn reset_downstream(&self) {
        self.reset_tx.send_modify(|n| *n += 1);
    }

    pub(crate) fn upstream_chain(&self) -> Vec<String> {
        self.upstream_chain.read().unwrap_or_else(PoisonError::into_inner).clone()
    }

    /// Records what the upstream said about itself in a handshake. Returns the upstream's
    /// `max_entry_bytes` if it changed, for the caller to persist off the async thread.
    pub(crate) fn note_upstream(&self, r: &crate::pb::HandshakeResponse) -> Option<u64> {
        let mut chain = vec![r.server_id.clone()];
        chain.extend(r.upstream_chain.iter().cloned());
        *self.upstream_chain.write().unwrap_or_else(PoisonError::into_inner) = chain;
        self.note_positions(r.last_seq, r.origin_last_seq);
        self.note_contact();
        let changed = r.max_entry_bytes > 0 && self.max_entry_bytes.swap(r.max_entry_bytes, Ordering::SeqCst) != r.max_entry_bytes;
        changed.then_some(r.max_entry_bytes)
    }

    pub(crate) fn note_positions(&self, upstream_last_seq: Seq, origin_last_seq: Seq) {
        self.upstream_last_seq.store(upstream_last_seq, Ordering::SeqCst);
        self.origin_last_seq.fetch_max(origin_last_seq, Ordering::SeqCst);
    }

    pub(crate) fn note_contact(&self) {
        *self.last_contact.lock().unwrap_or_else(PoisonError::into_inner) = Some(SystemTime::now());
    }

    pub(crate) fn set_last_error(&self, e: Option<String>) {
        *self.last_error.lock().unwrap_or_else(PoisonError::into_inner) = e;
    }

    /// Calls the listener, if any. A panic is caught, logged, and disables the listener.
    pub(crate) fn notify(&self, f: impl FnOnce(&dyn ReplicationListener)) {
        let Some(l) = &self.cfg.listener else { return };
        if self.listener_failed.load(Ordering::SeqCst) {
            return;
        }
        if catch_unwind(AssertUnwindSafe(|| f(l.as_ref()))).is_err() {
            self.listener_failed.store(true, Ordering::SeqCst);
            tracing::error!(node = %self.cfg.identifier, "replication listener panicked; it is disabled until restart");
        }
    }

    pub(crate) fn status(&self) -> SlaveStatus {
        let pos = self.store.position();
        SlaveStatus {
            state: self.state(),
            origin_identity: self.ident().map(|(id, _)| id),
            applied_seq: pos.last_seq,
            first_retained_seq: self.store.first_retained_seq(),
            upstream_last_seq: self.upstream_last_seq.load(Ordering::SeqCst),
            origin_last_seq: self.origin_last_seq.load(Ordering::SeqCst).max(pos.last_seq),
            upstream_chain: self.upstream_chain(),
            last_contact: *self.last_contact.lock().unwrap_or_else(PoisonError::into_inner),
            last_error: self.last_error.lock().unwrap_or_else(PoisonError::into_inner).clone(),
            listener_failed: self.listener_failed.load(Ordering::SeqCst),
        }
    }
}
```

In `src/slave/client.rs`, replace the import `use super::config::{SlaveConfig, UpstreamConfig};` with:

```rust
use super::config::UpstreamConfig;
use super::inner::Inner;
```

and replace `connect` with:

```rust
/// A client for the slave's current upstream. Connects on first use and reconnects by itself.
pub(crate) fn connect(inner: &Inner) -> Result<Client> {
    connect_upstream(&inner.upstream(), &inner.cfg.identifier, inner.cfg.connect_timeout)
}
```

- [ ] **Step 5: Cancellation points and outcomes in the driver**

Replace `src/slave/driver.rs` with:

```rust
//! The replication driver: one task per slave that connects, handshakes and acts on the
//! answer, forever (PROTOCOL.md §11.3).

use std::future::Future;
use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::WatchStream;
use tokio_stream::StreamExt;

use tonic::Status;

use super::apply::{self, Applied};
use super::bootstrap;
use super::client::{self, Client};
use super::config::BackoffState;
use super::inner::Inner;
use super::policy::{self, Action};
use super::repoint::{self, RepointDivergence, RepointOutcome};
use super::state::{HaltReason, SlaveState};
use crate::background::blocking;
use crate::error::Error;
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::net::convert::{checked_history, entry_from_pb, identity_from_pb, identity_to_pb};
use crate::net::DECODE_SLACK;
use crate::pb;
use crate::pb::tail_client_message::Kind as ClientKind;
use crate::pb::tail_message::Kind;
use crate::store::meta::key;

/// How a session ended.
#[derive(Debug)]
pub(crate) enum SessionEnd {
    /// Back off, then handshake again.
    Retry(String),
    /// Handshake again after a short pause.
    Again,
    Halt(HaltReason),
}

/// Pause before an immediate re-handshake, so a confused upstream cannot cause a hot loop.
const AGAIN_PAUSE: Duration = Duration::from_millis(50);
/// Backoff resets after this much uninterrupted streaming (PROTOCOL.md §11.4).
const STABLE_STREAMING: Duration = Duration::from_secs(60);

/// Maps a policy action for a failed call to how the session ends.
pub(crate) fn end_for(inner: &Inner, action: Action) -> SessionEnd {
    match action {
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Bootstrap => {
            inner.force_bootstrap.store(true, Ordering::SeqCst);
            SessionEnd::Again
        }
        Action::Again | Action::Tail | Action::AdoptEpochThenTail => SessionEnd::Again,
    }
}

/// Awaits `f` (a network call), or ends the session with `Again` once the slave is repointed
/// away from the upstream of session `generation`. Store writes are never cancelled this way: the
/// driver checks only between them.
pub(crate) async fn unless_repointed<T>(inner: &Inner, generation: u64, f: impl Future<Output = T>) -> Result<T, SessionEnd> {
    let mut repoints = inner.repoint_tx.subscribe();
    tokio::select! {
        biased;
        _ = repoints.wait_for(|g| *g != generation) => Err(SessionEnd::Again),
        v = f => Ok(v),
    }
}

pub(crate) fn status_end(inner: &Inner, s: &Status) -> SessionEnd {
    end_for(inner, policy::after_status(s, &inner.policies()))
}

/// Starts the driver. It holds only a `Weak<Inner>` while it waits (halted, or backing off),
/// and a strong reference only for the duration of one session, which shutdown cancels.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let weak = Arc::downgrade(inner);
    let shutdown = inner.shutdown_tx.subscribe();
    let halt_cleared = inner.halt_cleared.clone();
    let repoints = inner.repoint_tx.subscribe();
    let backoff = BackoffState::new(inner.cfg.reconnect.clone());
    tokio::spawn(run(weak, shutdown, repoints, halt_cleared, backoff))
}

async fn run(
    weak: Weak<Inner>,
    mut shutdown: watch::Receiver<bool>,
    mut repoints: watch::Receiver<u64>,
    halt_cleared: Arc<Notify>,
    mut backoff: BackoffState,
) {
    // The client of the upstream of repoint generation `.0`.
    let mut client: Option<(u64, Client)> = None;
    loop {
        if *shutdown.borrow_and_update() {
            return;
        }
        let Some(inner) = weak.upgrade() else { return };
        if matches!(inner.state(), SlaveState::Halted(_)) {
            drop(inner);
            tokio::select! {
                _ = halt_cleared.notified() => backoff.reset(),
                r = shutdown.changed() => if r.is_err() { return },
            }
            continue;
        }
        let generation = *repoints.borrow_and_update();
        if client.as_ref().is_some_and(|(g, _)| *g != generation) {
            client = None;
            backoff.reset();
        }
        let end = tokio::select! {
            end = session(&inner, generation, &mut client, &mut backoff) => end,
            r = shutdown.changed() => {
                if r.is_err() {
                    return;
                }
                continue;
            }
        };
        if !matches!(end, SessionEnd::Halt(_)) {
            inner.settle_repoint_end(generation, &end);
        }
        let pause = match end {
            SessionEnd::Halt(reason) => {
                let i = inner.clone();
                let _ = blocking(move || {
                    i.halt(reason);
                    Ok(())
                })
                .await;
                // A repoint waiting on this session sees the halt in place.
                inner.settle_repoint_end(generation, &end);
                continue;
            }
            SessionEnd::Again => AGAIN_PAUSE,
            SessionEnd::Retry(msg) => {
                tracing::warn!(node = %inner.cfg.identifier, upstream = %inner.upstream_uri(), error = %msg, "replication retry");
                inner.set_last_error(Some(msg));
                inner.set_state(SlaveState::Connecting);
                backoff.next_delay()
            }
        };
        drop(inner);
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
            _ = repoints.changed() => {}
            r = shutdown.changed() => if r.is_err() { return },
        }
    }
}

fn handshake_request(inner: &Inner) -> pb::HandshakeRequest {
    let ident = inner.ident();
    let pos = inner.store.position();
    pb::HandshakeRequest {
        protocol_version: PROTOCOL_VERSION,
        envelope_version: u32::from(ENVELOPE_VERSION),
        client_id: inner.cfg.identifier.clone(),
        known_identity: ident.as_ref().map(|(id, _)| identity_to_pb(id)),
        last_seq: if ident.is_some() { pos.last_seq } else { 0 },
        last_crc32c: if ident.is_some() { pos.last_entry_crc } else { None },
    }
}

/// One connect → handshake → act cycle against the upstream of repoint `generation`.
async fn session(inner: &Arc<Inner>, generation: u64, client: &mut Option<(u64, Client)>, backoff: &mut BackoffState) -> SessionEnd {
    inner.set_state(SlaveState::Connecting);
    let c = match client {
        Some((_, c)) => c.clone(),
        None => match client::connect(inner) {
            Ok(c) => client.insert((generation, c)).1.clone(),
            Err(e) => return SessionEnd::Retry(e.to_string()),
        },
    };
    let hs = match unless_repointed(inner, generation, c.clone().handshake(handshake_request(inner))).await {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(s)) => return status_end(inner, &s),
        Err(end) => return end,
    };
    if hs.decision() != pb::Decision::Reject {
        if let Some(max) = inner.note_upstream(&hs) {
            let i = inner.clone();
            let persisted = blocking(move || i.store.write_meta(&[(key::MAX_ENTRY_BYTES, &max.to_be_bytes())], false));
            if let Err(e) = persisted.await {
                tracing::warn!(error = %e, "could not persist max_entry_bytes");
            }
        }
    } else {
        inner.note_contact();
    }
    let force = inner.force_bootstrap.load(Ordering::SeqCst);
    let mut action = policy::after_handshake(&hs, &inner.policies(), force);
    // An explicit repoint never resolves divergence by the configured policy.
    if let Some(mode) = inner.repoint_mode(generation) {
        let diverged = hs.decision() == pb::Decision::Reject && hs.reason() == pb::Reason::Diverged;
        if diverged && !force {
            action = match mode {
                RepointDivergence::Rebootstrap { .. } => Action::Bootstrap,
                RepointDivergence::Refuse | RepointDivergence::Repair { .. } => {
                    let local_epoch = inner.ident().map_or(0, |(id, _)| id.epoch);
                    let err = match mode {
                        RepointDivergence::Refuse => repoint::divergence(local_epoch, inner.store.position().last_seq, &hs),
                        _ => Error::Unsupported("divergence repair needs verify sessions (Plan 3)".into()),
                    };
                    tracing::warn!(node = %inner.cfg.identifier, error = %err, "repoint refused: diverged");
                    inner.fail_repoint_on_halt(generation, err);
                    Action::Halt(HaltReason::Diverged)
                }
            };
        }
    }
    match action {
        Action::Tail => {
            inner.settle_repoint(generation, Ok(RepointOutcome::Resumed));
            tail(inner, generation, &c, &hs, None, backoff).await
        }
        Action::AdoptEpochThenTail => match adopt_epoch(inner, &hs).await {
            Ok(()) => {
                inner.settle_repoint(generation, Ok(RepointOutcome::EpochAdvanced));
                tail(inner, generation, &c, &hs, None, backoff).await
            }
            Err(end) => end,
        },
        Action::Bootstrap => match bootstrap::install(inner, generation, &c, &hs).await {
            Ok(lease) => {
                inner.settle_repoint(generation, Ok(RepointOutcome::Bootstrapped));
                tail(inner, generation, &c, &hs, Some(lease), backoff).await
            }
            Err(end) => end,
        },
        Action::Halt(r) => SessionEnd::Halt(r),
        Action::Retry(m) => SessionEnd::Retry(m),
        Action::Again => SessionEnd::Again,
    }
}

/// RESUME / EPOCH_ADVANCED: persist the new identity and history atomically, then tail.
/// An invalid identity or history from the upstream halts the slave (`Corrupt`).
async fn adopt_epoch(inner: &Arc<Inner>, hs: &pb::HandshakeResponse) -> Result<(), SessionEnd> {
    let id = hs.identity.as_ref().ok_or(SessionEnd::Halt(HaltReason::Corrupt))?;
    let id = identity_from_pb(id).map_err(|_| SessionEnd::Halt(HaltReason::Corrupt))?;
    let history = checked_history(&id, &hs.epoch_history).map_err(|e| {
        tracing::error!(node = %inner.cfg.identifier, error = %e, "upstream sent an invalid epoch history");
        SessionEnd::Halt(HaltReason::Corrupt)
    })?;
    let (i, h) = (inner.clone(), history.clone());
    blocking(move || i.store.adopt_identity(&id, &h)).await.map_err(|e| SessionEnd::Retry(e.to_string()))?;
    tracing::info!(node = %inner.cfg.identifier, epoch = id.epoch, "adopted new epoch from upstream");
    inner.set_ident(Some((id, history)));
    inner.reset_downstream();
    Ok(())
}

/// Tails from `last_seq + 1` and applies entries until the stream ends.
async fn tail(
    inner: &Arc<Inner>,
    generation: u64,
    c: &Client,
    hs: &pb::HandshakeResponse,
    lease: Option<Vec<u8>>,
    backoff: &mut BackoffState,
) -> SessionEnd {
    let Some((identity, _)) = inner.ident() else { return SessionEnd::Again };
    let pos = inner.store.position();
    let fetch = &inner.cfg.fetch;
    let limit = usize::try_from(hs.max_entry_bytes.max(u64::from(fetch.max_batch_bytes)))
        .unwrap_or(usize::MAX)
        .saturating_add(DECODE_SLACK);
    let req = pb::TailRequest {
        client_id: inner.cfg.identifier.clone(),
        identity: Some(identity_to_pb(&identity)),
        from_seq: pos.last_seq + 1,
        prev_crc32c: pos.last_entry_crc,
        bootstrap_lease: lease,
        min_batch_bytes: fetch.min_batch_bytes,
        max_wait_ms: u32::try_from(fetch.max_wait.as_millis()).unwrap_or(u32::MAX),
        max_batch_bytes: fetch.max_batch_bytes,
    };
    // The request stream: `start`, then an ack whenever `acks` changes. A watch coalesces
    // acks that back up: the upstream always gets the latest applied seq.
    let (acks, ack_rx) = watch::channel(pos.last_seq);
    let start = pb::TailClientMessage { kind: Some(ClientKind::Start(req)) };
    let ack_stream = WatchStream::from_changes(ack_rx)
        .map(|applied_seq| pb::TailClientMessage { kind: Some(ClientKind::Ack(pb::TailAck { applied_seq })) });
    let outbound = tokio_stream::iter([start]).chain(ack_stream);
    let mut stream = match unless_repointed(inner, generation, c.clone().max_decoding_message_size(limit).tail(outbound)).await {
        Ok(Ok(r)) => r.into_inner(),
        Ok(Err(s)) => return status_end(inner, &s),
        Err(end) => return end,
    };
    inner.set_state(SlaveState::Streaming);
    inner.set_last_error(None);
    let idle = Duration::from_millis(u64::from(hs.heartbeat_ms.max(1)) * 3);
    let started = Instant::now();
    loop {
        let next = match unless_repointed(inner, generation, tokio::time::timeout(idle, stream.message())).await {
            Ok(next) => next,
            Err(end) => return end,
        };
        let msg = match next {
            Err(_) => return SessionEnd::Retry(format!("no message from upstream for {idle:?}")),
            Ok(Err(s)) => return status_end(inner, &s),
            Ok(Ok(None)) => return SessionEnd::Retry("upstream ended the tail stream".into()),
            Ok(Ok(Some(m))) => m,
        };
        inner.note_contact();
        if started.elapsed() >= STABLE_STREAMING {
            backoff.reset();
        }
        match msg.kind {
            Some(Kind::Heartbeat(h)) => {
                inner.note_positions(h.upstream_last_seq, h.origin_last_seq);
                // At least one ack per heartbeat interval keeps this replica in sync upstream.
                acks.send_replace(inner.store.position().last_seq);
            }
            Some(Kind::Batch(batch)) => {
                if batch.entries.is_empty() {
                    return SessionEnd::Retry("empty entry batch from upstream".into());
                }
                // Each entry is its own atomic write, so a crash mid-batch leaves a clean boundary.
                for e in batch.entries {
                    match apply::apply(inner, entry_from_pb(e)).await {
                        Applied::Committed | Applied::Skipped => {}
                        Applied::End(end) => return end,
                    }
                }
                acks.send_replace(inner.store.position().last_seq);
            }
            None => return SessionEnd::Retry("tail message without content".into()),
        }
    }
}
```

Replace `src/slave/bootstrap.rs` with (the same install; its network awaits and the failpoint freeze go through `unless_repointed`):

```rust
//! Installing a bootstrap from the upstream (PROTOCOL.md §8.3, DESIGN.md §10).

use std::cmp::Ordering as KeyOrdering;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tonic::Streaming;

use super::client::Client;
use super::driver::{status_end, unless_repointed, SessionEnd};
use super::inner::Inner;
use super::state::{HaltReason, SlaveState};
use crate::codec::digest::Digest;
use crate::codec::{envelope, validate_cf_name, ENVELOPE_VERSION};
use crate::config::MAX_BOOTSTRAP_CHUNK_BYTES;
use crate::error::{Error, Result};
use crate::failpoint;
use crate::net::convert::{cf_from_pb, checked_history, identity_from_pb};
use crate::net::DECODE_SLACK;
use crate::pb;
use crate::pb::bootstrap_chunk::Kind;
use crate::store::bootstrap::BootstrapFinish;
use crate::types::{CfEntry, DbIdentity, EpochStart, Seq};

/// Runs `f` with the slave's state on a blocking thread.
async fn blocking<T: Send + 'static>(inner: &Arc<Inner>, f: impl FnOnce(&Inner) -> Result<T> + Send + 'static) -> Result<T> {
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
            return Err(retry(format!("bootstrap envelope version {} != {ENVELOPE_VERSION}", h.envelope_version)));
        }
        let corrupt = |e: Error| {
            tracing::error!(error = %e, "invalid bootstrap header");
            SessionEnd::Halt(HaltReason::Corrupt)
        };
        let identity = h.identity.as_ref().ok_or_else(|| Error::corrupt("bootstrap header without identity"));
        let identity = identity.and_then(identity_from_pb).map_err(corrupt)?;
        let history = checked_history(&identity, &h.epoch_history).map_err(corrupt)?;
        let cfs: Vec<CfEntry> = h.cf_specs.iter().map(cf_from_pb).collect();
        for c in &cfs {
            validate_cf_name(&c.spec.name).map_err(|e| retry(e.to_string()))?;
        }
        if !cfs.windows(2).all(|w| w[0].spec.name.as_bytes() < w[1].spec.name.as_bytes()) {
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

async fn next(inner: &Inner, generation: u64, s: &mut Streaming<pb::BootstrapChunk>) -> std::result::Result<Kind, SessionEnd> {
    match unless_repointed(inner, generation, s.message()).await? {
        Ok(Some(chunk)) => chunk.kind.ok_or_else(|| retry("empty bootstrap chunk")),
        Ok(None) => Err(retry("bootstrap stream ended before the footer")),
        Err(status) => Err(status_end(inner, &status)),
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
    inner.force_bootstrap.store(false, Ordering::SeqCst);
    inner.set_ident(None);
    inner.reset_downstream();
    inner.seq_tx.send_replace(0);
    inner.set_state(SlaveState::Bootstrapping { target_seq: 0, records: 0 });
    blocking(inner, |i| i.store.reset_for_bootstrap())
        .await
        .map_err(|e| retry(format!("could not reset for bootstrap: {e}")))?;

    let limit = usize::try_from(hs.max_entry_bytes)
        .unwrap_or(usize::MAX)
        .max(MAX_BOOTSTRAP_CHUNK_BYTES)
        .saturating_add(DECODE_SLACK);
    let req = pb::BootstrapRequest { client_id: inner.cfg.identifier.clone() };
    let mut stream = match unless_repointed(inner, generation, c.clone().max_decoding_message_size(limit).bootstrap(req)).await? {
        Ok(r) => r.into_inner(),
        Err(s) => return Err(status_end(inner, &s)),
    };
    let Kind::Header(header) = next(inner, generation, &mut stream).await? else {
        return Err(retry("bootstrap stream did not start with a header"));
    };
    let plan = Plan::from_header(header)?;
    let s = plan.snapshot_seq;
    tracing::info!(node = %inner.cfg.identifier, snapshot_seq = s, cfs = plan.cfs.len(), "bootstrapping");
    inner.notify(|l| l.on_bootstrap_begin(s));
    inner.set_state(SlaveState::Bootstrapping { target_seq: s, records: 0 });

    for cf in &plan.cfs {
        let cf = cf.clone();
        match blocking(inner, move |i| i.store.begin_bootstrap_cf(&cf)).await {
            Ok(()) => {}
            Err(Error::UnknownComparator(name)) => {
                tracing::error!(node = %inner.cfg.identifier, comparator = %name, "bootstrap needs an unregistered comparator");
                return Err(SessionEnd::Halt(HaltReason::SpecMismatch));
            }
            Err(e) => return Err(retry(format!("could not create bootstrap column family: {e}"))),
        }
    }

    let mut digest = Digest::new();
    let mut marked = 0usize; // CFs whose digest marker has been added
    let mut last_key: Option<Vec<u8>> = None;
    let mut count = 0u64;
    let footer = loop {
        match next(inner, generation, &mut stream).await? {
            Kind::Records(r) => {
                let j = plan.cfs.iter().position(|c| c.spec.name == r.cf).ok_or_else(|| retry(format!("records for unknown CF {}", r.cf)))?;
                if j + 1 < marked {
                    return Err(retry(format!("records for CF {} arrived out of order", r.cf)));
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
                        if inner.store.tuning().comparators.compare(comparator, prev, &rec.key) != KeyOrdering::Less {
                            return Err(retry(format!("bootstrap keys of CF {} are not strictly ascending", r.cf)));
                        }
                    }
                    envelope::decode(&rec.envelope).map_err(|e| retry(format!("bad envelope from upstream: {e}")))?;
                    digest.record(&rec.key, &rec.envelope);
                    last_key = Some(rec.key.clone());
                    batch.push((rec.key, rec.envelope));
                }
                count += batch.len() as u64;
                let cf = plan.cfs[j].clone();
                blocking(inner, move |i| i.store.write_bootstrap_records(&cf, &batch))
                    .await
                    .map_err(|e| retry(format!("could not write bootstrap records: {e}")))?;
                inner.set_state(SlaveState::Bootstrapping { target_seq: s, records: count });
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
    inner.max_entry_bytes.store(plan.max_entry_bytes, Ordering::SeqCst);
    inner.set_ident(Some((plan.identity, plan.history)));
    tracing::info!(node = %inner.cfg.identifier, snapshot_seq = s, records = count, "bootstrap complete");
    inner.notify(|l| l.on_bootstrap_complete(s));
    inner.seq_tx.send_replace(s);
    Ok(plan.lease_id)
}
```

- [ ] **Step 6: Run the tests**

Run: `cargo test --test repoint`
Expected: PASS, 4 tests: `failover_runbook_end_to_end` (choose the candidate from `promotion_info`, `promote_checked`, repoint the sibling → `EpochAdvanced` with reads served throughout and no second bootstrap; the new master lists it in sync; repointing to the same upstream → `Resumed`), `divergence_is_refused_unless_data_loss_is_accepted` (`Err(Diverged { fork_seq, local_last_seq, divergent_entries: 2 })`, halted `Diverged` with its data readable; `Repair` → `Err(Unsupported)`, still halted with data; `Rebootstrap { accept_loss: AcceptDataLoss }` → `Bootstrapped`, converges, divergent keys gone), `repoint_refuses_loops` (itself and its own downstream → `Err(Halted(LoopDetected))` with data intact; a valid upstream then → `Resumed`), `repoint_to_a_stale_master_keeps_retrying` (restarted old master → `Retrying { last_error: "STALE_EPOCH: …" }`, not halted, split-brain write not applied; back to the new master → `Resumed`, no bootstrap).

Run: `cargo test && cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings`
Expected: PASS; no warnings.

- [ ] **Step 7: Update DESIGN.md and PROTOCOL.md**

In DESIGN.md §13.1, after:

```
- `promote_checked` and `demote` refuse by default and **hand the untouched, still-running node back**: `Err(PromoteRefused { error, slave })` / `Err(DemoteRefused { error, master })`, with `into_slave()` / `into_master()`. Only a failure after the node has started shutting down for the change (an I/O error during shutdown, a `ReadSnapshot` that still holds the database, a failure while rewriting `__meta`) loses the handle (`None`); the directory is then reopened by the application. Plain `promote` keeps its behaviour: a refusal shuts the slave down with its directory unchanged.
```

add:

```
**Repoint.** `Slave::repoint(upstream, RepointOptions) -> Result<RepointOutcome>` switches a running slave to another upstream; reads keep being served. It closes the current upstream stream (a bootstrap in progress is abandoned and starts from scratch against the new upstream), clears a halt, and handshakes the new upstream under all the normal rules (loop detection, `STALE_EPOCH`, epoch continuity). It returns once that handshake settles: `Resumed`, `EpochAdvanced`, `Bootstrapped`, or `Retrying { last_error }` when the new upstream cannot be used yet (unreachable, `STALE_EPOCH`, `NOT_READY`; the slave keeps retrying it). The new upstream is not persisted: after a restart, open the slave with it.

**Divergence is never resolved implicitly by a repoint.** The configured `on_divergence` is not used for the repoint's handshake; `RepointOptions::on_divergence` is:
- `Refuse` (default): `Error::Diverged { fork_seq, local_last_seq, divergent_entries }`; the slave halts `Diverged` with its data intact and readable.
- `Rebootstrap { accept_loss: AcceptDataLoss }`: discard all local data and bootstrap from the new upstream.
- `Repair { accept_loss: AcceptDataLoss }`: roll back only the divergent entries (§22). Until verify sessions exist it returns `Error::Unsupported`, and the slave halts `Diverged` with its data intact.

`AcceptDataLoss` is a unit struct the caller writes out; nothing supplies it by default. A repoint at the slave itself or at one of its own downstream nodes halts `LoopDetected` (`Err(Halted(LoopDetected))`) until it is repointed again.

**Failover runbook.**
1. Fence the old master: stop it, or cut it off from clients and replicas.
2. Gather the status of every replica (`admin::node_status`, or `Slave::promotion_info` in-process).
3. Promote the most advanced compatible one: `admin::choose_promotion_candidate`, then `Slave::promote_checked` with the others as peers. If the wrong node was promoted, `Master::demote` undoes it while it has no writes.
4. `Slave::repoint` every other replica at the new master. Compatible replicas resume or advance their epoch; diverged ones are refused until the operator accepts the loss of their divergent entries.

With `min_insync_replicas ≥ 1`, the most advanced in-sync replica holds every acknowledged write, so step 3 loses none of them.
```

In DESIGN.md §13 (policies), replace:

```
  - `on_divergence: DivergencePolicy` — `Halt` (default) halts with `Diverged`; `Rebootstrap` bootstraps. Applies to `DIVERGED`.
```

with:

```
  - `on_divergence: DivergencePolicy` — `Halt` (default) halts with `Diverged`; `Rebootstrap` bootstraps. Applies to `DIVERGED` on reconnects; an explicit `Slave::repoint` uses `RepointOptions::on_divergence` instead (§13.1).
```

In DESIGN.md §13 (policies), replace:

```
a demoted master never does, and the operator must re-point the slave.
```

with:

```
a demoted master never does, and the operator must re-point the slave (`Slave::repoint`).
```

In DESIGN.md §16 (impl Slave), after:

```
    pub async fn clear_halt(&self, action: HaltAction) -> Result<()>;   // operator action
```

add:

```
    pub async fn repoint(&self, upstream: UpstreamConfig, opts: RepointOptions) -> Result<RepointOutcome>;
                                             // switch upstream at runtime (§13.1)
```

In DESIGN.md §16 (Slave types), after:

```
pub enum HaltAction { Retry, Rebootstrap }
```

add:

```
pub struct AcceptDataLoss;                   // written out by the caller to accept discarding data
pub struct RepointOptions { pub on_divergence: RepointDivergence }   // Default: Refuse
pub enum RepointDivergence { Refuse, Repair { accept_loss: AcceptDataLoss }, Rebootstrap { accept_loss: AcceptDataLoss } }
pub enum RepointOutcome { Resumed, EpochAdvanced, Bootstrapped, Repaired { discarded: RangeInclusive<Seq> },
                          Retrying { last_error: String } }
```

In DESIGN.md §16.2, replace:

```
and `AckTimeout { seq, acked, required }` (written, not acknowledged in time; §5.2), 
```

with:

```
and `AckTimeout { seq, acked, required }` (written, not acknowledged in time; §5.2), `Diverged { fork_seq, local_last_seq, divergent_entries }` (a refused `repoint`), `Unsupported`, 
```

In DESIGN.md §16.3, replace:

```
It is the only way out of `Diverged` or `IdentityChanged` under `Refuse`.
```

with:

```
It is the only way out of `Diverged` or `IdentityChanged` under `Refuse`, apart from `repoint`, which clears any halt before it handshakes the new upstream (§13.1).
```

In DESIGN.md §22, replace:

```
- Divergence is detected by CRC comparison on connect and by rejecting slaves ahead of the master. It is not repaired automatically.
```

with:

```
- Divergence is detected by CRC comparison on connect and by rejecting slaves ahead of the master. It is not repaired automatically: an explicit `repoint` refuses by default and discards entries only with `AcceptDataLoss`.
```

In PROTOCOL.md §6.4, replace:

```
- Replicas that had received entries the promoted node never saw are at or past the fork point and get `DIVERGED` (rule 8). Their divergence policy decides whether they halt or bootstrap.
```

with:

```
- Replicas that had received entries the promoted node never saw are at or past the fork point and get `DIVERGED` (rule 8). Their divergence policy decides whether they halt or bootstrap. When an operator explicitly re-points a replica, an implementation SHOULD NOT discard its divergent entries without an explicit confirmation.
```

- [ ] **Step 8: Commit**

```bash
git add src tests DESIGN.md PROTOCOL.md
git commit -m "feat: Slave::repoint with explicit data-loss consent and the failover runbook

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 14: TLS, mutual TLS and token authentication end to end

**Files:**
- Create: `tests/security.rs`

**Interfaces:**
- Consumes: Task 4 `TlsServerConfig`, `TlsClientConfig`, `TlsIdentity`, `TokenAuthenticator`, `TokenCredentials`, `AllowMethods`, `MtlsSubjectAllowList`, `Method`; Task 8/10 harness; rcgen 0.14 (`KeyPair::generate`, `CertificateParams::{new, self_signed, signed_by}`, `Issuer::new`, `DnType::CommonName`, `IsCa::Ca(BasicConstraints::Unconstrained)`).
- Produces: nothing new in `src/` — this task proves the Task 4/5/8 code end to end. If a test fails, fix the code it points at (`src/server/mod.rs::call_info`/`authorize`, `src/slave/client.rs`, `src/config.rs::to_tonic`).

- [ ] **Step 1: Write the tests**

Create `tests/security.rs`:

```rust
//! TLS, mutual TLS and token authentication, end to end, with certificates made by rcgen.

mod support;

use std::sync::Arc;

use replicated_rocksdb::{
    AllowMethods, CfSpec, Error, HaltReason, Master, Method, MtlsSubjectAllowList, Slave, SlaveState,
    TlsClientConfig, TlsIdentity, TlsServerConfig, TokenAuthenticator, TokenCredentials,
};
use support::{assert_converged, wait_state, wait_until, Cluster};

/// A CA and the certificates it signs.
struct Pki {
    ca_pem: Vec<u8>,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl Pki {
    fn new() -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params.distinguished_name.push(rcgen::DnType::CommonName, "rrdb test CA");
        let cert = params.self_signed(&key).unwrap();
        Pki { ca_pem: cert.pem().into_bytes(), issuer: rcgen::Issuer::new(params, key) }
    }

    /// A certificate for `localhost` (server) or with subject CN `cn` (client).
    fn identity(&self, sans: &[&str], cn: &str) -> TlsIdentity {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>()).unwrap();
        params.distinguished_name.push(rcgen::DnType::CommonName, cn);
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        TlsIdentity { cert_pem: cert.pem().into_bytes(), key_pem: key.serialize_pem().into_bytes() }
    }

    fn server(&self, mutual: bool) -> TlsServerConfig {
        TlsServerConfig {
            identity: self.identity(&["localhost"], "localhost"),
            client_ca_pem: mutual.then(|| self.ca_pem.clone()),
        }
    }

    fn client(&self, identity: Option<TlsIdentity>) -> TlsClientConfig {
        TlsClientConfig { ca_pem: self.ca_pem.clone(), domain_name: Some("localhost".into()), identity }
    }
}

async fn tls_master(c: &Cluster, pki: &Pki, mutual: bool, auth: Arc<dyn replicated_rocksdb::Authenticator>) -> Master {
    let mut cfg = c.master_config("a");
    let serve = cfg.serve.as_mut().unwrap();
    serve.tls = Some(pki.server(mutual));
    serve.authenticator = auth;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    m
}

async fn tls_slave(
    c: &Cluster,
    m: &Master,
    name: &str,
    tls: TlsClientConfig,
    token: Option<&str>,
) -> Slave {
    let (mut cfg, _) = c.slave_config(name, m.serve_addr().unwrap());
    cfg.serve = None;
    cfg.upstream.tls = Some(tls);
    if let Some(t) = token {
        cfg.upstream.credentials = Arc::new(TokenCredentials::new(t));
    }
    Slave::open(cfg).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokens_over_tls_accept_any_valid_secret_and_reject_others() {
    let c = Cluster::new();
    let pki = Pki::new();
    let auth = Arc::new(TokenAuthenticator::new(["old-token", "new-token"]));
    let m = tls_master(&c, &pki, false, auth).await;
    let old = tls_slave(&c, &m, "old", pki.client(None), Some("old-token")).await;
    let new = tls_slave(&c, &m, "new", pki.client(None), Some("new-token")).await;
    assert_converged(&m, &[("old", &old), ("new", &new)]).await;

    let bad = tls_slave(&c, &m, "bad", pki.client(None), Some("wrong-token")).await;
    wait_state(&bad, "AuthRejected", |s| *s == SlaveState::Halted(HaltReason::AuthRejected)).await;
    let none = tls_slave(&c, &m, "none", pki.client(None), None).await;
    wait_state(&none, "AuthRejected", |s| *s == SlaveState::Halted(HaltReason::AuthRejected)).await;
    assert!(matches!(bad.wait_for_seq(1, support::WAIT).await, Err(Error::Halted(HaltReason::AuthRejected))));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_can_be_denied_per_method() {
    let c = Cluster::new();
    let pki = Pki::new();
    let token = Arc::new(TokenAuthenticator::new(["t"]));
    let auth = Arc::new(AllowMethods::new(token, &[Method::Handshake, Method::Tail]));
    let m = tls_master(&c, &pki, false, auth).await;
    let s = tls_slave(&c, &m, "b", pki.client(None), Some("t")).await;
    wait_state(&s, "AuthRejected", |st| *st == SlaveState::Halted(HaltReason::AuthRejected)).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_authentication_on_plaintext_is_refused_unless_allowed() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    cfg.serve.as_mut().unwrap().authenticator = Arc::new(TokenAuthenticator::new(["t"]));
    assert!(matches!(Master::open(cfg.clone()).await, Err(Error::Config(_))));

    cfg.serve.as_mut().unwrap().allow_plaintext_token = true;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut scfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    scfg.serve = None;
    scfg.upstream.credentials = Arc::new(TokenCredentials::new("t"));
    let s = Slave::open(scfg).await.unwrap();
    assert_converged(&m, &[("b", &s)]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_tls_allow_list_by_certificate_subject() {
    let c = Cluster::new();
    let pki = Pki::new();
    let allow = MtlsSubjectAllowList::new().allow_all_methods("node-b").allow("node-ro", &[Method::Handshake, Method::Tail]);
    let m = tls_master(&c, &pki, true, Arc::new(allow)).await;

    let b = tls_slave(&c, &m, "b", pki.client(Some(pki.identity(&[], "node-b"))), None).await;
    assert_converged(&m, &[("b", &b)]).await;

    let intruder = tls_slave(&c, &m, "x", pki.client(Some(pki.identity(&[], "intruder"))), None).await;
    wait_state(&intruder, "AuthRejected", |s| *s == SlaveState::Halted(HaltReason::AuthRejected)).await;

    let read_only = tls_slave(&c, &m, "ro", pki.client(Some(pki.identity(&[], "node-ro"))), None).await;
    wait_state(&read_only, "AuthRejected on Bootstrap", |s| *s == SlaveState::Halted(HaltReason::AuthRejected)).await;

    // Without a client certificate the TLS handshake itself fails: a transport error, retried.
    let anonymous = tls_slave(&c, &m, "anon", pki.client(None), None).await;
    wait_until("a transport error", || anonymous.status().last_error.is_some()).await;
    assert_eq!(anonymous.status().state, SlaveState::Connecting);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_ca_is_a_retried_transport_error() {
    let c = Cluster::new();
    let pki = Pki::new();
    let other = Pki::new();
    let m = tls_master(&c, &pki, false, Arc::new(replicated_rocksdb::AllowAll)).await;
    let s = tls_slave(&c, &m, "b", other.client(None), None).await;
    wait_until("a transport error", || s.status().last_error.is_some()).await;
    assert_eq!(s.status().state, SlaveState::Connecting);
    let ok = tls_slave(&c, &m, "ok", pki.client(None), None).await;
    assert_converged(&m, &[("ok", &ok)]).await;
}
```

- [ ] **Step 2: Run them**

Run: `cargo test --test security`
Expected: PASS, 5 tests:
- `tokens_over_tls_accept_any_valid_secret_and_reject_others` (two rotated secrets converge; a wrong or missing token halts `AuthRejected`);
- `bootstrap_can_be_denied_per_method` (`PERMISSION_DENIED` on Bootstrap → `AuthRejected`);
- `token_authentication_on_plaintext_is_refused_unless_allowed`;
- `mutual_tls_allow_list_by_certificate_subject` (CN `node-b` converges; CN `intruder` and a Handshake/Tail-only CN halt `AuthRejected`; no client certificate is a retried transport error);
- `a_wrong_ca_is_a_retried_transport_error`.

- [ ] **Step 3: Commit**

```bash
git add tests
git commit -m "test: tls, mutual tls and token authentication end to end

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 15: Failure and resume, with fault injection

**Files:**
- Create: `tests/failure_resume.rs`, `tests/failpoints.rs`
- Modify: `src/slave/mod.rs` (`kill`), `src/server/mod.rs` (`abort`'s `allow`), `src/failpoint.rs` (`io_error`), `src/master/writer.rs` (failpoint in `bump_epoch`), `src/store/cfs.rs`, `src/store/mod.rs`, `src/codec/mod.rs` (unused Plan 1 placeholders), `Cargo.toml` (`[[test]]`), `DESIGN.md` (§16)

**Interfaces:**
- Consumes: Task 5 `failpoint::fire` points `server-tail-after-entry`; Task 8 points `slave-after-apply`, `slave-bootstrap-after-chunk` (a repoint also releases the latter's freeze, Task 13); Task 12 `Master::{put_with, replicas}`, `WriteConcern`, `Error::AckTimeout`; Task 13 `Slave::repoint`, `RepointOptions`, `RepointOutcome`; Task 5 `ServerHandle::abort`, `await_sole_owner`; Plan 1 `Inner::bump_epoch` (fences the writer when its synced write fails); harness (`restart_master`, `restart_slave`, `copy_dir`, `restore_dir`).
- Produces: `#[cfg(feature = "failpoints")] Slave::kill(self) -> Result<()>` (async): aborts every task without graceful steps and waits until the database is closed; `failpoint::io_error(name) -> Result<()>`; point `master-bump-epoch-write`.

- [ ] **Step 1: Write the failure/resume tests (no fault injection)**

Create `tests/failure_resume.rs`:

```rust
//! Failure and resume without fault injection: restarts anywhere in a chain, a downstream
//! that is ahead of its upstream, and a persisted LogGap halt.

mod support;

use std::time::Duration;

use replicated_rocksdb::{CfSpec, GapPolicy, HaltAction, HaltReason, LogApi, Master, ReadApi, Slave, SlaveState};
use support::{assert_converged, copy_dir, restore_dir, wait_state, wait_until, Cluster};

/// A → B → C with writes between every restart: master, mid-chain and leaf restarts all
/// resume from the durable position, with no bootstrap, gap or duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restarts_of_master_mid_chain_and_leaf_resume_without_bootstrap() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, lb) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    let (c_cfg, lc) = c.slave_config("c", b.serve_addr().unwrap());
    let cn = Slave::open(c_cfg.clone()).await.unwrap();
    let mut n = 0u32;
    let mut write = |m: &Master, count: u32| {
        for _ in 0..count {
            n += 1;
            m.put("c", &n.to_be_bytes(), &n.to_le_bytes()).unwrap();
        }
    };
    write(&a, 10);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let a = c.restart_master(a, c.master_config("a")).await;
    write(&a, 10);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let b = c.restart_slave(b, b_cfg).await;
    write(&a, 10);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    let cn = c.restart_slave(cn, c_cfg.clone()).await;
    write(&a, 10);
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;

    assert_eq!((lb.bootstraps(), lc.bootstraps()), (1, 1), "only the initial bootstraps");
    lb.assert_no_violations();
    lc.assert_no_violations();
}

/// B crashes after forwarding entries it had not flushed, so C is ahead of B. B answers
/// RESUME; C's Tail waits with heartbeats until B has caught up from A, then continues.
/// (A restored copy of B's directory stands in for the unflushed tail of B's log.)
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_downstream_ahead_of_its_upstream_waits_and_converges() {
    let c = Cluster::new();
    let a = c.master("a").await;
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    let b_port = b.serve_addr().unwrap().port();
    b.shutdown().await.unwrap();
    copy_dir(&c.path("b"), &c.path("b-old"));
    let b = Slave::open({
        let mut cfg = b_cfg.clone();
        cfg.serve.as_mut().unwrap().port = b_port;
        cfg
    })
    .await
    .unwrap();

    let (c_cfg, lc) = c.slave_config("c", b.serve_addr().unwrap());
    let cn = Slave::open(c_cfg).await.unwrap();
    for i in 0..10u8 {
        a.put("c", &[i], b"v").unwrap();
    }
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    let c_seq = cn.last_seq();

    // B loses everything after the old copy; A is stopped so B cannot catch up yet.
    let a_port = a.serve_addr().unwrap().port();
    a.shutdown().await.unwrap();
    b.shutdown().await.unwrap();
    restore_dir(&c.path("b-old"), &c.path("b"));
    let mut cfg = b_cfg.clone();
    cfg.serve.as_mut().unwrap().port = b_port;
    let b = Slave::open(cfg).await.unwrap();
    assert!(b.last_seq() < c_seq);

    // C resumes against B without a bootstrap and waits (heartbeats only).
    wait_state(&cn, "C streaming from B", |s| *s == SlaveState::Streaming).await;
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert_eq!(cn.last_seq(), c_seq);
    assert_eq!(cn.status().state, SlaveState::Streaming);

    // A comes back; B catches up and passes C; C continues.
    let mut a_cfg = c.master_config("a");
    a_cfg.serve.as_mut().unwrap().port = a_port;
    let a = Master::open(a_cfg).await.unwrap();
    a.put("c", b"after", b"v").unwrap();
    assert_converged(&a, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(lc.bootstraps(), 1, "only C's initial bootstrap");
    lc.assert_no_violations();
}

/// GapPolicy::Halt: a slave that fell behind retention halts with LogGap, the halt survives
/// a restart, and clear_halt(Rebootstrap) recovers it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_log_gap_halt_persists_across_restart_until_cleared() {
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_entries = Some(2);
    mcfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.on_log_gap = GapPolicy::Halt;
    let b = Slave::open(cfg.clone()).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    // Wait for the Tail that presents the bootstrap lease; until then the lease holds retention.
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    let behind = b.last_seq();
    b.shutdown().await.unwrap();

    for i in 0..20u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    wait_until("master retention", || m.first_retained_seq() > behind + 1).await;
    let b = Slave::open(cfg.clone()).await.unwrap();
    wait_state(&b, "LogGap halt", |s| *s == SlaveState::Halted(HaltReason::LogGap)).await;
    assert_eq!(b.last_seq(), behind);
    assert_eq!(b.list_cfs(), vec![CfSpec::new("c")], "reads keep working");

    b.shutdown().await.unwrap();
    let (mut cfg2, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg2.on_log_gap = GapPolicy::Halt;
    let b = Slave::open(cfg2).await.unwrap();
    assert_eq!(b.status().state, SlaveState::Halted(HaltReason::LogGap), "persisted across restart");
    b.clear_halt(HaltAction::Rebootstrap).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1);
}
```

- [ ] **Step 2: Run them**

Run: `cargo test --test failure_resume`
Expected: PASS, 3 tests: `restarts_of_master_mid_chain_and_leaf_resume_without_bootstrap`, `a_downstream_ahead_of_its_upstream_waits_and_converges`, `a_log_gap_halt_persists_across_restart_until_cleared`. (These exercise code from Tasks 5–10; if one fails, the bug is there.)

- [ ] **Step 3: Write the failing fault-injection tests**

Create `tests/failpoints.rs`:

```rust
//! Fault injection, end to end. Needs `--features failpoints`. The tests share the global
//! failpoint registry, so they run one at a time (a file-level mutex plus `FailScenario`).

mod support;

use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use replicated_rocksdb::{
    CfSpec, Error, LogApi, Master, ReadApi, RepointOptions, RepointOutcome, Slave, SlaveState, UpstreamConfig, WriteConcern,
};
use support::{assert_converged, wait_until, Cluster};

static SERIAL: Mutex<()> = Mutex::const_new(());

async fn serial() -> (MutexGuard<'static, ()>, fail::FailScenario<'static>) {
    let guard = SERIAL.lock().await;
    (guard, fail::FailScenario::setup())
}

/// A slave killed in the middle of a bootstrap restarts it from scratch on reopen and
/// converges; the server's guard of the abandoned bootstrap is released.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_mid_bootstrap_restarts_the_bootstrap() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 64;
    mcfg.retention.max_entries = Some(1);
    mcfg.retention.check_interval = Duration::from_millis(20);
    // The server may finish streaming before the slave is killed; its lease then holds the
    // guard until it expires.
    mcfg.serve.as_mut().unwrap().limits.bootstrap_lease_ttl = Duration::from_millis(500);
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..100u8 {
        m.put("c", &[i], &[i; 100]).unwrap();
    }
    fail::cfg("slave-bootstrap-after-chunk", "1*return").unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    wait_until("frozen after the first chunk", || {
        matches!(b.status().state, SlaveState::Bootstrapping { records, .. } if records > 0)
    })
    .await;
    b.kill().await.unwrap();
    fail::remove("slave-bootstrap-after-chunk");
    // The abandoned bootstrap's guard is released (the job ends when the client is gone, or
    // its unclaimed lease expires): retention runs past its S again.
    for i in 0..3u8 {
        m.put("c", &[b'x', i], b"v").unwrap();
    }
    wait_until("the abandoned guard is released", || m.first_retained_seq() == m.last_seq()).await;

    let (cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    // The interrupted bootstrap left no readable state: reads are not ready until it is redone.
    match b.get("c", &[0]) {
        Err(replicated_rocksdb::Error::NotReady) => {}
        Ok(v) => assert_eq!(v, Some(vec![0; 100]), "only a completed bootstrap is readable"),
        Err(e) => panic!("unexpected error {e}"),
    }
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1);
    scenario.teardown();
}

/// A crash between commit and listener: the entry is durable and is not delivered again
/// (at-most-once); replication resumes after it with no gap or duplicate.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn crash_after_apply_resumes_without_gap_or_duplicate() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (cfg, first) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    m.put("c", b"1", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;

    fail::cfg("slave-after-apply", "1*return").unwrap();
    let crashed = m.put("c", b"2", b"v").unwrap();
    wait_until("entry committed", || b.last_seq() == crashed).await;
    b.kill().await.unwrap();
    fail::remove("slave-after-apply");
    assert!(!first.entries().contains(&crashed), "the listener never saw the crashed entry");

    for k in [&b"3"[..], b"4"] {
        m.put("c", k, b"v").unwrap();
    }
    let (cfg, second) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_eq!(b.get("c", b"2").unwrap(), Some(b"v".to_vec()), "durable before the crash");
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(second.entries(), vec![crashed + 1, crashed + 2]);
    assert_eq!(second.bootstraps(), 0);
    scenario.teardown();
}

/// The connection drops mid-tail: the slave reconnects and resumes, no bootstrap, no
/// duplicate or missing entry.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn connection_drop_mid_tail_resumes() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    let start = b.last_seq();
    fail::cfg("server-tail-after-entry", "3*return").unwrap();
    for i in 0..20u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    assert_converged(&m, &[("b", &b)]).await;
    fail::remove("server-tail-after-entry");
    assert_eq!(listener.entries(), ((start + 1)..=m.last_seq()).collect::<Vec<_>>());
    assert_eq!(listener.bootstraps(), 1, "only the initial bootstrap");
    let reconnects = listener.states().iter().filter(|s| **s == SlaveState::Streaming).count();
    assert!(reconnects >= 4, "three drops, three reconnects: {:?}", listener.states());
    scenario.teardown();
}

/// Aggressive retention (`max_entries = 1`) while a bootstrap is slow: the lease keeps entry
/// S + 1, so the slave tails from S + 1 without a second bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_lease_survives_aggressive_retention() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.retention.max_entries = Some(1);
    mcfg.retention.check_interval = Duration::from_millis(10);
    mcfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 64;
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..20u8 {
        m.put("c", &[i], &[i; 100]).unwrap();
    }
    let s = m.last_seq();
    // The slave pauses after its first chunk (until the point is removed) while the master
    // writes on and retention runs.
    fail::cfg("slave-bootstrap-after-chunk", "pause").unwrap();
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_until("bootstrap paused", || matches!(b.status().state, SlaveState::Bootstrapping { records, .. } if records > 0)).await;
    for i in 0..10u8 {
        m.put("c", &[b'x', i], b"v").unwrap();
    }
    wait_until("retention trims up to the bootstrap guard", || m.first_retained_seq() == s + 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await; // ten more retention rounds
    assert_eq!(m.first_retained_seq(), s + 1, "the bootstrap guard keeps S + 1");
    fail::remove("slave-bootstrap-after-chunk"); // unpauses the slave
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 1, "tailed from S + 1 with the lease: no second bootstrap");
    wait_until("retention resumes after the lease", || m.first_retained_seq() == m.last_seq()).await;
    scenario.teardown();
}

/// A failed epoch bump fences the master's writer until reopen (the durability of the bump
/// is unknown); the reopened master is at the old epoch and accepts writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn a_failed_epoch_bump_fences_the_master_writer() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    fail::cfg("master-bump-epoch-write", "1*return").unwrap();
    assert!(m.bump_epoch().is_err());
    fail::remove("master-bump-epoch-write");
    assert!(matches!(m.put("c", b"k", b"v"), Err(replicated_rocksdb::Error::WriterFenced)));
    assert_eq!(m.identity().epoch, 1);
    m.shutdown().await.unwrap();
    let m = Master::open(c.master_config("a")).await.unwrap();
    assert_eq!(m.identity().epoch, 1);
    assert_eq!(m.put("c", b"k", b"v").unwrap(), 2);
    scenario.teardown();
}

/// A repoint in the middle of a bootstrap abandons it; the bootstrap starts from scratch
/// against the new upstream and converges.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repoint_during_bootstrap_restarts_it_against_the_new_upstream() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let mut mcfg = c.master_config("a");
    mcfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 64;
    let m = Master::open(mcfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..100u8 {
        m.put("c", &[i], &[i; 100]).unwrap();
    }
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;

    fail::cfg("slave-bootstrap-after-chunk", "1*return").unwrap();
    let (cn, listener) = c.slave("c", m.serve_addr().unwrap()).await;
    wait_until("frozen after the first chunk", || {
        matches!(cn.status().state, SlaveState::Bootstrapping { records, .. } if records > 0)
    })
    .await;
    fail::remove("slave-bootstrap-after-chunk");
    let repoint = cn.repoint(UpstreamConfig::from_addr(b.serve_addr().unwrap()), RepointOptions::default());
    let outcome = tokio::time::timeout(support::WAIT, repoint).await.expect("repoint settles").unwrap();
    assert_eq!(outcome, RepointOutcome::Bootstrapped);
    m.put("c", b"after", b"v").unwrap();
    assert_converged(&m, &[("b", &b), ("c", &cn)]).await;
    assert_eq!(listener.bootstraps(), 1, "one completed bootstrap");
    assert_eq!(cn.status().upstream_chain, vec!["b".to_string(), "a".to_string()]);
    scenario.teardown();
}

/// The only in-sync replica commits an entry but never acknowledges it: the write is
/// committed on the master and returns `AckTimeout`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_replica_that_stops_acking_times_the_write_out() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_until("b in sync", || m.replicas().iter().any(|r| r.node_id == "b" && r.in_sync)).await;
    let wc = WriteConcern { min_insync_replicas: 1, ack_timeout: Duration::from_millis(300) };
    m.put_with("c", b"acked", b"v", wc).unwrap();

    fail::cfg("slave-after-apply", "1*return").unwrap();
    let err = m.put_with("c", b"never acked", b"v", wc).unwrap_err();
    let seq = m.last_seq();
    assert!(matches!(err, Error::AckTimeout { seq: s, acked: 0, required: 1 } if s == seq), "{err}");
    assert_eq!(m.get("c", b"never acked").unwrap(), Some(b"v".to_vec()), "committed on the master");
    wait_until("committed on b, not acknowledged", || b.last_seq() == seq).await;
    assert!(m.replicas().iter().all(|r| r.applied_seq < seq));
    b.kill().await.unwrap();
    fail::remove("slave-after-apply");
    scenario.teardown();
}
```

Append to `Cargo.toml`:

```toml
[[test]]
name = "failpoints"
required-features = ["failpoints"]
```

- [ ] **Step 4: Run them to confirm they fail**

Run: `cargo test --features failpoints --test failpoints`
Expected: FAIL to compile: `no method named kill found for struct Slave`.

Run: `cargo test`
Expected: PASS; `failpoints` is skipped without the feature.

- [ ] **Step 5: Implement `Slave::kill`**

In `src/slave/mod.rs`, before `impl Drop for Slave`, add:

```rust
#[cfg(feature = "failpoints")]
impl Slave {
    /// Test-only (feature `failpoints`): stops every task at once without any graceful step
    /// and waits until the database is closed, emulating a process crash.
    pub async fn kill(mut self) -> Result<()> {
        self.inner.shutdown_tx.send_replace(true);
        if let Some(server) = self.server.take() {
            server.abort();
        }
        for task in [self.driver.take(), self.maintenance.take()].into_iter().flatten() {
            task.abort();
            let _ = task.await;
        }
        if await_sole_owner(&self.inner, Duration::from_secs(10)).await { Ok(()) } else { Err(Error::Timeout) }
    }
}
```

`ServerHandle::abort` is now used: in `src/server/mod.rs`, replace the line `#[allow(dead_code)] // used by the slave crash tests (later Plan 2 task)` above `pub(crate) fn abort` with:

```rust
    #[cfg_attr(not(feature = "failpoints"), allow(dead_code))] // used by Slave::kill
```

Append to `src/failpoint.rs`:

```rust
/// `Err(Io)` when the named point fires: injects a storage failure.
pub(crate) fn io_error(name: &str) -> crate::Result<()> {
    if fire(name) { Err(crate::Error::Io(std::io::Error::other(format!("failpoint {name}")))) } else { Ok(()) }
}
```

In `src/master/writer.rs`, in `Inner::bump_epoch`, replace:

```rust
        if let Err(e) = self.store.write_meta(
            &[
                (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
            ],
            true,
        ) {
```

with:

```rust
        let written = crate::failpoint::io_error("master-bump-epoch-write").and_then(|()| {
            self.store.write_meta(
                &[
                    (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                    (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
                ],
                true,
            )
        });
        if let Err(e) = written {
```

- [ ] **Step 6: Run the fault-injection tests**

Run: `cargo test --features failpoints --test failpoints`
Expected: PASS, 7 tests (run one at a time through the file-level mutex): `crash_mid_bootstrap_restarts_the_bootstrap` (also asserts the abandoned bootstrap's guard is released: retention passes its `S` again), `crash_after_apply_resumes_without_gap_or_duplicate`, `connection_drop_mid_tail_resumes`, `bootstrap_lease_survives_aggressive_retention` (the slave is held with the `pause` action — no sleeps as synchronisation, no blocked timing windows — and released with `fail::remove`), `a_failed_epoch_bump_fences_the_master_writer`, `repoint_during_bootstrap_restarts_it_against_the_new_upstream` (a slave frozen after its first bootstrap chunk is repointed at a converged sibling: `Bootstrapped`, one completed bootstrap, converged, chain `["b", "a"]`), `a_replica_that_stops_acking_times_the_write_out` (the only in-sync replica commits an entry and freezes before acknowledging: `AckTimeout { acked: 0, required: 1 }`, the entry is committed on the master and on the replica, and `replicas()` still shows the older ack).

- [ ] **Step 7: Remove Plan 1 placeholders Plan 2 does not use**

These items were kept for Plan 2 behind `allow`s, but Plan 2 uses `cfs.pending.lock()`, `LogEntry::decode_ops()` and `LogSource::role()` instead. Delete them:
- in `src/store/cfs.rs`: the methods `pending_entries` and `push_pending`, each with its `#[allow(dead_code)] // used by bootstrap (Plan 2)` line;
- in `src/store/mod.rs`: the method `role(&self) -> Role` with its `#[allow(dead_code)] // used by the slave (later task)` line (the `role` field stays; `initialise_meta` uses it);
- in `src/codec/mod.rs`: the lines `#[allow(unused_imports)] // used by the replication client (Plan 2)` and `pub(crate) use entry::decode_ops;`.

Run: `grep -rnE "allow\((dead_code|unused_imports)\).*(Plan 2|\(later task\))" src`
Expected: no output.

- [ ] **Step 8: Run everything, lint and document**

Run: `cargo test && cargo test --features failpoints`
Expected: PASS: every unit, integration and doc test, with and without the feature.

Run: `cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings && cargo doc --no-deps`
Expected: no warnings; docs build. Fix anything flagged (typically an import left unused by an earlier stage).

- [ ] **Step 9: Update DESIGN.md §16**

In `impl Slave` (the block that ends `pub fn replicas(&self) -> Vec<ReplicaStatus>;` followed by `pub async fn shutdown(self) -> Result<()>;` — `impl Master` has a `shutdown` line too), replace:

```
    pub fn replicas(&self) -> Vec<ReplicaStatus>;
    pub async fn shutdown(self) -> Result<()>;
}
impl ReadApi for Slave {}  impl LogApi for Slave {}
```

with:

```
    pub fn replicas(&self) -> Vec<ReplicaStatus>;
    pub async fn shutdown(self) -> Result<()>;
    #[cfg(feature = "failpoints")]
    pub async fn kill(self) -> Result<()>;   // test-only: abort every task, emulating a crash
}
impl ReadApi for Slave {}  impl LogApi for Slave {}
```

- [ ] **Step 10: Commit**

```bash
git add Cargo.toml src tests DESIGN.md
git commit -m "test: failure and resume, with failpoints for crashes and dropped connections

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Out of scope for this plan (Plan 3)

| Spec item | Foundation built here |
|---|---|
| `VerifyBegin` / `VerifyRange` / `VerifyEnd`, `VerifyConfig`, `Slave::verify_now`, `MismatchPolicy`, `on_verify_complete` | Generated service with `UNIMPLEMENTED` stubs, `ReadSnapshot::{meta, raw_ctx}`, `ReadCtx::iter_raw`, `Digest`, `force_bootstrap` |
| Divergence repair (addendum §5): the `RepointDivergence::Repair` arm, `DivergencePolicy::Repair`, `Slave::diverged_entries`, `RepointOutcome::Repaired` actually returned | `repoint` (Task 13): the `Repair` arm in `slave::driver::session` returns `Error::Unsupported` and halts `Diverged` with data intact; `RepointOutcome::Repaired { discarded }`, `AcceptDataLoss`, `Error::Diverged`, `repoint::divergence` |
| `SlaveStatus::{lag_ms, last_verify}`, tracing spans per handshake/bootstrap/verify | `SlaveStatus`, `ReplicaStatus::lag_ms` (Task 6), `tracing` events already emitted |
