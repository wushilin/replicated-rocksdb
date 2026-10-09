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

> **Split note:** this plan is stored in two files because of the repository 500 KiB file limit. Tasks 1–8 are here; Tasks 9–15 are in [2026-10-09-plan2b-replication.md](2026-10-09-plan2b-replication.md). Header, Global Constraints, Review Focus and File Structure apply to both parts.

### Task 1: Dependencies, gRPC codegen and wire helpers

**Files:**
- Modify: `Cargo.toml`, `build.rs`, `src/lib.rs`
- Create: `src/net/mod.rs`, `src/net/convert.rs`

**Interfaces:**
- Consumes: Plan 1 `pb::*` (generated), `codec::PROTOCOL_VERSION`, `types::{DbIdentity, CfEntry, CfSpec, EpochStart, LogEntry}`, `Error::corrupt`.
- Produces:
  - Generated `pb::replication_server::{Replication, ReplicationServer}` and `pb::replication_client::ReplicationClient` (in addition to Plan 1's messages).
  - `net::{MD_PROTOCOL_VERSION = "x-rrdb-protocol-version", MD_NODE_ID = "x-rrdb-node-id", MD_AUTHORIZATION = "authorization", ERROR_DOMAIN = "rrdb.replication.v1", DECODE_SLACK: usize = 64 KiB, MAX_REQUEST_BYTES: usize = 4 MiB}`.
  - `net::code_for(pb::Reason) -> tonic::Code`, `net::reason_status(pb::Reason, impl Into<String>) -> Status`, `net::reason_of(&Status) -> Option<pb::Reason>`, `net::check_protocol_version(&MetadataMap) -> Result<(), Status>`, `net::bearer_token(&MetadataMap) -> Option<Vec<u8>>`, `net::node_id(&MetadataMap) -> Option<String>`.
  - `net::convert::checked_history(&DbIdentity, &[pb::EpochStart]) -> Result<Vec<EpochStart>>` (`Corrupt` unless non-empty, `{1,1}` first, epochs strictly increasing, start seqs non-decreasing, last epoch = identity epoch).
  - `net::convert::{identity_to_pb(&DbIdentity) -> pb::Identity, identity_from_pb(&pb::Identity) -> Result<DbIdentity>, cf_to_pb(&CfEntry) -> pb::CfSpec, cf_from_pb(&pb::CfSpec) -> CfEntry, epochs_to_pb(&[EpochStart]) -> Vec<pb::EpochStart>, epochs_from_pb(&[pb::EpochStart]) -> Vec<EpochStart>, entry_to_pb(LogEntry) -> pb::LogEntry, entry_from_pb(pb::LogEntry) -> LogEntry}`.

- [ ] **Step 1: Add the dependencies**

Replace `Cargo.toml` with:

```toml
[package]
name = "replicated-rocksdb"
version = "0.1.0"
edition = "2024"
rust-version = "1.85"
description = "Single-master, async log-shipping replication for RocksDB"

[dependencies]
rocksdb = { version = "0.25.0", default-features = false, features = ["lz4", "zstd", "bindgen-runtime", "multi-threaded-cf"] }
prost = "0.14.4"
bytes = "1.12"
tokio = { version = "1.53", features = ["rt-multi-thread", "macros", "sync", "time", "net"] }
uuid = { version = "1.27", features = ["v4"] }
blake3 = "1.8"
crc32c = "0.6.8"
thiserror = "2.0"
tracing = "0.1.44"
tonic = { version = "0.14.6", features = ["tls-ring"] }
tonic-prost = "0.14.6"
tonic-types = "0.14.6"
tokio-stream = { version = "0.1.19", features = ["net", "sync"] }
subtle = "2.6.1"
async-trait = "0.1.92"
fail = { version = "0.5.1", optional = true }

[features]
failpoints = ["dep:fail", "fail/failpoints"]

[build-dependencies]
tonic-prost-build = "0.14.6"

[dev-dependencies]
tempfile = "3.27"
rcgen = "0.14.10"
```

(`fail/failpoints` is required: without its own feature, `fail` compiles every failpoint to a no-op. `net` is needed for `tokio::net::TcpListener`.)

- [ ] **Step 2: Generate the gRPC server and client**

Replace `build.rs` with:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    tonic_prost_build::configure()
        .build_server(true)
        .build_client(true)
        .bytes(".rrdb.replication.v1.LogEntry.ops")
        .compile_protos(&["proto/replication.proto", "proto/local.proto"], &["proto"])?;
    Ok(())
}
```

- [ ] **Step 3: Write the failing tests**

Create `src/net/mod.rs` containing only its module declaration and tests:

```rust
//! Wire helpers shared by the replication server and client.

pub(crate) mod convert;

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::AsciiMetadataValue;

    #[test]
    fn every_reason_round_trips_through_a_status() {
        for n in 0..=15 {
            let reason = pb::Reason::try_from(n).unwrap();
            let s = reason_status(reason, "msg");
            assert_eq!(reason_of(&s), Some(reason), "{reason:?}");
            assert_eq!(s.code(), code_for(reason));
            assert_eq!(s.message(), "msg");
        }
    }

    #[test]
    fn codes_follow_the_error_table() {
        use pb::Reason as R;
        assert_eq!(code_for(R::ProtocolMismatch), Code::FailedPrecondition);
        assert_eq!(code_for(R::NotReady), Code::Unavailable);
        assert_eq!(code_for(R::LogGap), Code::OutOfRange);
        assert_eq!(code_for(R::Busy), Code::ResourceExhausted);
        assert_eq!(code_for(R::SessionNotFound), Code::NotFound);
        assert_eq!(code_for(R::UpstreamReset), Code::Unavailable);
        assert_eq!(code_for(R::ShuttingDown), Code::Unavailable);
        assert_eq!(code_for(R::Corrupt), Code::DataLoss);
        assert_eq!(code_for(R::StaleEpoch), Code::FailedPrecondition);
    }

    #[test]
    fn internal_failures_carry_error_info() {
        let s = internal_status("disk on fire");
        assert_eq!(s.code(), Code::Internal);
        assert_eq!(reason_of(&s), Some(pb::Reason::Unspecified));
    }

    #[test]
    fn plain_status_has_no_reason() {
        assert_eq!(reason_of(&Status::unavailable("x")), None);
        let foreign = Status::with_error_details(
            Code::Unavailable,
            "x",
            ErrorDetails::with_error_info("BUSY", "other.domain", HashMap::new()),
        );
        assert_eq!(reason_of(&foreign), None);
    }

    #[test]
    fn metadata_helpers() {
        let mut md = MetadataMap::new();
        assert!(check_protocol_version(&md).is_err());
        md.insert(MD_PROTOCOL_VERSION, AsciiMetadataValue::from(PROTOCOL_VERSION));
        check_protocol_version(&md).unwrap();
        md.insert(MD_PROTOCOL_VERSION, AsciiMetadataValue::from(PROTOCOL_VERSION + 1));
        let err = check_protocol_version(&md).unwrap_err();
        assert_eq!(reason_of(&err), Some(pb::Reason::ProtocolMismatch));

        assert_eq!(bearer_token(&md), None);
        md.insert(MD_AUTHORIZATION, AsciiMetadataValue::from_static("Bearer s3cr.et-_~+/="));
        assert_eq!(bearer_token(&md).unwrap(), b"s3cr.et-_~+/=");
        md.insert(MD_AUTHORIZATION, AsciiMetadataValue::from_static("Basic abc"));
        assert_eq!(bearer_token(&md), None);
        md.insert(MD_NODE_ID, AsciiMetadataValue::from_static("node-b"));
        assert_eq!(node_id(&md).as_deref(), Some("node-b"));
    }
}
```

Create `src/net/convert.rs` containing only its tests:

```rust
#[cfg(test)]
mod tests {
    use prost::Message;

    use super::*;
    use crate::codec::encode_ops;
    use crate::types::Op;

    #[test]
    fn identity_round_trips_and_rejects_short_uuid() {
        let id = DbIdentity { db_uuid: uuid::Uuid::new_v4(), epoch: 7 };
        assert_eq!(identity_from_pb(&identity_to_pb(&id)).unwrap(), id);
        let bad = pb::Identity { db_uuid: vec![1, 2, 3], epoch: 1 };
        assert!(matches!(identity_from_pb(&bad), Err(Error::Corrupt(_))));
    }

    #[test]
    fn cf_specs_and_epochs_round_trip() {
        let e = CfEntry { spec: CfSpec::new("a:b").with_comparator("rev"), create_seq: 9 };
        assert_eq!(cf_from_pb(&cf_to_pb(&e)), e);
        let h = vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 3, start_seq: 70 }];
        assert_eq!(epochs_from_pb(&epochs_to_pb(&h)), h);
    }

    #[test]
    fn epoch_histories_are_validated() {
        let id = |epoch| DbIdentity { db_uuid: uuid::Uuid::nil(), epoch };
        let h = |v: &[(u64, u64)]| v.iter().map(|&(epoch, start_seq)| pb::EpochStart { epoch, start_seq }).collect::<Vec<_>>();
        assert_eq!(checked_history(&id(1), &h(&[(1, 1)])).unwrap(), vec![EpochStart { epoch: 1, start_seq: 1 }]);
        checked_history(&id(3), &h(&[(1, 1), (2, 50), (3, 50)])).unwrap();
        for (identity, bad) in [
            (1, h(&[])),
            (1, h(&[(1, 2)])),
            (2, h(&[(2, 1)])),
            (2, h(&[(1, 1), (2, 10), (2, 20)])),
            (3, h(&[(1, 1), (3, 10), (2, 20)])),
            (2, h(&[(1, 1), (2, 10), (3, 5)])),
            (3, h(&[(1, 1), (2, 10)])),
        ] {
            assert!(matches!(checked_history(&id(identity), &bad), Err(Error::Corrupt(_))), "{bad:?}");
        }
    }

    #[test]
    fn entry_bytes_survive_the_wire_unchanged() {
        let ops = vec![Op::Put { cf: "c".into(), key: vec![], value: vec![0; 3], expire_at_ms: None }];
        let e = LogEntry::new(5, 2, 1_000, encode_ops(&ops));
        let wire = entry_to_pb(e.clone()).encode_to_vec();
        let back = entry_from_pb(pb::LogEntry::decode(wire.as_slice()).unwrap());
        assert_eq!(back, e);
        back.verify_crc().unwrap();
        assert_eq!(back.decode_ops().unwrap(), ops);
    }

    #[test]
    fn empty_and_absent_bytes_both_decode_to_empty() {
        // proto3 does not distinguish an empty `bytes` field from an absent one.
        let rec = pb::Record { key: vec![], envelope: vec![] };
        let back = pb::Record::decode(rec.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back.key, Vec::<u8>::new());
        // `KeyRange.lower` is `optional bytes`: an empty key is distinct from "unbounded".
        let r = pb::KeyRange { cf: "c".into(), lower: Some(vec![]), upper: None };
        let back = pb::KeyRange::decode(r.encode_to_vec().as_slice()).unwrap();
        assert_eq!(back.lower, Some(vec![]));
        assert_eq!(back.upper, None);
    }
}
```

Add to `src/lib.rs` (next to the other module declarations):

```rust
pub(crate) mod net;
```

- [ ] **Step 4: Run them to confirm they fail**

Run: `cargo test --lib net`
Expected: FAIL to compile: `cannot find function reason_status`, `cannot find function identity_to_pb`, `cannot find value MD_PROTOCOL_VERSION`. (The first build also compiles tonic and rustls.)

- [ ] **Step 5: Implement `src/net/mod.rs`**

Replace the module header (everything above the test module) with:

```rust
//! Wire helpers shared by the replication server and client: request metadata keys and the
//! error model (PROTOCOL.md §3.2, §10).

pub(crate) mod convert;

use std::collections::HashMap;

use tonic::metadata::MetadataMap;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

use crate::codec::PROTOCOL_VERSION;
use crate::pb;

pub(crate) const MD_PROTOCOL_VERSION: &str = "x-rrdb-protocol-version";
pub(crate) const MD_NODE_ID: &str = "x-rrdb-node-id";
pub(crate) const MD_AUTHORIZATION: &str = "authorization";
/// `google.rpc.ErrorInfo.domain` of every replication error.
pub(crate) const ERROR_DOMAIN: &str = "rrdb.replication.v1";
/// Extra room on top of the payload size for protobuf framing (PROTOCOL.md §11.4).
pub(crate) const DECODE_SLACK: usize = 64 << 10;
/// Requests are small; the server never accepts larger ones.
pub(crate) const MAX_REQUEST_BYTES: usize = 4 << 20;

/// The gRPC status code that carries `reason` (PROTOCOL.md §10).
pub(crate) fn code_for(reason: pb::Reason) -> Code {
    use pb::Reason as R;
    match reason {
        R::NotReady | R::UpstreamReset | R::ShuttingDown => Code::Unavailable,
        R::LogGap => Code::OutOfRange,
        R::Busy => Code::ResourceExhausted,
        R::SessionNotFound => Code::NotFound,
        R::Corrupt => Code::DataLoss,
        R::Unspecified => Code::Unknown,
        R::NoLocalState
        | R::IdentityChanged
        | R::ProtocolMismatch
        | R::EnvelopeMismatch
        | R::LoopDetected
        | R::Diverged
        | R::EpochAdvanced
        | R::StaleEpoch => Code::FailedPrecondition,
    }
}

/// A failed RPC carrying `ErrorInfo { reason, domain: "rrdb.replication.v1" }`.
pub(crate) fn reason_status(reason: pb::Reason, message: impl Into<String>) -> Status {
    let details = ErrorDetails::with_error_info(reason.as_str_name(), ERROR_DOMAIN, HashMap::new());
    Status::with_error_details(code_for(reason), message, details)
}

/// An unexpected server-side failure: `INTERNAL`, still with `ErrorInfo`
/// (`REASON_UNSPECIFIED`), so every failure carries the error domain.
pub(crate) fn internal_status(message: impl Into<String>) -> Status {
    let details =
        ErrorDetails::with_error_info(pb::Reason::Unspecified.as_str_name(), ERROR_DOMAIN, HashMap::new());
    Status::with_error_details(Code::Internal, message, details)
}

/// The protocol reason carried by `status`, if any.
pub(crate) fn reason_of(status: &Status) -> Option<pb::Reason> {
    let details = status.get_error_details();
    let info = details.error_info()?;
    if info.domain != ERROR_DOMAIN {
        return None;
    }
    pb::Reason::from_str_name(&info.reason)
}

/// Fails with `FAILED_PRECONDITION / PROTOCOL_MISMATCH` unless the call carries our
/// `x-rrdb-protocol-version` (every RPC except Handshake, PROTOCOL.md §3.2).
pub(crate) fn check_protocol_version(md: &MetadataMap) -> Result<(), Status> {
    let sent = md.get(MD_PROTOCOL_VERSION).and_then(|v| v.to_str().ok()).and_then(|v| v.parse::<u32>().ok());
    match sent {
        Some(v) if v == PROTOCOL_VERSION => Ok(()),
        other => Err(reason_status(
            pb::Reason::ProtocolMismatch,
            format!("protocol version {other:?} sent, this server speaks {PROTOCOL_VERSION}"),
        )),
    }
}

/// The bearer token from `authorization: Bearer <token>`, if present.
pub(crate) fn bearer_token(md: &MetadataMap) -> Option<Vec<u8>> {
    let v = md.get(MD_AUTHORIZATION)?.as_bytes();
    v.strip_prefix(b"Bearer ").map(<[u8]>::to_vec)
}

/// The self-asserted node id from `x-rrdb-node-id`.
pub(crate) fn node_id(md: &MetadataMap) -> Option<String> {
    md.get(MD_NODE_ID).and_then(|v| v.to_str().ok()).map(str::to_string)
}
```

- [ ] **Step 6: Implement `src/net/convert.rs`** (above the test module)

```rust
//! Conversions between crate types and their protobuf messages.

use crate::error::{Error, Result};
use crate::pb;
use crate::types::{CfEntry, CfSpec, DbIdentity, EpochStart, LogEntry};

pub(crate) fn identity_to_pb(id: &DbIdentity) -> pb::Identity {
    pb::Identity { db_uuid: id.db_uuid.as_bytes().to_vec(), epoch: id.epoch }
}

/// `Corrupt` if `db_uuid` is not 16 bytes.
pub(crate) fn identity_from_pb(p: &pb::Identity) -> Result<DbIdentity> {
    let db_uuid = uuid::Uuid::from_slice(&p.db_uuid)
        .map_err(|_| Error::corrupt(format!("identity has a {}-byte db_uuid", p.db_uuid.len())))?;
    Ok(DbIdentity { db_uuid, epoch: p.epoch })
}

pub(crate) fn cf_to_pb(e: &CfEntry) -> pb::CfSpec {
    pb::CfSpec { name: e.spec.name.clone(), comparator: e.spec.comparator.clone(), create_seq: e.create_seq }
}

pub(crate) fn cf_from_pb(p: &pb::CfSpec) -> CfEntry {
    CfEntry { spec: CfSpec { name: p.name.clone(), comparator: p.comparator.clone() }, create_seq: p.create_seq }
}

pub(crate) fn epochs_to_pb(h: &[EpochStart]) -> Vec<pb::EpochStart> {
    h.iter().map(|e| pb::EpochStart { epoch: e.epoch, start_seq: e.start_seq }).collect()
}

pub(crate) fn epochs_from_pb(h: &[pb::EpochStart]) -> Vec<EpochStart> {
    h.iter().map(|e| EpochStart { epoch: e.epoch, start_seq: e.start_seq }).collect()
}

/// Validates an epoch history received from upstream (PROTOCOL.md §5.5) for `identity`:
/// non-empty, starting with `{1, 1}`, epochs strictly increasing, start seqs non-decreasing,
/// and ending with the identity's epoch. Anything else is `Corrupt`.
pub(crate) fn checked_history(identity: &DbIdentity, h: &[pb::EpochStart]) -> Result<Vec<EpochStart>> {
    let history = epochs_from_pb(h);
    let bad = |why: &str| Err(Error::corrupt(format!("invalid epoch history {history:?}: {why}")));
    match history.first() {
        None => return bad("empty"),
        Some(first) if (first.epoch, first.start_seq) != (1, 1) => return bad("must start with {1, 1}"),
        Some(_) => {}
    }
    if !history.windows(2).all(|w| w[0].epoch < w[1].epoch && w[0].start_seq <= w[1].start_seq) {
        return bad("epochs must increase and start seqs must not decrease");
    }
    if history.last().map(|e| e.epoch) != Some(identity.epoch) {
        return bad("the last epoch must be the identity's epoch");
    }
    Ok(history)
}

/// `ops` is moved, never re-encoded.
pub(crate) fn entry_to_pb(e: LogEntry) -> pb::LogEntry {
    pb::LogEntry { seq: e.seq, epoch: e.epoch, ts_ms: e.ts_ms, ops: e.ops, crc32c: e.crc32c }
}

pub(crate) fn entry_from_pb(p: pb::LogEntry) -> LogEntry {
    LogEntry { seq: p.seq, epoch: p.epoch, ts_ms: p.ts_ms, ops: p.ops, crc32c: p.crc32c }
}
```

- [ ] **Step 7: Run the tests**

Run: `cargo test --lib net`
Expected: PASS, 10 tests (`net::tests` 5 including `internal_failures_carry_error_info`, `net::convert::tests` 5 including `epoch_histories_are_validated`). Dead-code warnings for helpers first used in Task 5 are expected until then.

Run: `cargo build --features failpoints`
Expected: builds.

- [ ] **Step 8: Commit**

```bash
git add Cargo.toml Cargo.lock build.rs src
git commit -m "feat: grpc codegen, error model and wire conversions for replication

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Store additions for replication and the `LogSource` trait

**Files:**
- Create: `src/store/repl.rs`, `src/source.rs`
- Modify: `src/store/meta.rs`, `src/store/mod.rs`, `src/lib.rs`, `DESIGN.md` (§11), `PROTOCOL.md` (§6.3 notes)

**Interfaces:**
- Consumes: Plan 1 `Store::{position, first_retained_seq, entries_after, cf, db, trim_changelog, read_snapshot}`, `MetaSnapshot::load`, `ReadSnapshot { snap, cfs, seq, now_ms }`, `ReadCtx { db, snap, now_ms }`, `OwnedSnapshot::{db, get_cf, iterator_cf}`, `PhysCf { entry, physical }`.
- Produces:
  - `meta::key::{NODE_STATE = b"node_state", MAX_ENTRY_BYTES = b"max_entry_bytes", BOOTSTRAP_CFS = b"bootstrap_cfs"}`; new `MetaSnapshot` fields `node_state: Option<Vec<u8>>`, `max_entry_bytes: Option<u64>`, `bootstrap_cfs: Vec<CfEntry>`.
  - `Store::open` treats `bootstrap_cfs` entries as known physical CFs.
  - `Store::entry_crc(&self, seq: Seq) -> Result<Option<u32>>` (changelog, or `last_entry_crc` when `seq == last_seq`).
  - `Store::read_entries(&self, from: Seq, max: usize) -> Result<Vec<LogEntry>>` (`Err(Gap)` if trimmed).
  - `ReadSnapshot::meta(&self) -> Result<MetaSnapshot>`, `ReadSnapshot::phys(&self, &str) -> Option<Arc<PhysCf>>`, `ReadSnapshot::raw_ctx(&self) -> ReadCtx<'_>`.
  - `ReadCtx::iter_raw(&self, Arc<PhysCf>) -> Result<RawIter<'a>>`; `RawIter: Iterator<Item = Result<(Box<[u8]>, Box<[u8]>)>>` (raw envelopes, expired included).
  - `pub(crate) trait LogSource: Send + Sync + 'static { fn node_id(&self) -> &str; fn role(&self) -> Role; fn is_ready(&self) -> bool; fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)>; fn store(&self) -> &Store; fn origin_last_seq(&self) -> Seq; fn upstream_chain(&self) -> Vec<String>; fn max_entry_bytes(&self) -> u64; fn heartbeat(&self) -> Duration; fn subscribe(&self) -> watch::Receiver<Seq>; fn resets(&self) -> watch::Receiver<u64>; }`

- [ ] **Step 1: Write the failing tests**

Create `src/store/repl.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::codec::envelope;
    use crate::config::Retention;
    use crate::error::Error;
    use crate::store::meta::{self, key};
    use crate::store::test_support::{apply, open_store};
    use crate::store::Store;
    use crate::types::{CfEntry, CfSpec, Op, Role};

    fn put(k: &[u8], v: &[u8], at: Option<u64>) -> Op {
        Op::Put { cf: "c".into(), key: k.to_vec(), value: v.to_vec(), expire_at_ms: at }
    }

    #[test]
    fn entry_crc_and_read_entries() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        for i in 0..4u8 {
            apply(&store, vec![put(&[i], b"v", None)]).unwrap();
        }
        let all = store.read_entries(1, 100).unwrap();
        assert_eq!(all.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2, 3, 4, 5]);
        assert_eq!(store.read_entries(2, 2).unwrap().iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2, 3]);
        assert!(store.read_entries(6, 10).unwrap().is_empty());
        assert_eq!(store.entry_crc(3).unwrap(), Some(all[2].crc32c));
        assert_eq!(store.entry_crc(0).unwrap(), None);
        assert_eq!(store.entry_crc(6).unwrap(), None);

        let r = Retention { max_age: None, max_entries: Some(2), check_interval: Duration::from_secs(1) };
        store.trim_changelog(0, &r).unwrap();
        assert_eq!(store.entry_crc(3).unwrap(), None, "trimmed");
        assert_eq!(store.entry_crc(5).unwrap(), Some(all[4].crc32c));
        assert!(matches!(store.read_entries(3, 10), Err(Error::Gap { requested: 3, first_retained: 4 })));

        // With the whole log trimmed, the CRC of the last entry is still known.
        let none = Retention { max_age: Some(Duration::from_millis(1)), max_entries: None, check_interval: Duration::from_secs(1) };
        store.trim_changelog(u64::MAX, &none).unwrap();
        assert_eq!(store.first_retained_seq(), 6);
        assert_eq!(store.entry_crc(5).unwrap(), Some(all[4].crc32c));
        assert_eq!(store.entry_crc(4).unwrap(), None);
    }

    #[test]
    fn snapshot_meta_and_raw_iteration_are_pinned() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put(b"b", b"2", Some(5)), put(b"", b"", None), put(b"a", b"1", None)]).unwrap();
        let snap = store.read_snapshot(u64::MAX).unwrap();
        apply(&store, vec![put(b"z", b"later", None)]).unwrap();
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("d") }]).unwrap();

        let m = snap.meta().unwrap();
        assert_eq!(m.last_seq, 2);
        assert_eq!(m.cf_specs, vec![CfEntry { spec: CfSpec::new("c"), create_seq: 1 }]);
        assert_eq!(m.last_entry_crc, store.read_entries(2, 1).unwrap().first().map(|e| e.crc32c));

        let phys = snap.phys("c").unwrap();
        assert!(snap.phys("d").is_none());
        let raw: Vec<(Vec<u8>, Vec<u8>)> = snap
            .raw_ctx()
            .iter_raw(phys)
            .unwrap()
            .map(|r| r.map(|(k, v)| (k.into_vec(), v.into_vec())).unwrap())
            .collect();
        let keys: Vec<&[u8]> = raw.iter().map(|(k, _)| &k[..]).collect();
        assert_eq!(keys, vec![&b""[..], b"a", b"b"], "expired b is included; z is not");
        assert_eq!(&raw[2].1[..], envelope::encode(2, Some(5), b"2").as_slice());
    }

    #[test]
    fn replication_meta_keys_load_and_bootstrap_cfs_are_known_on_open() {
        let dir = tempfile::tempdir().unwrap();
        let leftover = CfEntry { spec: CfSpec::new("x"), create_seq: 7 };
        {
            let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
            assert_eq!((m.node_state, m.max_entry_bytes), (None, None));
            assert!(m.bootstrap_cfs.is_empty());
            store
                .write_meta(
                    &[
                        (key::NODE_STATE, b"bootstrapping"),
                        (key::MAX_ENTRY_BYTES, &(16u64 << 20).to_be_bytes()),
                        (key::BOOTSTRAP_CFS, &meta::encode_cf_list(std::slice::from_ref(&leftover))),
                    ],
                    true,
                )
                .unwrap();
            // A crash right after an in-progress bootstrap created its physical CF.
            store.db().create_cf(leftover.physical_name(), &rocksdb::Options::default()).unwrap();
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(m.node_state.as_deref(), Some(&b"bootstrapping"[..]));
        assert_eq!(m.max_entry_bytes, Some(16 << 20));
        assert_eq!(m.bootstrap_cfs, vec![leftover.clone()]);
        assert!(store.db().cf_handle(&leftover.physical_name()).is_some());
    }
}
```

Add to the `src/store/mod.rs` module list:

```rust
pub(crate) mod repl;
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib store::repl`
Expected: FAIL to compile: `no method named entry_crc`, `no method named read_entries`, `no method named meta`, `cannot find value NODE_STATE in module key`.

- [ ] **Step 3: Add the replication keys to `__meta`**

In `src/store/meta.rs`, in `mod key`, after `pub const CF_CREATE_INTENT: &[u8] = b"cf_create_intent";` add:

```rust
    pub const NODE_STATE: &[u8] = b"node_state";
    pub const MAX_ENTRY_BYTES: &[u8] = b"max_entry_bytes";
    pub const BOOTSTRAP_CFS: &[u8] = b"bootstrap_cfs";
```

In `struct MetaSnapshot`, after `pub cf_create_intent: Option<CfEntry>,` add:

```rust
    /// Slave only: `ready`, `bootstrapping` or `halted:<Reason>`. `None` means ready.
    pub node_state: Option<Vec<u8>>,
    /// Slave only: the master's `max_entry_bytes` as learnt from upstream.
    pub max_entry_bytes: Option<u64>,
    /// Slave only: physical CFs created by a bootstrap that has not finished.
    pub bootstrap_cfs: Vec<CfEntry>,
```

In `MetaSnapshot::load`, after the `cf_create_intent: …` line add:

```rust
            node_state: get(key::NODE_STATE)?,
            max_entry_bytes: get(key::MAX_ENTRY_BYTES)?.map(|v| be64(&v)).transpose()?,
            bootstrap_cfs: get(key::BOOTSTRAP_CFS)?.map(|v| decode_cf_list(&v)).transpose()?.unwrap_or_default(),
```

In `Store::open` (`src/store/mod.rs`), replace:

```rust
            .flat_map(|m| m.cf_specs.iter().chain(&m.pending_drops).chain(&m.cf_create_intent))
```

with:

```rust
            .flat_map(|m| m.cf_specs.iter().chain(&m.pending_drops).chain(&m.cf_create_intent).chain(&m.bootstrap_cfs))
```

- [ ] **Step 4: Implement `src/store/repl.rs`** (above the test module)

```rust
//! Store helpers for the replication server: entry lookups, batched log reads, and raw reads
//! through a snapshot (bootstrap and verify read envelopes, never stripped values).

use std::sync::Arc;

use rocksdb::{DBIteratorWithThreadMode, IteratorMode, DB};

use super::cfs::PhysCf;
use super::meta::{MetaSnapshot, CF_CHANGELOG, CF_META};
use super::read::ReadCtx;
use super::Store;
use crate::error::{Error, Result};
use crate::read::ReadSnapshot;
use crate::types::{LogEntry, Seq};

impl Store {
    /// The `crc32c` of entry `seq`, if known: from the changelog while it is retained, and
    /// for `seq == last_seq` also from `last_entry_crc` (known after a bootstrap or after
    /// retention emptied the log).
    pub(crate) fn entry_crc(&self, seq: Seq) -> Result<Option<u32>> {
        let pos = self.position();
        if seq == 0 || seq > pos.last_seq {
            return Ok(None);
        }
        if seq >= self.first_retained_seq() {
            let cf = self.cf(CF_CHANGELOG);
            if let Some(v) = self.db().get_cf(&cf, seq.to_be_bytes())? {
                return Ok(Some(LogEntry::decode(&v)?.crc32c));
            }
        }
        Ok(if seq == pos.last_seq { pos.last_entry_crc } else { None })
    }

    /// Up to `max` consecutive entries starting at `from`. Fewer if the log ends sooner;
    /// `Err(Gap)` if `from` (or a later entry in the range) has been trimmed.
    pub(crate) fn read_entries(&self, from: Seq, max: usize) -> Result<Vec<LogEntry>> {
        self.entries_after(from.saturating_sub(1))?.take(max).collect()
    }
}

impl ReadSnapshot {
    /// `__meta` as of this snapshot: `last_seq`, identity, epoch history and `cf_specs` are
    /// always consistent with the data read through the same snapshot.
    pub(crate) fn meta(&self) -> Result<MetaSnapshot> {
        let cf = self.snap.db().cf_handle(CF_META).ok_or_else(|| Error::corrupt("__meta is not open"))?;
        MetaSnapshot::load(|k| self.snap.get_cf(&cf, k))?.ok_or_else(|| Error::corrupt("__meta is empty in snapshot"))
    }

    /// The physical CF this snapshot holds for logical CF `name`.
    pub(crate) fn phys(&self, name: &str) -> Option<Arc<PhysCf>> {
        self.cfs.get(name).cloned()
    }

    pub(crate) fn raw_ctx(&self) -> ReadCtx<'_> {
        ReadCtx { db: self.snap.db(), snap: Some(&self.snap), now_ms: self.now_ms }
    }
}

/// `(key, envelope)` pairs of one CF in comparator order, expired values included.
pub(crate) struct RawIter<'a> {
    inner: DBIteratorWithThreadMode<'a, DB>,
    _cf: Arc<PhysCf>,
}

impl Iterator for RawIter<'_> {
    type Item = Result<(Box<[u8]>, Box<[u8]>)>;

    fn next(&mut self) -> Option<Self::Item> {
        self.inner.next().map(|r| r.map_err(Error::from))
    }
}

impl<'a> ReadCtx<'a> {
    /// Through the snapshot when there is one; the iterator cannot outlive it.
    pub(crate) fn iter_raw(&self, cf: Arc<PhysCf>) -> Result<RawIter<'a>> {
        let handle = self
            .db
            .cf_handle(&cf.physical)
            .ok_or_else(|| Error::corrupt(format!("physical column family {} is missing", cf.physical)))?;
        let inner = match self.snap {
            Some(s) => s.iterator_cf(&handle, IteratorMode::Start),
            None => self.db.iterator_cf(&handle, IteratorMode::Start),
        };
        Ok(RawIter { inner, _cf: cf })
    }
}
```

- [ ] **Step 5: Add the `LogSource` trait**

Create `src/source.rs`:

```rust
//! What the replication server needs from the node it runs on. Implemented by the master
//! and by slaves that serve downstream replicas (DESIGN.md §11).

use std::time::Duration;

use tokio::sync::watch;

use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

pub(crate) trait LogSource: Send + Sync + 'static {
    /// This node's configured identifier (`server_id` in the handshake).
    fn node_id(&self) -> &str;
    fn role(&self) -> Role;
    /// False while this node is bootstrapping itself (it has no consistent state).
    fn is_ready(&self) -> bool;
    /// Origin identity and epoch history, read together.
    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)>;
    fn store(&self) -> &Store;
    /// The highest master seq this node knows of. On the master, its `last_seq`.
    fn origin_last_seq(&self) -> Seq;
    /// Node ids between this node and the master, closest first. Empty on the master.
    fn upstream_chain(&self) -> Vec<String>;
    /// The master's `max_entry_bytes`, forwarded down the chain.
    fn max_entry_bytes(&self) -> u64;
    /// Interval of Tail heartbeats this node sends.
    fn heartbeat(&self) -> Duration;
    /// Receives this node's `last_seq` after each commit.
    fn subscribe(&self) -> watch::Receiver<Seq>;
    /// Bumped whenever every served stream must end with `UPSTREAM_RESET` (an epoch bump on
    /// the master, an epoch adoption or a bootstrap on a slave).
    fn resets(&self) -> watch::Receiver<u64>;
}
```

Add to `src/lib.rs`:

```rust
pub(crate) mod source;
```

- [ ] **Step 6: Run the tests**

Run: `cargo test --lib store`
Expected: PASS, including the 3 new tests in `store::repl` and every Plan 1 store test.

- [ ] **Step 7: Update the spec**

In DESIGN.md §11, replace the code block that starts with `trait LogSource: Send + Sync {` (through its closing ```` ``` ````) with:

```rust
pub(crate) trait LogSource: Send + Sync + 'static {
    fn node_id(&self) -> &str;
    fn role(&self) -> Role;
    fn is_ready(&self) -> bool;                                 // false while bootstrapping
    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)>;
    fn store(&self) -> &Store;                                  // log, snapshots, CRCs, guards
    fn origin_last_seq(&self) -> Seq;                           // master: last_seq
    fn upstream_chain(&self) -> Vec<String>;                    // [] on master, closest upstream first
    fn max_entry_bytes(&self) -> u64;                           // master's value, forwarded down the chain
    fn heartbeat(&self) -> Duration;
    fn subscribe(&self) -> watch::Receiver<Seq>;
    fn resets(&self) -> watch::Receiver<u64>;                   // bumped when served streams must end
}
```

In PROTOCOL.md §6.3, after the note that starts with `- **Rule 14 with a replica server.**`, add:

```
- **Rules 9 and 12, and Tail's `prev_crc32c` check.** "The server holds entry `n`" includes the case where `n` is the server's own `last_seq` and the server knows that entry's `crc32c` from its own position, even if its log no longer contains the entry (right after the server bootstrapped, or after retention emptied its log).
```

Run: `cargo test --test proto_sync`
Expected: PASS (the proto block is unchanged).

- [ ] **Step 8: Commit**

```bash
git add src DESIGN.md PROTOCOL.md
git commit -m "feat: store helpers for replication and the LogSource trait

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: The handshake decision table

**Files:**
- Create: `src/server/mod.rs` (module list only, replaced in Task 5), `src/server/handshake.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `codec::{PROTOCOL_VERSION, ENVELOPE_VERSION}`, `types::{fork_after, DbIdentity, EpochStart, Seq}`, `pb::{HandshakeRequest, Decision, Reason}`.
- Produces:
  - `server::handshake::ServerView { server_id: String, is_master: bool, ready: bool, identity: Option<DbIdentity>, epoch_history: Vec<EpochStart>, upstream_chain: Vec<String>, last_seq: Seq, first_retained_seq: Seq, crc_at_client_last: Option<u32> }`.
  - `server::handshake::Verdict { decision: pb::Decision, reason: pb::Reason, detail: String }`.
  - `server::handshake::decide(&ServerView, &pb::HandshakeRequest) -> Verdict` — PROTOCOL.md §6.3 rules 1–14 in order.

- [ ] **Step 1: Write the failing tests** (one per rule)

Create `src/server/handshake.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use pb::Decision::{Bootstrap, Reject, Resume};
    use pb::Reason as R;

    fn uuid() -> uuid::Uuid {
        uuid::Uuid::from_u128(0x1234)
    }

    /// A master at epoch 2 (fork after epoch 1 at seq 101), last_seq 200, log from 50.
    fn master() -> ServerView {
        ServerView {
            server_id: "a".into(),
            is_master: true,
            ready: true,
            identity: Some(DbIdentity { db_uuid: uuid(), epoch: 2 }),
            epoch_history: vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 101 }],
            upstream_chain: vec![],
            last_seq: 200,
            first_retained_seq: 50,
            crc_at_client_last: Some(0xAAAA),
        }
    }

    fn replica() -> ServerView {
        ServerView { server_id: "b".into(), is_master: false, upstream_chain: vec!["a".into()], ..master() }
    }

    /// A client of epoch 2 at seq 150 whose last entry matches.
    fn req() -> pb::HandshakeRequest {
        pb::HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            envelope_version: u32::from(ENVELOPE_VERSION),
            client_id: "c".into(),
            known_identity: Some(pb::Identity { db_uuid: uuid().as_bytes().to_vec(), epoch: 2 }),
            last_seq: 150,
            last_crc32c: Some(0xAAAA),
        }
    }

    fn check(v: &ServerView, r: &pb::HandshakeRequest, decision: pb::Decision, reason: pb::Reason) {
        let got = decide(v, r);
        assert_eq!((got.decision, got.reason), (decision, reason), "detail: {}", got.detail);
    }

    #[test]
    fn rule_01_protocol_mismatch() {
        let mut r = req();
        r.protocol_version += 1;
        r.envelope_version += 1;
        check(&master(), &r, Reject, R::ProtocolMismatch);
    }

    #[test]
    fn rule_02_envelope_mismatch() {
        let mut r = req();
        r.envelope_version += 1;
        r.client_id = "a".into();
        check(&master(), &r, Reject, R::EnvelopeMismatch);
    }

    #[test]
    fn rule_03_loop_detected_for_self_and_chain() {
        let mut r = req();
        r.client_id = "b".into();
        check(&replica(), &r, Reject, R::LoopDetected);
        r.client_id = "a".into();
        let mut v = replica();
        v.ready = false;
        check(&v, &r, Reject, R::LoopDetected);
    }

    #[test]
    fn rule_04_not_ready() {
        let mut v = replica();
        v.ready = false;
        let mut r = req();
        r.known_identity = None;
        check(&v, &r, Reject, R::NotReady);
        let mut v = replica();
        v.identity = None;
        check(&v, &r, Reject, R::NotReady);
    }

    #[test]
    fn rule_05_no_local_state() {
        let mut r = req();
        r.known_identity = None;
        r.last_seq = 0;
        r.last_crc32c = None;
        check(&master(), &r, Bootstrap, R::NoLocalState);
    }

    #[test]
    fn rule_06_identity_changed_uuid() {
        let mut r = req();
        r.known_identity = Some(pb::Identity { db_uuid: uuid::Uuid::from_u128(9).as_bytes().to_vec(), epoch: 5 });
        check(&master(), &r, Bootstrap, R::IdentityChanged);
    }

    #[test]
    fn rule_07_stale_epoch() {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 3;
        r.last_seq = 999;
        check(&master(), &r, Reject, R::StaleEpoch);
    }

    #[test]
    fn rule_08_diverged_at_or_past_fork() {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.last_seq = 101;
        check(&master(), &r, Reject, R::Diverged);
        r.last_seq = 100;
        r.last_crc32c = Some(0xBBBB);
        check(&master(), &r, Bootstrap, R::IdentityChanged);
    }

    #[test]
    fn rule_09_epoch_advanced_when_crc_proves_continuity() {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.last_seq = 100;
        check(&master(), &r, Resume, R::EpochAdvanced);
        check(&replica(), &r, Resume, R::EpochAdvanced);
    }

    #[test]
    fn rule_10_epoch_change_without_proof() {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.last_seq = 30;
        r.last_crc32c = Some(0xAAAA);
        let mut v = master();
        v.crc_at_client_last = None; // entry 30 was trimmed
        check(&v, &r, Bootstrap, R::IdentityChanged);
        r.last_crc32c = None;
        check(&master(), &r, Bootstrap, R::IdentityChanged);
    }

    #[test]
    fn rule_11_client_ahead_of_master() {
        let mut r = req();
        r.last_seq = 201;
        let mut v = master();
        v.crc_at_client_last = None;
        check(&v, &r, Reject, R::Diverged);
    }

    #[test]
    fn rule_12_crc_differs() {
        let mut r = req();
        r.last_crc32c = Some(0xBBBB);
        check(&master(), &r, Reject, R::Diverged);
        check(&replica(), &r, Reject, R::Diverged);
    }

    #[test]
    fn rule_13_log_gap() {
        let mut r = req();
        r.last_seq = 48;
        let mut v = master();
        v.crc_at_client_last = None;
        check(&v, &r, Bootstrap, R::LogGap);
        r.last_seq = 49; // needs 50 = first retained
        check(&v, &r, Resume, R::Unspecified);
    }

    #[test]
    fn rule_14_resume_including_replica_behind_client() {
        check(&master(), &req(), Resume, R::Unspecified);
        let mut r = req();
        r.last_seq = 250; // ahead of a replica that crashed unsynced: allowed
        let mut v = replica();
        v.crc_at_client_last = None;
        check(&v, &r, Resume, R::Unspecified);
        // An empty client log with identity (bootstrapped at seq 0) resumes from 1.
        let mut r = req();
        r.last_seq = 0;
        r.last_crc32c = None;
        let mut v = master();
        v.first_retained_seq = 1;
        v.crc_at_client_last = None;
        check(&v, &r, Resume, R::Unspecified);
    }
}
```

Create `src/server/mod.rs`:

```rust
//! The replication gRPC server.

pub(crate) mod handshake;
```

Add to `src/lib.rs`:

```rust
pub(crate) mod server;
```

`fork_after` is now used: in `src/types.rs`, delete the line `#[allow(dead_code)] // used by the replication server (Plan 2)` above `pub(crate) fn fork_after`.

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib server::handshake`
Expected: FAIL to compile: `cannot find struct ServerView`, `cannot find function decide`.

- [ ] **Step 3: Implement** (above the test module)

```rust
//! The handshake decision table (PROTOCOL.md §6.3). Pure: the server gathers a
//! [`ServerView`] and this module decides, so every rule is unit-tested in isolation.

use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::pb;
use crate::types::{fork_after, DbIdentity, EpochStart, Seq};

/// What the server knows about itself when answering one handshake.
#[derive(Clone, Debug)]
pub(crate) struct ServerView {
    pub server_id: String,
    pub is_master: bool,
    /// False while the server bootstraps itself or before it has any data.
    pub ready: bool,
    pub identity: Option<DbIdentity>,
    pub epoch_history: Vec<EpochStart>,
    pub upstream_chain: Vec<String>,
    pub last_seq: Seq,
    pub first_retained_seq: Seq,
    /// `crc32c` of the server's entry at the client's `last_seq`, if the server holds it.
    pub crc_at_client_last: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Verdict {
    pub decision: pb::Decision,
    pub reason: pb::Reason,
    pub detail: String,
}

fn verdict(decision: pb::Decision, reason: pb::Reason, detail: impl Into<String>) -> Verdict {
    Verdict { decision, reason, detail: detail.into() }
}

/// Evaluates PROTOCOL.md §6.3 rules 1–14 in order; the first match wins.
pub(crate) fn decide(v: &ServerView, r: &pb::HandshakeRequest) -> Verdict {
    use pb::Decision::{Bootstrap, Reject, Resume};
    use pb::Reason as R;

    // 1, 2: exact version match.
    if r.protocol_version != PROTOCOL_VERSION {
        let d = format!("client protocol {} != server protocol {PROTOCOL_VERSION}", r.protocol_version);
        return verdict(Reject, R::ProtocolMismatch, d);
    }
    if r.envelope_version != u32::from(ENVELOPE_VERSION) {
        let d = format!("client envelope {} != server envelope {ENVELOPE_VERSION}", r.envelope_version);
        return verdict(Reject, R::EnvelopeMismatch, d);
    }
    // 3: the client is this server or one of its upstreams.
    if r.client_id == v.server_id || v.upstream_chain.contains(&r.client_id) {
        let d = format!("{} is upstream of {} (chain {:?})", r.client_id, v.server_id, v.upstream_chain);
        return verdict(Reject, R::LoopDetected, d);
    }
    // 4: no consistent state to serve.
    let Some(server_id) = v.identity.filter(|_| v.ready) else {
        return verdict(Reject, R::NotReady, format!("{} is bootstrapping", v.server_id));
    };
    // 5
    let Some(known) = &r.known_identity else {
        return verdict(Bootstrap, R::NoLocalState, "client has no data");
    };
    // 6
    if known.db_uuid.as_slice() != server_id.db_uuid.as_bytes() {
        return verdict(Bootstrap, R::IdentityChanged, "database uuid differs");
    }
    let client_crc_matches = r.last_crc32c.is_some() && r.last_crc32c == v.crc_at_client_last;
    // 7
    if known.epoch > server_id.epoch {
        let d = format!("client epoch {} is newer than server epoch {}", known.epoch, server_id.epoch);
        return verdict(Reject, R::StaleEpoch, d);
    }
    if known.epoch < server_id.epoch {
        // 8: the client holds entries at or past the fork point after its epoch.
        if let Some(fork) = fork_after(&v.epoch_history, known.epoch) {
            if r.last_seq >= fork {
                let d = format!("client last_seq {} is at or past fork point {fork} of epoch {}", r.last_seq, known.epoch);
                return verdict(Reject, R::Diverged, d);
            }
        }
        // 9: continuity proven by the CRC of the client's last entry.
        if client_crc_matches {
            return verdict(Resume, R::EpochAdvanced, format!("epoch {} -> {}", known.epoch, server_id.epoch));
        }
        // 10
        return verdict(Bootstrap, R::IdentityChanged, "continuity across the epoch change cannot be proven");
    }
    // 11: nobody can be ahead of the fsyncing master.
    if v.is_master && r.last_seq > v.last_seq {
        let d = format!("client last_seq {} is ahead of master last_seq {}", r.last_seq, v.last_seq);
        return verdict(Reject, R::Diverged, d);
    }
    // 12
    if r.last_crc32c.is_some() && v.crc_at_client_last.is_some() && !client_crc_matches {
        return verdict(Reject, R::Diverged, format!("entry {} differs", r.last_seq));
    }
    // 13
    if r.last_seq.saturating_add(1) < v.first_retained_seq {
        let d = format!("client needs {} but first retained is {}", r.last_seq + 1, v.first_retained_seq);
        return verdict(Bootstrap, R::LogGap, d);
    }
    // 14
    verdict(Resume, R::Unspecified, "")
}
```

- [ ] **Step 4: Run the tests**

Run: `cargo test --lib server::handshake`
Expected: PASS, 14 tests (`rule_01_protocol_mismatch` … `rule_14_resume_including_replica_behind_client`).

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "feat: handshake decision table with one test per rule

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 4: Authentication, authorisation and serve/TLS configuration

**Files:**
- Create: `src/auth.rs`
- Modify: `src/config.rs`, `src/lib.rs`, `DESIGN.md` (§14, §16), `PROTOCOL.md` (§11.4, §12)

**Interfaces:**
- Consumes: `Error::Config`, `Result`.
- Produces:
  - `pub enum Method { Handshake, Tail, Bootstrap, Verify }`.
  - `pub struct PeerInfo { pub peer_id: String, pub peer_id_verified: bool, pub remote_addr: SocketAddr, pub client_cert: Option<Vec<u8>> }`.
  - `pub enum AuthError { Unauthenticated, PermissionDenied }`.
  - `#[async_trait] pub trait Authenticator: Send + Sync + 'static { async fn authenticate(&self, peer: &PeerInfo, method: Method, token: Option<&[u8]>) -> Result<(), AuthError>; fn uses_tokens(&self) -> bool { false } }`.
  - `pub trait CredentialsProvider: Send + Sync + 'static { fn token(&self) -> Option<Vec<u8>>; }`.
  - `AllowAll`, `TokenAuthenticator::new(impl IntoIterator<Item = impl Into<Vec<u8>>>)`, `TokenCredentials::new(impl Into<Vec<u8>>)`, `NoCredentials`, `MtlsSubjectAllowList::{new, allow(subject, &[Method]), allow_all_methods(subject)}`, `AllowMethods::new(Arc<dyn Authenticator>, &[Method])`.
  - `pub(crate) struct FailureLimiter { new(max_per_minute: u32), is_blocked(IpAddr, Instant) -> bool, record_failure(IpAddr, Instant) }`.
  - `pub(crate) fn subject_common_name(cert_der: &[u8]) -> Option<String>`.
  - In `config`: `TlsIdentity { cert_pem, key_pem }`, `TlsServerConfig { identity, client_ca_pem: Option<Vec<u8>> }` with `pub(crate) fn to_tonic(&self) -> ServerTlsConfig`, `TlsClientConfig { ca_pem, domain_name: Option<String>, identity: Option<TlsIdentity> }` with `pub(crate) fn to_tonic(&self, default_domain: &str) -> ClientTlsConfig`, `ServeLimits { max_concurrent_bootstraps, max_verify_sessions, bootstrap_chunk_bytes, bootstrap_lease_ttl, verify_session_timeout, max_auth_failures_per_minute, max_tail_batch_bytes }` (Default per Global Constraints), `pub const MAX_BOOTSTRAP_CHUNK_BYTES: usize = 16 << 20`, `ServeConfig { bind: IpAddr, port: u16, tls, authenticator: Arc<dyn Authenticator>, allow_plaintext_token: bool, limits: ServeLimits }` with `new(bind, port, authenticator)` and `pub(crate) fn validate(&self) -> Result<()>`.

- [ ] **Step 1: Write the failing tests**

Create `src/auth.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str, verified: bool) -> PeerInfo {
        PeerInfo {
            peer_id: id.into(),
            peer_id_verified: verified,
            remote_addr: "127.0.0.1:9".parse().unwrap(),
            client_cert: None,
        }
    }

    #[tokio::test]
    async fn token_authenticator_accepts_any_configured_secret() {
        let a = TokenAuthenticator::new([b"old-secret".to_vec(), b"new-secret".to_vec()]);
        let p = peer("b", false);
        assert!(a.uses_tokens());
        a.authenticate(&p, Method::Tail, Some(b"old-secret")).await.unwrap();
        a.authenticate(&p, Method::Tail, Some(b"new-secret")).await.unwrap();
        for bad in [Some(&b"new-secre"[..]), Some(b"new-secret!"), Some(b""), None] {
            assert_eq!(a.authenticate(&p, Method::Tail, bad).await, Err(AuthError::Unauthenticated));
        }
    }

    #[tokio::test]
    async fn allow_methods_restricts_after_authenticating() {
        let a = AllowMethods::new(Arc::new(TokenAuthenticator::new(["t"])), &[Method::Handshake, Method::Tail]);
        let p = peer("b", false);
        assert!(a.uses_tokens());
        a.authenticate(&p, Method::Tail, Some(b"t")).await.unwrap();
        assert_eq!(a.authenticate(&p, Method::Bootstrap, Some(b"t")).await, Err(AuthError::PermissionDenied));
        assert_eq!(a.authenticate(&p, Method::Bootstrap, Some(b"x")).await, Err(AuthError::Unauthenticated));
    }

    #[tokio::test]
    async fn mtls_allow_list_needs_a_verified_listed_subject() {
        let a = MtlsSubjectAllowList::new().allow_all_methods("node-b").allow("node-c", &[Method::Handshake]);
        assert!(!a.uses_tokens());
        a.authenticate(&peer("node-b", true), Method::Bootstrap, None).await.unwrap();
        a.authenticate(&peer("node-c", true), Method::Handshake, None).await.unwrap();
        assert_eq!(a.authenticate(&peer("node-c", true), Method::Tail, None).await, Err(AuthError::PermissionDenied));
        assert_eq!(a.authenticate(&peer("node-x", true), Method::Tail, None).await, Err(AuthError::PermissionDenied));
        assert_eq!(a.authenticate(&peer("node-b", false), Method::Tail, None).await, Err(AuthError::Unauthenticated));
    }

    #[tokio::test]
    async fn allow_all_and_credentials() {
        AllowAll.authenticate(&peer("x", false), Method::Verify, None).await.unwrap();
        assert_eq!(TokenCredentials::new("abc").token(), Some(b"abc".to_vec()));
        assert_eq!(NoCredentials.token(), None);
        assert!(!format!("{:?}", TokenCredentials::new("abc")).contains("abc"), "tokens are never printed");
    }

    #[test]
    fn failure_limiter_blocks_after_max_within_a_minute() {
        let l = FailureLimiter::new(3);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let other: IpAddr = "10.0.0.2".parse().unwrap();
        let t0 = Instant::now();
        for i in 0..3 {
            assert!(!l.is_blocked(ip, t0));
            l.record_failure(ip, t0 + Duration::from_secs(i));
        }
        assert!(l.is_blocked(ip, t0 + Duration::from_secs(10)));
        assert!(!l.is_blocked(other, t0 + Duration::from_secs(10)));
        assert!(!l.is_blocked(ip, t0 + Duration::from_secs(61)), "the oldest failure left the window");
        assert!(!l.is_blocked(ip, t0 + Duration::from_secs(120)));
    }

    #[test]
    fn common_name_is_read_from_a_real_certificate() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(rcgen::DnType::OrganizationName, "rrdb");
        params.distinguished_name.push(rcgen::DnType::CommonName, "node-b");
        let cert = params.self_signed(&key).unwrap();
        assert_eq!(subject_common_name(cert.der()).as_deref(), Some("node-b"));
        assert_eq!(subject_common_name(&[0x30, 0x82, 0xFF]), None);
        assert_eq!(subject_common_name(&[]), None);
    }
}
```

Append to `src/config.rs` the serve-config tests:

```rust
#[cfg(test)]
mod serve_tests {
    use super::*;
    use crate::auth::{AllowAll, TokenAuthenticator};

    #[test]
    fn token_auth_on_plaintext_is_refused_unless_allowed() {
        let localhost = std::net::IpAddr::from([127, 0, 0, 1]);
        let mut cfg = ServeConfig::new(localhost, 0, Arc::new(TokenAuthenticator::new(["t"])));
        assert!(matches!(cfg.validate(), Err(Error::Config(m)) if m.contains("without TLS")));
        cfg.allow_plaintext_token = true;
        cfg.validate().unwrap();
        ServeConfig::new(localhost, 0, Arc::new(AllowAll)).validate().unwrap();
    }

    #[test]
    fn limits_are_validated() {
        let mut cfg = ServeConfig::new(std::net::IpAddr::from([127, 0, 0, 1]), 0, Arc::new(AllowAll));
        cfg.limits.bootstrap_chunk_bytes = MAX_BOOTSTRAP_CHUNK_BYTES + 1;
        assert!(cfg.validate().is_err());
        cfg.limits.bootstrap_chunk_bytes = 1;
        cfg.validate().unwrap();
        cfg.limits.max_concurrent_bootstraps = 0;
        assert!(cfg.validate().is_err());
    }
}
```

Add to `src/lib.rs`:

```rust
pub mod auth;
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib -- auth:: config::serve_tests`
Expected: FAIL to compile: `cannot find struct TokenAuthenticator`, `cannot find struct ServeConfig`, `cannot find value MAX_BOOTSTRAP_CHUNK_BYTES`.

- [ ] **Step 3: Implement `src/auth.rs`** (above the test module)

```rust
//! Authentication and authorisation of replication calls (DESIGN.md §14, PROTOCOL.md §3.3).
//!
//! Every call is authenticated on its own; there is no session state. Tokens travel as
//! `authorization: Bearer <token>` and are compared in constant time. Tokens are never logged.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use subtle::ConstantTimeEq;

/// The RPC being authorised. `Bootstrap` and `Verify` expose the whole database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    Handshake,
    Tail,
    Bootstrap,
    Verify,
}

/// Who is calling.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    /// The client certificate's subject CommonName under mutual TLS, else `x-rrdb-node-id`.
    pub peer_id: String,
    /// True only when `peer_id` comes from a client certificate the TLS layer verified.
    pub peer_id_verified: bool,
    pub remote_addr: SocketAddr,
    /// DER of the client certificate, under mutual TLS.
    pub client_cert: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// Missing or invalid credentials (`UNAUTHENTICATED`).
    #[error("unauthenticated")]
    Unauthenticated,
    /// Valid credentials that may not make this call (`PERMISSION_DENIED`).
    #[error("permission denied")]
    PermissionDenied,
}

#[async_trait::async_trait]
pub trait Authenticator: Send + Sync + 'static {
    async fn authenticate(&self, peer: &PeerInfo, method: Method, token: Option<&[u8]>) -> Result<(), AuthError>;

    /// True if this authenticator relies on bearer tokens. A server refuses to start with
    /// such an authenticator on a plaintext listener unless `allow_plaintext_token` is set.
    fn uses_tokens(&self) -> bool {
        false
    }
}

/// Supplies the bearer token a client sends on every call.
pub trait CredentialsProvider: Send + Sync + 'static {
    fn token(&self) -> Option<Vec<u8>>;
}

/// Accepts every call. For tests and trusted loopback setups.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAll;

#[async_trait::async_trait]
impl Authenticator for AllowAll {
    async fn authenticate(&self, _: &PeerInfo, _: Method, _: Option<&[u8]>) -> Result<(), AuthError> {
        Ok(())
    }
}

/// Accepts calls carrying any of the configured secrets. Several secrets can be valid at
/// once so they can be rotated.
pub struct TokenAuthenticator {
    secrets: Vec<Vec<u8>>,
}

impl TokenAuthenticator {
    pub fn new(secrets: impl IntoIterator<Item = impl Into<Vec<u8>>>) -> Self {
        TokenAuthenticator { secrets: secrets.into_iter().map(Into::into).collect() }
    }
}

impl fmt::Debug for TokenAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TokenAuthenticator({} secrets)", self.secrets.len())
    }
}

#[async_trait::async_trait]
impl Authenticator for TokenAuthenticator {
    async fn authenticate(&self, _: &PeerInfo, _: Method, token: Option<&[u8]>) -> Result<(), AuthError> {
        let Some(token) = token else { return Err(AuthError::Unauthenticated) };
        // Compare against every secret without early exit.
        let mut ok = subtle::Choice::from(0);
        for s in &self.secrets {
            ok |= s.as_slice().ct_eq(token);
        }
        if bool::from(ok) { Ok(()) } else { Err(AuthError::Unauthenticated) }
    }

    fn uses_tokens(&self) -> bool {
        true
    }
}

/// Sends a fixed bearer token.
pub struct TokenCredentials {
    secret: Vec<u8>,
}

impl TokenCredentials {
    /// `secret` must consist of RFC 6750 `b64token` characters.
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        TokenCredentials { secret: secret.into() }
    }
}

impl fmt::Debug for TokenCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenCredentials(..)")
    }
}

impl CredentialsProvider for TokenCredentials {
    fn token(&self) -> Option<Vec<u8>> {
        Some(self.secret.clone())
    }
}

/// Sends no credentials.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCredentials;

impl CredentialsProvider for NoCredentials {
    fn token(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Mutual TLS: accepts a call only from a verified client certificate whose subject
/// CommonName is listed, and only for the methods listed for it.
#[derive(Clone, Debug, Default)]
pub struct MtlsSubjectAllowList {
    allowed: HashMap<String, Vec<Method>>,
}

impl MtlsSubjectAllowList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allows `subject` (certificate CommonName) to call `methods`.
    pub fn allow(mut self, subject: impl Into<String>, methods: &[Method]) -> Self {
        self.allowed.insert(subject.into(), methods.to_vec());
        self
    }

    /// Allows `subject` to call every method.
    pub fn allow_all_methods(self, subject: impl Into<String>) -> Self {
        self.allow(subject, &[Method::Handshake, Method::Tail, Method::Bootstrap, Method::Verify])
    }
}

#[async_trait::async_trait]
impl Authenticator for MtlsSubjectAllowList {
    async fn authenticate(&self, peer: &PeerInfo, method: Method, _: Option<&[u8]>) -> Result<(), AuthError> {
        if !peer.peer_id_verified {
            return Err(AuthError::Unauthenticated);
        }
        match self.allowed.get(&peer.peer_id) {
            Some(methods) if methods.contains(&method) => Ok(()),
            _ => Err(AuthError::PermissionDenied),
        }
    }
}

/// Wraps another authenticator and additionally restricts which methods may be called,
/// for example to keep `Bootstrap` and `Verify` away from some credentials.
pub struct AllowMethods {
    inner: Arc<dyn Authenticator>,
    methods: Vec<Method>,
}

impl AllowMethods {
    pub fn new(inner: Arc<dyn Authenticator>, methods: &[Method]) -> Self {
        AllowMethods { inner, methods: methods.to_vec() }
    }
}

#[async_trait::async_trait]
impl Authenticator for AllowMethods {
    async fn authenticate(&self, peer: &PeerInfo, method: Method, token: Option<&[u8]>) -> Result<(), AuthError> {
        self.inner.authenticate(peer, method, token).await?;
        if self.methods.contains(&method) { Ok(()) } else { Err(AuthError::PermissionDenied) }
    }

    fn uses_tokens(&self) -> bool {
        self.inner.uses_tokens()
    }
}

/// Limits failed authentications per remote address: after `max` failures within one
/// minute, calls from that address are refused without checking credentials.
#[derive(Debug)]
pub(crate) struct FailureLimiter {
    max: usize,
    window: Duration,
    failures: Mutex<HashMap<IpAddr, VecDeque<Instant>>>,
}

impl FailureLimiter {
    pub(crate) fn new(max_per_minute: u32) -> Self {
        FailureLimiter {
            max: max_per_minute as usize,
            window: Duration::from_secs(60),
            failures: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn is_blocked(&self, ip: IpAddr, now: Instant) -> bool {
        let mut map = self.failures.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(q) = map.get_mut(&ip) else { return false };
        while q.front().is_some_and(|t| now.saturating_duration_since(*t) >= self.window) {
            q.pop_front();
        }
        if q.is_empty() {
            map.remove(&ip);
            return false;
        }
        q.len() >= self.max
    }

    pub(crate) fn record_failure(&self, ip: IpAddr, now: Instant) {
        self.failures.lock().unwrap_or_else(PoisonError::into_inner).entry(ip).or_default().push_back(now);
    }
}

/// The subject CommonName of a DER-encoded X.509 certificate, if it has one.
/// A minimal DER walk: Certificate → tbsCertificate → subject → first CN attribute.
pub(crate) fn subject_common_name(cert_der: &[u8]) -> Option<String> {
    const SEQUENCE: u8 = 0x30;
    const SET: u8 = 0x31;
    const OID: u8 = 0x06;
    const CONTEXT_0: u8 = 0xA0;
    const CN_OID: &[u8] = &[0x55, 0x04, 0x03]; // 2.5.4.3

    let (tag, cert, _) = der_tlv(cert_der)?;
    if tag != SEQUENCE {
        return None;
    }
    let (tag, tbs, _) = der_tlv(cert)?;
    if tag != SEQUENCE {
        return None;
    }
    // [0] version (optional), serialNumber, signature, issuer, validity, subject
    let (tag, _, after) = der_tlv(tbs)?;
    let mut rest = if tag == CONTEXT_0 { der_tlv(after)?.2 } else { after };
    for _ in 0..3 {
        rest = der_tlv(rest)?.2;
    }
    let (tag, mut subject, _) = der_tlv(rest)?;
    if tag != SEQUENCE {
        return None;
    }
    while !subject.is_empty() {
        let (tag, mut set, r) = der_tlv(subject)?;
        subject = r;
        if tag != SET {
            return None;
        }
        while !set.is_empty() {
            let (tag, atv, r) = der_tlv(set)?;
            set = r;
            if tag != SEQUENCE {
                return None;
            }
            let (tag, oid, value) = der_tlv(atv)?;
            if tag == OID && oid == CN_OID {
                let (_, s, _) = der_tlv(value)?;
                return String::from_utf8(s.to_vec()).ok();
            }
        }
    }
    None
}

/// One DER TLV at the start of `b`: `(tag, contents, rest)`.
fn der_tlv(b: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, b) = b.split_first()?;
    let (&first, b) = b.split_first()?;
    let (len, b) = if first < 0x80 {
        (usize::from(first), b)
    } else {
        let n = usize::from(first & 0x7F);
        if n == 0 || n > 4 || b.len() < n {
            return None;
        }
        (b[..n].iter().fold(0usize, |acc, &x| (acc << 8) | usize::from(x)), &b[n..])
    };
    if b.len() < len {
        return None;
    }
    Some((tag, &b[..len], &b[len..]))
}
```

- [ ] **Step 4: Add the serve and TLS configuration**

In `src/config.rs`, insert above the `serve_tests` module:

```rust
// ---------------- Replication serving and TLS (Plan 2) ----------------

/// PEM-encoded certificate chain and private key.
#[derive(Clone)]
pub struct TlsIdentity {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

impl std::fmt::Debug for TlsIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TlsIdentity(..)")
    }
}

/// TLS for a replication server. With `client_ca_pem`, clients must present a certificate
/// signed by that CA (mutual TLS); its subject CommonName becomes the peer id.
#[derive(Clone, Debug)]
pub struct TlsServerConfig {
    pub identity: TlsIdentity,
    pub client_ca_pem: Option<Vec<u8>>,
}

impl TlsServerConfig {
    pub(crate) fn to_tonic(&self) -> tonic::transport::ServerTlsConfig {
        use tonic::transport::{Certificate, Identity, ServerTlsConfig};
        let mut t = ServerTlsConfig::new()
            .identity(Identity::from_pem(&self.identity.cert_pem, &self.identity.key_pem));
        if let Some(ca) = &self.client_ca_pem {
            t = t.client_ca_root(Certificate::from_pem(ca));
        }
        t
    }
}

/// TLS for connecting to an upstream.
#[derive(Clone, Debug)]
pub struct TlsClientConfig {
    /// PEM of the CA that signed the upstream's certificate.
    pub ca_pem: Vec<u8>,
    /// Name to verify the upstream certificate against; defaults to `UpstreamConfig::server`.
    pub domain_name: Option<String>,
    /// Client certificate for mutual TLS.
    pub identity: Option<TlsIdentity>,
}

impl TlsClientConfig {
    pub(crate) fn to_tonic(&self, default_domain: &str) -> tonic::transport::ClientTlsConfig {
        use tonic::transport::{Certificate, ClientTlsConfig, Identity};
        let mut t = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&self.ca_pem))
            .domain_name(self.domain_name.clone().unwrap_or_else(|| default_domain.to_string()));
        if let Some(id) = &self.identity {
            t = t.identity(Identity::from_pem(&id.cert_pem, &id.key_pem));
        }
        t
    }
}

/// Server-side limits (PROTOCOL.md §12).
#[derive(Clone, Debug)]
pub struct ServeLimits {
    pub max_concurrent_bootstraps: usize,
    pub max_verify_sessions: usize,
    /// Soft size of one `BootstrapRecords` message. At most [`MAX_BOOTSTRAP_CHUNK_BYTES`].
    pub bootstrap_chunk_bytes: usize,
    pub bootstrap_lease_ttl: Duration,
    pub verify_session_timeout: Duration,
    /// Failed authentications per remote address per minute before calls are refused unchecked.
    pub max_auth_failures_per_minute: u32,
    /// Cap on a Tail client's `max_batch_bytes` (PROTOCOL.md §7.5).
    pub max_tail_batch_bytes: usize,
}

/// Upper bound for `bootstrap_chunk_bytes`; clients size their Bootstrap decode limit with it.
pub const MAX_BOOTSTRAP_CHUNK_BYTES: usize = 16 << 20;

impl Default for ServeLimits {
    fn default() -> Self {
        ServeLimits {
            max_concurrent_bootstraps: 2,
            max_verify_sessions: 4,
            bootstrap_chunk_bytes: 1 << 20,
            bootstrap_lease_ttl: Duration::from_secs(15 * 60),
            verify_session_timeout: Duration::from_secs(5 * 60),
            max_auth_failures_per_minute: 10,
            max_tail_batch_bytes: 4 << 20,
        }
    }
}

/// Where and how a node serves replication to downstream replicas.
#[derive(Clone)]
pub struct ServeConfig {
    pub bind: std::net::IpAddr,
    /// 0 picks a free port; read it back with `serve_addr()`.
    pub port: u16,
    pub tls: Option<TlsServerConfig>,
    pub authenticator: Arc<dyn crate::auth::Authenticator>,
    /// Allows token authentication without TLS. Loopback and tests only.
    pub allow_plaintext_token: bool,
    pub limits: ServeLimits,
}

impl ServeConfig {
    pub fn new(bind: std::net::IpAddr, port: u16, authenticator: Arc<dyn crate::auth::Authenticator>) -> Self {
        ServeConfig { bind, port, tls: None, authenticator, allow_plaintext_token: false, limits: ServeLimits::default() }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.tls.is_none() && self.authenticator.uses_tokens() && !self.allow_plaintext_token {
            return Err(Error::Config(
                "token authentication without TLS is refused; configure tls or set allow_plaintext_token".into(),
            ));
        }
        let l = &self.limits;
        if l.max_concurrent_bootstraps == 0
            || l.max_verify_sessions == 0
            || l.max_auth_failures_per_minute == 0
            || l.max_tail_batch_bytes == 0
        {
            return Err(Error::Config("serve limits must be positive".into()));
        }
        if l.bootstrap_chunk_bytes == 0 || l.bootstrap_chunk_bytes > MAX_BOOTSTRAP_CHUNK_BYTES {
            return Err(Error::Config(format!("bootstrap_chunk_bytes must be 1..={MAX_BOOTSTRAP_CHUNK_BYTES}")));
        }
        if l.bootstrap_lease_ttl.is_zero() || l.verify_session_timeout.is_zero() {
            return Err(Error::Config("serve timeouts must be positive".into()));
        }
        Ok(())
    }
}
```

Add the re-exports to `src/lib.rs`, replacing `pub use config::{MasterConfig, ReaperConfig, Retention};` with:

```rust
pub use auth::{
    AllowAll, AllowMethods, AuthError, Authenticator, CredentialsProvider, Method, MtlsSubjectAllowList, NoCredentials,
    PeerInfo, TokenAuthenticator, TokenCredentials,
};
pub use config::{
    MasterConfig, ReaperConfig, Retention, ServeConfig, ServeLimits, TlsClientConfig, TlsIdentity, TlsServerConfig,
    MAX_BOOTSTRAP_CHUNK_BYTES,
};
```

- [ ] **Step 5: Run the tests**

Run: `cargo test --lib -- auth:: config::serve_tests`
Expected: PASS: 6 tests in `auth::tests` (including `common_name_is_read_from_a_real_certificate`, which builds a certificate with rcgen) and 2 in `config::serve_tests`.

- [ ] **Step 6: Update the spec**

In DESIGN.md §14, replace:

```
    pub peer_id: String,              // mTLS certificate subject if present, else x-rrdb-node-id
```

with:

```
    pub peer_id: String,              // mTLS certificate subject CommonName if present, else x-rrdb-node-id
```

and replace:

```
- `AllowAll` (for tests) and `MtlsSubjectAllowList`.
```

with:

```
- `AllowAll` (for tests) and `MtlsSubjectAllowList` (verified client certificate whose subject CommonName is listed, per method).
- `AllowMethods::new(inner, &[Method])` restricts any authenticator to some methods (`PERMISSION_DENIED` otherwise); `NoCredentials` sends no token.
```

and replace:

```
- The server limits the rate of failed authentications per remote address (default: 10/min, then `UNAUTHENTICATED` without checking the token).
```

with:

```
- The server limits the rate of failed authentications per remote address (`ServeLimits::max_auth_failures_per_minute`, default 10; then `UNAUTHENTICATED` without checking the token).
```

and replace the trait block:

```
#[async_trait]
pub trait Authenticator: Send + Sync {
    async fn authenticate(&self, peer: &PeerInfo, method: Method, token: Option<&[u8]>)
        -> Result<(), AuthError>;     // AuthError::{Unauthenticated, PermissionDenied}
}
pub trait CredentialsProvider: Send + Sync {
    fn token(&self) -> Option<Vec<u8>>;
}
```

with:

```
#[async_trait]
pub trait Authenticator: Send + Sync + 'static {
    async fn authenticate(&self, peer: &PeerInfo, method: Method, token: Option<&[u8]>)
        -> Result<(), AuthError>;     // AuthError::{Unauthenticated, PermissionDenied}
    /// True for token-based authenticators: refused on plaintext unless allow_plaintext_token.
    fn uses_tokens(&self) -> bool { false }
}
pub trait CredentialsProvider: Send + Sync + 'static {
    fn token(&self) -> Option<Vec<u8>>;
}
```

In DESIGN.md §17, replace:

```
- The client sets the Tail decode limit to `max_entry_bytes + 64 KiB` from the handshake. Only the master enforces the size limit; downstream nodes accept what arrives.
```

with:

```
- The client sets the Tail decode limit to `max(max_entry_bytes, max_batch_bytes) + 64 KiB` (`max_entry_bytes` from the handshake) and the Bootstrap decode limit to `max(max_entry_bytes, 16 MiB) + 64 KiB`; servers keep `bootstrap_chunk_bytes` at most 16 MiB. Only the master enforces the size limit; downstream nodes accept what arrives.
```

In DESIGN.md §16, replace:

```
    pub limits: ServeLimits,   // { max_concurrent_bootstraps: 2, max_verify_sessions: 4,
                               //   bootstrap_chunk_bytes: 1 MiB, bootstrap_lease_ttl: 15 min,
                               //   verify_session_timeout: 5 min }
```

with:

```
    pub limits: ServeLimits,   // { max_concurrent_bootstraps: 2, max_verify_sessions: 4,
                               //   bootstrap_chunk_bytes: 1 MiB (at most 16 MiB),
                               //   bootstrap_lease_ttl: 15 min, verify_session_timeout: 5 min,
                               //   max_auth_failures_per_minute: 10, max_tail_batch_bytes: 4 MiB }
```

In PROTOCOL.md §11.4, replace:

```
- The client sets its maximum decode size for `Tail` to `max(max_entry_bytes, max_batch_bytes) + 64 KiB`, and for `Bootstrap` to `max(max_entry_bytes, bootstrap_chunk_bytes) + 64 KiB`.
```

with:

```
- The client sets its maximum decode size for `Tail` to `max(max_entry_bytes, max_batch_bytes) + 64 KiB`, and for `Bootstrap` to `max(max_entry_bytes, 16 MiB) + 64 KiB`. The client does not know the server's `bootstrap_chunk_bytes`, so servers MUST keep it at most 16 MiB.
```

In the PROTOCOL.md §12 table, replace:

```
| `bootstrap_chunk_bytes` | server | 1 MiB |
```

with:

```
| `bootstrap_chunk_bytes` | server | 1 MiB (at most 16 MiB) |
```

- [ ] **Step 7: Commit**

```bash
git add src DESIGN.md PROTOCOL.md
git commit -m "feat: authenticators, credentials, failure limiter and serve/TLS config

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Replication server (Handshake, Tail with fetch batching, Bootstrap with leases) and master serving

**Files:**
- Create: `src/failpoint.rs`, `src/server/tail.rs`, `src/server/bootstrap.rs`, `src/server/test_client.rs`, `src/server/tests.rs`, `src/master/source.rs`
- Modify: `src/server/mod.rs` (replace), `src/config.rs` (`MasterConfig.serve`), `src/master/writer.rs`, `src/master/mod.rs`, `src/lib.rs`, `DESIGN.md` (§16), `PROTOCOL.md` (§7.2)

**Interfaces:**
- Consumes: Task 1 `net::*`, `net::convert::*`; Task 2 `LogSource`, `Store::{entry_crc, read_entries}`, `ReadSnapshot::{meta, phys, raw_ctx}`, `ReadCtx::iter_raw`; Task 3 `handshake::{ServerView, decide}`; Task 4 `ServeConfig`, `Authenticator`, `FailureLimiter`, `subject_common_name`; Plan 1 `Store::{guard, read_snapshot, position, first_retained_seq, cfs}`, `RetentionGuard`, `Digest`, `master::writer::Inner`, `Master`.
- Produces:
  - `failpoint::fire(name: &str) -> bool` (constant `false` without feature `failpoints`).
  - `server::start(source: Weak<dyn LogSource>, cfg: ServeConfig) -> Result<ServerHandle>` (async).
  - `server::ServerHandle::{addr(&self) -> SocketAddr, async shutdown(self), abort(self)}`; dropping it signals shutdown.
  - `server::await_sole_owner<T: ?Sized>(&Arc<T>, Duration) -> bool` (async), `server::stopped(&mut watch::Receiver<bool>)` (async), `server::READ_BATCH: usize = 64` (re-exported for tests).
  - `server::ServerState::take_lease(&self, lease_id: &[u8], client_id: &str, from_seq: Seq) -> Option<Lease>` — the Tail producer owns the lease until it has read entries past `S`.
  - `server::tail::{Fetch { min_bytes: usize, max_wait: Duration, max_bytes: usize } (clamp(&pb::TailRequest, heartbeat: Duration, max_tail_batch_bytes: usize) -> Fetch), DEFAULT_MAX_BATCH_BYTES = 1 MiB}`; Tail sends `TailMessage::Batch(EntryBatch)` per PROTOCOL.md §7.5.
  - `MasterConfig.serve: Option<ServeConfig>` (default `None`); `Master::serve_addr(&self) -> Option<SocketAddr>`; `Master::shutdown` now ends streams and waits until the DB is closed (`Err(Timeout)` after 10 s).
  - `master::writer::Inner.reset_tx: watch::Sender<u64>` (bumped by `bump_epoch`), `Inner::identity_and_history(&self) -> (DbIdentity, Vec<EpochStart>)`; `impl LogSource for master::writer::Inner`.
  - Test-only `server::test_client::{Client, Meta { node_id, token, version }, client(addr, node_id), client_with(addr, Meta), hello(client_id), tail_req(&DbIdentity, from_seq) -> pb::TailRequest (real-time), next_batch(&mut Streaming<TailMessage>) -> Vec<LogEntry>, serving_master(dir, tweak) -> (Master, Arc<ManualClock>), eventually(what, f), T0}`.

- [ ] **Step 1: Let the master serve**

In `src/config.rs`, in `struct MasterConfig`, after `pub clock: Arc<dyn Clock>,` add:

```rust
    /// Replication server. `None` runs an embedded, non-replicating master.
    pub serve: Option<ServeConfig>,
```

In `MasterConfig::new`, after `clock: Arc::new(SystemClock),` add `serve: None,`. In `MasterConfig::validate`, just before the final `Ok(())`, add:

```rust
        if let Some(serve) = &self.serve {
            serve.validate()?;
        }
```

In `src/master/writer.rs`, in `struct Inner` after `pub shutdown_tx: watch::Sender<bool>,` add:

```rust
    /// Bumped by `bump_epoch`: every served replication stream ends with `UPSTREAM_RESET`.
    pub reset_tx: watch::Sender<u64>,
```

In `Inner::new`, after `shutdown_tx: watch::channel(false).0,` add `reset_tx: watch::channel(0).0,`. After `fn epoch_history` add:

```rust
    /// Identity and epoch history, read under one lock.
    pub(crate) fn identity_and_history(&self) -> (DbIdentity, Vec<EpochStart>) {
        self.identity.read().unwrap_or_else(PoisonError::into_inner).clone()
    }
```

In `Inner::bump_epoch`, replace:

```rust
        *guard = (id, history);
        tracing::info!(epoch = id.epoch, start_seq = start, "epoch bumped");
```

with:

```rust
        *guard = (id, history);
        drop(guard);
        self.reset_tx.send_modify(|n| *n += 1);
        tracing::info!(epoch = id.epoch, start_seq = start, "epoch bumped");
```

- [ ] **Step 2: Write the failing server tests**

Create `src/server/test_client.rs`:

```rust
//! A bare protocol client for server unit tests.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use crate::auth::AllowAll;
use crate::clock::ManualClock;
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::config::{MasterConfig, ServeConfig};
use crate::master::Master;
use crate::pb;
use crate::net::convert::{entry_from_pb, identity_to_pb};
use crate::pb::replication_client::ReplicationClient;
use crate::pb::tail_message::Kind;
use crate::types::{DbIdentity, LogEntry};

pub(crate) const T0: u64 = 1_700_000_000_000;

#[derive(Clone)]
pub(crate) struct Meta {
    pub node_id: &'static str,
    pub token: Option<&'static str>,
    pub version: Option<u32>,
}

impl Interceptor for Meta {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let md = req.metadata_mut();
        if let Some(v) = self.version {
            md.insert("x-rrdb-protocol-version", AsciiMetadataValue::from(v));
        }
        md.insert("x-rrdb-node-id", AsciiMetadataValue::from_static(self.node_id));
        if let Some(t) = self.token {
            md.insert("authorization", AsciiMetadataValue::try_from(format!("Bearer {t}")).unwrap());
        }
        Ok(req)
    }
}

pub(crate) type Client = ReplicationClient<InterceptedService<Channel, Meta>>;

pub(crate) fn client_with(addr: SocketAddr, meta: Meta) -> Client {
    let channel = Endpoint::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
    ReplicationClient::with_interceptor(channel, meta).max_decoding_message_size(64 << 20)
}

pub(crate) fn client(addr: SocketAddr, node_id: &'static str) -> Client {
    client_with(addr, Meta { node_id, token: None, version: Some(PROTOCOL_VERSION) })
}

/// A real-time Tail request (`min_batch_bytes = 0`, server-default batch size).
pub(crate) fn tail_req(id: &DbIdentity, from_seq: u64) -> pb::TailRequest {
    pb::TailRequest {
        client_id: "b".into(),
        identity: Some(identity_to_pb(id)),
        from_seq,
        prev_crc32c: None,
        bootstrap_lease: None,
        min_batch_bytes: 0,
        max_wait_ms: 0,
        max_batch_bytes: 0,
    }
}

/// Awaits `fut` for at most 10 s, so a regression fails the test instead of hanging it.
pub(crate) async fn within<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), fut).await.unwrap_or_else(|_| panic!("timed out after 10 s: {what}"))
}

/// The next entry batch of a Tail stream, skipping heartbeats.
pub(crate) async fn next_batch(s: &mut tonic::Streaming<pb::TailMessage>) -> Vec<LogEntry> {
    within("the next entry batch", async {
        loop {
            match s.message().await.unwrap().unwrap().kind.unwrap() {
                Kind::Batch(b) => return b.entries.into_iter().map(entry_from_pb).collect(),
                Kind::Heartbeat(_) => {}
            }
        }
    })
    .await
}

/// The status a stream ends with, skipping its remaining messages.
pub(crate) async fn stream_error<T>(s: &mut tonic::Streaming<T>) -> Status {
    within("the stream to end with a status", async {
        loop {
            match s.message().await {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("stream ended without a status"),
                Err(e) => return e,
            }
        }
    })
    .await
}

/// A handshake from a client with no data.
pub(crate) fn hello(client_id: &str) -> pb::HandshakeRequest {
    pb::HandshakeRequest {
        protocol_version: PROTOCOL_VERSION,
        envelope_version: u32::from(ENVELOPE_VERSION),
        client_id: client_id.into(),
        known_identity: None,
        last_seq: 0,
        last_crc32c: None,
    }
}

/// A master serving on 127.0.0.1:0 with `AllowAll`, a manual clock and a 100 ms heartbeat.
pub(crate) async fn serving_master(dir: &Path, tweak: impl FnOnce(&mut MasterConfig)) -> (Master, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(T0));
    let mut cfg = MasterConfig::new("a", dir);
    cfg.clock = clock.clone();
    cfg.heartbeat = Duration::from_millis(100);
    cfg.retention.check_interval = Duration::from_secs(3600);
    cfg.reaper.interval = Duration::from_secs(3600);
    cfg.serve = Some(ServeConfig::new([127, 0, 0, 1].into(), 0, Arc::new(AllowAll)));
    tweak(&mut cfg);
    (Master::open(cfg).await.unwrap(), clock)
}

/// Polls `f` every 10 ms for up to 10 s.
pub(crate) async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
```

Create `src/server/tests.rs`:

```rust
//! Server behaviour against a real master, through a bare protocol client.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::Code;

use super::test_client::{
    client, client_with, eventually, hello, next_batch, serving_master, stream_error, tail_req, within, Meta,
};
use crate::auth::{AllowMethods, Method, TokenAuthenticator};
use crate::codec::digest::Digest;
use crate::codec::{envelope, PROTOCOL_VERSION};
use crate::log::LogApi;
use crate::net::convert::identity_to_pb;
use crate::net::reason_of;
use crate::pb;
use crate::pb::bootstrap_chunk::Kind as Chunk;
use crate::pb::tail_message::Kind as Msg;
use crate::types::{CfSpec, DbIdentity, LogEntry};

/// The next entry of a real-time stream: one entry per batch when entries are written one
/// at a time after the stream caught up.
async fn next_entry(s: &mut tonic::Streaming<pb::TailMessage>) -> LogEntry {
    let mut batch = next_batch(s).await;
    assert_eq!(batch.len(), 1, "real-time batches carry the one entry that became available");
    batch.remove(0)
}

#[tokio::test]
async fn handshake_reports_the_master_state() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let r = c.handshake(hello("b")).await.unwrap().into_inner();
    assert_eq!(r.server_id, "a");
    assert_eq!((r.decision(), r.reason()), (pb::Decision::Bootstrap, pb::Reason::NoLocalState));
    assert_eq!(r.identity, Some(identity_to_pb(&m.identity())));
    assert_eq!(r.epoch_history, vec![pb::EpochStart { epoch: 1, start_seq: 1 }]);
    assert_eq!((r.first_retained_seq, r.last_seq, r.origin_last_seq), (1, 2, 2));
    assert_eq!((r.max_entry_bytes, r.heartbeat_ms), (16 << 20, 100));
    assert!(r.upstream_chain.is_empty());
    assert_eq!(r.cf_specs.len(), 1);

    let mut known = hello("b");
    known.known_identity = Some(identity_to_pb(&m.identity()));
    known.last_seq = 2;
    known.last_crc32c = m.entries_since(1).unwrap().next().unwrap().ok().map(|e| e.crc32c);
    let r = c.handshake(known).await.unwrap().into_inner();
    assert_eq!((r.decision(), r.reason()), (pb::Decision::Resume, pb::Reason::Unspecified));
}

#[tokio::test]
async fn version_mismatch_is_answered_in_the_handshake_body() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    // No protocol metadata at all: Handshake still answers.
    let mut c = client_with(m.serve_addr().unwrap(), Meta { node_id: "b", token: None, version: None });
    let mut req = hello("b");
    req.protocol_version = PROTOCOL_VERSION + 1;
    let r = c.handshake(req).await.unwrap().into_inner();
    assert_eq!((r.decision(), r.reason()), (pb::Decision::Reject, pb::Reason::ProtocolMismatch));
    assert_eq!(r.protocol_version, PROTOCOL_VERSION);
    // Every other RPC fails without the metadata.
    let err = c.tail(tail_req(&m.identity(), 1)).await.unwrap_err();
    assert_eq!((err.code(), reason_of(&err)), (Code::FailedPrecondition, Some(pb::Reason::ProtocolMismatch)));
}

#[tokio::test]
async fn client_id_must_match_the_node_id_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let err = client(m.serve_addr().unwrap(), "b").handshake(hello("someone-else")).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn tail_streams_entries_in_order_then_heartbeats_then_new_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"", b"").unwrap();
    m.put("c", b"\0", b"\0").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let mut s = c.tail(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    // The backlog arrives as one batch.
    let backlog = next_batch(&mut s).await;
    assert_eq!(backlog.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2, 3]);
    backlog.iter().for_each(|e| e.verify_crc().unwrap());
    // Idle: heartbeats only, carrying the server's position.
    match within("a heartbeat", s.message()).await.unwrap().unwrap().kind.unwrap() {
        Msg::Heartbeat(h) => assert_eq!((h.upstream_last_seq, h.origin_last_seq), (3, 3)),
        Msg::Batch(b) => panic!("unexpected batch of {}", b.entries.len()),
    }
    m.put("c", b"k", b"v").unwrap();
    assert_eq!(next_entry(&mut s).await.seq, 4);
}

#[tokio::test]
async fn tail_validation_follows_the_spec() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    eventually("retention", || m.first_retained_seq() == 6).await;
    let id = m.identity();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let reason = |e: tonic::Status| (e.code(), reason_of(&e));

    let other = DbIdentity { db_uuid: uuid::Uuid::new_v4(), epoch: 1 };
    let e = c.tail(tail_req(&other, 6)).await.unwrap_err();
    assert_eq!(reason(e), (Code::FailedPrecondition, Some(pb::Reason::IdentityChanged)));

    let e = c.tail(tail_req(&id, 8)).await.unwrap_err();
    assert_eq!(reason(e), (Code::FailedPrecondition, Some(pb::Reason::Diverged)), "past master last_seq + 1");

    let mut bad_prev = tail_req(&id, 7);
    bad_prev.prev_crc32c = Some(0xDEAD_BEEF);
    let e = c.tail(bad_prev).await.unwrap_err();
    assert_eq!(reason(e), (Code::FailedPrecondition, Some(pb::Reason::Diverged)));

    let e = c.tail(tail_req(&id, 2)).await.unwrap_err();
    assert_eq!(reason(e), (Code::OutOfRange, Some(pb::Reason::LogGap)));

    let e = c.tail(tail_req(&id, 0)).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);

    // from_seq = last_seq + 1 is valid: the stream waits for the next write.
    let mut s = c.tail(tail_req(&id, 7)).await.unwrap().into_inner();
    m.put("c", b"x", b"v").unwrap();
    assert_eq!(next_entry(&mut s).await.seq, 7);
}

#[tokio::test]
async fn bump_epoch_ends_streams_with_upstream_reset() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b").tail(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    assert_eq!(next_entry(&mut s).await.seq, 1);
    m.bump_epoch().unwrap();
    let err = stream_error(&mut s).await;
    assert_eq!((err.code(), reason_of(&err)), (Code::Unavailable, Some(pb::Reason::UpstreamReset)));
}

#[tokio::test]
async fn shutdown_ends_streams_and_releases_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b").tail(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    assert_eq!(next_entry(&mut s).await.seq, 1);
    m.shutdown().await.unwrap();
    let err = stream_error(&mut s).await;
    assert_eq!((err.code(), reason_of(&err)), (Code::Unavailable, Some(pb::Reason::ShuttingDown)));
    // The directory can be reopened immediately.
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    assert_eq!(m.last_seq(), 1);
}

#[tokio::test]
async fn embedded_master_does_not_serve() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.serve = None).await;
    assert!(m.serve_addr().is_none());
}

#[tokio::test]
async fn failed_authentications_are_rate_limited_per_address() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let serve = cfg.serve.as_mut().unwrap();
        serve.authenticator = Arc::new(TokenAuthenticator::new(["good"]));
        serve.allow_plaintext_token = true;
        serve.limits.max_auth_failures_per_minute = 3;
    })
    .await;
    let addr = m.serve_addr().unwrap();
    let meta = |token| Meta { node_id: "b", token: Some(token), version: Some(PROTOCOL_VERSION) };
    client_with(addr, meta("good")).handshake(hello("b")).await.unwrap();
    for _ in 0..3 {
        let e = client_with(addr, meta("bad")).handshake(hello("b")).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        assert_eq!(reason_of(&e), None, "auth failures carry no ErrorInfo");
    }
    let e = client_with(addr, meta("good")).handshake(hello("b")).await.unwrap_err();
    assert_eq!(e.code(), Code::Unauthenticated, "blocked without checking the token");
}

#[tokio::test]
async fn methods_can_be_denied_per_credential() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let serve = cfg.serve.as_mut().unwrap();
        let token = Arc::new(TokenAuthenticator::new(["t"]));
        serve.authenticator = Arc::new(AllowMethods::new(token, &[Method::Handshake, Method::Tail]));
        serve.allow_plaintext_token = true;
    })
    .await;
    let mut c = client_with(m.serve_addr().unwrap(), Meta { node_id: "b", token: Some("t"), version: Some(PROTOCOL_VERSION) });
    c.handshake(hello("b")).await.unwrap();
    let e = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn verify_is_not_implemented_yet() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let e = client(m.serve_addr().unwrap(), "b").verify_begin(pb::VerifyBeginRequest::default()).await.unwrap_err();
    assert_eq!(e.code(), Code::Unimplemented);
}

/// Reads a whole bootstrap stream: (header, records per message, footer).
async fn read_bootstrap(
    c: &mut super::test_client::Client,
) -> (pb::BootstrapHeader, Vec<pb::BootstrapRecords>, pb::BootstrapFooter) {
    within("a whole bootstrap stream", async {
        let mut s = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap().into_inner();
        let Chunk::Header(h) = s.message().await.unwrap().unwrap().kind.unwrap() else { panic!("header first") };
        let mut msgs = Vec::new();
        loop {
            match s.message().await.unwrap().unwrap().kind.unwrap() {
                Chunk::Records(r) => msgs.push(r),
                Chunk::Footer(f) => return (h, msgs, f),
                Chunk::Header(_) => panic!("second header"),
            }
        }
    })
    .await
}

#[tokio::test]
async fn bootstrap_streams_a_consistent_snapshot_with_a_valid_digest() {
    let dir = tempfile::tempdir().unwrap();
    let (m, clock) = serving_master(dir.path(), |cfg| {
        cfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 40;
    })
    .await;
    m.create_cf(CfSpec::new("b-empty")).unwrap();
    m.create_cf(CfSpec::new("a")).unwrap();
    m.put("a", b"", b"").unwrap();
    m.put("a", b"\0", b"\0\0").unwrap();
    m.put_with_ttl("a", b"ttl", b"gone", Duration::from_secs(1)).unwrap();
    for i in 0..10u8 {
        m.put("a", &[b'k', i], &[i; 20]).unwrap();
    }
    clock.advance(Duration::from_secs(5)); // "ttl" is expired but not reaped: still sent
    let mut c = client(m.serve_addr().unwrap(), "b");
    let (h, msgs, f) = read_bootstrap(&mut c).await;

    assert_eq!(h.snapshot_seq, m.last_seq());
    assert_eq!(h.snapshot_crc32c, m.entries_since(m.last_seq() - 1).unwrap().next().unwrap().ok().map(|e| e.crc32c));
    assert_eq!(h.cf_specs.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["a", "b-empty"]);
    assert_eq!(h.identity, Some(identity_to_pb(&m.identity())));
    assert_eq!(h.lease_id.len(), 16);
    assert_eq!(h.lease_ttl_ms, 15 * 60 * 1000);
    assert!(msgs.len() > 1, "small chunk size splits the CF");
    assert!(msgs.iter().all(|r| r.cf == "a" && !r.records.is_empty()));

    let mut d = Digest::new();
    for spec in &h.cf_specs {
        d.cf_marker(&spec.name, spec.create_seq);
        for r in msgs.iter().filter(|r| r.cf == spec.name) {
            for rec in &r.records {
                d.record(&rec.key, &rec.envelope);
            }
        }
    }
    let (count, digest) = d.finish_bootstrap(h.snapshot_seq);
    assert_eq!((f.record_count, f.digest.as_slice()), (13, digest.as_slice()));
    assert_eq!(count, 13);
    let keys: Vec<Vec<u8>> = msgs.iter().flat_map(|r| r.records.iter().map(|x| x.key.clone())).collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]), "strictly ascending");
    let ttl = msgs.iter().flat_map(|r| &r.records).find(|x| x.key == b"ttl").expect("expired value is sent");
    assert_eq!(envelope::decode(&ttl.envelope).unwrap().value, b"gone");
}

#[tokio::test]
async fn bootstrap_lease_holds_retention_until_tail_presents_it() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let (h, _, _) = read_bootstrap(&mut c).await;
    let s = h.snapshot_seq;
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    eventually("retention to trim up to the lease", || m.first_retained_seq() == s + 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await; // five more retention rounds
    assert_eq!(m.first_retained_seq(), s + 1, "the lease keeps S + 1");

    let mut req = tail_req(&m.identity(), s + 1);
    req.bootstrap_lease = Some(h.lease_id.clone());
    let mut stream = c.tail(req).await.unwrap().into_inner();
    assert_eq!(next_batch(&mut stream).await[0].seq, s + 1);
    eventually("retention after the lease is released", || m.first_retained_seq() == m.last_seq()).await;
}

#[tokio::test]
async fn an_unclaimed_lease_expires() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
        cfg.serve.as_mut().unwrap().limits.bootstrap_lease_ttl = Duration::from_millis(1500);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (h, _, _) = read_bootstrap(&mut client(m.serve_addr().unwrap(), "b")).await;
    for i in 0..3u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    // Held by the unclaimed lease first, then released when it expires (1 s sweep).
    eventually("retention to stop at the lease", || m.first_retained_seq() == h.snapshot_seq + 1).await;
    eventually("lease expiry", || m.first_retained_seq() == m.last_seq()).await;
}

#[tokio::test]
async fn concurrent_bootstraps_are_limited() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let limits = &mut cfg.serve.as_mut().unwrap().limits;
        limits.max_concurrent_bootstraps = 1;
        limits.bootstrap_chunk_bytes = 1;
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    // 32 MiB, one record per message: far more than the channel, socket and HTTP/2 buffers
    // hold, so the unread first bootstrap keeps its producer, and its permit, busy.
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    let mut c = client(m.serve_addr().unwrap(), "b");
    let _held = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap().into_inner();
    let err = client(m.serve_addr().unwrap(), "c")
        .bootstrap(pb::BootstrapRequest { client_id: "c".into() })
        .await
        .unwrap_err();
    assert_eq!((err.code(), reason_of(&err)), (Code::ResourceExhausted, Some(pb::Reason::Busy)));
}

/// Clients that stop reading must not keep the server's producers, and so the database,
/// alive: shutdown ends them and the directory can be reopened at once.
#[tokio::test]
async fn shutdown_with_unread_streams_releases_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    // 32 MiB of data: far more than socket and HTTP/2 buffers, so both producers block.
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    let mut c = client(m.serve_addr().unwrap(), "b");
    let mut tail = c.tail(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    assert!(within("the first tail message", tail.message()).await.unwrap().is_some());
    let mut boot = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap().into_inner();
    let first = within("the bootstrap header", boot.message()).await.unwrap().unwrap();
    assert!(matches!(first.kind, Some(Chunk::Header(_))));
    tokio::time::timeout(Duration::from_secs(20), m.shutdown()).await.expect("shutdown hangs").unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    assert_eq!(m.last_seq(), 129);
}

// ---------------- Fetch batching (PROTOCOL.md §7.5) ----------------

fn fetch_req(id: &DbIdentity, from_seq: u64, min_batch_bytes: u32, max_wait_ms: u32, max_batch_bytes: u32) -> pb::TailRequest {
    pb::TailRequest { min_batch_bytes, max_wait_ms, max_batch_bytes, ..tail_req(id, from_seq) }
}

fn batch_bytes(batch: &[LogEntry]) -> usize {
    batch.iter().map(LogEntry::encoded_len).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_time_mode_delivers_each_entry_promptly_one_batch_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_secs(1)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b").tail(tail_req(&m.identity(), 2)).await.unwrap().into_inner();
    for i in 0..5u8 {
        let seq = m.put("c", &[i], b"v").unwrap();
        let t0 = Instant::now(); // after the fsynced put returned
        let batch = next_batch(&mut s).await;
        let latency = t0.elapsed();
        assert_eq!(batch.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![seq], "one entry per batch");
        // Real time: nothing waits for more data (the generous bound only guards against a
        // regression to the 1 s heartbeat or a max_wait; it is not a latency benchmark).
        assert!(latency < Duration::from_millis(900), "entry {seq} took {latency:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_writes_wait_for_max_wait_and_arrive_together() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_secs(1)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let req = fetch_req(&m.identity(), 2, 1 << 20, 300, 0);
    let mut s = client(m.serve_addr().unwrap(), "b").tail(req).await.unwrap().into_inner();
    m.put("c", &[0], b"v").unwrap();
    let t0 = Instant::now(); // entry 2 became available no later than this
    for i in 1..3u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(batch.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2, 3, 4], "one batch");
    assert!(waited >= Duration::from_millis(200), "sent before max_wait (300 ms): {waited:?}");
    assert!(waited < Duration::from_secs(3), "sent long after max_wait: {waited:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reaching_min_batch_bytes_sends_before_max_wait() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_secs(30)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", &[0], &[7; 100]).unwrap();
    let one = m.entries_since(1).unwrap().next().unwrap().unwrap().encoded_len();
    let req = fetch_req(&m.identity(), 3, u32::try_from(3 * one).unwrap(), 20_000, 0);
    let mut s = client(m.serve_addr().unwrap(), "b").tail(req).await.unwrap().into_inner();
    for i in 1..=3u8 {
        m.put("c", &[i], &[7; 100]).unwrap();
    }
    let t0 = Instant::now(); // min_batch_bytes is reached no later than this
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(batch.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
    assert!(batch_bytes(&batch) >= 3 * one);
    assert!(waited < Duration::from_secs(5), "waited {waited:?} (max_wait is 20 s) although min_batch_bytes was reached");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_up_is_split_at_max_batch_bytes_without_gaps() {
    let dir = tempfile::tempdir().unwrap();
    // The server caps the client's 1 MiB request at 64 KiB.
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.serve.as_mut().unwrap().limits.max_tail_batch_bytes = 64 << 10;
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..1000u32 {
        m.put("c", &i.to_be_bytes(), &[1; 1024]).unwrap();
    }
    // min_batch_bytes is huge and max_wait long: catch-up must still not wait.
    let req = fetch_req(&m.identity(), 1, u32::MAX, 100, 1 << 20);
    let mut s = client(m.serve_addr().unwrap(), "b").tail(req).await.unwrap().into_inner();
    let mut batches = Vec::new();
    let mut seqs = Vec::new();
    while seqs.len() < 1001 {
        let b = next_batch(&mut s).await;
        assert!(batch_bytes(&b) <= 64 << 10, "a batch of {} bytes exceeds the cap", batch_bytes(&b));
        seqs.extend(b.iter().map(|e| e.seq));
        batches.push(b);
    }
    assert_eq!(seqs, (1..=1001).collect::<Vec<_>>(), "consecutive, no gaps or duplicates");
    assert!(batches.len() >= 16, "1 MiB of backlog needs at least 16 batches of 64 KiB, got {}", batches.len());
    // Catch-up is not paced by max_wait: with 100 ms per batch it would take over 1.6 s.
    // (Each `next_batch` is bounded at 10 s; the shape assertions above carry the test.)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_larger_than_max_batch_bytes_is_sent_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"small-1", b"v").unwrap();
    let big = m.put("c", b"big", &[9; 4096]).unwrap();
    m.put("c", b"small-2", b"v").unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b").tail(fetch_req(&m.identity(), 2, 0, 0, 1024)).await.unwrap().into_inner();
    let mut got = Vec::new();
    while got.iter().map(Vec::len).sum::<usize>() < 3 {
        got.push(next_batch(&mut s).await);
    }
    let seqs: Vec<Vec<u64>> = got.iter().map(|b| b.iter().map(|e| e.seq).collect()).collect();
    assert_eq!(seqs, vec![vec![2], vec![big], vec![4]]);
    assert!(batch_bytes(&got[1]) > 1024);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_wait_is_clamped_to_the_heartbeat() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_millis(200)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let req = fetch_req(&m.identity(), 2, 1 << 20, 30_000, 0);
    let mut s = client(m.serve_addr().unwrap(), "b").tail(req).await.unwrap().into_inner();
    m.put("c", b"k", b"v").unwrap();
    let t0 = Instant::now();
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(batch.len(), 1);
    assert!(waited >= Duration::from_millis(100), "sent before the clamped max_wait: {waited:?}");
    assert!(waited < Duration::from_secs(5), "max_wait (30 s) was not clamped to the 200 ms heartbeat: {waited:?}");
}
```

Create `src/master/source.rs` with the master's `LogSource` impl test (the impl follows in Step 6):

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::Code;

    use crate::config::Retention;
        use crate::net::reason_of;
    use crate::pb;
    use crate::pb::tail_message::Kind;
    use crate::server::test_client::{client, next_batch, serving_master, tail_req, within};
    use crate::types::CfSpec;

    /// Retention overtakes a reader that stopped consuming: the stream ends with LOG_GAP
    /// once the entries it already read are delivered.
    #[tokio::test]
    async fn slow_reader_gets_log_gap_when_retention_overtakes_it() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        // 50 MiB of log: far more than socket and HTTP/2 buffers, so the producer blocks.
        for i in 0..200u8 {
            m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
        }
        let req = tail_req(&m.identity(), 1);
        let mut s = client(m.serve_addr().unwrap(), "b").tail(req).await.unwrap().into_inner();
        // The producer has read ahead once the first batch arrives.
        let first = next_batch(&mut s).await;
        let mut last = first.last().unwrap().seq;
        assert_eq!(first[0].seq, 1);
        let keep_one = Retention { max_age: None, max_entries: Some(1), check_interval: Duration::from_secs(1) };
        m.inner.store.trim_changelog(0, &keep_one).unwrap();

        let err = within("the slow reader's LOG_GAP", async {
            loop {
                match s.message().await {
                    Ok(Some(pb::TailMessage { kind: Some(Kind::Batch(b)) })) => {
                        for e in b.entries {
                            assert_eq!(e.seq, last + 1);
                            last = e.seq;
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("stream ended without a status"),
                    Err(e) => return e,
                }
            }
        })
        .await;
        // Entries the producer had already read are delivered; the next read hits the gap.
        assert!(last < 200 && last % crate::server::READ_BATCH as u64 == 0, "last delivered: {last}");
        assert_eq!((err.code(), reason_of(&err)), (Code::OutOfRange, Some(pb::Reason::LogGap)));
    }
}
```

and add `mod source;` to the module list at the top of `src/master/mod.rs`.

- [ ] **Step 3: Run them to confirm they fail**

Run: `cargo test --lib -- server:: master::source`
Expected: FAIL to compile: unresolved import `crate::server::test_client` (the file exists but `server/mod.rs` does not declare it until Step 5), `no method named serve_addr found for struct Master`, `cannot find value READ_BATCH in module crate::server`.

- [ ] **Step 4: Add the failpoint shim**

Create `src/failpoint.rs`:

```rust
//! Test-only fault injection (feature `failpoints`). Without the feature every check is a
//! constant `false` and compiles away.
//!
//! Points: `slave-after-apply` (after an entry commits, before the listener),
//! `slave-bootstrap-after-chunk` (after a bootstrap records message is written),
//! `server-tail-after-entry` (the server ends the Tail stream after sending an entry),
//! `master-bump-epoch-write` (the synced write of `bump_epoch` fails).
//!
//! Configure with `fail::cfg(name, "1*return")` to make the point fire once, or
//! `fail::cfg(name, "pause")` to block the thread there until `fail::remove(name)`.

/// True when the named point is configured to fire (`return`). A `pause` action blocks the
/// calling thread until the point is removed, then returns false.
#[cfg(feature = "failpoints")]
pub(crate) fn fire(name: &str) -> bool {
    fail::eval(name, |_| ()).is_some()
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
pub(crate) fn fire(_name: &str) -> bool {
    false
}
```

Add to `src/lib.rs`:

```rust
pub(crate) mod failpoint;
```

- [ ] **Step 5: Implement the server**

Replace `src/server/mod.rs` with:

```rust
//! The replication gRPC server (PROTOCOL.md §6–§8), shared by the master and by slaves that
//! serve downstream replicas. It runs over a [`LogSource`] and holds it weakly, so a running
//! server never keeps a closed node's database open.

pub(crate) mod bootstrap;
pub(crate) mod handshake;
mod tail;
#[cfg(test)]
pub(crate) use tail::READ_BATCH;
#[cfg(test)]
pub(crate) mod test_client;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::auth::{subject_common_name, AuthError, FailureLimiter, Method, PeerInfo};
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::config::ServeConfig;
use crate::error::{Error, Result};
use crate::net::convert::{cf_to_pb, epochs_to_pb, identity_to_pb};
use crate::net::{self, reason_status};
use crate::pb;
use crate::pb::replication_server::{Replication, ReplicationServer};
use crate::source::LogSource;
use crate::store::retention::RetentionGuard;
use crate::types::{Role, Seq};

/// A bootstrap's retention guard, kept after the footer until the client's Tail presents
/// the lease or the lease expires (PROTOCOL.md §8.4).
pub(crate) struct Lease {
    pub client_id: String,
    pub snapshot_seq: Seq,
    pub expires_at: Instant,
    _guard: RetentionGuard,
}

pub(crate) struct ServerState {
    source: Weak<dyn LogSource>,
    pub(crate) cfg: ServeConfig,
    leases: Mutex<HashMap<[u8; 16], Lease>>,
    bootstraps: Arc<Semaphore>,
    pub(crate) shutdown: watch::Receiver<bool>,
    auth_failures: FailureLimiter,
}

impl ServerState {
    fn source(&self) -> Result<Arc<dyn LogSource>, Status> {
        self.source.upgrade().ok_or_else(|| reason_status(pb::Reason::ShuttingDown, "node is shutting down"))
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }

    fn leases(&self) -> std::sync::MutexGuard<'_, HashMap<[u8; 16], Lease>> {
        self.leases.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn insert_lease(&self, id: [u8; 16], client_id: String, snapshot_seq: Seq, guard: RetentionGuard) {
        let expires_at = Instant::now() + self.cfg.limits.bootstrap_lease_ttl;
        self.leases().insert(id, Lease { client_id, snapshot_seq, expires_at, _guard: guard });
    }

    pub(crate) fn remove_lease(&self, id: &[u8; 16]) {
        self.leases().remove(id);
    }

    /// Takes the lease presented by `client_id`'s Tail from `from_seq` (PROTOCOL.md §7.2),
    /// if it matches. The Tail producer keeps it until it has read the entries after the
    /// snapshot, so aggressive retention cannot trim `S + 1` in between.
    pub(crate) fn take_lease(&self, lease_id: &[u8], client_id: &str, from_seq: Seq) -> Option<Lease> {
        let id = <[u8; 16]>::try_from(lease_id).ok()?;
        let mut leases = self.leases();
        let matches = leases.get(&id).is_some_and(|l| l.client_id == client_id && from_seq <= l.snapshot_seq + 1);
        if matches { leases.remove(&id) } else { None }
    }

    pub(crate) fn expire_leases(&self, now: Instant) {
        self.leases().retain(|_, l| l.expires_at > now);
    }

    /// Authenticates one call (PROTOCOL.md §3.3) and returns the node it may read.
    async fn authorize(&self, call: &CallInfo, method: Method) -> Result<Arc<dyn LogSource>, Status> {
        let ip = call.peer.remote_addr.ip();
        if self.auth_failures.is_blocked(ip, Instant::now()) {
            return Err(Status::unauthenticated("too many failed authentication attempts"));
        }
        match self.cfg.authenticator.authenticate(&call.peer, method, call.token.as_deref()).await {
            Ok(()) => self.source(),
            Err(AuthError::Unauthenticated) => {
                self.auth_failures.record_failure(ip, Instant::now());
                Err(Status::unauthenticated("authentication failed"))
            }
            Err(AuthError::PermissionDenied) => Err(Status::permission_denied(format!("{method:?} is not allowed"))),
        }
    }
}

/// What a handler extracts from the request before any `.await`.
pub(crate) struct CallInfo {
    pub peer: PeerInfo,
    pub token: Option<Vec<u8>>,
    /// `x-rrdb-node-id`.
    pub node_id: Option<String>,
}

/// Checks the protocol version metadata (except for Handshake) and collects peer info.
pub(crate) fn call_info<T>(req: &Request<T>, method: Method) -> Result<CallInfo, Status> {
    if method != Method::Handshake {
        net::check_protocol_version(req.metadata())?;
    }
    let node_id = net::node_id(req.metadata());
    let remote_addr = req.remote_addr().unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
    let client_cert = req.peer_certs().and_then(|certs| certs.first().map(|c| c.as_ref().to_vec()));
    let peer = match client_cert.as_deref().and_then(subject_common_name) {
        Some(cn) => PeerInfo { peer_id: cn, peer_id_verified: true, remote_addr, client_cert },
        None => PeerInfo { peer_id: node_id.clone().unwrap_or_default(), peer_id_verified: false, remote_addr, client_cert },
    };
    Ok(CallInfo { peer, token: net::bearer_token(req.metadata()), node_id })
}

/// Maps a store error to a status. Corruption and gaps keep their protocol reasons.
pub(crate) fn store_status(e: Error) -> Status {
    match e {
        Error::Corrupt(m) => reason_status(pb::Reason::Corrupt, m),
        e @ Error::Gap { .. } => reason_status(pb::Reason::LogGap, e.to_string()),
        other => net::internal_status(other.to_string()),
    }
}

struct Svc(Arc<ServerState>);

pub(crate) type TailStream = ReceiverStream<Result<pb::TailMessage, Status>>;
pub(crate) type BootstrapStream = ReceiverStream<Result<pb::BootstrapChunk, Status>>;
pub(crate) type VerifyRangeStream = ReceiverStream<Result<pb::VerifyRangeResponse, Status>>;

#[tonic::async_trait]
impl Replication for Svc {
    async fn handshake(&self, req: Request<pb::HandshakeRequest>) -> Result<Response<pb::HandshakeResponse>, Status> {
        let call = call_info(&req, Method::Handshake)?;
        let src = self.0.authorize(&call, Method::Handshake).await?;
        let r = req.into_inner();
        if call.node_id.as_deref() != Some(r.client_id.as_str()) {
            return Err(Status::invalid_argument("client_id must equal the x-rrdb-node-id metadata"));
        }
        let view = server_view(&*src, r.last_seq)?;
        let v = handshake::decide(&view, &r);
        tracing::info!(
            server = %view.server_id, client = %r.client_id, last_seq = r.last_seq,
            decision = v.decision.as_str_name(), reason = v.reason.as_str_name(), detail = %v.detail,
            "handshake"
        );
        let identity = view.identity.filter(|_| v.reason != pb::Reason::NotReady);
        Ok(Response::new(pb::HandshakeResponse {
            protocol_version: PROTOCOL_VERSION,
            envelope_version: u32::from(ENVELOPE_VERSION),
            server_id: view.server_id,
            decision: v.decision as i32,
            reason: v.reason as i32,
            detail: v.detail,
            upstream_chain: view.upstream_chain,
            identity: identity.as_ref().map(identity_to_pb),
            cf_specs: src.store().cfs.live_entries().iter().map(cf_to_pb).collect(),
            first_retained_seq: view.first_retained_seq,
            last_seq: view.last_seq,
            origin_last_seq: src.origin_last_seq(),
            max_entry_bytes: src.max_entry_bytes(),
            heartbeat_ms: u32::try_from(src.heartbeat().as_millis()).unwrap_or(u32::MAX),
            epoch_history: epochs_to_pb(&view.epoch_history),
        }))
    }

    type TailStream = TailStream;

    async fn tail(&self, req: Request<pb::TailRequest>) -> Result<Response<TailStream>, Status> {
        tail::tail(&self.0, req).await
    }

    type BootstrapStream = BootstrapStream;

    async fn bootstrap(&self, req: Request<pb::BootstrapRequest>) -> Result<Response<BootstrapStream>, Status> {
        bootstrap::bootstrap(&self.0, req).await
    }

    async fn verify_begin(&self, _: Request<pb::VerifyBeginRequest>) -> Result<Response<pb::VerifyBeginResponse>, Status> {
        Err(Status::unimplemented("Verify is not implemented by this version"))
    }

    type VerifyRangeStream = VerifyRangeStream;

    async fn verify_range(
        &self,
        _: Request<tonic::Streaming<pb::VerifyRangeRequest>>,
    ) -> Result<Response<VerifyRangeStream>, Status> {
        Err(Status::unimplemented("Verify is not implemented by this version"))
    }

    async fn verify_end(&self, _: Request<pb::VerifyEndRequest>) -> Result<Response<pb::VerifyEndResponse>, Status> {
        Err(Status::unimplemented("Verify is not implemented by this version"))
    }
}

fn server_view(src: &dyn LogSource, client_last_seq: Seq) -> Result<handshake::ServerView, Status> {
    let ident = src.identity();
    let store = src.store();
    Ok(handshake::ServerView {
        server_id: src.node_id().to_string(),
        is_master: src.role() == Role::Master,
        ready: src.is_ready(),
        identity: ident.as_ref().map(|(id, _)| *id),
        epoch_history: ident.map(|(_, h)| h).unwrap_or_default(),
        upstream_chain: src.upstream_chain(),
        last_seq: store.position().last_seq,
        first_retained_seq: store.first_retained_seq(),
        crc_at_client_last: store.entry_crc(client_last_seq).map_err(store_status)?,
    })
}

/// A running server. Dropping it signals shutdown without waiting.
pub(crate) struct ServerHandle {
    addr: SocketAddr,
    state: Arc<ServerState>,
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<std::result::Result<(), tonic::transport::Error>>>,
    sweep: Option<JoinHandle<()>>,
}

impl ServerHandle {
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// Ends every stream with `SHUTTING_DOWN`, stops accepting calls, and waits (up to 5 s
    /// each) until every bootstrap job has returned its permit (and dropped its snapshot)
    /// and the server task has finished.
    pub(crate) async fn shutdown(mut self) {
        self.shutdown_tx.send_replace(true);
        let all = u32::try_from(self.state.cfg.limits.max_concurrent_bootstraps).unwrap_or(u32::MAX);
        if tokio::time::timeout(Duration::from_secs(5), self.state.bootstraps.acquire_many(all)).await.is_err() {
            tracing::warn!(addr = %self.addr, "bootstrap jobs did not stop in 5 s");
        }
        self.state.leases().clear();
        if let Some(sweep) = self.sweep.take() {
            let _ = sweep.await;
        }
        if let Some(mut task) = self.task.take() {
            if tokio::time::timeout(Duration::from_secs(5), &mut task).await.is_err() {
                tracing::warn!(addr = %self.addr, "replication server did not stop in 5 s; aborting it");
                task.abort();
            }
        }
    }

    /// Stops immediately, without waiting for anything (emulates a crash).
    #[cfg_attr(not(feature = "failpoints"), allow(dead_code))]
    pub(crate) fn abort(mut self) {
        self.shutdown_tx.send_replace(true);
        self.state.leases().clear();
        if let Some(t) = self.sweep.take() {
            t.abort();
        }
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown_tx.send_replace(true);
    }
}

/// Binds `cfg.bind:cfg.port` (0 picks a free port) and serves `source`.
pub(crate) async fn start(source: Weak<dyn LogSource>, cfg: ServeConfig) -> Result<ServerHandle> {
    cfg.validate()?;
    let listener = TcpListener::bind((cfg.bind, cfg.port)).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state = Arc::new(ServerState {
        source,
        bootstraps: Arc::new(Semaphore::new(cfg.limits.max_concurrent_bootstraps)),
        auth_failures: FailureLimiter::new(cfg.limits.max_auth_failures_per_minute),
        leases: Mutex::new(HashMap::new()),
        shutdown: shutdown_rx.clone(),
        cfg,
    });
    let mut builder = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));
    if let Some(tls) = &state.cfg.tls {
        builder = builder.tls_config(tls.to_tonic()).map_err(|e| Error::Config(format!("server TLS: {e}")))?;
    }
    let service = ReplicationServer::new(Svc(state.clone())).max_decoding_message_size(net::MAX_REQUEST_BYTES);
    let mut signal = shutdown_rx.clone();
    let task = tokio::spawn(builder.add_service(service).serve_with_incoming_shutdown(
        TcpListenerStream::new(listener),
        async move { stopped(&mut signal).await },
    ));
    let sweep = tokio::spawn(sweep_leases(state.clone(), shutdown_rx));
    tracing::info!(%addr, "replication server listening");
    Ok(ServerHandle { addr, state, shutdown_tx, task: Some(task), sweep: Some(sweep) })
}

async fn sweep_leases(state: Arc<ServerState>, mut shutdown: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => state.expire_leases(Instant::now()),
            r = shutdown.changed() => if r.is_err() || *shutdown.borrow() { return },
        }
    }
}

/// Ends a stream with `status`. If the channel is full it waits for room, so a slow client
/// still learns why (for example `LOG_GAP`), but gives up on shutdown or reset.
pub(crate) async fn end_stream<T>(
    tx: &mpsc::Sender<Result<T, Status>>,
    status: Status,
    shutdown: &mut watch::Receiver<bool>,
    resets: &mut watch::Receiver<u64>,
) {
    if let Err(mpsc::error::TrySendError::Full(msg)) = tx.try_send(Err(status)) {
        tokio::select! {
            biased;
            _ = tx.send(msg) => {}
            _ = stopped(shutdown) => {}
            _ = resets.changed() => {}
        }
    }
}

/// Starts serving `source` if `serve` is configured.
pub(crate) async fn start_for<S: LogSource>(source: &Arc<S>, serve: Option<ServeConfig>) -> Result<Option<ServerHandle>> {
    match serve {
        Some(cfg) => {
            let weak: Weak<dyn LogSource> = Arc::downgrade(source) as Weak<dyn LogSource>;
            Ok(Some(start(weak, cfg).await?))
        }
        None => Ok(None),
    }
}

/// Resolves once `rx` holds `true` (or its sender is gone).
pub(crate) async fn stopped(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

/// Waits until `arc` is the only strong reference left (background tasks that held a clone
/// have finished), so the database it owns closes when the caller drops it.
pub(crate) async fn await_sole_owner<T: ?Sized>(arc: &Arc<T>, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while Arc::strong_count(arc) > 1 {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}
```

Create `src/server/tail.rs`:

```rust
//! Tail: validation (PROTOCOL.md §7.2) and the per-stream producer (§7.3).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::{call_info, store_status, Lease, ServerState, TailStream};
use crate::auth::Method;
use crate::clock::{Clock, SystemClock};
use crate::error::Error;
use crate::failpoint;
use crate::net::convert::{entry_to_pb, identity_from_pb};
use crate::net::{internal_status, reason_status};
use crate::pb;
use crate::pb::tail_message::Kind;
use crate::source::LogSource;
use crate::types::{LogEntry, Role, Seq};

/// Entries read from the changelog per blocking read.
pub(crate) const READ_BATCH: usize = 64;
/// Messages buffered per stream before back-pressure applies.
const CHANNEL: usize = 16;

pub(super) async fn tail(state: &Arc<ServerState>, req: Request<pb::TailRequest>) -> Result<Response<TailStream>, Status> {
    let call = call_info(&req, Method::Tail)?;
    let src = state.authorize(&call, Method::Tail).await?;
    let r = req.into_inner();
    validate(&*src, &r)?;
    state.expire_leases(std::time::Instant::now());
    let lease = r.bootstrap_lease.as_deref().and_then(|id| state.take_lease(id, &r.client_id, r.from_seq));
    let (tx, rx) = mpsc::channel(CHANNEL);
    let fetch = Fetch::clamp(&r, src.heartbeat(), state.cfg.limits.max_tail_batch_bytes);
    tokio::spawn(produce(src, tx, r.from_seq, fetch, lease, state.shutdown.clone()));
    Ok(Response::new(ReceiverStream::new(rx)))
}

fn validate(src: &dyn LogSource, r: &pb::TailRequest) -> Result<(), Status> {
    let ident = src.identity().filter(|_| src.is_ready());
    let Some((server_id, _)) = ident else {
        return Err(reason_status(pb::Reason::NotReady, "server is bootstrapping"));
    };
    let want = r.identity.as_ref().ok_or_else(|| Status::invalid_argument("identity is required"))?;
    let want = identity_from_pb(want).map_err(|e| Status::invalid_argument(e.to_string()))?;
    if want != server_id {
        return Err(reason_status(pb::Reason::IdentityChanged, format!("server identity is {server_id:?}")));
    }
    if r.from_seq == 0 {
        return Err(Status::invalid_argument("from_seq starts at 1"));
    }
    let store = src.store();
    let last = store.position().last_seq;
    if src.role() == Role::Master && r.from_seq > last + 1 {
        return Err(reason_status(pb::Reason::Diverged, format!("from_seq {} is past master last_seq {last}", r.from_seq)));
    }
    if let Some(prev) = r.prev_crc32c {
        if let Some(have) = store.entry_crc(r.from_seq - 1).map_err(store_status)? {
            if have != prev {
                return Err(reason_status(pb::Reason::Diverged, format!("entry {} differs", r.from_seq - 1)));
            }
        }
    }
    let first = store.first_retained_seq();
    if r.from_seq < first {
        return Err(reason_status(pb::Reason::LogGap, format!("from_seq {} < first retained {first}", r.from_seq)));
    }
    Ok(())
}

fn heartbeat(src: &dyn LogSource) -> pb::TailMessage {
    pb::TailMessage {
        kind: Some(Kind::Heartbeat(pb::Heartbeat {
            upstream_last_seq: src.store().position().last_seq,
            origin_last_seq: src.origin_last_seq(),
            ts_ms: SystemClock.now_ms(),
        })),
    }
}

type Tx = mpsc::Sender<Result<pb::TailMessage, Status>>;

/// The sending side of one stream. Every send also watches shutdown and resets, so a
/// client that stops reading can never keep the producer (and the node's database) alive.
struct Out {
    tx: Tx,
    shutdown: watch::Receiver<bool>,
    resets: watch::Receiver<u64>,
}

enum Sent {
    Ok,
    /// The client went away.
    Gone,
    /// The stream must end with this status.
    End(Status),
}

impl Out {
    async fn send(&mut self, msg: pb::TailMessage) -> Sent {
        tokio::select! {
            r = self.tx.send(Ok(msg)) => if r.is_ok() { Sent::Ok } else { Sent::Gone },
            _ = super::stopped(&mut self.shutdown) => Sent::End(shutting_down()),
            _ = self.resets.changed() => Sent::End(upstream_reset()),
        }
    }

    async fn end(&mut self, status: Status) {
        super::end_stream(&self.tx, status, &mut self.shutdown, &mut self.resets).await;
    }
}

fn shutting_down() -> Status {
    reason_status(pb::Reason::ShuttingDown, "server is shutting down")
}

fn upstream_reset() -> Status {
    reason_status(pb::Reason::UpstreamReset, "server state was replaced")
}

/// Fetch batching parameters of one stream, clamped to the server's limits (PROTOCOL.md §7.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Fetch {
    pub min_bytes: usize,
    pub max_wait: Duration,
    pub max_bytes: usize,
}

/// `max_batch_bytes = 0` means this.
pub(crate) const DEFAULT_MAX_BATCH_BYTES: usize = 1 << 20;

impl Fetch {
    pub(crate) fn clamp(r: &pb::TailRequest, heartbeat: Duration, max_tail_batch_bytes: usize) -> Fetch {
        let max_bytes = match r.max_batch_bytes {
            0 => DEFAULT_MAX_BATCH_BYTES,
            n => n as usize,
        };
        Fetch {
            min_bytes: r.min_batch_bytes as usize,
            max_wait: Duration::from_millis(u64::from(r.max_wait_ms)).min(heartbeat),
            max_bytes: max_bytes.min(max_tail_batch_bytes).max(1),
        }
    }
}

/// Entries read but not yet sent.
struct Pending {
    entries: Vec<pb::LogEntry>,
    bytes: usize,
    /// When the oldest pending entry became available to this stream.
    since: Option<Instant>,
}

impl Pending {
    fn take(&mut self) -> pb::TailMessage {
        self.bytes = 0;
        self.since = None;
        pb::TailMessage { kind: Some(Kind::Batch(pb::EntryBatch { entries: std::mem::take(&mut self.entries) })) }
    }
}

/// Streams entries from `next` in order, in batches (PROTOCOL.md §7.5), with heartbeats,
/// until the client goes away or the stream must end (reset, shutdown, gap, corruption).
async fn produce(
    src: Arc<dyn LogSource>,
    tx: Tx,
    mut next: Seq,
    fetch: Fetch,
    mut lease: Option<Lease>,
    shutdown: watch::Receiver<bool>,
) {
    let hb: Duration = src.heartbeat();
    let mut seqs = src.subscribe();
    let mut resets = src.resets();
    resets.borrow_and_update();
    let mut out = Out { tx, shutdown, resets };
    let mut ahead: VecDeque<LogEntry> = VecDeque::new();
    let mut pending = Pending { entries: Vec::new(), bytes: 0, since: None };
    // The first heartbeat goes out at once, so the client learns positions immediately.
    let mut next_hb = Instant::now();
    macro_rules! send {
        ($msg:expr) => {
            match out.send($msg).await {
                Sent::Ok => {}
                Sent::Gone => return,
                Sent::End(status) => return out.end(status).await,
            }
        };
    }
    macro_rules! flush {
        () => {{
            send!(pending.take());
            if Instant::now() >= next_hb {
                send!(heartbeat(&*src));
                next_hb = Instant::now() + hb;
            }
        }};
    }
    loop {
        if out.resets.has_changed().unwrap_or(true) {
            return out.end(upstream_reset()).await;
        }
        if *out.shutdown.borrow() {
            return out.end(shutting_down()).await;
        }
        // Read ahead everything that is available (in blocks of READ_BATCH).
        if ahead.is_empty() && next <= src.store().position().last_seq {
            let s = src.clone();
            let read = tokio::task::spawn_blocking(move || s.store().read_entries(next, READ_BATCH)).await;
            let failure = match read {
                Ok(Ok(entries)) => {
                    if !entries.is_empty() && lease.take().is_some() {
                        tracing::debug!("bootstrap lease released: the entries after the snapshot are read");
                    }
                    ahead.extend(entries);
                    None
                }
                Ok(Err(e @ Error::Gap { .. })) => Some(reason_status(pb::Reason::LogGap, e.to_string())),
                Ok(Err(e)) => Some(store_status(e)),
                Err(e) => Some(internal_status(format!("log read failed: {e}"))),
            };
            if let Some(status) = failure {
                // Entries already read are valid: deliver them before ending the stream.
                if !pending.entries.is_empty() {
                    send!(pending.take());
                }
                return out.end(status).await;
            }
        }
        // Move available entries into the pending batch; flush when it is full or big enough.
        if let Some(entry) = ahead.pop_front() {
            if let Err(e) = entry.verify_crc() {
                return out.end(store_status(e)).await;
            }
            let size = entry.encoded_len();
            if !pending.entries.is_empty() && pending.bytes + size > fetch.max_bytes {
                flush!();
            }
            next = entry.seq + 1;
            pending.since.get_or_insert_with(Instant::now);
            pending.bytes += size;
            pending.entries.push(entry_to_pb(entry));
            if failpoint::fire("server-tail-after-entry") {
                send!(pending.take());
                return out.end(Status::unavailable("failpoint server-tail-after-entry")).await;
            }
            if pending.bytes >= fetch.max_bytes || (fetch.min_bytes > 0 && pending.bytes >= fetch.min_bytes) {
                flush!();
            }
            continue;
        }
        // Everything available is pending: real-time mode sends it now, otherwise it waits
        // for min_batch_bytes or until the oldest pending entry has waited max_wait.
        let deadline = pending.since.map(|t| t + fetch.max_wait);
        if !pending.entries.is_empty() && (fetch.min_bytes == 0 || deadline.is_some_and(|d| Instant::now() >= d)) {
            flush!();
            continue;
        }
        let wake = match deadline {
            Some(d) if d < next_hb => d,
            _ => next_hb,
        };
        tokio::select! {
            r = seqs.changed() => if r.is_err() { return },
            _ = out.resets.changed() => return out.end(upstream_reset()).await,
            r = out.shutdown.changed() => if r.is_err() { return },
            _ = out.tx.closed() => return,
            _ = tokio::time::sleep_until(wake) => {
                if Instant::now() >= next_hb {
                    send!(heartbeat(&*src));
                    next_hb = Instant::now() + hb;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(min: u32, wait_ms: u32, max: u32) -> pb::TailRequest {
        pb::TailRequest { min_batch_bytes: min, max_wait_ms: wait_ms, max_batch_bytes: max, ..Default::default() }
    }

    #[test]
    fn fetch_parameters_are_clamped_to_server_limits() {
        let hb = Duration::from_millis(500);
        let f = Fetch::clamp(&req(10, 100, 2048), hb, 4 << 20);
        assert_eq!(f, Fetch { min_bytes: 10, max_wait: Duration::from_millis(100), max_bytes: 2048 });
        assert_eq!(Fetch::clamp(&req(0, 60_000, 0), hb, 4 << 20).max_wait, hb, "max_wait <= heartbeat");
        assert_eq!(Fetch::clamp(&req(0, 0, 0), hb, 4 << 20).max_bytes, DEFAULT_MAX_BATCH_BYTES, "0 = 1 MiB");
        assert_eq!(Fetch::clamp(&req(0, 0, u32::MAX), hb, 4 << 20).max_bytes, 4 << 20, "capped");
        assert_eq!(Fetch::clamp(&req(0, 0, 0), hb, 1000).max_bytes, 1000, "default is capped too");
    }
}
```

Create `src/server/bootstrap.rs`:

```rust
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
```

Notes for the implementer:
- `ServerState` holds the source **weakly**; each handler upgrades it per call. Tonic runs every connection on its own spawned task that outlives an aborted server task, so a strong reference there could keep a closed node's database open.
- Every producer send also watches shutdown and resets (`Out::send` in `tail.rs`, the `send` closure in `bootstrap.rs`), for the same reason: a client that stops reading must not keep a producer alive. Ending a stream with an error goes through one helper, `end_stream`, which waits for room (so a slow client still learns the reason) but also gives up on shutdown or reset. `Handle::block_on` is used from the bootstrap job's `spawn_blocking` thread, which is not an async context.
- The bootstrap job owns its `ReadSnapshot` and iterates it inside the blocking closure; it returns its semaphore permit only after the job (and so the snapshot) is dropped, and `ServerHandle::shutdown` waits (≤ 5 s) until every permit is back. So after `Master::shutdown`/`Slave::shutdown` no server-side snapshot keeps the database open.
- `store_status` keeps `CORRUPT` and `LOG_GAP`; any other store failure becomes `net::internal_status` (`INTERNAL` with `ErrorInfo`). Server start for master and slave goes through `server::start_for`.
- The bootstrap guard is registered at `position().last_seq` *before* the snapshot (a provisional guard) and then at the snapshot's `S`, so retention can never remove `S + 1` in between. The lease is inserted before the footer is sent, so the client's Tail always finds it; the Tail producer then owns it until it has read entries past `S`.
- Fetch batching (`produce` in `tail.rs`): available entries are read ahead in blocks of `READ_BATCH` and moved into a pending batch. The batch is flushed when adding the next entry would exceed `max_bytes` (an oversize entry alone fills a batch), when it reaches `min_bytes` (if `min_bytes > 0`), and — once everything available is pending — at once in real-time mode (`min_bytes == 0`) or when the oldest pending entry has waited `max_wait`. So catch-up always sends full batches without waiting, and real-time mode sends each newly committed entry immediately. Before ending a stream with an error (for example `LOG_GAP`), entries already read are delivered, and the error itself is sent with an awaited send, so a slow client still learns the reason.

- [ ] **Step 6: Serve from the master**

Above the test module in `src/master/source.rs`, add:

```rust
//! The master as a replication source.

use std::time::Duration;

use tokio::sync::watch;

use super::writer::Inner;
use crate::source::LogSource;
use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

impl LogSource for Inner {
    fn node_id(&self) -> &str {
        &self.cfg.identifier
    }

    fn role(&self) -> Role {
        Role::Master
    }

    fn is_ready(&self) -> bool {
        true
    }

    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)> {
        Some(self.identity_and_history())
    }

    fn store(&self) -> &Store {
        &self.store
    }

    fn origin_last_seq(&self) -> Seq {
        self.store.position().last_seq
    }

    fn upstream_chain(&self) -> Vec<String> {
        Vec::new()
    }

    fn max_entry_bytes(&self) -> u64 {
        self.cfg.max_entry_bytes
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
}
```

In `src/master/mod.rs`:

Replace `use std::sync::Arc;` and `use std::time::Duration;` with:

```rust
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
```

After `use crate::read::{ReadApi, ReadSnapshot};` add:

```rust
use crate::server::ServerHandle;
```

Add the field `server: Option<ServerHandle>,` to `struct Master` (after `task`).

The retention guard is now used by the server: in `src/store/retention.rs`, delete the three `#[allow(dead_code)] // used by the replication server (Plan 2)` lines (on `GuardSet`, `RetentionGuard` and `Store::guard`). In `Master::open`, replace:

```rust
        let task = maintenance::spawn(&inner);
        Ok(Master { inner, task: Some(task) })
```

with:

```rust
        // The epoch bump above happens before the replication port opens (DESIGN.md §13).
        let server = crate::server::start_for(&inner, inner.cfg.serve.clone()).await?;
        let task = maintenance::spawn(&inner);
        Ok(Master { inner, task: Some(task), server })
```

Replace `Master::shutdown` with:

```rust
    /// The address the replication server listens on, if this master serves replication.
    pub fn serve_addr(&self) -> Option<SocketAddr> {
        self.server.as_ref().map(ServerHandle::addr)
    }

    /// Ends every replication stream with `SHUTTING_DOWN`, stops background work, and waits
    /// until the database is closed, so the directory can be reopened at once.
    /// `Err(Timeout)` if something still holds the database after 10 s.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
        self.inner.shutdown_tx.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        if crate::server::await_sole_owner(&self.inner, Duration::from_secs(10)).await {
            Ok(())
        } else {
            Err(crate::Error::Timeout)
        }
    }
```

- [ ] **Step 7: Run the tests**

Run: `cargo test --lib -- server:: master::source`
Expected: PASS: 22 tests in `server::tests` — including the fetch-batching tests `real_time_mode_delivers_each_entry_promptly_one_batch_at_a_time` (one entry per batch, well under the 1 s heartbeat), `small_writes_wait_for_max_wait_and_arrive_together` (three writes, one batch, not before ~max_wait), `reaching_min_batch_bytes_sends_before_max_wait` (far sooner than the 20 s max_wait), `catch_up_is_split_at_max_batch_bytes_without_gaps` (1 000-entry backlog, every batch ≤ the server's 64 KiB cap, consecutive seqs), `an_entry_larger_than_max_batch_bytes_is_sent_alone` and `max_wait_is_clamped_to_the_heartbeat` (a 30 s max_wait behaves like the 200 ms heartbeat). Timers start after the fsynced `put` returns; lower bounds carry the semantics and upper bounds are generous, so slow CI disks do not fail them. Every stream wait is bounded at 10 s (`within`, `next_batch`, `stream_error`) — a regression fails instead of hanging. Also 14 in `server::handshake`, `server::tail::tests::fetch_parameters_are_clamped_to_server_limits`, and `master::source::tests::slow_reader_gets_log_gap_when_retention_overtakes_it`.

Run: `cargo test`
Expected: PASS, including every Plan 1 test (an embedded master has `serve: None`).

- [ ] **Step 8: Update DESIGN.md §10 and §16, and PROTOCOL.md §7.2 and §8.4**

Replace:

```
    pub serve: ServeConfig,
```

(in `MasterConfig`) with:

```
    pub serve: Option<ServeConfig>,     // None = embedded, non-replicating master
```

and after `pub fn identity(&self) -> DbIdentity;` in `impl Master` add:

```
    pub fn serve_addr(&self) -> Option<SocketAddr>;     // bound replication address (port 0 picks one)
```

In PROTOCOL.md §7.2, replace:

```
If `bootstrap_lease` matches a lease this server issued to the same `client_id`, and `from_seq ≤ snapshot_seq + 1`, the server releases that lease as soon as the stream is established.
```

with:

```
If `bootstrap_lease` matches a lease this server issued to the same `client_id`, and `from_seq ≤ snapshot_seq + 1`, the stream takes the lease over and the server releases it as soon as the stream has read the entries following the snapshot from its log (or when the stream ends). Releasing it earlier would let aggressive retention remove entry `snapshot_seq + 1` before the stream reads it.
```

In PROTOCOL.md §8.4, replace:

```
- a `Tail` from the same `client_id` presents the `lease_id` with `from_seq ≤ S + 1` (§7.2);
```

with:

```
- a `Tail` from the same `client_id` that presented the `lease_id` with `from_seq ≤ S + 1` has read the entries after `S` from the log, or has ended (§7.2);
```

In DESIGN.md §10 (serving side), replace:

```
6. Keep the guard until the client's `Tail` presents the lease, or the lease expires (default 15 min), or the stream fails before the footer. Without this, retention could remove `S + 1` between the end of the bootstrap and the start of the tail, and the slave would loop through bootstraps.
```

with:

```
6. Keep the guard until the client's `Tail` that presents the lease has read the entries after `S` (or ends), or the lease expires (default 15 min), or the stream fails before the footer. Without this, retention could remove `S + 1` between the end of the bootstrap and the tail's first read, and the slave would loop through bootstraps.
```

Run: `cargo test --test proto_sync`
Expected: PASS.

- [ ] **Step 9: Commit**

```bash
git add src DESIGN.md PROTOCOL.md
git commit -m "feat: replication server with handshake, batched tail, bootstrap leases; master serves

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Bidirectional Tail with acknowledgements, replica status and the `Status` RPC

**Files:**
- Create: `src/server/replicas.rs`, `src/server/status.rs`, `src/admin.rs`
- Modify: `proto/replication.proto` (replace), `src/server/mod.rs` (replace), `src/server/tail.rs` (replace), `src/server/bootstrap.rs` (replace), `src/server/test_client.rs` (replace), `src/server/tests.rs` (replace), `src/source.rs` (replace), `src/master/source.rs` (tests), `src/auth.rs` (`Method::Status`), `src/config.rs` (`replica_lag_time_max`), `src/master/writer.rs`, `src/master/mod.rs`, `src/lib.rs`, `PROTOCOL.md` (§2, §3.3, §6.5, §7, §7.1, §7.3, §7.6, §10, §11.2, §11.4, §12, §14), `DESIGN.md` (§11, §14, §16, §17, §18, §19)

Tasks 1–5 are implemented (commit `7bdd8fc`); their code under `src/` is the source of truth. The files this task replaces are given whole; they keep every Task 5 behaviour (validation, fetch batching, leases, shutdown and reset handling) and add the acks.

**Interfaces:**
- Consumes: Task 5 `ServerState`, `Svc`, `call_info`, `CallInfo::check_client_id`, `ServerState::{authorize, expire_leases, take_lease}`, `server::{start, start_for, end_stream, await_sole_owner, stopped}`, `tail::{validate, Fetch, Pending, framed_len, produce}`, `bootstrap::Job`, `test_client::{client, serving_master, tail_req, next_batch, stream_error, within, eventually}`; Task 2 `LogSource`, `Store::{position, read_entries}`; Task 4 `Method`, `PeerInfo`, `ServeLimits`; Plan 1 `master::writer::Inner`, `Master`.
- Produces:
  - Wire: `rpc Tail(stream TailClientMessage) returns (stream TailMessage)`, `TailClientMessage { oneof kind { TailRequest start = 1; TailAck ack = 2; } }`, `TailAck { applied_seq }`; `rpc Status(StatusRequest) returns (StatusResponse)` with `StatusRequest { client_id }`, `ReplicaSummary { node_id, applied_seq, in_sync }`, `StatusResponse { node_id, role, identity, epoch_history, last_seq, last_entry_crc, last_ts_ms, node_state, upstream, replicas }`.
  - `pub struct FetchSettings { min_batch_bytes: u64, max_wait: Duration, max_batch_bytes: u64 }`, `pub struct ReplicaStatus { node_id, remote_addr, peer_verified, epoch, connected_since, sent_seq, applied_seq, lag_entries, lag_ms: Option<u64>, in_sync, fetch }` (both re-exported from the crate root).
  - `server::replicas::{ReplicaRegistry (Default) { open(self: &Arc<Self>, NewReplica) -> ReplicaHandle, acks(&self, epoch, lag_max) -> Vec<(Seq, bool)>, wait_until(&self, timeout, impl FnMut(&Self) -> bool) -> bool, summary(&self, epoch, lag_max) -> Vec<(String, Seq, bool)>, status(&self, &dyn LogSource, now_ms, lag_max) -> Vec<ReplicaStatus> }, NewReplica { node_id, remote_addr, peer_verified, epoch, has_up_to, fetch }, ReplicaHandle::cell(&self) -> &Arc<ReplicaCell> (removes the stream on drop), ReplicaCell::{note_sent(seq), ack(applied_seq, node_last_seq) -> Result<(), String>}}`. `acks` and `wait_until` are first used by Task 12.
  - `server::start(Weak<dyn LogSource>, ServeConfig, Arc<ReplicaRegistry>)`, `server::start_for(&Arc<S>, Option<ServeConfig>, Arc<ReplicaRegistry>)`, `ServerState.replicas`, `ServerHandle::replica_lag_time_max(&self) -> Duration`.
  - `server::status::{node_info(&dyn LogSource, &ReplicaRegistry, lag_max) -> NodeInfo, to_pb(&NodeInfo) -> pb::StatusResponse}`; `pub mod admin` with `NodeInfo { node_id, role, identity, epoch_history, last_seq, last_entry_crc, last_ts_ms, node_state, upstream, replicas }` and `ReplicaSummary { node_id, applied_seq, in_sync }`.
  - `LogSource::{node_state(&self) -> String (default "ready"), upstream(&self) -> Option<String> (default None)}`; `Method::Status` (included in `MtlsSubjectAllowList::allow_all_methods`); `ServeLimits::replica_lag_time_max: Duration` (default 10 s, must be positive).
  - `master::writer::Inner.replicas: Arc<ReplicaRegistry>`; `Master::replicas(&self) -> Vec<ReplicaStatus>` (empty without `serve`).
  - Test helpers: `test_client::{TailExt::{tail_start, tail_with_acks}, ack(seq) -> pb::TailClientMessage}`.

- [ ] **Step 1: Write the replica registry's failing tests**

Create `src/server/replicas.rs` with only its test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn registry() -> Arc<ReplicaRegistry> {
        Arc::new(ReplicaRegistry::default())
    }

    fn new(has_up_to: Seq) -> NewReplica {
        NewReplica {
            node_id: "b".into(),
            remote_addr: "127.0.0.1:1".parse().unwrap(),
            peer_verified: false,
            epoch: 1,
            has_up_to,
            fetch: FetchSettings { min_batch_bytes: 0, max_wait: Duration::ZERO, max_batch_bytes: 1 << 20 },
        }
    }

    #[test]
    fn acks_must_be_monotonic_and_not_above_what_was_sent() {
        let reg = registry();
        let h = reg.open(new(5));
        h.cell().ack(5, 9).unwrap();
        assert!(h.cell().ack(6, 9).unwrap_err().contains("above the last sent"));
        h.cell().note_sent(9);
        h.cell().ack(7, 9).unwrap();
        assert!(h.cell().ack(6, 9).unwrap_err().contains("below the previous"));
        assert_eq!(reg.acks(1, Duration::from_secs(10)), vec![(7, false)], "not caught up yet");
        h.cell().ack(9, 9).unwrap();
        assert_eq!(reg.acks(1, Duration::from_secs(10)), vec![(9, true)]);
        assert_eq!(reg.acks(2, Duration::from_secs(10)), vec![(9, false)], "other epoch: not in sync");
        drop(h);
        assert!(reg.acks(1, Duration::from_secs(10)).is_empty(), "closed streams leave the registry");
    }

    #[test]
    fn in_sync_expires_after_lag_time_max() {
        let reg = registry();
        let h = reg.open(new(3));
        h.cell().ack(3, 3).unwrap();
        assert_eq!(reg.acks(1, Duration::from_millis(50)), vec![(3, true)]);
        std::thread::sleep(Duration::from_millis(80));
        assert_eq!(reg.acks(1, Duration::from_millis(50)), vec![(3, false)]);
    }

    #[test]
    fn wait_until_wakes_on_acks_and_times_out() {
        let reg = registry();
        let h = reg.open(new(0));
        h.cell().note_sent(4);
        let cell = h.cell().clone();
        let t = std::thread::spawn(move || {
            std::thread::sleep(Duration::from_millis(30));
            cell.ack(4, 4).unwrap();
        });
        assert!(reg.wait_until(Duration::from_secs(5), |r| r.acks(1, Duration::from_secs(10)).iter().any(|a| a.0 >= 4)));
        t.join().unwrap();
        let t0 = Instant::now();
        assert!(!reg.wait_until(Duration::from_millis(50), |r| r.acks(1, Duration::from_secs(10)).iter().any(|a| a.0 >= 5)));
        assert!(t0.elapsed() >= Duration::from_millis(50));
    }
}
```

Add to the module list in `src/server/mod.rs`:

```rust
pub(crate) mod replicas;
```

Run: `cargo test --lib server::replicas`
Expected: FAIL to compile: `cannot find type ReplicaRegistry in this scope`, `cannot find struct NewReplica`, `cannot find struct FetchSettings`.

- [ ] **Step 2: Implement the registry** (above the test module in `src/server/replicas.rs`)

```rust
//! Per-stream state of the replicas a node serves (addendum §1): what was sent, what the
//! replica acknowledged, and whether it is in sync.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex, PoisonError};
use std::time::{Duration, Instant, SystemTime};

use crate::source::LogSource;
use crate::types::Seq;

/// The fetch settings of one Tail stream, after the server clamped them.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct FetchSettings {
    pub min_batch_bytes: u64,
    pub max_wait: Duration,
    pub max_batch_bytes: u64,
}

/// A direct downstream replica, as its upstream sees it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ReplicaStatus {
    /// The replica's `x-rrdb-node-id` (or certificate CommonName under mutual TLS).
    pub node_id: String,
    pub remote_addr: SocketAddr,
    /// True when `node_id` comes from a verified client certificate.
    pub peer_verified: bool,
    /// The epoch of the identity the replica follows.
    pub epoch: u64,
    pub connected_since: SystemTime,
    /// The last seq sent to the replica.
    pub sent_seq: Seq,
    /// The last seq the replica acknowledged as committed.
    pub applied_seq: Seq,
    /// This node's `last_seq` minus `applied_seq`.
    pub lag_entries: u64,
    /// Now minus `ts_ms` of the first unacknowledged entry; 0 when caught up, `None` when that
    /// entry is no longer retained here.
    pub lag_ms: Option<u64>,
    /// Follows this node's current identity and was fully caught up within
    /// `ServeLimits::replica_lag_time_max`.
    pub in_sync: bool,
    pub fetch: FetchSettings,
}

#[derive(Debug)]
struct Progress {
    sent_seq: Seq,
    applied_seq: Seq,
    /// The last time `applied_seq` reached this node's `last_seq`.
    caught_up_at: Option<Instant>,
}

/// One open Tail stream.
#[derive(Debug)]
pub(crate) struct ReplicaCell {
    node_id: String,
    remote_addr: SocketAddr,
    peer_verified: bool,
    epoch: u64,
    connected_since: SystemTime,
    fetch: FetchSettings,
    progress: Mutex<Progress>,
    registry: Arc<ReplicaRegistry>,
}

impl ReplicaCell {
    fn progress(&self) -> std::sync::MutexGuard<'_, Progress> {
        self.progress.lock().unwrap_or_else(PoisonError::into_inner)
    }

    /// Records that entries up to `seq` were handed to the stream.
    pub(crate) fn note_sent(&self, seq: Seq) {
        let mut p = self.progress();
        p.sent_seq = p.sent_seq.max(seq);
    }

    /// Records an ack. `node_last_seq` is this node's `last_seq` right now. An ack that goes
    /// backwards or above the last sent seq is a protocol violation.
    pub(crate) fn ack(&self, applied_seq: Seq, node_last_seq: Seq) -> Result<(), String> {
        {
            let mut p = self.progress();
            if applied_seq < p.applied_seq {
                return Err(format!("ack {applied_seq} is below the previous ack {}", p.applied_seq));
            }
            if applied_seq > p.sent_seq {
                return Err(format!("ack {applied_seq} is above the last sent seq {}", p.sent_seq));
            }
            p.applied_seq = applied_seq;
            if applied_seq >= node_last_seq {
                p.caught_up_at = Some(Instant::now());
            }
        }
        self.registry.notify();
        Ok(())
    }
}

/// Removes its stream from the registry when dropped (the Tail producer owns it).
pub(crate) struct ReplicaHandle {
    id: u64,
    cell: Arc<ReplicaCell>,
}

impl ReplicaHandle {
    pub(crate) fn cell(&self) -> &Arc<ReplicaCell> {
        &self.cell
    }
}

impl Drop for ReplicaHandle {
    fn drop(&mut self) {
        let reg = &self.cell.registry;
        reg.streams().remove(&self.id);
        reg.notify();
    }
}

/// The replicas of one node. Shared by its server and (on the master) its writer.
#[derive(Debug, Default)]
pub(crate) struct ReplicaRegistry {
    next_id: AtomicU64,
    streams: Mutex<HashMap<u64, Arc<ReplicaCell>>>,
    /// Bumped on every ack and every stream change; writers waiting for acks wait on it.
    changes: Mutex<u64>,
    changed: Condvar,
}

/// What a new stream registers.
pub(crate) struct NewReplica {
    pub node_id: String,
    pub remote_addr: SocketAddr,
    pub peer_verified: bool,
    pub epoch: u64,
    /// `from_seq - 1`: the replica already has everything up to here.
    pub has_up_to: Seq,
    pub fetch: FetchSettings,
}

impl ReplicaRegistry {
    fn streams(&self) -> std::sync::MutexGuard<'_, HashMap<u64, Arc<ReplicaCell>>> {
        self.streams.lock().unwrap_or_else(PoisonError::into_inner)
    }

    fn notify(&self) {
        *self.changes.lock().unwrap_or_else(PoisonError::into_inner) += 1;
        self.changed.notify_all();
    }

    pub(crate) fn open(self: &Arc<Self>, r: NewReplica) -> ReplicaHandle {
        let id = self.next_id.fetch_add(1, Ordering::SeqCst);
        let cell = Arc::new(ReplicaCell {
            node_id: r.node_id,
            remote_addr: r.remote_addr,
            peer_verified: r.peer_verified,
            epoch: r.epoch,
            connected_since: SystemTime::now(),
            fetch: r.fetch,
            progress: Mutex::new(Progress { sent_seq: r.has_up_to, applied_seq: r.has_up_to, caught_up_at: None }),
            registry: self.clone(),
        });
        self.streams().insert(id, cell.clone());
        self.notify();
        ReplicaHandle { id, cell }
    }

    /// `(applied_seq, in_sync)` of every open stream, for the given current epoch.
    #[cfg_attr(not(test), allow(dead_code))] // used by the write concern (later Plan 2 task)
    pub(crate) fn acks(&self, epoch: u64, lag_max: Duration) -> Vec<(Seq, bool)> {
        self.streams()
            .values()
            .map(|c| {
                let p = c.progress();
                let in_sync = c.epoch == epoch && p.caught_up_at.is_some_and(|t| t.elapsed() <= lag_max);
                (p.applied_seq, in_sync)
            })
            .collect()
    }

    /// Blocks until `done` holds for the current acks, or `timeout` passes. Returns whether
    /// `done` held. For synchronous writers; never call it on an async worker thread.
    #[cfg_attr(not(test), allow(dead_code))] // used by the write concern (later Plan 2 task)
    pub(crate) fn wait_until(&self, timeout: Duration, mut done: impl FnMut(&Self) -> bool) -> bool {
        let deadline = Instant::now() + timeout;
        let mut changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
        loop {
            // `done` takes the stream locks; never hold `changes` while calling it.
            let seen = *changes;
            drop(changes);
            if done(self) {
                return true;
            }
            changes = self.changes.lock().unwrap_or_else(PoisonError::into_inner);
            if *changes != seen {
                continue;
            }
            let now = Instant::now();
            if now >= deadline {
                return false;
            }
            changes = self.changed.wait_timeout(changes, deadline - now).unwrap_or_else(PoisonError::into_inner).0;
        }
    }

    /// `(node_id, applied_seq, in_sync)` of every open stream, for `Status`.
    pub(crate) fn summary(&self, epoch: u64, lag_max: Duration) -> Vec<(String, Seq, bool)> {
        let mut out: Vec<_> = self
            .streams()
            .values()
            .map(|c| {
                let p = c.progress();
                (c.node_id.clone(), p.applied_seq, c.epoch == epoch && p.caught_up_at.is_some_and(|t| t.elapsed() <= lag_max))
            })
            .collect();
        out.sort();
        out
    }

    /// The status of every open stream.
    pub(crate) fn status(&self, src: &dyn LogSource, now_ms: u64, lag_max: Duration) -> Vec<ReplicaStatus> {
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
                    in_sync: c.epoch == epoch && p.caught_up_at.is_some_and(|t| t.elapsed() <= lag_max),
                    fetch: c.fetch,
                }
            })
            .collect();
        out.sort_by(|a, b| a.node_id.cmp(&b.node_id).then(a.connected_since.cmp(&b.connected_since)));
        out
    }
}
```

`acks` and `wait_until` carry `#[cfg_attr(not(test), allow(dead_code))]` until the write concern uses them (Task 12 removes the attribute).

Run: `cargo test --lib server::replicas`
Expected: PASS, 3 tests: `acks_must_be_monotonic_and_not_above_what_was_sent`, `in_sync_expires_after_lag_time_max`, `wait_until_wakes_on_acks_and_times_out`.

- [ ] **Step 3: Change the wire protocol and the server tests**

Replace `proto/replication.proto` with:

```proto
syntax = "proto3";

package rrdb.replication.v1;

service Replication {
  rpc Handshake  (HandshakeRequest)          returns (HandshakeResponse);
  rpc Tail       (stream TailClientMessage)  returns (stream TailMessage);
  rpc Bootstrap  (BootstrapRequest)          returns (stream BootstrapChunk);
  rpc VerifyBegin(VerifyBeginRequest)        returns (VerifyBeginResponse);
  rpc VerifyRange(stream VerifyRangeRequest) returns (stream VerifyRangeResponse);
  rpc VerifyEnd  (VerifyEndRequest)          returns (VerifyEndResponse);
  rpc Status     (StatusRequest)             returns (StatusResponse);
}

// ---------- shared ----------

message Identity   { bytes db_uuid = 1; uint64 epoch = 2; }
message CfSpec     { string name = 1; optional string comparator = 2; uint64 create_seq = 3; }
message EpochStart { uint64 epoch = 1; uint64 start_seq = 2; }

enum Reason {
  REASON_UNSPECIFIED = 0;
  NO_LOCAL_STATE     = 1;
  IDENTITY_CHANGED   = 2;
  LOG_GAP            = 3;
  PROTOCOL_MISMATCH  = 4;   // frozen
  ENVELOPE_MISMATCH  = 5;   // frozen
  NOT_READY          = 6;
  LOOP_DETECTED      = 7;
  DIVERGED           = 8;
  BUSY               = 9;
  SESSION_NOT_FOUND  = 10;
  UPSTREAM_RESET     = 11;
  CORRUPT            = 12;
  SHUTTING_DOWN      = 13;
  EPOCH_ADVANCED     = 14;
  STALE_EPOCH        = 15;
}

// ---------- log ----------

message LogEntry {
  uint64  seq    = 1;
  uint64  epoch  = 2;
  uint64  ts_ms  = 3;
  bytes   ops    = 4;   // serialised OpList; never re-encoded
  fixed32 crc32c = 5;   // CRC-32C(BE64(seq) ‖ BE64(epoch) ‖ BE64(ts_ms) ‖ ops)
}

message OpList { repeated Op ops = 1; }

message Op {
  oneof kind {
    Put         put          = 1;
    Delete      delete       = 2;
    DeleteRange delete_range = 3;
    CreateCf    create_cf    = 4;
    DropCf      drop_cf      = 5;
  }
}
message Put         { string cf = 1; bytes key = 2; bytes value = 3; optional uint64 expire_at_ms = 4; }
message Delete      { string cf = 1; bytes key = 2; }
message DeleteRange { string cf = 1; bytes start = 2; bytes end = 3; }
message CreateCf    { string name = 1; optional string comparator = 2; }
message DropCf      { string name = 1; }

// ---------- handshake ----------

enum Decision {
  DECISION_UNSPECIFIED = 0;
  RESUME               = 1;
  BOOTSTRAP            = 2;
  REJECT               = 3;   // frozen
}

message HandshakeRequest {
  uint32 protocol_version          = 1;   // frozen
  uint32 envelope_version          = 2;   // frozen
  string client_id                 = 3;   // frozen
  optional Identity known_identity = 4;
  uint64 last_seq                  = 5;
  optional fixed32 last_crc32c     = 6;
}

message HandshakeResponse {
  uint32   protocol_version        = 1;   // frozen
  uint32   envelope_version        = 2;   // frozen
  string   server_id               = 3;   // frozen
  Decision decision                = 4;   // frozen
  Reason   reason                  = 5;   // frozen
  string   detail                  = 6;   // frozen
  repeated string upstream_chain   = 7;
  optional Identity identity       = 8;
  repeated CfSpec cf_specs         = 9;
  uint64   first_retained_seq      = 10;
  uint64   last_seq                = 11;
  uint64   origin_last_seq         = 12;
  uint64   max_entry_bytes         = 13;
  uint32   heartbeat_ms            = 14;
  repeated EpochStart epoch_history = 15;
}

// ---------- tail ----------

message TailRequest {
  string   client_id               = 1;
  Identity identity                = 2;
  uint64   from_seq                = 3;
  optional fixed32 prev_crc32c     = 4;
  optional bytes bootstrap_lease   = 5;
  uint32   min_batch_bytes         = 6;
  uint32   max_wait_ms             = 7;
  uint32   max_batch_bytes         = 8;
}

// The first client message is `start`; every later one is `ack`.
message TailClientMessage { oneof kind { TailRequest start = 1; TailAck ack = 2; } }
// Every entry with seq <= applied_seq is committed on the replica. Non-decreasing.
message TailAck           { uint64 applied_seq = 1; }

message TailMessage { oneof kind { EntryBatch batch = 1; Heartbeat heartbeat = 2; } }
message EntryBatch  { repeated LogEntry entries = 1; }
message Heartbeat   { uint64 upstream_last_seq = 1; uint64 origin_last_seq = 2; uint64 ts_ms = 3; }

// ---------- bootstrap ----------

message BootstrapRequest { string client_id = 1; }

message BootstrapChunk {
  oneof kind {
    BootstrapHeader  header  = 1;
    BootstrapRecords records = 2;
    BootstrapFooter  footer  = 3;
  }
}

message BootstrapHeader {
  Identity identity                = 1;
  uint64   snapshot_seq            = 2;
  optional fixed32 snapshot_crc32c = 3;
  repeated CfSpec cf_specs         = 4;
  uint64   max_entry_bytes         = 5;
  uint32   envelope_version        = 6;
  bytes    lease_id                = 7;
  uint64   lease_ttl_ms            = 8;
  repeated EpochStart epoch_history = 9;
}

message Record           { bytes key = 1; bytes envelope = 2; }
message BootstrapRecords { string cf = 1; repeated Record records = 2; }
message BootstrapFooter  { uint64 record_count = 1; bytes digest = 2; }

// ---------- verify ----------

enum VerifyMode { VERIFY_MODE_UNSPECIFIED = 0; QUICK = 1; FULL = 2; }

message VerifyBeginRequest  { string client_id = 1; VerifyMode mode = 2; }
message VerifyBeginResponse {
  bytes    session_id         = 1;
  Identity identity           = 2;
  uint64   snapshot_seq       = 3;
  repeated CfSpec cf_specs    = 4;
  uint64   session_timeout_ms = 5;
}

message KeyRange  { string cf = 1; optional bytes lower = 2; optional bytes upper = 3; }
message HashRange { KeyRange range = 1; uint64 count = 2; bytes hash = 3; }
message ListRange { KeyRange range = 1; uint32 limit = 2; }

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

message VerifyEndRequest  { bytes session_id = 1; }
message VerifyEndResponse {}

// ---------- status (operator tooling) ----------

message StatusRequest  { string client_id = 1; }
message ReplicaSummary { string node_id = 1; uint64 applied_seq = 2; bool in_sync = 3; }
message StatusResponse {
  string   node_id                  = 1;
  string   role                     = 2;   // "master" or "slave"
  optional Identity identity        = 3;
  repeated EpochStart epoch_history = 4;
  uint64   last_seq                 = 5;
  optional fixed32 last_entry_crc   = 6;
  uint64   last_ts_ms               = 7;   // ts of the entry at last_seq; 0 if unknown
  string   node_state               = 8;   // "ready", "bootstrapping" or "halted:<Reason>"
  string   upstream                 = 9;   // empty on the master
  repeated ReplicaSummary replicas  = 10;
}
```

In PROTOCOL.md §14, replace the contents of the code block (between ```` ```proto ```` and ```` ``` ````) with the new `proto/replication.proto`, byte for byte.

Replace `src/server/test_client.rs` with (adds `TailExt` — every Tail opened by a test now starts with `start` — and `ack`):

```rust
//! A bare protocol client for server unit tests.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::auth::AllowAll;
use crate::clock::ManualClock;
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::config::{MasterConfig, ServeConfig};
use crate::master::Master;
use crate::pb;
use crate::net::convert::{entry_from_pb, identity_to_pb};
use crate::pb::replication_client::ReplicationClient;
use crate::pb::tail_client_message::Kind as ClientKind;
use crate::pb::tail_message::Kind;
use crate::types::{DbIdentity, LogEntry};

pub(crate) const T0: u64 = 1_700_000_000_000;

#[derive(Clone)]
pub(crate) struct Meta {
    pub node_id: &'static str,
    pub token: Option<&'static str>,
    pub version: Option<u32>,
}

impl Interceptor for Meta {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let md = req.metadata_mut();
        if let Some(v) = self.version {
            md.insert("x-rrdb-protocol-version", AsciiMetadataValue::from(v));
        }
        md.insert("x-rrdb-node-id", AsciiMetadataValue::from_static(self.node_id));
        if let Some(t) = self.token {
            md.insert("authorization", AsciiMetadataValue::try_from(format!("Bearer {t}")).unwrap());
        }
        Ok(req)
    }
}

pub(crate) type Client = ReplicationClient<InterceptedService<Channel, Meta>>;

pub(crate) fn client_with(addr: SocketAddr, meta: Meta) -> Client {
    let channel = Endpoint::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
    ReplicationClient::with_interceptor(channel, meta).max_decoding_message_size(64 << 20)
}

pub(crate) fn client(addr: SocketAddr, node_id: &'static str) -> Client {
    client_with(addr, Meta { node_id, token: None, version: Some(PROTOCOL_VERSION) })
}

/// A real-time Tail request (`min_batch_bytes = 0`, server-default batch size).
pub(crate) fn tail_req(id: &DbIdentity, from_seq: u64) -> pb::TailRequest {
    pb::TailRequest {
        client_id: "b".into(),
        identity: Some(identity_to_pb(id)),
        from_seq,
        prev_crc32c: None,
        bootstrap_lease: None,
        min_batch_bytes: 0,
        max_wait_ms: 0,
        max_batch_bytes: 0,
    }
}

/// Awaits `fut` for at most 10 s, so a regression fails the test instead of hanging it.
pub(crate) async fn within<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), fut).await.unwrap_or_else(|_| panic!("timed out after 10 s: {what}"))
}

/// The next entry batch of a Tail stream, skipping heartbeats.
pub(crate) async fn next_batch(s: &mut tonic::Streaming<pb::TailMessage>) -> Vec<LogEntry> {
    within("the next entry batch", async {
        loop {
            match s.message().await.unwrap().unwrap().kind.unwrap() {
                Kind::Batch(b) => return b.entries.into_iter().map(entry_from_pb).collect(),
                Kind::Heartbeat(_) => {}
            }
        }
    })
    .await
}

/// The status a stream ends with, skipping its remaining messages.
pub(crate) async fn stream_error<T>(s: &mut tonic::Streaming<T>) -> Status {
    within("the stream to end with a status", async {
        loop {
            match s.message().await {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("stream ended without a status"),
                Err(e) => return e,
            }
        }
    })
    .await
}

/// Opens Tail streams (PROTOCOL.md §7: the request stream starts with `start`).
pub(crate) trait TailExt {
    /// A Tail that sends only `start`, then half-closes its request stream (no acks).
    async fn tail_start(&mut self, req: pb::TailRequest) -> Result<Response<tonic::Streaming<pb::TailMessage>>, Status>;
    /// A Tail whose acks the test sends through the returned channel.
    async fn tail_with_acks(
        &mut self,
        req: pb::TailRequest,
    ) -> Result<(tonic::Streaming<pb::TailMessage>, mpsc::Sender<pb::TailClientMessage>), Status>;
}

impl TailExt for Client {
    async fn tail_start(&mut self, req: pb::TailRequest) -> Result<Response<tonic::Streaming<pb::TailMessage>>, Status> {
        let start = pb::TailClientMessage { kind: Some(ClientKind::Start(req)) };
        self.tail(tokio_stream::iter([start])).await
    }

    async fn tail_with_acks(
        &mut self,
        req: pb::TailRequest,
    ) -> Result<(tonic::Streaming<pb::TailMessage>, mpsc::Sender<pb::TailClientMessage>), Status> {
        let (tx, rx) = mpsc::channel(16);
        tx.send(pb::TailClientMessage { kind: Some(ClientKind::Start(req)) }).await.unwrap();
        let s = self.tail(ReceiverStream::new(rx)).await?.into_inner();
        Ok((s, tx))
    }
}

/// An ack message.
pub(crate) fn ack(applied_seq: u64) -> pb::TailClientMessage {
    pb::TailClientMessage { kind: Some(ClientKind::Ack(pb::TailAck { applied_seq })) }
}

/// A handshake from a client with no data.
pub(crate) fn hello(client_id: &str) -> pb::HandshakeRequest {
    pb::HandshakeRequest {
        protocol_version: PROTOCOL_VERSION,
        envelope_version: u32::from(ENVELOPE_VERSION),
        client_id: client_id.into(),
        known_identity: None,
        last_seq: 0,
        last_crc32c: None,
    }
}

/// A master serving on 127.0.0.1:0 with `AllowAll`, a manual clock and a 100 ms heartbeat.
pub(crate) async fn serving_master(dir: &Path, tweak: impl FnOnce(&mut MasterConfig)) -> (Master, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(T0));
    let mut cfg = MasterConfig::new("a", dir);
    cfg.clock = clock.clone();
    cfg.heartbeat = Duration::from_millis(100);
    cfg.retention.check_interval = Duration::from_secs(3600);
    cfg.reaper.interval = Duration::from_secs(3600);
    cfg.serve = Some(ServeConfig::new([127, 0, 0, 1].into(), 0, Arc::new(AllowAll)));
    tweak(&mut cfg);
    (Master::open(cfg).await.unwrap(), clock)
}

/// Polls `f` every 10 ms for up to 10 s.
pub(crate) async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
```

Replace `src/server/tests.rs` with (every `.tail(req)` becomes `.tail_start(req)`; four new tests at the end):

```rust
//! Server behaviour against a real master, through a bare protocol client.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::Code;

use super::test_client::{
    ack,
    client, client_with, eventually, hello, next_batch, serving_master, stream_error, tail_req, within, Meta, TailExt,
};
use crate::auth::{AllowMethods, Method, TokenAuthenticator};
use crate::codec::digest::Digest;
use crate::codec::{envelope, PROTOCOL_VERSION};
use crate::log::LogApi;
use crate::net::convert::identity_to_pb;
use crate::net::reason_of;
use crate::pb;
use crate::pb::bootstrap_chunk::Kind as Chunk;
use crate::pb::tail_message::Kind as Msg;
use crate::types::{CfSpec, DbIdentity, LogEntry};

/// The next entry of a real-time stream: one entry per batch when entries are written one
/// at a time after the stream caught up.
async fn next_entry(s: &mut tonic::Streaming<pb::TailMessage>) -> LogEntry {
    let mut batch = next_batch(s).await;
    assert_eq!(batch.len(), 1, "real-time batches carry the one entry that became available");
    batch.remove(0)
}

#[tokio::test]
async fn handshake_reports_the_master_state() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let r = c.handshake(hello("b")).await.unwrap().into_inner();
    assert_eq!(r.server_id, "a");
    assert_eq!((r.decision(), r.reason()), (pb::Decision::Bootstrap, pb::Reason::NoLocalState));
    assert_eq!(r.identity, Some(identity_to_pb(&m.identity())));
    assert_eq!(r.epoch_history, vec![pb::EpochStart { epoch: 1, start_seq: 1 }]);
    assert_eq!((r.first_retained_seq, r.last_seq, r.origin_last_seq), (1, 2, 2));
    assert_eq!((r.max_entry_bytes, r.heartbeat_ms), (16 << 20, 100));
    assert!(r.upstream_chain.is_empty());
    assert_eq!(r.cf_specs.len(), 1);

    let mut known = hello("b");
    known.known_identity = Some(identity_to_pb(&m.identity()));
    known.last_seq = 2;
    known.last_crc32c = m.entries_since(1).unwrap().next().unwrap().ok().map(|e| e.crc32c);
    let r = c.handshake(known).await.unwrap().into_inner();
    assert_eq!((r.decision(), r.reason()), (pb::Decision::Resume, pb::Reason::Unspecified));
}

#[tokio::test]
async fn version_mismatch_is_answered_in_the_handshake_body() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    // No protocol metadata at all: Handshake still answers.
    let mut c = client_with(m.serve_addr().unwrap(), Meta { node_id: "b", token: None, version: None });
    let mut req = hello("b");
    req.protocol_version = PROTOCOL_VERSION + 1;
    let r = c.handshake(req).await.unwrap().into_inner();
    assert_eq!((r.decision(), r.reason()), (pb::Decision::Reject, pb::Reason::ProtocolMismatch));
    assert_eq!(r.protocol_version, PROTOCOL_VERSION);
    // Every other RPC fails without the metadata.
    let err = c.tail_start(tail_req(&m.identity(), 1)).await.unwrap_err();
    assert_eq!((err.code(), reason_of(&err)), (Code::FailedPrecondition, Some(pb::Reason::ProtocolMismatch)));
}

#[tokio::test]
async fn client_id_must_match_the_node_id_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let err = client(m.serve_addr().unwrap(), "b").handshake(hello("someone-else")).await.unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn malformed_handshake_input_is_invalid_argument() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let addr = m.serve_addr().unwrap();
    let mut c = client(addr, "b");

    let mut short_uuid = hello("b");
    short_uuid.known_identity = Some(pb::Identity { db_uuid: vec![1; 15], epoch: 1 });
    let e = c.handshake(short_uuid).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "15-byte db_uuid");
    assert_eq!(reason_of(&e), Some(pb::Reason::Unspecified), "INVALID_ARGUMENT carries ErrorInfo");

    // An empty client_id (the node-id metadata is "b", so it would also mismatch).
    assert_eq!(c.handshake(hello("")).await.unwrap_err().code(), Code::InvalidArgument, "empty client_id");

    // client_id and node id agree but are not valid ids.
    let long: &'static str = Box::leak("x".repeat(129).into_boxed_str());
    for bad in ["has space", "semi;colon", "a/b", long] {
        let mut c = client(addr, bad);
        let e = c.handshake(hello(bad)).await.unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "client_id {bad:?}");
    }

    // The node-id metadata is checked on every RPC.
    let mut c = client(addr, "bad id");
    let e = c.tail_start(tail_req(&m.identity(), 1)).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "tail with a bad x-rrdb-node-id");
    let e = c.bootstrap(pb::BootstrapRequest { client_id: "bad id".into() }).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "bootstrap with a bad x-rrdb-node-id");

    // Tail and Bootstrap also require client_id to equal the metadata.
    let mut c = client(addr, "b");
    let mut req = tail_req(&m.identity(), 1);
    req.client_id = "other".into();
    assert_eq!(c.tail_start(req).await.unwrap_err().code(), Code::InvalidArgument, "tail client_id mismatch");
    let e = c.bootstrap(pb::BootstrapRequest { client_id: "other".into() }).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "bootstrap client_id mismatch");
}

#[tokio::test]
async fn tail_streams_entries_in_order_then_heartbeats_then_new_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"", b"").unwrap();
    m.put("c", b"\0", b"\0").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let mut s = c.tail_start(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    // The backlog arrives as one batch.
    let backlog = next_batch(&mut s).await;
    assert_eq!(backlog.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![1, 2, 3]);
    backlog.iter().for_each(|e| e.verify_crc().unwrap());
    // Idle: heartbeats only, carrying the server's position.
    match within("a heartbeat", s.message()).await.unwrap().unwrap().kind.unwrap() {
        Msg::Heartbeat(h) => assert_eq!((h.upstream_last_seq, h.origin_last_seq), (3, 3)),
        Msg::Batch(b) => panic!("unexpected batch of {}", b.entries.len()),
    }
    m.put("c", b"k", b"v").unwrap();
    assert_eq!(next_entry(&mut s).await.seq, 4);
}

#[tokio::test]
async fn tail_validation_follows_the_spec() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    eventually("retention", || m.first_retained_seq() == 6).await;
    let id = m.identity();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let reason = |e: tonic::Status| (e.code(), reason_of(&e));

    let other = DbIdentity { db_uuid: uuid::Uuid::new_v4(), epoch: 1 };
    let e = c.tail_start(tail_req(&other, 6)).await.unwrap_err();
    assert_eq!(reason(e), (Code::FailedPrecondition, Some(pb::Reason::IdentityChanged)));

    let e = c.tail_start(tail_req(&id, 8)).await.unwrap_err();
    assert_eq!(reason(e), (Code::FailedPrecondition, Some(pb::Reason::Diverged)), "past master last_seq + 1");

    let mut bad_prev = tail_req(&id, 7);
    bad_prev.prev_crc32c = Some(0xDEAD_BEEF);
    let e = c.tail_start(bad_prev).await.unwrap_err();
    assert_eq!(reason(e), (Code::FailedPrecondition, Some(pb::Reason::Diverged)));

    let e = c.tail_start(tail_req(&id, 2)).await.unwrap_err();
    assert_eq!(reason(e), (Code::OutOfRange, Some(pb::Reason::LogGap)));

    let e = c.tail_start(tail_req(&id, 0)).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);

    // from_seq = last_seq + 1 is valid: the stream waits for the next write.
    let mut s = c.tail_start(tail_req(&id, 7)).await.unwrap().into_inner();
    m.put("c", b"x", b"v").unwrap();
    assert_eq!(next_entry(&mut s).await.seq, 7);
}

#[tokio::test]
async fn bump_epoch_ends_streams_with_upstream_reset() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    assert_eq!(next_entry(&mut s).await.seq, 1);
    m.bump_epoch().unwrap();
    let err = stream_error(&mut s).await;
    assert_eq!((err.code(), reason_of(&err)), (Code::Unavailable, Some(pb::Reason::UpstreamReset)));
}

#[tokio::test]
async fn shutdown_ends_streams_and_releases_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    assert_eq!(next_entry(&mut s).await.seq, 1);
    m.shutdown().await.unwrap();
    let err = stream_error(&mut s).await;
    assert_eq!((err.code(), reason_of(&err)), (Code::Unavailable, Some(pb::Reason::ShuttingDown)));
    // The directory can be reopened immediately.
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    assert_eq!(m.last_seq(), 1);
}

#[tokio::test]
async fn embedded_master_does_not_serve() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.serve = None).await;
    assert!(m.serve_addr().is_none());
}

#[tokio::test]
async fn failed_authentications_are_rate_limited_per_address() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let serve = cfg.serve.as_mut().unwrap();
        serve.authenticator = Arc::new(TokenAuthenticator::new(["good"]));
        serve.allow_plaintext_token = true;
        serve.limits.max_auth_failures_per_minute = 3;
    })
    .await;
    let addr = m.serve_addr().unwrap();
    let meta = |token| Meta { node_id: "b", token: Some(token), version: Some(PROTOCOL_VERSION) };
    client_with(addr, meta("good")).handshake(hello("b")).await.unwrap();
    for _ in 0..3 {
        let e = client_with(addr, meta("bad")).handshake(hello("b")).await.unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        assert_eq!(reason_of(&e), None, "auth failures carry no ErrorInfo");
    }
    let e = client_with(addr, meta("good")).handshake(hello("b")).await.unwrap_err();
    assert_eq!(e.code(), Code::Unauthenticated, "blocked without checking the token");
}

#[tokio::test]
async fn methods_can_be_denied_per_credential() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let serve = cfg.serve.as_mut().unwrap();
        let token = Arc::new(TokenAuthenticator::new(["t"]));
        serve.authenticator = Arc::new(AllowMethods::new(token, &[Method::Handshake, Method::Tail]));
        serve.allow_plaintext_token = true;
    })
    .await;
    let mut c = client_with(m.serve_addr().unwrap(), Meta { node_id: "b", token: Some("t"), version: Some(PROTOCOL_VERSION) });
    c.handshake(hello("b")).await.unwrap();
    let e = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
}

#[tokio::test]
async fn verify_is_not_implemented_yet() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let e = client(m.serve_addr().unwrap(), "b").verify_begin(pb::VerifyBeginRequest::default()).await.unwrap_err();
    assert_eq!(e.code(), Code::Unimplemented);
    assert_eq!(reason_of(&e), Some(pb::Reason::Unspecified), "UNIMPLEMENTED carries ErrorInfo");
}

/// Reads a whole bootstrap stream: (header, records per message, footer).
async fn read_bootstrap(
    c: &mut super::test_client::Client,
) -> (pb::BootstrapHeader, Vec<pb::BootstrapRecords>, pb::BootstrapFooter) {
    within("a whole bootstrap stream", async {
        let mut s = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap().into_inner();
        let Chunk::Header(h) = s.message().await.unwrap().unwrap().kind.unwrap() else { panic!("header first") };
        let mut msgs = Vec::new();
        loop {
            match s.message().await.unwrap().unwrap().kind.unwrap() {
                Chunk::Records(r) => msgs.push(r),
                Chunk::Footer(f) => return (h, msgs, f),
                Chunk::Header(_) => panic!("second header"),
            }
        }
    })
    .await
}

#[tokio::test]
async fn bootstrap_streams_a_consistent_snapshot_with_a_valid_digest() {
    let dir = tempfile::tempdir().unwrap();
    let (m, clock) = serving_master(dir.path(), |cfg| {
        cfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 40;
    })
    .await;
    m.create_cf(CfSpec::new("b-empty")).unwrap();
    m.create_cf(CfSpec::new("a")).unwrap();
    m.put("a", b"", b"").unwrap();
    m.put("a", b"\0", b"\0\0").unwrap();
    m.put_with_ttl("a", b"ttl", b"gone", Duration::from_secs(1)).unwrap();
    for i in 0..10u8 {
        m.put("a", &[b'k', i], &[i; 20]).unwrap();
    }
    clock.advance(Duration::from_secs(5)); // "ttl" is expired but not reaped: still sent
    let mut c = client(m.serve_addr().unwrap(), "b");
    let (h, msgs, f) = read_bootstrap(&mut c).await;

    assert_eq!(h.snapshot_seq, m.last_seq());
    assert_eq!(h.snapshot_crc32c, m.entries_since(m.last_seq() - 1).unwrap().next().unwrap().ok().map(|e| e.crc32c));
    assert_eq!(h.cf_specs.iter().map(|c| c.name.as_str()).collect::<Vec<_>>(), vec!["a", "b-empty"]);
    assert_eq!(h.identity, Some(identity_to_pb(&m.identity())));
    assert_eq!(h.lease_id.len(), 16);
    assert_eq!(h.lease_ttl_ms, 15 * 60 * 1000);
    assert!(msgs.len() > 1, "small chunk size splits the CF");
    assert!(msgs.iter().all(|r| r.cf == "a" && !r.records.is_empty()));

    let mut d = Digest::new();
    for spec in &h.cf_specs {
        d.cf_marker(&spec.name, spec.create_seq);
        for r in msgs.iter().filter(|r| r.cf == spec.name) {
            for rec in &r.records {
                d.record(&rec.key, &rec.envelope);
            }
        }
    }
    let (count, digest) = d.finish_bootstrap(h.snapshot_seq);
    assert_eq!((f.record_count, f.digest.as_slice()), (13, digest.as_slice()));
    assert_eq!(count, 13);
    let keys: Vec<Vec<u8>> = msgs.iter().flat_map(|r| r.records.iter().map(|x| x.key.clone())).collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]), "strictly ascending");
    let ttl = msgs.iter().flat_map(|r| &r.records).find(|x| x.key == b"ttl").expect("expired value is sent");
    assert_eq!(envelope::decode(&ttl.envelope).unwrap().value, b"gone");
}

#[tokio::test]
async fn bootstrap_lease_holds_retention_until_tail_presents_it() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let (h, _, _) = read_bootstrap(&mut c).await;
    let s = h.snapshot_seq;
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    eventually("retention to trim up to the lease", || m.first_retained_seq() == s + 1).await;
    tokio::time::sleep(Duration::from_millis(100)).await; // five more retention rounds
    assert_eq!(m.first_retained_seq(), s + 1, "the lease keeps S + 1");

    let mut req = tail_req(&m.identity(), s + 1);
    req.bootstrap_lease = Some(h.lease_id.clone());
    let mut stream = c.tail_start(req).await.unwrap().into_inner();
    assert_eq!(next_batch(&mut stream).await[0].seq, s + 1);
    eventually("retention after the lease is released", || m.first_retained_seq() == m.last_seq()).await;
}

#[tokio::test]
async fn an_unclaimed_lease_expires() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
        cfg.serve.as_mut().unwrap().limits.bootstrap_lease_ttl = Duration::from_millis(1500);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (h, _, _) = read_bootstrap(&mut client(m.serve_addr().unwrap(), "b")).await;
    for i in 0..3u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    // Held by the unclaimed lease first, then released when it expires (1 s sweep).
    eventually("retention to stop at the lease", || m.first_retained_seq() == h.snapshot_seq + 1).await;
    eventually("lease expiry", || m.first_retained_seq() == m.last_seq()).await;
}

#[tokio::test]
async fn concurrent_bootstraps_are_limited() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let limits = &mut cfg.serve.as_mut().unwrap().limits;
        limits.max_concurrent_bootstraps = 1;
        limits.bootstrap_chunk_bytes = 1;
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    // 32 MiB, one record per message: far more than the channel, socket and HTTP/2 buffers
    // hold, so the unread first bootstrap keeps its producer, and its permit, busy.
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    let mut c = client(m.serve_addr().unwrap(), "b");
    let _held = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap().into_inner();
    let err = client(m.serve_addr().unwrap(), "c")
        .bootstrap(pb::BootstrapRequest { client_id: "c".into() })
        .await
        .unwrap_err();
    assert_eq!((err.code(), reason_of(&err)), (Code::ResourceExhausted, Some(pb::Reason::Busy)));
}

/// Clients that stop reading must not keep the server's producers, and so the database,
/// alive: shutdown ends them and the directory can be reopened at once.
#[tokio::test]
async fn shutdown_with_unread_streams_releases_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    // 32 MiB of data: far more than socket and HTTP/2 buffers, so both producers block.
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    let mut c = client(m.serve_addr().unwrap(), "b");
    let mut tail = c.tail_start(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    assert!(within("the first tail message", tail.message()).await.unwrap().is_some());
    let mut boot = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap().into_inner();
    let first = within("the bootstrap header", boot.message()).await.unwrap().unwrap();
    assert!(matches!(first.kind, Some(Chunk::Header(_))));
    tokio::time::timeout(Duration::from_secs(20), m.shutdown()).await.expect("shutdown hangs").unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    assert_eq!(m.last_seq(), 129);
}

// ---------------- Fetch batching (PROTOCOL.md §7.5) ----------------

fn fetch_req(id: &DbIdentity, from_seq: u64, min_batch_bytes: u32, max_wait_ms: u32, max_batch_bytes: u32) -> pb::TailRequest {
    pb::TailRequest { min_batch_bytes, max_wait_ms, max_batch_bytes, ..tail_req(id, from_seq) }
}

fn batch_bytes(batch: &[LogEntry]) -> usize {
    batch.iter().map(LogEntry::encoded_len).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_time_mode_delivers_each_entry_promptly_one_batch_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_secs(1)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(tail_req(&m.identity(), 2)).await.unwrap().into_inner();
    for i in 0..5u8 {
        let seq = m.put("c", &[i], b"v").unwrap();
        let t0 = Instant::now(); // after the fsynced put returned
        let batch = next_batch(&mut s).await;
        let latency = t0.elapsed();
        assert_eq!(batch.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![seq], "one entry per batch");
        // Real time: nothing waits for more data (the generous bound only guards against a
        // regression to the 1 s heartbeat or a max_wait; it is not a latency benchmark).
        assert!(latency < Duration::from_millis(900), "entry {seq} took {latency:?}");
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_writes_wait_for_max_wait_and_arrive_together() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_secs(3)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let req = fetch_req(&m.identity(), 2, 1 << 20, 1500, 0);
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(req).await.unwrap().into_inner();
    m.put("c", &[0], b"v").unwrap();
    let t0 = Instant::now(); // entry 2 became available no later than this
    for i in 1..3u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(batch.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![2, 3, 4], "one batch");
    assert!(waited >= Duration::from_millis(1000), "sent before max_wait (1500 ms): {waited:?}");
    assert!(waited < Duration::from_secs(6), "sent long after max_wait: {waited:?}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reaching_min_batch_bytes_sends_before_max_wait() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_secs(30)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", &[0], &[7; 100]).unwrap();
    let one = m.entries_since(1).unwrap().next().unwrap().unwrap().encoded_len();
    let req = fetch_req(&m.identity(), 3, u32::try_from(3 * one).unwrap(), 20_000, 0);
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(req).await.unwrap().into_inner();
    for i in 1..=3u8 {
        m.put("c", &[i], &[7; 100]).unwrap();
    }
    let t0 = Instant::now(); // min_batch_bytes is reached no later than this
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(batch.iter().map(|e| e.seq).collect::<Vec<_>>(), vec![3, 4, 5]);
    assert!(batch_bytes(&batch) >= 3 * one);
    assert!(waited < Duration::from_secs(5), "waited {waited:?} (max_wait is 20 s) although min_batch_bytes was reached");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_up_is_split_at_max_batch_bytes_without_gaps() {
    let dir = tempfile::tempdir().unwrap();
    // The server caps the client's 1 MiB request at 64 KiB.
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.serve.as_mut().unwrap().limits.max_tail_batch_bytes = 64 << 10;
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..1000u32 {
        m.put("c", &i.to_be_bytes(), &[1; 1024]).unwrap();
    }
    // min_batch_bytes is huge and max_wait long: catch-up must still not wait.
    let req = fetch_req(&m.identity(), 1, u32::MAX, 100, 1 << 20);
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(req).await.unwrap().into_inner();
    let mut batches = Vec::new();
    let mut seqs = Vec::new();
    while seqs.len() < 1001 {
        let b = next_batch(&mut s).await;
        assert!(batch_bytes(&b) <= 64 << 10, "a batch of {} bytes exceeds the cap", batch_bytes(&b));
        seqs.extend(b.iter().map(|e| e.seq));
        batches.push(b);
    }
    assert_eq!(seqs, (1..=1001).collect::<Vec<_>>(), "consecutive, no gaps or duplicates");
    assert!(batches.len() >= 16, "1 MiB of backlog needs at least 16 batches of 64 KiB, got {}", batches.len());
    // Catch-up is not paced by max_wait: with 100 ms per batch it would take over 1.6 s.
    // (Each `next_batch` is bounded at 10 s; the shape assertions above carry the test.)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_larger_than_max_batch_bytes_is_sent_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"small-1", b"v").unwrap();
    let big = m.put("c", b"big", &[9; 4096]).unwrap();
    m.put("c", b"small-2", b"v").unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(fetch_req(&m.identity(), 2, 0, 0, 1024)).await.unwrap().into_inner();
    let mut got = Vec::new();
    while got.iter().map(Vec::len).sum::<usize>() < 3 {
        got.push(next_batch(&mut s).await);
    }
    let seqs: Vec<Vec<u64>> = got.iter().map(|b| b.iter().map(|e| e.seq).collect()).collect();
    assert_eq!(seqs, vec![vec![2], vec![big], vec![4]]);
    assert!(batch_bytes(&got[1]) > 1024);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_wait_is_clamped_to_the_heartbeat() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_millis(200)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let req = fetch_req(&m.identity(), 2, 1 << 20, 30_000, 0);
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(req).await.unwrap().into_inner();
    m.put("c", b"k", b"v").unwrap();
    let t0 = Instant::now();
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(batch.len(), 1);
    assert!(waited >= Duration::from_millis(100), "sent before the clamped max_wait: {waited:?}");
    assert!(waited < Duration::from_secs(5), "max_wait (30 s) was not clamped to the 200 ms heartbeat: {waited:?}");
}

/// Large log: both producers block on a client that does not read.
async fn big_log(dir: &std::path::Path) -> crate::Master {
    let (m, _) = serving_master(dir, |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    m
}

/// A reset while the channel is full must still reach the client as a status, not a clean EOF.
#[tokio::test]
async fn tail_reset_under_backpressure_ends_with_upstream_reset() {
    let dir = tempfile::tempdir().unwrap();
    let m = big_log(dir.path()).await;
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(tail_req(&m.identity(), 1)).await.unwrap().into_inner();
    assert!(within("the first tail message", s.message()).await.unwrap().is_some());
    tokio::time::sleep(Duration::from_millis(300)).await; // the producer is blocked, channel full
    m.bump_epoch().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let err = stream_error(&mut s).await;
    assert_eq!((err.code(), reason_of(&err)), (Code::Unavailable, Some(pb::Reason::UpstreamReset)));
}

#[tokio::test]
async fn bootstrap_reset_under_backpressure_ends_with_upstream_reset() {
    let dir = tempfile::tempdir().unwrap();
    let m = big_log(dir.path()).await;
    let mut s = client(m.serve_addr().unwrap(), "b")
        .bootstrap(pb::BootstrapRequest { client_id: "b".into() })
        .await
        .unwrap()
        .into_inner();
    let first = within("the bootstrap header", s.message()).await.unwrap().unwrap();
    assert!(matches!(first.kind, Some(Chunk::Header(_))));
    tokio::time::sleep(Duration::from_millis(300)).await; // the job is blocked, channel full
    m.bump_epoch().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let err = stream_error(&mut s).await;
    assert_eq!((err.code(), reason_of(&err)), (Code::Unavailable, Some(pb::Reason::UpstreamReset)));
}

/// A batch never exceeds `max_batch_bytes` on the wire, per-entry framing included.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_fit_max_batch_bytes_including_framing() {
    use prost::Message;
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..200u32 {
        m.put("c", &i.to_be_bytes(), &[1; 60]).unwrap();
    }
    let mut s = client(m.serve_addr().unwrap(), "b").tail_start(fetch_req(&m.identity(), 1, 0, 0, 1000)).await.unwrap().into_inner();
    let mut seen = 0;
    while seen < 201 {
        let batch = next_batch(&mut s).await;
        let wire = pb::EntryBatch { entries: batch.iter().cloned().map(crate::net::convert::entry_to_pb).collect() }.encoded_len();
        assert!(wire <= 1000, "batch of {} entries is {wire} bytes on the wire", batch.len());
        seen += batch.len();
    }
}

// ---------------- Acks and replica status (addendum §1) ----------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_drive_replica_status_and_in_sync() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..3u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let mut c = client(m.serve_addr().unwrap(), "b");
    let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
    let batch = next_batch(&mut s).await;
    assert_eq!(batch.last().unwrap().seq, 4);
    eventually("the replica is registered", || m.replicas().len() == 1).await;
    let r = &m.replicas()[0];
    assert_eq!((r.node_id.as_str(), r.sent_seq, r.applied_seq, r.lag_entries), ("b", 4, 0, 4));
    assert!(!r.in_sync, "not caught up yet");
    assert!(r.lag_ms.is_some());
    assert_eq!(r.epoch, 1);
    assert_eq!(r.fetch.max_batch_bytes, 1 << 20);

    acks.send(ack(2)).await.unwrap();
    eventually("the partial ack", || m.replicas()[0].applied_seq == 2).await;
    assert_eq!(m.replicas()[0].lag_entries, 2);
    assert!(!m.replicas()[0].in_sync);
    acks.send(ack(4)).await.unwrap();
    eventually("caught up and in sync", || m.replicas()[0].in_sync).await;
    let r = &m.replicas()[0];
    assert_eq!((r.applied_seq, r.lag_entries, r.lag_ms), (4, 0, Some(0)));

    // A new write is sent but not acknowledged: lag grows, the replica stays in sync for
    // replica_lag_time_max.
    m.put("c", b"k", b"v").unwrap();
    assert_eq!(next_batch(&mut s).await[0].seq, 5);
    eventually("sent", || m.replicas()[0].sent_seq == 5).await;
    let r = &m.replicas()[0];
    assert_eq!((r.applied_seq, r.lag_entries), (4, 1));
    assert!(r.in_sync);

    drop(acks);
    drop(s);
    eventually("the closed stream leaves the registry", || m.replicas().is_empty()).await;
}

#[tokio::test]
async fn ack_violations_end_the_stream_with_invalid_argument() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");

    // Above the last sent seq.
    let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 3)).await.unwrap();
    acks.send(ack(3)).await.unwrap();
    let e = stream_error(&mut s).await;
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");

    // Backwards.
    let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
    next_batch(&mut s).await;
    eventually("sent", || m.replicas().iter().any(|r| r.sent_seq == 2)).await;
    acks.send(ack(2)).await.unwrap();
    acks.send(ack(1)).await.unwrap();
    assert_eq!(stream_error(&mut s).await.code(), Code::InvalidArgument);

    // A second `start`.
    let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
    acks.send(pb::TailClientMessage { kind: Some(pb::tail_client_message::Kind::Start(tail_req(&m.identity(), 1))) })
        .await
        .unwrap();
    assert_eq!(stream_error(&mut s).await.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn the_first_tail_message_must_be_start() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let mut c = client(m.serve_addr().unwrap(), "b");
    let e = c.tail(tokio_stream::iter([super::test_client::ack(0)])).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    let e = c.tail(tokio_stream::iter(Vec::<pb::TailClientMessage>::new())).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "an empty request stream");
}

/// A reset while a bootstrap client has stopped reading: the job releases its snapshot and its
/// bootstrap slot at once, and only then waits to deliver `UPSTREAM_RESET`.
#[tokio::test]
async fn a_reset_releases_a_stalled_bootstraps_snapshot_and_slot() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.serve.as_mut().unwrap().limits.max_concurrent_bootstraps = 1;
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    // 32 MiB: far more than the channel and transport buffers hold.
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    let src = m.log_source();
    let db_refs = Arc::strong_count(src.store().db());
    let mut stalled = client(m.serve_addr().unwrap(), "b");
    let mut boot = stalled.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap().into_inner();
    assert!(matches!(within("the header", boot.message()).await.unwrap().unwrap().kind, Some(Chunk::Header(_))));
    assert!(Arc::strong_count(src.store().db()) > db_refs, "the job holds a snapshot");
    m.bump_epoch().unwrap();
    eventually("the snapshot is released", || Arc::strong_count(src.store().db()) == db_refs).await;
    let mut other = client(m.serve_addr().unwrap(), "c");
    let next = other.bootstrap(pb::BootstrapRequest { client_id: "c".into() }).await;
    assert!(next.is_ok(), "the slot is free again: {:?}", next.err());
}
```

In `src/master/source.rs` (test module), import `TailExt` and replace both `.tail(req)` calls with `.tail_start(req)`:

```rust
    use crate::server::test_client::{client, next_batch, serving_master, tail_req, within, TailExt};
```

Run: `cargo test --lib server`
Expected: FAIL to compile: `Svc` does not implement `status`, and `tail` has the wrong request type (`expected Request<Streaming<TailClientMessage>>`).

- [ ] **Step 4: Configuration, method and `LogSource` additions**

In `src/config.rs`, in `ServeLimits`, after `pub max_tail_batch_bytes: usize,` add:

```rust
    /// A replica counts as in sync only if it was fully caught up within this long.
    pub replica_lag_time_max: Duration,
```

in its `Default`, after `max_tail_batch_bytes: 4 << 20,` add:

```rust
            replica_lag_time_max: Duration::from_secs(10),
```

and in `ServeConfig::validate`, replace:

```rust
        if l.bootstrap_lease_ttl.is_zero() || l.verify_session_timeout.is_zero() {
```

with:

```rust
        if l.bootstrap_lease_ttl.is_zero() || l.verify_session_timeout.is_zero() || l.replica_lag_time_max.is_zero() {
```

In `src/auth.rs`, add the variant to `Method` (after `Verify,`):

```rust
    /// The read-only `Status` RPC (operator tooling).
    Status,
```

and in `MtlsSubjectAllowList::allow_all_methods` replace the method list with:

```rust
        self.allow(subject, &[Method::Handshake, Method::Tail, Method::Bootstrap, Method::Verify, Method::Status])
```

Replace `src/source.rs` with (two methods with defaults; the slave overrides them in Task 10):

```rust
//! What the replication server needs from the node it runs on. Implemented by the master
//! and by slaves that serve downstream replicas (DESIGN.md §11).

use std::time::Duration;

use tokio::sync::watch;

use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

pub(crate) trait LogSource: Send + Sync + 'static {
    /// This node's configured identifier (`server_id` in the handshake).
    fn node_id(&self) -> &str;
    fn role(&self) -> Role;
    /// False while this node is bootstrapping itself (it has no consistent state).
    fn is_ready(&self) -> bool;
    /// Origin identity and epoch history, read together.
    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)>;
    fn store(&self) -> &Store;
    /// The highest master seq this node knows of. On the master, its `last_seq`.
    fn origin_last_seq(&self) -> Seq;
    /// Node ids between this node and the master, closest first. Empty on the master.
    fn upstream_chain(&self) -> Vec<String>;
    /// The master's `max_entry_bytes`, forwarded down the chain.
    fn max_entry_bytes(&self) -> u64;
    /// Interval of Tail heartbeats this node sends.
    fn heartbeat(&self) -> Duration;
    /// Receives this node's `last_seq` after each commit.
    fn subscribe(&self) -> watch::Receiver<Seq>;
    /// Bumped whenever every served stream must end with `UPSTREAM_RESET` (an epoch bump on
    /// the master, an epoch adoption or a bootstrap on a slave).
    fn resets(&self) -> watch::Receiver<u64>;
    /// `ready`, `bootstrapping` or `halted:<Reason>` (the `Status` RPC).
    fn node_state(&self) -> String {
        "ready".into()
    }
    /// The upstream address; `None` on the master.
    fn upstream(&self) -> Option<String> {
        None
    }
}
```

- [ ] **Step 5: The `Status` report**

Create `src/admin.rs` (Task 11 completes it with comparison and promotion tooling):

```rust
//! Operator tooling for failover (addendum §4). This task adds the node report served by the
//! `Status` RPC; comparing nodes and choosing a promotion candidate follow with promotion.

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
```

Create `src/server/status.rs`:

```rust
//! The read-only `Status` RPC (addendum §4) and the in-process equivalent.

use std::time::Duration;

use super::replicas::ReplicaRegistry;
use crate::admin::{NodeInfo, ReplicaSummary};
use crate::net::convert::{epochs_to_pb, identity_to_pb};
use crate::pb;
use crate::source::LogSource;
use crate::types::Role;

/// What this node reports about itself.
pub(crate) fn node_info(src: &dyn LogSource, replicas: &ReplicaRegistry, lag_max: Duration) -> NodeInfo {
    let ident = src.identity();
    let pos = src.store().position();
    let last_ts_ms = src
        .store()
        .read_entries(pos.last_seq, 1, usize::MAX)
        .ok()
        .and_then(|v| v.first().filter(|e| e.seq == pos.last_seq).map(|e| e.ts_ms))
        .unwrap_or(0);
    let epoch = ident.as_ref().map_or(0, |(id, _)| id.epoch);
    NodeInfo {
        node_id: src.node_id().to_string(),
        role: src.role(),
        identity: ident.as_ref().map(|(id, _)| *id),
        epoch_history: ident.map(|(_, h)| h).unwrap_or_default(),
        last_seq: pos.last_seq,
        last_entry_crc: pos.last_entry_crc,
        last_ts_ms,
        node_state: src.node_state(),
        upstream: src.upstream(),
        replicas: replicas
            .summary(epoch, lag_max)
            .into_iter()
            .map(|(node_id, applied_seq, in_sync)| ReplicaSummary { node_id, applied_seq, in_sync })
            .collect(),
    }
}

pub(crate) fn to_pb(i: &NodeInfo) -> pb::StatusResponse {
    pb::StatusResponse {
        node_id: i.node_id.clone(),
        role: match i.role {
            Role::Master => "master".into(),
            Role::Slave => "slave".into(),
        },
        identity: i.identity.as_ref().map(identity_to_pb),
        epoch_history: epochs_to_pb(&i.epoch_history),
        last_seq: i.last_seq,
        last_entry_crc: i.last_entry_crc,
        last_ts_ms: i.last_ts_ms,
        node_state: i.node_state.clone(),
        upstream: i.upstream.clone().unwrap_or_default(),
        replicas: i
            .replicas
            .iter()
            .map(|r| pb::ReplicaSummary { node_id: r.node_id.clone(), applied_seq: r.applied_seq, in_sync: r.in_sync })
            .collect(),
    }
}
```

- [ ] **Step 6: Bidirectional Tail with the ack reader**

Replace `src/server/tail.rs` with:

```rust
//! Tail: validation (PROTOCOL.md §7.2) and the per-stream producer (§7.3).

use std::collections::VecDeque;
use std::sync::Arc;
use std::time::Duration;

use tokio::sync::{mpsc, oneshot, watch};
use tokio::time::Instant;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use super::replicas::{FetchSettings, NewReplica, ReplicaCell, ReplicaHandle};
use super::{call_info, store_status, Lease, ServerState, TailStream};
use crate::auth::Method;
use crate::clock::{Clock, SystemClock};
use crate::error::Error;
use crate::failpoint;
use crate::net::convert::{entry_to_pb, identity_from_pb};
use crate::net::{internal_status, invalid_argument, reason_status};
use crate::pb;
use crate::pb::tail_client_message::Kind as ClientKind;
use crate::pb::tail_message::Kind;
use crate::source::LogSource;
use crate::types::{LogEntry, Role, Seq};

/// Entries read from the changelog per blocking read.
pub(crate) const READ_BATCH: usize = 64;
/// Messages buffered per stream before back-pressure applies.
const CHANNEL: usize = 16;

/// How long the server waits for the client's `start` message.
const START_TIMEOUT: Duration = Duration::from_secs(10);

pub(super) async fn tail(
    state: &Arc<ServerState>,
    req: Request<tonic::Streaming<pb::TailClientMessage>>,
) -> Result<Response<TailStream>, Status> {
    let call = call_info(&req, Method::Tail)?;
    let src = state.authorize(&call, Method::Tail).await?;
    let mut inbound = req.into_inner();
    let r = match tokio::time::timeout(START_TIMEOUT, inbound.message()).await {
        Ok(Ok(Some(pb::TailClientMessage { kind: Some(ClientKind::Start(r)) }))) => r,
        Ok(Ok(_)) => return Err(invalid_argument("the first Tail message must be `start`")),
        Ok(Err(status)) => return Err(status),
        Err(_) => return Err(invalid_argument("no `start` message within 10 s")),
    };
    call.check_client_id(&r.client_id)?;
    // Subscribe before validating, so a reset after validation is never missed.
    let mut resets = src.resets();
    resets.borrow_and_update();
    validate(&*src, &r)?;
    state.expire_leases(std::time::Instant::now());
    let lease = r.bootstrap_lease.as_deref().and_then(|id| state.take_lease(id, &r.client_id, r.from_seq));
    let (tx, rx) = mpsc::channel(CHANNEL);
    let fetch = Fetch::clamp(&r, src.heartbeat(), state.cfg.limits.max_tail_batch_bytes);
    // `validate` checked the identity, so this is the epoch the replica follows.
    let epoch = r.identity.as_ref().map_or(0, |id| id.epoch);
    let replica = state.replicas.open(NewReplica {
        node_id: call.peer.peer_id.clone(),
        remote_addr: call.peer.remote_addr,
        peer_verified: call.peer.peer_id_verified,
        epoch,
        has_up_to: r.from_seq - 1,
        fetch: fetch.settings(),
    });
    let (violation_tx, violation_rx) = oneshot::channel();
    let (done_tx, done_rx) = oneshot::channel();
    tokio::spawn(read_acks(inbound, replica.cell().clone(), src.subscribe(), violation_tx, done_rx));
    let stream = Stream { replica, violation: Some(violation_rx), _done: done_tx };
    let out = Out { tx, shutdown: state.shutdown.clone(), resets };
    tokio::spawn(produce(src, out, r.from_seq, fetch, lease, stream));
    Ok(Response::new(ReceiverStream::new(rx)))
}

fn validate(src: &dyn LogSource, r: &pb::TailRequest) -> Result<(), Status> {
    let ident = src.identity().filter(|_| src.is_ready());
    let Some((server_id, _)) = ident else {
        return Err(reason_status(pb::Reason::NotReady, "server is bootstrapping"));
    };
    let want = r.identity.as_ref().ok_or_else(|| crate::net::invalid_argument("identity is required"))?;
    let want = identity_from_pb(want).map_err(|e| crate::net::invalid_argument(e.to_string()))?;
    if want != server_id {
        return Err(reason_status(pb::Reason::IdentityChanged, format!("server identity is {server_id:?}")));
    }
    if r.from_seq == 0 {
        return Err(crate::net::invalid_argument("from_seq starts at 1"));
    }
    let store = src.store();
    let last = store.position().last_seq;
    if src.role() == Role::Master && r.from_seq > last + 1 {
        return Err(reason_status(pb::Reason::Diverged, format!("from_seq {} is past master last_seq {last}", r.from_seq)));
    }
    if let Some(prev) = r.prev_crc32c {
        if let Some(have) = store.entry_crc(r.from_seq - 1).map_err(store_status)? {
            if have != prev {
                return Err(reason_status(pb::Reason::Diverged, format!("entry {} differs", r.from_seq - 1)));
            }
        }
    }
    let first = store.first_retained_seq();
    if r.from_seq < first {
        return Err(reason_status(pb::Reason::LogGap, format!("from_seq {} < first retained {first}", r.from_seq)));
    }
    Ok(())
}

fn heartbeat(src: &dyn LogSource) -> pb::TailMessage {
    pb::TailMessage {
        kind: Some(Kind::Heartbeat(pb::Heartbeat {
            upstream_last_seq: src.store().position().last_seq,
            origin_last_seq: src.origin_last_seq(),
            ts_ms: SystemClock.now_ms(),
        })),
    }
}

type Tx = mpsc::Sender<Result<pb::TailMessage, Status>>;

/// The producer's ties to its stream: its registry entry (removed when the producer ends),
/// a protocol violation found by the ack reader, and `_done`, whose drop stops the reader.
struct Stream {
    replica: ReplicaHandle,
    /// `None` once the ack reader has finished without a violation.
    violation: Option<oneshot::Receiver<Status>>,
    _done: oneshot::Sender<()>,
}

impl Stream {
    /// A violation the ack reader reported, if any (non-blocking).
    fn violation(&mut self) -> Option<Status> {
        let rx = self.violation.as_mut()?;
        match rx.try_recv() {
            Ok(status) => Some(status),
            Err(oneshot::error::TryRecvError::Empty) => None,
            Err(oneshot::error::TryRecvError::Closed) => {
                self.violation = None;
                None
            }
        }
    }
}

/// Reads the client's acks (addendum §1) until the stream ends. It holds no reference to
/// the node, only the stream's registry cell and the node's seq channel.
async fn read_acks(
    mut inbound: tonic::Streaming<pb::TailClientMessage>,
    cell: Arc<ReplicaCell>,
    seqs: watch::Receiver<Seq>,
    violation: oneshot::Sender<Status>,
    mut done: oneshot::Receiver<()>,
) {
    loop {
        let msg = tokio::select! {
            m = inbound.message() => m,
            _ = &mut done => return,
        };
        let problem = match msg {
            Ok(Some(pb::TailClientMessage { kind: Some(ClientKind::Ack(a)) })) => {
                let node_last = *seqs.borrow();
                match cell.ack(a.applied_seq, node_last) {
                    Ok(()) => continue,
                    Err(e) => e,
                }
            }
            Ok(Some(_)) => "after `start`, only `ack` messages are allowed".to_string(),
            // The client half-closed or went away; the producer notices on its own.
            Ok(None) | Err(_) => return,
        };
        let _ = violation.send(invalid_argument(problem));
        return;
    }
}

/// The sending side of one stream. Every send also watches shutdown and resets, so a
/// client that stops reading can never keep the producer (and the node's database) alive.
struct Out {
    tx: Tx,
    shutdown: watch::Receiver<bool>,
    resets: watch::Receiver<u64>,
}

enum Sent {
    Ok,
    /// The client went away.
    Gone,
    /// The stream must end with this status.
    End(Status),
}

impl Out {
    async fn send(&mut self, msg: pb::TailMessage) -> Sent {
        tokio::select! {
            biased;
            _ = super::stopped(&mut self.shutdown) => Sent::End(shutting_down()),
            _ = self.resets.changed() => Sent::End(upstream_reset()),
            r = self.tx.send(Ok(msg)) => if r.is_ok() { Sent::Ok } else { Sent::Gone },
        }
    }

    async fn end(&mut self, status: Status) {
        super::end_stream(&self.tx, status, &mut self.shutdown, &mut self.resets).await;
    }
}

fn shutting_down() -> Status {
    reason_status(pb::Reason::ShuttingDown, "server is shutting down")
}

fn upstream_reset() -> Status {
    reason_status(pb::Reason::UpstreamReset, "server state was replaced")
}

/// Fetch batching parameters of one stream, clamped to the server's limits (PROTOCOL.md §7.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Fetch {
    pub min_bytes: usize,
    pub max_wait: Duration,
    pub max_bytes: usize,
}

/// `max_batch_bytes = 0` means this.
pub(crate) const DEFAULT_MAX_BATCH_BYTES: usize = 1 << 20;

impl Fetch {
    fn settings(&self) -> FetchSettings {
        FetchSettings {
            min_batch_bytes: self.min_bytes as u64,
            max_wait: self.max_wait,
            max_batch_bytes: self.max_bytes as u64,
        }
    }

    pub(crate) fn clamp(r: &pb::TailRequest, heartbeat: Duration, max_tail_batch_bytes: usize) -> Fetch {
        let max_bytes = match r.max_batch_bytes {
            0 => DEFAULT_MAX_BATCH_BYTES,
            n => n as usize,
        };
        Fetch {
            min_bytes: r.min_batch_bytes as usize,
            max_wait: Duration::from_millis(u64::from(r.max_wait_ms)).min(heartbeat),
            max_bytes: max_bytes.min(max_tail_batch_bytes).max(1),
        }
    }
}

/// Size of an entry inside an `EntryBatch`: its encoded length plus its framing (field tag
/// and length varint), so a batch never exceeds `max_batch_bytes` on the wire.
fn framed_len(entry_len: usize) -> usize {
    let mut varint = 1;
    let mut n = entry_len >> 7;
    while n > 0 {
        varint += 1;
        n >>= 7;
    }
    entry_len + 1 + varint
}

/// Entries read but not yet sent.
struct Pending {
    entries: Vec<pb::LogEntry>,
    bytes: usize,
    /// When the oldest pending entry became available to this stream.
    since: Option<Instant>,
}

impl Pending {
    fn take(&mut self) -> pb::TailMessage {
        self.bytes = 0;
        self.since = None;
        pb::TailMessage { kind: Some(Kind::Batch(pb::EntryBatch { entries: std::mem::take(&mut self.entries) })) }
    }
}

/// Streams entries from `next` in order, in batches (PROTOCOL.md §7.5), with heartbeats,
/// until the client goes away or the stream must end (reset, shutdown, gap, corruption).
async fn produce(
    src: Arc<dyn LogSource>,
    mut out: Out,
    mut next: Seq,
    fetch: Fetch,
    mut lease: Option<Lease>,
    mut stream: Stream,
) {
    let hb: Duration = src.heartbeat();
    let mut seqs = src.subscribe();
    let mut ahead: VecDeque<LogEntry> = VecDeque::new();
    let mut pending = Pending { entries: Vec::new(), bytes: 0, since: None };
    // The first heartbeat goes out at once, so the client learns positions immediately.
    let mut next_hb = Instant::now();
    macro_rules! send {
        ($msg:expr) => {
            match out.send($msg).await {
                Sent::Ok => {}
                Sent::Gone => return,
                Sent::End(status) => return out.end(status).await,
            }
        };
    }
    // The registry learns what was sent before the client can see it, so an ack is never
    // ahead of `sent_seq`.
    macro_rules! send_pending {
        () => {{
            if let Some(last) = pending.entries.last() {
                stream.replica.cell().note_sent(last.seq);
            }
            send!(pending.take());
        }};
    }
    macro_rules! flush {
        () => {{
            send_pending!();
            if Instant::now() >= next_hb {
                send!(heartbeat(&*src));
                next_hb = Instant::now() + hb;
            }
        }};
    }
    loop {
        if out.resets.has_changed().unwrap_or(true) {
            // Mark the reset seen, so `end_stream` waits for room instead of dropping the status.
            out.resets.borrow_and_update();
            return out.end(upstream_reset()).await;
        }
        if *out.shutdown.borrow() {
            return out.end(shutting_down()).await;
        }
        if let Some(status) = stream.violation() {
            return out.end(status).await;
        }
        // Read ahead everything that is available (in blocks of READ_BATCH).
        if ahead.is_empty() && next <= src.store().position().last_seq {
            let s = src.clone();
            let read = tokio::task::spawn_blocking(move || s.store().read_entries(next, READ_BATCH, fetch.max_bytes)).await;
            let failure = match read {
                Ok(Ok(entries)) => {
                    // The lease only protects `S + 1`: release it once that entry is read.
                    if lease.as_ref().is_some_and(|l| entries.last().is_some_and(|e| e.seq > l.snapshot_seq)) {
                        lease = None;
                        tracing::debug!("bootstrap lease released: the entries after the snapshot are read");
                    }
                    ahead.extend(entries);
                    None
                }
                Ok(Err(e @ Error::Gap { .. })) => Some(reason_status(pb::Reason::LogGap, e.to_string())),
                Ok(Err(e)) => Some(store_status(e)),
                Err(e) => Some(internal_status(format!("log read failed: {e}"))),
            };
            if let Some(status) = failure {
                // Entries already read are valid: deliver them before ending the stream.
                if !pending.entries.is_empty() {
                    send_pending!();
                }
                return out.end(status).await;
            }
        }
        // Move available entries into the pending batch; flush when it is full or big enough.
        if let Some(entry) = ahead.pop_front() {
            if let Err(e) = entry.verify_crc() {
                return out.end(store_status(e)).await;
            }
            let size = framed_len(entry.encoded_len());
            if !pending.entries.is_empty() && pending.bytes + size > fetch.max_bytes {
                flush!();
            }
            next = entry.seq + 1;
            pending.since.get_or_insert_with(Instant::now);
            pending.bytes += size;
            pending.entries.push(entry_to_pb(entry));
            if failpoint::fire("server-tail-after-entry") {
                send_pending!();
                return out.end(Status::unavailable("failpoint server-tail-after-entry")).await;
            }
            if pending.bytes >= fetch.max_bytes || (fetch.min_bytes > 0 && pending.bytes >= fetch.min_bytes) {
                flush!();
            }
            continue;
        }
        // Everything available is pending: real-time mode sends it now, otherwise it waits
        // for min_batch_bytes or until the oldest pending entry has waited max_wait.
        let deadline = pending.since.map(|t| t + fetch.max_wait);
        if !pending.entries.is_empty() && (fetch.min_bytes == 0 || deadline.is_some_and(|d| Instant::now() >= d)) {
            flush!();
            continue;
        }
        let wake = match deadline {
            Some(d) if d < next_hb => d,
            _ => next_hb,
        };
        tokio::select! {
            r = seqs.changed() => if r.is_err() { return },
            _ = out.resets.changed() => return out.end(upstream_reset()).await,
            r = out.shutdown.changed() => if r.is_err() { return },
            _ = out.tx.closed() => return,
            v = async { stream.violation.as_mut().expect("guarded by the condition").await }, if stream.violation.is_some() => {
                match v {
                    Ok(status) => return out.end(status).await,
                    Err(_) => stream.violation = None,
                }
            }
            _ = tokio::time::sleep_until(wake) => {
                if Instant::now() >= next_hb {
                    send!(heartbeat(&*src));
                    next_hb = Instant::now() + hb;
                }
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn req(min: u32, wait_ms: u32, max: u32) -> pb::TailRequest {
        pb::TailRequest { min_batch_bytes: min, max_wait_ms: wait_ms, max_batch_bytes: max, ..Default::default() }
    }

    #[test]
    fn fetch_parameters_are_clamped_to_server_limits() {
        let hb = Duration::from_millis(500);
        let f = Fetch::clamp(&req(10, 100, 2048), hb, 4 << 20);
        assert_eq!(f, Fetch { min_bytes: 10, max_wait: Duration::from_millis(100), max_bytes: 2048 });
        assert_eq!(Fetch::clamp(&req(0, 60_000, 0), hb, 4 << 20).max_wait, hb, "max_wait <= heartbeat");
        assert_eq!(Fetch::clamp(&req(0, 0, 0), hb, 4 << 20).max_bytes, DEFAULT_MAX_BATCH_BYTES, "0 = 1 MiB");
        assert_eq!(Fetch::clamp(&req(0, 0, u32::MAX), hb, 4 << 20).max_bytes, 4 << 20, "capped");
        assert_eq!(Fetch::clamp(&req(0, 0, 0), hb, 1000).max_bytes, 1000, "default is capped too");
    }

    #[test]
    fn framing_counts_tag_and_length_varint() {
        assert_eq!(framed_len(0), 2);
        assert_eq!(framed_len(127), 127 + 2);
        assert_eq!(framed_len(128), 128 + 3);
        assert_eq!(framed_len(16_383), 16_383 + 3);
        assert_eq!(framed_len(16_384), 16_384 + 4);
    }

    /// A reset between validation and the first poll of the producer is not missed: the
    /// receiver is subscribed (and marked seen) first, as the handler does.
    #[tokio::test]
    async fn a_reset_after_subscription_but_before_streaming_ends_the_stream() {
        use crate::server::test_client::{serving_master, within};
        use crate::types::CfSpec;
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| cfg.serve = None).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let src = m.log_source();
        let mut resets = src.resets();
        resets.borrow_and_update();
        m.bump_epoch().unwrap();
        let (tx, rx) = mpsc::channel(CHANNEL);
        let (_shutdown_tx, shutdown) = watch::channel(false);
        let fetch = Fetch::clamp(&req(0, 0, 0), Duration::from_secs(1), 1 << 20);
        let registry = Arc::new(super::super::replicas::ReplicaRegistry::default());
        let replica = registry.open(NewReplica {
            node_id: "b".into(),
            remote_addr: "127.0.0.1:1".parse().unwrap(),
            peer_verified: false,
            epoch: 1,
            has_up_to: 0,
            fetch: fetch.settings(),
        });
        let (_violation_tx, violation) = oneshot::channel();
        let (done, _done_rx) = oneshot::channel();
        let stream = Stream { replica, violation: Some(violation), _done: done };
        tokio::spawn(produce(src, Out { tx, shutdown, resets }, 1, fetch, None, stream));
        let mut rx = rx;
        let err = within("the stream status", async {
            loop {
                match rx.recv().await {
                    Some(Err(e)) => return e,
                    Some(Ok(_)) => {}
                    None => panic!("stream ended without a status"),
                }
            }
        })
        .await;
        assert_eq!(crate::net::reason_of(&err), Some(pb::Reason::UpstreamReset));
    }
}
```

The first client message must be `start` (10 s); the stream then registers in the replica registry and a separate task reads acks (`read_acks`), checking each against the cell (`ReplicaCell::ack`). A violation is handed to the producer through a oneshot and ends the stream with `INVALID_ARGUMENT`; `send_pending!` records the last seq sent before it sends. `produce` takes the `Out` sender bundle (it had too many arguments for clippy otherwise).

- [ ] **Step 7: Release a bootstrap's snapshot and slot before its final status**

After a reset, Task 5's bootstrap job waited in `end_stream` for a client that might never read again, still holding its `ReadSnapshot` and its permit. Replace `src/server/bootstrap.rs` with (the job now returns its sender and final status; the blocking closure drops the snapshot and the permit, then spawns the final send):

```rust
//! Bootstrap: a consistent snapshot streamed as header, records and footer, with a
//! retention guard kept as a lease until the client's Tail (PROTOCOL.md §8).

use std::sync::Arc;

use tokio::sync::{mpsc, watch, TryAcquireError};
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
    // Subscribe before anything else, so a reset after this point is never missed.
    let mut resets = src.resets();
    resets.borrow_and_update();
    if state.is_shutting_down() {
        return Err(reason_status(pb::Reason::ShuttingDown, "server is shutting down"));
    }
    state.expire_leases(std::time::Instant::now());
    if !src.is_ready() {
        return Err(reason_status(pb::Reason::NotReady, "server is bootstrapping"));
    }
    let permit = state
        .bootstraps
        .clone()
        .try_acquire_owned()
        .map_err(|e| match e {
            TryAcquireError::Closed => reason_status(pb::Reason::ShuttingDown, "server is shutting down"),
            TryAcquireError::NoPermits => reason_status(pb::Reason::Busy, "too many bootstraps in progress"),
        })?;
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
    let (mut stop, mut reset) = (state.shutdown.clone(), job.resets.clone());
    let runtime = job.runtime.clone();
    tokio::task::spawn_blocking(move || {
        let (tx, end) = job.run();
        // The job, and with it the snapshot and (unless leased) the guard, is gone; only then
        // is the permit returned (`ServerHandle::shutdown` waits for all permits). A final
        // status may have to wait for a stalled client: that happens afterwards, holding
        // nothing but the channel, so a reset never pins the snapshot or a bootstrap slot.
        drop(permit);
        if let Some(status) = end {
            reset.borrow_and_update();
            runtime.spawn(async move { super::end_stream(&tx, status, &mut stop, &mut reset).await });
        }
    });
    Ok(Response::new(ReceiverStream::new(rx)))
}

type Tx = mpsc::Sender<Result<pb::BootstrapChunk, Status>>;

struct Job {
    state: Arc<ServerState>,
    snap: ReadSnapshot,
    meta: MetaSnapshot,
    header: pb::BootstrapHeader,
    guard: RetentionGuard,
    lease_id: [u8; 16],
    client_id: String,
    tx: Tx,
    resets: watch::Receiver<u64>,
    runtime: tokio::runtime::Handle,
}

enum Sent {
    Ok,
    /// The client went away.
    Gone,
    /// The stream must end with this status.
    End(Status),
}

fn chunk(kind: Kind) -> Result<pb::BootstrapChunk, Status> {
    Ok(pb::BootstrapChunk { kind: Some(kind) })
}

impl Job {
    /// Runs on a blocking thread and consumes the job: the snapshot and the guard are dropped
    /// when it returns. Returns a sender and the status the stream must still end with, if
    /// any; the caller delivers it after releasing the job's resources.
    fn run(self) -> (Tx, Option<Status>) {
        let Job { state, snap, meta, header, guard, lease_id, client_id, tx, resets, runtime } = self;
        let out = tx.clone();
        // A send also gives up on shutdown or reset, so a client that stops reading cannot
        // keep this job (and the node's database) alive.
        let mut stop = state.shutdown.clone();
        let mut reset = resets.clone();
        let mut send = |m: Result<pb::BootstrapChunk, Status>| {
            runtime.block_on(async {
                tokio::select! {
                    biased;
                    _ = super::stopped(&mut stop) => Sent::End(reason_status(pb::Reason::ShuttingDown, "server is shutting down")),
                    _ = reset.changed() => Sent::End(reason_status(pb::Reason::UpstreamReset, "server state was replaced")),
                    r = tx.send(m) => if r.is_ok() { Sent::Ok } else { Sent::Gone },
                }
            })
        };
        // Sends a message; on a stop the stream ends with its status and the job returns.
        macro_rules! send {
            ($m:expr) => {
                match send($m) {
                    Sent::Ok => {}
                    Sent::Gone => return (out, None),
                    Sent::End(status) => return (out, Some(status)),
                }
            };
        }
        send!(chunk(Kind::Header(header)));
        let chunk_bytes = state.cfg.limits.bootstrap_chunk_bytes;
        let mut digest = Digest::new();
        let ctx = snap.raw_ctx();
        for cf in &meta.cf_specs {
            digest.cf_marker(&cf.spec.name, cf.create_seq);
            let Some(phys) = snap.phys(&cf.spec.name).filter(|p| p.entry == *cf) else {
                return (out, Some(reason_status(pb::Reason::Corrupt, format!("CF {} is missing from the snapshot", cf.spec.name))));
            };
            let iter = match ctx.iter_raw(phys) {
                Ok(iter) => iter,
                Err(e) => {
                    return (out, Some(store_status(e)));
                }
            };
            let mut records = Vec::new();
            let mut size = 0usize;
            for item in iter {
                if state.is_shutting_down() {
                    return (out, Some(reason_status(pb::Reason::ShuttingDown, "server is shutting down")));
                }
                if resets.has_changed().unwrap_or(true) {
                    return (out, Some(reason_status(pb::Reason::UpstreamReset, "server state was replaced")));
                }
                let (key, envelope) = match item {
                    Ok(kv) => kv,
                    Err(e) => {
                        return (out, Some(store_status(e)));
                    }
                };
                digest.record(&key, &envelope);
                size += key.len() + envelope.len();
                records.push(pb::Record { key: key.into_vec(), envelope: envelope.into_vec() });
                if size >= chunk_bytes {
                    let msg = pb::BootstrapRecords { cf: cf.spec.name.clone(), records: std::mem::take(&mut records) };
                    send!(chunk(Kind::Records(msg)));
                    size = 0;
                }
            }
            if !records.is_empty() {
                send!(chunk(Kind::Records(pb::BootstrapRecords { cf: cf.spec.name.clone(), records })));
            }
        }
        let (record_count, digest) = digest.finish_bootstrap(meta.last_seq);
        // The lease exists before the client can see the footer and call Tail.
        state.insert_lease(lease_id, client_id, meta.last_seq, guard);
        let footer = pb::BootstrapFooter { record_count, digest: digest.to_vec() };
        match send(chunk(Kind::Footer(footer))) {
            Sent::Ok => (out, None),
            Sent::Gone => {
                state.remove_lease(&lease_id);
                (out, None)
            }
            Sent::End(status) => {
                state.remove_lease(&lease_id);
                (out, Some(status))
            }
        }
    }
}
```

- [ ] **Step 8: The server: registry, `Status`, new Tail signature**

Replace `src/server/mod.rs` with:

```rust
//! The replication gRPC server (PROTOCOL.md §6–§8), shared by the master and by slaves that
//! serve downstream replicas. It runs over a [`LogSource`] and holds it weakly, so a running
//! server never keeps a closed node's database open.

pub(crate) mod bootstrap;
pub(crate) mod handshake;
pub(crate) mod replicas;
pub(crate) mod status;
mod tail;
#[cfg(test)]
pub(crate) use tail::READ_BATCH;
#[cfg(test)]
pub(crate) mod test_client;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::{mpsc, watch, Semaphore};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::auth::{subject_common_name, AuthError, FailureLimiter, Method, PeerInfo};
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::config::{validate_identifier, ServeConfig};
use crate::error::{Error, Result};
use crate::net::convert::{cf_to_pb, epochs_to_pb, identity_to_pb};
use crate::net::{self, reason_status};
use crate::pb;
use crate::pb::replication_server::{Replication, ReplicationServer};
use crate::source::LogSource;
use crate::store::retention::RetentionGuard;
use replicas::ReplicaRegistry;
use crate::types::{Role, Seq};

/// A bootstrap's retention guard, kept after the footer until the client's Tail presents
/// the lease or the lease expires (PROTOCOL.md §8.4).
pub(crate) struct Lease {
    pub client_id: String,
    pub snapshot_seq: Seq,
    pub expires_at: Instant,
    _guard: RetentionGuard,
}

pub(crate) struct ServerState {
    source: Weak<dyn LogSource>,
    pub(crate) cfg: ServeConfig,
    leases: Mutex<HashMap<[u8; 16], Lease>>,
    bootstraps: Arc<Semaphore>,
    pub(crate) shutdown: watch::Receiver<bool>,
    auth_failures: FailureLimiter,
    /// The node's downstream replicas (shared with the node, which reports and waits on them).
    pub(crate) replicas: Arc<ReplicaRegistry>,
}

impl ServerState {
    fn source(&self) -> Result<Arc<dyn LogSource>, Status> {
        self.source.upgrade().ok_or_else(|| reason_status(pb::Reason::ShuttingDown, "node is shutting down"))
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }

    fn leases(&self) -> std::sync::MutexGuard<'_, HashMap<[u8; 16], Lease>> {
        self.leases.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn insert_lease(&self, id: [u8; 16], client_id: String, snapshot_seq: Seq, guard: RetentionGuard) {
        let expires_at = Instant::now() + self.cfg.limits.bootstrap_lease_ttl;
        self.leases().insert(id, Lease { client_id, snapshot_seq, expires_at, _guard: guard });
    }

    pub(crate) fn remove_lease(&self, id: &[u8; 16]) {
        self.leases().remove(id);
    }

    /// Takes the lease presented by `client_id`'s Tail from `from_seq` (PROTOCOL.md §7.2),
    /// if it matches. The Tail producer keeps it until it has read the entries after the
    /// snapshot, so aggressive retention cannot trim `S + 1` in between.
    pub(crate) fn take_lease(&self, lease_id: &[u8], client_id: &str, from_seq: Seq) -> Option<Lease> {
        let id = <[u8; 16]>::try_from(lease_id).ok()?;
        let mut leases = self.leases();
        let matches = leases.get(&id).is_some_and(|l| l.client_id == client_id && from_seq <= l.snapshot_seq + 1);
        if matches { leases.remove(&id) } else { None }
    }

    pub(crate) fn expire_leases(&self, now: Instant) {
        self.leases().retain(|_, l| l.expires_at > now);
    }

    /// Authenticates one call (PROTOCOL.md §3.3) and returns the node it may read.
    async fn authorize(&self, call: &CallInfo, method: Method) -> Result<Arc<dyn LogSource>, Status> {
        let ip = call.peer.remote_addr.ip();
        if self.auth_failures.is_blocked(ip, Instant::now()) {
            return Err(Status::unauthenticated("too many failed authentication attempts"));
        }
        match self.cfg.authenticator.authenticate(&call.peer, method, call.token.as_deref()).await {
            Ok(()) => self.source(),
            Err(AuthError::Unauthenticated) => {
                self.auth_failures.record_failure(ip, Instant::now());
                Err(Status::unauthenticated("authentication failed"))
            }
            Err(AuthError::PermissionDenied) => Err(Status::permission_denied(format!("{method:?} is not allowed"))),
        }
    }
}

/// What a handler extracts from the request before any `.await`.
pub(crate) struct CallInfo {
    pub peer: PeerInfo,
    pub token: Option<Vec<u8>>,
    /// `x-rrdb-node-id`, validated.
    pub node_id: String,
}

impl CallInfo {
    /// The request's `client_id` must be a valid id and equal the `x-rrdb-node-id` metadata.
    pub(crate) fn check_client_id(&self, client_id: &str) -> Result<(), Status> {
        validate_identifier(client_id).map_err(|e| net::invalid_argument(format!("client_id: {e}")))?;
        if client_id != self.node_id {
            return Err(net::invalid_argument("client_id must equal the x-rrdb-node-id metadata"));
        }
        Ok(())
    }
}

/// Checks the protocol version metadata (except for Handshake), validates the node id
/// metadata and collects peer info.
pub(crate) fn call_info<T>(req: &Request<T>, method: Method) -> Result<CallInfo, Status> {
    if method != Method::Handshake {
        net::check_protocol_version(req.metadata())?;
    }
    let node_id = net::node_id(req.metadata()).ok_or_else(|| net::invalid_argument("x-rrdb-node-id is required"))?;
    validate_identifier(&node_id).map_err(|e| net::invalid_argument(format!("x-rrdb-node-id: {e}")))?;
    let remote_addr = req.remote_addr().unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
    let client_cert = req.peer_certs().and_then(|certs| certs.first().map(|c| c.as_ref().to_vec()));
    let peer = match client_cert.as_deref().and_then(subject_common_name) {
        Some(cn) => PeerInfo { peer_id: cn, peer_id_verified: true, remote_addr, client_cert },
        None => PeerInfo { peer_id: node_id.clone(), peer_id_verified: false, remote_addr, client_cert },
    };
    Ok(CallInfo { peer, token: net::bearer_token(req.metadata()), node_id })
}

/// Maps a store error to a status. Corruption and gaps keep their protocol reasons.
pub(crate) fn store_status(e: Error) -> Status {
    match e {
        Error::Corrupt(m) => reason_status(pb::Reason::Corrupt, m),
        e @ Error::Gap { .. } => reason_status(pb::Reason::LogGap, e.to_string()),
        other => net::internal_status(other.to_string()),
    }
}

struct Svc(Arc<ServerState>);

pub(crate) type TailStream = ReceiverStream<Result<pb::TailMessage, Status>>;
pub(crate) type BootstrapStream = ReceiverStream<Result<pb::BootstrapChunk, Status>>;
pub(crate) type VerifyRangeStream = ReceiverStream<Result<pb::VerifyRangeResponse, Status>>;

#[tonic::async_trait]
impl Replication for Svc {
    async fn handshake(&self, req: Request<pb::HandshakeRequest>) -> Result<Response<pb::HandshakeResponse>, Status> {
        let call = call_info(&req, Method::Handshake)?;
        let src = self.0.authorize(&call, Method::Handshake).await?;
        let r = req.into_inner();
        call.check_client_id(&r.client_id)?;
        if let Some(known) = &r.known_identity {
            crate::net::convert::identity_from_pb(known)
                .map_err(|_| net::invalid_argument("known_identity.db_uuid must be 16 bytes"))?;
        }
        let view = server_view(&*src, r.last_seq)?;
        let v = handshake::decide(&view, &r);
        tracing::info!(
            server = %view.server_id, client = %r.client_id, last_seq = r.last_seq,
            decision = v.decision.as_str_name(), reason = v.reason.as_str_name(), detail = %v.detail,
            "handshake"
        );
        let identity = view.identity.filter(|_| v.reason != pb::Reason::NotReady);
        Ok(Response::new(pb::HandshakeResponse {
            protocol_version: PROTOCOL_VERSION,
            envelope_version: u32::from(ENVELOPE_VERSION),
            server_id: view.server_id,
            decision: v.decision as i32,
            reason: v.reason as i32,
            detail: v.detail,
            upstream_chain: view.upstream_chain,
            identity: identity.as_ref().map(identity_to_pb),
            cf_specs: src.store().cfs.live_entries().iter().map(cf_to_pb).collect(),
            first_retained_seq: view.first_retained_seq,
            last_seq: view.last_seq,
            origin_last_seq: src.origin_last_seq(),
            max_entry_bytes: src.max_entry_bytes(),
            heartbeat_ms: u32::try_from(src.heartbeat().as_millis()).unwrap_or(u32::MAX),
            epoch_history: epochs_to_pb(&view.epoch_history),
        }))
    }

    type TailStream = TailStream;

    async fn tail(&self, req: Request<tonic::Streaming<pb::TailClientMessage>>) -> Result<Response<TailStream>, Status> {
        tail::tail(&self.0, req).await
    }

    type BootstrapStream = BootstrapStream;

    async fn bootstrap(&self, req: Request<pb::BootstrapRequest>) -> Result<Response<BootstrapStream>, Status> {
        bootstrap::bootstrap(&self.0, req).await
    }

    async fn verify_begin(&self, _: Request<pb::VerifyBeginRequest>) -> Result<Response<pb::VerifyBeginResponse>, Status> {
        Err(net::unimplemented("Verify is not implemented by this version"))
    }

    type VerifyRangeStream = VerifyRangeStream;

    async fn verify_range(
        &self,
        _: Request<tonic::Streaming<pb::VerifyRangeRequest>>,
    ) -> Result<Response<VerifyRangeStream>, Status> {
        Err(net::unimplemented("Verify is not implemented by this version"))
    }

    async fn status(&self, req: Request<pb::StatusRequest>) -> Result<Response<pb::StatusResponse>, Status> {
        let call = call_info(&req, Method::Status)?;
        let src = self.0.authorize(&call, Method::Status).await?;
        call.check_client_id(&req.get_ref().client_id)?;
        let info = status::node_info(&*src, &self.0.replicas, self.0.cfg.limits.replica_lag_time_max);
        Ok(Response::new(status::to_pb(&info)))
    }

    async fn verify_end(&self, _: Request<pb::VerifyEndRequest>) -> Result<Response<pb::VerifyEndResponse>, Status> {
        Err(net::unimplemented("Verify is not implemented by this version"))
    }
}

fn server_view(src: &dyn LogSource, client_last_seq: Seq) -> Result<handshake::ServerView, Status> {
    let ident = src.identity();
    let store = src.store();
    Ok(handshake::ServerView {
        server_id: src.node_id().to_string(),
        is_master: src.role() == Role::Master,
        ready: src.is_ready(),
        identity: ident.as_ref().map(|(id, _)| *id),
        epoch_history: ident.map(|(_, h)| h).unwrap_or_default(),
        upstream_chain: src.upstream_chain(),
        last_seq: store.position().last_seq,
        first_retained_seq: store.first_retained_seq(),
        crc_at_client_last: store.entry_crc(client_last_seq).map_err(store_status)?,
    })
}

/// A running server. Dropping it signals shutdown without waiting.
pub(crate) struct ServerHandle {
    addr: SocketAddr,
    state: Arc<ServerState>,
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<std::result::Result<(), tonic::transport::Error>>>,
    sweep: Option<JoinHandle<()>>,
}

impl ServerHandle {
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// How long a replica may go without being fully caught up and still count as in sync.
    pub(crate) fn replica_lag_time_max(&self) -> Duration {
        self.state.cfg.limits.replica_lag_time_max
    }

    /// Ends every stream with `SHUTTING_DOWN`, stops accepting calls, and waits (up to 5 s
    /// each) until every bootstrap job has returned its permit (and dropped its snapshot)
    /// and the server task has finished.
    pub(crate) async fn shutdown(mut self) {
        self.shutdown_tx.send_replace(true);
        let all = u32::try_from(self.state.cfg.limits.max_concurrent_bootstraps).unwrap_or(u32::MAX);
        if tokio::time::timeout(Duration::from_secs(5), self.state.bootstraps.acquire_many(all)).await.is_err() {
            tracing::warn!(addr = %self.addr, "bootstrap jobs did not stop in 5 s");
        }
        // A Bootstrap that arrives later on an existing connection is refused.
        self.state.bootstraps.close();
        self.state.leases().clear();
        if let Some(sweep) = self.sweep.take() {
            let _ = sweep.await;
        }
        if let Some(mut task) = self.task.take() {
            if tokio::time::timeout(Duration::from_secs(5), &mut task).await.is_err() {
                tracing::warn!(addr = %self.addr, "replication server did not stop in 5 s; aborting it");
                task.abort();
            }
        }
    }

    /// Stops immediately, without waiting for anything (emulates a crash).
    #[allow(dead_code)] // used by the slave crash tests (later Plan 2 task)
    pub(crate) fn abort(mut self) {
        self.shutdown_tx.send_replace(true);
        self.state.leases().clear();
        if let Some(t) = self.sweep.take() {
            t.abort();
        }
        if let Some(t) = self.task.take() {
            t.abort();
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown_tx.send_replace(true);
    }
}

/// Binds `cfg.bind:cfg.port` (0 picks a free port) and serves `source`. Its Tail streams
/// register in `replicas`.
pub(crate) async fn start(source: Weak<dyn LogSource>, cfg: ServeConfig, replicas: Arc<ReplicaRegistry>) -> Result<ServerHandle> {
    cfg.validate()?;
    let listener = TcpListener::bind((cfg.bind, cfg.port)).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state = Arc::new(ServerState {
        source,
        bootstraps: Arc::new(Semaphore::new(cfg.limits.max_concurrent_bootstraps)),
        auth_failures: FailureLimiter::new(cfg.limits.max_auth_failures_per_minute),
        leases: Mutex::new(HashMap::new()),
        shutdown: shutdown_rx.clone(),
        cfg,
        replicas,
    });
    let mut builder = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));
    if let Some(tls) = &state.cfg.tls {
        builder = builder.tls_config(tls.to_tonic()).map_err(|e| Error::Config(format!("server TLS: {e}")))?;
    }
    let service = ReplicationServer::new(Svc(state.clone())).max_decoding_message_size(net::MAX_REQUEST_BYTES);
    let mut signal = shutdown_rx.clone();
    let task = tokio::spawn(builder.add_service(service).serve_with_incoming_shutdown(
        TcpListenerStream::new(listener),
        async move { stopped(&mut signal).await },
    ));
    let sweep = tokio::spawn(sweep_leases(state.clone(), shutdown_rx));
    tracing::info!(%addr, "replication server listening");
    Ok(ServerHandle { addr, state, shutdown_tx, task: Some(task), sweep: Some(sweep) })
}

async fn sweep_leases(state: Arc<ServerState>, mut shutdown: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    loop {
        tokio::select! {
            _ = tick.tick() => state.expire_leases(Instant::now()),
            r = shutdown.changed() => if r.is_err() || *shutdown.borrow() { return },
        }
    }
}

/// Ends a stream with `status`. If the channel is full it waits for room, so a slow client
/// still learns why (for example `LOG_GAP`), but gives up on shutdown or reset.
pub(crate) async fn end_stream<T>(
    tx: &mpsc::Sender<Result<T, Status>>,
    status: Status,
    shutdown: &mut watch::Receiver<bool>,
    resets: &mut watch::Receiver<u64>,
) {
    if let Err(mpsc::error::TrySendError::Full(msg)) = tx.try_send(Err(status)) {
        tokio::select! {
            biased;
            _ = tx.send(msg) => {}
            _ = stopped(shutdown) => {}
            _ = resets.changed() => {}
        }
    }
}

/// Starts serving `source` if `serve` is configured.
pub(crate) async fn start_for<S: LogSource>(
    source: &Arc<S>,
    serve: Option<ServeConfig>,
    replicas: Arc<ReplicaRegistry>,
) -> Result<Option<ServerHandle>> {
    match serve {
        Some(cfg) => {
            let weak: Weak<dyn LogSource> = Arc::downgrade(source) as Weak<dyn LogSource>;
            Ok(Some(start(weak, cfg, replicas).await?))
        }
        None => Ok(None),
    }
}

/// Resolves once `rx` holds `true` (or its sender is gone).
pub(crate) async fn stopped(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

/// Waits until `arc` is the only strong reference left (background tasks that held a clone
/// have finished), so the database it owns closes when the caller drops it.
pub(crate) async fn await_sole_owner<T: ?Sized>(arc: &Arc<T>, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while Arc::strong_count(arc) > 1 {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}
```

In `src/master/writer.rs`, import `Arc` (`use std::sync::{Arc, Mutex, MutexGuard, PoisonError, RwLock};`), add to `Inner` after `pub reset_tx: watch::Sender<u64>,`:

```rust
    /// Downstream replicas served by this master (acks, in-sync state).
    pub(crate) replicas: Arc<crate::server::replicas::ReplicaRegistry>,
```

and initialise it in `Inner::new` after `reset_tx: watch::channel(0).0,`:

```rust
            replicas: Arc::default(),
```

In `src/master/mod.rs`, add `use crate::server::replicas::ReplicaStatus;`, pass the registry to the server in `Master::open`:

```rust
        let server = crate::server::start_for(&inner, inner.cfg.serve.clone(), inner.replicas.clone()).await?;
```

and add before `pub fn serve_addr`:

```rust
    /// Direct downstream replicas: what each was sent and acknowledged, its lag, and whether
    /// it is in sync. Empty when the master does not serve replication.
    pub fn replicas(&self) -> Vec<ReplicaStatus> {
        match &self.server {
            Some(s) => self.inner.replicas.status(&*self.inner, self.inner.now_ms(), s.replica_lag_time_max()),
            None => Vec::new(),
        }
    }
```

In `src/lib.rs`, add `pub mod admin;` to the module list and, after `pub use log::{EntryIter, LogApi};`:

```rust
pub use server::replicas::{FetchSettings, ReplicaStatus};
```

- [ ] **Step 9: Run the tests**

Run: `cargo test --lib server && cargo test --lib master`
Expected: PASS, including the 4 new server tests: `acks_drive_replica_status_and_in_sync` (sent/applied seqs, `lag_entries`, `lag_ms`, `in_sync` follow the acks; the replica leaves the registry when its stream ends), `ack_violations_end_the_stream_with_invalid_argument` (an ack above the last sent seq, and one going backwards), `the_first_tail_message_must_be_start`, `a_reset_releases_a_stalled_bootstraps_snapshot_and_slot` (a client that stops reading a bootstrap + an epoch bump: a second Bootstrap is not `BUSY` within 10 s and the database is released).

Run: `cargo test`
Expected: PASS (every suite so far, including `tests/proto_sync.rs`).

- [ ] **Step 10: Update PROTOCOL.md and DESIGN.md**

In PROTOCOL.md §2, replace:

```
The service has six RPCs:
```

with:

```
The service has seven RPCs:
```

In PROTOCOL.md §2 (table), replace:

```
| `Tail` | server stream | Stream log entries from a given seq, with heartbeats. Does not end on its own. |
```

with:

```
| `Tail` | bidi stream | Stream log entries from a given seq, with heartbeats; the client acknowledges what it has committed. Does not end on its own. |
```

In PROTOCOL.md §2 (table), after:

```
| `VerifyEnd` | unary | Close a verify session. |
```

add:

```
| `Status` | unary | Report the server's identity, position and replicas. Read-only; for operators and promotion tooling. |
```

In PROTOCOL.md §3.3, after:

```
- A server SHOULD authorise per RPC. `Bootstrap` and `VerifyRange` with `ListRange` expose every key, and `Bootstrap` exposes every value. The method being called is available to the server's authenticator.
```

add:

```
- `Status` exposes node ids, positions and epoch histories, but no keys or values.
```

In PROTOCOL.md §6.4 (end), after:

```
The protocol cannot stop an operator from running two masters at once. If two nodes both start the same epoch number with different histories, rule 12 detects the divergence wherever the conflicting entry is still retained.
```

add:

````
### 6.5 Status

```proto
rpc Status(StatusRequest) returns (StatusResponse);
```

A read-only unary RPC for operators and promotion tooling. It is authenticated and authorised like every other call (the method is `Status`), `client_id` MUST equal `x-rrdb-node-id` (otherwise `INVALID_ARGUMENT`), and it changes nothing on the server.

| Field | Meaning |
|---|---|
| `node_id`, `role` | The server's node id; `"master"` or `"slave"`. |
| `identity`, `epoch_history` | As in the handshake. `identity` is absent while the node holds no consistent data. |
| `last_seq`, `last_entry_crc` | The node's durable `last_seq` and the `crc32c` of the entry at `last_seq` (absent at seq 0). |
| `last_ts_ms` | `ts_ms` of the entry at `last_seq`; 0 if it is not retained. |
| `node_state` | `"ready"`, `"bootstrapping"` or `"halted:<Reason>"`. |
| `upstream` | The address the node replicates from; empty on the master. |
| `replicas` | One `ReplicaSummary { node_id, applied_seq, in_sync }` per open Tail stream (§7.6). |

Comparing the `Status` of several replicas shows which one holds the most of the log (newest epoch, then highest `last_seq`) and which ones have diverged from it (they hold seqs at or past a point where the two epoch histories differ, or a different `last_entry_crc` at the same seq). Nothing in the protocol acts on it automatically.
````

In PROTOCOL.md §7, replace:

````
```proto
rpc Tail(TailRequest) returns (stream TailMessage);
```
````

with:

````
```proto
rpc Tail(stream TailClientMessage) returns (stream TailMessage);
```

Tail is a bidirectional stream. The client's first message is `start`, carrying a `TailRequest` (§7.1); every later client message is an `ack` (§7.6). The server sends entry batches and heartbeats (§7.3).
````

In PROTOCOL.md §7.1, after:

```
### 7.1 Request
```

add:

````
```proto
message TailClientMessage { oneof kind { TailRequest start = 1; TailAck ack = 2; } }
```

The first client message MUST be `start`. The server ends the call with `INVALID_ARGUMENT` if it is anything else, and SHOULD do so if it has not arrived within 10 s. The fields of `start`:
````

In PROTOCOL.md §7.3, replace:

```
  - with `DATA_LOSS` / `CORRUPT` if a stored entry fails its CRC.
```

with:

```
  - with `DATA_LOSS` / `CORRUPT` if a stored entry fails its CRC;
  - with `INVALID_ARGUMENT` if a client message breaks the rules of §7.6.
- A stream that ends without a status (a clean end of stream) carries no decision. The client MUST treat it as transient: back off and handshake again (§11.4).
```

In PROTOCOL.md §7.5 (end), after:

```
The server clamps the client's values to its own limits: `max_wait_ms` to at most `heartbeat_ms`, and `max_batch_bytes` to at most `max_tail_batch_bytes` (RECOMMENDED default 4 MiB). If `max_batch_bytes` is 0, it uses 1 MiB. Heartbeats are unaffected: a stream with nothing pending still gets one every `heartbeat_ms`.
```

add:

````
### 7.6 Acknowledgements

```proto
message TailAck { uint64 applied_seq = 1; }
```

After `start`, the client sends `ack` messages on the same stream. `applied_seq` means: every entry with seq ≤ `applied_seq` is committed in the client's store (written to its write-ahead log; also fsynced if the client fsyncs its writes). Until the first ack the server assumes `applied_seq = from_seq − 1`.

- The client SHOULD send an ack after applying each `EntryBatch`, coalescing acks that back up (only the latest `applied_seq` matters), and MUST send one at least once per `heartbeat_ms`, so that a replica with nothing to apply stays in sync.
- `applied_seq` MUST NOT decrease and MUST NOT exceed the last seq the server sent on this stream. A second `start` is not allowed either. The server ends a stream that breaks one of these rules with `INVALID_ARGUMENT`.
- Acks never change what the server sends: it does not resend or skip entries because of them. On a reconnect the client's position is still its own durable `last_seq` (§7).
- Per open stream the server tracks the client's node id and address, whether the id was verified (mutual TLS), the epoch the client follows, when the stream opened, the last seq sent, the last seq acknowledged, the last time `applied_seq` reached the server's `last_seq`, and the stream's fetch settings.
- A replica is **in sync** while its stream is open, it follows the server's current identity, and its `applied_seq` reached the server's `last_seq` within the last `replica_lag_time_max` (RECOMMENDED 10 s). A master MAY make a write wait until a number of in-sync replicas have acknowledged it.
````

In PROTOCOL.md §10, replace:

```
Authentication failures use `UNAUTHENTICATED` or `PERMISSION_DENIED` without `ErrorInfo` (§3.3). A malformed request uses `INVALID_ARGUMENT`.
```

with:

```
Authentication failures use `UNAUTHENTICATED` or `PERMISSION_DENIED` without `ErrorInfo` (§3.3). A malformed request uses `INVALID_ARGUMENT`, and so does a Tail client that breaks the message order or the ack rules (§7.1, §7.6).
```

In PROTOCOL.md §11.2 (table), replace:

```
| A stream ends with a retryable error, or the connection is lost | Back off and handshake again. |
```

with:

```
| A stream ends with a retryable error or without any status, or the connection is lost | Back off and handshake again. |
```

In PROTOCOL.md §11.4, after:

```
- If no `TailMessage` arrives for `3 × heartbeat_ms`, the client MUST cancel the stream and reconnect.
```

add:

```
- A Tail stream that ends cleanly (end of stream without a status) MUST be treated like a lost connection: back off and handshake again. It never means that the server has nothing more to send.
```

In PROTOCOL.md §12 (table), after:

```
| `max_tail_batch_bytes` (cap on the client's `max_batch_bytes`) | server | 4 MiB |
```

add:

```
| Tail acks | client | after each batch, and at least every `heartbeat_ms` |
| `replica_lag_time_max` (in-sync window, §7.6) | server | 10 000 ms |
```

In DESIGN.md §11 (LogSource), after:

```
    fn resets(&self) -> watch::Receiver<u64>;                   // bumped when served streams must end
```

add:

```
    fn node_state(&self) -> String { "ready".into() }          // Status RPC: ready | bootstrapping | halted:<Reason>
    fn upstream(&self) -> Option<String> { None }               // Status RPC: upstream address (slaves)
```

In DESIGN.md §11, after:

```
- Heartbeats carry `upstream_last_seq` and `origin_last_seq`, so every node can report its true lag behind the origin.
```

add:

```
- **Acks and replica status.** Tail is bidirectional: after `start`, the replica acknowledges the last seq it has committed, after each applied batch (coalesced) and at least once per heartbeat (PROTOCOL.md §7.6). The serving node keeps per-stream state (sent and acknowledged seqs, lag, in sync or not) in a replica registry shared by its server and the node. `Master::replicas()` and `Slave::replicas()` report it for direct downstream replicas, and the read-only `Status` RPC (PROTOCOL.md §6.5) reports a summary of it with the node's own position.
```

In DESIGN.md §14, replace:

```
pub enum Method { Handshake, Tail, Bootstrap, Verify }
```

with:

```
pub enum Method { Handshake, Tail, Bootstrap, Verify, Status }   // Status: read-only positions, no data
```

In DESIGN.md §16 (ServeConfig), replace:

```
                               //   max_auth_failures_per_minute: 10, max_tail_batch_bytes: 4 MiB }
}
```

with:

```
                               //   max_auth_failures_per_minute: 10, max_tail_batch_bytes: 4 MiB,
                               //   replica_lag_time_max: 10 s }
}

pub struct ReplicaStatus {          // one direct downstream replica (Master/Slave::replicas)
    pub node_id: String,            // x-rrdb-node-id, or the certificate CommonName under mTLS
    pub remote_addr: SocketAddr,
    pub peer_verified: bool,
    pub epoch: u64,                 // epoch of the identity the replica follows
    pub connected_since: SystemTime,
    pub sent_seq: Seq,
    pub applied_seq: Seq,           // last acknowledged (PROTOCOL.md §7.6)
    pub lag_entries: u64,           // this node's last_seq − applied_seq
    pub lag_ms: Option<u64>,        // now − ts_ms of the first unacknowledged entry; 0 when caught
                                    // up; None when that entry is no longer retained here
    pub in_sync: bool,              // current identity, fully caught up within replica_lag_time_max
    pub fetch: FetchSettings,       // the stream's batching, as clamped by the server
}
```

In DESIGN.md §17 (table), replace:

```
| `Tail` | server stream | entry batches from `from_seq` (client-chosen `min_batch_bytes` / `max_wait_ms` / `max_batch_bytes`), plus heartbeats |
```

with:

```
| `Tail` | bidi stream | client: `start`, then acks of its committed seq; server: entry batches from `from_seq` (client-chosen `min_batch_bytes` / `max_wait_ms` / `max_batch_bytes`), plus heartbeats |
```

In DESIGN.md §17 (table), after:

```
| `VerifyBegin` / `VerifyRange` / `VerifyEnd` | unary / bidi / unary | snapshot-pinned range digests and key listings |
```

add:

```
| `Status` | unary | read-only identity, position, node state, upstream and replicas (operator tooling) |
```

In DESIGN.md §17, replace:

```
- A Tail stream with no message for `3 × heartbeat` is cancelled and reconnected.
```

with:

```
- A Tail stream with no message for `3 × heartbeat` is cancelled and reconnected; one that ends without a status is treated the same way.
```

In DESIGN.md §18 (table), after:

```
| Tail fetch batching (slave) | `min_batch_bytes` 0 (real-time), `max_wait` 100 ms, `max_batch_bytes` 1 MiB; server cap 4 MiB |
```

add:

```
| In-sync window (`ServeLimits::replica_lag_time_max`) | 10 s |
```

In DESIGN.md §19, replace:

```
- **Status APIs** (`Slave::status`, `Master::replicas`, `ReplicaStatus`) are the source for metrics.
```

with:

```
- **Status APIs** (`Slave::status`, `Master::replicas`, `Slave::replicas`, `ReplicaStatus`, the `Status` RPC) are the source for metrics.
```

Run: `cargo test --test proto_sync`
Expected: PASS.

- [ ] **Step 11: Lint and commit**

Run: `cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings`
Expected: no warnings.

```bash
git add proto src DESIGN.md PROTOCOL.md
git commit -m "feat: bidirectional tail with replica acks, replica status and the Status RPC

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Slave foundations — configuration, states, policy table, listener, bootstrap store helpers

**Files:**
- Create: `src/slave/config.rs`, `src/slave/state.rs`, `src/slave/policy.rs`, `src/listener.rs`, `src/store/bootstrap.rs`, `src/slave/mod.rs` (module list only, replaced in Task 8)
- Modify: `src/error.rs`, `src/store/mod.rs`, `src/lib.rs`, `DESIGN.md` (§3.2, §6, §10, §16, §16.3, §18), `PROTOCOL.md` (§10, §11.2)

**Interfaces:**
- Consumes: Task 2 meta keys and fields; Task 4 `ServeConfig`, `TlsClientConfig`, `CredentialsProvider`, `NoCredentials`; Task 1 `net::reason_of`, `net::reason_status`; Plan 1 `validate_identifier`, `Retention`, `RocksTuning`, `Clock`, `Store::{cfs, write_meta, load_meta, drop_unreferenced_cfs, set_position, set_first_retained_seq, tuning, retain_cf_options}`, `PhysCf::new`, `meta::{encode_*}`.
- Produces:
  - `slave::config::{UpstreamConfig { server, port, tls, credentials } (new, from_addr, pub(crate) uri), IdentityPolicy { Refuse (default), Rebootstrap }, DivergencePolicy { Halt (default), Rebootstrap }, GapPolicy { Rebootstrap (default), Halt }, FetchConfig { min_batch_bytes: u32, max_wait: Duration, max_batch_bytes: u32 } (Default 0 / 100 ms / 1 MiB), Backoff { initial, max, factor }, BackoffState { new, reset, next_delay }, SlaveConfig { identifier, path, upstream, serve, retention, listener, on_identity_change, on_divergence, on_log_gap, reconnect, fetch, heartbeat, connect_timeout, sync_writes (default false: an ack means "in the replica's WAL"), rocksdb, clock } (new, pub(crate) validate)}`.
  - `slave::state::{HaltReason (is_persisted, pub(crate) name, pub(crate) from_name), SlaveState { Connecting, Bootstrapping { target_seq, records }, Streaming, Halted(HaltReason) }, HaltAction { Retry, Rebootstrap }, NodeState { Ready, Bootstrapping, Halted } (encode, decode), SlaveStatus { state, origin_identity, applied_seq, first_retained_seq, upstream_last_seq, origin_last_seq, upstream_chain, last_contact, last_error, listener_failed }}`.
  - `slave::policy::{Policies { identity, divergence, gap }, Action { Tail, AdoptEpochThenTail, Bootstrap, Halt(HaltReason), Retry(String), Again }, after_handshake(&pb::HandshakeResponse, &Policies, force_bootstrap: bool) -> Action, after_status(&Status, &Policies) -> Action}`.
  - `pub trait ReplicationListener: Send + Sync + 'static { on_entry, on_bootstrap_begin, on_bootstrap_complete, on_state_change }` (all default no-ops).
  - `Error::Halted(HaltReason)`.
  - `store::bootstrap::{NODE_READY = b"ready", NODE_BOOTSTRAPPING = b"bootstrapping", BootstrapFinish { identity, epoch_history, cf_specs, snapshot_seq, snapshot_crc, max_entry_bytes }}`, `Store::{reset_for_bootstrap(&self) -> Result<()>, begin_bootstrap_cf(&self, &CfEntry) -> Result<()>, write_bootstrap_records(&self, &CfEntry, &[(Vec<u8>, Vec<u8>)]) -> Result<()>, finish_bootstrap(&self, &BootstrapFinish) -> Result<()>, adopt_identity(&self, &DbIdentity, &[EpochStart]) -> Result<()>, delete_all_keys(&self, &impl AsColumnFamilyRef) -> Result<()>}` (unsynced batches of 1 000; also used by Task 11).

- [ ] **Step 1: Write the failing tests**

Create `src/store/bootstrap.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope;
    use crate::read::ReadApi;
    use crate::store::test_support::apply;
    use crate::types::{CfSpec, Op, Role};

    fn slave_store(dir: &std::path::Path) -> Store {
        Store::open(dir, Role::Slave, Default::default()).unwrap().0
    }

    fn put(cf: &str, k: &[u8], v: &[u8]) -> Op {
        Op::Put { cf: cf.into(), key: k.to_vec(), value: v.to_vec(), expire_at_ms: None }
    }

    fn finish(cfs: Vec<CfEntry>, seq: Seq) -> BootstrapFinish {
        BootstrapFinish {
            identity: DbIdentity { db_uuid: uuid::Uuid::from_u128(7), epoch: 2 },
            epoch_history: vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 3 }],
            cf_specs: cfs,
            snapshot_seq: seq,
            snapshot_crc: Some(0xABCD),
            max_entry_bytes: 1 << 20,
        }
    }

    fn rec(k: &[u8], seq: Seq, v: &[u8]) -> (Vec<u8>, Vec<u8>) {
        (k.to_vec(), envelope::encode(seq, None, v))
    }

    #[test]
    fn reset_discards_everything_and_persists_bootstrapping() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put("c", b"k", b"v")]).unwrap();
        store.reset_for_bootstrap().unwrap();
        assert_eq!(store.position(), Position::default());
        assert_eq!(store.first_retained_seq(), 1);
        assert!(store.cfs.live_entries().is_empty());
        assert!(store.db().cf_handle("u:c:1").is_none(), "unreferenced CF dropped at once");
        assert!(store.entries_after(0).unwrap().next().is_none(), "changelog cleared");
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!(m.node_state.as_deref(), Some(NODE_BOOTSTRAPPING));
        assert_eq!((m.identity, m.last_seq, m.last_entry_crc), (None, 0, None));
        assert!(m.cf_specs.is_empty() && m.pending_drops.is_empty());
    }

    #[test]
    fn full_install_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let c = CfEntry { spec: CfSpec::new("c"), create_seq: 2 };
        let empty = CfEntry { spec: CfSpec::new("e"), create_seq: 1 };
        {
            let store = slave_store(dir.path());
            store.reset_for_bootstrap().unwrap();
            store.begin_bootstrap_cf(&c).unwrap();
            store.begin_bootstrap_cf(&empty).unwrap();
            store.write_bootstrap_records(&c, &[rec(b"", 3, b""), rec(b"\0", 4, b"\0")]).unwrap();
            store.write_bootstrap_records(&c, &[rec(b"k", 5, b"v")]).unwrap();
            store.finish_bootstrap(&finish(vec![c.clone(), empty.clone()], 9)).unwrap();
            assert_eq!(store.position(), Position { last_seq: 9, last_entry_crc: Some(0xABCD) });
            assert_eq!(store.first_retained_seq(), 10);
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(m.node_state.as_deref(), Some(NODE_READY));
        assert!(m.bootstrap_cfs.is_empty());
        assert_eq!(m.cf_specs, vec![c, empty]);
        assert_eq!((m.last_seq, m.last_entry_crc, m.max_entry_bytes), (9, Some(0xABCD), Some(1 << 20)));
        assert_eq!(m.identity.unwrap().epoch, 2);
        assert_eq!(m.epoch_history.len(), 2);
        let snap = store.read_snapshot(0).unwrap();
        assert_eq!(snap.get("c", b"").unwrap(), Some(vec![]));
        assert_eq!(snap.get("c", b"\0").unwrap(), Some(vec![0]));
        assert_eq!(snap.get_with_meta("c", b"k").unwrap().unwrap().1.write_seq, 5);
    }

    #[test]
    fn crash_mid_bootstrap_reopens_and_the_next_reset_drops_leftovers() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = crate::tuning::RocksTuning::new();
        tuning.comparators.register("rev", |a: &[u8], b: &[u8]| b.cmp(a));
        let c = CfEntry { spec: CfSpec::new("c").with_comparator("rev"), create_seq: 4 };
        {
            let store = Store::open(dir.path(), Role::Slave, tuning.clone()).unwrap().0;
            store.reset_for_bootstrap().unwrap();
            store.begin_bootstrap_cf(&c).unwrap();
            store.write_bootstrap_records(&c, &[rec(b"z", 1, b"v")]).unwrap();
            // crash: no finish
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, tuning).unwrap();
        assert_eq!(m.node_state.as_deref(), Some(NODE_BOOTSTRAPPING));
        assert_eq!(m.bootstrap_cfs, vec![c.clone()]);
        assert!(store.db().cf_handle("u:c:4").is_some());
        store.reset_for_bootstrap().unwrap();
        assert!(store.db().cf_handle("u:c:4").is_none());
        assert!(store.load_meta().unwrap().unwrap().bootstrap_cfs.is_empty());
    }

    #[test]
    fn a_cf_held_by_a_snapshot_is_taken_over_without_disturbing_the_snapshot() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put("c", b"old", b"1")]).unwrap();
        let snap = store.read_snapshot(0).unwrap();
        store.reset_for_bootstrap().unwrap();
        assert!(store.db().cf_handle("u:c:1").is_some(), "the snapshot still references it");

        let c = CfEntry { spec: CfSpec::new("c"), create_seq: 1 };
        store.begin_bootstrap_cf(&c).unwrap();
        store.write_bootstrap_records(&c, &[rec(b"new", 7, b"2")]).unwrap();
        store.finish_bootstrap(&finish(vec![c], 7)).unwrap();

        assert_eq!(snap.get("c", b"old").unwrap(), Some(b"1".to_vec()));
        assert_eq!(snap.get("c", b"new").unwrap(), None);
        let now = store.read_snapshot(0).unwrap();
        assert_eq!(now.get("c", b"old").unwrap(), None);
        assert_eq!(now.get("c", b"new").unwrap(), Some(b"2".to_vec()));
        drop(snap);
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0, "the live CF is no longer pending");
        assert!(store.load_meta().unwrap().unwrap().pending_drops.is_empty());
    }

    #[test]
    fn adopt_identity_persists_identity_and_history_together() {
        let dir = tempfile::tempdir().unwrap();
        let store = slave_store(dir.path());
        let id = DbIdentity { db_uuid: uuid::Uuid::from_u128(1), epoch: 3 };
        let h = vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 3, start_seq: 40 }];
        store.adopt_identity(&id, &h).unwrap();
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!((m.identity, m.epoch_history), (Some(id), h));
    }
}
```

Add to the `src/store/mod.rs` module list:

```rust
pub(crate) mod bootstrap;
```

Create `src/slave/config.rs`, `src/slave/state.rs` and `src/slave/policy.rs` with only their test modules:

```rust
// src/slave/config.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_with_full_jitter_and_resets() {
        let mut b = BackoffState::new(Backoff {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(400),
            factor: 2,
        });
        for cap in [100, 200, 400, 400] {
            let d = b.next_delay();
            assert!(d <= Duration::from_millis(cap), "{d:?} > {cap} ms");
        }
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(100));
    }

    #[test]
    fn upstream_uri_brackets_ipv6() {
        assert_eq!(UpstreamConfig::new("10.0.0.1", 7000).uri(), "http://10.0.0.1:7000");
        assert_eq!(UpstreamConfig::from_addr("[::1]:7000".parse().unwrap()).uri(), "http://[::1]:7000");
    }

    #[test]
    fn config_is_validated() {
        let up = UpstreamConfig::new("127.0.0.1", 7000);
        SlaveConfig::new("b", "/tmp/x", up.clone()).validate().unwrap();
        assert!(SlaveConfig::new("bad id", "/tmp/x", up.clone()).validate().is_err());
        assert!(SlaveConfig::new("b", "/tmp/x", UpstreamConfig::new("h", 0)).validate().is_err());
        let mut cfg = SlaveConfig::new("b", "/tmp/x", up);
        cfg.reconnect.max = Duration::from_millis(1);
        assert!(cfg.validate().is_err());
    }
}
```

```rust
// src/slave/state.rs
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn node_state_round_trips_and_only_data_halts_persist() {
        for s in [NodeState::Ready, NodeState::Bootstrapping] {
            assert_eq!(NodeState::decode(Some(&s.encode())).unwrap(), s);
        }
        for r in ALL {
            let encoded = NodeState::Halted(r).encode();
            if r.is_persisted() {
                assert_eq!(NodeState::decode(Some(&encoded)).unwrap(), NodeState::Halted(r));
            } else {
                assert!(NodeState::decode(Some(&encoded)).is_err(), "{r:?} is never persisted");
            }
        }
        assert_eq!(NodeState::decode(None).unwrap(), NodeState::Ready);
        assert_eq!(NodeState::Halted(HaltReason::LogGap).encode(), b"halted:LogGap");
        assert!(NodeState::decode(Some(b"halted:Nope")).is_err());
    }

    #[test]
    fn persisted_halts_are_exactly_the_data_related_ones() {
        let persisted: Vec<_> = ALL.into_iter().filter(|r| r.is_persisted()).collect();
        assert_eq!(
            persisted,
            vec![
                HaltReason::IdentityChanged,
                HaltReason::Diverged,
                HaltReason::LogGap,
                HaltReason::Corrupt,
                HaltReason::SpecMismatch
            ]
        );
    }
}
```

```rust
// src/slave/policy.rs
#[cfg(test)]
mod tests {
    use super::*;
    use crate::net::reason_status;

    const DEFAULTS: Policies = Policies {
        identity: IdentityPolicy::Refuse,
        divergence: DivergencePolicy::Halt,
        gap: GapPolicy::Rebootstrap,
    };
    const OPPOSITE: Policies = Policies {
        identity: IdentityPolicy::Rebootstrap,
        divergence: DivergencePolicy::Rebootstrap,
        gap: GapPolicy::Halt,
    };

    fn answer(d: Decision, r: Reason) -> pb::HandshakeResponse {
        pb::HandshakeResponse { decision: d as i32, reason: r as i32, detail: "why".into(), ..Default::default() }
    }

    fn act(d: Decision, r: Reason, p: &Policies) -> Action {
        after_handshake(&answer(d, r), p, false)
    }

    #[test]
    fn resume_rows() {
        assert_eq!(act(Decision::Resume, Reason::Unspecified, &DEFAULTS), Action::Tail);
        assert_eq!(act(Decision::Resume, Reason::EpochAdvanced, &DEFAULTS), Action::AdoptEpochThenTail);
    }

    #[test]
    fn bootstrap_rows_follow_policies() {
        assert_eq!(act(Decision::Bootstrap, Reason::NoLocalState, &DEFAULTS), Action::Bootstrap);
        assert_eq!(act(Decision::Bootstrap, Reason::IdentityChanged, &DEFAULTS), Action::Halt(HaltReason::IdentityChanged));
        assert_eq!(act(Decision::Bootstrap, Reason::IdentityChanged, &OPPOSITE), Action::Bootstrap);
        assert_eq!(act(Decision::Bootstrap, Reason::LogGap, &DEFAULTS), Action::Bootstrap);
        assert_eq!(act(Decision::Bootstrap, Reason::LogGap, &OPPOSITE), Action::Halt(HaltReason::LogGap));
    }

    #[test]
    fn reject_rows() {
        assert_eq!(act(Decision::Reject, Reason::Diverged, &DEFAULTS), Action::Halt(HaltReason::Diverged));
        assert_eq!(act(Decision::Reject, Reason::Diverged, &OPPOSITE), Action::Bootstrap);
        assert_eq!(act(Decision::Reject, Reason::StaleEpoch, &DEFAULTS), Action::Retry("STALE_EPOCH: why".into()));
        assert_eq!(act(Decision::Reject, Reason::NotReady, &DEFAULTS), Action::Retry("NOT_READY: why".into()));
        assert_eq!(act(Decision::Reject, Reason::ProtocolMismatch, &DEFAULTS), Action::Halt(HaltReason::ProtocolMismatch));
        assert_eq!(act(Decision::Reject, Reason::EnvelopeMismatch, &DEFAULTS), Action::Halt(HaltReason::EnvelopeMismatch));
        assert_eq!(act(Decision::Reject, Reason::LoopDetected, &DEFAULTS), Action::Halt(HaltReason::LoopDetected));
        assert!(matches!(act(Decision::Unspecified, Reason::Busy, &DEFAULTS), Action::Retry(_)));
    }

    #[test]
    fn forced_bootstrap_overrides_only_answers_that_let_the_client_proceed() {
        let forced = |d, r| after_handshake(&answer(d, r), &DEFAULTS, true);
        assert_eq!(forced(Decision::Resume, Reason::Unspecified), Action::Bootstrap);
        assert_eq!(forced(Decision::Bootstrap, Reason::IdentityChanged), Action::Bootstrap);
        assert_eq!(forced(Decision::Reject, Reason::Diverged), Action::Bootstrap);
        assert!(matches!(forced(Decision::Reject, Reason::StaleEpoch), Action::Retry(_)));
        assert_eq!(forced(Decision::Reject, Reason::LoopDetected), Action::Halt(HaltReason::LoopDetected));
    }

    #[test]
    fn status_rows() {
        let s = |r| reason_status(r, "x");
        assert_eq!(after_status(&Status::unauthenticated("x"), &DEFAULTS), Action::Halt(HaltReason::AuthRejected));
        assert_eq!(after_status(&Status::permission_denied("x"), &DEFAULTS), Action::Halt(HaltReason::AuthRejected));
        assert_eq!(after_status(&s(Reason::ProtocolMismatch), &DEFAULTS), Action::Halt(HaltReason::ProtocolMismatch));
        assert_eq!(after_status(&s(Reason::Diverged), &DEFAULTS), Action::Halt(HaltReason::Diverged));
        assert_eq!(after_status(&s(Reason::Diverged), &OPPOSITE), Action::Bootstrap);
        for r in [Reason::LogGap, Reason::IdentityChanged, Reason::UpstreamReset] {
            assert_eq!(after_status(&s(r), &DEFAULTS), Action::Again, "{r:?}");
        }
        for r in [Reason::NotReady, Reason::Busy, Reason::ShuttingDown, Reason::Corrupt] {
            assert!(matches!(after_status(&s(r), &DEFAULTS), Action::Retry(_)), "{r:?}");
        }
        assert!(matches!(after_status(&Status::unavailable("connection refused"), &DEFAULTS), Action::Retry(_)));
    }
}
```

Create `src/slave/mod.rs`:

```rust
//! A read-only replica.

mod config;
mod policy;
mod state;

pub use config::{Backoff, DivergencePolicy, FetchConfig, GapPolicy, IdentityPolicy, SlaveConfig, UpstreamConfig};
pub use state::{HaltAction, HaltReason, SlaveState, SlaveStatus};
```

Add to `src/lib.rs`:

```rust
pub mod listener;
mod slave;

pub use listener::ReplicationListener;
pub use slave::{
    Backoff, DivergencePolicy, FetchConfig, GapPolicy, HaltAction, HaltReason, IdentityPolicy, SlaveConfig, SlaveState,
    SlaveStatus, UpstreamConfig,
};
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib -- store::bootstrap slave::`
Expected: FAIL to compile: `no method named reset_for_bootstrap`, `cannot find type BackoffState`, `cannot find type NodeState`, `cannot find function after_handshake`, `file not found for module listener`.

- [ ] **Step 3: Add `Error::Halted`**

In `src/error.rs`, after the `Timeout` variant add:

```rust
    /// The slave stopped advancing; see `SlaveStatus` and `Slave::clear_halt`.
    #[error("replication is halted: {0:?}")]
    Halted(crate::slave::HaltReason),
```

- [ ] **Step 4: Implement the listener trait**

Create `src/listener.rs`:

```rust
//! Callbacks from a slave's apply loop (DESIGN.md §15).

use crate::slave::SlaveState;
use crate::types::{LogEntry, Seq};

/// Observes a slave's replication. Called inline in the apply loop, after commit and
/// before the new seq is published to `wait_for_seq` and `subscribe`: a slow listener
/// slows replication down. Must not wait on the slave's own progress
/// (for example `wait_for_seq`), which would deadlock. A panic is caught and logged; the
/// listener is then not called again until the process restarts.
///
/// Delivery is at-most-once across crashes. Within a process, `on_entry` seqs are strictly
/// increasing between bootstraps; after `on_bootstrap_complete(S)` delivery continues at
/// `S + 1` (state was replaced wholesale, so rebuild derived state). For at-least-once
/// delivery keep your own cursor and use `LogApi::entries_since`.
pub trait ReplicationListener: Send + Sync + 'static {
    fn on_entry(&self, _entry: &LogEntry) {}
    fn on_bootstrap_begin(&self, _snapshot_seq: Seq) {}
    fn on_bootstrap_complete(&self, _snapshot_seq: Seq) {}
    fn on_state_change(&self, _state: &SlaveState) {}
}
```

- [ ] **Step 5: Implement `src/slave/config.rs`** (above the test module)

```rust
//! Slave configuration (DESIGN.md §16).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{CredentialsProvider, NoCredentials};
use crate::clock::{Clock, SystemClock};
use crate::config::{validate_identifier, Retention, ServeConfig, TlsClientConfig};
use crate::error::{Error, Result};
use crate::listener::ReplicationListener;
use crate::tuning::RocksTuning;

/// The node this slave replicates from: the master or another slave.
#[derive(Clone)]
pub struct UpstreamConfig {
    /// Host name or IP address.
    pub server: String,
    pub port: u16,
    pub tls: Option<TlsClientConfig>,
    pub credentials: Arc<dyn CredentialsProvider>,
}

impl UpstreamConfig {
    pub fn new(server: impl Into<String>, port: u16) -> Self {
        UpstreamConfig { server: server.into(), port, tls: None, credentials: Arc::new(NoCredentials) }
    }

    pub fn from_addr(addr: SocketAddr) -> Self {
        Self::new(addr.ip().to_string(), addr.port())
    }

    pub(crate) fn uri(&self) -> String {
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        if self.server.contains(':') && !self.server.starts_with('[') {
            format!("{scheme}://[{}]:{}", self.server, self.port)
        } else {
            format!("{scheme}://{}:{}", self.server, self.port)
        }
    }
}

/// What to do when the upstream's database identity differs, or continuity across an
/// epoch change cannot be proven.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IdentityPolicy {
    /// Halt with `IdentityChanged` (persisted).
    #[default]
    Refuse,
    Rebootstrap,
}

/// What to do when the upstream reports `DIVERGED`.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DivergencePolicy {
    /// Halt with `Diverged` (persisted).
    #[default]
    Halt,
    Rebootstrap,
}

/// What to do when the entries this slave needs are no longer retained upstream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum GapPolicy {
    #[default]
    Rebootstrap,
    /// Halt with `LogGap` (persisted).
    Halt,
}

/// Tail fetch batching (PROTOCOL.md §7.5), like Kafka's `fetch.min.bytes`,
/// `fetch.max.wait.ms` and `fetch.max.bytes`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchConfig {
    /// The upstream waits until this many bytes of entries are ready. 0 = real-time push.
    pub min_batch_bytes: u32,
    /// The longest a ready entry waits for `min_batch_bytes` (the upstream clamps it to its
    /// heartbeat). Ignored when `min_batch_bytes` is 0.
    pub max_wait: Duration,
    /// The most entry bytes per batch (the upstream clamps it to its `max_tail_batch_bytes`).
    pub max_batch_bytes: u32,
}

impl Default for FetchConfig {
    fn default() -> Self {
        FetchConfig { min_batch_bytes: 0, max_wait: Duration::from_millis(100), max_batch_bytes: 1 << 20 }
    }
}

/// Reconnect backoff: exponential with full jitter.
#[derive(Clone, Debug)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    pub factor: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff { initial: Duration::from_millis(100), max: Duration::from_secs(30), factor: 2 }
    }
}

/// Backoff progress of one driver.
#[derive(Debug)]
pub(crate) struct BackoffState {
    cfg: Backoff,
    cap: Duration,
}

impl BackoffState {
    pub(crate) fn new(cfg: Backoff) -> Self {
        let cap = cfg.initial;
        BackoffState { cfg, cap }
    }

    pub(crate) fn reset(&mut self) {
        self.cap = self.cfg.initial;
    }

    /// A random delay in `[0, cap]`; the cap then grows by `factor` up to `max`.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let cap_ms = u64::try_from(self.cap.as_millis()).unwrap_or(u64::MAX);
        let random = uuid::Uuid::new_v4().as_u128() as u64;
        let delay = Duration::from_millis(random % cap_ms.saturating_add(1));
        self.cap = self.cap.saturating_mul(self.cfg.factor).min(self.cfg.max);
        delay
    }
}

#[derive(Clone)]
pub struct SlaveConfig {
    /// Unique within the topology: 1–128 characters of `[A-Za-z0-9._-]`.
    pub identifier: String,
    pub path: PathBuf,
    pub upstream: UpstreamConfig,
    /// `Some` = this slave also serves downstream replicas.
    pub serve: Option<ServeConfig>,
    pub retention: Retention,
    pub listener: Option<Arc<dyn ReplicationListener>>,
    pub on_identity_change: IdentityPolicy,
    pub on_divergence: DivergencePolicy,
    pub on_log_gap: GapPolicy,
    pub reconnect: Backoff,
    /// Tail fetch batching. Default: real-time push.
    pub fetch: FetchConfig,
    /// Fsync every applied entry before acknowledging it. Default `false`: an ack then means
    /// "in the replica's WAL", which survives a process crash but not a machine crash.
    pub sync_writes: bool,
    /// Heartbeat interval when serving downstream replicas. Default 5 s.
    pub heartbeat: Duration,
    /// TCP connect timeout to the upstream. Default 5 s.
    pub connect_timeout: Duration,
    pub rocksdb: RocksTuning,
    /// Time source for TTL read filtering and retention.
    pub clock: Arc<dyn Clock>,
}

impl SlaveConfig {
    pub fn new(identifier: impl Into<String>, path: impl Into<PathBuf>, upstream: UpstreamConfig) -> Self {
        SlaveConfig {
            identifier: identifier.into(),
            path: path.into(),
            upstream,
            serve: None,
            retention: Retention::default(),
            listener: None,
            on_identity_change: IdentityPolicy::default(),
            on_divergence: DivergencePolicy::default(),
            on_log_gap: GapPolicy::default(),
            reconnect: Backoff::default(),
            fetch: FetchConfig::default(),
            sync_writes: false,
            heartbeat: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            rocksdb: RocksTuning::default(),
            clock: Arc::new(SystemClock),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_identifier(&self.identifier)?;
        if self.upstream.server.is_empty() || self.upstream.port == 0 {
            return Err(Error::Config("upstream server and port are required".into()));
        }
        if self.reconnect.initial.is_zero() || self.reconnect.max < self.reconnect.initial || self.reconnect.factor == 0 {
            return Err(Error::Config("reconnect backoff needs 0 < initial <= max and factor >= 1".into()));
        }
        if self.fetch.max_batch_bytes == 0 {
            return Err(Error::Config("fetch.max_batch_bytes must be positive".into()));
        }
        if self.heartbeat.is_zero() || self.connect_timeout.is_zero() || self.retention.check_interval.is_zero() {
            return Err(Error::Config("heartbeat, connect_timeout and retention.check_interval must be positive".into()));
        }
        if let Some(serve) = &self.serve {
            serve.validate()?;
        }
        Ok(())
    }
}
```

- [ ] **Step 6: Implement `src/slave/state.rs`** (above the test module)

```rust
//! Slave states: the runtime state machine and what is persisted (DESIGN.md §16.3).

use std::time::SystemTime;

use crate::error::{Error, Result};
use crate::store::bootstrap::{NODE_BOOTSTRAPPING, NODE_READY};
use crate::types::{DbIdentity, Seq};

/// Why a slave stopped advancing. Reads keep working while halted.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HaltReason {
    // Data-related: persisted, survive restart, cleared only by `clear_halt`.
    IdentityChanged,
    Diverged,
    LogGap,
    Corrupt,
    SpecMismatch,
    // Configuration-related: not persisted, re-evaluated on restart.
    ProtocolMismatch,
    EnvelopeMismatch,
    AuthRejected,
    LoopDetected,
}

const ALL: [HaltReason; 9] = [
    HaltReason::IdentityChanged,
    HaltReason::Diverged,
    HaltReason::LogGap,
    HaltReason::Corrupt,
    HaltReason::SpecMismatch,
    HaltReason::ProtocolMismatch,
    HaltReason::EnvelopeMismatch,
    HaltReason::AuthRejected,
    HaltReason::LoopDetected,
];

impl HaltReason {
    /// True for data-related halts, which survive a restart.
    pub fn is_persisted(self) -> bool {
        matches!(
            self,
            HaltReason::IdentityChanged
                | HaltReason::Diverged
                | HaltReason::LogGap
                | HaltReason::Corrupt
                | HaltReason::SpecMismatch
        )
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            HaltReason::IdentityChanged => "IdentityChanged",
            HaltReason::Diverged => "Diverged",
            HaltReason::LogGap => "LogGap",
            HaltReason::Corrupt => "Corrupt",
            HaltReason::SpecMismatch => "SpecMismatch",
            HaltReason::ProtocolMismatch => "ProtocolMismatch",
            HaltReason::EnvelopeMismatch => "EnvelopeMismatch",
            HaltReason::AuthRejected => "AuthRejected",
            HaltReason::LoopDetected => "LoopDetected",
        }
    }

    pub(crate) fn from_name(name: &str) -> Option<HaltReason> {
        ALL.into_iter().find(|r| r.name() == name)
    }
}

/// Runtime state of a slave.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SlaveState {
    Connecting,
    Bootstrapping { target_seq: Seq, records: u64 },
    Streaming,
    Halted(HaltReason),
}

/// Operator action for [`Slave::clear_halt`](crate::Slave::clear_halt).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum HaltAction {
    /// Handshake again with the current data (after fixing the cause).
    Retry,
    /// Discard local data and bootstrap from the upstream.
    Rebootstrap,
}

/// `__meta.node_state`: `ready`, `bootstrapping` or `halted:<Reason>`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum NodeState {
    Ready,
    Bootstrapping,
    Halted(HaltReason),
}

impl NodeState {
    pub(crate) fn encode(self) -> Vec<u8> {
        match self {
            NodeState::Ready => NODE_READY.to_vec(),
            NodeState::Bootstrapping => NODE_BOOTSTRAPPING.to_vec(),
            NodeState::Halted(r) => format!("halted:{}", r.name()).into_bytes(),
        }
    }

    /// `None` (key absent) is `Ready`.
    pub(crate) fn decode(raw: Option<&[u8]>) -> Result<NodeState> {
        let Some(raw) = raw else { return Ok(NodeState::Ready) };
        if raw == NODE_READY {
            return Ok(NodeState::Ready);
        }
        if raw == NODE_BOOTSTRAPPING {
            return Ok(NodeState::Bootstrapping);
        }
        std::str::from_utf8(raw)
            .ok()
            .and_then(|s| s.strip_prefix("halted:"))
            .and_then(HaltReason::from_name)
            .filter(|r| r.is_persisted())
            .map(NodeState::Halted)
            .ok_or_else(|| Error::corrupt(format!("unknown node_state {:?}", String::from_utf8_lossy(raw))))
    }
}

/// A snapshot of a slave's replication status.
#[derive(Clone, Debug)]
pub struct SlaveStatus {
    pub state: SlaveState,
    pub origin_identity: Option<DbIdentity>,
    pub applied_seq: Seq,
    pub first_retained_seq: Seq,
    pub upstream_last_seq: Seq,
    pub origin_last_seq: Seq,
    /// Node ids from the upstream to the master, closest first.
    pub upstream_chain: Vec<String>,
    pub last_contact: Option<SystemTime>,
    /// The last connection or handshake problem, e.g. `STALE_EPOCH`.
    pub last_error: Option<String>,
    /// True after the listener panicked; it is not called again until restart.
    pub listener_failed: bool,
}
```

- [ ] **Step 7: Implement `src/slave/policy.rs`** (above the test module)

```rust
//! What a slave does after a handshake answer or a failed call (PROTOCOL.md §11.2).
//! Pure, so every row of the table is unit-tested.

use tonic::{Code, Status};

use super::config::{DivergencePolicy, GapPolicy, IdentityPolicy};
use super::state::HaltReason;
use crate::net::reason_of;
use crate::pb;
use crate::pb::{Decision, Reason};

#[derive(Clone, Copy, Debug)]
pub(crate) struct Policies {
    pub identity: IdentityPolicy,
    pub divergence: DivergencePolicy,
    pub gap: GapPolicy,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Action {
    /// Tail from `last_seq + 1`.
    Tail,
    /// Persist the server's identity and epoch history, then tail.
    AdoptEpochThenTail,
    Bootstrap,
    Halt(HaltReason),
    /// Back off, then handshake again; the message goes to `SlaveStatus::last_error`.
    Retry(String),
    /// Handshake again at once.
    Again,
}

fn diverged(p: &Policies) -> Action {
    match p.divergence {
        DivergencePolicy::Halt => Action::Halt(HaltReason::Diverged),
        DivergencePolicy::Rebootstrap => Action::Bootstrap,
    }
}

/// The action for a handshake answer. `force_bootstrap` is set by `clear_halt(Rebootstrap)`
/// and by a gap seen mid-stream: any answer that lets the client proceed becomes a bootstrap.
pub(crate) fn after_handshake(r: &pb::HandshakeResponse, p: &Policies, force_bootstrap: bool) -> Action {
    let decision = Decision::try_from(r.decision).unwrap_or(Decision::Unspecified);
    let reason = Reason::try_from(r.reason).unwrap_or(Reason::Unspecified);
    let proceed = matches!(decision, Decision::Resume | Decision::Bootstrap)
        || (decision == Decision::Reject && reason == Reason::Diverged);
    if force_bootstrap && proceed {
        return Action::Bootstrap;
    }
    match (decision, reason) {
        (Decision::Resume, Reason::EpochAdvanced) => Action::AdoptEpochThenTail,
        (Decision::Resume, _) => Action::Tail,
        (Decision::Bootstrap, Reason::NoLocalState) => Action::Bootstrap,
        (Decision::Bootstrap, Reason::IdentityChanged) => match p.identity {
            IdentityPolicy::Refuse => Action::Halt(HaltReason::IdentityChanged),
            IdentityPolicy::Rebootstrap => Action::Bootstrap,
        },
        (Decision::Bootstrap, Reason::LogGap) => match p.gap {
            GapPolicy::Rebootstrap => Action::Bootstrap,
            GapPolicy::Halt => Action::Halt(HaltReason::LogGap),
        },
        (Decision::Reject, Reason::Diverged) => diverged(p),
        (Decision::Reject, Reason::StaleEpoch | Reason::NotReady) => {
            Action::Retry(format!("{}: {}", reason.as_str_name(), r.detail))
        }
        (Decision::Reject, Reason::ProtocolMismatch) => Action::Halt(HaltReason::ProtocolMismatch),
        (Decision::Reject, Reason::EnvelopeMismatch) => Action::Halt(HaltReason::EnvelopeMismatch),
        (Decision::Reject, Reason::LoopDetected) => Action::Halt(HaltReason::LoopDetected),
        (d, r) => Action::Retry(format!("unexpected handshake answer {}/{}", d.as_str_name(), r.as_str_name())),
    }
}

/// The action for a failed RPC or a stream that ended with an error.
pub(crate) fn after_status(s: &Status, p: &Policies) -> Action {
    if matches!(s.code(), Code::Unauthenticated | Code::PermissionDenied) {
        return Action::Halt(HaltReason::AuthRejected);
    }
    match reason_of(s) {
        Some(Reason::ProtocolMismatch) => Action::Halt(HaltReason::ProtocolMismatch),
        Some(Reason::EnvelopeMismatch) => Action::Halt(HaltReason::EnvelopeMismatch),
        Some(Reason::LoopDetected) => Action::Halt(HaltReason::LoopDetected),
        Some(Reason::Diverged) => diverged(p),
        // The next handshake decides (LOG_GAP and identity changes then follow policy).
        Some(Reason::LogGap | Reason::IdentityChanged | Reason::UpstreamReset) => Action::Again,
        _ => Action::Retry(format!("{:?}: {}", s.code(), s.message())),
    }
}
```

- [ ] **Step 8: Implement `src/store/bootstrap.rs`** (above the test module)

```rust
//! Store side of installing a bootstrap on a slave (DESIGN.md §10, receiving side), and
//! adopting a new epoch.
//!
//! Crash safety: `reset_for_bootstrap` persists `node_state = bootstrapping` and removes the
//! identity in the same synced batch, so a crash at any later point restarts the bootstrap.
//! Every physical CF a bootstrap creates is first recorded in `__meta.bootstrap_cfs`
//! (synced), so the next open knows its comparator and the next reset drops it.

use std::sync::PoisonError;

use rocksdb::{AsColumnFamilyRef, IteratorMode, WriteBatch, WriteOptions};

use super::cfs::PhysCf;
use super::meta::{self, key, CF_CHANGELOG, CF_META, CF_TTL_INDEX};
use super::{Position, Store};
use crate::error::{Error, Result};
use crate::tuning::CfKind;
use crate::types::{CfEntry, DbIdentity, EpochStart, Seq};

pub(crate) const NODE_READY: &[u8] = b"ready";
pub(crate) const NODE_BOOTSTRAPPING: &[u8] = b"bootstrapping";

/// Keys deleted per batch when clearing a CF.
const CLEAR_BATCH: usize = 1_000;

/// Everything the final bootstrap batch commits.
#[derive(Clone, Debug)]
pub(crate) struct BootstrapFinish {
    pub identity: DbIdentity,
    pub epoch_history: Vec<EpochStart>,
    pub cf_specs: Vec<CfEntry>,
    pub snapshot_seq: Seq,
    pub snapshot_crc: Option<u32>,
    pub max_entry_bytes: u64,
}

impl Store {
    fn sync_write(&self, batch: WriteBatch) -> Result<()> {
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        Ok(self.db().write_opt(batch, &wo)?)
    }

    fn stored_bootstrap_cfs(&self) -> Result<Vec<CfEntry>> {
        Ok(self.load_meta()?.map(|m| m.bootstrap_cfs).unwrap_or_default())
    }

    /// Discards all replicated state: live CFs and leftovers of an interrupted bootstrap
    /// become pending drops (physically dropped once no snapshot references them), the
    /// changelog and TTL index are cleared, and identity, history and position are removed.
    /// Persists `node_state = bootstrapping` in the same synced batch.
    pub(crate) fn reset_for_bootstrap(&self) -> Result<()> {
        let leftovers = self.stored_bootstrap_cfs()?;
        {
            let mut live = self.cfs.write();
            let mut pending = self.cfs.pending.lock().unwrap_or_else(PoisonError::into_inner);
            for e in leftovers {
                if !pending.iter().any(|p| p.entry == e) && !live.values().any(|p| p.entry == e) {
                    pending.push(PhysCf::new(e));
                }
            }
            pending.extend(std::mem::take(&mut *live).into_values());
            let pending_list: Vec<CfEntry> = pending.iter().map(|p| p.entry.clone()).collect();
            let meta_cf = self.cf(CF_META);
            let mut batch = WriteBatch::default();
            batch.put_cf(&meta_cf, key::NODE_STATE, NODE_BOOTSTRAPPING);
            batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(&[]));
            batch.put_cf(&meta_cf, key::PENDING_CF_DROPS, meta::encode_cf_list(&pending_list));
            batch.put_cf(&meta_cf, key::LAST_SEQ, 0u64.to_be_bytes());
            for k in [key::BOOTSTRAP_CFS, key::LAST_ENTRY_CRC, key::ORIGIN_IDENTITY, key::EPOCH_HISTORY, key::MAX_ENTRY_BYTES] {
                batch.delete_cf(&meta_cf, k);
            }
            // Changelog keys are 8 bytes; [0xFF; 9] sorts after all of them.
            batch.delete_range_cf(&self.cf(CF_CHANGELOG), &[][..], &[0xFFu8; 9][..]);
            self.sync_write(batch)?;
            self.delete_all_keys(&self.cf(CF_TTL_INDEX))?;
            self.set_position(Position::default());
            self.set_first_retained_seq(1);
        }
        self.drop_unreferenced_cfs()?;
        Ok(())
    }

    /// Prepares the physical CF for one bootstrap CF. It is recorded in `bootstrap_cfs`
    /// before it is created. If a pending CF with the same physical name is still referenced
    /// by a reader's snapshot, it is taken over and emptied (snapshots keep their view).
    pub(crate) fn begin_bootstrap_cf(&self, entry: &CfEntry) -> Result<()> {
        let physical = entry.physical_name();
        // Fails early with UnknownComparator, before anything is recorded.
        let options = self.tuning().cf_options(CfKind::User(&entry.spec))?;
        self.drop_unreferenced_cfs()?;
        let mut recorded = self.stored_bootstrap_cfs()?;
        recorded.push(entry.clone());
        let reused = {
            let mut pending = self.cfs.pending.lock().unwrap_or_else(PoisonError::into_inner);
            let taken = match pending.iter().position(|p| p.physical == physical) {
                Some(i) if pending[i].entry != *entry => {
                    return Err(Error::Config(format!(
                        "column family {physical} is still referenced by an open snapshot with a different spec; \
                         release it and the bootstrap will be retried"
                    )));
                }
                Some(i) => Some(pending.swap_remove(i)),
                None => None,
            };
            let pending_list: Vec<CfEntry> = pending.iter().map(|p| p.entry.clone()).collect();
            let meta_cf = self.cf(CF_META);
            let mut batch = WriteBatch::default();
            batch.put_cf(&meta_cf, key::BOOTSTRAP_CFS, meta::encode_cf_list(&recorded));
            batch.put_cf(&meta_cf, key::PENDING_CF_DROPS, meta::encode_cf_list(&pending_list));
            self.sync_write(batch)?;
            taken
        };
        if reused.is_some() || self.db().cf_handle(&physical).is_some() {
            self.clear_physical_cf(&physical)
        } else {
            self.db().create_cf(&physical, &options)?;
            // rust-rocksdb does not keep a created CF's comparator alive; the store does.
            self.retain_cf_options(options);
            Ok(())
        }
    }

    fn clear_physical_cf(&self, physical: &str) -> Result<()> {
        let handle = self
            .db()
            .cf_handle(physical)
            .ok_or_else(|| Error::corrupt(format!("physical column family {physical} is missing")))?;
        self.delete_all_keys(&handle)
    }

    /// Deletes every key of a CF, in unsynced batches of `CLEAR_BATCH` keys.
    pub(crate) fn delete_all_keys(&self, cf: &impl AsColumnFamilyRef) -> Result<()> {
        let mut batch = WriteBatch::default();
        for item in self.db().iterator_cf(cf, IteratorMode::Start) {
            batch.delete_cf(cf, item?.0);
            if batch.len() >= CLEAR_BATCH {
                self.db().write(std::mem::take(&mut batch))?;
            }
        }
        if !batch.is_empty() {
            self.db().write(batch)?;
        }
        Ok(())
    }

    /// Writes one bootstrap records message in one batch, without fsync (the final
    /// bootstrap batch is synced, which makes everything before it durable).
    pub(crate) fn write_bootstrap_records(&self, entry: &CfEntry, records: &[(Vec<u8>, Vec<u8>)]) -> Result<()> {
        let physical = entry.physical_name();
        let handle = self
            .db()
            .cf_handle(&physical)
            .ok_or_else(|| Error::corrupt(format!("bootstrap column family {physical} is missing")))?;
        let mut batch = WriteBatch::default();
        for (k, v) in records {
            batch.put_cf(&handle, k, v);
        }
        Ok(self.db().write(batch)?)
    }

    /// Commits a completed bootstrap in one synced batch and makes it live.
    pub(crate) fn finish_bootstrap(&self, f: &BootstrapFinish) -> Result<()> {
        let mut live = self.cfs.write();
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ORIGIN_IDENTITY, meta::encode_identity(&f.identity));
        batch.put_cf(&meta_cf, key::EPOCH_HISTORY, meta::encode_epoch_history(&f.epoch_history));
        batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(&f.cf_specs));
        batch.put_cf(&meta_cf, key::LAST_SEQ, f.snapshot_seq.to_be_bytes());
        match f.snapshot_crc {
            Some(crc) => batch.put_cf(&meta_cf, key::LAST_ENTRY_CRC, crc.to_be_bytes()),
            None => batch.delete_cf(&meta_cf, key::LAST_ENTRY_CRC),
        }
        batch.put_cf(&meta_cf, key::MAX_ENTRY_BYTES, f.max_entry_bytes.to_be_bytes());
        batch.put_cf(&meta_cf, key::NODE_STATE, NODE_READY);
        batch.delete_cf(&meta_cf, key::BOOTSTRAP_CFS);
        self.sync_write(batch)?;
        *live = f.cf_specs.iter().map(|e| (e.spec.name.clone(), PhysCf::new(e.clone()))).collect();
        drop(live);
        self.set_position(Position { last_seq: f.snapshot_seq, last_entry_crc: f.snapshot_crc });
        self.set_first_retained_seq(f.snapshot_seq + 1);
        Ok(())
    }

    /// Persists a new identity and epoch history together (EPOCH_ADVANCED), synced.
    pub(crate) fn adopt_identity(&self, identity: &DbIdentity, history: &[EpochStart]) -> Result<()> {
        self.write_meta(
            &[
                (key::ORIGIN_IDENTITY, &meta::encode_identity(identity)),
                (key::EPOCH_HISTORY, &meta::encode_epoch_history(history)),
            ],
            true,
        )
    }
}
```

- [ ] **Step 9: Run the tests**

Run: `cargo test --lib -- store::bootstrap slave::`
Expected: PASS: 5 tests in `store::bootstrap`, 3 in `slave::config`, 2 in `slave::state`, 5 in `slave::policy`.

- [ ] **Step 10: Update DESIGN.md and PROTOCOL.md**

In the §3.2 table, replace:

```
| `node_state` | slave | `Ready` / `Bootstrapping { target_seq }` / `Halted(reason)`, persisted (see §16.3) |
```

with:

```
| `node_state` | slave | text: `ready`, `bootstrapping` or `halted:<Reason>` (data-related reasons only); absent means `ready`. Persisted, see §16.3 |
| `bootstrap_cfs` | slave | physical CFs created by a bootstrap that has not finished, written (synced) before each is created; part of the known CF set at open, dropped by the next bootstrap reset |
```

In §10 (receiving slave), replace:

```
1. Persist `node_state = Bootstrapping { target_seq }`. Reads return `NotReady`. A crash in this state restarts the bootstrap from scratch.
```

with:

```
1. Persist `node_state = bootstrapping` together with the removal of identity, history, position and CF list (one synced batch). Reads return `NotReady`. A crash in this state restarts the bootstrap from scratch.
```

and replace:

```
5. Write the final batch: `origin_identity`, `cf_specs`, `last_seq = S`, `last_entry_crc`, `max_entry_bytes`, `node_state = Ready`.
```

with:

```
5. Write the final batch (synced): `origin_identity`, `epoch_history`, `cf_specs`, `last_seq = S`, `last_entry_crc`, `max_entry_bytes`, `node_state = ready`, and clear `bootstrap_cfs`.
```

In the §16.3 table, replace:

```
| `Ready` | `Connecting` or `Streaming` |
| `Bootstrapping { target_seq }` | `Bootstrapping { .. }`. After a restart, the bootstrap starts over. |
| `Halted(reason)`, data-related reasons only | `Halted(reason)`. The slave does not contact its upstream until `clear_halt`. |
```

with:

```
| `ready` (or absent) | `Connecting` or `Streaming` |
| `bootstrapping` | `Bootstrapping { .. }`. After a restart, the bootstrap starts over. |
| `halted:<Reason>`, data-related reasons only | `Halted(reason)`. The slave does not contact its upstream until `clear_halt`. |
```

In PROTOCOL.md §10, replace:

```
| `CORRUPT` | `DATA_LOSS` | no | Stored data failed an integrity check. |
```

with:

```
| `CORRUPT` | `DATA_LOSS` | client: yes, with backoff | Stored data failed an integrity check on the server. The client's own data is intact, so it backs off and retries; the server's operator must repair it. |
```

In the PROTOCOL.md §11.2 table, after the row `` | `REJECT` / `NOT_READY` | Back off and retry the handshake. | `` add:

```
| A call or stream fails with `CORRUPT` (the server's stored data) | Back off and retry. The client's data is not affected. |
```

In §16 `SlaveConfig`, replace:

```
                                             // 0 = real-time push; larger = batch like Kafka fetch.min.bytes
    pub rocksdb: RocksTuning,
}
```

with:

```
                                             // 0 = real-time push; larger = batch like Kafka fetch.min.bytes
    pub heartbeat: Duration,                 // when serving downstream; default 5 s
    pub connect_timeout: Duration,           // default 5 s
    pub rocksdb: RocksTuning,
    pub clock: Arc<dyn Clock>,               // TTL read filtering and retention
}
```

and in `SlaveStatus`, after `pub last_error: Option<String>, …` add:

```
    pub listener_failed: bool,               // the listener panicked; disabled until restart
```

In DESIGN.md §6, replace:

```
- Writes are not synced on slaves. A slave that crashes resumes from its durable `last_seq` and fetches the rest again from upstream.
```

with:

```
- Writes are not synced on slaves by default (`SlaveConfig::sync_writes = false`). A slave that crashes resumes from its durable `last_seq` and fetches the rest again from upstream. Its acks (§11) then promise that an entry is in its write-ahead log, which survives a process crash but not a power loss. With `sync_writes = true` every applied entry is fsynced, so an acknowledged entry survives a power loss of the replica too.
```

In DESIGN.md §16 (SlaveConfig), after:

```
    pub connect_timeout: Duration,           // default 5 s
```

add:

```
    pub sync_writes: bool,                   // fsync each applied entry; default false
```

In DESIGN.md §18 (table), replace:

```
| Slave fsync | off |
```

with:

```
| Slave fsync | off (`SlaveConfig::sync_writes`) |
```

- [ ] **Step 11: Commit**

```bash
git add src DESIGN.md PROTOCOL.md
git commit -m "feat: slave config, persisted states, policy table and crash-safe bootstrap store helpers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

### Task 8: Slave runtime — driver, apply, bootstrap install, read API, harness and core end-to-end tests

**Files:**
- Create: `src/slave/inner.rs`, `src/slave/client.rs`, `src/slave/driver.rs`, `src/slave/apply.rs`, `src/slave/bootstrap.rs`, `src/slave/maintenance.rs`, `tests/support/mod.rs`, `tests/replication.rs`
- Modify: `src/slave/mod.rs` (replace), `src/lib.rs`, `src/master/maintenance.rs` (replace), `src/types.rs`, `src/net/mod.rs`, `src/net/convert.rs`, `src/config.rs` (stale `allow`s), `DESIGN.md` (§6, §15), `PROTOCOL.md` (§11.2)

**Interfaces:**
- Consumes: Task 7 everything; Task 6 `pb::{TailClientMessage, TailAck}` (the bidirectional Tail), `Master::replicas` (tests); Task 5 `server::await_sole_owner`, `ServerHandle`, `failpoint::fire`; Task 2 `Store::{entry_crc}`, meta fields; Task 1 `net::{DECODE_SLACK, MD_*}`, conversions including `checked_history` (an invalid history in an `EPOCH_ADVANCED` answer or a bootstrap header halts `Corrupt`); Plan 1 `Store::{open, prepare, commit, position, live_cf, live_ctx, read_snapshot, entries_after, trim_changelog, drop_unreferenced_cfs}`, `epoch_for`, `Digest`, `envelope::decode`, `validate_cf_name`.
- Produces:
  - Tail requests carry `SlaveConfig::fetch` (`min_batch_bytes`, `max_wait_ms`, `max_batch_bytes`); each `EntryBatch` is applied entry by entry, each its own atomic write (`commit(sync = cfg.sync_writes)`); Tail decode limit `max(max_entry_bytes, fetch.max_batch_bytes) + 64 KiB`.
  - The Tail request stream is `start`, then `ack { applied_seq }` whenever a `watch::Sender<Seq>` changes: after each applied batch and on every heartbeat (`send_replace` of the current `last_seq`), so acks that back up coalesce to the latest. A stream that ends without a status is a `Retry` (back off, handshake again).
  - `pub struct Slave` with `async open(SlaveConfig) -> Result<Slave>`, `status() -> SlaveStatus`, `applied_seq() -> Seq`, `async wait_for_seq(seq, timeout) -> Result<()>` (`Err(Halted(r))`, `Err(Timeout)`), `watch_state() -> watch::Receiver<SlaveState>`, `snapshot() -> Result<ReadSnapshot>` (`Err(NotReady)` without data), `async shutdown(self) -> Result<()>`; `impl ReadApi for Slave` (all reads `Err(NotReady)` without data), `impl LogApi for Slave`.
  - `slave::inner::Inner` with `pub store, pub cfg, pub state_tx, pub seq_tx, pub reset_tx, pub shutdown_tx, pub origin_last_seq, pub upstream_last_seq, pub max_entry_bytes, pub force_bootstrap, pub halt_cleared` and `new(Store, SlaveConfig, &MetaSnapshot)`, `policies()`, `ident()`, `set_ident(..)`, `is_ready()`, `now_ms()`, `state()`, `set_state(SlaveState)`, `halt(HaltReason)` (a synced write: called through `background::blocking`), `reset_downstream()`, `upstream_chain()`, `note_upstream(&pb::HandshakeResponse) -> Option<u64>` (the changed `max_entry_bytes`, persisted by the driver on a blocking thread), `note_positions(upstream, origin)`, `note_contact()`, `set_last_error(Option<String>)`, `notify(impl FnOnce(&dyn ReplicationListener))`, `status()`.
  - `slave::client::{Client, connect(&SlaveConfig) -> Result<Client>, connect_upstream(&UpstreamConfig, identifier, connect_timeout) -> Result<Client>}`; `slave::driver::{SessionEnd { Retry(String), Again, Halt(HaltReason) }, spawn(&Arc<Inner>) -> JoinHandle<()>, end_for, status_end}` (the driver holds `Weak<Inner>` while waiting; `Inner.halt_cleared` is an `Arc<Notify>` it shares); `slave::maintenance::spawn(&Arc<Inner>) -> JoinHandle<()>` (Weak, upgraded per tick); `slave::apply::{Applied, apply(&Arc<Inner>, LogEntry)}`; `slave::bootstrap::install(&Arc<Inner>, &Client, &pb::HandshakeResponse) -> Result<Vec<u8> /* lease */, SessionEnd>`.
  - Harness (`tests/support/mod.rs`): `Cluster { clock } (new, path, serve, master_config, master, restart_master, restart_master_after_backup, slave_config, slave)`, `wait_until`, `wait_state`, `Node`, `Dump`, `dump`, `assert_same`, `assert_converged`, `edge_cases`, `write_edge_cases`, `check_edge_cases`, `copy_dir`, `restore_dir`, `RecordingListener`, constants `T0`, `HEARTBEAT`, `WAIT`.

- [ ] **Step 1: Write the harness**

Create `tests/support/mod.rs`:

```rust
//! Multi-node test harness: real nodes in one process, talking gRPC over 127.0.0.1.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use replicated_rocksdb::{
    AllowAll, Backoff, CfSpec, IterMode, LogApi, LogEntry, ManualClock, Master, MasterConfig, ReadApi,
    ReplicationListener, Seq, ServeConfig, Slave, SlaveConfig, SlaveState, UpstreamConfig, ValueMeta,
};

pub const T0: u64 = 1_700_000_000_000;
pub const HEARTBEAT: Duration = Duration::from_millis(500);
pub const WAIT: Duration = Duration::from_secs(30);

pub fn localhost() -> IpAddr {
    IpAddr::from([127, 0, 0, 1])
}

/// Nodes of one test: a shared manual clock and a temp directory per node name.
pub struct Cluster {
    pub clock: Arc<ManualClock>,
    dir: tempfile::TempDir,
}

impl Cluster {
    pub fn new() -> Self {
        Cluster { clock: Arc::new(ManualClock::new(T0)), dir: tempfile::tempdir().unwrap() }
    }

    pub fn path(&self, name: &str) -> PathBuf {
        self.dir.path().join(name)
    }

    /// Plaintext, `AllowAll`, port 0.
    pub fn serve(&self) -> ServeConfig {
        ServeConfig::new(localhost(), 0, Arc::new(AllowAll))
    }

    /// Serving master with a manual clock and background maintenance effectively off.
    pub fn master_config(&self, name: &str) -> MasterConfig {
        let mut cfg = MasterConfig::new(name, self.path(name));
        cfg.clock = self.clock.clone();
        cfg.heartbeat = HEARTBEAT;
        cfg.retention.check_interval = Duration::from_secs(3600);
        cfg.reaper.interval = Duration::from_secs(3600);
        cfg.serve = Some(self.serve());
        cfg
    }

    pub async fn master(&self, name: &str) -> Master {
        Master::open(self.master_config(name)).await.unwrap()
    }

    /// Reopens a master on the port it served before, so its slaves find it again.
    pub async fn restart_master(&self, m: Master, cfg: MasterConfig) -> Master {
        let port = m.serve_addr().unwrap().port();
        m.shutdown().await.unwrap();
        let mut cfg = cfg;
        cfg.serve.as_mut().unwrap().port = port;
        Master::open(cfg).await.unwrap()
    }

    /// Stops `m`, copies its directory to `backup` (a consistent backup of a stopped node),
    /// and reopens it on the same port.
    pub async fn restart_master_after_backup(&self, m: Master, name: &str, backup: &str) -> Master {
        let port = m.serve_addr().unwrap().port();
        m.shutdown().await.unwrap();
        copy_dir(&self.path(name), &self.path(backup));
        let mut cfg = self.master_config(name);
        cfg.serve.as_mut().unwrap().port = port;
        Master::open(cfg).await.unwrap()
    }

    /// A serving slave with fast reconnects and a recording listener.
    pub fn slave_config(&self, name: &str, upstream: SocketAddr) -> (SlaveConfig, Arc<RecordingListener>) {
        let listener = Arc::new(RecordingListener::default());
        let mut cfg = SlaveConfig::new(name, self.path(name), UpstreamConfig::from_addr(upstream));
        cfg.clock = self.clock.clone();
        cfg.heartbeat = HEARTBEAT;
        cfg.retention.check_interval = Duration::from_secs(3600);
        cfg.reconnect = Backoff { initial: Duration::from_millis(20), max: Duration::from_millis(200), factor: 2 };
        cfg.listener = Some(listener.clone());
        (cfg, listener)
    }

    pub async fn slave(&self, name: &str, upstream: SocketAddr) -> (Slave, Arc<RecordingListener>) {
        let (cfg, listener) = self.slave_config(name, upstream);
        (Slave::open(cfg).await.unwrap(), listener)
    }
}

/// Polls `f` every 20 ms; panics after 30 s.
pub async fn wait_until(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + WAIT;
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

pub async fn wait_state(s: &Slave, what: &str, f: impl Fn(&SlaveState) -> bool) {
    wait_until(what, || f(&s.status().state)).await;
}

/// Read and log access, as master and slaves both offer.
pub trait Node: ReadApi + LogApi {}
impl<T: ReadApi + LogApi> Node for T {}

/// Every live CF with every visible `(key, value, meta)`, read through the public API.
pub type Dump = BTreeMap<String, Vec<(Vec<u8>, Vec<u8>, ValueMeta)>>;

pub fn dump(n: &dyn Node) -> Dump {
    let mut out = Dump::new();
    for spec in n.list_cfs() {
        let rows = n
            .iter(&spec.name, IterMode::Start)
            .unwrap()
            .map(|r| {
                let (k, _) = r.unwrap();
                let (v, meta) = n.get_with_meta(&spec.name, &k).unwrap().expect("iterated key is readable");
                (k, v, meta)
            })
            .collect();
        out.insert(spec.name, rows);
    }
    out
}

/// Exact equality, with a short message instead of megabytes of Debug output.
pub fn assert_same(what: &str, a: &Dump, b: &Dump) {
    assert_eq!(a.keys().collect::<Vec<_>>(), b.keys().collect::<Vec<_>>(), "{what}: CF lists differ");
    for (cf, rows) in a {
        let other = &b[cf];
        assert_eq!(rows.len(), other.len(), "{what}: CF {cf} has {} vs {} keys", rows.len(), other.len());
        for (x, y) in rows.iter().zip(other) {
            assert!(
                x == y,
                "{what}: CF {cf} differs at a key of {} bytes (meta {:?} vs {:?})",
                x.0.len(),
                x.2,
                y.2
            );
        }
    }
}

/// Waits until every slave has applied the master's `last_seq`, then compares dumps exactly.
pub async fn assert_converged(master: &dyn Node, slaves: &[(&str, &Slave)]) {
    let seq = master.last_seq();
    let expected = dump(master);
    for (name, s) in slaves {
        s.wait_for_seq(seq, WAIT).await.unwrap_or_else(|e| panic!("{name} did not reach {seq}: {e}"));
        assert_eq!(s.last_seq(), seq, "{name} is past the master");
        assert_same(name, &expected, &dump(*s));
    }
}

/// Zero-length and NUL-byte keys and values, a 1 MiB key, a value close to the 16 MiB entry limit.
pub fn edge_cases() -> Vec<(Vec<u8>, Vec<u8>)> {
    vec![
        (b"".to_vec(), b"".to_vec()),
        (b"\0".to_vec(), b"\0".to_vec()),
        (b"\0k\0".to_vec(), b"\0\0\0".to_vec()),
        (vec![0; 64], vec![0; 64]),
        (vec![0x5A; 1 << 20], b"".to_vec()),
        (b"big".to_vec(), vec![0xC3; (16 << 20) - 4096]),
    ]
}

/// Creates `cf` and writes every edge case, one entry each.
pub fn write_edge_cases(m: &Master, cf: &str) {
    m.create_cf(CfSpec::new(cf)).unwrap();
    for (k, v) in edge_cases() {
        m.put(cf, &k, &v).unwrap();
    }
}

/// Checks the edge cases through point reads on `n`.
pub fn check_edge_cases(n: &dyn Node, cf: &str) {
    for (k, v) in edge_cases() {
        let got = n.get(cf, &k).unwrap().unwrap_or_else(|| panic!("key of {} bytes missing", k.len()));
        assert!(got == v, "value of key with {} bytes differs", k.len());
    }
    assert_eq!(n.get(cf, b"").unwrap(), Some(vec![]), "an empty value is a value");
    assert_eq!(n.get(cf, b"\0\0").unwrap(), None, "b\"\\0\\0\" was never written");
}

/// Copies a closed node directory (a consistent backup of a stopped node).
pub fn copy_dir(from: &Path, to: &Path) {
    std::fs::create_dir_all(to).unwrap();
    for entry in std::fs::read_dir(from).unwrap() {
        let entry = entry.unwrap();
        let target = to.join(entry.file_name());
        if entry.file_type().unwrap().is_dir() {
            copy_dir(&entry.path(), &target);
        } else {
            std::fs::copy(entry.path(), target).unwrap();
        }
    }
}

/// Replaces `dir` with a copy of `backup`.
pub fn restore_dir(backup: &Path, dir: &Path) {
    std::fs::remove_dir_all(dir).unwrap();
    copy_dir(backup, dir);
}

/// Records listener calls. Violations are collected, not panicked (a panic would disable
/// the listener): within a process, entry seqs must be strictly increasing between bootstraps.
#[derive(Default)]
pub struct RecordingListener {
    pub entries: Mutex<Vec<Seq>>,
    pub bootstraps_begun: AtomicUsize,
    pub bootstraps: AtomicUsize,
    pub states: Mutex<Vec<SlaveState>>,
    pub violations: Mutex<Vec<String>>,
    last: AtomicU64,
}

impl RecordingListener {
    pub fn bootstraps(&self) -> usize {
        self.bootstraps.load(Ordering::SeqCst)
    }

    pub fn entries(&self) -> Vec<Seq> {
        self.entries.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub fn states(&self) -> Vec<SlaveState> {
        self.states.lock().unwrap_or_else(PoisonError::into_inner).clone()
    }

    pub fn assert_no_violations(&self) {
        let v = self.violations.lock().unwrap_or_else(PoisonError::into_inner);
        assert!(v.is_empty(), "listener violations: {v:?}");
    }
}

impl ReplicationListener for RecordingListener {
    fn on_entry(&self, entry: &LogEntry) {
        let last = self.last.swap(entry.seq, Ordering::SeqCst);
        if entry.seq <= last {
            self.violations.lock().unwrap_or_else(PoisonError::into_inner).push(format!("entry {} after {last}", entry.seq));
        }
        self.entries.lock().unwrap_or_else(PoisonError::into_inner).push(entry.seq);
    }

    fn on_bootstrap_begin(&self, _: Seq) {
        self.bootstraps_begun.fetch_add(1, Ordering::SeqCst);
    }

    fn on_bootstrap_complete(&self, snapshot_seq: Seq) {
        self.last.store(snapshot_seq, Ordering::SeqCst);
        self.bootstraps.fetch_add(1, Ordering::SeqCst);
    }

    fn on_state_change(&self, state: &SlaveState) {
        self.states.lock().unwrap_or_else(PoisonError::into_inner).push(state.clone());
    }
}
```

- [ ] **Step 2: Write the failing end-to-end tests**

Create `tests/replication.rs`:

```rust
//! Core replication, end to end: a master and slaves in one process over real gRPC.

mod support;

use std::net::TcpListener;
use std::time::Duration;

use replicated_rocksdb::{
    Batch, CfSpec, Direction, Error, FetchConfig, IterMode, LogApi, Op, ReadApi, Slave, SlaveState,
};
use support::{assert_converged, check_edge_cases, dump, wait_state, wait_until, write_edge_cases, Cluster, WAIT};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slave_bootstraps_then_tails_every_kind_of_write() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("users")).unwrap();
    m.put("users", b"alice", b"1").unwrap();
    m.put("users", b"bob", b"2").unwrap();

    let (b, lb) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(lb.bootstraps(), 1);
    assert!(lb.entries().is_empty(), "no per-record events during bootstrap");
    let boot_seq = b.last_seq();

    m.create_cf(CfSpec::new("orders")).unwrap();
    let mut batch = Batch::new();
    batch.put("users", b"carol", b"3").put("orders", b"o1", b"x").delete("users", b"alice");
    m.write(batch).unwrap();
    m.delete_range("users", b"a", b"c").unwrap();
    m.create_cf(CfSpec::new("tmp")).unwrap();
    m.put("tmp", b"k", b"v").unwrap();
    m.drop_cf("tmp").unwrap();
    m.create_cf(CfSpec::new("tmp")).unwrap();
    assert_converged(&m, &[("b", &b)]).await;

    assert_eq!(b.list_cfs(), m.list_cfs());
    assert_eq!(b.get("users", b"bob").unwrap(), None, "range-deleted");
    assert_eq!(b.get("users", b"carol").unwrap(), Some(b"3".to_vec()));
    assert_eq!(b.get("tmp", b"k").unwrap(), None, "re-created CF starts empty");
    assert_eq!(b.get_with_meta("orders", b"o1").unwrap(), m.get_with_meta("orders", b"o1").unwrap());
    assert_eq!(lb.entries(), ((boot_seq + 1)..=m.last_seq()).collect::<Vec<_>>());
    lb.assert_no_violations();

    // The slave's log holds the master's entries byte for byte.
    let ours: Vec<_> = b.entries_since(boot_seq).unwrap().map(Result::unwrap).collect();
    let theirs: Vec<_> = m.entries_since(boot_seq).unwrap().map(Result::unwrap).collect();
    assert_eq!(ours, theirs);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn read_api_on_a_slave() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for k in [&b"a"[..], b"ab", b"abc", b"b"] {
        m.put("c", k, k).unwrap();
    }
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("b", &b)]).await;

    assert_eq!(b.multi_get("c", &[b"a", b"zz"]).unwrap(), vec![Some(b"a".to_vec()), None]);
    let keys = |it: replicated_rocksdb::DbIter<'_>| it.map(|r| r.unwrap().0).collect::<Vec<_>>();
    assert_eq!(keys(b.prefix_iter("c", b"ab").unwrap()), vec![b"ab".to_vec(), b"abc".to_vec()]);
    assert_eq!(
        keys(b.iter("c", IterMode::From(b"ab", Direction::Reverse)).unwrap()),
        vec![b"ab".to_vec(), b"a".to_vec()]
    );
    assert!(matches!(b.get("nope", b"k"), Err(Error::NoSuchCf(_))));

    // A snapshot keeps a dropped CF readable on the slave too.
    let snap = b.snapshot().unwrap();
    m.drop_cf("c").unwrap();
    b.wait_for_seq(m.last_seq(), WAIT).await.unwrap();
    assert!(b.list_cfs().is_empty());
    assert_eq!(snap.get("c", b"abc").unwrap(), Some(b"abc".to_vec()));
    assert_eq!(snap.seq(), m.last_seq() - 1);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn ttl_values_replicate_and_the_reapers_delete_replicates() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    m.put_with_ttl("c", b"session", b"s1", Duration::from_secs(10)).unwrap();
    m.put_with_ttl("c", b"", b"", Duration::from_secs(10)).unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let (_, meta) = b.get_with_meta("c", b"session").unwrap().unwrap();
    assert_eq!(meta.expire_at_ms, Some(support::T0 + 10_000));

    c.clock.advance(Duration::from_secs(10));
    assert_eq!(b.get("c", b"session").unwrap(), None, "expired values are hidden on slaves before reaping");
    let before = m.last_seq();
    assert_eq!(m.reap_expired().unwrap().deleted, 2);
    assert_converged(&m, &[("b", &b)]).await;
    let reap: Vec<Op> = b.entries_since(before).unwrap().flat_map(|e| e.unwrap().decode_ops().unwrap()).collect();
    assert_eq!(reap.len(), 2);
    assert!(reap.iter().all(|op| matches!(op, Op::Delete { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn edge_case_data_through_tail_and_bootstrap() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("seed")).unwrap();
    let (tailing, lt) = c.slave("tailing", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("tailing", &tailing)]).await;

    write_edge_cases(&m, "edge");
    assert_converged(&m, &[("tailing", &tailing)]).await;
    assert_eq!(lt.bootstraps(), 1, "the edge cases arrived through Tail");
    check_edge_cases(&tailing, "edge");

    let (booted, lb) = c.slave("booted", m.serve_addr().unwrap()).await;
    assert_converged(&m, &[("booted", &booted)]).await;
    assert_eq!(lb.bootstraps(), 1);
    assert!(lb.entries().is_empty(), "the edge cases arrived through Bootstrap");
    check_edge_cases(&booted, "edge");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn restart_resumes_without_bootstrap() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let seq = b.last_seq();
    b.shutdown().await.unwrap();

    for i in 0..10u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let (cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    let b = Slave::open(cfg).await.unwrap();
    assert_eq!(b.get("c", &[0]).unwrap(), None, "local data is readable at once");
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 0);
    assert_eq!(listener.entries(), ((seq + 1)..=m.last_seq()).collect::<Vec<_>>());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_and_wait_for_seq() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, listener) = c.slave("b", m.serve_addr().unwrap()).await;
    wait_state(&b, "streaming", |s| *s == SlaveState::Streaming).await;
    assert_eq!(*b.watch_state().borrow(), SlaveState::Streaming);
    m.put("c", b"k", b"v").unwrap();
    b.wait_for_seq(2, WAIT).await.unwrap();
    wait_until("heartbeat positions", || b.status().upstream_last_seq == 2).await;
    let s = b.status();
    assert_eq!(s.state, SlaveState::Streaming);
    assert_eq!(s.origin_identity, Some(m.identity()));
    assert_eq!((s.applied_seq, s.origin_last_seq), (2, 2));
    assert_eq!(s.upstream_chain, vec!["a".to_string()]);
    assert!(s.last_contact.is_some());
    assert_eq!(s.last_error, None);
    assert!(!s.listener_failed);
    assert!(matches!(b.wait_for_seq(3, Duration::from_millis(100)).await, Err(Error::Timeout)));
    // The master sees B's acknowledgements (addendum §1).
    wait_until("b acknowledged 2", || m.replicas().iter().any(|r| r.applied_seq == 2 && r.in_sync)).await;
    let r = m.replicas();
    assert_eq!(r.len(), 1);
    assert_eq!((r[0].node_id.as_str(), r[0].sent_seq, r[0].lag_entries, r[0].lag_ms), ("b", 2, 0, Some(0)));
    assert_eq!((r[0].epoch, r[0].peer_verified), (1, false));
    assert!(r[0].remote_addr.ip().is_loopback());
    let states = listener.states();
    assert!(states.contains(&SlaveState::Bootstrapping { target_seq: 1, records: 0 }), "{states:?}");
    assert_eq!(states.last(), Some(&SlaveState::Streaming));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_slave_without_data_is_not_ready_and_reports_the_connection_error() {
    let c = Cluster::new();
    let nobody = TcpListener::bind("127.0.0.1:0").unwrap().local_addr().unwrap(); // closed again at once
    let (b, _) = c.slave("b", nobody).await;
    assert!(matches!(b.get("c", b"k"), Err(Error::NotReady)));
    assert!(matches!(b.snapshot(), Err(Error::NotReady)));
    assert!(matches!(b.wait_for_seq(1, Duration::from_millis(200)).await, Err(Error::Timeout)));
    wait_until("a connection error", || b.status().last_error.is_some()).await;
    assert_eq!(b.status().state, SlaveState::Connecting);
    assert!(dump(&b).is_empty());
}

/// With fetch batching on, entries arrive in batches but are still applied one entry per
/// atomic write, each delivered to the listener.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_batching_slave_applies_every_entry_on_its_own() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.fetch = FetchConfig { min_batch_bytes: 64 << 10, max_wait: Duration::from_millis(150), max_batch_bytes: 16 << 10 };
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let first = m.last_seq() + 1;
    for i in 0..200u32 {
        m.put("c", &i.to_be_bytes(), &[3; 300]).unwrap();
    }
    write_edge_cases(&m, "edge");
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.entries(), (first..=m.last_seq()).collect::<Vec<_>>());
    check_edge_cases(&b, "edge");
    listener.assert_no_violations();
    let fetch = m.replicas()[0].fetch;
    assert_eq!((fetch.min_batch_bytes, fetch.max_wait, fetch.max_batch_bytes), (64 << 10, Duration::from_millis(150), 16 << 10));
}

/// `sync_writes = true` fsyncs every applied entry; replication and resume are unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_sync_writes_slave_replicates_and_resumes() {
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.sync_writes = true;
    let b = Slave::open(cfg.clone()).await.unwrap();
    for i in 0..20u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    assert_converged(&m, &[("b", &b)]).await;
    b.shutdown().await.unwrap();
    m.put("c", b"later", b"v").unwrap();
    let (mut cfg, listener) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.sync_writes = true;
    let b = Slave::open(cfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert_eq!(listener.bootstraps(), 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_panicking_listener_is_disabled_and_replication_goes_on() {
    struct Panics;
    impl replicated_rocksdb::ReplicationListener for Panics {
        fn on_entry(&self, _: &replicated_rocksdb::LogEntry) {
            panic!("listener bug");
        }
    }
    let c = Cluster::new();
    let m = c.master("a").await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.listener = Some(std::sync::Arc::new(Panics));
    let b = Slave::open(cfg).await.unwrap();
    b.wait_for_seq(1, WAIT).await.unwrap();
    m.put("c", b"k1", b"v").unwrap();
    m.put("c", b"k2", b"v").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    assert!(b.status().listener_failed);
}
```

- [ ] **Step 3: Run them to confirm they fail**

Run: `cargo test --test replication`
Expected: FAIL to compile: `unresolved import replicated_rocksdb::Slave`.

- [ ] **Step 4: Add the background helpers and the slave's shared state**

Create `src/background.rs` (periodic ticks, logged maintenance runs, and the one place that turns a failed `spawn_blocking` into `Error::Io`):

```rust
//! Helpers for background and blocking work, shared by master and slave.

use std::sync::Arc;
use std::time::Duration;

use tokio::time::{interval_at, Instant, Interval, MissedTickBehavior};

use crate::error::{Error, Result};

/// A tick every `period`, the first one after one period; missed ticks are delayed.
pub(crate) fn every(period: Duration) -> Interval {
    let mut t = interval_at(Instant::now() + period, period);
    t.set_missed_tick_behavior(MissedTickBehavior::Delay);
    t
}

/// Runs one unit of maintenance on a blocking thread. Failures are logged, not returned.
pub(crate) async fn run_logged<T: Send + Sync + 'static>(inner: Arc<T>, task: &'static str, f: fn(&T) -> Result<()>) {
    match tokio::task::spawn_blocking(move || f(&inner)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(task, error = %e, "maintenance task failed"),
        Err(e) => tracing::error!(task, error = %e, "maintenance task panicked"),
    }
}

/// Runs `f` (RocksDB I/O, fsync) on a blocking thread, off the async workers.
/// A panic in `f` becomes `Error::Io`.
pub(crate) async fn blocking<T: Send + 'static>(f: impl FnOnce() -> Result<T> + Send + 'static) -> Result<T> {
    tokio::task::spawn_blocking(f)
        .await
        .map_err(|e| Error::Io(std::io::Error::other(format!("blocking task failed: {e}"))))?
}
```

Add to `src/lib.rs`:

```rust
pub(crate) mod background;
```

Replace `src/master/maintenance.rs` with the same task using these helpers (behaviour unchanged):

```rust
//! Background work: retention, TTL reaping and physical CF drops.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use super::writer::Inner;
use crate::background::{every, run_logged};
use crate::error::Result;

const CF_DROP_INTERVAL: Duration = Duration::from_secs(5);

/// The task holds only a `Weak<Inner>`, upgraded for the duration of one tick, so dropping the
/// `Master` releases the database (and its LOCK) without waiting for the task.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let mut shutdown = inner.shutdown_tx.subscribe();
    let (retention_every, reaper_every) = (inner.cfg.retention.check_interval, inner.cfg.reaper.interval);
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        let mut retention = every(retention_every);
        let mut reaper = every(reaper_every);
        let mut cf_drops = every(CF_DROP_INTERVAL);
        loop {
            let (name, f): (&'static str, fn(&Inner) -> Result<()>) = tokio::select! {
                _ = shutdown.changed() => break,
                _ = retention.tick() => ("retention", |i| i.trim_changelog().map(drop)),
                _ = reaper.tick() => ("reaper", |i| i.reap_round().map(drop)),
                _ = cf_drops.tick() => ("cf-drop", |i| i.store.drop_unreferenced_cfs().map(drop)),
            };
            let Some(inner) = weak.upgrade() else { break };
            run_logged(inner, name, f).await;
        }
    })
}
```

`epoch_for` is now used by the slave: in `src/types.rs`, delete the line `#[allow(dead_code)] // used by the replication server (Plan 2)` above `pub(crate) fn epoch_for`. The client below uses the wire helpers Task 1 kept behind `allow`s: delete every line `#[allow(dead_code)] // used by the replication client (later Plan 2 task)` (two in `src/net/mod.rs`, above `DECODE_SLACK` and `reason_of`; four in `src/net/convert.rs`, above `cf_from_pb`, `epochs_from_pb`, `checked_history` and `entry_from_pb`) and the indented one above `TlsClientConfig::to_tonic` in `src/config.rs`.

Create `src/slave/inner.rs`:

```rust
//! Shared state of one slave: the store plus everything the driver, the API and the
//! replication server (when serving) read.

use std::panic::{catch_unwind, AssertUnwindSafe};
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::{Arc, Mutex, PoisonError, RwLock};
use std::time::SystemTime;

use tokio::sync::{watch, Notify};

use super::config::SlaveConfig;
use super::policy::Policies;
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
            cfg,
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
        })
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

- [ ] **Step 5: Implement the upstream client**

Create `src/slave/client.rs`:

```rust
//! The upstream connection: a lazily connected channel whose interceptor adds the protocol
//! metadata and credentials to every call (PROTOCOL.md §3.2).

use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use super::config::{SlaveConfig, UpstreamConfig};
use crate::auth::CredentialsProvider;
use crate::codec::PROTOCOL_VERSION;
use crate::error::{Error, Result};
use crate::net::{MD_AUTHORIZATION, MD_NODE_ID, MD_PROTOCOL_VERSION};
use crate::pb::replication_client::ReplicationClient;

#[derive(Clone)]
pub(crate) struct CallMeta {
    node_id: AsciiMetadataValue,
    credentials: Arc<dyn CredentialsProvider>,
}

/// RFC 6750 `b64token`: `1*( ALPHA / DIGIT / "-" / "." / "_" / "~" / "+" / "/" ) *"="`.
fn is_b64token(t: &[u8]) -> bool {
    let body = t.iter().position(|&b| b == b'=').unwrap_or(t.len());
    body > 0
        && t[..body].iter().all(|b| b.is_ascii_alphanumeric() || b"-._~+/".contains(b))
        && t[body..].iter().all(|&b| b == b'=')
}

impl Interceptor for CallMeta {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let md = req.metadata_mut();
        md.insert(MD_PROTOCOL_VERSION, AsciiMetadataValue::from(PROTOCOL_VERSION));
        md.insert(MD_NODE_ID, self.node_id.clone());
        if let Some(token) = self.credentials.token() {
            if !is_b64token(&token) {
                return Err(Status::unauthenticated("the configured token has characters outside RFC 6750 b64token"));
            }
            let mut v = b"Bearer ".to_vec();
            v.extend_from_slice(&token);
            let value = AsciiMetadataValue::try_from(v.as_slice())
                .map_err(|_| Status::unauthenticated("the configured token is not valid header text"))?;
            md.insert(MD_AUTHORIZATION, value);
        }
        Ok(req)
    }
}

pub(crate) type Client = ReplicationClient<InterceptedService<Channel, CallMeta>>;

/// A client for the configured upstream. Connects on first use and reconnects by itself.
pub(crate) fn connect(cfg: &SlaveConfig) -> Result<Client> {
    connect_upstream(&cfg.upstream, &cfg.identifier, cfg.connect_timeout)
}

/// A client for `up`, identifying itself as `identifier`.
pub(crate) fn connect_upstream(up: &UpstreamConfig, identifier: &str, connect_timeout: Duration) -> Result<Client> {
    let mut endpoint = Endpoint::from_shared(up.uri())
        .map_err(|e| Error::Config(format!("upstream address {}: {e}", up.uri())))?
        .connect_timeout(connect_timeout)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    if let Some(tls) = &up.tls {
        endpoint = endpoint
            .tls_config(tls.to_tonic(&up.server))
            .map_err(|e| Error::Config(format!("upstream TLS: {e}")))?;
    }
    crate::config::validate_identifier(identifier)?;
    let node_id = AsciiMetadataValue::try_from(identifier)
        .map_err(|_| Error::Config("identifier is not valid header text".into()))?;
    let meta = CallMeta { node_id, credentials: up.credentials.clone() };
    Ok(ReplicationClient::with_interceptor(endpoint.connect_lazy(), meta))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn b64token_rules() {
        for ok in [&b"abc"[..], b"A-Z._~+/09", b"abc==", b"x="] {
            assert!(is_b64token(ok), "{ok:?}");
        }
        for bad in [&b""[..], b"=", b"a b", b"a=b", b"\xff", b"tok\n"] {
            assert!(!is_b64token(bad), "{bad:?}");
        }
    }
}
```

- [ ] **Step 6: Implement applying entries**

Create `src/slave/apply.rs`:

```rust
//! Applying one received entry (PROTOCOL.md §7.4, DESIGN.md §6).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use super::config::GapPolicy;
use super::driver::SessionEnd;
use super::inner::Inner;
use super::state::HaltReason;
use crate::error::Error;
use crate::failpoint;
use crate::types::{epoch_for, LogEntry};

pub(crate) enum Applied {
    Committed,
    /// Already applied (re-sent after a reconnect).
    Skipped,
    End(SessionEnd),
}

enum Failure {
    Prepare(Error),
    Commit(Error),
}

pub(crate) async fn apply(inner: &Arc<Inner>, entry: LogEntry) -> Applied {
    let seq = entry.seq;
    // 1. CRC
    if let Err(e) = entry.verify_crc() {
        tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "received a corrupt entry");
        return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
    }
    // 2. Epoch: the upstream's history changed; the next handshake brings it.
    let history = inner.ident().map(|(_, h)| h).unwrap_or_default();
    if epoch_for(&history, seq) != Some(entry.epoch) {
        tracing::info!(node = %inner.cfg.identifier, seq, epoch = entry.epoch, "entry epoch differs from local history; handshaking again");
        return Applied::End(SessionEnd::Again);
    }
    // 3, 4. Exactly once, no gaps.
    let last = inner.store.position().last_seq;
    if seq <= last {
        return Applied::Skipped;
    }
    if seq != last + 1 {
        tracing::warn!(node = %inner.cfg.identifier, seq, last, "gap in the tail stream");
        return Applied::End(match inner.cfg.on_log_gap {
            GapPolicy::Halt => SessionEnd::Halt(HaltReason::LogGap),
            GapPolicy::Rebootstrap => {
                inner.force_bootstrap.store(true, Ordering::SeqCst);
                SessionEnd::Again
            }
        });
    }
    // 5. Decode and validate, then apply in one batch with the changelog copy and last_seq.
    let ops = match entry.decode_ops() {
        Ok(ops) => ops,
        Err(e) => {
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "undecodable entry");
            return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
        }
    };
    let i = inner.clone();
    let e = entry.clone();
    let result = tokio::task::spawn_blocking(move || {
        let prepared = i.store.prepare(&e, &ops).map_err(Failure::Prepare)?;
        i.store.commit(prepared, i.cfg.sync_writes).map_err(Failure::Commit)
    })
    .await;
    match result {
        Ok(Ok(())) => {}
        Ok(Err(Failure::Prepare(Error::UnknownComparator(name)))) => {
            tracing::error!(node = %inner.cfg.identifier, seq, comparator = %name, "comparator is not registered");
            return Applied::End(SessionEnd::Halt(HaltReason::SpecMismatch));
        }
        Ok(Err(Failure::Prepare(e))) => {
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "entry does not apply to local state");
            return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
        }
        // A failed commit is never retried in-process: its CF side effects (a created
        // physical CF) must not be reused with another entry. Halt; restart re-evaluates.
        Ok(Err(Failure::Commit(e))) => {
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "commit failed; halting");
            return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
        }
        Err(e) => {
            tracing::error!(node = %inner.cfg.identifier, seq, error = %e, "apply task failed; halting");
            return Applied::End(SessionEnd::Halt(HaltReason::Corrupt));
        }
    }
    inner.upstream_last_seq.fetch_max(seq, Ordering::SeqCst);
    inner.origin_last_seq.fetch_max(seq, Ordering::SeqCst);
    tracing::trace!(node = %inner.cfg.identifier, seq, "applied");
    if failpoint::fire("slave-after-apply") {
        // Emulated crash between commit and listener: freeze until the test kills the node.
        std::future::pending::<()>().await;
    }
    inner.notify(|l| l.on_entry(&entry));
    // Published after the listener, so whoever waits for this seq also sees its effects.
    inner.seq_tx.send_replace(seq);
    Applied::Committed
}
```

- [ ] **Step 7: Implement the bootstrap install**

Create `src/slave/bootstrap.rs`:

```rust
//! Installing a bootstrap from the upstream (PROTOCOL.md §8.3, DESIGN.md §10).

use std::cmp::Ordering as KeyOrdering;
use std::sync::atomic::Ordering;
use std::sync::Arc;

use tonic::Streaming;

use super::client::Client;
use super::driver::{status_end, SessionEnd};
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

async fn next(inner: &Inner, s: &mut Streaming<pb::BootstrapChunk>) -> std::result::Result<Kind, SessionEnd> {
    match s.message().await {
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
    let mut stream = match c.clone().max_decoding_message_size(limit).bootstrap(req).await {
        Ok(r) => r.into_inner(),
        Err(s) => return Err(status_end(inner, &s)),
    };
    let Kind::Header(header) = next(inner, &mut stream).await? else {
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
        match next(inner, &mut stream).await? {
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
                    // Emulated crash mid-bootstrap: freeze until the test kills the node.
                    std::future::pending::<()>().await;
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

- [ ] **Step 8: Implement the driver**

Create `src/slave/driver.rs` (the Tail request stream is `start` chained with a `WatchStream` of acks; `tokio_stream::wrappers::WatchStream::from_changes` yields only values sent after it was created):

```rust
//! The replication driver: one task per slave that connects, handshakes and acts on the
//! answer, forever (PROTOCOL.md §11.3).

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
use super::state::{HaltReason, SlaveState};
use crate::background::blocking;
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

pub(crate) fn status_end(inner: &Inner, s: &Status) -> SessionEnd {
    end_for(inner, policy::after_status(s, &inner.policies()))
}

/// Starts the driver. It holds only a `Weak<Inner>` while it waits (halted, or backing off),
/// and a strong reference only for the duration of one session, which shutdown cancels.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let weak = Arc::downgrade(inner);
    let shutdown = inner.shutdown_tx.subscribe();
    let halt_cleared = inner.halt_cleared.clone();
    let backoff = BackoffState::new(inner.cfg.reconnect.clone());
    tokio::spawn(run(weak, shutdown, halt_cleared, backoff))
}

async fn run(weak: Weak<Inner>, mut shutdown: watch::Receiver<bool>, halt_cleared: Arc<Notify>, mut backoff: BackoffState) {
    let mut client: Option<Client> = None;
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
        let end = tokio::select! {
            end = session(&inner, &mut client, &mut backoff) => end,
            r = shutdown.changed() => {
                if r.is_err() {
                    return;
                }
                continue;
            }
        };
        let pause = match end {
            SessionEnd::Halt(reason) => {
                let i = inner.clone();
                let _ = blocking(move || {
                    i.halt(reason);
                    Ok(())
                })
                .await;
                continue;
            }
            SessionEnd::Again => AGAIN_PAUSE,
            SessionEnd::Retry(msg) => {
                tracing::warn!(node = %inner.cfg.identifier, upstream = %inner.cfg.upstream.uri(), error = %msg, "replication retry");
                inner.set_last_error(Some(msg));
                inner.set_state(SlaveState::Connecting);
                backoff.next_delay()
            }
        };
        drop(inner);
        tokio::select! {
            _ = tokio::time::sleep(pause) => {}
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

/// One connect → handshake → act cycle.
async fn session(inner: &Arc<Inner>, client: &mut Option<Client>, backoff: &mut BackoffState) -> SessionEnd {
    inner.set_state(SlaveState::Connecting);
    let c = match client {
        Some(c) => c.clone(),
        None => match client::connect(&inner.cfg) {
            Ok(c) => client.insert(c).clone(),
            Err(e) => return SessionEnd::Retry(e.to_string()),
        },
    };
    let hs = match c.clone().handshake(handshake_request(inner)).await {
        Ok(r) => r.into_inner(),
        Err(s) => return status_end(inner, &s),
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
    match policy::after_handshake(&hs, &inner.policies(), force) {
        Action::Tail => tail(inner, &c, &hs, None, backoff).await,
        Action::AdoptEpochThenTail => match adopt_epoch(inner, &hs).await {
            Ok(()) => tail(inner, &c, &hs, None, backoff).await,
            Err(end) => end,
        },
        Action::Bootstrap => match bootstrap::install(inner, &c, &hs).await {
            Ok(lease) => tail(inner, &c, &hs, Some(lease), backoff).await,
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
    let mut stream = match c.clone().max_decoding_message_size(limit).tail(outbound).await {
        Ok(r) => r.into_inner(),
        Err(s) => return status_end(inner, &s),
    };
    inner.set_state(SlaveState::Streaming);
    inner.set_last_error(None);
    let idle = Duration::from_millis(u64::from(hs.heartbeat_ms.max(1)) * 3);
    let started = Instant::now();
    loop {
        let msg = match tokio::time::timeout(idle, stream.message()).await {
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

- [ ] **Step 9: Implement maintenance and the public API**

Create `src/slave/maintenance.rs`:

```rust
//! Background work on a slave: changelog retention and physical CF drops.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;

use super::inner::Inner;
use crate::background::{every, run_logged};
use crate::error::Result;

const CF_DROP_INTERVAL: Duration = Duration::from_secs(5);

/// Holds only a `Weak<Inner>`, upgraded for one tick, so dropping the `Slave` releases the
/// database without waiting for this task.
pub(crate) fn spawn(inner: &Arc<Inner>) -> JoinHandle<()> {
    let mut shutdown = inner.shutdown_tx.subscribe();
    let retention_every = inner.cfg.retention.check_interval;
    let weak = Arc::downgrade(inner);
    tokio::spawn(async move {
        let mut retention = every(retention_every);
        let mut cf_drops = every(CF_DROP_INTERVAL);
        loop {
            let (name, f): (&'static str, fn(&Inner) -> Result<()>) = tokio::select! {
                _ = shutdown.changed() => break,
                _ = retention.tick() => ("retention", |i| i.store.trim_changelog(i.now_ms(), &i.cfg.retention).map(drop)),
                _ = cf_drops.tick() => ("cf-drop", |i| i.store.drop_unreferenced_cfs().map(drop)),
            };
            let Some(inner) = weak.upgrade() else { break };
            run_logged(inner, name, f).await;
        }
    })
}
```

Replace `src/slave/mod.rs` with:

```rust
//! A read-only replica (DESIGN.md §6, §10, §16).

mod apply;
mod bootstrap;
mod client;
mod config;
mod driver;
mod inner;
mod maintenance;
mod policy;
mod state;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::error::{Error, Result};
use crate::log::{EntryIter, LogApi};
use crate::read::{ReadApi, ReadSnapshot};
use crate::server::{await_sole_owner, ServerHandle};
use crate::store::read::DbIter;
use crate::store::Store;
use crate::types::{CfSpec, Direction, IterMode, Role, Seq, ValueMeta};
use inner::Inner;

pub use config::{Backoff, DivergencePolicy, FetchConfig, GapPolicy, IdentityPolicy, SlaveConfig, UpstreamConfig};
pub use state::{HaltAction, HaltReason, SlaveState, SlaveStatus};

/// A read-only replica. It has no write methods: writing to a slave is a compile error.
///
/// It replicates from its upstream in the background, serves reads from local data
/// (except while bootstrapping, when reads return `Error::NotReady`), and keeps serving
/// reads while halted.
pub struct Slave {
    inner: Arc<Inner>,
    driver: Option<JoinHandle<()>>,
    maintenance: Option<JoinHandle<()>>,
    server: Option<ServerHandle>,
}

impl Slave {
    /// Opens or creates a slave database and starts replicating. Must be called within a
    /// tokio runtime. Local data is readable at once unless a bootstrap was interrupted.
    pub async fn open(cfg: SlaveConfig) -> Result<Self> {
        cfg.validate()?;
        let (store, meta) = Store::open(&cfg.path, Role::Slave, cfg.rocksdb.clone())?;
        let inner = Arc::new(Inner::new(store, cfg, &meta)?);
        let driver = driver::spawn(&inner);
        let maintenance = maintenance::spawn(&inner);
        Ok(Slave { inner, driver: Some(driver), maintenance: Some(maintenance), server: None })
    }

    pub fn status(&self) -> SlaveStatus {
        self.inner.status()
    }

    /// Last applied seq.
    pub fn applied_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }

    /// Waits until `applied_seq() >= seq`. `Err(Halted(reason))` if the slave halts first,
    /// `Err(Timeout)` after `timeout`.
    pub async fn wait_for_seq(&self, seq: Seq, timeout: Duration) -> Result<()> {
        let mut seqs = self.inner.seq_tx.subscribe();
        let mut states = self.inner.state_tx.subscribe();
        let wait = async {
            loop {
                if *seqs.borrow_and_update() >= seq {
                    return Ok(());
                }
                if let SlaveState::Halted(r) = *states.borrow_and_update() {
                    return Err(Error::Halted(r));
                }
                tokio::select! {
                    r = seqs.changed() => if r.is_err() { return Err(Error::NotReady) },
                    r = states.changed() => if r.is_err() { return Err(Error::NotReady) },
                }
            }
        };
        tokio::time::timeout(timeout, wait).await.map_err(|_| Error::Timeout)?
    }

    pub fn watch_state(&self) -> watch::Receiver<SlaveState> {
        self.inner.state_tx.subscribe()
    }

    /// A consistent view at the current applied seq. `Err(NotReady)` while bootstrapping.
    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        self.check_ready()?;
        self.inner.store.read_snapshot(self.inner.now_ms())
    }

    fn check_ready(&self) -> Result<()> {
        if self.inner.is_ready() { Ok(()) } else { Err(Error::NotReady) }
    }

    /// Stops replication and serving and waits until every task has released the slave
    /// (`Err(Timeout)` after 10 s). It does not wait for `ReadSnapshot`s: each keeps the
    /// database open until it is dropped.
    pub async fn shutdown(mut self) -> Result<()> {
        if let Some(server) = self.server.take() {
            server.shutdown().await;
        }
        self.inner.shutdown_tx.send_replace(true);
        for task in [self.driver.take(), self.maintenance.take()].into_iter().flatten() {
            let _ = task.await;
        }
        if await_sole_owner(&self.inner, Duration::from_secs(10)).await { Ok(()) } else { Err(Error::Timeout) }
    }
}

impl Drop for Slave {
    fn drop(&mut self) {
        self.inner.shutdown_tx.send_replace(true);
    }
}

impl ReadApi for Slave {
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        self.check_ready()?;
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).get(&cf, key)
    }

    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>> {
        self.check_ready()?;
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).iter(cf, mode, None)
    }

    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>> {
        self.check_ready()?;
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).iter(cf, IterMode::From(prefix, Direction::Forward), Some(prefix))
    }

    /// Empty while bootstrapping.
    fn list_cfs(&self) -> Vec<CfSpec> {
        self.inner.store.cfs.live_entries().into_iter().map(|e| e.spec).collect()
    }
}

impl LogApi for Slave {
    fn last_seq(&self) -> Seq {
        self.inner.store.position().last_seq
    }

    fn first_retained_seq(&self) -> Seq {
        self.inner.store.first_retained_seq()
    }

    fn entries_since(&self, seq: Seq) -> Result<EntryIter<'_>> {
        self.inner.store.entries_after(seq)
    }

    fn subscribe(&self) -> watch::Receiver<Seq> {
        self.inner.seq_tx.subscribe()
    }
}
```

In `src/lib.rs`, replace the `pub use slave::{…};` block with:

```rust
pub use slave::{
    Backoff, DivergencePolicy, FetchConfig, GapPolicy, HaltAction, HaltReason, IdentityPolicy, Slave, SlaveConfig,
    SlaveState, SlaveStatus, UpstreamConfig,
};
```

- [ ] **Step 10: Run the tests**

Run: `cargo test --lib slave::client && cargo test --test replication`
Expected: PASS: `b64token_rules`, and 10 tests in `replication` (`slave_bootstraps_then_tails_every_kind_of_write`, `read_api_on_a_slave`, `ttl_values_replicate_and_the_reapers_delete_replicates`, `edge_case_data_through_tail_and_bootstrap`, `restart_resumes_without_bootstrap`, `status_and_wait_for_seq` (also: the master's `replicas()` shows the slave's acknowledged seq, zero lag and `in_sync`), `a_slave_without_data_is_not_ready_and_reports_the_connection_error`, `a_batching_slave_applies_every_entry_on_its_own` (fetch batching on; each entry still its own write and listener call; the master reports the clamped fetch settings), `a_sync_writes_slave_replicates_and_resumes`, `a_panicking_listener_is_disabled_and_replication_goes_on`). The edge-case test moves a ~16 MiB value through Tail and Bootstrap; allow it a few seconds in debug builds.

Run: `cargo test`
Expected: PASS (all unit and integration tests so far).

- [ ] **Step 11: Update DESIGN.md §6 and §15, and PROTOCOL.md §11.2**

Replace:

```
- Strictly increasing `seq`, with no duplicates within a process.
```

with:

```
- Strictly increasing `seq`, with no duplicates within a process, between bootstraps. After `on_bootstrap_complete(S)` delivery continues at `S + 1`; `S` can be lower than a seq delivered before a re-bootstrap, because state was replaced wholesale. Each entry of a Tail batch is committed and delivered on its own.
- Called before the new seq is published to `wait_for_seq` and `subscribe`, so whoever waits for a seq also sees what the listener did with it.
```

In DESIGN.md §6, replace the lines of the apply loop from:

```
  ops = decode + validate (PROTOCOL.md §5.2)           → Halted(Corrupt / SpecMismatch)
```

through:

```
  listener.on_entry(&entry)
```

with:

```
  ops = decode + validate (PROTOCOL.md §5.2)           → Halted(Corrupt / SpecMismatch)
  wb = WriteBatch:
         ops with envelopes (write_seq = entry.seq)
         __changelog[seq] = entry (unchanged)
         __meta.last_seq = seq, last_entry_crc = entry.crc32c
  db.write(wb)          (sync = SlaveConfig::sync_writes, default false)
                                                       failure → Halted(Corrupt), never retried in-process
  if a verify capture is requested at this seq: take the snapshot now (§12)
  listener.on_entry(&entry)
  publish seq to wait_for_seq and downstream tail subscribers
after each batch (coalesced), and at least once per heartbeat:
  ack applied_seq = last_seq to the upstream (PROTOCOL.md §7.6)
```

and after the bullet that starts with `- **One entry is one WriteBatch, and an entry is never split.**` add:

```
- **Upstream epoch histories are validated** (in an `EPOCH_ADVANCED` handshake and in a bootstrap header): non-empty, first record `{1, 1}`, epochs strictly increasing, start seqs non-decreasing, last epoch equal to the identity's epoch. An invalid history halts the slave with `Corrupt`.
```

In the PROTOCOL.md §11.2 table, after the row that starts with `` | `RESUME` / `EPOCH_ADVANCED` | `` add:

```
| An epoch history (handshake or bootstrap header) that is empty, does not start with `{1, 1}`, has non-increasing epochs or decreasing start seqs, or does not end with the identity's epoch; or a local failure to apply or commit a received entry | Halt with `Corrupt` (persisted). The entry is never retried in the same process. |
```

Run: `cargo clippy --all-targets -- -D warnings && cargo clippy --all-targets --features failpoints -- -D warnings`
Expected: no warnings.

- [ ] **Step 12: Commit**

```bash
git add src tests DESIGN.md PROTOCOL.md
git commit -m "feat: slave with bootstrap install, tail apply, read API and end-to-end tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

