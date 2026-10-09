# Plan 2 of 3 — Replication, Re-bootstrap, Failure/Resume and Promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Turn Plan 1's single-node master into a replicating system: a gRPC replication server (Handshake, Tail with Kafka-style fetch batching, Bootstrap with retention leases) with token/mTLS auth and TLS, read-only `Slave`s that bootstrap, tail, chain, halt and recover by policy, and `Slave::promote` — every path proven by real multi-node tests over 127.0.0.1.

**Architecture:** One replication server (`src/server/`) runs over a `LogSource` trait that both `master::Inner` and `slave::Inner` implement; it holds the source weakly and every producer gives up on shutdown or reset, so a server never keeps a closed node's database open. A slave is a Plan 1 `Store` plus one driver task: connect → handshake → act on a pure policy table → bootstrap (a crash-safe install into fresh physical CFs) and/or tail (each entry applied through the same `Store::prepare`/`commit` as the master, unsynced). Decision tables (handshake rules, slave policies) are pure functions with one unit test per row; everything else is tested end to end with nodes talking real gRPC on 127.0.0.1.

**Tech Stack:** Plan 1's stack (Rust 1.99, edition 2024, rocksdb 0.25.0, prost 0.14.4, tokio 1.53, …) plus tonic 0.14.6 (`tls-ring`), tonic-prost 0.14.6, tonic-types 0.14.6 (`ErrorInfo`), tokio-stream 0.1.19, subtle 2.6.1, async-trait 0.1.92, optional fail 0.5.1 (feature `failpoints`); dev rcgen 0.14.10.

**Spec:** [DESIGN.md](../../../DESIGN.md), [PROTOCOL.md](../../../PROTOCOL.md) (as of commit `f960d4e`, which added Tail fetch batching, PROTOCOL.md §7.1, §7.3–§7.5) and [the Plan 2/3 design notes](../specs/2026-10-09-plan2-3-design-notes.md) (all of it except the "Plan 3: Verify" section). Read DESIGN.md §6, §10–§16 and PROTOCOL.md §3, §6–§8, §10–§12 before starting. This plan builds on [Plan 1](2026-10-09-plan1-storage-and-master.md). **The Plan 1 code under `src/` is the source of truth, not the Plan 1 document**: in particular `Store.db` is `Arc<DbCore>` (`Deref<Target = DB>`, never wrap the DB in another `Arc`), runtime-created CFs keep their options alive through `Store::retain_cf_options`, `OwnedSnapshot` reads only through `get_cf`/`iterator_cf` bound to `&self`, background tasks hold `Weak<Inner>`, `Store::commit` re-checks the seq, and `Store::ttl_due` takes a cursor. Every type and function the Plan 1 Interfaces blocks name exists, as implemented (`Store`, `Store::retain_cf_options`, `DbCore`, `prepare`/`commit`, `MetaSnapshot`, `CfRegistry`, `PhysCf`, `ReadCtx`, `ReadSnapshot`, `OwnedSnapshot`, `entries_after`, `guard`/`RetentionGuard`, `trim_changelog`, `drop_unreferenced_cfs`, `master::writer::Inner`, `Master`, `LogApi`, `Digest`, `envelope`, …) exists exactly as written there.

**Series:**
- Plan 1: storage engine and master (done before this plan).
- **Plan 2 (this):** replication server, auth/TLS, `Slave` (bootstrap, tail, halts, policies, listener), chaining, promotion, failure/resume tests.
- Plan 3: Verify (sessions, range hashing, `ListRange`), `replicas()`, `lag_ms`, `last_verify`, observability spans.

## Global Constraints

- Everything in Plan 1's Global Constraints still holds; in particular: absolute ops only (`Put`, `Delete`, `DeleteRange`, `CreateCf`, `DropCf`), no merge; mutation + `__changelog[seq]` + `__meta.last_seq` in one `WriteBatch`; `__meta.last_seq` is the source of truth; the master always fsyncs; `LogEntry.ops` is never re-encoded.
- **No read-modify-write API** (no `update`, CAS or merge). Nothing in this plan adds one; the rustdoc of `Master::put`, `Master::write`, `Master::get` and the crate docs keep the DESIGN.md §5.1 warning.
- **Slaves forward entries unchanged** (`seq`, `epoch`, `ts_ms`, `ops` bytes, `crc32c`). Identity always refers to the origin master.
- Slaves apply each entry with `Store::prepare` + `Store::commit(sync = false)`; one entry is one `WriteBatch`. Bootstrap records are written unsynced; the final bootstrap batch is synced.
- `PROTOCOL_VERSION = 1` and `ENVELOPE_VERSION = 1` must match exactly. Handshake reports a mismatch in its body; every other RPC requires `x-rrdb-protocol-version` and fails with `FAILED_PRECONDITION / PROTOCOL_MISMATCH`.
- Request metadata on every call: `x-rrdb-protocol-version`, `x-rrdb-node-id`, and `authorization: Bearer <token>` with token auth.
- Every protocol failure is a gRPC status with `google.rpc.ErrorInfo { domain: "rrdb.replication.v1", reason: <Reason name> }`, including unexpected server failures (`INTERNAL` with reason `REASON_UNSPECIFIED`, `net::internal_status`); authentication failures (`UNAUTHENTICATED`, `PERMISSION_DENIED`) carry no `ErrorInfo`; malformed requests are `INVALID_ARGUMENT` and the Verify stubs `UNIMPLEMENTED`.
- Tail fetch batching (PROTOCOL.md §7.5): `TailMessage` carries `EntryBatch { repeated LogEntry entries }` or `Heartbeat`; the server sends a pending batch when `min_batch_bytes = 0` and it is non-empty, when it reaches `min_batch_bytes`, when `max_wait_ms` has passed since its oldest entry became available, or when the next entry would exceed `max_batch_bytes` (a single larger entry goes alone); `max_wait_ms` ≤ `heartbeat_ms`, `max_batch_bytes` ≤ `ServeLimits::max_tail_batch_bytes` (default 4 MiB), 0 → 1 MiB. Catch-up is never slowed by `max_wait_ms`. Slaves apply each entry of a batch as its own atomic write. `SlaveConfig::fetch` defaults to `{ min_batch_bytes: 0, max_wait: 100 ms, max_batch_bytes: 1 MiB }`.
- Decode limits: server requests 4 MiB; client Tail `max(max_entry_bytes, max_batch_bytes) + 64 KiB`; client Bootstrap `max(max_entry_bytes, 16 MiB) + 64 KiB`; `bootstrap_chunk_bytes` ≤ 16 MiB.
- Defaults (DESIGN.md §18): heartbeat 5 s, idle timeout 3 × heartbeat, reconnect 100 ms → 30 s ×2 with full jitter (reset after 60 s of streaming), bootstrap lease TTL 15 min, 2 concurrent bootstraps, 4 verify sessions, chunk 1 MiB, 10 failed authentications per minute per address, `IdentityPolicy::Refuse`, `DivergencePolicy::Halt`, `GapPolicy::Rebootstrap`.
- Token authentication on a plaintext listener is refused unless `allow_plaintext_token`. Tokens are compared in constant time (`subtle`) and never logged or printed.
- Data-related halts (`IdentityChanged`, `Diverged`, `LogGap`, `Corrupt`, `SpecMismatch`) are persisted in `__meta.node_state` and survive restart; configuration halts (`ProtocolMismatch`, `EnvelopeMismatch`, `AuthRejected`, `LoopDetected`) are not.
- TTL stays master-driven: slaves never reap and have no `__ttl_index`; promotion rebuilds it.
- Plan 1 code conventions apply to new code: every `create_cf` is followed by `Store::retain_cf_options(opts)`; snapshot iterators are created and consumed where the `ReadSnapshot` is owned (the bootstrap job iterates inside the blocking closure that owns it); periodic background tasks (master and slave maintenance) hold `Weak<Inner>` and upgrade only for one tick; the slave driver holds `Weak<Inner>` while it waits (halted, backing off) and a strong reference only for one session, and Tail producers hold the source for one stream — sessions and streams are cancelled by the shutdown signal that `shutdown()` and `Drop` send, so dropping a node releases its database. RocksDB I/O that may fsync (halts, epoch adoption, meta writes) runs on blocking threads (`background::blocking`), never on async workers.
- Every epoch history received from upstream (handshake `EPOCH_ADVANCED`, bootstrap header) is validated: non-empty, first record `{1, 1}`, epochs strictly increasing, start seqs non-decreasing, last epoch equal to the identity's epoch; otherwise the slave halts `Corrupt`.
- On a slave, a failed `Store::prepare` or `Store::commit` halts the slave (`SpecMismatch` for an unknown comparator, otherwise `Corrupt`); the same entry is never retried in-process.
- The Verify RPCs exist (generated service) but return `UNIMPLEMENTED` until Plan 3.
- Tests that matter are **real**: nodes in one process, real gRPC over `127.0.0.1` with port 0, data compared exactly with the harness `dump`, bootstraps counted with the `RecordingListener`. Failpoint tests live only in `tests/failpoints.rs`, run serially, and need `--features failpoints`.
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **A client that stops reading must not pin the database.** Tonic serves each connection on its own spawned task, so a Tail or Bootstrap producer blocked on a full channel would keep `Arc<dyn LogSource>` (and the RocksDB handle) alive forever and make `shutdown()` time out. Every producer send also waits on shutdown and reset. (Test `shutdown_with_unread_streams_releases_the_database` in Task 5.)
2. **A slave pointed at its own address** must halt with `LoopDetected` (rule 3, `client_id == server_id`), not loop or deadlock. (Test `a_slave_pointed_at_itself_halts_loop_detected` in Task 9.)
3. **Re-bootstrap while a reader holds a snapshot of a CF with the same physical name** (`u:{name}:{create_seq}` repeats when re-bootstrapping from the same origin): the snapshot keeps its old view, the new data is correct, and the live CF is never physically dropped afterwards. (Test `a_cf_held_by_a_snapshot_is_taken_over_without_disturbing_the_snapshot` in Task 6.)
4. **Promotion while a `ReadSnapshot` is alive** keeps the DB open: promotion must fail cleanly and leave a slave directory that reopens and resumes. Refused promotions must shut the slave down (not leak tasks holding the DB). (Test `promotion_is_refused_without_data_and_needs_released_snapshots` in Task 10.)
5. **A restarted slave whose bootstrap was interrupted** must never serve the partial data: reads answer `NotReady` until the bootstrap is redone from scratch, and the leftover physical CFs (whose comparators must be known at open) are dropped. (Test `crash_mid_bootstrap_restarts_the_bootstrap` in Task 12, store-level `crash_mid_bootstrap_reopens_and_the_next_reset_drops_leftovers` in Task 6.)

## Design deltas this plan introduces

Each owning task updates DESIGN.md or PROTOCOL.md with the exact text given there.

- **`MasterConfig.serve: Option<ServeConfig>`** (Task 5, amends DESIGN.md §16): `None` is an embedded, non-replicating master. `Master::serve_addr()` and `Slave::serve_addr()` return the bound address (tests bind port 0).
- **`__meta.node_state` is text** `ready` | `bootstrapping` | `halted:<Reason>`, and **`__meta.bootstrap_cfs`** lists physical CFs created by an unfinished bootstrap so the next open knows their comparators (Task 6, DESIGN.md §3.2, §10, §16.3).
- **A server treats its own `last_entry_crc` as the CRC of its entry `last_seq`** even when its log no longer holds that entry (right after a bootstrap, or after retention emptied the log), for handshake rules 9 and 12 and Tail's `prev_crc32c` check (Task 2, PROTOCOL.md §6.3 notes).
- **`LogSource`** is the trait from the design notes (Task 2, replaces the sketch in DESIGN.md §11).
- **Bootstrap decode limit** is `max(max_entry_bytes, 16 MiB) + 64 KiB` and servers keep `bootstrap_chunk_bytes` ≤ 16 MiB, because a client cannot know the server's chunk size (Task 4, PROTOCOL.md §11.4 and §12).
- **Auth additions** (Task 4, DESIGN.md §14): `AllowMethods` (per-method restriction around any authenticator), `NoCredentials`, `ServeLimits::max_auth_failures_per_minute`; the mTLS peer id is the client certificate's subject CommonName.
- **Slave config and status additions** (Task 6, DESIGN.md §16): `SlaveConfig::{heartbeat, connect_timeout, clock}`, `SlaveStatus::listener_failed`. `VerifyConfig`, `lag_ms`, `last_verify` and `replicas()` come with Plan 3.
- **Bootstrap lease hand-over** (Task 5, PROTOCOL.md §7.2): the Tail that presents the lease takes it over and releases it once it has read the entries after the snapshot (or when it ends), instead of releasing it the moment the stream is established — otherwise `max_entries = 1` retention can trim `S + 1` before the producer's first read.
- **Listener ordering** (Task 7, DESIGN.md §15): seqs are strictly increasing *between bootstraps*; after `on_bootstrap_complete(S)` delivery continues at `S + 1`. The listener is called before the new seq is published to `wait_for_seq`/`subscribe`, so whoever waits for a seq also sees the listener's effects.
- **Refused promotion shuts the slave down** (Task 10, DESIGN.md §13.1): `promote(self)` consumes the slave, so on refusal it is shut down cleanly and its directory is unchanged. An outstanding `ReadSnapshot` is detected after shutdown (it still holds the shared `DbCore`) and reported as `Error::Config` naming the snapshot.
- **Rulings from the Plan 1 final review:** upstream epoch histories are validated (Global Constraints); a failed commit on a slave halts it `Corrupt` instead of retrying; a `failpoints`-gated test covers the master's `bump_epoch` fence (point `master-bump-epoch-write`, Task 12).
- **Test-only `Slave::kill`** behind feature `failpoints` (Task 12, DESIGN.md §16). The feature enables `fail/failpoints` as well (the design notes listed only `dep:fail`, but `fail` compiles every point to a no-op without its own feature). `futures-util` from the design notes is not added: nothing needs it.
- **Pre-flight rulings:** shared helpers instead of duplicated blocks (`server::{start_for, end_stream}`, `background::{every, run_logged, blocking}`, `Store::delete_all_keys`); bootstrap jobs drop their snapshot before returning their permit and `ServerHandle::shutdown` waits (≤ 5 s) for all permits; the stream-ending send also gives up on reset; tests never use sleep as synchronisation and bound every wait (10 s); Plan 1 placeholder `allow`s are removed as their items get used, and the items Plan 2 does not use are deleted (Task 12). The server-test helpers (`test_client::{eventually, within, T0}`) duplicate the integration harness on purpose: unit tests cannot use `tests/support`.

## File Structure

```
Cargo.toml                    + tonic, tonic-prost, tonic-types, tokio-stream, subtle, async-trait, fail; rcgen (Task 1); [[test]] failpoints (Task 12)
build.rs                      server + client codegen (Task 1)
src/lib.rs                    module tree and re-exports (Tasks 1, 2, 3, 4, 5, 6, 7)
src/net/mod.rs                metadata keys, Reason <-> status with ErrorInfo, internal_status, decode limits (Task 1)
src/net/convert.rs            DbIdentity / CfEntry / EpochStart / LogEntry <-> pb, checked_history (Task 1)
src/store/meta.rs             + node_state, max_entry_bytes, bootstrap_cfs keys and fields (Task 2)
src/store/mod.rs              + bootstrap_cfs in the known set at open; new modules (Tasks 2, 6, 10); drop unused Store::role (Task 12)
src/store/repl.rs             entry_crc, read_entries, ReadSnapshot::{meta, phys, raw_ctx}, ReadCtx::iter_raw (Task 2)
src/source.rs                 LogSource trait (Task 2)
src/server/mod.rs             module list (Task 3); replaced by the full server (Task 5)
src/server/handshake.rs       pure decision table, one test per rule (Task 3)
src/types.rs                  drop the stale allow on fork_after (Task 3) and epoch_for (Task 7)
src/auth.rs                   Method, PeerInfo, Authenticator, tokens, mTLS allow-list, AllowMethods, limiter, CN parser (Task 4)
src/config.rs                 + TLS configs, ServeLimits, ServeConfig (Task 4); MasterConfig.serve (Task 5)
src/failpoint.rs              `fire(name)`: fail::eval behind feature `failpoints` (Task 5); `io_error(name)` (Task 12)
src/server/tail.rs            Tail validation, fetch batching (§7.5) and producer (Task 5)
src/server/bootstrap.rs       Bootstrap producer, digest, lease hand-over (Task 5)
src/server/test_client.rs     bare protocol client and bounded-wait helpers for server unit tests (Task 5)
src/server/tests.rs           server unit tests against a real master (Task 5)
src/store/retention.rs        drop the stale allows on GuardSet, RetentionGuard, guard (Task 5)
src/master/source.rs          LogSource for the master; slow-reader test (Task 5)
src/master/writer.rs          + reset_tx, identity_and_history (Task 5); failpoint in bump_epoch (Task 12)
src/master/mod.rs             + server start (start_for), serve_addr, shutdown waits for the DB (Task 5)
src/error.rs                  + Error::Halted (Task 6)
src/listener.rs               ReplicationListener (Task 6)
src/store/bootstrap.rs        reset_for_bootstrap, begin_bootstrap_cf, write_bootstrap_records, finish_bootstrap, adopt_identity, delete_all_keys (Task 6)
src/slave/mod.rs              module list (Task 6); Slave API, ReadApi, LogApi (Task 7); clear_halt (Task 8); serving (Task 9); promote module (Task 10); kill (Task 12)
src/slave/config.rs           SlaveConfig, UpstreamConfig, FetchConfig, policies, Backoff (Task 6)
src/slave/state.rs            HaltReason, SlaveState, HaltAction, NodeState, SlaveStatus (Task 6)
src/slave/policy.rs           pure handshake/status → action table (Task 6)
src/background.rs             every, run_logged, blocking: helpers for background and blocking work (Task 7)
src/master/maintenance.rs     uses the background helpers (Task 7)
src/slave/inner.rs            shared slave state (Task 7)
src/slave/client.rs           upstream channel + metadata interceptor (Task 7)
src/slave/driver.rs           connect → handshake → act loop, tail (Task 7)
src/slave/apply.rs            per-entry checks and apply (Task 7)
src/slave/bootstrap.rs        bootstrap install (Task 7)
src/slave/maintenance.rs      retention and CF drops on slaves (Task 7)
src/slave/source.rs           LogSource for slaves (Task 9)
src/store/promote.rs          rebuild_ttl_index, promote_to_master (Task 10)
src/slave/promote.rs          Slave::promote (Task 10)
src/store/cfs.rs              drop unused pending_entries, push_pending (Task 12)
src/codec/mod.rs              drop the unused decode_ops re-export (Task 12)
tests/support/mod.rs          Cluster harness, dump, edge cases, RecordingListener (Task 7; serving in Task 9)
tests/replication.rs          core replication (Task 7)
tests/rebootstrap.rs          policies, re-bootstrap, restore, epoch bump (Task 8)
tests/chain.rs                chains, loops, reset propagation (Task 9)
tests/promotion.rs            promotion (Task 10)
tests/security.rs             TLS, mTLS, tokens (Task 11)
tests/failure_resume.rs       restarts, downstream ahead, persisted LogGap (Task 12)
tests/failpoints.rs           fault injection, serial, --features failpoints (Task 12)
```

---

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
### Task 6: Slave foundations — configuration, states, policy table, listener, bootstrap store helpers

**Files:**
- Create: `src/slave/config.rs`, `src/slave/state.rs`, `src/slave/policy.rs`, `src/listener.rs`, `src/store/bootstrap.rs`, `src/slave/mod.rs` (module list only, replaced in Task 7)
- Modify: `src/error.rs`, `src/store/mod.rs`, `src/lib.rs`, `DESIGN.md` (§3.2, §10, §16, §16.3)

**Interfaces:**
- Consumes: Task 2 meta keys and fields; Task 4 `ServeConfig`, `TlsClientConfig`, `CredentialsProvider`, `NoCredentials`; Task 1 `net::reason_of`, `net::reason_status`; Plan 1 `validate_identifier`, `Retention`, `RocksTuning`, `Clock`, `Store::{cfs, write_meta, load_meta, drop_unreferenced_cfs, set_position, set_first_retained_seq, tuning, retain_cf_options}`, `PhysCf::new`, `meta::{encode_*}`.
- Produces:
  - `slave::config::{UpstreamConfig { server, port, tls, credentials } (new, from_addr, pub(crate) uri), IdentityPolicy { Refuse (default), Rebootstrap }, DivergencePolicy { Halt (default), Rebootstrap }, GapPolicy { Rebootstrap (default), Halt }, FetchConfig { min_batch_bytes: u32, max_wait: Duration, max_batch_bytes: u32 } (Default 0 / 100 ms / 1 MiB), Backoff { initial, max, factor }, BackoffState { new, reset, next_delay }, SlaveConfig { identifier, path, upstream, serve, retention, listener, on_identity_change, on_divergence, on_log_gap, reconnect, fetch, heartbeat, connect_timeout, rocksdb, clock } (new, pub(crate) validate)}`.
  - `slave::state::{HaltReason (is_persisted, pub(crate) name, pub(crate) from_name), SlaveState { Connecting, Bootstrapping { target_seq, records }, Streaming, Halted(HaltReason) }, HaltAction { Retry, Rebootstrap }, NodeState { Ready, Bootstrapping, Halted } (encode, decode), SlaveStatus { state, origin_identity, applied_seq, first_retained_seq, upstream_last_seq, origin_last_seq, upstream_chain, last_contact, last_error, listener_failed }}`.
  - `slave::policy::{Policies { identity, divergence, gap }, Action { Tail, AdoptEpochThenTail, Bootstrap, Halt(HaltReason), Retry(String), Again }, after_handshake(&pb::HandshakeResponse, &Policies, force_bootstrap: bool) -> Action, after_status(&Status, &Policies) -> Action}`.
  - `pub trait ReplicationListener: Send + Sync + 'static { on_entry, on_bootstrap_begin, on_bootstrap_complete, on_state_change }` (all default no-ops).
  - `Error::Halted(HaltReason)`.
  - `store::bootstrap::{NODE_READY = b"ready", NODE_BOOTSTRAPPING = b"bootstrapping", BootstrapFinish { identity, epoch_history, cf_specs, snapshot_seq, snapshot_crc, max_entry_bytes }}`, `Store::{reset_for_bootstrap(&self) -> Result<()>, begin_bootstrap_cf(&self, &CfEntry) -> Result<()>, write_bootstrap_records(&self, &CfEntry, &[(Vec<u8>, Vec<u8>)]) -> Result<()>, finish_bootstrap(&self, &BootstrapFinish) -> Result<()>, adopt_identity(&self, &DbIdentity, &[EpochStart]) -> Result<()>, delete_all_keys(&self, &impl AsColumnFamilyRef) -> Result<()>}` (unsynced batches of 1 000; also used by Task 10).

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

- [ ] **Step 11: Commit**

```bash
git add src DESIGN.md PROTOCOL.md
git commit -m "feat: slave config, persisted states, policy table and crash-safe bootstrap store helpers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Slave runtime — driver, apply, bootstrap install, read API, harness and core end-to-end tests

**Files:**
- Create: `src/slave/inner.rs`, `src/slave/client.rs`, `src/slave/driver.rs`, `src/slave/apply.rs`, `src/slave/bootstrap.rs`, `src/slave/maintenance.rs`, `tests/support/mod.rs`, `tests/replication.rs`
- Modify: `src/slave/mod.rs` (replace), `src/lib.rs`, `DESIGN.md` (§15)

**Interfaces:**
- Consumes: Task 6 everything; Task 5 `server::await_sole_owner`, `ServerHandle`, `failpoint::fire`; Task 2 `Store::{entry_crc}`, meta fields; Task 1 `net::{DECODE_SLACK, MD_*}`, conversions including `checked_history` (an invalid history in an `EPOCH_ADVANCED` answer or a bootstrap header halts `Corrupt`); Plan 1 `Store::{open, prepare, commit, position, live_cf, live_ctx, read_snapshot, entries_after, trim_changelog, drop_unreferenced_cfs}`, `epoch_for`, `Digest`, `envelope::decode`, `validate_cf_name`.
- Produces:
  - Tail requests carry `SlaveConfig::fetch` (`min_batch_bytes`, `max_wait_ms`, `max_batch_bytes`); each `EntryBatch` is applied entry by entry, each its own atomic write; Tail decode limit `max(max_entry_bytes, fetch.max_batch_bytes) + 64 KiB`.
  - `pub struct Slave` with `async open(SlaveConfig) -> Result<Slave>`, `status() -> SlaveStatus`, `applied_seq() -> Seq`, `async wait_for_seq(seq, timeout) -> Result<()>` (`Err(Halted(r))`, `Err(Timeout)`), `watch_state() -> watch::Receiver<SlaveState>`, `snapshot() -> Result<ReadSnapshot>` (`Err(NotReady)` without data), `async shutdown(self) -> Result<()>`; `impl ReadApi for Slave` (all reads `Err(NotReady)` without data), `impl LogApi for Slave`.
  - `slave::inner::Inner` with `pub store, pub cfg, pub state_tx, pub seq_tx, pub reset_tx, pub shutdown_tx, pub origin_last_seq, pub upstream_last_seq, pub max_entry_bytes, pub force_bootstrap, pub halt_cleared` and `new(Store, SlaveConfig, &MetaSnapshot)`, `policies()`, `ident()`, `set_ident(..)`, `is_ready()`, `now_ms()`, `state()`, `set_state(SlaveState)`, `halt(HaltReason)` (a synced write: called through `background::blocking`), `reset_downstream()`, `upstream_chain()`, `note_upstream(&pb::HandshakeResponse) -> Option<u64>` (the changed `max_entry_bytes`, persisted by the driver on a blocking thread), `note_positions(upstream, origin)`, `note_contact()`, `set_last_error(Option<String>)`, `notify(impl FnOnce(&dyn ReplicationListener))`, `status()`.
  - `slave::client::{Client, connect(&SlaveConfig) -> Result<Client>}`; `slave::driver::{SessionEnd { Retry(String), Again, Halt(HaltReason) }, spawn(&Arc<Inner>) -> JoinHandle<()>, end_for, status_end}` (the driver holds `Weak<Inner>` while waiting; `Inner.halt_cleared` is an `Arc<Notify>` it shares); `slave::maintenance::spawn(&Arc<Inner>) -> JoinHandle<()>` (Weak, upgraded per tick); `slave::apply::{Applied, apply(&Arc<Inner>, LogEntry)}`; `slave::bootstrap::install(&Arc<Inner>, &Client, &pb::HandshakeResponse) -> Result<Vec<u8> /* lease */, SessionEnd>`.
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

`epoch_for` is now used by the slave: in `src/types.rs`, delete the line `#[allow(dead_code)] // used by the replication server (Plan 2)` above `pub(crate) fn epoch_for`.

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

use super::config::SlaveConfig;
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
    let up = &cfg.upstream;
    let mut endpoint = Endpoint::from_shared(up.uri())
        .map_err(|e| Error::Config(format!("upstream address {}: {e}", up.uri())))?
        .connect_timeout(cfg.connect_timeout)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    if let Some(tls) = &up.tls {
        endpoint = endpoint
            .tls_config(tls.to_tonic(&up.server))
            .map_err(|e| Error::Config(format!("upstream TLS: {e}")))?;
    }
    let node_id = AsciiMetadataValue::try_from(cfg.identifier.as_str())
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
        i.store.commit(prepared, false).map_err(Failure::Commit)
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

Create `src/slave/driver.rs`:

```rust
//! The replication driver: one task per slave that connects, handshakes and acts on the
//! answer, forever (PROTOCOL.md §11.3).

use std::sync::atomic::Ordering;
use std::sync::{Arc, Weak};
use std::time::{Duration, Instant};

use tokio::sync::{watch, Notify};
use tokio::task::JoinHandle;

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
    let mut stream = match c.clone().max_decoding_message_size(limit).tail(req).await {
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
            Some(Kind::Heartbeat(h)) => inner.note_positions(h.upstream_last_seq, h.origin_last_seq),
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
Expected: PASS: `b64token_rules`, and 9 tests in `replication` (`slave_bootstraps_then_tails_every_kind_of_write`, `read_api_on_a_slave`, `ttl_values_replicate_and_the_reapers_delete_replicates`, `edge_case_data_through_tail_and_bootstrap`, `restart_resumes_without_bootstrap`, `status_and_wait_for_seq`, `a_slave_without_data_is_not_ready_and_reports_the_connection_error`, `a_batching_slave_applies_every_entry_on_its_own` (fetch batching on; each entry still its own write and listener call), `a_panicking_listener_is_disabled_and_replication_goes_on`). The edge-case test moves a ~16 MiB value through Tail and Bootstrap; allow it a few seconds in debug builds.

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
  db.write(wb)          (sync = false by default)      failure → Halted(Corrupt), never retried in-process
  if a verify capture is requested at this seq: take the snapshot now (§12)
  listener.on_entry(&entry)
  publish seq to wait_for_seq and downstream tail subscribers
```

and after the bullet that starts with `- **One entry is one WriteBatch, and an entry is never split.**` add:

```
- **Upstream epoch histories are validated** (in an `EPOCH_ADVANCED` handshake and in a bootstrap header): non-empty, first record `{1, 1}`, epochs strictly increasing, start seqs non-decreasing, last epoch equal to the identity's epoch. An invalid history halts the slave with `Corrupt`.
```

In the PROTOCOL.md §11.2 table, after the row that starts with `` | `RESUME` / `EPOCH_ADVANCED` | `` add:

```
| An epoch history (handshake or bootstrap header) that is empty, does not start with `{1, 1}`, has non-increasing epochs or decreasing start seqs, or does not end with the identity's epoch; or a local failure to apply or commit a received entry | Halt with `Corrupt` (persisted). The entry is never retried in the same process. |
```

- [ ] **Step 12: Commit**

```bash
git add src tests DESIGN.md PROTOCOL.md
git commit -m "feat: slave with bootstrap install, tail apply, read API and end-to-end tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---
### Task 8: Halt recovery and re-bootstrap policies end to end

**Files:**
- Modify: `src/slave/mod.rs` (`clear_halt`)
- Create: `tests/rebootstrap.rs`

**Interfaces:**
- Consumes: Task 7 `Slave`, `Inner::{state, set_state, set_last_error, force_bootstrap, halt_cleared}`, harness; Task 6 `NodeState`, `HaltAction`; Task 2 `meta::key::NODE_STATE`.
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

### Task 9: Chained replication — slaves serve downstream replicas

**Files:**
- Create: `src/slave/source.rs`, `tests/chain.rs`
- Modify: `src/slave/mod.rs` (serve, `serve_addr`), `tests/support/mod.rs` (serving slaves, `restart_slave`), `DESIGN.md` (§16)

**Interfaces:**
- Consumes: Task 5 `server::start`, `ServerHandle`; Task 2 `LogSource`; Task 7 `Inner::{ident, is_ready, upstream_chain, origin_last_seq, max_entry_bytes, seq_tx, reset_tx}`.
- Produces: `impl LogSource for slave::inner::Inner`; `Slave::open` starts a server when `cfg.serve` is `Some`; `Slave::serve_addr(&self) -> Option<SocketAddr>`; harness `Cluster::restart_slave(&self, Slave, SlaveConfig) -> Slave` and serving slaves by default.

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

Create `src/slave/source.rs`:

```rust
//! A slave as a replication source for downstream replicas (chaining, DESIGN.md §11).

use std::sync::atomic::Ordering;
use std::time::Duration;

use tokio::sync::watch;

use super::inner::Inner;
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
}
```

In `src/slave/mod.rs`, add `mod source;` to the module list and `use std::net::SocketAddr;` to the imports. In `Slave::open`, replace these lines (as Task 7 wrote them):

```rust
        let driver = driver::spawn(&inner);
        let maintenance = maintenance::spawn(&inner);
        Ok(Slave { inner, driver: Some(driver), maintenance: Some(maintenance), server: None })
    }
```

with:

```rust
        let server = crate::server::start_for(&inner, inner.cfg.serve.clone()).await?;
        let driver = driver::spawn(&inner);
        let maintenance = maintenance::spawn(&inner);
        Ok(Slave { inner, driver: Some(driver), maintenance: Some(maintenance), server })
    }

    /// The address this slave serves downstream replicas on, if `serve` is configured.
    pub fn serve_addr(&self) -> Option<SocketAddr> {
        self.server.as_ref().map(ServerHandle::addr)
    }
```

- [ ] **Step 5: Run the tests**

Run: `cargo test --test chain`
Expected: PASS, 4 tests: `a_chain_replicates_everything_and_bootstraps_from_the_middle` (C bootstraps from B while A writes; CF create/drop, TTL and the edge-case dataset reach C; C's log equals A's byte for byte; `upstream_chain == ["b", "a"]`), `a_loop_is_detected_and_the_halt_is_not_persisted`, `a_slave_pointed_at_itself_halts_loop_detected`, `an_upstream_rebootstrap_resets_downstream_streams`.

Run: `cargo test`
Expected: PASS (the earlier suites now run with serving slaves).

- [ ] **Step 6: Update DESIGN.md §16**

In `impl Slave`, after `pub fn applied_seq(&self) -> Seq;` add:

```
    pub fn serve_addr(&self) -> Option<SocketAddr>;     // when serve is configured
```

- [ ] **Step 7: Commit**

```bash
git add src tests DESIGN.md
git commit -m "feat: slaves serve downstream replicas; chain, loop and reset tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Promotion

**Files:**
- Create: `src/store/promote.rs`, `src/slave/promote.rs`, `tests/promotion.rs`
- Modify: `src/store/mod.rs`, `src/slave/mod.rs`, `DESIGN.md` (§13.1)

**Interfaces:**
- Consumes: Task 6 `Store::delete_all_keys`; Task 7 `background::blocking`; Plan 1 `ttl::index_key`, `Store::{ttl_due, handle, cfs}`, `Master::open`, `MasterConfig::validate`; Task 6 `Store::finish_bootstrap` (tests); Task 7 `Slave::shutdown`, `Inner::{state, is_ready, reset_downstream}`.
- Produces: `Store::rebuild_ttl_index(&self) -> Result<u64>`, `Store::promote_to_master(&self) -> Result<DbIdentity>`, `Slave::promote(self, cfg: MasterConfig) -> Result<Master>` (async; `Err(Config)` naming the `ReadSnapshot` if one still holds the database after the slave shut down).

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

use replicated_rocksdb::{
    CfSpec, DivergencePolicy, EpochStart, Error, HaltReason, LogApi, Master, Op, ReadApi, Slave, SlaveState,
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
```

- [ ] **Step 2: Run them to confirm they fail**

Run: `cargo test --lib store::promote`
Expected: FAIL to compile: `no method named rebuild_ttl_index`, `no method named promote_to_master`.

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
```

Run: `cargo test --lib store::promote`
Expected: PASS, 2 tests.

- [ ] **Step 4: Implement `Slave::promote`**

Create `src/slave/promote.rs`:

```rust
//! Promotion of a slave to master (DESIGN.md §13.1).

use std::sync::Arc;

use super::state::{HaltReason, SlaveState};
use super::Slave;
use crate::config::MasterConfig;
use crate::error::{Error, Result};
use crate::master::Master;
use crate::store::Store;
use crate::types::Role;

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
        let refusal = match self.inner.state() {
            SlaveState::Bootstrapping { .. } => Some("cannot promote while bootstrapping".to_string()),
            SlaveState::Halted(r @ (HaltReason::Corrupt | HaltReason::SpecMismatch)) => {
                Some(format!("cannot promote a slave halted with {r:?}"))
            }
            _ if !self.inner.is_ready() => Some("this slave has no consistent data to promote".to_string()),
            _ if cfg.path != self.inner.cfg.path => Some("MasterConfig.path must be the slave's path".to_string()),
            _ => cfg.validate().err().map(|e| e.to_string()),
        };
        if let Some(msg) = refusal {
            self.shutdown().await?;
            return Err(Error::Config(msg));
        }
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
```

Add `mod promote;` to the module list in `src/slave/mod.rs`.

- [ ] **Step 5: Run the end-to-end tests**

Run: `cargo test --test promotion`
Expected: PASS, 3 tests: `promotion_end_to_end` (including that the new master's reaper `Delete` reaches C through `entries_since`), `the_old_master_is_refused_with_stale_epoch_and_the_client_retries`, `promotion_is_refused_without_data_and_needs_released_snapshots` (`Error::Config` naming the open `ReadSnapshot`; the directory reopens as a slave afterwards).

- [ ] **Step 6: Update DESIGN.md §13.1**

After step 5 of §13.1 (`5. Open with Master::open(cfg). …`), add:

```
`promote(self)` consumes the slave. If it is refused (bootstrapping, no data, halted `Corrupt`/`SpecMismatch`, wrong `cfg.path`, invalid `cfg`) or fails (for example because a `ReadSnapshot` of the slave is still alive and keeps the database open), the slave has been shut down cleanly and its directory is unchanged, so it can be reopened as a slave.
```

- [ ] **Step 7: Commit**

```bash
git add src tests DESIGN.md
git commit -m "feat: Slave::promote with TTL index rebuild and end-to-end promotion tests

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 11: TLS, mutual TLS and token authentication end to end

**Files:**
- Create: `tests/security.rs`

**Interfaces:**
- Consumes: Task 4 `TlsServerConfig`, `TlsClientConfig`, `TlsIdentity`, `TokenAuthenticator`, `TokenCredentials`, `AllowMethods`, `MtlsSubjectAllowList`, `Method`; Task 7/9 harness; rcgen 0.14 (`KeyPair::generate`, `CertificateParams::{new, self_signed, signed_by}`, `Issuer::new`, `DnType::CommonName`, `IsCa::Ca(BasicConstraints::Unconstrained)`).
- Produces: nothing new in `src/` — this task proves the Task 4/5/7 code end to end. If a test fails, fix the code it points at (`src/server/mod.rs::call_info`/`authorize`, `src/slave/client.rs`, `src/config.rs::to_tonic`).

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

### Task 12: Failure and resume, with fault injection

**Files:**
- Create: `tests/failure_resume.rs`, `tests/failpoints.rs`
- Modify: `src/slave/mod.rs` (`kill`), `src/failpoint.rs` (`io_error`), `src/master/writer.rs` (failpoint in `bump_epoch`), `src/store/cfs.rs`, `src/store/mod.rs`, `src/codec/mod.rs` (unused Plan 1 placeholders), `Cargo.toml` (`[[test]]`), `DESIGN.md` (§16)

**Interfaces:**
- Consumes: Task 5 `failpoint::fire` points `server-tail-after-entry`; Task 7 points `slave-after-apply`, `slave-bootstrap-after-chunk`; Task 5 `ServerHandle::abort`, `await_sole_owner`; Plan 1 `Inner::bump_epoch` (fences the writer when its synced write fails); harness (`restart_master`, `restart_slave`, `copy_dir`, `restore_dir`).
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
Expected: PASS, 3 tests: `restarts_of_master_mid_chain_and_leaf_resume_without_bootstrap`, `a_downstream_ahead_of_its_upstream_waits_and_converges`, `a_log_gap_halt_persists_across_restart_until_cleared`. (These exercise code from Tasks 5–9; if one fails, the bug is there.)

- [ ] **Step 3: Write the failing fault-injection tests**

Create `tests/failpoints.rs`:

```rust
//! Fault injection, end to end. Needs `--features failpoints`. The tests share the global
//! failpoint registry, so they run one at a time (a file-level mutex plus `FailScenario`).

mod support;

use std::time::Duration;

use tokio::sync::{Mutex, MutexGuard};

use replicated_rocksdb::{CfSpec, LogApi, Master, ReadApi, Slave, SlaveState};
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
Expected: PASS, 5 tests (run one at a time through the file-level mutex): `crash_mid_bootstrap_restarts_the_bootstrap` (also asserts the abandoned bootstrap's guard is released: retention passes its `S` again), `crash_after_apply_resumes_without_gap_or_duplicate`, `connection_drop_mid_tail_resumes`, `bootstrap_lease_survives_aggressive_retention` (the slave is held with the `pause` action — no sleeps as synchronisation, no blocked timing windows — and released with `fail::remove`), `a_failed_epoch_bump_fences_the_master_writer`.

- [ ] **Step 7: Remove Plan 1 placeholders Plan 2 does not use**

These items were kept for Plan 2 behind `allow`s, but Plan 2 uses `cfs.pending.lock()`, `LogEntry::decode_ops()` and `LogSource::role()` instead. Delete them:
- in `src/store/cfs.rs`: the methods `pending_entries` and `push_pending`, each with its `#[allow(dead_code)] // used by bootstrap (Plan 2)` line;
- in `src/store/mod.rs`: the method `role(&self) -> Role` with its `#[allow(dead_code)] // used by the slave (later task)` line (the `role` field stays; `initialise_meta` uses it);
- in `src/codec/mod.rs`: the lines `#[allow(unused_imports)] // used by the replication client (Plan 2)` and `pub(crate) use entry::decode_ops;`.

Run: `grep -rn "Plan 2)" src`
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
| `Master::replicas()`, `Slave::replicas()`, `ReplicaStatus` | Tail producers in `src/server/tail.rs` |
| `SlaveStatus::{lag_ms, last_verify}`, tracing spans per handshake/bootstrap/verify | `SlaveStatus`, `tracing` events already emitted |
