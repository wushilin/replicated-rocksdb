# Hardening batch Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Ship items A, B, C, D, F of the hardening batch (required `uses_tokens`, zstd wire
compression, more RocksDB tuning, slave group commit, HTTP/JSON status), each as its own
branch and PR, plus E (a read-only security review).

**Architecture:** Each item is a self-contained branch off `main`, one git worktree each.
F branches off B (both touch `ServeConfig` and `server::start`). Items never change
`PROTOCOL_VERSION`, `replication.proto` messages, or the bytes of a `LogEntry`.

**Tech Stack:** Rust 2024 (MSRV 1.85), rocksdb 0.25, tokio, tonic 0.14 + prost, axum (via
tonic or direct), serde/serde_json.

**Spec:** `docs/superpowers/specs/2026-10-11-hardening-batch.md`. Read it, and read
CLAUDE.md and the DESIGN.md sections named in your task, before changing code.

## Global Constraints

- Every invariant in CLAUDE.md holds. In particular: a `LogEntry` is never split across
  batches; slaves forward entries verbatim; mutation, `__changelog[seq]` and `__meta.last_seq`
  are in the same `WriteBatch`.
- `PROTOCOL_VERSION` unchanged; `proto/replication.proto` unchanged (it must equal
  PROTOCOL.md §14); the codec still passes §15 vectors.
- No merge operator, compaction filter, native TTL or prefix extractor can be configured.
- Tokens are never logged.
- Rustdoc on every new public item. Match surrounding comment density and naming.
- `cargo test --all-targets`, `cargo test --features failpoints --test failpoints`,
  `cargo clippy --all-targets -- -D warnings` and `cargo doc --no-deps` pass before a
  branch is offered for review.
- Commits end with `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

- A custom authenticator that wraps another (like `AllowMethods`) must report the inner's
  `uses_tokens`, otherwise the plaintext guard is bypassed through the wrapper.
- B: a slave with `Zstd` talking to an upstream with `None` (and the reverse) must still
  replicate; compression must not change entry CRCs or forwarded `ops` bytes in a chain
  A→B→C with mixed settings.
- C: `property(Some(cf))` on a dropped CF, or on an internal CF name like `__meta`, must
  return `UnknownCf`, never touch internal CFs.
- F: the HTTP endpoints must apply the same failure limiter as gRPC (a token brute force over
  HTTP gets 429), and gRPC on the same port keeps working while HTTP is enabled.
- D: a verify capture seq, a `repair_until` target, or a CF op in the middle of a received
  batch must end the group exactly there; a crash between groups must resume from the durable
  `last_seq` with no gap.

---

## Execution layout

| Item | Branch | Base | Worktree |
|------|--------|------|----------|
| A | `feat/required-uses-tokens` | `main` | `../rrdb-wt/a` |
| B | `feat/wire-compression` | `main` | `../rrdb-wt/b` |
| C | `feat/rocks-tuning` | `main` | `../rrdb-wt/c` |
| D | `feat/group-commit` | `main` | `../rrdb-wt/d` |
| F | `feat/http-status` | `feat/wire-compression` | `../rrdb-wt/f` |
| E | none (read-only review of `main`) | | |

Each worktree builds into its own `target/`. Merge order into `main`: A, B, C, F, D.

---

## Item A: `Authenticator::uses_tokens()` required

### Task A1: remove the default and make every impl explicit

**Files:**
- Modify: `src/auth.rs` (trait at ~line 55; impls at ~103, ~141, ~248, ~282)
- Modify: `tests/repair.rs:416` (`VerifyAllowedFor`), any other `impl Authenticator` found by
  `grep -rn "impl Authenticator for" src tests examples`
- Modify: `DESIGN.md` §14 trait snippet

**Interfaces:**
- Produces: `fn uses_tokens(&self) -> bool;` (no default) on `Authenticator`.

- [ ] **Step 1: Write the failing tests** in `src/auth.rs` `mod tests`:

```rust
#[test]
fn allow_methods_reports_the_inner_uses_tokens() {
    let tok: Arc<dyn Authenticator> = Arc::new(TokenAuthenticator::new(["s3cret"]));
    assert!(AllowMethods::new(tok, &[Method::Status]).uses_tokens());
    let open: Arc<dyn Authenticator> = Arc::new(AllowAll);
    assert!(!AllowMethods::new(open, &[Method::Status]).uses_tokens());
}
```

and in `src/config.rs` tests, next to `token_auth_on_plaintext_is_refused_unless_allowed`:

```rust
#[test]
fn wrapped_token_auth_on_plaintext_is_refused() {
    let inner: Arc<dyn crate::auth::Authenticator> =
        Arc::new(crate::auth::TokenAuthenticator::new(["s3cret"]));
    let auth = Arc::new(crate::auth::AllowMethods::new(inner, &[crate::auth::Method::Status]));
    let cfg = ServeConfig::new("10.0.0.1".parse().unwrap(), 0, auth);
    assert!(cfg.validate().is_err());
}
```

- [ ] **Step 2: Run** `cargo test --lib auth:: config::` and check whether the new tests fail
  (they fail if `AllowMethods` does not delegate today).
- [ ] **Step 3: Implement.** Delete the default body; write `fn uses_tokens(&self) -> bool`
  in each impl (`AllowAll` false, `TokenAuthenticator` true, `MtlsSubjectAllowList` false,
  `AllowMethods` `self.inner.uses_tokens()`, test impls whatever they wrap or `false`).
  Replace the "**Security:** ... fails open" paragraph with: what the method means, that it
  is required so a token authenticator cannot accidentally run on plaintext, and that
  wrappers must delegate.
- [ ] **Step 4: Add a `compile_fail` doctest** on the trait:

```rust
/// ```compile_fail
/// use replicated_rocksdb::auth::{Authenticator, AuthError, Method, PeerInfo};
/// struct Mine;
/// #[async_trait::async_trait]
/// impl Authenticator for Mine {
///     async fn authenticate(&self, _: &PeerInfo, _: Method, _: Option<&[u8]>)
///         -> Result<(), AuthError> { Ok(()) }
/// }
/// ```
```

  Check the module path used by existing doctests (`replicated_rocksdb::auth` vs re-export at
  root) and use the public one. The doctest must fail **only** because `uses_tokens` is
  missing: verify by temporarily adding the method and seeing it compile.
- [ ] **Step 5: Update DESIGN.md §14** snippet to show `fn uses_tokens(&self) -> bool;`.
- [ ] **Step 6: Run the full gate** (Global Constraints) and commit:
  `feat(auth)!: require Authenticator::uses_tokens so token auth cannot fail open`.

---

## Item B: optional zstd wire compression

### Task B1: `WireCompression` config and dependency

**Files:**
- Modify: `Cargo.toml` (`tonic` features: add `"zstd"`)
- Modify: `src/config.rs` (`ServeConfig`), `src/slave/config.rs` (`UpstreamConfig`),
  `src/lib.rs` (export)
- Create: nothing (put the enum in `src/config.rs` beside `ServeConfig`)

**Interfaces:**
- Produces:

```rust
/// gRPC message compression this side sends (PROTOCOL.md §3.5). Every node always accepts
/// zstd, so the settings of two peers never need to match.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum WireCompression {
    #[default]
    None,
    Zstd,
}
impl WireCompression {
    pub(crate) fn encoding(self) -> Option<tonic::codec::CompressionEncoding> {
        match self {
            WireCompression::None => None,
            WireCompression::Zstd => Some(tonic::codec::CompressionEncoding::Zstd),
        }
    }
}
```

  `ServeConfig.compression: WireCompression` (default `None` in `ServeConfig::new`);
  `UpstreamConfig.compression: WireCompression` (default `None` in `UpstreamConfig::new`).
  If either struct is built by struct literal anywhere (tests, examples, playground, node
  config), add the field there.

- [ ] **Step 1: Failing unit test** in `src/config.rs`: `ServeConfig::new(..).compression ==
  WireCompression::None`; same for `UpstreamConfig::new`.
- [ ] **Step 2: Implement** the enum, fields, export; `cargo build --all-targets`.
- [ ] **Step 3: Commit** `feat(net): WireCompression setting for servers and upstreams`.

### Task B2: wire it into server and client

**Files:**
- Modify: `src/server/mod.rs` (`start`, ~line 536: `ReplicationServer::new(..)`)
- Modify: `src/slave/client.rs` (where the `ReplicationClient` is created from the channel)
- Test: `tests/compression.rs` (new)

- [ ] **Step 1: Write the failing integration test** `tests/compression.rs`, modelled on an
  existing master + slave test in `tests/` (reuse its helpers). Cases, each: master serves
  with compression X, slave upstream with Y; write 1 000 keys with 1 KiB compressible
  values, create a CF, wait for the slave to catch up, `verify_now(Full)` is clean, values
  equal:
  - `(Zstd, None)`, `(None, Zstd)`, `(Zstd, Zstd)`.
  - A chain master(Zstd) → B(serve None, upstream Zstd) → C(upstream None): C's
    `__changelog` entries have the same CRCs as the master's (compare via `Status`/log read
    helpers already used by chain tests).
  - Bootstrap path: start the slave after the writes so it bootstraps compressed.
- [ ] **Step 2: Run** `cargo test --test compression` → fails to compile (field missing) or
  passes trivially; make sure it compiles against B1 and then proves compression is in effect:
  add a check that the server actually negotiated zstd. If no public hook shows it, a
  `tracing` debug line is not enough; instead assert via a tonic interceptor in the test
  client that the response `grpc-encoding` header is `zstd` for `(Zstd, Zstd)`. Use
  `server::test_client` if it builds raw clients.
- [ ] **Step 3: Implement.** Server:
  `service = service.accept_compressed(CompressionEncoding::Zstd)`, and
  `if let Some(e) = cfg.compression.encoding() { service = service.send_compressed(e) }`.
  Client: `client.accept_compressed(Zstd)` always; `send_compressed` when the upstream
  config says so. Every client constructor in `src/slave/client.rs` and anything else that
  builds a `ReplicationClient` (`grep -rn "ReplicationClient::" src`) goes through one helper.
- [ ] **Step 4: Run** the compression tests, then the full gate.
- [ ] **Step 5: Docs.** PROTOCOL.md: new short subsection under transport/§3 ("Message
  compression"): optional, negotiated by gRPC `grpc-encoding` / `grpc-accept-encoding`;
  implementations MUST accept `identity` and `zstd` and MAY send `zstd`; compression never
  changes message bytes after decompression, so CRCs and verbatim forwarding are unaffected.
  DESIGN.md §15/§16 config listings and §18 defaults gain the field. README config section,
  one line.
- [ ] **Step 6: Commit** `feat(net): optional zstd compression of replication traffic`.

---

## Item C: more RocksDB tuning

### Task C1: table, compaction and IO knobs on `Tunable`

**Files:**
- Modify: `src/tuning.rs`
- Modify: `src/lib.rs` (export `CompactionStyle`)

**Interfaces:**
- Produces on `Tunable`: `set_block_size(usize)`, `set_bloom_filter(f64 bits_per_key)`,
  `set_cache_index_and_filter_blocks(bool)`, `set_compaction_style(CompactionStyle)`,
  `set_level_zero_file_num_compaction_trigger(i32)`, `set_max_bytes_for_level_base(u64)`,
  `set_level_compaction_dynamic_level_bytes(bool)`, `set_bottommost_compression(Compression)`,
  `set_bytes_per_sync(u64)`, `set_wal_bytes_per_sync(u64)`, `set_max_total_wal_size(u64)`.
  `pub enum CompactionStyle { Level, Universal }`.
- `Tunable` gains private state for the block-based table (`table: BlockBasedOptions`
  plus a `table_touched: bool`, or an `Option<BlockBasedOptions>`), applied to `Options` by
  `RocksTuning` after the callback returns (`opts.set_block_based_table_factory(&t)`), so
  several table setters combine. The shared cache from C2 is attached there too.

- [ ] **Step 1: Failing tests** in `src/tuning.rs` tests: a callback that calls every new
  setter for `CfKind::User` and `CfKind::Db` produces options without panicking, and a store
  opened with them (use the existing `store::test_support::open_store` pattern, or the
  existing test that opens with a tuning) can write and read back a key. For a knob RocksDB
  reports through a property, assert it; otherwise opening and round-tripping is the test.
- [ ] **Step 2: Implement** the setters by forwarding to `rocksdb::Options` /
  `BlockBasedOptions` (rocksdb 0.25 API: `set_block_size`, `set_bloom_filter(bits, false)`,
  `set_cache_index_and_filter_blocks`, `set_compaction_style(DBCompactionStyle::..)`,
  `set_level_zero_file_num_compaction_trigger`, `set_max_bytes_for_level_base`,
  `set_level_compaction_dynamic_level_bytes`, `set_bottommost_compression_type`,
  `set_bytes_per_sync`, `set_wal_bytes_per_sync`, `set_max_total_wal_size`).
- [ ] **Step 3: compile_fail doctests** next to the existing merge-operator one, one each for
  `set_compaction_filter`, `set_prefix_extractor`, and `set_ttl`/`set_periodic_compaction`
  style native TTL if `Options` has a method of that name reachable by `Deref` (there is no
  `Deref`, so each simply fails to resolve; the doctest documents intent). Rustdoc on
  `Tunable` explains the prefix extractor exclusion (iterators assume total order; bootstrap,
  verify and `prefix_iter` would silently skip keys).
- [ ] **Step 4: Gate and commit** `feat(tuning): block table, compaction and IO knobs`.

### Task C2: shared block cache and rate limiter on `RocksTuning`

**Files:** `src/tuning.rs`, `src/store/mod.rs` (where `db_options`/`cf_options` are used)

**Interfaces:**
- Produces: `RocksTuning::with_block_cache(self, bytes: usize) -> Self`,
  `RocksTuning::with_rate_limit(self, bytes_per_sec: i64) -> Self`. The cache is created
  once per `RocksTuning` value (`Option<rocksdb::Cache>`, cloned handle) and attached to the
  block-based table of every CF (internal and user), including CFs with no table setter
  called. The rate limiter is set on the DB options (`set_ratelimiter(bytes, 100_000, 10)`).

- [ ] **Step 1: Failing test:** with `with_block_cache(8 << 20)`, open a store, create two
  user CFs, write and read keys in both; `property(None, "rocksdb.block-cache-capacity")`
  (C3) equals `8 << 20`. (Order: write this test after C3 exists, or assert via the cache
  handle's `get_usage()` > 0 after reads.)
- [ ] **Step 2: Implement**, keeping `RocksTuning: Clone` (cache handles are clonable).
- [ ] **Step 3: Gate and commit** `feat(tuning): shared block cache and rate limiter`.

### Task C3: read-only RocksDB properties

**Files:** `src/store/mod.rs` (or a new `src/store/props.rs`), `src/master/mod.rs`,
`src/slave/mod.rs`, `src/node/handle.rs`

**Interfaces:**
- Produces on `Master`, `Slave`, `Node`:

```rust
/// A RocksDB property (for example `rocksdb.estimate-num-keys`). `cf = None` is DB-wide;
/// `Some(name)` is a user column family. `Ok(None)` when RocksDB does not know the property.
pub fn property(&self, cf: Option<&str>, name: &str) -> Result<Option<String>>;
pub fn property_int(&self, cf: Option<&str>, name: &str) -> Result<Option<u64>>;
```

  Store helper `pub(crate) fn property(&self, cf: Option<&str>, name: &str, int: bool)`
  resolving user CFs through `self.cfs.read()`; unknown or internal → the same error the
  read API gives for an unknown CF (`grep -n "NoSuchCf\|UnknownCf" src/error.rs`).
  `Node` delegates to the current role; while switching it returns the same error other
  `Node` methods return.

- [ ] **Step 1: Failing tests:** DB-wide `rocksdb.estimate-num-keys` is `Some`; user CF after
  10 puts is `Some(n)`; `Some("__meta")` and `Some("nope")` → error; an unknown property →
  `Ok(None)`; same through `Node` and `Slave` (reuse an existing node test harness).
- [ ] **Step 2: Implement.** **Step 3:** rustdoc, README tuning section listing the new
  knobs and `property`, DESIGN.md §16 public API. **Step 4: Gate, commit**
  `feat: read-only RocksDB properties on Master, Slave and Node`.

---

## Item F: HTTP/JSON status on the replication port (branch off B)

### Task F1: feasibility spike (throwaway) and decision

- [ ] Check whether tonic 0.14's `Server` can serve an axum `Router` for HTTP/1.1 and
  gRPC on one listener with TLS: `Server::builder().accept_http1(true)`, and
  `Routes::from(axum_router).add_service(grpc)` / `Routes::into_axum_router` (look at
  `~/.cargo/registry/src/*/tonic-0.14*/src/service/router.rs`). Build a 30-line scratch test.
- [ ] If it works: one listener, `ServeConfig.http_status: bool`. If not: separate port
  `ServeConfig.http_status: Option<u16>` with the same TLS acceptor and authenticator.
  Record the decision and why in the commit message and DESIGN.md §15. Delete the scratch.

### Task F2: authentication shared with gRPC

**Files:** `src/server/mod.rs` (`ServerState::authorize`, `call_info`), new
`src/server/http.rs`

**Interfaces:**
- Produces: `pub(crate) async fn authorize_http(state: &ServerState, peer: PeerInfo,
  authorization: Option<&HeaderValue>) -> Result<Arc<dyn LogSource>, HttpAuthError>` built on
  the same code as `authorize` (same failure limiter, `Method::Status`, token parsing per
  PROTOCOL.md §3.2). Refactor so gRPC and HTTP call one function; do not duplicate the
  limiter logic. `HttpAuthError` maps to 401 / 403 / 429 / 503 (no source).

- [ ] **Step 1: Failing tests** (unit, in `src/server/http.rs`): missing header → 401; bad
  token → 401 and counts toward the limiter; after `max_auth_failures_per_minute` failures →
  429 even with a good token; good token → Ok; `PermissionDenied` from an `AllowMethods`
  without `Status` → 403. Peer identity from mTLS is passed the same way gRPC gets it.
- [ ] **Step 2: Implement**, **Step 3: run**, **Step 4: commit**
  `refactor(server): one authorization path for gRPC and HTTP`.

### Task F3: endpoints

**Files:** `src/server/http.rs`, `src/server/status.rs` (JSON view), `src/server/mod.rs`,
`src/config.rs`, `Cargo.toml` (`serde`, `serde_json` to `[dependencies]`; `axum` only if
tonic does not re-export what is needed)

**Interfaces:**
- `/v1/status` JSON is built from `status::node_info(..)` → a `#[derive(Serialize)] struct
  StatusJson` (in `status.rs`) with: `node_id`, `role`, `identity: {db_uuid, epoch,
  nonce...}` (UUID canonical string; match the fields `Identity` has), `epoch_history:
  [{epoch, start_seq, nonce}]`, `last_seq`, `last_entry_crc: "0x%08x" | null`,
  `last_ts_ms`, `node_state`, `upstream`, `replicas: [{node_id, applied_seq, in_sync}]`,
  `max_burned_epoch`. Same source as the gRPC handler; no second computation.
- `/v1/health` → `{"ok":true}`; `/v1/ready` → 200/503 `{"ready":bool,"node_state":".."}`;
  `/metrics` → `text/plain; version=0.0.4` with `rrdb_last_seq`, `rrdb_last_ts_ms`,
  `rrdb_role{role="master"} 1`, `rrdb_node_ready`, `rrdb_epoch`,
  `rrdb_replica_applied_seq{node_id=".."}`, `rrdb_replica_in_sync{node_id=".."}` (escape label
  values per the Prometheus text format). Unknown path 404, non-GET 405.

- [ ] **Step 1: Failing integration test** `tests/http_status.rs`: master with TLS (reuse the
  rcgen helpers from the existing TLS test), `TokenAuthenticator`, `http_status` on, one
  slave replicating over gRPC on the same port. Use a minimal HTTP client: `hyper`/`reqwest`
  only if already in the dependency tree for tests, else add `reqwest` (rustls) as a
  dev-dependency. Assert: `/v1/status` with token → 200, `last_seq` equals the master's;
  without → 401; `/v1/ready` 200 on master; a halted slave serving with `http_status` →
  503; `/metrics` contains `rrdb_last_seq <n>` and a replica line; 404 and 405 cases; the slave
  is still in sync (gRPC unaffected). With `http_status` off, `/v1/status` is not served
  (connection-level error or 404, whichever the listener gives, asserted explicitly).
- [ ] **Step 2: Implement.** **Step 3: run, gate.**
- [ ] **Step 4: Docs:** README "Status over HTTP" with curl examples (`curl -H
  "authorization: Bearer $TOKEN" https://node:7000/v1/status`, `--cacert`), DESIGN.md §15
  and §19 (Observability), CLAUDE.md untouched unless an invariant changes (none should).
- [ ] **Step 5: Commit** `feat(server): JSON status, readiness and Prometheus metrics over HTTP`.

---

## Item D: group commit on slaves

### Task D1: store support for committing a group

**Files:** `src/store/apply.rs`

**Interfaces:**
- Produces:

```rust
/// Prepares `entry` on top of `prev` (the previous entry of the same group), instead of the
/// stored position. CF entries are never grouped.
pub(crate) fn prepare_after(&self, prev: Position, entry: &LogEntry, ops: &[Op]) -> Result<Prepared>;
/// Commits several prepared non-CF entries in one WriteBatch; the stored position becomes
/// the last one's. Errors if any carries a CF change.
pub(crate) fn commit_group(&self, group: Vec<Prepared>, sync: bool) -> Result<()>;
```

  `prepare` becomes `prepare_after(self.position(), ..)`. `Prepared` must expose its
  resulting `Position` (seq, crc) so the caller can chain. `commit_group` concatenates
  batches (`WriteBatch` has no append in rocksdb 0.25: build one batch by preparing into a
  shared `&mut WriteBatch` instead; restructure `prepare_after` to write into a caller-given
  batch if needed, keeping `prepare` for the master's path unchanged in behaviour).
  `__meta.last_seq` / `last_entry_crc` are written once per group with the last entry's
  values, or once per entry (later keys overwrite earlier ones inside the batch, fine either
  way); `__changelog[seq]` for every entry.

- [ ] **Step 1: Failing tests** in `src/store/apply.rs` tests: three Put entries committed as
  one group → position is the third; all three changelog rows exist; reads show all values;
  a group containing a CF entry is refused; `prepare_after` with a non-consecutive seq is
  `Corrupt`.
- [ ] **Step 2: Implement.** Check every reader of `Prepared` (master write path too:
  `grep -rn "prepare(\|commit(" src/master src/store`) still behaves identically.
- [ ] **Step 3: Gate, commit** `feat(store): commit several entries in one WriteBatch`.

### Task D2: slave applies groups

**Files:** `src/slave/apply.rs`, `src/slave/driver.rs` (~line 880–900, the per-entry loop),
`src/slave/config.rs` (`max_group_entries: usize`, default 256, must be ≥ 1; 4 MiB ops cap
as a const), DESIGN.md §6, §16, §18

**Rules (spec §D):** per-entry checks (CRC, epoch, exactly-once skip, gap) run before an entry
joins the group; a failing entry ends the group *before* it: commit the group, then handle the
failure exactly as today. A group ends after: a CF entry (which commits alone), the requested
verify capture seq, the `repair_until` seq, the end of the received batch, the entry/byte
limits, or when `wait_while_held` would block. After commit, per entry in order:
`finish_repair_at`, `capture_after_commit`, `pending_ts`, `on_entry`, publish seq. One
`spawn_blocking` per group. Commit-failure handling (transient IO → Retry, `CfInUse` →
Retry, else Halt) is unchanged and applies to the whole group (nothing committed).

- [ ] **Step 1: Failing tests:**
  - test hook counting commits (a `#[cfg(test)]` or failpoint counter in `Inner`): 1 000
    puts in a burst with `sync_writes = true` → commits < 1 000 and slave data equals master.
  - verify capture mid-burst: start `verify_now` while a burst streams; report clean and
    `report.seq` equals the capture seq (existing verify tests show how to hold the applier).
  - a `CreateCf` between puts in one burst: the CF entry commits alone (counter), data right.
  - corrupt entry mid-batch (failpoint or crafted upstream in `server::test_client`): entries
    before it are applied (`last_seq` = previous), slave halts `Corrupt`.
  - repair target mid-batch: an existing repair test still passes with grouping enabled.
  - `max_group_entries = 1` reproduces today's behaviour (all existing tests pass with it).
- [ ] **Step 2: Implement.** **Step 3:** run `cargo test --all-targets` and the failpoints
  suite; run the whole suite a second time with `max_group_entries = 1` forced via an env var
  read only in tests, or a test-only default switch, to prove equivalence.
- [ ] **Step 4: Docs:** DESIGN.md §6 per spec (pseudo-code shows grouping; the bullet "One
  entry is one WriteBatch" becomes "An entry is never split; consecutive entries may share one
  WriteBatch; every snapshot is still a consistent state at some seq"). CLAUDE.md atomicity
  line stays true (an entry is never split); adjust wording only if it says "one entry per
  batch".
- [ ] **Step 5: Commit** `feat(slave): group commit of consecutive entries`.

---

## Item E: security review (read-only, runs in parallel)

- [ ] Review `main` for: authn/authz on every RPC; consent scoping (CLAUDE.md "Consent is
  scoped"); limits and memory bounds per RPC (message sizes, stream counts, verify sessions,
  bootstrap concurrency, lease table, auth limiter); TLS server and client verification
  (hostname, CA, mTLS subject extraction); token handling (never logged, constant-time,
  validation); `allow_plaintext_token`; examples/playground HTTP surface. Each finding verified
  against code, with file:line, severity and a concrete failure scenario. Output a report;
  no code changes.
