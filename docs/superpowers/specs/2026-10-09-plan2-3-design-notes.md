# Design notes for Plans 2 and 3

Implementation-level decisions for the replication layer (Plan 2) and integrity verification (Plan 3). They refine DESIGN.md and PROTOCOL.md; where they disagree, the spec wins unless the note says it amends the spec, in which case the owning task updates the spec text.

User requirements these plans must satisfy, with **real** (end-to-end, in-process, multi-node over real gRPC on 127.0.0.1) test cases:

1. Core features: replication master→slave and chains, TTL replication, CF lifecycle replication, read API on slaves.
2. Replication with rebootstrap: retention gap, identity change, divergence, mismatch policy — each with a test where the node actually re-bootstraps and converges.
3. Promotion (`Slave::promote`) with siblings continuing (EPOCH_ADVANCED), a sibling past the fork re-bootstrapping or halting per policy, and the old master refused (STALE_EPOCH).
4. Failure and resume: master restart, mid-chain restart, leaf restart, crash mid-bootstrap, crash after apply, connection drop mid-tail, downstream ahead of upstream, bootstrap lease under aggressive retention.
5. Integrity scans (Verify) with real corruption injected on disk, keys present only upstream between chunk boundaries, Quick vs Full, chains, and mismatch policy Rebootstrap.
6. Edge-case data on every path (tail, bootstrap, verify, promotion): zero-length keys and values, keys/values containing NUL bytes (incl. all-NUL), a 1 MiB key, values close to the 16 MiB entry limit. Rust has no null `&[u8]`; "null key/value" is covered by zero-length and NUL-byte cases. In protobuf an empty `bytes` field is indistinguishable from an absent one; the codec must round-trip both to empty `Vec<u8>` (it does) and `KeyRange.lower/upper` use `optional bytes`, so an empty key is distinct from "unbounded".

## Dependencies (added in Plan 2 Task 1)

```toml
tonic = { version = "0.14.6", features = ["tls-ring"] }
tonic-prost = "0.14.6"
tonic-types = "0.14.6"
tokio-stream = { version = "0.1.19", features = ["net", "sync"] }
futures-util = "0.3.34"
subtle = "2.6.1"
async-trait = "0.1.92"
fail = { version = "0.5.1", optional = true }

[features]
failpoints = ["dep:fail"]

[dev-dependencies]
rcgen = "0.14.10"
```

`build.rs`: switch to `build_server(true).build_client(true)`. Generated: `pb::replication_server::{Replication, ReplicationServer}`, `pb::replication_client::ReplicationClient`.

Verified APIs: `tonic_types::{ErrorDetails, StatusExt}` (`Status::with_error_details(code, msg, ErrorDetails::with_error_info(reason, domain, HashMap::new()))`, `status.get_error_details().error_info()`), `Request::remote_addr()`, `Request::peer_certs()` (TLS), `ServerTlsConfig::{identity, client_ca_root, client_auth_optional}`, `ClientTlsConfig::{domain_name, ca_certificate, identity}`, generated `max_decoding_message_size` on client and server, `Server::serve_with_incoming_shutdown`.

## Module layout

```
src/net/mod.rs         metadata keys (x-rrdb-protocol-version, x-rrdb-node-id, authorization), Reason <-> gRPC status with ErrorInfo
                       (domain "rrdb.replication.v1"), `fn reason_of(&Status) -> Option<pb::Reason>`
src/net/convert.rs     DbIdentity/CfEntry/EpochStart/LogEntry <-> pb
src/source.rs          LogSource trait (below)
src/auth.rs            Method, PeerInfo, Authenticator, CredentialsProvider, TokenAuthenticator, TokenCredentials,
                       AllowAll, MtlsSubjectAllowList, AuthError; failure rate limiter
src/server/mod.rs      ServerState, start(source, ServeConfig) -> ServerHandle { addr, shutdown() }
src/server/handshake.rs pure `decide(&ServerView, &HandshakeRequest) -> Decision` (+ exhaustive unit tests, one per rule)
src/server/tail.rs     tail producer task
src/server/bootstrap.rs bootstrap producer + leases
src/server/verify.rs   (Plan 3) sessions, HashRange, ListRange; Plan 2 returns Unimplemented
src/slave/mod.rs       Slave public API, ReadApi/LogApi impls
src/slave/config.rs    SlaveConfig, UpstreamConfig, IdentityPolicy, GapPolicy, DivergencePolicy, Backoff
src/slave/state.rs     SlaveState, HaltReason (persisted vs not), NodeState encode/decode, SlaveStatus
src/slave/driver.rs    connect → handshake → act loop; backoff; policies
src/slave/apply.rs     per-entry checks (§7.4) and apply via Store::prepare/commit(sync=false)
src/slave/bootstrap.rs bootstrap install
src/slave/promote.rs   promotion (DESIGN §13.1)
src/slave/verifier.rs  (Plan 3)
src/listener.rs        ReplicationListener
src/store/bootstrap.rs store helpers for bootstrap install and promotion
tests/support/mod.rs   cluster harness (below)
```

## LogSource

The server is generic over this trait; `master::Inner` and `slave::Inner` implement it.

```rust
pub(crate) trait LogSource: Send + Sync + 'static {
    fn node_id(&self) -> &str;
    fn role(&self) -> Role;
    fn is_ready(&self) -> bool;                                 // false while bootstrapping
    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)>;
    fn store(&self) -> &Store;
    fn origin_last_seq(&self) -> Seq;                           // master: last_seq
    fn upstream_chain(&self) -> Vec<String>;                    // master: []
    fn max_entry_bytes(&self) -> u64;
    fn heartbeat(&self) -> Duration;
    fn subscribe(&self) -> watch::Receiver<Seq>;
    fn resets(&self) -> watch::Receiver<u64>;                   // bumped when served streams must end (UPSTREAM_RESET)
}
```

Master changes: `MasterConfig.serve: Option<ServeConfig>` (**amends DESIGN §16**: `None` = embedded, non-replicating master; tests and embedded users need it), `Inner.reset_tx`, `bump_epoch` bumps `reset_tx`. `Master::serve_addr() -> Option<SocketAddr>` (tests bind port 0).

Store additions: `entry_crc(seq) -> Result<Option<u32>>`, `read_entries(from, max) -> Result<Vec<LogEntry>>` (via `entries_after`, propagates `Gap`), `ReadSnapshot::meta()` (MetaSnapshot through the snapshot), `ReadCtx::iter_raw(cf)` (raw `(key, envelope)` through the snapshot).

## Server

- One `ServerState { source: Arc<dyn LogSource>, cfg: ServeConfig, leases: Mutex<HashMap<[u8;16], Lease>>, bootstrap_permits: Semaphore, shutdown: watch::Sender<bool> }`.
- Bind with `tokio::net::TcpListener` (port 0 allowed) and `serve_with_incoming_shutdown(TcpListenerStream)`; TLS via `Server::builder().tls_config(..)`. Refuse to start with a token authenticator on plaintext unless `allow_plaintext_token`.
- Shutdown: set `shutdown`, which ends every stream with `UNAVAILABLE/SHUTTING_DOWN` (tonic's graceful shutdown would otherwise wait forever on tail streams), then await the server task.
- Every handler: check `x-rrdb-protocol-version` (except Handshake, which reports in the body), authenticate (`Method`), then work. Errors carry `ErrorInfo{reason, domain}`.
- Tail: producer task feeding a bounded `mpsc` (capacity 16) wrapped in `ReceiverStream`. Reads entries with `spawn_blocking(read_entries(next, 64))`, verifies CRC before sending (`DATA_LOSS/CORRUPT`), heartbeats when idle for `heartbeat` and at least every `heartbeat` while streaming, ends on reset/shutdown/receiver closed, `OUT_OF_RANGE/LOG_GAP` when retention overtook it. Waits (heartbeats only) while `from_seq > last_seq + 1`.
- Bootstrap: `try_acquire` permit else `RESOURCE_EXHAUSTED/BUSY`; snapshot via `store.read_snapshot(0)` + `meta()`; `guard(S)`; producer in `spawn_blocking` using `blocking_send`; records grouped per CF, message soft limit `bootstrap_chunk_bytes`, at least one record; digest per PROTOCOL §5.4. After the footer is sent the guard moves into a `Lease { client_id, snapshot_seq, guard, expires_at }`; leases expire after `bootstrap_lease_ttl` (checked on every Tail/Bootstrap call and by a 1 s sweep). Tail with a matching lease (`client_id` equal, `from_seq <= S + 1`) removes it.
- Decode/encode limits: server `max_decoding_message_size(4 MiB)` (requests are small); client Tail/Bootstrap decode limit per PROTOCOL §11.4.

## Slave

- `slave::Inner { store, cfg, ident: RwLock<Option<(DbIdentity, Vec<EpochStart>)>>, state_tx: watch::Sender<SlaveState>, seq_tx, reset_tx, origin_last_seq, upstream_last_seq, upstream_chain, max_entry_bytes, last_contact, last_error, ready: AtomicBool, force_bootstrap: AtomicBool, halt_cleared: Notify, listener }`.
- New `__meta` keys: `node_state` (text: `ready` | `bootstrapping` | `halted:<Reason>`), `max_entry_bytes`, `bootstrap_cfs` (CfEntryList of physical CFs created by an in-progress bootstrap; included in Store::open's known set; dropped by the next `reset_for_bootstrap`).
- Driver loop (one tokio task): if persisted-halted, wait for `clear_halt` or shutdown. Connect (lazy `Endpoint`, TLS, interceptor adding the three metadata keys, `connect_timeout` 5 s). Handshake, then:

| Outcome | Action |
|---|---|
| RESUME | Tail from `last_seq + 1` with `prev_crc32c` |
| RESUME/EPOCH_ADVANCED | persist identity+history (one synced batch), then Tail |
| BOOTSTRAP/NO_LOCAL_STATE | bootstrap |
| BOOTSTRAP/IDENTITY_CHANGED | policy Refuse → halt IdentityChanged (persisted); Rebootstrap → bootstrap |
| BOOTSTRAP/LOG_GAP | policy Rebootstrap → bootstrap; Halt → halt LogGap (persisted) |
| REJECT/DIVERGED | policy Halt → halt Diverged (persisted); Rebootstrap → bootstrap |
| REJECT/STALE_EPOCH, NOT_READY | backoff, retry; set `last_error` |
| REJECT/PROTOCOL_MISMATCH, ENVELOPE_MISMATCH, LOOP_DETECTED | halt (not persisted) |
| UNAUTHENTICATED / PERMISSION_DENIED | halt AuthRejected (not persisted) |
| transport error, UNAVAILABLE, RESOURCE_EXHAUSTED, stream end | backoff, reconnect |

- Tail apply: per entry the §7.4 checks; epoch mismatch → end stream and handshake again; apply in `spawn_blocking` (prepare + commit(sync=false)); `UnknownComparator` → halt SpecMismatch; other prepare errors → halt Corrupt. After commit: `seq_tx.send`, listener `on_entry` (inline, `catch_unwind`).
- Heartbeats update `upstream_last_seq`, `origin_last_seq`, `last_contact`. Idle timeout `3 × heartbeat_ms` → reconnect.
- Bootstrap install (DESIGN §10): `ready=false`, bump `reset_tx`, persist `node_state=bootstrapping` (synced), `store.reset_for_bootstrap()` (live CFs → pending, drop unreferenced, clear changelog and ttl index). Physical name collision with a still-referenced pending CF (a reader's snapshot) → reuse that CF after deleting its keys in batches (snapshot readers are unaffected). Records validated (cf in specs, strictly ascending under the comparator, envelope decodes), written per message in one batch (sync=false). Footer check (count + digest) failure → restart. `finish_bootstrap` writes identity, history, cf_specs, last_seq=S, last_entry_crc (or deletes it), max_entry_bytes, node_state=ready, clears bootstrap_cfs, in one synced batch; reload registry; `first_retained = S + 1`; `ready=true`; then Tail with the lease.
- `wait_for_seq` returns `Err(Halted(reason))` if halted, `Err(Timeout)` on timeout.
- Reads return `Err(NotReady)` while bootstrapping.
- `clear_halt(Retry | Rebootstrap)`: persists `ready`, sets `force_bootstrap` for Rebootstrap, notifies the driver.
- `SlaveConfig.clock: Arc<dyn Clock>` (TTL read filtering and retention).

## Promotion (DESIGN §13.1)

`Slave::promote(self, cfg: MasterConfig) -> Result<Master>`: refuse if Bootstrapping or halted Corrupt/SpecMismatch (`Error::Config`); shutdown (driver, server); requires no outstanding `ReadSnapshot`s (else the DB lock is held → error surfaced); offline `Store::open(path, Role::Slave)`; `rebuild_ttl_index()`; one synced batch: role=master, identity epoch' = max(history)+1, history + {epoch', last_seq+1}, last_ts = ts of entry `last_seq` if retained else 0, delete node_state/max_entry_bytes; close; `Master::open(cfg)`.

## Failpoints (feature `failpoints`)

`fail::fail_point!` at: `slave-after-apply` (after commit, before listener), `slave-bootstrap-after-chunk` (after writing a Records message), `server-tail-after-entry` (server ends the stream with UNAVAILABLE after sending an entry). Tests using them live in one test binary, run serially (`fail::FailScenario` + a file-level mutex), and require `--features failpoints`. A test-only `Slave::kill(self)` (cfg feature `failpoints`) aborts all tasks without graceful steps, emulating a process crash.

## Test harness (`tests/support/mod.rs`)

- `Cluster` helper: start a master on `127.0.0.1:0` with a shared `ManualClock`; start slaves pointing at any node's `serve_addr()`; `AllowAll` auth on plaintext by default.
- `wait_until(|| cond, 10s)` polling helper; `wait_converged(&[&dyn ReadApi+LogApi], seq)`.
- `dump(node) -> BTreeMap<String, Vec<(Vec<u8>, Vec<u8>, ValueMeta)>>` via the public read API (keys from `iter`, meta from `get_with_meta`) to compare nodes exactly.
- `RecordingListener` counting entries/bootstraps and asserting strictly increasing seqs.
- Edge-case dataset `edge_cases()`: `(b"", b"")`, `(b"\0", b"\0")`, `(b"\0k\0", b"\0\0\0")`, `(vec![0;64], vec![0;64])`, `(vec![0x5A; 1<<20], b"")`, `(b"big", vec![0xC3; (16<<20) - 4096])`.

## Plan 3: Verify

> **Superseded** by DESIGN.md §12 and §12.1 and PROTOCOL.md §9 as Plan 3 merged them: listings resume from the last key received (not `last key + \0`), `Mismatch::CfSpec` carries `CfVersion`s, and `replicas()` reports real acks (Plan 2). Kept for history.

- Server: `VerifyBegin` opens a session `{ snapshot: ReadSnapshot, mode, last_used }` in `HashMap<[u8;16], Session>` (max `max_verify_sessions` → BUSY; idle > `verify_session_timeout` → removed; later use → NOT_FOUND/SESSION_NOT_FOUND). `VerifyRange` processes requests in order on a blocking thread per message: HashRange → iterate `[lower, upper)` raw through the session snapshot (seek to `lower` or start; stop at first key `>= upper` under the CF comparator), digest records with payload by mode; ListRange → up to `limit` (≤1024) `KeyDigest { key, write_seq, payload_hash = blake3(payload) }`.
- Client (`slave/verifier.rs`): `VerifyBegin`; if identity differs → abort; request the applier to capture a snapshot at exactly `S` (the applier checks after each commit; if `last_seq == S` already, capture now; if `> S`, abort with `VerifyOutcome::Retry`); compare cf_specs; per CF build contiguous half-open ranges from local keys (`chunk_keys`/`chunk_bytes`); pipeline up to 16 HashRange; on mismatch bisect at the local median key until `<= min_chunk_keys` local keys, then ListRange (paginate with `lower = last key + \0`) and diff key-by-key against local `(write_seq, payload hash)`; build `VerifyReport`. `MismatchPolicy::Rebootstrap` sets `force_bootstrap`. `VerifyConfig.interval` schedules periodic runs; `Slave::verify_now(mode)`.
- Mismatch types: `Mismatch::CfSpec { local: Option<CfSpec>, upstream: Option<CfSpec> }`, `Mismatch::Key { cf, key, local: Option<Seq>, upstream: Option<Seq> }`, `Mismatch::Range { cf, lower, upper }` (listing truncated).
- Integrity tests (real): corrupt a value on disk (shut slave down, open its directory with raw `rocksdb` under the test, overwrite one envelope's value bytes keeping `write_seq`, reopen) → `Quick` passes, `Full` reports exactly that key; delete a key on disk → both modes report it with `local: None`; add an extra key on disk → reported `upstream: None`; a key that exists only upstream between two local chunk boundaries is found; edge-case dataset verifies clean in both modes; chain A→B→C verifies on each link; `MismatchPolicy::Rebootstrap` converges after corruption; session timeout and BUSY limits.
- Observability: `tracing` spans for handshake/bootstrap/verify/reaper with node_id/upstream/seq/epoch; `SlaveStatus.lag_ms`, `Master::replicas()`/`Slave::replicas()` returning `ReplicaStatus { client_id, remote_addr, acked_seq (= last sent), connected_since }` tracked by the server per Tail stream.
