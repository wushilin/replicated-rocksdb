# Hardening batch: auth, wire compression, tuning, group commit, HTTP status

Date: 2026-10-11. Six independent items (A–F). Each lands as its own commit series and PR,
in the order A → B → C → F → D. E runs alongside and its findings become follow-up fixes.

All items keep every invariant in CLAUDE.md. None changes `PROTOCOL_VERSION`, the
`replication.proto` messages, or the bytes of any `LogEntry`.

## A. `Authenticator::uses_tokens()` becomes required

**Problem.** `uses_tokens()` defaults to `false`, so a custom token authenticator that forgets
to override it runs on a plaintext listener without the `allow_plaintext_token` guard (fails
open).

**Change.**
- Remove the default body: `fn uses_tokens(&self) -> bool;`.
- Built-ins say it explicitly: `AllowAll` → false, `TokenAuthenticator` → true,
  `MtlsSubjectAllowList` → false, `AllowMethods` → delegates to `inner`. Any other
  implementation in the crate, tests, examples and playground is updated.
- Rustdoc: replace the "fails open" warning with what the method means and why it is
  required. DESIGN.md §14 snippet updated.

**Tests.** A `compile_fail` doctest: an `Authenticator` impl without `uses_tokens` does not
compile. Existing plaintext-guard tests stay green; add one that `AllowMethods` around a
`TokenAuthenticator` is refused on plaintext.

**Compatibility.** Breaking for custom authenticators (compile error). Acceptable pre-1.0.

## B. Optional zstd compression of replication traffic

**Mechanism.** gRPC message compression (`grpc-encoding`), via tonic's `zstd` feature.
Compression happens below protobuf: entries, `ops` bytes and CRCs are unchanged, and slaves
still forward entries verbatim.

**Config.**
- New `pub enum WireCompression { None, Zstd }` (default `None`), exported from the crate
  root.
- `ServeConfig.compression: WireCompression`: what the server compresses its responses with.
- `UpstreamConfig.compression: WireCompression`: what the slave compresses its requests with
  and asks the server for.
- Every server and every client **always accepts** zstd (`accept_compressed(Zstd)`), so
  mixed settings interoperate. A side sends compressed only when configured, and gRPC only
  compresses a response when the peer advertised it in `grpc-accept-encoding`.
- Applies to every RPC (Handshake, Tail, Bootstrap, Verify, Status), client and server.

**Protocol doc.** PROTOCOL.md gets a short section: message compression is optional and
negotiated by gRPC; implementations MUST accept `zstd` and `identity`, and MAY send `zstd`.
§14/§15 unchanged.

**Tests.** Integration test: master serving with `Zstd`, slave with `None`, and the reverse,
and both `Zstd`: bootstrap, tail and verify all succeed and data matches. A unit test that the
config default is `None`.

## C. More RocksDB tuning, still restricted

**Additions to `Tunable`** (performance only, never semantics):
- Block-based table: `set_block_size`, `set_bloom_filter(bits_per_key)`,
  `set_cache_index_and_filter_blocks(bool)`. These build `BlockBasedOptions` internally, also
  attaching the shared block cache when one is configured.
- Compaction: `set_compaction_style(CompactionStyle::{Level, Universal})` (crate enum),
  `set_level_zero_file_num_compaction_trigger`, `set_max_bytes_for_level_base`,
  `set_level_compaction_dynamic_level_bytes`, `set_bottommost_compression(Compression)`.
- IO: `set_bytes_per_sync`, `set_wal_bytes_per_sync`, `set_max_total_wal_size`.

**Additions to `RocksTuning`:**
- `with_block_cache(bytes)`: one LRU cache shared by every CF of the node, including
  internal CFs.
- `with_rate_limit(bytes_per_sec)`: DB-wide compaction and flush rate limiter.

**Read-only properties.** `property(&self, cf: Option<&str>, name: &str) -> Result<Option<String>>`
and `property_int(...) -> Result<Option<u64>>` on `Master`, `Slave` and `Node`. `cf = None`
means the default CF / DB-wide. `cf = Some(name)` resolves a *user* CF through the registry
(unknown CF → `Error::UnknownCf` as elsewhere). Internal CFs are not addressable.

**Still excluded.** Merge operator, compaction filter, native TTL, and **prefix extractor**:
with one set, RocksDB iterators may skip keys outside the seek prefix unless
`total_order_seek` is on, which would silently break bootstrap, verify and `prefix_iter`
(they assume total-order scans). Each excluded setter gets a `compile_fail` doctest like the
existing merge-operator one. The `Tunable` rustdoc says why.

**Tests.** Options are applied (open a store with each knob and read back via
`property`/`get_options` where RocksDB exposes it); property lookup on a user CF, on an unknown
CF, and DB-wide; a shared cache across two CFs.

## F. HTTP/JSON status endpoints on the replication port

**Goal.** `curl -H "authorization: Bearer $TOKEN" https://node:port/v1/status` shows the node's
state as JSON, with the same TLS and the same authenticator as replication.

**Listener.** Same port and TLS as replication. tonic's server accepts HTTP/1.1 next to gRPC
(`accept_http1(true)`) and serves an axum router for the paths below; gRPC requests are routed
as today. Opt-in: `ServeConfig.http_status: bool`, default `false`. If tonic 0.14 cannot serve
both on one listener cleanly, fall back to `ServeConfig.http_status: Option<u16>` (separate
port, same TLS and authenticator) and document why.

**Auth.** Every path is authenticated exactly like the `Status` RPC: bearer token from the
`authorization` header, mTLS peer identity when present, `Method::Status`, and the same
per-address failure limiter. 401 for `Unauthenticated`, 403 for `PermissionDenied`, 429 when
the limiter blocks. Tokens are never logged.

**Endpoints** (all `GET`, JSON unless noted):
- `/v1/status`: the same data as the `Status` RPC (`StatusResponse`), rendered as JSON with
  readable fields: UUIDs as canonical strings, CRCs as `0x`-prefixed hex, epoch history as an
  array of `{epoch, start_seq, nonce}`, replicas as an array. Built from the same code path as
  the RPC, so the two never disagree.
- `/v1/health`: `200 {"ok": true}` while the server runs (liveness).
- `/v1/ready`: `200` when `node_state == "ready"`, otherwise `503`; body
  `{"ready": bool, "node_state": "..."}`.
- `/metrics`: Prometheus text format: `rrdb_last_seq`, `rrdb_last_ts_ms`, `rrdb_role{role}`,
  `rrdb_node_ready`, `rrdb_epoch`, and for each replica `rrdb_replica_applied_seq{node_id}`
  and `rrdb_replica_in_sync{node_id}`.
- Anything else: 404. Non-GET: 405.

**Deps.** `serde` and `serde_json` move to normal dependencies; `axum` only if tonic does not
already expose what is needed.

**Tests.** Integration: master with `http_status` and a `TokenAuthenticator` over TLS:
`/v1/status` with the token → 200 and correct `last_seq`; without the token → 401; a wrong
token repeated → 429; `/v1/ready` on a halted slave → 503; `/metrics` parses. gRPC
replication still works on the same port. Docs: README section "Status over HTTP" with curl
examples; DESIGN.md §15 (Listener) updated.

## D. Group commit on slaves

**Goal.** With `sync_writes = true`, one fsync per group of entries instead of per entry.

**Change.** The slave applies consecutive entries from one received batch in one
`WriteBatch`: every op, every `__changelog[seq]`, and `__meta.last_seq` / `last_entry_crc`
set to the last entry of the group. One `spawn_blocking`, one `db.write`.

**A group ends (the entry is the last of its group) when:**
- the entry is a `CreateCf` or `DropCf` (CF entries always commit alone, as today, since they
  hold the registry lock);
- the entry's seq is a requested verify capture seq;
- the entry's seq reaches `repair_until`;
- the received batch ends, or a limit is hit (`SlaveConfig.max_group_entries`, default 256,
  and 4 MiB of ops);
- the applier is held for a verify (`wait_while_held`).

**Per-entry checks are unchanged** (CRC, epoch, exactly-once, gap) and run before an entry
joins a group. A failure ends the group before that entry: the earlier entries commit, then
the failure is handled exactly as today.

**Prepare must not depend on earlier entries of the same group.** The implementer verifies
that `Store::prepare` reads nothing that an earlier non-CF entry of the group could change; if
it does, the group carries that state forward or the item stops and reports back.

**After the group commits**, for each entry in order: `finish_repair_at`, `capture_after_commit`,
`on_entry`, publish the seq. Acks are unchanged (coalesced).

**Doc.** DESIGN.md §6: "One entry is one WriteBatch" becomes "An entry is never split;
consecutive entries may share one WriteBatch. Every snapshot is still a consistent state at
some seq." Same for CLAUDE.md's atomicity invariant wording if needed (meaning unchanged).

**Tests.** Existing apply, verify, repair and failpoint suites green. New: a 1 000-entry
burst with `sync_writes = true` commits in fewer batches than entries (counted through a test
hook); a verify capture in the middle of a group lands exactly at `S`; a CF entry in a burst
commits alone; a corrupt entry mid-batch commits the entries before it, then halts.

## E. Security review (read-only)

A full review of the crate: authentication and authorization on every RPC (and later the HTTP
endpoints), consent paths (repoint, rejoin, clear_halt, policies), resource limits per RPC and
memory bounds, TLS client and server verification, token handling and logging, and the
playground's HTTP surface. Output: findings ranked by severity with file:line, each verified.
Real issues become follow-up fixes, planned separately.
