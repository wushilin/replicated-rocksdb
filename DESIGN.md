# replicated-rocksdb — Design

Status: agreed design, pre-implementation. Revised 2026-10-09.
Wire protocol: [PROTOCOL.md](PROTOCOL.md) is the normative specification. This document covers storage, the write and apply paths, and the crate API. Where the two overlap, PROTOCOL.md wins.

## 1. Goals and non-goals

**Goals**
- Embeddable Rust crate. Each process opens one local instance that is the read/write gate to its RocksDB.
- Single writable **Master**; any number of read-only **Slaves** that replicate from it, directly or via other slaves (chaining).
- Replicas stay **identical over time**: same logical data and same physical values (including metadata) at the same sequence number.
- Divergence is **detected, never silent**: a node never applies an entry whose history disagrees with its own.
- Bounded changelog (time-based retention) on every node.
- Built-in integrity verification between a slave and its upstream.

**Non-goals (v1)**
- Strong / synchronous consistency. Replication is **async**, designed for **low update frequency**.
- Automatic failover / leader election. Promotion of a slave to master is **manual** (§13.1); the operator decides when the old master is gone.
- Relative operations of any kind (`merge`, counters, appends). Not supported, not configurable.
- Network query API for application clients. The network port is replication-only; applications read in-process via `ReadApi`.

## 2. Topology and roles

```
Master(A) ──► Slave(B) ──► Slave(C)
                 └──────► Slave(D)
```

| Role | Writes | Serves replication | Upstream |
|---|---|---|---|
| Master | yes (only via the gated writer) | always | none |
| Slave | never (no write methods exist) | optional (`serve: Some(..)`) | exactly one (master or slave) |

- The role is persisted in `__meta`. Opening a slave directory as `Master` (or vice versa) is refused.
- Every node has an `identifier` that **must be unique within the topology** (1–128 chars of `[A-Za-z0-9._-]`). Loop detection, bootstrap leases and status reporting all rely on it.

## 3. Storage layout

### 3.1 Column families

| Physical CF | Purpose | Replicated | Bootstrapped | Verified |
|---|---|---|---|---|
| `__meta` | node state (see 3.2) | no (each node maintains its own) | no | no |
| `__changelog` | replay buffer of `LogEntry` keyed by seq | entries are forwarded, CF itself is node-local (retention differs) | no | no |
| `__ttl_index` | expiry index, **master only**, derived | no | no | no |
| `u:{name}:{create_seq}` | user CF `name` | yes | yes | yes |

- User CF names must be non-empty UTF-8 and must not start with `__`.
- The physical name includes `create_seq` (the seq of the `CreateCf` entry). It is deterministic across nodes and lets a CF be dropped and re-created with the same name while the old physical CF is still pending physical deletion.
- `__ttl_index` is a derived structure that only the master reads. Key: `BE64(expire_at_ms) ‖ BE64(create_seq) ‖ BE16(len(name)) ‖ name ‖ key`, empty value. Including `create_seq` stops a dropped and re-created CF from matching old index entries. Slaves never reap, so they do not need it. It can be rebuilt from a full scan of the envelopes, for example on a future promotion.

### 3.2 `__meta` keys

| Key | Nodes | Meaning |
|---|---|---|
| `role` | all | `master` / `slave` |
| `protocol_version` | all | wire protocol version this DB was written with |
| `envelope_version` | all | value envelope format version |
| `origin_identity` | all | `DbIdentity { db_uuid, epoch }` of the origin master |
| `last_seq` | all | master: last committed seq. Slave: last applied seq. **Always written in the same WriteBatch as the entry.** |
| `last_entry_crc` | all | `crc32c` of the entry at `last_seq`, if known (it is unknown after a bootstrap from a node that had already trimmed `S`). Used for divergence checks (PROTOCOL.md §6.3). |
| `last_ts` | master | last `ts_ms` issued (keeps timestamps monotonic) |
| `epoch_history` | all | `Vec<EpochStart { epoch, start_seq }>`, sorted by epoch; first is `{1, 1}` (PROTOCOL.md §5.5). Written atomically with `origin_identity`. |
| `max_entry_bytes` | slave | the master's value, as received in the handshake; forwarded downstream |
| `node_state` | slave | `Ready` / `Bootstrapping { target_seq }` / `Halted(reason)`, persisted (see §16.3) |
| `cf_specs` | all | live user CFs: `Vec<CfSpec>` plus `create_seq`, sorted by name |
| `pending_cf_drops` | all | physical CFs logically dropped, awaiting physical drop |
| `cf_create_intent` | all | the `CfEntry` being created; written with fsync before the physical CF is created, cleared in the `CreateCf` commit batch |

`last_seq` is the source of truth for position. The changelog can be completely empty after retention, for example on a quiet master.

### 3.3 Changelog

- Key: `seq` as u64 big-endian. Value: the protobuf-encoded `LogEntry` (PROTOCOL.md §5.1).
- `first_retained_seq` = the first key in `__changelog`, or `last_seq + 1` if the changelog is empty.
- The master writes entries through the gated writer. Slaves store each received entry in the same batch as applying it.
- **The `ops` field is opaque bytes, stored and forwarded verbatim; it is never re-encoded.** Protobuf encoding is not canonical, so re-encoding could change the bytes and break the CRC downstream. A slave decodes `ops` only to apply them.

### 3.4 Value envelope (every user value)

```
┌────────┬───────┬───────────────┬──────────────────────────┬────────────┐
│ ver u8 │ flags │ write_seq u64 │ expire_at_ms u64         │ user value │
│ (=1)   │ u8    │ BE            │ only if flags & TTL      │            │
└────────┴───────┴───────────────┴──────────────────────────┴────────────┘
header: 10 bytes, 18 with TTL. flags bit 0 = HAS_TTL; bits 1–7 reserved, must be 0.
```

- `write_seq`: the seq of the entry that wrote this value. It is deterministic on every node.
- `expire_at_ms`: absolute expiry, computed on the master.
- Keys are stored raw, so ordering, prefix iteration and comparators are unaffected.
- `ReadApi` strips the header. Bootstrap and verification operate on the full envelope.
- An unknown `ver`, a reserved flag bit set, or a value shorter than its header is reported as `Error::Corrupt`.
- There is no per-record checksum or timestamp. At-rest corruption is covered by RocksDB block checksums, transport by the `LogEntry` CRC, and divergence by the integrity check (§12).
- Consequence: the DB directory is not readable with plain RocksDB tools. Access goes through the crate.

## 4. Operations and log entries

**Invariant: every op is absolute.** Each op sets state to a given value and never depends on prior state. Replaying in order is always safe.

```rust
pub type Seq = u64;

pub enum Op {
    Put         { cf: String, key: Vec<u8>, value: Vec<u8>, expire_at_ms: Option<u64> },
    Delete      { cf: String, key: Vec<u8> },
    DeleteRange { cf: String, start: Vec<u8>, end: Vec<u8> },   // [start, end)
    CreateCf    { spec: CfSpec },
    DropCf      { name: String },
}

pub struct LogEntry {
    pub seq: Seq,
    pub epoch: u64,
    pub ts_ms: u64,      // master commit time, monotonic
    pub ops: Bytes,      // encoded OpList, exactly as the master produced it
    pub crc32c: u32,     // CRC-32C(BE64(seq) ‖ BE64(epoch) ‖ BE64(ts_ms) ‖ ops)
}
impl LogEntry { pub fn decode_ops(&self) -> Result<Vec<Op>>; }
```

The CRC covers the header fields as well as the ops, so an entry whose seq or epoch was altered in transit is also caught. It is checked on receive and when an entry is read for forwarding. The exact encoding is in PROTOCOL.md §5.1–5.2, with test vectors in §15.

Rules:
- An entry has at least one op.
- `CreateCf` and `DropCf` are always the **only op** in their entry. A data batch never contains CF ops.
- A data entry may touch several CFs.
- `Put` carries the user value. Each node builds the envelope (`write_seq = entry.seq`, plus `expire_at_ms`) when it applies the op.
- There is no merge. `RocksTuning`'s callback receives a restricted `Tunable`, not raw `rocksdb::Options`, so a merge operator, compaction filter or native TTL cannot be set at all.

## 5. Master write path

```
lock(writer)
  validate: batch non-empty; every CF exists; DeleteRange start < end;
            encoded entry size <= max_entry_bytes
  seq = last_seq + 1
  ts  = max(now_ms, last_ts)
  ops = encode(OpList); crc = crc32c(seq, epoch, ts, ops)
  wb  = WriteBatch:
          user ops with envelopes (write_seq = seq)
          __ttl_index[expire_at|cf|key] for TTL puts        (master-local)
          __changelog[seq] = LogEntry{seq, epoch, ts, ops, crc}
          __meta.last_seq = seq, last_entry_crc = crc, last_ts = ts
  db.write(wb, sync = true)
  publish seq to tail subscribers (tokio::watch)
unlock
```

- A failed validation logs nothing and consumes no seq. Errors: `EmptyBatch`, `NoSuchCf`, `InvalidRange`, `EntryTooLarge`.
- **The master always fsyncs and this is not configurable.** If the master lost acknowledged entries that slaves had already received, it would reuse those seqs for different writes, and the replicas would diverge. The handshake rejects a slave that is ahead of the master (`DIVERGED`) as a backstop, but fsync is the primary guarantee.
- If `db.write` fails, the in-memory `last_seq` is not advanced and the master reports the error. An fsync failure is treated as fatal: the master stops accepting writes until it is reopened, because the durability of the batch is unknown.
- Writes are synchronous (`fn`, not `async fn`). The write rate is low, and callers in async code can use `spawn_blocking`.

### 5.1 Read-modify-write is the application's responsibility

The crate provides **no** read-modify-write primitive: no `update`, no compare-and-set, no merge. The writer lock serialises individual write calls only. **Between your `get` and your `put`, another writer can change the key, and that change will be lost.**

If an application derives a new value from an old one (counters, append-to-list, "insert if absent", and so on), it must hold **its own lock** around the whole read → compute → write sequence:

```rust
// Application code, not crate code.
let _g = my_locks.lock_for(cf, key);            // app-level lock: global, per key, or striped
let old = master.get(cf, key)?;
let new = compute(old);
master.put(cf, key, &new)?;                     // logged as an absolute Put
// drop(_g)
```

Rules for that lock:
- **Every** writer that modifies those keys must take the same lock. One code path that skips it brings the race back.
- This is sufficient because there is exactly one master process and all writes are in-process. The lock does not need to be distributed.
- Hold the lock only for the read, the compute and the write. No I/O to other systems while holding it.
- The value written is still the absolute result, so replication and the absolute-ops invariant are unaffected.

**Interaction with TTL reaping (safe; no app action needed).** The reaper does not take the application's lock. Suppose a key expires between your `get` (which returns `None`, because expired values are hidden) and your `put`. The reaper deletes a key only if its current `expire_at` still matches the index entry, and your `put` has replaced that value. So the reaper skips it, and your new value is not deleted.

The same rules are in the rustdoc for `Master::put`, `Master::write` and `Master::get`, and in the crate-level docs.

## 6. Slave apply path

```
for each LogEntry received (in order):
  verify crc                                          → Halted(Corrupt)
  verify entry.epoch == epoch_for(entry.seq)            → end stream, handshake again
  if entry.seq <= last_seq: skip                       (exactly-once)
  if entry.seq != last_seq + 1:                        → GapPolicy
  ops = decode + validate (PROTOCOL.md §5.2)           → Halted(Corrupt / SpecMismatch)
  wb = WriteBatch:
         ops with envelopes (write_seq = entry.seq)
         __changelog[seq] = entry (unchanged)
         __meta.last_seq = seq, last_entry_crc = entry.crc32c
  db.write(wb)          (sync = false by default)
  publish seq to downstream tail subscribers
  if a verify capture is requested at this seq: take the snapshot now (§12)
  listener.on_entry(&entry)
```

- **One entry is one WriteBatch, and an entry is never split.** That makes every RocksDB snapshot on a slave a consistent state at some seq.
- Writes are not synced on slaves. A slave that crashes resumes from its durable `last_seq` and fetches the rest again from upstream.
- **A downstream replica can be ahead of its upstream slave.** If upstream B crashes unsynced after forwarding to C, C's `last_seq` can exceed B's. On handshake B replies `RESUME`, and the Tail stream sends only heartbeats until B passes C's seq. The content is identical because seqs come from the same origin log. This is allowed only when the upstream is a slave. The master rejects it (§13).
- **Divergence check on connect.** The slave sends `last_entry_crc` in the handshake and `prev_crc32c` in `Tail`. If the upstream still holds that entry and its CRC differs, the upstream answers `DIVERGED` and the slave halts instead of applying a different history.

## 7. Column family lifecycle

`CreateCf` and `DropCf` are not part of a RocksDB `WriteBatch`, so they are made crash-safe this way:

- **CreateCf (seq N):** write `__meta.cf_create_intent = (name, comparator, N)` with fsync, create the physical CF `u:{name}:{N}` if it doesn't exist (idempotent), then write one batch: add the spec to `__meta.cf_specs`, clear the intent, write `__changelog[N]`, and set `last_seq = N`. On open, an unregistered CF named by the intent is dropped and the intent cleared; replay (slave) or a retried write (master) recreates it. The intent exists because RocksDB can only reopen a CF with its comparator, and an unregistered CF's comparator would otherwise be unknown. Opening is two-phase: `__meta` is read through a read-only open first, so every user CF is reopened with its registered comparator.
- **DropCf:** logical. One batch removes the spec from `cf_specs`, adds the physical CF to `pending_cf_drops`, and writes the changelog entry and `last_seq`.
- **Physical drop is deferred while anything still references the CF.** Every RocksDB snapshot handle the crate gives out (`ReadSnapshot`, iterators, bootstrap and verify sessions) holds a reference-counted handle to each physical CF in its `cf_specs`. A background task drops a pending CF once its count reaches zero. A `ReadSnapshot` taken before the drop therefore still sees the CF and its data, through `list_cfs` too, because `cf_specs` is read through the snapshot.
- On the master, creating a CF that already exists returns `AlreadyExists` and logs nothing.
- Bootstrap and verify iterate over the `cf_specs` read **through their snapshot**, never the live CF list.

### 7.1 Comparators

- `CfSpec.comparator` is a **name**. Each node registers named comparators through `RocksTuning::comparators(&mut ComparatorRegistry)`. Without a comparator, keys are ordered bytewise.
- The master rejects `create_cf` with an unregistered name (`UnknownComparator`). A slave that receives one halts with `SpecMismatch` without applying it. It resumes after the comparator is registered and the halt is cleared.
- A comparator name is a permanent contract: its ordering must never change. To change an ordering, use a new name. Bootstrap order, `DeleteRange` and verify ranges all depend on every node ordering keys the same way.

## 8. TTL

RocksDB native TTL (`DBWithTTL`, or compaction filters) expires data during compaction, which runs at different times on each node. It is **not used**.

- `put_with_ttl(cf, k, v, ttl)`: the master computes `expire_at_ms = ts + ttl` (with `ts` the entry's `ts_ms`) and logs it as an absolute value. TTL is set **per write**, on any CF.
- **Reads on every node hide values with `expire_at_ms <= now`.** Clock skew between nodes only shifts this visibility window.
- **What `now` means:** `get` and `multi_get` read the clock once per call. An iterator reads it once, when it is created. A `ReadSnapshot` reads it once, when it is taken, so repeated reads through the same snapshot always agree.
- **Reaper (master only):** every `reaper.interval`, scans `__ttl_index` up to `now`. For each index entry, under the writer lock:
  - if the current envelope's `expire_at_ms` still equals the index's value, it adds a `Delete` to the pending entry and removes the index entry in the same batch;
  - otherwise the entry is stale (the key was overwritten or range-deleted), and only the index entry is removed. That removal is master-local and not logged.
- The reaper writes **at most `max_deletes_per_entry` deletes per `LogEntry`** (default 1 000). It releases the writer lock between entries, so application writes are not blocked for long. One round writes at most `max_deletes_per_round` deletes (default 100 000). The rest wait for the next round.
- A plain `put` on a key with a TTL clears the TTL.
- Slaves never reap. Expiry deletes reach them through the log.

## 9. Changelog retention

Runs on **every node**, master and slaves.

```rust
pub struct Retention {
    pub max_age: Option<Duration>,     // default: 14 days
    pub max_entries: Option<u64>,      // default: None
    pub check_interval: Duration,      // default: 10 min
}
```

- Age is measured from `LogEntry.ts_ms`, the master's commit time, against the local clock. All nodes therefore trim at about the same point regardless of how many hops away they are.
- `ts_ms` is monotonic, so trimming scans from the front up to the first entry that hasn't expired, then calls `delete_range`. A periodic `compact_range` on `__changelog` clears the tombstones.
- **Retention never deletes past a guard.** While a bootstrap at seq `S` is held (see §10), every entry with seq `> S` is kept, whatever its age.
- Tail streams do **not** hold back retention. A downstream replica that falls behind `first_retained_seq` gets `LOG_GAP` and follows its `GapPolicy`.

## 10. Bootstrap

Used when a slave has no data, is behind the retention window, or (by policy) after an identity change. Normative steps and messages: PROTOCOL.md §8.

**Serving side (master or slave):**
1. Refuse (`NOT_READY`) if this node is itself `Bootstrapping`, and (`BUSY`) if it is already serving `max_concurrent_bootstraps`.
2. `snap = db.snapshot()`. No write lock is taken.
3. Read through `snap`: `S = __meta.last_seq`, `last_entry_crc`, `origin_identity`, `cf_specs`.
4. Register a **retention guard** for `S` and issue a lease (`lease_id`, `lease_ttl`).
5. Stream `Header`, then `Records` for each CF in `cf_specs` order with **raw envelopes** (including values that are expired but not yet reaped), then `Footer { record_count, digest }`.
6. Keep the guard until the client's `Tail` that presents the lease has read the entries after `S` (or ends), or the lease expires (default 15 min), or the stream fails before the footer. Without this, retention could remove `S + 1` between the end of the bootstrap and the tail's first read, and the slave would loop through bootstraps.

**Receiving slave:**
1. Persist `node_state = Bootstrapping { target_seq }`. Reads return `NotReady`. A crash in this state restarts the bootstrap from scratch.
2. **End all outgoing Tail, Bootstrap and Verify streams** it serves (`UPSTREAM_RESET`), then drop every user CF (including `pending_cf_drops`, once their references are released) and clear `__changelog`.
3. Create the physical CFs listed in `cf_specs` and apply the chunks in batches, checking key order and each envelope.
4. Check the footer (`record_count` and digest, PROTOCOL.md §5.4). On a mismatch, start again.
5. Write the final batch: `origin_identity`, `cf_specs`, `last_seq = S`, `last_entry_crc`, `max_entry_bytes`, `node_state = Ready`.
6. Tail from `S + 1`, presenting the lease.

Since `S`, `cf_specs` and the data all come from the same snapshot, a bootstrap from any node in a chain gives identical data, and an identical digest, for the same `S`.

## 11. Chained replication

- The replication server is shared code. It runs over a `LogSource` trait, implemented by both master and slave:

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

- A slave forwards entries **unchanged**, byte for byte. `seq` means the same thing on every hop.
- Identity always refers to the **origin master**.
- **What happens after the handshake** follows the decision table in PROTOCOL.md §6.3, which the server evaluates in order: version checks, loop detection, readiness, identity, divergence, retention gap, resume. `Tail`, `Bootstrap` and `VerifyBegin` repeat the checks they depend on, so a server whose state changed after the handshake cannot serve the wrong data.
- **Loop detection:** the handshake response includes `server_id` and `upstream_chain`. The server rejects with `LOOP_DETECTED` if the client's `identifier` appears in either.
- Heartbeats carry `upstream_last_seq` and `origin_last_seq`, so every node can report its true lag behind the origin.
- If the upstream is `Halted`, it still serves its data, which stays consistent but stops advancing. If the upstream bootstraps again, it ends its downstream streams with `UPSTREAM_RESET`, and they reconnect and handshake again.

## 12. Integrity verification (slave-initiated)

Normative messages: PROTOCOL.md §9.

**Comparing at the same seq**
1. Slave → `VerifyBegin`. The upstream opens a snapshot, reads `S`, identity and `cf_specs` through it, and returns them with a `session_id`.
2. The slave's applier captures a snapshot **exactly at `S`**: it applies up to `S` and snapshots before `S + 1`. Normally the slave is at or behind `S`, since it can only have received what the upstream had. The exception is the case in §6, where it can be ahead. If it is already past `S`, the session is abandoned and retried later.
3. First compare `cf_specs`. Then, for each CF present on both sides, compare ranges.

**Ranges**
- Ranges are **half-open and contiguous** over the whole key space of a CF: `(-∞, k1)`, `[k1, k2)`, …, `[kn, +∞)`. The slave picks the boundaries from its own keys (`chunk_keys` keys or `chunk_bytes` bytes). Because there are no gaps between ranges, a key that exists only on the upstream is always inside some range.
- For each range the slave sends `(count, hash)`. The upstream hashes the same range of its snapshot and replies with whether they match, plus its own values.
- Hash: BLAKE3 over length-prefixed `(key, payload)` records in key order (PROTOCOL.md §5.4).
  - `VerifyMode::Quick`: payload = envelope header (`ver`, `flags`, `write_seq`, `expire_at`). Detects missing, extra or stale writes without reading large values.
  - `VerifyMode::Full`: payload = full envelope. Also detects value corruption.
- On a mismatch the slave splits the range at its median key, repeating down to `min_chunk_keys`. Then it asks the upstream to list that range (`ListRange`: key, `write_seq`, payload hash) and diffs it key by key. Listing also finds keys that exist only on the upstream.
- Up to 16 range requests are pipelined. The slave throttles itself with `max_bytes_per_sec`. The upstream limits concurrent sessions and closes sessions idle for longer than `session_timeout`.

**Result**
- `VerifyReport { seq, mode, cfs_checked, keys_checked, mismatches: Vec<Mismatch>, duration }`, where `Mismatch` is `CfSpec { .. }` or `Key { cf, key, local: Option<write_seq>, upstream: Option<write_seq> }`, or `Range { cf, lower, upper }` if listing was cut short.
- `MismatchPolicy::Report` (default) or `MismatchPolicy::Rebootstrap`. There is no automatic range repair in v1.
- In a chain, each slave verifies against its own upstream. If every link matches, the whole chain matches.

## 13. Identity, epoch, versioning

- `DbIdentity { db_uuid: Uuid, epoch: u64 }` is created when a master DB is first opened, with `epoch = 1` and epoch history `[{1, 1}]`.
- **Epoch history** (PROTOCOL.md §5.5): one `{epoch, start_seq}` record per epoch. Every entry satisfies `entry.epoch == epoch_for(entry.seq)`. Every node persists the history with its identity and forwards it in the handshake and the bootstrap header.
- **`Master::bump_epoch()`** (admin action): under the writer lock, `epoch += 1`, append `{epoch, last_seq + 1}` to the history, and persist both in one synced batch. **Seq numbering continues.** Then end every replication stream with `UPSTREAM_RESET`.
- **Epoch continuity.** A slave of an older epoch whose last entry lies before the fork point, and whose last entry's CRC matches the upstream's, continues without a bootstrap (`EPOCH_ADVANCED`). It persists the new identity and history before tailing. A slave with entries at or past the fork point is `DIVERGED`. Rules: PROTOCOL.md §6.3 rules 6–10.
- **Policies on the slave:**
  - `on_identity_change: IdentityPolicy` — `Refuse` (default) halts with `IdentityChanged`; `Rebootstrap` bootstraps. Applies when `db_uuid` differs, or when continuity across an epoch change cannot be proven.
  - `on_divergence: DivergencePolicy` — `Halt` (default) halts with `Diverged`; `Rebootstrap` bootstraps. Applies to `DIVERGED`.
  - `STALE_EPOCH` (the upstream is behind the slave's epoch) is retried with backoff and reported in `SlaveStatus.last_error`. An upstream replica that has not yet learned the new epoch catches up; a demoted master never does, and the operator must re-point the slave.
- **Restoring a master from a backup** must change its epoch before any replica connects. Open it with `MasterConfig { bump_epoch_on_open: true, .. }`. That bumps the epoch before the replication port opens. Slaves at or behind the restored seq with matching entries continue; slaves past it get `DIVERGED`. If the bump is forgotten, the backstop still applies: a slave ahead of the master, or one whose last entry has a different CRC, gets `DIVERGED` instead of silently diverging.
- **`PROTOCOL_VERSION: u32` must match exactly** in the handshake, and on every RPC through the `x-rrdb-protocol-version` metadata. The crate version is not compared. Upgrade procedure: stop all nodes, upgrade, start. `envelope_version` must also match exactly.

### 13.1 Promotion

`Slave::promote(self, cfg: MasterConfig) -> Result<Master>` turns a slave into the master. It is an operator action, and **the operator must stop or fence the old master first**: the crate cannot detect two live masters, only the divergence they cause.

1. Refuse if the slave is `Bootstrapping`, or halted with `Corrupt` or `SpecMismatch`. Its data must be a consistent state at some seq.
2. Stop replication: disconnect upstream and end every downstream stream with `UPSTREAM_RESET`. Shut down the slave and close its DB.
3. Reopen the directory offline and rebuild `__ttl_index` from a full scan of the envelopes: clear it, then add an entry for every value with `expire_at`. The index is derived, so a crash here leaves a valid slave.
4. In one synced batch: `role = master`; `epoch' = 1 + max epoch in history`; append `{epoch', last_seq + 1}` to the history; set `origin_identity = (db_uuid, epoch')`; set `last_ts = max(ts_ms of the entry at last_seq, if retained)`, so the new master's timestamps never go backwards; clear `node_state`.
5. Open with `Master::open(cfg)`. `cfg.path` must be the slave's path. Writes start at `last_seq + 1`. The master serves replication as usual.

After promotion:
- Former siblings and downstream slaves re-pointed at the new master continue if they are at or behind its `last_seq` with matching entries. Those that received entries it never saw are `DIVERGED` and follow their divergence policy.
- The old master can never rejoin as master. Replicas of the new epoch refuse to follow it (`STALE_EPOCH`, retried and reported). To reuse the machine, wipe its directory and start it as a new slave, which bootstraps.
## 14. Auth and TLS

```rust
pub enum Method { Handshake, Tail, Bootstrap, Verify }

pub struct PeerInfo {
    pub peer_id: String,              // mTLS certificate subject CommonName if present, else x-rrdb-node-id
    pub peer_id_verified: bool,       // true only when taken from a verified client certificate
    pub remote_addr: SocketAddr,
    pub client_cert: Option<Vec<u8>>,
}

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

Built-in implementations:
- `TokenAuthenticator::new(Vec<secret>)` and `TokenCredentials::new(secret)`. The token is sent as `authorization: Bearer <token>` on **every** RPC (via a tonic interceptor), with no session state. Comparison is constant-time (`subtle`). Multiple secrets are accepted so they can be rotated. Tokens are never logged.
- `AllowAll` (for tests) and `MtlsSubjectAllowList` (verified client certificate whose subject CommonName is listed, per method).
- `AllowMethods::new(inner, &[Method])` restricts any authenticator to some methods (`PERMISSION_DENIED` otherwise); `NoCredentials` sends no token.
- **Token auth without TLS is refused** unless `allow_plaintext_token: true` is set.
- `method` lets deployments allow `Bootstrap` and `Verify` only to some peers. Those calls expose the whole database.
- The server limits the rate of failed authentications per remote address (`ServeLimits::max_auth_failures_per_minute`, default 10; then `UNAUTHENTICATED` without checking the token).

## 15. Listener

```rust
pub trait ReplicationListener: Send + Sync + 'static {
    fn on_entry(&self, entry: &LogEntry) {}
    fn on_bootstrap_begin(&self, snapshot_seq: Seq) {}
    fn on_bootstrap_complete(&self, snapshot_seq: Seq) {}  // state replaced wholesale; rebuild derived state
    fn on_state_change(&self, state: &SlaveState) {}
    fn on_verify_complete(&self, report: &VerifyReport) {}
}
```

Listeners exist on slaves only. On the master, the application already sees every write it makes. To observe the log there, use `LogApi::entries_since` or `LogApi::subscribe`.

Guarantees:
- Strictly increasing `seq`, with no duplicates within a process.
- Called **after commit**, so reads already reflect the entry.
- Called inline in the apply loop. A slow listener slows replication down (back-pressure) and doesn't lose events. Heavy work should be handed off to a channel. A listener must not block on the slave's own replication progress (for example `wait_for_seq`), because that would deadlock the apply loop.
- **At-most-once across crashes.** For at-least-once delivery, keep your own cursor and use `entries_since(seq)`. It returns `Err(Gap)` if the cursor is older than the retention window, in which case resync with a full read.
- No per-record events during bootstrap; only begin and complete.
- A panic in a listener is caught and logged, and the entry stays committed. The listener is not called again until the process restarts, and `status()` reports this.

## 16. Public API

```rust
pub struct CfSpec { pub name: String, pub comparator: Option<String> }

pub enum IterMode<'a> { Start, End, From(&'a [u8], Direction) }

pub trait ReadApi {
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>>;
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>>; // write_seq, expire_at
    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>>;
    fn iter(&self, cf: &str, mode: IterMode) -> Result<DbIter<'_>>;
    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>>;
    fn list_cfs(&self) -> Vec<CfSpec>;
}

pub trait LogApi {
    fn last_seq(&self) -> Seq;
    fn first_retained_seq(&self) -> Seq;
    fn entries_since(&self, seq: Seq) -> Result<EntryIter<'_>>;   // Err(Gap{..}) when trimmed
    fn subscribe(&self) -> watch::Receiver<Seq>;
}

pub struct ReadSnapshot { .. }       // impl ReadApi; fn seq(&self) -> Seq; `now` fixed at creation;
                                     // owns an Arc of the DB, so it can be moved into tasks

// Shared
pub struct ServeConfig {
    pub bind: IpAddr,
    pub port: u16,
    pub tls: Option<TlsServerConfig>,
    pub authenticator: Arc<dyn Authenticator>,
    pub allow_plaintext_token: bool,
    pub limits: ServeLimits,   // { max_concurrent_bootstraps: 2, max_verify_sessions: 4,
                               //   bootstrap_chunk_bytes: 1 MiB (at most 16 MiB),
                               //   bootstrap_lease_ttl: 15 min, verify_session_timeout: 5 min,
                               //   max_auth_failures_per_minute: 10, max_tail_batch_bytes: 4 MiB }
}

// ---------------- Master ----------------
pub struct MasterConfig {
    pub identifier: String,
    pub path: PathBuf,
    pub serve: Option<ServeConfig>,     // None = embedded, non-replicating master
    pub retention: Retention,
    pub reaper: ReaperConfig,           // { interval: 30 s, max_deletes_per_entry: 1 000,
                                        //   max_deletes_per_round: 100 000 }
    pub max_entry_bytes: u64,           // default 16 MiB
    pub heartbeat: Duration,            // default 5 s
    pub bump_epoch_on_open: bool,       // set when opening a restored backup (§13)
    pub rocksdb: RocksTuning,           // comparator registry + callback over a restricted
                                        // `Tunable` (no merge/filter/TTL setters exist)
}

impl Master {
    pub async fn open(cfg: MasterConfig) -> Result<Self>;
    pub fn identity(&self) -> DbIdentity;
    pub fn serve_addr(&self) -> Option<SocketAddr>;     // bound replication address (port 0 picks one)

    pub fn put(&self, cf: &str, k: &[u8], v: &[u8]) -> Result<Seq>;
    pub fn put_with_ttl(&self, cf: &str, k: &[u8], v: &[u8], ttl: Duration) -> Result<Seq>;
    pub fn delete(&self, cf: &str, k: &[u8]) -> Result<Seq>;
    pub fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Seq>;
    pub fn write(&self, batch: Batch) -> Result<Seq>;
    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq>;
    pub fn drop_cf(&self, name: &str) -> Result<Seq>;

    pub fn snapshot(&self) -> Result<ReadSnapshot>;
    pub fn replicas(&self) -> Vec<ReplicaStatus>;      // direct downstream only
    pub fn bump_epoch(&self) -> Result<DbIdentity>;
    pub fn epoch_history(&self) -> Vec<EpochStart>;
    pub fn reap_expired(&self) -> Result<ReapStats>;      // one reaper round now
    pub async fn shutdown(self) -> Result<()>;
}
impl ReadApi for Master {}  impl LogApi for Master {}

pub struct Batch { .. }   // put / put_with_ttl / delete / delete_range → one LogEntry;
                          // may span CFs; must be non-empty

// ---------------- Slave ----------------
pub struct UpstreamConfig {
    pub server: String,
    pub port: u16,
    pub tls: Option<TlsClientConfig>,
    pub credentials: Arc<dyn CredentialsProvider>,
}

pub struct SlaveConfig {
    pub identifier: String,
    pub path: PathBuf,
    pub upstream: UpstreamConfig,
    pub serve: Option<ServeConfig>,          // Some = also serves downstream replicas
    pub retention: Retention,
    pub verify: VerifyConfig,                // { interval: Option<Duration>, mode, chunk_keys, chunk_bytes,
                                             //   min_chunk_keys, max_bytes_per_sec, on_mismatch }
    pub listener: Option<Arc<dyn ReplicationListener>>,
    pub on_identity_change: IdentityPolicy,  // Refuse (default) | Rebootstrap
    pub on_divergence: DivergencePolicy,     // Halt (default) | Rebootstrap
    pub on_log_gap: GapPolicy,               // Rebootstrap (default) | Halt
    pub reconnect: Backoff,                  // default 100 ms → 30 s, ×2, full jitter
    pub fetch: FetchConfig,                  // { min_batch_bytes: 0, max_wait: 100 ms, max_batch_bytes: 1 MiB }
                                             // 0 = real-time push; larger = batch like Kafka fetch.min.bytes
    pub rocksdb: RocksTuning,
}

pub enum SlaveState {
    Connecting,
    Bootstrapping { target_seq: Seq, records: u64 },
    Streaming,
    Halted(HaltReason),
}

pub enum HaltReason {
    // Data-related: persisted, survive restart, cleared only by clear_halt.
    IdentityChanged, Diverged, LogGap, Corrupt, SpecMismatch,
    // Configuration-related: not persisted, re-evaluated on restart.
    ProtocolMismatch, EnvelopeMismatch, AuthRejected, LoopDetected,
}

pub enum HaltAction { Retry, Rebootstrap }

pub struct SlaveStatus {
    pub state: SlaveState,
    pub origin_identity: Option<DbIdentity>,
    pub applied_seq: Seq,
    pub first_retained_seq: Seq,
    pub upstream_last_seq: Seq,
    pub origin_last_seq: Seq,
    pub lag_ms: Option<u64>,                 // now − ts_ms of the last applied entry, while behind
    pub upstream_chain: Vec<String>,
    pub last_contact: Option<SystemTime>,
    pub last_verify: Option<VerifyReport>,
    pub last_error: Option<String>,          // last connection/handshake problem, e.g. STALE_EPOCH
}

impl Slave {
    pub async fn open(cfg: SlaveConfig) -> Result<Self>;   // serves local data immediately unless Bootstrapping
    pub fn status(&self) -> SlaveStatus;
    pub fn applied_seq(&self) -> Seq;
    pub async fn wait_for_seq(&self, seq: Seq, timeout: Duration) -> Result<()>;
    pub fn watch_state(&self) -> watch::Receiver<SlaveState>;
    pub async fn verify_now(&self, mode: VerifyMode) -> Result<VerifyReport>;
    pub async fn clear_halt(&self, action: HaltAction) -> Result<()>;   // operator action
    pub async fn promote(self, cfg: MasterConfig) -> Result<Master>;    // operator action (§13.1)
    pub fn snapshot(&self) -> Result<ReadSnapshot>;
    pub fn replicas(&self) -> Vec<ReplicaStatus>;
    pub async fn shutdown(self) -> Result<()>;
}
impl ReadApi for Slave {}  impl LogApi for Slave {}
// A Slave has no write methods. Writing to it is a compile error.
```

### 16.1 Read-modify-write

See §5.1. `Master::get`, `Master::put`, `Master::write` and the crate-level docs repeat it.

### 16.2 Errors

`Error` is a `thiserror` enum. The variants applications match on: `NotReady` (slave bootstrapping), `NoSuchCf`, `AlreadyExists`, `EmptyBatch`, `InvalidRange`, `EntryTooLarge`, `UnknownComparator`, `Gap { requested, first_retained }`, `Corrupt`, `Halted(HaltReason)` (from `wait_for_seq`), `Timeout`, `Io`, `RocksDb`. Reads on a halted slave succeed. Halting stops the slave from advancing; it does not stop reads.

### 16.3 Persisted vs runtime slave state

| `__meta.node_state` (persisted) | `SlaveState` (runtime) |
|---|---|
| `Ready` | `Connecting` or `Streaming` |
| `Bootstrapping { target_seq }` | `Bootstrapping { .. }`. After a restart, the bootstrap starts over. |
| `Halted(reason)`, data-related reasons only | `Halted(reason)`. The slave does not contact its upstream until `clear_halt`. |

`clear_halt(Retry)` returns to `Connecting` (useful after fixing a missing comparator, or after `LogGap` with `GapPolicy::Halt` once the operator chooses to wait). `clear_halt(Rebootstrap)` starts a fresh bootstrap. It is the only way out of `Diverged` or `IdentityChanged` under `Refuse`.

## 17. Wire protocol

Fully specified in **[PROTOCOL.md](PROTOCOL.md)**: transport and metadata, the handshake decision table, Tail, Bootstrap with retention lease, Verify with contiguous ranges and `ListRange`, the error model, timing defaults, the complete `.proto`, and test vectors.

| RPC | Shape | Summary |
|---|---|---|
| `Handshake` | unary | versions, identity and position → `RESUME` / `BOOTSTRAP` / `REJECT` with a reason |
| `Tail` | server stream | entry batches from `from_seq` (client-chosen `min_batch_bytes` / `max_wait_ms` / `max_batch_bytes`), plus heartbeats |
| `Bootstrap` | server stream | header, records, footer with digest and lease |
| `VerifyBegin` / `VerifyRange` / `VerifyEnd` | unary / bidi / unary | snapshot-pinned range digests and key listings |

- Every call carries `x-rrdb-protocol-version`, `x-rrdb-node-id` and, with token auth, `authorization`.
- Failures carry a gRPC status plus `google.rpc.ErrorInfo { domain: "rrdb.replication.v1", reason }`.
- The client sets the Tail decode limit to `max(max_entry_bytes, max_batch_bytes) + 64 KiB` (`max_entry_bytes` from the handshake) and the Bootstrap decode limit to `max(max_entry_bytes, 16 MiB) + 64 KiB`; servers keep `bootstrap_chunk_bytes` at most 16 MiB. Only the master enforces the size limit; downstream nodes accept what arrives.
- A Tail stream with no message for `3 × heartbeat` is cancelled and reconnected.

## 18. Defaults

| Setting | Default |
|---|---|
| Changelog retention `max_age` | 14 days |
| Retention check interval | 10 min |
| `max_entry_bytes` | 16 MiB |
| Master fsync | always (not configurable) |
| Slave fsync | off |
| Heartbeat / idle timeout | 5 s / 15 s |
| Reconnect backoff | 100 ms → 30 s, ×2, full jitter |
| Identity change policy | Refuse |
| Log gap policy | Rebootstrap |
| Bootstrap lease TTL | 15 min |
| Max concurrent bootstraps / verify sessions served | 2 / 4 |
| Bootstrap chunk size | 1 MiB |
| Tail fetch batching (slave) | `min_batch_bytes` 0 (real-time), `max_wait` 100 ms, `max_batch_bytes` 1 MiB; server cap 4 MiB |
| Verify | off by default; `Quick` when enabled |
| Verify session timeout | 5 min |
| Mismatch policy | Report |
| Reaper | every 30 s; 1 000 deletes per entry; 100 000 per round |

## 19. Observability

- **`tracing` spans** for each handshake, bootstrap, verify session and reaper round, with `node_id`, `upstream`, `seq` and `epoch` fields. Each applied entry is logged at `trace` level only.
- **Status APIs** (`Slave::status`, `Master::replicas`, `ReplicaStatus`) are the source for metrics. The crate does not depend on a metrics library. A recommended set for applications to export:
  - `applied_seq`, `origin_last_seq`, lag in seqs and `lag_ms`;
  - state (with halt reason), reconnects, bootstraps;
  - entries and bytes applied and served;
  - verify runs, mismatches, last verify time;
  - changelog size and `first_retained_seq`;
  - reaper deletes, number of index entries due.
- Every halt is logged once at `error` level with its reason and the seqs involved, and is reported through `on_state_change`.

## 20. Crate layout and dependencies

```
replicated-rocksdb/
  proto/replication.proto      (verbatim copy of PROTOCOL.md §14)
  src/
    lib.rs
    types.rs          Seq, DbIdentity, CfSpec, Op, LogEntry, Batch, ValueMeta
    codec.rs          LogEntry encoding + crc, value envelope, digest framing
    store/            shared DB wrapper: reserved CFs, __meta, ReadApi/LogApi, TTL filtering,
                      CF lifecycle + refcounted handles, comparators, retention + guards, snapshots
    source.rs         LogSource trait + impls
    server/           replication gRPC server (shared by master & slave): handshake decisions,
                      tail, bootstrap + leases, verify sessions, limits
    master/           writer, reaper, open/identity/epoch
    slave/            upstream client, applier, bootstrap install, verifier, halt handling
    auth.rs           traits + Token/AllowAll/Mtls impls, auth-failure rate limit
    listener.rs  config.rs  error.rs
  tests/
    vectors.rs        PROTOCOL.md §15 test vectors
```

Dependencies: `rocksdb`, `tokio`, `tonic` (tls), `prost`, `tonic-types` (ErrorInfo), `bytes`, `uuid`, `blake3`, `crc32c`, `subtle`, `thiserror`, `tracing`, `async-trait`.

## 21. Test plan (integration, in-process nodes)

- A→B convergence under concurrent writes; verify `Full` matches.
- A→B→C chain, with C bootstrapping from B while A writes and creates/drops CFs. Checksums for A, B and C must be identical.
- Slave crash and restart at random points: no gaps, no duplicates, listener order preserved.
- C ahead of B after B crashes unsynced: C waits and then converges.
- Retention trims the log past a stopped slave; the slave bootstraps again on restart.
- **Bootstrap lease:** aggressive retention (`max_entries = 1`) during a slow bootstrap; the slave still tails from `S + 1` without bootstrapping again.
- Quiet master with every changelog entry expired: `last_seq` is preserved and slaves resume.
- TTL: expiry is visible on reads before reaping; the reaper `Delete` replicates; overwrite clears the TTL; stale index entries are cleaned; reaping more than `max_deletes_per_entry` keys produces several entries.
- **Divergence:** master restored from an older backup without an epoch bump → slaves halt `Diverged`; with `bump_epoch_on_open` → slaves at or behind the restore point continue (`EPOCH_ADVANCED`), slaves past it are `DIVERGED` and follow `on_divergence`.
- **Promotion:** A→B, A→C, A→D. Stop A. Promote B. C (behind B) continues from B without a bootstrap; D (ahead of B) is `DIVERGED` and, with `Rebootstrap`, converges. A restarted A is refused with `STALE_EPOCH`. Writes on the new master replicate, TTLs written before promotion are reaped by the new master, and `Full` verify matches on every link.
- **Failure and resume:** kill and restart the master, a mid-chain slave and a leaf while writes continue; crash a slave mid-bootstrap (failpoint) and mid-apply; drop connections at random. Every node converges, with no gaps or duplicates.
- **Edge-case data on every path** (write, reopen, tail, bootstrap, verify, promotion): zero-length keys and values, keys and values containing NUL bytes (including all-NUL), a 1 MiB key, and values up to the 16 MiB entry limit.
- **Epoch bump:** streams end with `UPSTREAM_RESET`; seq continues; a `Rebootstrap` slave converges.
- Identity change triggers `Halted`. Protocol mismatch triggers `REJECT`. A loop triggers `LoopDetected`.
- Halt persistence: a data-related halt survives restart; `clear_halt(Rebootstrap)` recovers it.
- CF drop while a `ReadSnapshot` is open: the snapshot still reads the CF; the physical drop happens after the snapshot is released.
- Unregistered comparator: the master rejects it; a slave without the comparator halts `SpecMismatch` and recovers after it is registered.
- Token auth accept/reject; per-method authorisation; token without TLS is refused.
- Entry over `max_entry_bytes` is rejected on the master. A large valid entry passes through the chain.
- Injected corruption on a slave is detected by `Full` verify and localised to the key. A key that exists only on the upstream, between the slave's chunk boundaries, is found.
- Wire codec matches the PROTOCOL.md §15 test vectors.

## 22. Decisions and future work

**Decided**
- No `update` or compare-and-set API. Read-modify-write is done by the application under its own lock (§5.1).
- `LogEntry.ops` is opaque bytes, forwarded verbatim; the CRC covers the header fields and the ops.
- Divergence is detected by CRC comparison on connect and by rejecting slaves ahead of the master. It is not repaired automatically.
- The bootstrap retention guard lasts until the follow-up Tail presents the lease, or the lease expires.
- Verify ranges are half-open and contiguous; leaf mismatches are resolved by listing keys.
- Data-related halts are persisted and need operator action; configuration halts are not persisted.
- Epoch history with fork points lets replicas keep their data across epoch bumps and promotions when their history is a proven prefix.
- Promotion is manual (`Slave::promote`). Fencing the old master is the operator's job; `STALE_EPOCH` and the CRC checks are backstops.

**Future**
- Automatic range repair after a verify mismatch.
- Semi-sync writes (wait for N replica acks).
