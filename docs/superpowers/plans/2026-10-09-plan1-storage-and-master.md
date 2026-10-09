# Plan 1 of 3 — Storage Engine and Master Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build the `replicated-rocksdb` crate up to a fully working, single-node `Master`: wire codec with test vectors, the RocksDB-backed store (reserved CFs, `__meta`, value envelopes, changelog, CF lifecycle, snapshots), the fsynced write path, the read API with TTL filtering, the TTL reaper, and changelog retention.

**Architecture:** A crate-private `Store` owns the RocksDB handle and implements everything that master and slave share: two-phase open, `prepare`/`commit` of one `LogEntry` per `WriteBatch`, the CF registry with reference-counted physical CFs, snapshots, reads and retention. `Master` wraps the `Store` with the writer lock, identity/epoch, TTL index and reaper, and a tokio maintenance task. Plan 2 adds the replication server and `Slave` on top of the same `Store::prepare`/`commit`.

**Tech Stack:** Rust 1.99 (edition 2024), rocksdb 0.25.0 (`multi-threaded-cf`), prost 0.14.4 + tonic-prost-build 0.14.6 (messages only in this plan), tokio 1.53, bytes, uuid, blake3, crc32c, thiserror, tracing. Dev: tempfile.

**Spec:** [DESIGN.md](../../../DESIGN.md) and [PROTOCOL.md](../../../PROTOCOL.md). Read DESIGN.md §3–§9 and §16, and PROTOCOL.md §5 and §15, before starting.

**Series:**
- **Plan 1 (this):** storage engine and Master. Ends with a usable embedded, non-replicating master.
- **Plan 2:** replication server (Handshake, Tail, Bootstrap with lease), auth and TLS, `Slave` (applier, bootstrap install, halts, listener), chained replication tests.
- **Plan 3:** Verify (sessions, range hashing, `ListRange` narrowing), observability, and the remaining DESIGN.md §21 tests.

## Global Constraints

- Crate name `replicated-rocksdb`, edition 2024, `rust-version = "1.85"`.
- Dependency versions, exact minimums: `rocksdb = 0.25.0` (`default-features = false`, features `lz4`, `zstd`, `bindgen-runtime`, `multi-threaded-cf`), `prost = 0.14.4`, `tonic-prost-build = 0.14.6`, `tokio = 1.53`, `bytes = 1.12`, `uuid = 1.27` (`v4`), `blake3 = 1.8`, `crc32c = 0.6.8`, `thiserror = 2.0`, `tracing = 0.1.44`, dev `tempfile = 3.27`.
- `PROTOCOL_VERSION = 1`, `ENVELOPE_VERSION = 1`. Protobuf package `rrdb.replication.v1`.
- **Absolute ops only:** `Put`, `Delete`, `DeleteRange`, `CreateCf`, `DropCf`. No merge, no relative ops. No API may let a caller set a merge operator.
- **Atomicity:** mutation, `__changelog[seq]` and `__meta.last_seq` always go in the same `WriteBatch`. One `LogEntry` = one `WriteBatch`.
- **`__meta.last_seq` is the source of truth for position**, never the last changelog key.
- **Master always fsyncs** (`WriteOptions::set_sync(true)`), not configurable. A failed master write fences the writer until reopen.
- `LogEntry.ops` is opaque bytes. It is stored and forwarded verbatim, never re-encoded. `crc32c = CRC-32C(BE64(seq) ‖ BE64(epoch) ‖ BE64(ts_ms) ‖ ops)`.
- Every user value is stored as the envelope `[ver=1][flags][write_seq BE64][expire_at_ms BE64 if flags&1][value]`; reserved flag bits must be 0.
- User CF names: non-empty, at most 255 bytes, must not start with `__`. Physical name `u:{name}:{create_seq}`.
- Reserved CFs: `__meta`, `__changelog`, `__ttl_index`.
- Node identifier: 1–128 characters of `[A-Za-z0-9._-]`.
- Defaults: `max_entry_bytes` 16 MiB; retention `max_age` 14 days, `max_entries` None, `check_interval` 10 min; reaper `interval` 30 s, `max_deletes_per_entry` 1 000, `max_deletes_per_round` 100 000; heartbeat 5 s.
- **No read-modify-write API.** The rustdoc of `Master::put`, `Master::write`, `Master::get` and the crate-level docs must say the application must hold its own lock around get → compute → put (DESIGN.md §5.1).
- `proto/replication.proto` must be byte-identical to the proto block in PROTOCOL.md §14.
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.

## Review Focus

1. **Empty, NUL and large keys and values.** `b""` and `b"\0"` are valid, distinct keys and values; 1 MiB keys and values near the entry limit must round-trip. Put, get, iterate, delete and `delete_range(b"", b"a")` must all work. (Tests in Task 6 for writes and Task 7 for reads.)
2. **`max_entry_bytes` boundary.** An entry whose encoded size equals the limit is accepted; one byte more is rejected with `EntryTooLarge` and consumes no seq. (Test in Task 8.)
3. **Concurrent writers.** Several threads writing at once get unique, gap-free seqs, and `ts_ms` never decreases even when the clock goes backwards. (Test in Task 8.)
4. **CF names with `:` and non-ASCII characters.** These must survive create, write, reopen and drop, because the physical name `u:{name}:{seq}` is never parsed back. (Test in Task 6.)
5. **Extreme TTLs.** `Duration::ZERO` expires immediately, and `Duration::MAX` must not overflow or panic (saturating add). (Test in Task 9.)

## Design deltas this plan introduces

These are small decisions the spec left open. Each owning task updates DESIGN.md or PROTOCOL.md with the exact text given there.

- **`RocksTuning` is a restricted wrapper** (Task 4). The callback receives a `Tunable` exposing only safe setters. Merge operators, compaction filters and native TTL cannot be set, so they are rejected by construction.
- **`__meta.cf_create_intent`** (Task 6). Written with fsync before a physical CF is created and cleared in the `CreateCf` commit batch. On open, an unregistered CF named by the intent is dropped. Without it, a crash could leave a CF whose comparator is unknown, and RocksDB cannot reopen it.
- **Two-phase open** (Task 5). The `__meta` CF is read through a read-only open first, so the comparator of every user CF is known before the read-write open.
- **CF name limit of 255 bytes** (Task 3). This goes into PROTOCOL.md §5.2 rule 3.
- **`ReadSnapshot` has no lifetime parameter** (Task 7). It owns an `Arc` of the DB, so it can be moved into tasks.
- **`__ttl_index` key** = `BE64(expire_at_ms) ‖ BE64(create_seq) ‖ BE16(len(name)) ‖ name ‖ key` (Task 9). Including `create_seq` stops a dropped and re-created CF from matching old index entries.
- **Epoch history** (Tasks 2, 5, 8) persisted in `__meta.epoch_history` and exposed as `Master::epoch_history()`, per the promotion changes in DESIGN.md §13 and PROTOCOL.md §5.5.
- **`Clock` injection** (Task 2). `MasterConfig.clock: Arc<dyn Clock>` (default `SystemClock`); tests use `ManualClock`.

## File Structure

```
Cargo.toml                    crate manifest (Task 1)
build.rs                      prost codegen for proto/*.proto (Task 1)
.gitignore                    (Task 1)
proto/replication.proto       verbatim copy of PROTOCOL.md §14 (Task 1)
proto/local.proto             node-local metadata messages (Task 1)
src/lib.rs                    module tree, re-exports, crate docs (Tasks 1, 8)
src/proto.rs                  generated code include (Task 1)
src/error.rs                  Error, Result (Task 2)
src/types.rs                  Seq, DbIdentity, Role, CfSpec, CfEntry, Op, LogEntry, ValueMeta, Batch, IterMode (Task 2)
src/clock.rs                  Clock, SystemClock, ManualClock (Task 2)
src/codec/mod.rs              PROTOCOL_VERSION, ENVELOPE_VERSION, re-exports (Task 3)
src/codec/entry.rs            entry_crc, LogEntry encode/decode, Op <-> pb, structure checks (Task 3)
src/codec/envelope.rs         value envelope encode/decode (Task 3)
src/codec/digest.rs           BLAKE3 digest framing (Task 3)
src/tuning.rs                 RocksTuning, Tunable, ComparatorRegistry, CfKind, Compression (Task 4)
src/store/mod.rs              Store: two-phase open, position, meta writes, CF handles (Task 5)
src/store/meta.rs             __meta keys, value encodings, MetaSnapshot (Task 5)
src/store/cfs.rs              CfRegistry, PhysCf, drop_unreferenced_cfs (Tasks 5, 6)
src/store/apply.rs            Store::prepare / Store::commit (Task 6)
src/store/test_support.rs     cfg(test) helpers (Task 6)
src/store/snapshot.rs         OwnedSnapshot (Task 7)
src/store/read.rs             ReadCtx, DbIter (Task 7)
src/read.rs                   ReadApi trait, ReadSnapshot (Task 7)
src/store/log.rs              EntryIter (Task 8)
src/log.rs                    LogApi trait (Task 8)
src/config.rs                 MasterConfig, Retention, ReaperConfig, identifier validation (Task 8)
src/master/mod.rs             Master public API (Task 8)
src/master/writer.rs          Inner, WriterState, commit path, bump_epoch (Task 8)
src/store/ttl.rs              TTL index keys, ttl_due, liveness check (Task 9)
src/master/reaper.rs          reap_round, ReapStats (Task 9)
src/store/retention.rs        GuardSet, RetentionGuard, trim_changelog (Task 10)
src/master/maintenance.rs     background maintenance task (Task 10)
tests/proto_sync.rs           proto file == PROTOCOL.md §14 (Task 1)
tests/common/mod.rs           integration test helpers (Task 8)
tests/master_write.rs         master write path tests (Task 8)
tests/master_ttl.rs           TTL and reaper tests (Task 9)
tests/master_retention.rs     retention and maintenance tests (Task 10)
```

---

### Task 1: Crate scaffold, protobuf codegen and proto sync test

**Files:**
- Create: `Cargo.toml`, `build.rs`, `.gitignore`, `proto/replication.proto`, `proto/local.proto`, `src/lib.rs`, `src/proto.rs`
- Test: `tests/proto_sync.rs`

**Interfaces:**
- Consumes: nothing.
- Produces: generated modules `crate::proto::replication::v1` (alias `pb`) and `crate::proto::local::v1`. Key generated types: `pb::LogEntry { seq: u64, epoch: u64, ts_ms: u64, ops: bytes::Bytes, crc32c: u32 }`, `pb::OpList { ops: Vec<pb::Op> }`, `pb::Op { kind: Option<pb::op::Kind> }`, `pb::op::Kind::{Put, Delete, DeleteRange, CreateCf, DropCf}`, `pb::Put { cf, key, value, expire_at_ms: Option<u64> }`, `pb::Delete { cf, key }`, `pb::DeleteRange { cf, start, end }`, `pb::CreateCf { name, comparator: Option<String> }`, `pb::DropCf { name }`, `pb::CfSpec { name, comparator: Option<String>, create_seq: u64 }`, `pb::Identity { db_uuid: Vec<u8>, epoch: u64 }`, `pb::EpochStart { epoch: u64, start_seq: u64 }`, `local::v1::CfEntryList { cfs: Vec<pb::CfSpec> }`, `local::v1::EpochHistory { epochs: Vec<pb::EpochStart> }`.

- [ ] **Step 1: Initialise git and the manifest**

```bash
cd /home/code/home_workspace/replicated-rocksdb
git init -b main
```

Create `.gitignore`:

```
/target
```

Create `Cargo.toml`:

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
tokio = { version = "1.53", features = ["rt-multi-thread", "macros", "sync", "time"] }
uuid = { version = "1.27", features = ["v4"] }
blake3 = "1.8"
crc32c = "0.6.8"
thiserror = "2.0"
tracing = "0.1.44"

[build-dependencies]
tonic-prost-build = "0.14.6"

[dev-dependencies]
tempfile = "3.27"
```

- [ ] **Step 2: Write the failing proto sync test**

Create `tests/proto_sync.rs`:

```rust
//! `proto/replication.proto` must be a verbatim copy of PROTOCOL.md §14.

#[test]
fn proto_file_matches_protocol_spec() {
    let root = env!("CARGO_MANIFEST_DIR");
    let spec = std::fs::read_to_string(format!("{root}/PROTOCOL.md")).unwrap();
    let section = spec.split("\n## 14.").nth(1).expect("PROTOCOL.md has a §14");
    let open = "```proto\n";
    let start = section.find(open).expect("§14 has a proto block") + open.len();
    let len = section[start..].find("\n```").expect("proto block is closed");
    let from_spec = &section[start..start + len + 1];
    let file = std::fs::read_to_string(format!("{root}/proto/replication.proto"))
        .expect("proto/replication.proto exists");
    assert_eq!(
        file, from_spec,
        "proto/replication.proto must be a verbatim copy of PROTOCOL.md §14"
    );
}
```

- [ ] **Step 3: Run it to confirm it fails**

Run: `cargo test --test proto_sync`
Expected: FAIL. The build fails, because there is no `src/lib.rs` yet. (The first build also compiles RocksDB, which takes several minutes.)

- [ ] **Step 4: Create the proto files**

```bash
mkdir -p proto
awk '/^## 14\./{f=1} f&&/^```proto/{p=1;next} p&&/^```/{exit} p' PROTOCOL.md > proto/replication.proto
```

Create `proto/local.proto`:

```proto
syntax = "proto3";

// Node-local metadata. Never sent on the wire.
package rrdb.local.v1;

import "replication.proto";

message CfEntryList  { repeated rrdb.replication.v1.CfSpec cfs = 1; }
message EpochHistory { repeated rrdb.replication.v1.EpochStart epochs = 1; }
```

Create `build.rs`:

```rust
fn main() -> Result<(), Box<dyn std::error::Error>> {
    println!("cargo:rerun-if-changed=proto");
    tonic_prost_build::configure()
        .build_server(false)
        .build_client(false)
        .bytes(".rrdb.replication.v1.LogEntry.ops")
        .compile_protos(&["proto/replication.proto", "proto/local.proto"], &["proto"])?;
    Ok(())
}
```

Create `src/proto.rs`. The module nesting mirrors the protobuf packages, because prost emits `super::super::replication::v1::CfSpec` for the cross-package reference:

```rust
//! Generated protobuf code. `proto` stands for the `rrdb` package root.

pub mod replication {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/rrdb.replication.v1.rs"));
    }
}

pub mod local {
    pub mod v1 {
        include!(concat!(env!("OUT_DIR"), "/rrdb.local.v1.rs"));
    }
}
```

Create `src/lib.rs`:

```rust
//! Single-master, async log-shipping replication for RocksDB.

#![forbid(unsafe_op_in_unsafe_fn)]

pub(crate) mod proto;

#[allow(unused_imports)]
pub(crate) use proto::replication::v1 as pb;
```

- [ ] **Step 5: Run the test and the build**

Run: `cargo test --test proto_sync`
Expected: PASS (`proto_file_matches_protocol_spec ... ok`).

- [ ] **Step 6: Commit**

```bash
git add .gitignore Cargo.toml Cargo.lock build.rs proto src tests CLAUDE.md DESIGN.md PROTOCOL.md docs
git commit -m "chore: scaffold crate with protobuf codegen and proto sync test

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Core types, errors and clock

**Files:**
- Create: `src/error.rs`, `src/types.rs`, `src/clock.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `pub enum Error` (variants below), `pub type Result<T>`, `Error::corrupt(impl Into<String>) -> Error`.
  - `pub type Seq = u64`; `pub struct DbIdentity { pub db_uuid: Uuid, pub epoch: u64 }`; `pub enum Role { Master, Slave }` with `pub(crate) fn as_bytes(self) -> &'static [u8]` and `pub(crate) fn from_bytes(&[u8]) -> Option<Role>`.
  - `pub struct CfSpec { pub name: String, pub comparator: Option<String> }` with `new`, `with_comparator`.
  - `pub(crate) struct CfEntry { pub spec: CfSpec, pub create_seq: Seq }` with `physical_name() -> String`.
  - `pub enum Op { Put{cf,key,value,expire_at_ms}, Delete{cf,key}, DeleteRange{cf,start,end}, CreateCf{spec}, DropCf{name} }` with `pub fn cf_name(&self) -> &str`.
  - `pub struct LogEntry { pub seq, pub epoch, pub ts_ms, pub ops: Bytes, pub crc32c: u32 }`.
  - `pub struct ValueMeta { pub write_seq: Seq, pub expire_at_ms: Option<u64> }`.
  - `pub struct EpochStart { pub epoch: u64, pub start_seq: Seq }`; `pub(crate) fn epoch_for(history: &[EpochStart], seq: Seq) -> Option<u64>`; `pub(crate) fn fork_after(history: &[EpochStart], epoch: u64) -> Option<Seq>`.
  - `pub struct Batch` with `new, put, put_with_ttl, delete, delete_range, len, is_empty`, `pub(crate) fn into_ops(self, ts_ms: u64) -> Vec<Op>`.
  - `pub enum Direction { Forward, Reverse }`, `pub enum IterMode<'a> { Start, End, From(&'a [u8], Direction) }`.
  - `pub trait Clock: Send + Sync + 'static { fn now_ms(&self) -> u64; }`, `SystemClock`, `ManualClock { new(ms), set(ms), advance(Duration) }`.

- [ ] **Step 1: Write the failing tests**

Create `src/types.rs` with only the test module first:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn physical_name_uses_create_seq() {
        let e = CfEntry { spec: CfSpec::new("a:b"), create_seq: 42 };
        assert_eq!(e.physical_name(), "u:a:b:42");
    }

    #[test]
    fn batch_resolves_ttl_against_commit_time() {
        let mut b = Batch::new();
        b.put("c", b"k1", b"v1")
            .put_with_ttl("c", b"k2", b"v2", Duration::from_secs(5))
            .put_with_ttl("c", b"k3", b"v3", Duration::MAX)
            .delete("c", b"k4")
            .delete_range("c", b"a", b"b");
        assert_eq!(b.len(), 5);
        let ops = b.into_ops(1_000);
        assert_eq!(
            ops[0],
            Op::Put { cf: "c".into(), key: b"k1".to_vec(), value: b"v1".to_vec(), expire_at_ms: None }
        );
        assert_eq!(
            ops[1],
            Op::Put { cf: "c".into(), key: b"k2".to_vec(), value: b"v2".to_vec(), expire_at_ms: Some(6_000) }
        );
        assert_eq!(
            ops[2],
            Op::Put { cf: "c".into(), key: b"k3".to_vec(), value: b"v3".to_vec(), expire_at_ms: Some(u64::MAX) }
        );
        assert_eq!(ops[3], Op::Delete { cf: "c".into(), key: b"k4".to_vec() });
        assert_eq!(
            ops[4],
            Op::DeleteRange { cf: "c".into(), start: b"a".to_vec(), end: b"b".to_vec() }
        );
    }

    #[test]
    fn epoch_history_lookups() {
        let h = [
            EpochStart { epoch: 1, start_seq: 1 },
            EpochStart { epoch: 2, start_seq: 701 },
            EpochStart { epoch: 4, start_seq: 900 },
        ];
        assert_eq!(epoch_for(&h, 0), None);
        assert_eq!(epoch_for(&h, 1), Some(1));
        assert_eq!(epoch_for(&h, 700), Some(1));
        assert_eq!(epoch_for(&h, 701), Some(2));
        assert_eq!(epoch_for(&h, 10_000), Some(4));
        assert_eq!(fork_after(&h, 1), Some(701));
        assert_eq!(fork_after(&h, 2), Some(900));
        assert_eq!(fork_after(&h, 3), Some(900));
        assert_eq!(fork_after(&h, 4), None);
    }

    #[test]
    fn role_round_trips() {
        for r in [Role::Master, Role::Slave] {
            assert_eq!(Role::from_bytes(r.as_bytes()), Some(r));
        }
        assert_eq!(Role::from_bytes(b"other"), None);
    }
}
```

Create `src/clock.rs` with only its test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn manual_clock_moves_only_when_told() {
        let c = ManualClock::new(10);
        assert_eq!(c.now_ms(), 10);
        c.advance(Duration::from_millis(5));
        assert_eq!(c.now_ms(), 15);
        c.set(3);
        assert_eq!(c.now_ms(), 3);
    }

    #[test]
    fn system_clock_is_after_2020() {
        assert!(SystemClock.now_ms() > 1_577_836_800_000);
    }
}
```

Add to `src/lib.rs`:

```rust
pub mod clock;
pub mod error;
pub mod types;
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --lib`
Expected: FAIL to compile: `cannot find type CfEntry`, `cannot find type ManualClock`, `file not found for module error`.

- [ ] **Step 3: Implement**

Create `src/error.rs`:

```rust
use crate::types::{Role, Seq};

/// Errors returned by this crate.
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("node is not ready: it is bootstrapping")]
    NotReady,
    #[error("no such column family: {0}")]
    NoSuchCf(String),
    #[error("column family already exists: {0}")]
    AlreadyExists(String),
    #[error("invalid column family name {0:?}: must be 1-255 bytes and must not start with \"__\"")]
    InvalidCfName(String),
    #[error("batch is empty")]
    EmptyBatch,
    #[error("invalid range: start must sort strictly before end")]
    InvalidRange,
    #[error("entry too large: {size} bytes exceeds max_entry_bytes {max}")]
    EntryTooLarge { size: u64, max: u64 },
    #[error("unknown comparator: {0}")]
    UnknownComparator(String),
    #[error("log gap: seq {requested} requested, first retained is {first_retained}")]
    Gap { requested: Seq, first_retained: Seq },
    #[error("corrupt data: {0}")]
    Corrupt(String),
    #[error("database was created as {found:?} and cannot be opened as {expected:?}")]
    WrongRole { expected: Role, found: Role },
    #[error("{what} is {found}, this build supports {expected}")]
    VersionMismatch { what: &'static str, found: u64, expected: u64 },
    #[error("the writer is fenced after a failed write; reopen the database")]
    WriterFenced,
    #[error("invalid configuration: {0}")]
    Config(String),
    #[error("timed out")]
    Timeout,
    #[error(transparent)]
    RocksDb(#[from] rocksdb::Error),
    #[error(transparent)]
    Io(#[from] std::io::Error),
}

impl Error {
    pub(crate) fn corrupt(msg: impl Into<String>) -> Self {
        Error::Corrupt(msg.into())
    }
}

pub type Result<T, E = Error> = std::result::Result<T, E>;
```

Put this above the test module in `src/types.rs`:

```rust
//! Core value types shared by the whole crate.

use std::time::Duration;

use bytes::Bytes;

/// Sequence number of a log entry. Assigned by the master; 0 means "nothing applied".
pub type Seq = u64;

/// Identity of the origin master. The same on every node of a topology.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DbIdentity {
    pub db_uuid: uuid::Uuid,
    pub epoch: u64,
}

/// Role persisted in `__meta`. A directory created for one role cannot be opened as the other.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Role {
    Master,
    Slave,
}

impl Role {
    pub(crate) fn as_bytes(self) -> &'static [u8] {
        match self {
            Role::Master => b"master",
            Role::Slave => b"slave",
        }
    }

    pub(crate) fn from_bytes(b: &[u8]) -> Option<Role> {
        match b {
            b"master" => Some(Role::Master),
            b"slave" => Some(Role::Slave),
            _ => None,
        }
    }
}

/// A user column family as the application sees it.
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct CfSpec {
    pub name: String,
    /// Name of a comparator registered on every node; `None` means bytewise order.
    pub comparator: Option<String>,
}

impl CfSpec {
    pub fn new(name: impl Into<String>) -> Self {
        CfSpec { name: name.into(), comparator: None }
    }

    pub fn with_comparator(mut self, comparator: impl Into<String>) -> Self {
        self.comparator = Some(comparator.into());
        self
    }
}

/// A registered user CF: its spec plus the seq of the `CreateCf` entry that created it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct CfEntry {
    pub spec: CfSpec,
    pub create_seq: Seq,
}

impl CfEntry {
    /// Physical RocksDB CF name, `u:{name}:{create_seq}`. Never parsed back.
    pub fn physical_name(&self) -> String {
        format!("u:{}:{}", self.spec.name, self.create_seq)
    }
}

/// One absolute operation. Applying it never depends on prior state.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Op {
    Put { cf: String, key: Vec<u8>, value: Vec<u8>, expire_at_ms: Option<u64> },
    Delete { cf: String, key: Vec<u8> },
    /// Removes keys in `[start, end)` under the CF's comparator.
    DeleteRange { cf: String, start: Vec<u8>, end: Vec<u8> },
    CreateCf { spec: CfSpec },
    DropCf { name: String },
}

impl Op {
    /// The logical CF this op touches, or creates or drops.
    pub fn cf_name(&self) -> &str {
        match self {
            Op::Put { cf, .. } | Op::Delete { cf, .. } | Op::DeleteRange { cf, .. } => cf,
            Op::CreateCf { spec } => &spec.name,
            Op::DropCf { name } => name,
        }
    }
}

/// One replicated log entry. `ops` holds the encoded `OpList` exactly as the master produced it.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct LogEntry {
    pub seq: Seq,
    pub epoch: u64,
    /// Master commit time in ms since the Unix epoch; non-decreasing in `seq`.
    pub ts_ms: u64,
    pub ops: Bytes,
    pub crc32c: u32,
}

/// Metadata stored in a value's envelope.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ValueMeta {
    /// Seq of the entry that wrote the value.
    pub write_seq: Seq,
    /// Absolute expiry in ms since the Unix epoch, if the value was written with a TTL.
    pub expire_at_ms: Option<u64>,
}

/// Start of one epoch in the epoch history (PROTOCOL.md §5.5).
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct EpochStart {
    pub epoch: u64,
    /// Seq of the first entry written in this epoch.
    pub start_seq: Seq,
}

/// The epoch an entry with this seq must carry, given a history sorted by epoch.
pub(crate) fn epoch_for(history: &[EpochStart], seq: Seq) -> Option<u64> {
    history.iter().rev().find(|e| e.start_seq <= seq).map(|e| e.epoch)
}

/// The fork point after `epoch`: the first seq of the next epoch, if any.
pub(crate) fn fork_after(history: &[EpochStart], epoch: u64) -> Option<Seq> {
    history.iter().find(|e| e.epoch > epoch).map(|e| e.start_seq)
}

#[derive(Clone, Debug)]
pub(crate) enum BatchOp {
    Put { cf: String, key: Vec<u8>, value: Vec<u8>, ttl: Option<Duration> },
    Delete { cf: String, key: Vec<u8> },
    DeleteRange { cf: String, start: Vec<u8>, end: Vec<u8> },
}

/// Several data operations committed as one log entry. May span CFs; must not be empty.
#[derive(Clone, Debug, Default)]
pub struct Batch {
    pub(crate) ops: Vec<BatchOp>,
}

impl Batch {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn put(&mut self, cf: &str, key: &[u8], value: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Put { cf: cf.into(), key: key.into(), value: value.into(), ttl: None });
        self
    }

    pub fn put_with_ttl(&mut self, cf: &str, key: &[u8], value: &[u8], ttl: Duration) -> &mut Self {
        self.ops.push(BatchOp::Put { cf: cf.into(), key: key.into(), value: value.into(), ttl: Some(ttl) });
        self
    }

    pub fn delete(&mut self, cf: &str, key: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::Delete { cf: cf.into(), key: key.into() });
        self
    }

    pub fn delete_range(&mut self, cf: &str, start: &[u8], end: &[u8]) -> &mut Self {
        self.ops.push(BatchOp::DeleteRange { cf: cf.into(), start: start.into(), end: end.into() });
        self
    }

    pub fn len(&self) -> usize {
        self.ops.len()
    }

    pub fn is_empty(&self) -> bool {
        self.ops.is_empty()
    }

    /// Converts to absolute ops: each TTL becomes `expire_at_ms = ts_ms + ttl` (saturating).
    pub(crate) fn into_ops(self, ts_ms: u64) -> Vec<Op> {
        self.ops
            .into_iter()
            .map(|op| match op {
                BatchOp::Put { cf, key, value, ttl } => Op::Put {
                    cf,
                    key,
                    value,
                    expire_at_ms: ttl.map(|t| expire_at(ts_ms, t)),
                },
                BatchOp::Delete { cf, key } => Op::Delete { cf, key },
                BatchOp::DeleteRange { cf, start, end } => Op::DeleteRange { cf, start, end },
            })
            .collect()
    }
}

/// `ts_ms + ttl` in ms, saturating at `u64::MAX`.
pub(crate) fn expire_at(ts_ms: u64, ttl: Duration) -> u64 {
    let ttl_ms = u64::try_from(ttl.as_millis()).unwrap_or(u64::MAX);
    ts_ms.saturating_add(ttl_ms)
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Direction {
    Forward,
    Reverse,
}

/// Where an iterator starts.
#[derive(Clone, Copy, Debug)]
pub enum IterMode<'a> {
    Start,
    End,
    From(&'a [u8], Direction),
}
```

Put this above the test module in `src/clock.rs`:

```rust
//! Time source. Injected so tests can control TTL expiry and retention.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync + 'static {
    /// Milliseconds since the Unix epoch.
    fn now_ms(&self) -> u64;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// A clock that changes only when told to. For tests.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        ManualClock(AtomicU64::new(now_ms))
    }

    pub fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }

    pub fn advance(&self, by: Duration) {
        let by = u64::try_from(by.as_millis()).unwrap_or(u64::MAX);
        let _ = self.0.fetch_update(Ordering::SeqCst, Ordering::SeqCst, |t| Some(t.saturating_add(by)));
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}
```

Add re-exports to `src/lib.rs`:

```rust
pub use clock::{Clock, ManualClock, SystemClock};
pub use error::{Error, Result};
pub use types::{Batch, CfSpec, DbIdentity, Direction, EpochStart, IterMode, LogEntry, Op, Role, Seq, ValueMeta};
```

- [ ] **Step 4: Run the tests to confirm they pass**

Run: `cargo test --lib`
Expected: PASS, 6 tests.

- [ ] **Step 5: Commit**

```bash
git add src
git commit -m "feat: core types, errors and injectable clock

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: Wire codec — log entries, envelopes and digests

**Files:**
- Create: `src/codec/mod.rs`, `src/codec/entry.rs`, `src/codec/envelope.rs`, `src/codec/digest.rs`
- Modify: `src/lib.rs`, `PROTOCOL.md` (§5.2 rule 3)

**Interfaces:**
- Consumes: `types::{Op, LogEntry, CfSpec, ValueMeta, Seq}`, `Error`, `pb::*`.
- Produces:
  - `pub const PROTOCOL_VERSION: u32 = 1; pub const ENVELOPE_VERSION: u8 = 1; pub const MAX_CF_NAME_BYTES: usize = 255;`
  - `pub fn entry_crc(seq, epoch, ts_ms, ops: &[u8]) -> u32`
  - `impl LogEntry { pub fn new(seq, epoch, ts_ms, ops: Bytes) -> Self; pub fn verify_crc(&self) -> Result<()>; pub fn decode_ops(&self) -> Result<Vec<Op>>; pub(crate) fn encode_to_vec(&self) -> Vec<u8>; pub(crate) fn encoded_len(&self) -> usize; pub(crate) fn decode(bytes: &[u8]) -> Result<LogEntry> }`
  - `pub(crate) fn encode_ops(ops: &[Op]) -> Bytes`, `pub(crate) fn decode_ops(bytes: &[u8]) -> Result<Vec<Op>>`, `pub(crate) fn check_structure(ops: &[Op]) -> Result<()>`, `pub fn validate_cf_name(name: &str) -> Result<()>`
  - `envelope::{FLAG_TTL, encode(write_seq, expire_at_ms: Option<u64>, value) -> Vec<u8>, decode(&[u8]) -> Result<Envelope<'_>>}`, `Envelope { meta: ValueMeta, header_len: usize, value: &[u8] }` with `is_expired(now_ms) -> bool`
  - `digest::Digest { new, cf_marker(name, create_seq), record(key, payload), records() -> u64, finish_range() -> (u64, [u8; 32]), finish_bootstrap(snapshot_seq) -> (u64, [u8; 32]) }`

- [ ] **Step 1: Write the failing tests (PROTOCOL.md §15 vectors)**

Create `src/codec/entry.rs` with only its tests. The module is `pub(crate)` because `envelope.rs` tests reuse `hex`:

```rust
#[cfg(test)]
pub(crate) mod tests {
    use super::*;
    use crate::types::CfSpec;

    pub(crate) fn hex(s: &str) -> Vec<u8> {
        s.split_whitespace().map(|b| u8::from_str_radix(b, 16).unwrap()).collect()
    }

    fn v1_ops() -> Vec<Op> {
        vec![Op::Put { cf: "users".into(), key: b"k1".to_vec(), value: b"v1".to_vec(), expire_at_ms: None }]
    }

    #[test]
    fn crc32c_sanity() {
        assert_eq!(crc32c::crc32c(b"123456789"), 0xE306_9283);
    }

    #[test]
    fn vector_v1_ops_bytes_and_crc() {
        let ops = encode_ops(&v1_ops());
        assert_eq!(ops.as_ref(), hex("0a 11 0a 0f 0a 05 75 73 65 72 73 12 02 6b 31 1a 02 76 31").as_slice());
        let e = LogEntry::new(1, 1, 1_700_000_000_000, ops);
        assert_eq!(e.crc32c, 0x7371_7118);
        e.verify_crc().unwrap();
    }

    #[test]
    fn entry_round_trips_through_storage_encoding() {
        let e = LogEntry::new(7, 2, 99, encode_ops(&v1_ops()));
        let back = LogEntry::decode(&e.encode_to_vec()).unwrap();
        assert_eq!(back, e);
        assert_eq!(back.decode_ops().unwrap(), v1_ops());
        assert_eq!(e.encoded_len(), e.encode_to_vec().len());
    }

    #[test]
    fn crc_covers_header_fields() {
        let mut e = LogEntry::new(1, 1, 5, encode_ops(&v1_ops()));
        e.seq = 2;
        assert!(matches!(e.verify_crc(), Err(Error::Corrupt(_))));
    }

    #[test]
    fn decode_rejects_structural_violations() {
        let empty = pb::OpList { ops: vec![] }.encode_to_vec();
        assert!(matches!(decode_ops(&empty), Err(Error::Corrupt(_))));

        let no_kind = pb::OpList { ops: vec![pb::Op { kind: None }] }.encode_to_vec();
        assert!(matches!(decode_ops(&no_kind), Err(Error::Corrupt(_))));

        let mixed = encode_ops(&[
            Op::CreateCf { spec: CfSpec::new("a") },
            Op::Delete { cf: "a".into(), key: vec![] },
        ]);
        assert!(matches!(decode_ops(&mixed), Err(Error::Corrupt(_))));

        let reserved = encode_ops(&[Op::Delete { cf: "__meta".into(), key: vec![] }]);
        assert!(matches!(decode_ops(&reserved), Err(Error::Corrupt(_))));

        assert!(matches!(decode_ops(b"\xff\xff"), Err(Error::Corrupt(_))));
    }

    #[test]
    fn cf_name_rules() {
        validate_cf_name("users").unwrap();
        validate_cf_name("a:b:ü").unwrap();
        validate_cf_name(&"x".repeat(255)).unwrap();
        for bad in ["", "__x", &"x".repeat(256)] {
            assert!(matches!(validate_cf_name(bad), Err(Error::InvalidCfName(_))), "{bad:?}");
        }
    }
}
```

Create `src/codec/envelope.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::entry::tests::hex;

    #[test]
    fn vector_v2_envelopes() {
        assert_eq!(encode(1, None, b"v1"), hex("01 00 00 00 00 00 00 00 00 01 76 31"));
        assert_eq!(
            encode(1, Some(1_700_000_060_000), b"v1"),
            hex("01 01 00 00 00 00 00 00 00 01 00 00 01 8b cf e6 52 60 76 31")
        );
    }

    #[test]
    fn decode_round_trips() {
        let raw = encode(9, Some(50), b"");
        let env = decode(&raw).unwrap();
        assert_eq!(env.meta, ValueMeta { write_seq: 9, expire_at_ms: Some(50) });
        assert_eq!(env.header_len, 18);
        assert_eq!(env.value, b"");
        assert!(!env.is_expired(49));
        assert!(env.is_expired(50));
        assert!(!decode(&encode(9, None, b"x")).unwrap().is_expired(u64::MAX));
    }

    #[test]
    fn decode_rejects_bad_envelopes() {
        let short = &encode(1, None, b"")[..9];
        let mut wrong_ver = encode(1, None, b"");
        wrong_ver[0] = 2;
        let mut reserved = encode(1, None, b"");
        reserved[1] = 0x02;
        let ttl_short = &encode(1, Some(5), b"")[..17];
        for raw in [short, &wrong_ver[..], &reserved[..], ttl_short] {
            assert!(matches!(decode(raw), Err(Error::Corrupt(_))));
        }
    }
}
```

Create `src/codec/digest.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope;

    fn hex32(s: &str) -> [u8; 32] {
        let v: Vec<u8> = (0..32).map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap()).collect();
        v.try_into().unwrap()
    }

    #[test]
    fn vector_v3_digests() {
        let env = envelope::encode(1, None, b"v1");

        let mut boot = Digest::new();
        boot.cf_marker("users", 1);
        boot.record(b"k1", &env);
        assert_eq!(
            boot.finish_bootstrap(1),
            (1, hex32("3aa1d6c512a91d54c3da5ef18a6efef531d61b7d3ec9b37c1ba907832fb22729"))
        );

        let mut quick = Digest::new();
        quick.record(b"k1", &env[..10]);
        assert_eq!(
            quick.finish_range(),
            (1, hex32("2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3"))
        );

        let mut full = Digest::new();
        full.record(b"k1", &env);
        assert_eq!(
            full.finish_range().1,
            hex32("14e556f28a8301390e9be56ecb4ec7ff3ceea3cd48316ab7f7f0488117e53ecd")
        );

        assert_eq!(
            Digest::new().finish_range(),
            (0, hex32("af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262"))
        );
    }
}
```

Create `src/codec/mod.rs`:

```rust
//! Wire and storage encodings shared by every node (PROTOCOL.md §5).

pub mod digest;
pub mod entry;
pub mod envelope;

pub use entry::{entry_crc, validate_cf_name};
pub(crate) use entry::{check_structure, decode_ops, encode_ops};

/// Wire protocol version. Must match exactly between nodes.
pub const PROTOCOL_VERSION: u32 = 1;
/// Value envelope format version. Must match exactly between nodes.
pub const ENVELOPE_VERSION: u8 = 1;
/// Maximum length of a user CF name, in bytes.
pub const MAX_CF_NAME_BYTES: usize = 255;
```

Add to `src/lib.rs`:

```rust
pub mod codec;
pub use codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --lib codec`
Expected: FAIL to compile: `cannot find function encode_ops`, `cannot find type Digest`, `cannot find function encode`.

- [ ] **Step 3: Implement `entry.rs`** (above the test module)

```rust
//! `LogEntry` and `Op` encoding (PROTOCOL.md §5.1–5.2).

use bytes::Bytes;
use prost::Message;

use super::MAX_CF_NAME_BYTES;
use crate::error::{Error, Result};
use crate::pb;
use crate::types::{CfSpec, LogEntry, Op, Seq};

/// CRC-32C over `BE64(seq) ‖ BE64(epoch) ‖ BE64(ts_ms) ‖ ops`.
pub fn entry_crc(seq: Seq, epoch: u64, ts_ms: u64, ops: &[u8]) -> u32 {
    let crc = crc32c::crc32c(&seq.to_be_bytes());
    let crc = crc32c::crc32c_append(crc, &epoch.to_be_bytes());
    let crc = crc32c::crc32c_append(crc, &ts_ms.to_be_bytes());
    crc32c::crc32c_append(crc, ops)
}

impl LogEntry {
    /// Builds an entry and computes its CRC. Only the master creates entries.
    pub fn new(seq: Seq, epoch: u64, ts_ms: u64, ops: Bytes) -> Self {
        let crc32c = entry_crc(seq, epoch, ts_ms, &ops);
        LogEntry { seq, epoch, ts_ms, ops, crc32c }
    }

    pub fn verify_crc(&self) -> Result<()> {
        if entry_crc(self.seq, self.epoch, self.ts_ms, &self.ops) == self.crc32c {
            Ok(())
        } else {
            Err(Error::corrupt(format!("log entry {} failed its CRC check", self.seq)))
        }
    }

    /// Decodes and structurally validates `ops`. Call `verify_crc` first.
    pub fn decode_ops(&self) -> Result<Vec<Op>> {
        decode_ops(&self.ops)
    }

    fn to_pb(&self) -> pb::LogEntry {
        pb::LogEntry {
            seq: self.seq,
            epoch: self.epoch,
            ts_ms: self.ts_ms,
            ops: self.ops.clone(),
            crc32c: self.crc32c,
        }
    }

    pub(crate) fn encode_to_vec(&self) -> Vec<u8> {
        self.to_pb().encode_to_vec()
    }

    pub(crate) fn encoded_len(&self) -> usize {
        self.to_pb().encoded_len()
    }

    pub(crate) fn decode(bytes: &[u8]) -> Result<LogEntry> {
        let p = pb::LogEntry::decode(bytes)
            .map_err(|e| Error::corrupt(format!("undecodable log entry: {e}")))?;
        Ok(LogEntry { seq: p.seq, epoch: p.epoch, ts_ms: p.ts_ms, ops: p.ops, crc32c: p.crc32c })
    }
}

pub(crate) fn encode_ops(ops: &[Op]) -> Bytes {
    let list = pb::OpList { ops: ops.iter().map(op_to_pb).collect() };
    Bytes::from(list.encode_to_vec())
}

/// Decodes an `OpList`. Any violation of PROTOCOL.md §5.2 rules 1–3 is `Corrupt`.
pub(crate) fn decode_ops(bytes: &[u8]) -> Result<Vec<Op>> {
    let list = pb::OpList::decode(bytes)
        .map_err(|e| Error::corrupt(format!("undecodable operation list: {e}")))?;
    let ops = list.ops.into_iter().map(op_from_pb).collect::<Result<Vec<_>>>()?;
    check_structure(&ops).map_err(|e| match e {
        Error::Corrupt(m) => Error::Corrupt(m),
        other => Error::Corrupt(other.to_string()),
    })?;
    Ok(ops)
}

/// PROTOCOL.md §5.2 rules 1–3: non-empty, CF ops alone, valid CF names.
pub(crate) fn check_structure(ops: &[Op]) -> Result<()> {
    if ops.is_empty() {
        return Err(Error::EmptyBatch);
    }
    let cf_ops = ops.iter().filter(|o| matches!(o, Op::CreateCf { .. } | Op::DropCf { .. })).count();
    if cf_ops > 0 && ops.len() > 1 {
        return Err(Error::corrupt("a column family operation must be the only operation in its entry"));
    }
    ops.iter().try_for_each(|op| validate_cf_name(op.cf_name()))
}

pub fn validate_cf_name(name: &str) -> Result<()> {
    if name.is_empty() || name.len() > MAX_CF_NAME_BYTES || name.starts_with("__") {
        Err(Error::InvalidCfName(name.to_string()))
    } else {
        Ok(())
    }
}

fn op_to_pb(op: &Op) -> pb::Op {
    use pb::op::Kind;
    let kind = match op {
        Op::Put { cf, key, value, expire_at_ms } => Kind::Put(pb::Put {
            cf: cf.clone(),
            key: key.clone(),
            value: value.clone(),
            expire_at_ms: *expire_at_ms,
        }),
        Op::Delete { cf, key } => Kind::Delete(pb::Delete { cf: cf.clone(), key: key.clone() }),
        Op::DeleteRange { cf, start, end } => Kind::DeleteRange(pb::DeleteRange {
            cf: cf.clone(),
            start: start.clone(),
            end: end.clone(),
        }),
        Op::CreateCf { spec } => Kind::CreateCf(pb::CreateCf {
            name: spec.name.clone(),
            comparator: spec.comparator.clone(),
        }),
        Op::DropCf { name } => Kind::DropCf(pb::DropCf { name: name.clone() }),
    };
    pb::Op { kind: Some(kind) }
}

fn op_from_pb(op: pb::Op) -> Result<Op> {
    use pb::op::Kind;
    Ok(match op.kind.ok_or_else(|| Error::corrupt("operation with no kind"))? {
        Kind::Put(p) => Op::Put { cf: p.cf, key: p.key, value: p.value, expire_at_ms: p.expire_at_ms },
        Kind::Delete(d) => Op::Delete { cf: d.cf, key: d.key },
        Kind::DeleteRange(r) => Op::DeleteRange { cf: r.cf, start: r.start, end: r.end },
        Kind::CreateCf(c) => Op::CreateCf { spec: CfSpec { name: c.name, comparator: c.comparator } },
        Kind::DropCf(d) => Op::DropCf { name: d.name },
    })
}
```

- [ ] **Step 4: Implement `envelope.rs`** (above the test module)

```rust
//! Value envelope (PROTOCOL.md §5.3).

use super::ENVELOPE_VERSION;
use crate::error::{Error, Result};
use crate::types::{Seq, ValueMeta};

pub const FLAG_TTL: u8 = 0x01;
const BASE_HEADER: usize = 10;
const TTL_HEADER: usize = 18;

pub fn encode(write_seq: Seq, expire_at_ms: Option<u64>, value: &[u8]) -> Vec<u8> {
    let header = if expire_at_ms.is_some() { TTL_HEADER } else { BASE_HEADER };
    let mut out = Vec::with_capacity(header + value.len());
    out.push(ENVELOPE_VERSION);
    out.push(if expire_at_ms.is_some() { FLAG_TTL } else { 0 });
    out.extend_from_slice(&write_seq.to_be_bytes());
    if let Some(at) = expire_at_ms {
        out.extend_from_slice(&at.to_be_bytes());
    }
    out.extend_from_slice(value);
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Envelope<'a> {
    pub meta: ValueMeta,
    /// 10, or 18 with TTL. `raw[..header_len]` is the Quick-verify payload.
    pub header_len: usize,
    pub value: &'a [u8],
}

impl Envelope<'_> {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.meta.expire_at_ms.is_some_and(|at| at <= now_ms)
    }
}

pub fn decode(raw: &[u8]) -> Result<Envelope<'_>> {
    if raw.len() < BASE_HEADER {
        return Err(Error::corrupt("value envelope is shorter than its header"));
    }
    if raw[0] != ENVELOPE_VERSION {
        return Err(Error::corrupt(format!("unknown envelope version {}", raw[0])));
    }
    let flags = raw[1];
    if flags & !FLAG_TTL != 0 {
        return Err(Error::corrupt(format!("reserved envelope flag bits set: {flags:#04x}")));
    }
    let write_seq = be64(&raw[2..10]);
    let (expire_at_ms, header_len) = if flags & FLAG_TTL != 0 {
        if raw.len() < TTL_HEADER {
            return Err(Error::corrupt("value envelope is shorter than its TTL header"));
        }
        (Some(be64(&raw[10..18])), TTL_HEADER)
    } else {
        (None, BASE_HEADER)
    };
    Ok(Envelope { meta: ValueMeta { write_seq, expire_at_ms }, header_len, value: &raw[header_len..] })
}

fn be64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    u64::from_be_bytes(a)
}
```

- [ ] **Step 5: Implement `digest.rs`** (above the test module)

```rust
//! BLAKE3 digest framing for Bootstrap and Verify (PROTOCOL.md §5.4).

use crate::types::Seq;

pub struct Digest {
    hasher: blake3::Hasher,
    records: u64,
}

impl Default for Digest {
    fn default() -> Self {
        Self::new()
    }
}

impl Digest {
    pub fn new() -> Self {
        Digest { hasher: blake3::Hasher::new(), records: 0 }
    }

    /// `0x01 ‖ BE64(len(name)) ‖ name ‖ BE64(create_seq)`
    pub fn cf_marker(&mut self, name: &str, create_seq: Seq) {
        self.hasher.update(&[0x01]);
        self.hasher.update(&(name.len() as u64).to_be_bytes());
        self.hasher.update(name.as_bytes());
        self.hasher.update(&create_seq.to_be_bytes());
    }

    /// `0x02 ‖ BE64(len(key)) ‖ key ‖ BE64(len(payload)) ‖ payload`
    pub fn record(&mut self, key: &[u8], payload: &[u8]) {
        self.hasher.update(&[0x02]);
        self.hasher.update(&(key.len() as u64).to_be_bytes());
        self.hasher.update(key);
        self.hasher.update(&(payload.len() as u64).to_be_bytes());
        self.hasher.update(payload);
        self.records += 1;
    }

    pub fn records(&self) -> u64 {
        self.records
    }

    /// Range hash for Verify: records only.
    pub fn finish_range(self) -> (u64, [u8; 32]) {
        (self.records, *self.hasher.finalize().as_bytes())
    }

    /// Bootstrap digest: appends `0x03 ‖ BE64(snapshot_seq) ‖ BE64(record_count)`.
    pub fn finish_bootstrap(mut self, snapshot_seq: Seq) -> (u64, [u8; 32]) {
        self.hasher.update(&[0x03]);
        self.hasher.update(&snapshot_seq.to_be_bytes());
        self.hasher.update(&self.records.to_be_bytes());
        (self.records, *self.hasher.finalize().as_bytes())
    }
}
```

- [ ] **Step 6: Run the tests to confirm they pass**

Run: `cargo test --lib codec`
Expected: PASS, 10 tests.

- [ ] **Step 7: Record the CF name limit in PROTOCOL.md**

In `PROTOCOL.md` §5.2, replace rule 3:

```
3. `cf` and `name` are logical column family (CF) names. They are non-empty, valid UTF-8, and MUST NOT start with `__`.
```

with:

```
3. `cf` and `name` are logical column family (CF) names. They are 1–255 bytes of valid UTF-8 and MUST NOT start with `__`.
```

Run `cargo test --test proto_sync` to confirm the proto block is unaffected (PASS).

- [ ] **Step 8: Commit**

```bash
git add src PROTOCOL.md
git commit -m "feat: wire codec for log entries, envelopes and digests with spec test vectors

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: RocksDB tuning wrapper and comparator registry

**Files:**
- Create: `src/tuning.rs`
- Modify: `src/lib.rs`, `DESIGN.md` (§16 `rocksdb` field comments, §4 merge rule)

**Interfaces:**
- Consumes: `types::CfSpec`, `Error`.
- Produces:
  - `pub type CompareFn = dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync;`
  - `pub struct ComparatorRegistry` with `register(&mut self, name, f) -> &mut Self`, `contains(&self, &str) -> bool`, `pub(crate) fn compare(&self, name: Option<&str>, a: &[u8], b: &[u8]) -> Ordering`.
  - `pub enum CfKind<'a> { Db, Meta, Changelog, TtlIndex, User(&'a CfSpec) }` (Copy).
  - `pub enum Compression { None, Lz4, Zstd }`.
  - `pub struct Tunable<'a>` with setters `set_write_buffer_size`, `set_max_write_buffer_number`, `set_target_file_size_base`, `set_compression`, `set_max_background_jobs`, `set_max_open_files`, `increase_parallelism`.
  - `pub struct RocksTuning { pub comparators: ComparatorRegistry, .. }` with `new()`, `with_options(f)`, `pub(crate) fn db_options(&self) -> rocksdb::Options`, `pub(crate) fn cf_options(&self, CfKind<'_>) -> Result<rocksdb::Options>`.

- [ ] **Step 1: Write the failing tests**

Create `src/tuning.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering as AtomicOrdering};
    use std::sync::Arc;

    #[test]
    fn compare_uses_registered_comparator_or_bytewise() {
        let mut reg = ComparatorRegistry::default();
        reg.register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        assert!(reg.contains("reverse"));
        assert!(!reg.contains("other"));
        assert_eq!(reg.compare(None, b"a", b"b"), Ordering::Less);
        assert_eq!(reg.compare(Some("reverse"), b"a", b"b"), Ordering::Greater);
    }

    #[test]
    fn unknown_comparator_is_rejected() {
        let t = RocksTuning::new();
        let spec = CfSpec::new("c").with_comparator("missing");
        assert!(matches!(t.cf_options(CfKind::User(&spec)), Err(Error::UnknownComparator(n)) if n == "missing"));
        t.cf_options(CfKind::User(&CfSpec::new("plain"))).unwrap();
    }

    #[test]
    fn options_callback_sees_every_kind() {
        let calls = Arc::new(AtomicUsize::new(0));
        let c = calls.clone();
        let t = RocksTuning::new().with_options(move |_kind, opts| {
            c.fetch_add(1, AtomicOrdering::SeqCst);
            opts.set_write_buffer_size(8 << 20);
            opts.set_compression(Compression::Lz4);
        });
        let _ = t.db_options();
        for kind in [CfKind::Meta, CfKind::Changelog, CfKind::TtlIndex, CfKind::User(&CfSpec::new("u"))] {
            t.cf_options(kind).unwrap();
        }
        assert_eq!(calls.load(AtomicOrdering::SeqCst), 5);
    }
}
```

Add `pub mod tuning;` to `src/lib.rs`.

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --lib tuning`
Expected: FAIL to compile: `cannot find type ComparatorRegistry`.

- [ ] **Step 3: Implement** (above the test module)

```rust
//! RocksDB tuning. Callers get a restricted [`Tunable`] instead of raw `rocksdb::Options`,
//! so merge operators, compaction filters and native TTL cannot be configured at all.

use std::cmp::Ordering;
use std::collections::HashMap;
use std::fmt;
use std::sync::Arc;

use rocksdb::{DBCompressionType, Options};

use crate::error::{Error, Result};
use crate::types::CfSpec;

pub type CompareFn = dyn Fn(&[u8], &[u8]) -> Ordering + Send + Sync;

/// Named key orderings. A name must map to the same ordering on every node, forever.
#[derive(Clone, Default)]
pub struct ComparatorRegistry {
    map: HashMap<String, Arc<CompareFn>>,
}

impl fmt::Debug for ComparatorRegistry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_set().entries(self.map.keys()).finish()
    }
}

impl ComparatorRegistry {
    pub fn register(
        &mut self,
        name: impl Into<String>,
        f: impl Fn(&[u8], &[u8]) -> Ordering + Send + Sync + 'static,
    ) -> &mut Self {
        self.map.insert(name.into(), Arc::new(f));
        self
    }

    pub fn contains(&self, name: &str) -> bool {
        self.map.contains_key(name)
    }

    pub(crate) fn get(&self, name: &str) -> Option<Arc<CompareFn>> {
        self.map.get(name).cloned()
    }

    /// Orders two keys of a CF. `None`, or a name that is not registered, means bytewise;
    /// callers validate names before data reaches this point.
    pub(crate) fn compare(&self, name: Option<&str>, a: &[u8], b: &[u8]) -> Ordering {
        match name.and_then(|n| self.map.get(n)) {
            Some(f) => f(a, b),
            None => a.cmp(b),
        }
    }
}

/// Which RocksDB options a tuning callback is being asked about.
#[derive(Clone, Copy, Debug)]
pub enum CfKind<'a> {
    Db,
    Meta,
    Changelog,
    TtlIndex,
    User(&'a CfSpec),
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Compression {
    None,
    Lz4,
    Zstd,
}

/// The tuning knobs this crate allows. There is deliberately no way to set a merge
/// operator, compaction filter or TTL:
///
/// ```compile_fail
/// fn f(t: &mut replicated_rocksdb::tuning::Tunable<'_>) {
///     t.set_merge_operator_associative("m", |_, _, _| None);
/// }
/// ```
pub struct Tunable<'a> {
    opts: &'a mut Options,
}

impl Tunable<'_> {
    pub fn set_write_buffer_size(&mut self, bytes: usize) {
        self.opts.set_write_buffer_size(bytes);
    }
    pub fn set_max_write_buffer_number(&mut self, n: i32) {
        self.opts.set_max_write_buffer_number(n);
    }
    pub fn set_target_file_size_base(&mut self, bytes: u64) {
        self.opts.set_target_file_size_base(bytes);
    }
    pub fn set_compression(&mut self, c: Compression) {
        self.opts.set_compression_type(match c {
            Compression::None => DBCompressionType::None,
            Compression::Lz4 => DBCompressionType::Lz4,
            Compression::Zstd => DBCompressionType::Zstd,
        });
    }
    pub fn set_max_background_jobs(&mut self, n: i32) {
        self.opts.set_max_background_jobs(n);
    }
    pub fn set_max_open_files(&mut self, n: i32) {
        self.opts.set_max_open_files(n);
    }
    pub fn increase_parallelism(&mut self, threads: i32) {
        self.opts.increase_parallelism(threads);
    }
}

type OptionsFn = dyn Fn(CfKind<'_>, &mut Tunable<'_>) + Send + Sync;

/// RocksDB tuning for one node: named comparators plus an optional options callback.
#[derive(Clone, Default)]
pub struct RocksTuning {
    pub comparators: ComparatorRegistry,
    options: Option<Arc<OptionsFn>>,
}

impl fmt::Debug for RocksTuning {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("RocksTuning")
            .field("comparators", &self.comparators)
            .field("options", &self.options.is_some())
            .finish()
    }
}

impl RocksTuning {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn with_options(mut self, f: impl Fn(CfKind<'_>, &mut Tunable<'_>) + Send + Sync + 'static) -> Self {
        self.options = Some(Arc::new(f));
        self
    }

    pub(crate) fn db_options(&self) -> Options {
        let mut opts = Options::default();
        opts.create_if_missing(true);
        opts.create_missing_column_families(true);
        if let Some(cb) = &self.options {
            cb(CfKind::Db, &mut Tunable { opts: &mut opts });
        }
        opts
    }

    pub(crate) fn cf_options(&self, kind: CfKind<'_>) -> Result<Options> {
        let mut opts = Options::default();
        if let CfKind::User(spec) = kind {
            if let Some(name) = &spec.comparator {
                let f = self.comparators.get(name).ok_or_else(|| Error::UnknownComparator(name.clone()))?;
                opts.set_comparator(name.as_str(), Box::new(move |a: &[u8], b: &[u8]| f(a, b)));
            }
        }
        if let Some(cb) = &self.options {
            cb(kind, &mut Tunable { opts: &mut opts });
        }
        Ok(opts)
    }
}
```

Add to `src/lib.rs`:

```rust
pub use tuning::{CfKind, ComparatorRegistry, Compression, RocksTuning, Tunable};
```

- [ ] **Step 4: Run the tests (including the compile_fail doctest)**

Run: `cargo test --lib tuning && cargo test --doc`
Expected: PASS: 3 unit tests, and 1 doctest (`compile_fail`) that passes because the method does not exist.

- [ ] **Step 5: Update DESIGN.md**

In DESIGN.md §4, replace:

```
- There is no merge. The crate rejects any `Options` that set a merge operator, including options set through the `RocksTuning` callback.
```

with:

```
- There is no merge. `RocksTuning`'s callback receives a restricted `Tunable`, not raw `rocksdb::Options`, so a merge operator, compaction filter or native TTL cannot be set at all.
```

In DESIGN.md §16, replace the `MasterConfig.rocksdb` comment lines:

```
    pub rocksdb: RocksTuning,           // per-CF Options callback + comparator registry;
                                        // merge operators rejected
```

with:

```
    pub rocksdb: RocksTuning,           // comparator registry + callback over a restricted
                                        // `Tunable` (no merge/filter/TTL setters exist)
```

- [ ] **Step 6: Commit**

```bash
git add src DESIGN.md
git commit -m "feat: restricted RocksDB tuning and named comparator registry

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: Store open, `__meta` and the CF registry

**Files:**
- Create: `src/store/mod.rs`, `src/store/meta.rs`, `src/store/cfs.rs`
- Modify: `src/lib.rs`

**Interfaces:**
- Consumes: `RocksTuning::{db_options, cf_options}`, `CfKind`, `types::*`, `codec::{PROTOCOL_VERSION, ENVELOPE_VERSION}`, `local::v1::CfEntryList`, `pb::{CfSpec, Identity}`.
- Produces:
  - `meta::{CF_META, CF_CHANGELOG, CF_TTL_INDEX}`, `meta::key::{ROLE, PROTOCOL_VERSION, ENVELOPE_VERSION, ORIGIN_IDENTITY, LAST_SEQ, LAST_ENTRY_CRC, LAST_TS, EPOCH_HISTORY, CF_SPECS, PENDING_CF_DROPS, CF_CREATE_INTENT}`.
  - `meta::{encode_identity, decode_identity, encode_epoch_history, decode_epoch_history, encode_cf_list, decode_cf_list, encode_cf_entry, decode_cf_entry, be64, be32}`.
  - `meta::MetaSnapshot { role, identity: Option<DbIdentity>, epoch_history: Vec<EpochStart>, last_seq, last_entry_crc: Option<u32>, last_ts, cf_specs: Vec<CfEntry>, pending_drops: Vec<CfEntry>, cf_create_intent: Option<CfEntry> }` with `load(get) -> Result<Option<MetaSnapshot>>`.
  - `cfs::PhysCf { entry: CfEntry, physical: String }`, `cfs::CfRegistry` with `load(&[CfEntry], &[CfEntry])`, `get(&str) -> Option<Arc<PhysCf>>`, `read()`, `write()`, `live_entries() -> Vec<CfEntry>`, `pending_entries() -> Vec<CfEntry>`, `push_pending(Arc<PhysCf>)`.
  - `store::Store` with `pub(crate) fn open(path: &Path, role: Role, tuning: RocksTuning) -> Result<(Store, MetaSnapshot)>`, `role()`, `tuning()`, `position() -> Position`, `set_position(Position)`, `db() -> &Arc<DB>`, `cf(&str) -> Arc<BoundColumnFamily<'_>>` (reserved CFs only), `handle(&PhysCf) -> Result<Arc<BoundColumnFamily<'_>>>`, `live_cf(&str) -> Result<Arc<PhysCf>>`, `write_meta(&[(&[u8], &[u8])], sync: bool) -> Result<()>`, `first_retained_seq() -> Seq`, `set_first_retained_seq(Seq)`.
  - `store::Position { last_seq: Seq, last_entry_crc: Option<u32> }`.

- [ ] **Step 1: Write the failing tests**

Create `src/store/mod.rs` with only the module declarations and tests:

```rust
//! Crate-private RocksDB wrapper shared by master and slave.

pub(crate) mod cfs;
pub(crate) mod meta;

#[cfg(test)]
mod tests {
    use super::*;
    use crate::tuning::RocksTuning;

    #[test]
    fn fresh_store_initialises_meta() {
        let dir = tempfile::tempdir().unwrap();
        let (store, meta) = Store::open(dir.path(), Role::Master, RocksTuning::new()).unwrap();
        assert_eq!(meta.role, Role::Master);
        assert_eq!(meta.last_seq, 0);
        assert_eq!(meta.identity, None);
        assert!(meta.epoch_history.is_empty());
        assert!(meta.cf_specs.is_empty());
        assert_eq!(store.position(), Position { last_seq: 0, last_entry_crc: None });
        assert_eq!(store.first_retained_seq(), 1);
    }

    #[test]
    fn reopen_keeps_meta_and_rejects_wrong_role() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = Store::open(dir.path(), Role::Master, RocksTuning::new()).unwrap();
            store.write_meta(&[(meta::key::LAST_SEQ, &5u64.to_be_bytes())], true).unwrap();
        }
        let (store, meta) = Store::open(dir.path(), Role::Master, RocksTuning::new()).unwrap();
        assert_eq!(meta.last_seq, 5);
        assert_eq!(store.position().last_seq, 5);
        assert_eq!(store.first_retained_seq(), 6);
        drop(store);
        let err = Store::open(dir.path(), Role::Slave, RocksTuning::new()).err().unwrap();
        assert!(matches!(err, Error::WrongRole { expected: Role::Slave, found: Role::Master }));
    }

    #[test]
    fn version_mismatch_is_refused() {
        let dir = tempfile::tempdir().unwrap();
        {
            let (store, _) = Store::open(dir.path(), Role::Master, RocksTuning::new()).unwrap();
            store.write_meta(&[(meta::key::PROTOCOL_VERSION, &99u64.to_be_bytes())], true).unwrap();
        }
        let err = Store::open(dir.path(), Role::Master, RocksTuning::new()).err().unwrap();
        assert!(matches!(err, Error::VersionMismatch { what: "protocol_version", found: 99, expected: 1 }));
    }

    #[test]
    fn meta_encodings_round_trip() {
        let id = DbIdentity { db_uuid: uuid::Uuid::new_v4(), epoch: 3 };
        assert_eq!(meta::decode_identity(&meta::encode_identity(&id)).unwrap(), id);
        let list = vec![
            CfEntry { spec: CfSpec::new("a"), create_seq: 1 },
            CfEntry { spec: CfSpec::new("b").with_comparator("rev"), create_seq: 9 },
        ];
        assert_eq!(meta::decode_cf_list(&meta::encode_cf_list(&list)).unwrap(), list);
        assert_eq!(meta::decode_cf_entry(&meta::encode_cf_entry(&list[1])).unwrap(), list[1]);
        let h = vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 50 }];
        assert_eq!(meta::decode_epoch_history(&meta::encode_epoch_history(&h)).unwrap(), h);
        assert!(meta::be64(b"short").is_err());
    }
}
```

Add `pub(crate) mod store;` to `src/lib.rs`.

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --lib store`
Expected: FAIL to compile: `file not found for module cfs` / `cannot find type Store`.

- [ ] **Step 3: Implement `meta.rs`**

```rust
//! `__meta` keys and value encodings (DESIGN.md §3.2).

use prost::Message;

use crate::error::{Error, Result};
use crate::pb;
use crate::proto::local::v1 as local;
use crate::types::{CfEntry, CfSpec, DbIdentity, EpochStart, Role, Seq};

pub(crate) const CF_META: &str = "__meta";
pub(crate) const CF_CHANGELOG: &str = "__changelog";
pub(crate) const CF_TTL_INDEX: &str = "__ttl_index";

pub(crate) mod key {
    pub const ROLE: &[u8] = b"role";
    pub const PROTOCOL_VERSION: &[u8] = b"protocol_version";
    pub const ENVELOPE_VERSION: &[u8] = b"envelope_version";
    pub const ORIGIN_IDENTITY: &[u8] = b"origin_identity";
    pub const LAST_SEQ: &[u8] = b"last_seq";
    pub const LAST_ENTRY_CRC: &[u8] = b"last_entry_crc";
    pub const LAST_TS: &[u8] = b"last_ts";
    pub const EPOCH_HISTORY: &[u8] = b"epoch_history";
    pub const CF_SPECS: &[u8] = b"cf_specs";
    pub const PENDING_CF_DROPS: &[u8] = b"pending_cf_drops";
    pub const CF_CREATE_INTENT: &[u8] = b"cf_create_intent";
}

pub(crate) fn be64(b: &[u8]) -> Result<u64> {
    let a: [u8; 8] = b.try_into().map_err(|_| Error::corrupt("expected an 8-byte integer in __meta"))?;
    Ok(u64::from_be_bytes(a))
}

pub(crate) fn be32(b: &[u8]) -> Result<u32> {
    let a: [u8; 4] = b.try_into().map_err(|_| Error::corrupt("expected a 4-byte integer in __meta"))?;
    Ok(u32::from_be_bytes(a))
}

pub(crate) fn encode_identity(id: &DbIdentity) -> Vec<u8> {
    pb::Identity { db_uuid: id.db_uuid.as_bytes().to_vec(), epoch: id.epoch }.encode_to_vec()
}

pub(crate) fn decode_identity(b: &[u8]) -> Result<DbIdentity> {
    let p = pb::Identity::decode(b).map_err(|e| Error::corrupt(format!("bad identity in __meta: {e}")))?;
    let db_uuid = uuid::Uuid::from_slice(&p.db_uuid).map_err(|e| Error::corrupt(format!("bad db_uuid: {e}")))?;
    Ok(DbIdentity { db_uuid, epoch: p.epoch })
}

pub(crate) fn encode_epoch_history(h: &[EpochStart]) -> Vec<u8> {
    let epochs = h.iter().map(|e| pb::EpochStart { epoch: e.epoch, start_seq: e.start_seq }).collect();
    local::EpochHistory { epochs }.encode_to_vec()
}

pub(crate) fn decode_epoch_history(b: &[u8]) -> Result<Vec<EpochStart>> {
    let p = local::EpochHistory::decode(b).map_err(|e| Error::corrupt(format!("bad epoch history in __meta: {e}")))?;
    Ok(p.epochs.into_iter().map(|e| EpochStart { epoch: e.epoch, start_seq: e.start_seq }).collect())
}

fn entry_to_pb(e: &CfEntry) -> pb::CfSpec {
    pb::CfSpec { name: e.spec.name.clone(), comparator: e.spec.comparator.clone(), create_seq: e.create_seq }
}

fn entry_from_pb(p: pb::CfSpec) -> CfEntry {
    CfEntry { spec: CfSpec { name: p.name, comparator: p.comparator }, create_seq: p.create_seq }
}

pub(crate) fn encode_cf_list(list: &[CfEntry]) -> Vec<u8> {
    local::CfEntryList { cfs: list.iter().map(entry_to_pb).collect() }.encode_to_vec()
}

pub(crate) fn decode_cf_list(b: &[u8]) -> Result<Vec<CfEntry>> {
    let p = local::CfEntryList::decode(b).map_err(|e| Error::corrupt(format!("bad CF list in __meta: {e}")))?;
    Ok(p.cfs.into_iter().map(entry_from_pb).collect())
}

pub(crate) fn encode_cf_entry(e: &CfEntry) -> Vec<u8> {
    entry_to_pb(e).encode_to_vec()
}

pub(crate) fn decode_cf_entry(b: &[u8]) -> Result<CfEntry> {
    let p = pb::CfSpec::decode(b).map_err(|e| Error::corrupt(format!("bad CF entry in __meta: {e}")))?;
    Ok(entry_from_pb(p))
}

/// Everything in `__meta`, read in one go (live, read-only, or through a snapshot).
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct MetaSnapshot {
    pub role: Role,
    pub protocol_version: u64,
    pub envelope_version: u64,
    pub identity: Option<DbIdentity>,
    /// Empty until an identity is assigned (fresh master, or slave after bootstrap).
    pub epoch_history: Vec<EpochStart>,
    pub last_seq: Seq,
    pub last_entry_crc: Option<u32>,
    pub last_ts: u64,
    pub cf_specs: Vec<CfEntry>,
    pub pending_drops: Vec<CfEntry>,
    pub cf_create_intent: Option<CfEntry>,
}

impl MetaSnapshot {
    /// `Ok(None)` if the store has never been initialised (no `role` key).
    pub(crate) fn load(get: impl Fn(&[u8]) -> Result<Option<Vec<u8>>>) -> Result<Option<MetaSnapshot>> {
        let Some(role) = get(key::ROLE)? else { return Ok(None) };
        let role = Role::from_bytes(&role).ok_or_else(|| Error::corrupt("unknown role in __meta"))?;
        let u64_or = |k: &[u8], default: u64| -> Result<u64> {
            get(k)?.map(|v| be64(&v)).transpose().map(|v| v.unwrap_or(default))
        };
        Ok(Some(MetaSnapshot {
            role,
            protocol_version: u64_or(key::PROTOCOL_VERSION, 0)?,
            envelope_version: u64_or(key::ENVELOPE_VERSION, 0)?,
            identity: get(key::ORIGIN_IDENTITY)?.map(|v| decode_identity(&v)).transpose()?,
            epoch_history: get(key::EPOCH_HISTORY)?.map(|v| decode_epoch_history(&v)).transpose()?.unwrap_or_default(),
            last_seq: u64_or(key::LAST_SEQ, 0)?,
            last_entry_crc: get(key::LAST_ENTRY_CRC)?.map(|v| be32(&v)).transpose()?,
            last_ts: u64_or(key::LAST_TS, 0)?,
            cf_specs: get(key::CF_SPECS)?.map(|v| decode_cf_list(&v)).transpose()?.unwrap_or_default(),
            pending_drops: get(key::PENDING_CF_DROPS)?.map(|v| decode_cf_list(&v)).transpose()?.unwrap_or_default(),
            cf_create_intent: get(key::CF_CREATE_INTENT)?.map(|v| decode_cf_entry(&v)).transpose()?,
        }))
    }
}
```

- [ ] **Step 4: Implement `cfs.rs`**

```rust
//! Registry of live and pending-drop user CFs.
//!
//! Every reader that may touch a physical CF (snapshots, iterators, sessions) holds an
//! `Arc<PhysCf>`. A dropped CF is physically removed only when the registry's own
//! reference is the last one (DESIGN.md §7).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use crate::types::CfEntry;

#[derive(Debug)]
pub(crate) struct PhysCf {
    pub entry: CfEntry,
    pub physical: String,
}

impl PhysCf {
    pub(crate) fn new(entry: CfEntry) -> Arc<PhysCf> {
        let physical = entry.physical_name();
        Arc::new(PhysCf { entry, physical })
    }
}

#[derive(Debug, Default)]
pub(crate) struct CfRegistry {
    live: RwLock<BTreeMap<String, Arc<PhysCf>>>,
    pub(crate) pending: Mutex<Vec<Arc<PhysCf>>>,
}

impl CfRegistry {
    pub(crate) fn load(live: &[CfEntry], pending: &[CfEntry]) -> Self {
        let live = live.iter().map(|e| (e.spec.name.clone(), PhysCf::new(e.clone()))).collect();
        let pending = pending.iter().cloned().map(PhysCf::new).collect();
        CfRegistry { live: RwLock::new(live), pending: Mutex::new(pending) }
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<'_, BTreeMap<String, Arc<PhysCf>>> {
        self.live.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn write(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Arc<PhysCf>>> {
        self.live.write().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn get(&self, name: &str) -> Option<Arc<PhysCf>> {
        self.read().get(name).cloned()
    }

    /// Live CFs sorted by name.
    pub(crate) fn live_entries(&self) -> Vec<CfEntry> {
        self.read().values().map(|p| p.entry.clone()).collect()
    }

    pub(crate) fn pending_entries(&self) -> Vec<CfEntry> {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner).iter().map(|p| p.entry.clone()).collect()
    }

    pub(crate) fn push_pending(&self, cf: Arc<PhysCf>) {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner).push(cf);
    }
}
```

- [ ] **Step 5: Implement `Store` in `store/mod.rs`** (between the module declarations and the tests)

```rust
use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, PoisonError, RwLock};

use rocksdb::{BoundColumnFamily, ColumnFamilyDescriptor, IteratorMode, Options, WriteBatch, WriteOptions, DB};

use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::error::{Error, Result};
use crate::tuning::{CfKind, RocksTuning};
use crate::types::{CfEntry, CfSpec, DbIdentity, EpochStart, Role, Seq};
use cfs::{CfRegistry, PhysCf};
use meta::{key, MetaSnapshot, CF_CHANGELOG, CF_META, CF_TTL_INDEX};

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) struct Position {
    pub last_seq: Seq,
    pub last_entry_crc: Option<u32>,
}

pub(crate) struct Store {
    db: Arc<DB>,
    role: Role,
    tuning: RocksTuning,
    pub(crate) cfs: CfRegistry,
    pos: RwLock<Position>,
    first_retained: AtomicU64,
}

impl Store {
    /// Opens or creates a store. Two-phase: `__meta` is read through a read-only open first,
    /// so every user CF can be reopened with its registered comparator.
    pub(crate) fn open(path: &Path, role: Role, tuning: RocksTuning) -> Result<(Store, MetaSnapshot)> {
        let db_opts = tuning.db_options();
        let existing: Vec<String> =
            if path.join("CURRENT").exists() { DB::list_cf(&Options::default(), path)? } else { Vec::new() };
        let prior = if existing.iter().any(|n| n == CF_META) { read_meta_read_only(path)? } else { None };
        if let Some(m) = &prior {
            check_compatible(m, role)?;
        }

        let mut descriptors = vec![
            ColumnFamilyDescriptor::new(CF_META, tuning.cf_options(CfKind::Meta)?),
            ColumnFamilyDescriptor::new(CF_CHANGELOG, tuning.cf_options(CfKind::Changelog)?),
            ColumnFamilyDescriptor::new(CF_TTL_INDEX, tuning.cf_options(CfKind::TtlIndex)?),
        ];
        let known: HashMap<String, CfEntry> = prior
            .iter()
            .flat_map(|m| m.cf_specs.iter().chain(&m.pending_drops).chain(&m.cf_create_intent))
            .map(|e| (e.physical_name(), e.clone()))
            .collect();
        for name in existing.iter().filter(|n| n.starts_with("u:")) {
            let entry = known
                .get(name)
                .ok_or_else(|| Error::corrupt(format!("physical column family {name} is not in __meta")))?;
            descriptors.push(ColumnFamilyDescriptor::new(name, tuning.cf_options(CfKind::User(&entry.spec))?));
        }
        let db = Arc::new(DB::open_cf_descriptors(&db_opts, path, descriptors)?);

        let mut store = Store {
            db,
            role,
            tuning,
            cfs: CfRegistry::default(),
            pos: RwLock::new(Position::default()),
            first_retained: AtomicU64::new(1),
        };
        let meta = match prior {
            Some(m) => m,
            None => store.initialise_meta()?,
        };
        let meta = store.recover_cf_state(meta, &existing)?;
        store.cfs = CfRegistry::load(&meta.cf_specs, &meta.pending_drops);
        store.set_position(Position { last_seq: meta.last_seq, last_entry_crc: meta.last_entry_crc });
        let first = store.first_changelog_seq()?.unwrap_or(meta.last_seq + 1);
        store.set_first_retained_seq(first);
        Ok((store, meta))
    }

    fn initialise_meta(&self) -> Result<MetaSnapshot> {
        self.write_meta(
            &[
                (key::ROLE, self.role.as_bytes()),
                (key::PROTOCOL_VERSION, &u64::from(PROTOCOL_VERSION).to_be_bytes()),
                (key::ENVELOPE_VERSION, &u64::from(ENVELOPE_VERSION).to_be_bytes()),
                (key::LAST_SEQ, &0u64.to_be_bytes()),
                (key::CF_SPECS, &meta::encode_cf_list(&[])),
                (key::PENDING_CF_DROPS, &meta::encode_cf_list(&[])),
            ],
            true,
        )?;
        self.load_meta()?.ok_or_else(|| Error::corrupt("__meta missing right after initialisation"))
    }

    /// Finishes CF operations interrupted by a crash (DESIGN.md §7). Extended in Task 6.
    fn recover_cf_state(&self, meta: MetaSnapshot, _existing: &[String]) -> Result<MetaSnapshot> {
        Ok(meta)
    }

    pub(crate) fn load_meta(&self) -> Result<Option<MetaSnapshot>> {
        let cf = self.cf(CF_META);
        MetaSnapshot::load(|k| Ok(self.db.get_cf(&cf, k)?))
    }

    pub(crate) fn role(&self) -> Role {
        self.role
    }

    pub(crate) fn tuning(&self) -> &RocksTuning {
        &self.tuning
    }

    pub(crate) fn db(&self) -> &Arc<DB> {
        &self.db
    }

    pub(crate) fn position(&self) -> Position {
        *self.pos.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn set_position(&self, p: Position) {
        *self.pos.write().unwrap_or_else(PoisonError::into_inner) = p;
    }

    pub(crate) fn first_retained_seq(&self) -> Seq {
        self.first_retained.load(Ordering::SeqCst)
    }

    pub(crate) fn set_first_retained_seq(&self, seq: Seq) {
        self.first_retained.store(seq, Ordering::SeqCst);
    }

    /// Handle of a reserved CF. These are always open, so a miss is a programming error.
    pub(crate) fn cf(&self, name: &'static str) -> Arc<BoundColumnFamily<'_>> {
        self.db.cf_handle(name).unwrap_or_else(|| panic!("reserved column family {name} is not open"))
    }

    pub(crate) fn handle(&self, cf: &PhysCf) -> Result<Arc<BoundColumnFamily<'_>>> {
        self.db
            .cf_handle(&cf.physical)
            .ok_or_else(|| Error::corrupt(format!("physical column family {} is missing", cf.physical)))
    }

    pub(crate) fn live_cf(&self, name: &str) -> Result<Arc<PhysCf>> {
        self.cfs.get(name).ok_or_else(|| Error::NoSuchCf(name.to_string()))
    }

    pub(crate) fn write_meta(&self, kvs: &[(&[u8], &[u8])], sync: bool) -> Result<()> {
        let cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        for (k, v) in kvs {
            batch.put_cf(&cf, k, v);
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        Ok(self.db.write_opt(batch, &wo)?)
    }

    fn first_changelog_seq(&self) -> Result<Option<Seq>> {
        let cf = self.cf(CF_CHANGELOG);
        match self.db.iterator_cf(&cf, IteratorMode::Start).next() {
            Some(item) => Ok(Some(meta::be64(&item?.0)?)),
            None => Ok(None),
        }
    }
}

fn read_meta_read_only(path: &Path) -> Result<Option<MetaSnapshot>> {
    let db = DB::open_cf_descriptors_read_only(
        &Options::default(),
        path,
        [ColumnFamilyDescriptor::new(CF_META, Options::default())],
        false,
    )?;
    let cf = db.cf_handle(CF_META).ok_or_else(|| Error::corrupt("__meta missing in read-only open"))?;
    MetaSnapshot::load(|k| Ok(db.get_cf(&cf, k)?))
}

fn check_compatible(m: &MetaSnapshot, role: Role) -> Result<()> {
    if m.role != role {
        return Err(Error::WrongRole { expected: role, found: m.role });
    }
    if m.protocol_version != u64::from(PROTOCOL_VERSION) {
        return Err(Error::VersionMismatch {
            what: "protocol_version",
            found: m.protocol_version,
            expected: PROTOCOL_VERSION.into(),
        });
    }
    if m.envelope_version != u64::from(ENVELOPE_VERSION) {
        return Err(Error::VersionMismatch {
            what: "envelope_version",
            found: m.envelope_version,
            expected: ENVELOPE_VERSION.into(),
        });
    }
    Ok(())
}
```

Note: `DbIdentity`, `CfSpec` and `EpochStart` are imported for the tests (`use super::*`). If clippy flags them as unused outside tests, move them into the test module's imports.

- [ ] **Step 6: Run the tests to confirm they pass**

Run: `cargo test --lib store`
Expected: PASS, 4 tests.

- [ ] **Step 7: Commit**

```bash
git add src
git commit -m "feat: store open with two-phase meta read, role and version checks

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Applying entries — `prepare`/`commit`, CF lifecycle and crash recovery

**Files:**
- Create: `src/store/apply.rs`, `src/store/test_support.rs`
- Modify: `src/store/mod.rs` (module list, `recover_cf_state`), `src/store/cfs.rs` (`drop_unreferenced_cfs`), `DESIGN.md` (§3.2, §7)

**Interfaces:**
- Consumes: `Store` (Task 5), `codec::{check_structure, envelope, validate_cf_name}`, `ComparatorRegistry::compare`.
- Produces:
  - `pub(crate) struct Prepared { pub batch: WriteBatch, pub seq: Seq, pub crc: u32, cf_change: Option<CfChange> }`.
  - `Store::prepare(&self, entry: &LogEntry, ops: &[Op]) -> Result<Prepared>`. Validates and has no side effects. Errors: `EmptyBatch`, `InvalidCfName`, `NoSuchCf`, `AlreadyExists`, `UnknownComparator`, `InvalidRange`, `Corrupt` (seq out of order, CF op not alone).
  - `Store::commit(&self, p: Prepared, sync: bool) -> Result<()>`.
  - `Store::drop_unreferenced_cfs(&self) -> Result<usize>`.
  - `test_support::{open_store(&Path) -> Store, open_store_with(&Path, RocksTuning) -> Store, apply(&Store, Vec<Op>) -> Result<Seq>}` (cfg(test)).

- [ ] **Step 1: Write the failing tests**

Create `src/store/test_support.rs`:

```rust
//! Helpers for store unit tests.

use std::path::Path;

use super::Store;
use crate::codec::encode_ops;
use crate::error::Result;
use crate::tuning::RocksTuning;
use crate::types::{LogEntry, Op, Role, Seq};

pub(crate) fn open_store(dir: &Path) -> Store {
    open_store_with(dir, RocksTuning::new())
}

pub(crate) fn open_store_with(dir: &Path, tuning: RocksTuning) -> Store {
    Store::open(dir, Role::Master, tuning).unwrap().0
}

/// Applies `ops` as the next entry (epoch 1, ts = 1000 × seq), without fsync.
pub(crate) fn apply(store: &Store, ops: Vec<Op>) -> Result<Seq> {
    let seq = store.position().last_seq + 1;
    let entry = LogEntry::new(seq, 1, 1_000 * seq, encode_ops(&ops));
    let prepared = store.prepare(&entry, &ops)?;
    store.commit(prepared, false)?;
    Ok(seq)
}
```

Create `src/store/apply.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use crate::codec::envelope;
    use crate::error::Error;
    use crate::store::meta::{key, CF_CHANGELOG};
    use crate::store::test_support::{apply, open_store, open_store_with};
    use crate::tuning::RocksTuning;
    use crate::types::{CfSpec, LogEntry, Op};

    fn put(cf: &str, k: &[u8], v: &[u8]) -> Op {
        Op::Put { cf: cf.into(), key: k.to_vec(), value: v.to_vec(), expire_at_ms: None }
    }
    fn create(name: &str) -> Op {
        Op::CreateCf { spec: CfSpec::new(name) }
    }
    fn raw(store: &crate::store::Store, cf: &str, k: &[u8]) -> Option<Vec<u8>> {
        let phys = store.live_cf(cf).unwrap();
        store.db().get_cf(&store.handle(&phys).unwrap(), k).unwrap()
    }

    #[test]
    fn put_writes_envelope_changelog_and_position_atomically() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        assert_eq!(apply(&store, vec![create("users")]).unwrap(), 1);
        assert_eq!(apply(&store, vec![put("users", b"k", b"v")]).unwrap(), 2);

        let env = raw(&store, "users", b"k").unwrap();
        assert_eq!(envelope::decode(&env).unwrap().meta.write_seq, 2);
        let cl = store.db().get_cf(&store.cf(CF_CHANGELOG), 2u64.to_be_bytes()).unwrap().unwrap();
        let entry = LogEntry::decode(&cl).unwrap();
        assert_eq!(entry.seq, 2);
        assert_eq!(store.position().last_seq, 2);
        assert_eq!(store.position().last_entry_crc, Some(entry.crc32c));
        let meta = store.load_meta().unwrap().unwrap();
        assert_eq!(meta.last_seq, 2);
        assert_eq!(meta.last_entry_crc, Some(entry.crc32c));
        assert_eq!(meta.cf_specs.len(), 1);
    }

    #[test]
    fn empty_keys_and_values_work() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        apply(&store, vec![put("c", b"", b"")]).unwrap();
        let env = raw(&store, "c", b"").unwrap();
        assert_eq!(envelope::decode(&env).unwrap().value, b"");
        apply(&store, vec![Op::DeleteRange { cf: "c".into(), start: b"".to_vec(), end: b"a".to_vec() }]).unwrap();
        assert_eq!(raw(&store, "c", b""), None);
    }

    #[test]
    fn nul_empty_and_large_keys_and_values_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let big_key = vec![0xABu8; 1 << 20];
        let big_val: Vec<u8> = (0..8usize << 20).map(|i| (i % 251) as u8).collect();
        let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
            (b"".to_vec(), b"".to_vec()),
            (b"\0".to_vec(), b"\0".to_vec()),
            (b"\0\0k\0".to_vec(), b"v\0\0".to_vec()),
            (vec![0u8; 64], vec![0u8; 64]),
            (big_key, b"small".to_vec()),
            (b"big-value".to_vec(), big_val),
        ];
        {
            let store = open_store(dir.path());
            apply(&store, vec![create("c")]).unwrap();
            for (k, v) in &cases {
                apply(&store, vec![put("c", k, v)]).unwrap();
            }
        }
        let store = open_store(dir.path());
        for (k, v) in &cases {
            let env = raw(&store, "c", k).unwrap_or_else(|| panic!("missing key of length {}", k.len()));
            assert_eq!(envelope::decode(&env).unwrap().value, v.as_slice(), "key of length {}", k.len());
        }
        // b"" and b"\0" are distinct keys.
        apply(&store, vec![Op::Delete { cf: "c".into(), key: b"".to_vec() }]).unwrap();
        assert!(raw(&store, "c", b"").is_none());
        assert!(raw(&store, "c", b"\0").is_some());
    }

    #[test]
    fn validation_errors_leave_no_trace() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        let cases: Vec<(Vec<Op>, fn(&Error) -> bool)> = vec![
            (vec![], |e| matches!(e, Error::EmptyBatch)),
            (vec![put("missing", b"k", b"v")], |e| matches!(e, Error::NoSuchCf(_))),
            (vec![create("c")], |e| matches!(e, Error::AlreadyExists(_))),
            (vec![create("__x")], |e| matches!(e, Error::InvalidCfName(_))),
            (vec![Op::CreateCf { spec: CfSpec::new("d").with_comparator("nope") }], |e| {
                matches!(e, Error::UnknownComparator(_))
            }),
            (vec![Op::DeleteRange { cf: "c".into(), start: b"b".to_vec(), end: b"a".to_vec() }], |e| {
                matches!(e, Error::InvalidRange)
            }),
            (vec![Op::DeleteRange { cf: "c".into(), start: b"a".to_vec(), end: b"a".to_vec() }], |e| {
                matches!(e, Error::InvalidRange)
            }),
            (vec![Op::DropCf { name: "missing".into() }], |e| matches!(e, Error::NoSuchCf(_))),
            (vec![create("e"), put("c", b"k", b"v")], |e| matches!(e, Error::Corrupt(_))),
        ];
        for (ops, expected) in cases {
            let err = apply(&store, ops.clone()).unwrap_err();
            assert!(expected(&err), "{ops:?} gave {err:?}");
        }
        assert_eq!(store.position().last_seq, 1);
    }

    #[test]
    fn out_of_order_entry_is_corrupt() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let ops = vec![create("c")];
        let entry = LogEntry::new(5, 1, 0, crate::codec::encode_ops(&ops));
        assert!(matches!(store.prepare(&entry, &ops), Err(Error::Corrupt(_))));
    }

    #[test]
    fn drop_is_deferred_while_referenced_then_physical() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        apply(&store, vec![put("c", b"k", b"v")]).unwrap();
        let held = store.live_cf("c").unwrap();
        apply(&store, vec![Op::DropCf { name: "c".into() }]).unwrap();

        assert!(store.live_cf("c").is_err());
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0);
        assert!(store.db().cf_handle("u:c:1").is_some(), "still referenced");
        assert_eq!(store.load_meta().unwrap().unwrap().pending_drops.len(), 1);

        drop(held);
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
        assert!(store.db().cf_handle("u:c:1").is_none());
        assert!(store.load_meta().unwrap().unwrap().pending_drops.is_empty());
    }

    #[test]
    fn recreate_after_drop_uses_new_physical_cf() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![create("c")]).unwrap();
        apply(&store, vec![put("c", b"k", b"old")]).unwrap();
        apply(&store, vec![Op::DropCf { name: "c".into() }]).unwrap();
        assert_eq!(apply(&store, vec![create("c")]).unwrap(), 4);
        assert_eq!(store.live_cf("c").unwrap().physical, "u:c:4");
        assert_eq!(raw(&store, "c", b"k"), None);
    }

    #[test]
    fn odd_cf_names_survive_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            apply(&store, vec![create("a:b:1")]).unwrap();
            apply(&store, vec![create("ünï")]).unwrap();
            apply(&store, vec![put("a:b:1", b"k", b"v")]).unwrap();
        }
        let store = open_store(dir.path());
        assert!(raw(&store, "a:b:1", b"k").is_some());
        assert_eq!(store.live_cf("ünï").unwrap().physical, "u:ünï:2");
        apply(&store, vec![Op::DropCf { name: "a:b:1".into() }]).unwrap();
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
    }

    #[test]
    fn custom_comparator_orders_ranges_and_is_required_on_reopen() {
        let dir = tempfile::tempdir().unwrap();
        let mut tuning = RocksTuning::new();
        tuning.comparators.register("reverse", |a: &[u8], b: &[u8]| b.cmp(a));
        {
            let store = open_store_with(dir.path(), tuning.clone());
            apply(&store, vec![Op::CreateCf { spec: CfSpec::new("r").with_comparator("reverse") }]).unwrap();
            // Under "reverse", b"b" sorts before b"a", so [b, a) is a valid range.
            apply(&store, vec![Op::DeleteRange { cf: "r".into(), start: b"b".to_vec(), end: b"a".to_vec() }])
                .unwrap();
        }
        let err = crate::store::Store::open(dir.path(), crate::types::Role::Master, RocksTuning::new()).err();
        assert!(matches!(err, Some(Error::UnknownComparator(n)) if n == "reverse"));
        let store = open_store_with(dir.path(), tuning);
        assert!(store.live_cf("r").is_ok());
    }

    #[test]
    fn interrupted_create_is_cleaned_up_on_open() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            // Simulate a crash after the intent and the physical CF, before the commit batch.
            let intent = crate::types::CfEntry { spec: CfSpec::new("x"), create_seq: 1 };
            store
                .write_meta(&[(key::CF_CREATE_INTENT, &crate::store::meta::encode_cf_entry(&intent))], true)
                .unwrap();
            store.db().create_cf("u:x:1", &rocksdb::Options::default()).unwrap();
        }
        let store = open_store(dir.path());
        assert!(store.db().cf_handle("u:x:1").is_none());
        assert!(store.load_meta().unwrap().unwrap().cf_create_intent.is_none());
        assert_eq!(apply(&store, vec![create("x")]).unwrap(), 1);
    }
}
```

In `src/store/mod.rs`, add to the module list:

```rust
pub(crate) mod apply;
#[cfg(test)]
pub(crate) mod test_support;
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --lib store::apply`
Expected: FAIL to compile: `no method named prepare found for reference &Store`.

- [ ] **Step 3: Implement `apply.rs`** (above the test module)

```rust
//! Turning one `LogEntry` into one atomic `WriteBatch` (DESIGN.md §5, §6, §7).

use std::cmp::Ordering;
use std::sync::Arc;

use rocksdb::{WriteBatch, WriteOptions};

use super::cfs::PhysCf;
use super::meta::{self, key, CF_CHANGELOG, CF_META};
use super::{Position, Store};
use crate::codec::{check_structure, envelope};
use crate::error::{Error, Result};
use crate::tuning::CfKind;
use crate::types::{CfEntry, LogEntry, Op, Seq};

pub(crate) enum CfChange {
    Create(CfEntry),
    Drop(Arc<PhysCf>),
}

/// A validated entry ready to commit. Building one has no side effects.
pub(crate) struct Prepared {
    pub batch: WriteBatch,
    pub seq: Seq,
    pub crc: u32,
    cf_change: Option<CfChange>,
}

impl Store {
    pub(crate) fn prepare(&self, entry: &LogEntry, ops: &[Op]) -> Result<Prepared> {
        let last = self.position().last_seq;
        if entry.seq != last + 1 {
            return Err(Error::corrupt(format!("entry {} does not follow last_seq {last}", entry.seq)));
        }
        check_structure(ops)?;

        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        let mut cf_change = None;
        let live = self.cfs.read();
        let lookup = |name: &str| live.get(name).cloned().ok_or_else(|| Error::NoSuchCf(name.to_string()));

        for op in ops {
            match op {
                Op::Put { cf, key, value, expire_at_ms } => {
                    let phys = lookup(cf)?;
                    batch.put_cf(&self.handle(&phys)?, key, envelope::encode(entry.seq, *expire_at_ms, value));
                }
                Op::Delete { cf, key } => {
                    let phys = lookup(cf)?;
                    batch.delete_cf(&self.handle(&phys)?, key);
                }
                Op::DeleteRange { cf, start, end } => {
                    let phys = lookup(cf)?;
                    let cmp = self.tuning().comparators.compare(phys.entry.spec.comparator.as_deref(), start, end);
                    if cmp != Ordering::Less {
                        return Err(Error::InvalidRange);
                    }
                    batch.delete_range_cf(&self.handle(&phys)?, start, end);
                }
                Op::CreateCf { spec } => {
                    if live.contains_key(&spec.name) {
                        return Err(Error::AlreadyExists(spec.name.clone()));
                    }
                    if let Some(c) = &spec.comparator {
                        if !self.tuning().comparators.contains(c) {
                            return Err(Error::UnknownComparator(c.clone()));
                        }
                    }
                    let new = CfEntry { spec: spec.clone(), create_seq: entry.seq };
                    let mut specs: Vec<CfEntry> = live.values().map(|p| p.entry.clone()).collect();
                    specs.push(new.clone());
                    specs.sort_by(|a, b| a.spec.name.cmp(&b.spec.name));
                    batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(&specs));
                    batch.delete_cf(&meta_cf, key::CF_CREATE_INTENT);
                    cf_change = Some(CfChange::Create(new));
                }
                Op::DropCf { name } => {
                    let phys = lookup(name)?;
                    let specs: Vec<CfEntry> =
                        live.values().filter(|p| p.entry.spec.name != *name).map(|p| p.entry.clone()).collect();
                    let mut pending = self.cfs.pending_entries();
                    pending.push(phys.entry.clone());
                    batch.put_cf(&meta_cf, key::CF_SPECS, meta::encode_cf_list(&specs));
                    batch.put_cf(&meta_cf, key::PENDING_CF_DROPS, meta::encode_cf_list(&pending));
                    cf_change = Some(CfChange::Drop(phys));
                }
            }
        }
        drop(live);

        batch.put_cf(&self.cf(CF_CHANGELOG), entry.seq.to_be_bytes(), entry.encode_to_vec());
        batch.put_cf(&meta_cf, key::LAST_SEQ, entry.seq.to_be_bytes());
        batch.put_cf(&meta_cf, key::LAST_ENTRY_CRC, entry.crc32c.to_be_bytes());
        Ok(Prepared { batch, seq: entry.seq, crc: entry.crc32c, cf_change })
    }

    /// Writes a prepared entry. CF changes hold the registry write lock across the write,
    /// so snapshots always see a CF list that matches their data.
    pub(crate) fn commit(&self, p: Prepared, sync: bool) -> Result<()> {
        let mut wo = WriteOptions::default();
        wo.set_sync(sync);
        match p.cf_change {
            Some(CfChange::Create(entry)) => {
                let mut live = self.cfs.write();
                // Record the intent durably first: a crash after create_cf leaves a CF whose
                // comparator would otherwise be unknown at the next open.
                self.write_meta(&[(key::CF_CREATE_INTENT, &meta::encode_cf_entry(&entry))], true)?;
                let physical = entry.physical_name();
                if self.db().cf_handle(&physical).is_none() {
                    self.db().create_cf(&physical, &self.tuning().cf_options(CfKind::User(&entry.spec))?)?;
                }
                self.db().write_opt(p.batch, &wo)?;
                live.insert(entry.spec.name.clone(), PhysCf::new(entry));
            }
            Some(CfChange::Drop(phys)) => {
                let mut live = self.cfs.write();
                self.db().write_opt(p.batch, &wo)?;
                live.remove(&phys.entry.spec.name);
                self.cfs.push_pending(phys);
            }
            None => self.db().write_opt(p.batch, &wo)?,
        }
        self.set_position(Position { last_seq: p.seq, last_entry_crc: Some(p.crc) });
        Ok(())
    }
}
```

- [ ] **Step 4: Implement the physical drop in `cfs.rs`**

Append to `src/store/cfs.rs`:

```rust
use std::sync::PoisonError as _PoisonError;

use super::meta::{self, key};
use super::Store;
use crate::error::Result;

impl Store {
    /// Physically drops pending CFs that nothing references any more. Returns how many.
    pub(crate) fn drop_unreferenced_cfs(&self) -> Result<usize> {
        let mut pending = self.cfs.pending.lock().unwrap_or_else(_PoisonError::into_inner);
        let mut dropped = 0;
        let mut i = 0;
        while i < pending.len() {
            // Pending CFs are not in the live map, so no new references can appear;
            // a count of 1 (ours) means no reader holds it.
            if Arc::strong_count(&pending[i]) == 1 {
                self.db().drop_cf(&pending[i].physical)?;
                pending.swap_remove(i);
                dropped += 1;
            } else {
                i += 1;
            }
        }
        if dropped > 0 {
            let remaining: Vec<CfEntry> = pending.iter().map(|p| p.entry.clone()).collect();
            self.write_meta(&[(key::PENDING_CF_DROPS, &meta::encode_cf_list(&remaining))], true)?;
        }
        Ok(dropped)
    }
}
```

(Merge the new `use` lines into the file's existing imports and drop the alias if `PoisonError` is already imported.)

- [ ] **Step 5: Implement crash recovery in `Store::open`**

In `src/store/mod.rs`, replace the `recover_cf_state` stub with:

```rust
    /// Finishes CF operations interrupted by a crash (DESIGN.md §7):
    /// an unregistered CF named by the create intent is dropped (replay recreates it), and
    /// pending drops whose physical CF is already gone are forgotten.
    fn recover_cf_state(&self, mut meta: MetaSnapshot, existing: &[String]) -> Result<MetaSnapshot> {
        if let Some(intent) = meta.cf_create_intent.take() {
            let physical = intent.physical_name();
            let registered = meta.cf_specs.contains(&intent);
            if !registered && self.db.cf_handle(&physical).is_some() {
                self.db.drop_cf(&physical)?;
            }
            let cf = self.cf(CF_META);
            self.db.delete_cf(&cf, key::CF_CREATE_INTENT)?;
            self.db.flush_wal(true)?;
        }
        let before = meta.pending_drops.len();
        meta.pending_drops.retain(|e| existing.contains(&e.physical_name()));
        if meta.pending_drops.len() != before {
            self.write_meta(&[(key::PENDING_CF_DROPS, &meta::encode_cf_list(&meta.pending_drops))], true)?;
        }
        Ok(meta)
    }
```

- [ ] **Step 6: Run the tests to confirm they pass**

Run: `cargo test --lib store`
Expected: PASS, 14 tests.

- [ ] **Step 7: Update DESIGN.md**

In the DESIGN.md §3.2 table, add a row after `pending_cf_drops`:

```
| `cf_create_intent` | all | the `CfEntry` being created; written with fsync before the physical CF is created, cleared in the `CreateCf` commit batch |
```

In DESIGN.md §7, replace the `CreateCf` bullet with:

```
- **CreateCf (seq N):** write `__meta.cf_create_intent = (name, comparator, N)` with fsync, create the physical CF `u:{name}:{N}` if it doesn't exist (idempotent), then write one batch: add the spec to `__meta.cf_specs`, clear the intent, write `__changelog[N]`, and set `last_seq = N`. On open, an unregistered CF named by the intent is dropped and the intent cleared; replay (slave) or a retried write (master) recreates it. The intent exists because RocksDB can only reopen a CF with its comparator, and an unregistered CF's comparator would otherwise be unknown. Opening is two-phase: `__meta` is read through a read-only open first, so every user CF is reopened with its registered comparator.
```

- [ ] **Step 8: Commit**

```bash
git add src DESIGN.md
git commit -m "feat: atomic entry apply with CF lifecycle, deferred drops and crash recovery

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Read path — TTL filtering, iterators and `ReadSnapshot`

**Files:**
- Create: `src/store/snapshot.rs`, `src/store/read.rs`, `src/read.rs`
- Modify: `src/store/mod.rs`, `src/lib.rs`, `DESIGN.md` (§16 `ReadSnapshot`, `snapshot()` signatures)

**Interfaces:**
- Consumes: `Store`, `PhysCf`, `envelope::decode`, `IterMode`, `Direction`.
- Produces:
  - `store::snapshot::OwnedSnapshot { new(Arc<DB>), db() -> &DB, read_opts() -> ReadOptions }`.
  - `store::read::ReadCtx<'a> { db: &'a DB, snap: Option<&'a OwnedSnapshot>, now_ms: u64 }` with `get_raw(&PhysCf, &[u8])`, `get(&PhysCf, &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>>`, `iter(Arc<PhysCf>, IterMode<'_>, Option<&[u8]>) -> Result<DbIter<'a>>`.
  - `pub struct DbIter<'a>`: `Iterator<Item = Result<(Vec<u8>, Vec<u8>)>>`.
  - `Store::live_ctx(&self, now_ms) -> ReadCtx<'_>`, `Store::read_snapshot(&self, now_ms) -> Result<ReadSnapshot>`.
  - `pub trait ReadApi { get, get_with_meta, multi_get, iter, prefix_iter, list_cfs }`.
  - `pub struct ReadSnapshot` with `pub fn seq(&self) -> Seq`, and `impl ReadApi for ReadSnapshot`.

- [ ] **Step 1: Write the failing tests**

Create `src/store/read.rs` with only its tests:

```rust
#[cfg(test)]
mod tests {
    use crate::read::ReadApi;
    use crate::store::test_support::{apply, open_store};
    use crate::types::{CfSpec, Direction, IterMode, Op};

    fn put(k: &[u8], v: &[u8], ttl_at: Option<u64>) -> Op {
        Op::Put { cf: "c".into(), key: k.to_vec(), value: v.to_vec(), expire_at_ms: ttl_at }
    }

    fn setup() -> (tempfile::TempDir, crate::store::Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        apply(&store, vec![put(b"a", b"1", None), put(b"b", b"2", Some(100)), put(b"ba", b"3", None)]).unwrap();
        apply(&store, vec![put(b"c", b"4", None), put(b"", b"empty", None)]).unwrap();
        (dir, store)
    }

    fn keys(it: crate::store::read::DbIter<'_>) -> Vec<Vec<u8>> {
        it.map(|r| r.unwrap().0).collect()
    }

    #[test]
    fn get_strips_envelope_and_hides_expired() {
        let (_d, store) = setup();
        let cf = store.live_cf("c").unwrap();
        let (v, meta) = store.live_ctx(99).get(&cf, b"b").unwrap().unwrap();
        assert_eq!(v, b"2");
        assert_eq!(meta.write_seq, 2);
        assert_eq!(meta.expire_at_ms, Some(100));
        assert_eq!(store.live_ctx(100).get(&cf, b"b").unwrap(), None);
        assert_eq!(store.live_ctx(100).get(&cf, b"").unwrap().unwrap().0, b"empty");
        assert_eq!(store.live_ctx(0).get(&cf, b"zz").unwrap(), None);
    }

    #[test]
    fn iterators_respect_mode_prefix_and_expiry() {
        let (_d, store) = setup();
        let cf = store.live_cf("c").unwrap();
        let ctx = store.live_ctx(100);
        assert_eq!(keys(ctx.iter(cf.clone(), IterMode::Start, None).unwrap()), vec![b"".to_vec(), b"a".to_vec(), b"ba".to_vec(), b"c".to_vec()]);
        assert_eq!(keys(ctx.iter(cf.clone(), IterMode::End, None).unwrap()), vec![b"c".to_vec(), b"ba".to_vec(), b"a".to_vec(), b"".to_vec()]);
        assert_eq!(
            keys(ctx.iter(cf.clone(), IterMode::From(b"b", Direction::Forward), None).unwrap()),
            vec![b"ba".to_vec(), b"c".to_vec()]
        );
        assert_eq!(
            keys(ctx.iter(cf.clone(), IterMode::From(b"b", Direction::Forward), Some(b"b")).unwrap()),
            vec![b"ba".to_vec()]
        );
        let at_99 = store.live_ctx(99);
        assert_eq!(keys(at_99.iter(cf, IterMode::From(b"b", Direction::Forward), Some(b"b")).unwrap()).len(), 2);
    }

    #[test]
    fn snapshot_is_isolated_and_fixes_now() {
        let (_d, store) = setup();
        let snap = store.read_snapshot(99).unwrap();
        assert_eq!(snap.seq(), 3);
        apply(&store, vec![put(b"a", b"changed", None)]).unwrap();
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("later") }]).unwrap();
        assert_eq!(snap.get("c", b"a").unwrap().unwrap(), b"1");
        assert_eq!(snap.get("c", b"b").unwrap().unwrap(), b"2", "now is fixed at 99");
        assert_eq!(snap.list_cfs(), vec![CfSpec::new("c")]);
        assert_eq!(
            snap.multi_get("c", &[b"a", b"zz"]).unwrap(),
            vec![Some(b"1".to_vec()), None]
        );
        assert_eq!(keys(snap.prefix_iter("c", b"b").unwrap()).len(), 2);
        assert!(matches!(snap.get("later", b"k"), Err(crate::Error::NoSuchCf(_))));
    }

    #[test]
    fn nul_and_empty_keys_order_and_prefix() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("n") }]).unwrap();
        let p = |k: &[u8]| Op::Put { cf: "n".into(), key: k.to_vec(), value: k.to_vec(), expire_at_ms: None };
        apply(&store, vec![p(b"a"), p(b"\0\0"), p(b""), p(b"\0")]).unwrap();
        let cf = store.live_cf("n").unwrap();
        let ctx = store.live_ctx(0);
        let all: Vec<(Vec<u8>, Vec<u8>)> =
            ctx.iter(cf.clone(), IterMode::Start, None).unwrap().map(Result::unwrap).collect();
        assert_eq!(all.iter().map(|(k, _)| k.as_slice()).collect::<Vec<_>>(), vec![&b""[..], b"\0", b"\0\0", b"a"]);
        assert!(all.iter().all(|(k, v)| k == v), "values equal keys, including empty ones");
        assert_eq!(keys(ctx.iter(cf.clone(), IterMode::From(b"", Direction::Forward), Some(b"")).unwrap()).len(), 4);
        assert_eq!(
            keys(ctx.iter(cf, IterMode::From(b"\0", Direction::Forward), Some(b"\0")).unwrap()),
            vec![b"\0".to_vec(), b"\0\0".to_vec()]
        );
    }

    #[test]
    fn snapshot_keeps_dropped_cf_readable_until_released() {
        let (_d, store) = setup();
        let snap = store.read_snapshot(0).unwrap();
        apply(&store, vec![Op::DropCf { name: "c".into() }]).unwrap();
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 0);
        assert_eq!(snap.get("c", b"a").unwrap().unwrap(), b"1");
        drop(snap);
        assert_eq!(store.drop_unreferenced_cfs().unwrap(), 1);
    }
}
```

Add to the `src/store/mod.rs` module list:

```rust
pub(crate) mod read;
pub(crate) mod snapshot;
```

Create `src/read.rs` with just `//! Public read API.` for now, and add to `src/lib.rs`:

```rust
pub mod read;
pub use read::{ReadApi, ReadSnapshot};
pub use store::read::DbIter;
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --lib store::read`
Expected: FAIL to compile: `cannot find trait ReadApi`, `no method named live_ctx`.

- [ ] **Step 3: Implement `snapshot.rs`**

```rust
//! A RocksDB snapshot that owns its database handle, so it can outlive any borrow.

use std::sync::Arc;

use rocksdb::{ReadOptions, SnapshotWithThreadMode, DB};

pub(crate) struct OwnedSnapshot {
    // Field order matters: `snap` borrows the DB behind `db` and must be dropped first.
    snap: SnapshotWithThreadMode<'static, DB>,
    db: Arc<DB>,
}

impl OwnedSnapshot {
    pub(crate) fn new(db: Arc<DB>) -> Self {
        let snap = db.snapshot();
        // SAFETY: `snap` borrows the `DB` inside `db`. The `Arc` keeps that `DB` alive and
        // at a stable address for as long as `Self` exists, and `snap` is declared first, so
        // it is dropped before `db`. The `'static` lifetime never escapes this struct:
        // `read_opts` only lends it out behind `&self`.
        let snap = unsafe {
            std::mem::transmute::<SnapshotWithThreadMode<'_, DB>, SnapshotWithThreadMode<'static, DB>>(snap)
        };
        OwnedSnapshot { snap, db }
    }

    pub(crate) fn db(&self) -> &DB {
        &self.db
    }

    pub(crate) fn read_opts(&self) -> ReadOptions {
        let mut opts = ReadOptions::default();
        opts.set_snapshot(&self.snap);
        opts
    }
}
```

- [ ] **Step 4: Implement `store/read.rs`** (above the test module)

```rust
//! Low-level reads shared by live reads and snapshots. Values are unwrapped from their
//! envelope, and expired values are hidden (DESIGN.md §8).

use std::sync::Arc;

use rocksdb::{BoundColumnFamily, DBIteratorWithThreadMode, ReadOptions, DB};

use super::cfs::PhysCf;
use super::snapshot::OwnedSnapshot;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::types::{Direction, IterMode, ValueMeta};

pub(crate) struct ReadCtx<'a> {
    pub db: &'a DB,
    pub snap: Option<&'a OwnedSnapshot>,
    /// Fixed for the whole read: once per call, iterator or snapshot.
    pub now_ms: u64,
}

fn handle<'d>(db: &'d DB, cf: &PhysCf) -> Result<Arc<BoundColumnFamily<'d>>> {
    db.cf_handle(&cf.physical)
        .ok_or_else(|| Error::corrupt(format!("physical column family {} is missing", cf.physical)))
}

impl<'a> ReadCtx<'a> {
    fn opts(&self) -> ReadOptions {
        self.snap.map(OwnedSnapshot::read_opts).unwrap_or_default()
    }

    /// The raw envelope, expired or not.
    pub(crate) fn get_raw(&self, cf: &PhysCf, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.db.get_cf_opt(&handle(self.db, cf)?, key, &self.opts())?)
    }

    pub(crate) fn get(&self, cf: &PhysCf, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        let Some(raw) = self.get_raw(cf, key)? else { return Ok(None) };
        let env = envelope::decode(&raw)?;
        if env.is_expired(self.now_ms) {
            return Ok(None);
        }
        Ok(Some((env.value.to_vec(), env.meta)))
    }

    pub(crate) fn iter(&self, cf: Arc<PhysCf>, mode: IterMode<'_>, prefix: Option<&[u8]>) -> Result<DbIter<'a>> {
        let mode = match mode {
            IterMode::Start => rocksdb::IteratorMode::Start,
            IterMode::End => rocksdb::IteratorMode::End,
            IterMode::From(k, Direction::Forward) => rocksdb::IteratorMode::From(k, rocksdb::Direction::Forward),
            IterMode::From(k, Direction::Reverse) => rocksdb::IteratorMode::From(k, rocksdb::Direction::Reverse),
        };
        let inner = self.db.iterator_cf_opt(&handle(self.db, &cf)?, self.opts(), mode);
        Ok(DbIter { inner, _cf: cf, now_ms: self.now_ms, prefix: prefix.map(<[u8]>::to_vec), done: false })
    }
}

/// Iterator over `(key, value)` pairs of a CF, with envelopes stripped and expired values skipped.
pub struct DbIter<'a> {
    inner: DBIteratorWithThreadMode<'a, DB>,
    // Keeps the physical CF from being dropped while iterating.
    _cf: Arc<PhysCf>,
    now_ms: u64,
    prefix: Option<Vec<u8>>,
    done: bool,
}

impl Iterator for DbIter<'_> {
    type Item = Result<(Vec<u8>, Vec<u8>)>;

    fn next(&mut self) -> Option<Self::Item> {
        while !self.done {
            let (k, v) = match self.inner.next()? {
                Ok(kv) => kv,
                Err(e) => return Some(Err(e.into())),
            };
            if self.prefix.as_deref().is_some_and(|p| !k.starts_with(p)) {
                self.done = true;
                return None;
            }
            match envelope::decode(&v) {
                Err(e) => return Some(Err(e)),
                Ok(env) if env.is_expired(self.now_ms) => continue,
                Ok(env) => return Some(Ok((k.into_vec(), env.value.to_vec()))),
            }
        }
        None
    }
}
```

- [ ] **Step 5: Implement `src/read.rs`**

```rust
//! Public read API, shared by `Master`, `Slave` and `ReadSnapshot`.

use std::collections::BTreeMap;
use std::sync::Arc;

use crate::error::{Error, Result};
use crate::store::cfs::PhysCf;
use crate::store::read::{DbIter, ReadCtx};
use crate::store::snapshot::OwnedSnapshot;
use crate::types::{CfSpec, Direction, IterMode, Seq, ValueMeta};

/// Read access to replicated data. Expired values are never returned.
pub trait ReadApi {
    /// The value of `key`, or `None` if it is absent or expired.
    fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        Ok(self.get_with_meta(cf, key)?.map(|(v, _)| v))
    }

    /// Like [`get`](Self::get), plus the value's `write_seq` and expiry.
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>>;

    fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        keys.iter().map(|k| self.get(cf, k)).collect()
    }

    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>>;

    /// Keys starting with `prefix`, in order. Meaningful for bytewise-ordered CFs.
    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>>;

    /// Live user CFs, sorted by name.
    fn list_cfs(&self) -> Vec<CfSpec>;
}

/// A consistent, point-in-time view at one seq. "Now" for TTL purposes is fixed when the
/// snapshot is taken, so repeated reads always agree. Holding one keeps dropped CFs alive.
pub struct ReadSnapshot {
    pub(crate) snap: OwnedSnapshot,
    pub(crate) cfs: BTreeMap<String, Arc<PhysCf>>,
    pub(crate) seq: Seq,
    pub(crate) now_ms: u64,
}

impl ReadSnapshot {
    /// The seq this snapshot reflects.
    pub fn seq(&self) -> Seq {
        self.seq
    }

    fn ctx(&self) -> ReadCtx<'_> {
        ReadCtx { db: self.snap.db(), snap: Some(&self.snap), now_ms: self.now_ms }
    }

    fn cf(&self, name: &str) -> Result<Arc<PhysCf>> {
        self.cfs.get(name).cloned().ok_or_else(|| Error::NoSuchCf(name.to_string()))
    }
}

impl ReadApi for ReadSnapshot {
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        self.ctx().get(&self.cf(cf)?, key)
    }

    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>> {
        self.ctx().iter(self.cf(cf)?, mode, None)
    }

    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>> {
        self.ctx().iter(self.cf(cf)?, IterMode::From(prefix, Direction::Forward), Some(prefix))
    }

    fn list_cfs(&self) -> Vec<CfSpec> {
        self.cfs.values().map(|p| p.entry.spec.clone()).collect()
    }
}
```

- [ ] **Step 6: Add the store entry points** to `impl Store` in `src/store/mod.rs`:

```rust
    pub(crate) fn live_ctx(&self, now_ms: u64) -> read::ReadCtx<'_> {
        read::ReadCtx { db: &self.db, snap: None, now_ms }
    }

    /// Takes a snapshot. The registry read lock is held while the RocksDB snapshot is taken;
    /// CF changes commit under the write lock, so the CF list always matches the data.
    pub(crate) fn read_snapshot(&self, now_ms: u64) -> Result<crate::read::ReadSnapshot> {
        let live = self.cfs.read();
        let snap = snapshot::OwnedSnapshot::new(self.db.clone());
        let cfs = live.clone();
        drop(live);
        let meta_cf = self.cf(CF_META);
        let seq = match snap.db().get_cf_opt(&meta_cf, key::LAST_SEQ, &snap.read_opts())? {
            Some(v) => meta::be64(&v)?,
            None => 0,
        };
        drop(meta_cf);
        Ok(crate::read::ReadSnapshot { snap, cfs, seq, now_ms })
    }
```

- [ ] **Step 7: Run the tests to confirm they pass**

Run: `cargo test --lib`
Expected: PASS, all tests including 4 new ones in `store::read`.

- [ ] **Step 8: Update DESIGN.md §16**

Replace:

```
pub struct ReadSnapshot<'a> { .. }   // impl ReadApi; fn seq(&self) -> Seq; `now` fixed at creation
```

with:

```
pub struct ReadSnapshot { .. }       // impl ReadApi; fn seq(&self) -> Seq; `now` fixed at creation;
                                     // owns an Arc of the DB, so it can be moved into tasks
```

And replace both occurrences of `pub fn snapshot(&self) -> Result<ReadSnapshot<'_>>;` with `pub fn snapshot(&self) -> Result<ReadSnapshot>;`.

- [ ] **Step 9: Commit**

```bash
git add src DESIGN.md
git commit -m "feat: read path with TTL filtering, iterators and owned snapshots

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 8: `Master` — configuration, fsynced write path, identity, epoch, `LogApi` and docs

**Files:**
- Create: `src/config.rs`, `src/master/mod.rs`, `src/master/writer.rs`, `src/store/log.rs`, `src/log.rs`, `tests/common/mod.rs`, `tests/master_write.rs`
- Modify: `src/lib.rs` (modules, re-exports, crate docs), `src/store/mod.rs`

**Interfaces:**
- Consumes: `Store::{open, prepare, commit, write_meta, read_snapshot, live_ctx, live_cf, position, first_retained_seq}`, `codec::{encode_ops, validate_cf_name}`, `Batch::into_ops`, `expire_at`, `Clock`.
- Produces:
  - `pub struct Retention { pub max_age: Option<Duration>, pub max_entries: Option<u64>, pub check_interval: Duration }` (Default per Global Constraints).
  - `pub struct ReaperConfig { pub interval: Duration, pub max_deletes_per_entry: usize, pub max_deletes_per_round: usize }` (Default).
  - `pub struct MasterConfig { pub identifier, pub path, pub retention, pub reaper, pub max_entry_bytes: u64, pub heartbeat: Duration, pub bump_epoch_on_open: bool, pub rocksdb: RocksTuning, pub clock: Arc<dyn Clock> }` with `new(identifier, path)`, `pub(crate) fn validate(&self) -> Result<()>`.
  - `pub(crate) fn validate_identifier(&str) -> Result<()>`.
  - `pub struct Master` with `open`, `identity`, `epoch_history`, `put`, `put_with_ttl`, `delete`, `delete_range`, `write`, `create_cf`, `drop_cf`, `get` (inherent, documented), `snapshot`, `bump_epoch`, `shutdown`; `impl ReadApi`, `impl LogApi`.
  - `master::writer::{Inner, WriterState}`: `Inner::lock_writer() -> Result<MutexGuard<WriterState>>`, `Inner::commit_locked(&self, w: &mut WriterState, build: impl FnOnce(u64) -> Result<Vec<Op>>, extra: impl FnOnce(&mut WriteBatch, &Store) -> Result<()>) -> Result<Seq>`, `Inner::commit_ops(build) -> Result<Seq>`, `Inner::bump_epoch() -> Result<DbIdentity>`, `Inner::epoch_history() -> Vec<EpochStart>`, `Inner::now_ms() -> u64`.
  - `pub trait LogApi { last_seq, first_retained_seq, entries_since(seq) -> Result<EntryIter<'_>>, subscribe() -> watch::Receiver<Seq> }`; `pub struct EntryIter<'a>`: `Iterator<Item = Result<LogEntry>>` yielding entries with seq > the cursor, and `Err(Gap)` if one has been trimmed.
  - `Store::entries_after(&self, seq) -> Result<EntryIter<'_>>`.

- [ ] **Step 1: Write the failing integration tests**

Create `tests/common/mod.rs`:

```rust
#![allow(dead_code)]

use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{LogApi, LogEntry, ManualClock, MasterConfig};

pub const T0: u64 = 1_700_000_000_000;

/// Master config with a manual clock and maintenance effectively disabled.
pub fn config(dir: &Path, clock: &Arc<ManualClock>) -> MasterConfig {
    let mut cfg = MasterConfig::new("master-a", dir);
    cfg.clock = clock.clone();
    cfg.retention.check_interval = Duration::from_secs(3600);
    cfg.reaper.interval = Duration::from_secs(3600);
    cfg
}

pub fn clock() -> Arc<ManualClock> {
    Arc::new(ManualClock::new(T0))
}

pub fn log(m: &impl LogApi) -> Vec<LogEntry> {
    m.entries_since(0).unwrap().map(Result::unwrap).collect()
}
```

Create `tests/master_write.rs`:

```rust
mod common;

use std::sync::Arc;
use std::time::Duration;

use common::{clock, config, log, T0};
use replicated_rocksdb::{Batch, CfSpec, EpochStart, Error, IterMode, LogApi, Master, Op, ReadApi};

#[tokio::test]
async fn first_open_creates_identity_and_reopen_keeps_it() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    let id = m.identity();
    assert_eq!(id.epoch, 1);
    assert_eq!(m.epoch_history(), vec![EpochStart { epoch: 1, start_seq: 1 }]);
    m.create_cf(CfSpec::new("users")).unwrap();
    m.put("users", b"k", b"v").unwrap();
    m.shutdown().await.unwrap();

    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(m.identity(), id);
    assert_eq!(m.last_seq(), 2);
    assert_eq!(m.get("users", b"k").unwrap().unwrap(), b"v");
    assert_eq!(m.put("users", b"k2", b"v").unwrap(), 3);
}

#[tokio::test]
async fn every_write_is_one_logged_entry() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(m.create_cf(CfSpec::new("c")).unwrap(), 1);
    assert_eq!(m.put("c", b"a", b"1").unwrap(), 2);
    assert_eq!(m.put_with_ttl("c", b"b", b"2", Duration::from_secs(1)).unwrap(), 3);
    assert_eq!(m.delete("c", b"a").unwrap(), 4);
    assert_eq!(m.delete_range("c", b"a", b"z").unwrap(), 5);
    let mut b = Batch::new();
    b.put("c", b"x", b"1").put("c", b"y", b"2");
    assert_eq!(m.write(b).unwrap(), 6);
    assert_eq!(m.drop_cf("c").unwrap(), 7);

    let entries = log(&m);
    assert_eq!(entries.iter().map(|e| e.seq).collect::<Vec<_>>(), (1..=7).collect::<Vec<_>>());
    for e in &entries {
        e.verify_crc().unwrap();
        assert_eq!(e.epoch, 1);
        assert_eq!(e.ts_ms, T0);
    }
    assert_eq!(
        entries[2].decode_ops().unwrap(),
        vec![Op::Put { cf: "c".into(), key: b"b".to_vec(), value: b"2".to_vec(), expire_at_ms: Some(T0 + 1_000) }]
    );
    assert_eq!(entries[5].decode_ops().unwrap().len(), 2);
}

#[tokio::test]
async fn failed_validation_consumes_no_seq() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    assert!(matches!(m.put("nope", b"k", b"v"), Err(Error::NoSuchCf(_))));
    assert!(matches!(m.write(Batch::new()), Err(Error::EmptyBatch)));
    assert!(matches!(m.create_cf(CfSpec::new("c")), Err(Error::AlreadyExists(_))));
    assert!(matches!(m.create_cf(CfSpec::new("__x")), Err(Error::InvalidCfName(_))));
    assert!(matches!(m.delete_range("c", b"b", b"a"), Err(Error::InvalidRange)));
    assert_eq!(m.put("c", b"k", b"v").unwrap(), 2);
}

#[tokio::test]
async fn max_entry_bytes_is_enforced_at_the_boundary() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.max_entry_bytes = 200;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    // Find the value size whose encoded entry is exactly 200 bytes.
    let size_for = |n: usize| {
        let ops = vec![Op::Put { cf: "c".into(), key: b"k".to_vec(), value: vec![0; n], expire_at_ms: None }];
        replicated_rocksdb::codec::entry::encoded_entry_len(2, 1, T0, &ops)
    };
    let n = (0..200).find(|&n| size_for(n) == 200).expect("some value size encodes to exactly 200 bytes");
    let err = m.put("c", b"k", &vec![0; n + 1]).unwrap_err();
    assert!(matches!(err, Error::EntryTooLarge { max: 200, .. }), "{err:?}");
    assert_eq!(m.put("c", b"k", &vec![0; n]).unwrap(), 2);
}

#[tokio::test]
async fn timestamps_never_go_backwards() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    c.set(T0 - 60_000);
    m.put("c", b"k", b"v").unwrap();
    let entries = log(&m);
    assert_eq!(entries[1].ts_ms, T0);
}

#[tokio::test]
async fn concurrent_writers_get_unique_gap_free_seqs() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Arc::new(Master::open(config(dir.path(), &c)).await.unwrap());
    m.create_cf(CfSpec::new("c")).unwrap();
    let handles: Vec<_> = (0..8)
        .map(|t| {
            let m = m.clone();
            std::thread::spawn(move || {
                (0..50).map(|i| m.put("c", format!("{t}-{i}").as_bytes(), b"v").unwrap()).collect::<Vec<_>>()
            })
        })
        .collect();
    let mut seqs: Vec<u64> = handles.into_iter().flat_map(|h| h.join().unwrap()).collect();
    seqs.sort_unstable();
    assert_eq!(seqs, (2..=401).collect::<Vec<_>>());
    let ts: Vec<u64> = log(&*m).iter().map(|e| e.ts_ms).collect();
    assert!(ts.windows(2).all(|w| w[0] <= w[1]));
}

#[tokio::test]
async fn bump_epoch_continues_seq_and_tags_new_entries() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let before = m.identity();
    let after = m.bump_epoch().unwrap();
    assert_eq!(after.db_uuid, before.db_uuid);
    assert_eq!(after.epoch, 2);
    assert_eq!(m.put("c", b"k", b"v").unwrap(), 2);
    let entries = log(&m);
    assert_eq!((entries[0].epoch, entries[1].epoch), (1, 2));
    assert_eq!(
        m.epoch_history(),
        vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 2 }]
    );
    m.shutdown().await.unwrap();

    let mut cfg = config(dir.path(), &c);
    cfg.bump_epoch_on_open = true;
    let m = Master::open(cfg).await.unwrap();
    assert_eq!(m.identity().epoch, 3);
    assert_eq!(m.epoch_history().last(), Some(&EpochStart { epoch: 3, start_seq: 3 }));
}

#[tokio::test]
async fn edge_case_keys_and_values_through_the_master() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("e")).unwrap();
    let big_key = vec![0x5Au8; 1 << 20];
    let near_limit = vec![0xC3u8; (16 << 20) - 4096];
    let cases: Vec<(Vec<u8>, Vec<u8>)> = vec![
        (vec![], vec![]),
        (vec![0], vec![0]),
        (b"\0k\0".to_vec(), b"\0\0\0".to_vec()),
        (big_key.clone(), vec![]),
        (b"big".to_vec(), near_limit),
    ];
    for (k, v) in &cases {
        m.put("e", k, v).unwrap();
    }
    assert!(matches!(m.put("e", b"too-big", &vec![0; 16 << 20]), Err(Error::EntryTooLarge { .. })));
    m.shutdown().await.unwrap();

    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    for (k, v) in &cases {
        assert_eq!(m.get("e", k).unwrap().as_deref(), Some(v.as_slice()), "key of length {}", k.len());
    }
    // An empty value is a value, not an absence.
    assert_eq!(m.get("e", b"").unwrap(), Some(vec![]));
    assert_eq!(m.iter("e", IterMode::Start).unwrap().count(), cases.len());
    // [b"", b"\x01") covers exactly the empty key and the keys starting with NUL.
    m.delete_range("e", b"", b"\x01").unwrap();
    assert_eq!(m.get("e", b"").unwrap(), None);
    assert_eq!(m.get("e", b"\0").unwrap(), None);
    assert_eq!(m.get("e", b"\0k\0").unwrap(), None);
    assert!(m.get("e", &big_key).unwrap().is_some());
    m.put_with_ttl("e", b"", b"", Duration::from_secs(1)).unwrap();
    assert_eq!(m.get("e", b"").unwrap(), Some(vec![]));
    c.advance(Duration::from_secs(1));
    assert_eq!(m.get("e", b"").unwrap(), None);
}

#[tokio::test]
async fn read_api_and_snapshot_on_master() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"a", b"1").unwrap();
    let snap = m.snapshot().unwrap();
    m.put("c", b"a", b"2").unwrap();
    assert_eq!(snap.seq(), 2);
    assert_eq!(snap.get("c", b"a").unwrap().unwrap(), b"1");
    assert_eq!(m.get("c", b"a").unwrap().unwrap(), b"2");
    assert_eq!(m.list_cfs(), vec![CfSpec::new("c")]);
    let (_, meta) = m.get_with_meta("c", b"a").unwrap().unwrap();
    assert_eq!(meta.write_seq, 3);
}

#[tokio::test]
async fn subscribe_sees_each_commit() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    let mut rx = m.subscribe();
    m.create_cf(CfSpec::new("c")).unwrap();
    rx.changed().await.unwrap();
    assert_eq!(*rx.borrow(), 1);
}

#[tokio::test]
async fn invalid_identifier_is_rejected() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.identifier = "bad id!".into();
    assert!(matches!(Master::open(cfg).await, Err(Error::Config(_))));
}
```

The boundary test relies on `codec::entry::encoded_entry_len`. Add this public helper to `src/codec/entry.rs`:

```rust
/// Encoded size of the entry the master would write for `ops`. Used for `max_entry_bytes`.
pub fn encoded_entry_len(seq: Seq, epoch: u64, ts_ms: u64, ops: &[Op]) -> usize {
    LogEntry::new(seq, epoch, ts_ms, encode_ops(ops)).encoded_len()
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --test master_write`
Expected: FAIL to compile: `unresolved import replicated_rocksdb::Master`.

- [ ] **Step 3: Implement `src/config.rs`**

```rust
//! Configuration shared by master and slave.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::clock::{Clock, SystemClock};
use crate::error::{Error, Result};
use crate::tuning::RocksTuning;

/// Changelog retention. Runs on every node; age is measured from the master's `ts_ms`.
#[derive(Clone, Debug)]
pub struct Retention {
    pub max_age: Option<Duration>,
    pub max_entries: Option<u64>,
    pub check_interval: Duration,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            max_age: Some(Duration::from_secs(14 * 24 * 3600)),
            max_entries: None,
            check_interval: Duration::from_secs(600),
        }
    }
}

/// TTL reaper (master only).
#[derive(Clone, Debug)]
pub struct ReaperConfig {
    pub interval: Duration,
    pub max_deletes_per_entry: usize,
    pub max_deletes_per_round: usize,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        ReaperConfig { interval: Duration::from_secs(30), max_deletes_per_entry: 1_000, max_deletes_per_round: 100_000 }
    }
}

#[derive(Clone)]
pub struct MasterConfig {
    /// Unique within the topology: 1–128 characters of `[A-Za-z0-9._-]`.
    pub identifier: String,
    pub path: PathBuf,
    pub retention: Retention,
    pub reaper: ReaperConfig,
    /// Maximum encoded size of one log entry. Default 16 MiB.
    pub max_entry_bytes: u64,
    pub heartbeat: Duration,
    /// Set when opening a database restored from a backup (DESIGN.md §13).
    pub bump_epoch_on_open: bool,
    pub rocksdb: RocksTuning,
    pub clock: Arc<dyn Clock>,
}

impl MasterConfig {
    pub fn new(identifier: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        MasterConfig {
            identifier: identifier.into(),
            path: path.into(),
            retention: Retention::default(),
            reaper: ReaperConfig::default(),
            max_entry_bytes: 16 << 20,
            heartbeat: Duration::from_secs(5),
            bump_epoch_on_open: false,
            rocksdb: RocksTuning::default(),
            clock: Arc::new(SystemClock),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_identifier(&self.identifier)?;
        if self.max_entry_bytes == 0 {
            return Err(Error::Config("max_entry_bytes must be positive".into()));
        }
        if self.reaper.max_deletes_per_entry == 0 || self.reaper.max_deletes_per_round == 0 {
            return Err(Error::Config("reaper limits must be positive".into()));
        }
        if self.retention.check_interval.is_zero() || self.reaper.interval.is_zero() {
            return Err(Error::Config("maintenance intervals must be positive".into()));
        }
        Ok(())
    }
}

pub(crate) fn validate_identifier(id: &str) -> Result<()> {
    let ok = (1..=128).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(Error::Config(format!("identifier {id:?} must be 1-128 characters of [A-Za-z0-9._-]")))
    }
}
```

- [ ] **Step 4: Implement `src/store/log.rs` and `src/log.rs`**

`src/store/log.rs`:

```rust
//! Reading the changelog.

use rocksdb::{DBIteratorWithThreadMode, Direction, IteratorMode, DB};

use super::meta::{self, CF_CHANGELOG};
use super::Store;
use crate::error::{Error, Result};
use crate::types::{LogEntry, Seq};

/// Entries with seq greater than a cursor, in order. Yields `Err(Gap)` and stops if the
/// next expected entry has been trimmed by retention.
pub struct EntryIter<'a> {
    inner: DBIteratorWithThreadMode<'a, DB>,
    next: Seq,
    done: bool,
}

impl Iterator for EntryIter<'_> {
    type Item = Result<LogEntry>;

    fn next(&mut self) -> Option<Self::Item> {
        if self.done {
            return None;
        }
        let (k, v) = match self.inner.next()? {
            Ok(kv) => kv,
            Err(e) => {
                self.done = true;
                return Some(Err(e.into()));
            }
        };
        let result = meta::be64(&k).and_then(|seq| {
            if seq != self.next {
                return Err(Error::Gap { requested: self.next, first_retained: seq });
            }
            LogEntry::decode(&v)
        });
        match &result {
            Ok(_) => self.next += 1,
            Err(_) => self.done = true,
        }
        Some(result)
    }
}

impl Store {
    pub(crate) fn entries_after(&self, seq: Seq) -> Result<EntryIter<'_>> {
        let from = seq.saturating_add(1);
        let first = self.first_retained_seq();
        if from < first {
            return Err(Error::Gap { requested: from, first_retained: first });
        }
        let key = from.to_be_bytes();
        let cf = self.cf(CF_CHANGELOG);
        let inner = self.db().iterator_cf(&cf, IteratorMode::From(&key, Direction::Forward));
        Ok(EntryIter { inner, next: from, done: false })
    }
}
```

Add `pub(crate) mod log;` to the `src/store/mod.rs` module list.

`src/log.rs`:

```rust
//! Public access to a node's changelog.

use tokio::sync::watch;

use crate::error::Result;
use crate::types::Seq;

pub use crate::store::log::EntryIter;

pub trait LogApi {
    /// Last committed (master) or applied (slave) seq.
    fn last_seq(&self) -> Seq;
    /// Lowest seq still in the changelog, or `last_seq + 1` if it is empty.
    fn first_retained_seq(&self) -> Seq;
    /// Entries with seq greater than `seq`, in order. `Err(Gap)` if any were trimmed;
    /// resync with a full read in that case.
    fn entries_since(&self, seq: Seq) -> Result<EntryIter<'_>>;
    /// Receives the latest committed seq after each commit.
    fn subscribe(&self) -> watch::Receiver<Seq>;
}
```

- [ ] **Step 5: Implement `src/master/writer.rs`**

```rust
//! The gated writer: one entry at a time, always fsynced (DESIGN.md §5).

use std::sync::{Mutex, MutexGuard, PoisonError, RwLock};

use rocksdb::WriteBatch;
use tokio::sync::watch;

use crate::codec::encode_ops;
use crate::config::MasterConfig;
use crate::error::{Error, Result};
use crate::store::meta::{self, key, CF_META};
use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, LogEntry, Op, Seq};

pub(crate) struct WriterState {
    pub last_ts: u64,
    /// Set after a failed write: durability of that batch is unknown, so no more writes.
    pub fenced: bool,
}

pub(crate) struct Inner {
    pub store: Store,
    pub cfg: MasterConfig,
    writer: Mutex<WriterState>,
    // Identity and epoch history change together, under one lock.
    identity: RwLock<(DbIdentity, Vec<EpochStart>)>,
    pub seq_tx: watch::Sender<Seq>,
    pub shutdown_tx: watch::Sender<bool>,
}

impl Inner {
    pub(crate) fn new(
        store: Store,
        cfg: MasterConfig,
        last_ts: u64,
        identity: DbIdentity,
        history: Vec<EpochStart>,
    ) -> Self {
        let last_seq = store.position().last_seq;
        Inner {
            store,
            cfg,
            writer: Mutex::new(WriterState { last_ts, fenced: false }),
            identity: RwLock::new((identity, history)),
            seq_tx: watch::channel(last_seq).0,
            shutdown_tx: watch::channel(false).0,
        }
    }

    pub(crate) fn now_ms(&self) -> u64 {
        self.cfg.clock.now_ms()
    }

    pub(crate) fn identity(&self) -> DbIdentity {
        self.identity.read().unwrap_or_else(PoisonError::into_inner).0
    }

    pub(crate) fn epoch_history(&self) -> Vec<EpochStart> {
        self.identity.read().unwrap_or_else(PoisonError::into_inner).1.clone()
    }

    pub(crate) fn lock_writer(&self) -> Result<MutexGuard<'_, WriterState>> {
        let w = self.writer.lock().unwrap_or_else(PoisonError::into_inner);
        if w.fenced { Err(Error::WriterFenced) } else { Ok(w) }
    }

    pub(crate) fn commit_ops(&self, build: impl FnOnce(u64) -> Result<Vec<Op>>) -> Result<Seq> {
        let mut w = self.lock_writer()?;
        self.commit_locked(&mut w, build, |_, _| Ok(()))
    }

    /// Builds, validates and durably commits one entry. `build` gets the entry's `ts_ms`;
    /// `extra` may add master-local writes (TTL index) to the same batch.
    pub(crate) fn commit_locked(
        &self,
        w: &mut WriterState,
        build: impl FnOnce(u64) -> Result<Vec<Op>>,
        extra: impl FnOnce(&mut WriteBatch, &Store) -> Result<()>,
    ) -> Result<Seq> {
        if w.fenced {
            return Err(Error::WriterFenced);
        }
        let ts = self.now_ms().max(w.last_ts);
        let ops = build(ts)?;
        let seq = self.store.position().last_seq + 1;
        let entry = LogEntry::new(seq, self.identity().epoch, ts, encode_ops(&ops));
        let mut prepared = self.store.prepare(&entry, &ops)?;
        let size = entry.encoded_len() as u64;
        if size > self.cfg.max_entry_bytes {
            return Err(Error::EntryTooLarge { size, max: self.cfg.max_entry_bytes });
        }
        self.store.add_ttl_index(&mut prepared.batch, &ops)?;
        extra(&mut prepared.batch, &self.store)?;
        prepared.batch.put_cf(&self.store.cf(CF_META), key::LAST_TS, ts.to_be_bytes());
        if let Err(e) = self.store.commit(prepared, true) {
            w.fenced = true;
            tracing::error!(seq, error = %e, "master write failed; writer fenced until reopen");
            return Err(e);
        }
        w.last_ts = ts;
        self.seq_tx.send_replace(seq);
        Ok(seq)
    }

    /// New epoch, same db_uuid; seq numbering continues (DESIGN.md §13).
    /// Identity and history are written in one synced batch.
    pub(crate) fn bump_epoch(&self) -> Result<DbIdentity> {
        let _w = self.lock_writer()?;
        let mut guard = self.identity.write().unwrap_or_else(PoisonError::into_inner);
        let (mut id, mut history) = guard.clone();
        id.epoch = history.iter().map(|e| e.epoch).max().unwrap_or(id.epoch).max(id.epoch) + 1;
        let start = self.store.position().last_seq + 1;
        history.push(EpochStart { epoch: id.epoch, start_seq: start });
        self.store.write_meta(
            &[
                (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
            ],
            true,
        )?;
        *guard = (id, history);
        tracing::info!(epoch = id.epoch, start_seq = start, "epoch bumped");
        Ok(id)
    }
}
```

`commit_locked` calls `Store::add_ttl_index`, which Task 9 implements. For this task, add a stub to `impl Store` in `src/store/mod.rs`:

```rust
    /// Adds `__ttl_index` entries for TTL puts. Master only. Implemented in Task 9.
    pub(crate) fn add_ttl_index(&self, _batch: &mut rocksdb::WriteBatch, _ops: &[crate::types::Op]) -> Result<()> {
        Ok(())
    }
```

- [ ] **Step 6: Implement `src/master/mod.rs`**

```rust
//! The writable master node.

mod writer;

use std::sync::Arc;
use std::time::Duration;

use tokio::sync::watch;
use tokio::task::JoinHandle;

use crate::codec::validate_cf_name;
use crate::config::MasterConfig;
use crate::error::Result;
use crate::log::{EntryIter, LogApi};
use crate::read::{ReadApi, ReadSnapshot};
use crate::store::meta::{self, key};
use crate::store::read::DbIter;
use crate::store::Store;
use crate::types::{expire_at, Batch, CfSpec, DbIdentity, Direction, EpochStart, IterMode, Op, Role, Seq, ValueMeta};
use writer::Inner;

/// The single writable node. Every write is one fsynced log entry.
///
/// # Read-modify-write
///
/// There is no `update`, compare-and-set or merge. Each write method serialises one call
/// only: **between your `get` and your `put`, another writer can change the key, and that
/// change will be lost.** If a new value is derived from an old one, hold your own lock
/// around the whole get → compute → put sequence, and make every writer of those keys take
/// the same lock. See DESIGN.md §5.1.
pub struct Master {
    inner: Arc<Inner>,
    task: Option<JoinHandle<()>>,
}

impl Master {
    /// Opens or creates a master database. Must be called within a tokio runtime.
    pub async fn open(cfg: MasterConfig) -> Result<Self> {
        cfg.validate()?;
        let (store, meta) = Store::open(&cfg.path, Role::Master, cfg.rocksdb.clone())?;
        let (identity, history) = match meta.identity {
            Some(id) if !meta.epoch_history.is_empty() => (id, meta.epoch_history),
            Some(_) => return Err(crate::Error::corrupt("origin_identity without epoch_history in __meta")),
            None => {
                let id = DbIdentity { db_uuid: uuid::Uuid::new_v4(), epoch: 1 };
                let history = vec![EpochStart { epoch: 1, start_seq: 1 }];
                store.write_meta(
                    &[
                        (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                        (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
                    ],
                    true,
                )?;
                (id, history)
            }
        };
        let bump = cfg.bump_epoch_on_open;
        let inner = Arc::new(Inner::new(store, cfg, meta.last_ts, identity, history));
        if bump {
            inner.bump_epoch()?;
        }
        Ok(Master { inner, task: None })
    }

    pub fn identity(&self) -> DbIdentity {
        self.inner.identity()
    }

    /// One `{epoch, start_seq}` record per epoch, oldest first (PROTOCOL.md §5.5).
    pub fn epoch_history(&self) -> Vec<EpochStart> {
        self.inner.epoch_history()
    }

    /// Writes `value` under `key`, replacing any value and clearing any TTL.
    ///
    /// This is an absolute write. It is **not** safe to compute `value` from a previous
    /// [`get`](Self::get) without holding an application lock around both calls; see the
    /// [type-level docs](Self#read-modify-write).
    pub fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Seq> {
        let op = Op::Put { cf: cf.into(), key: key.into(), value: value.into(), expire_at_ms: None };
        self.inner.commit_ops(|_| Ok(vec![op]))
    }

    /// Like [`put`](Self::put), with an expiry of commit time + `ttl`. Expired values are
    /// hidden from reads at once and deleted by the reaper later.
    pub fn put_with_ttl(&self, cf: &str, key: &[u8], value: &[u8], ttl: Duration) -> Result<Seq> {
        let (cf, key, value) = (cf.to_string(), key.to_vec(), value.to_vec());
        self.inner.commit_ops(|ts| Ok(vec![Op::Put { cf, key, value, expire_at_ms: Some(expire_at(ts, ttl)) }]))
    }

    pub fn delete(&self, cf: &str, key: &[u8]) -> Result<Seq> {
        let op = Op::Delete { cf: cf.into(), key: key.into() };
        self.inner.commit_ops(|_| Ok(vec![op]))
    }

    /// Deletes keys in `[start, end)` under the CF's comparator. `start` must sort before `end`.
    pub fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Seq> {
        let op = Op::DeleteRange { cf: cf.into(), start: start.into(), end: end.into() };
        self.inner.commit_ops(|_| Ok(vec![op]))
    }

    /// Commits all operations in `batch` atomically, as one log entry.
    ///
    /// A batch is not a transaction over reads: values computed from earlier reads can be
    /// stale by the time the batch commits unless the application holds its own lock; see
    /// the [type-level docs](Self#read-modify-write).
    pub fn write(&self, batch: Batch) -> Result<Seq> {
        self.inner.commit_ops(|ts| Ok(batch.into_ops(ts)))
    }

    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq> {
        validate_cf_name(&spec.name)?;
        self.inner.commit_ops(|_| Ok(vec![Op::CreateCf { spec }]))
    }

    pub fn drop_cf(&self, name: &str) -> Result<Seq> {
        validate_cf_name(name)?;
        let op = Op::DropCf { name: name.into() };
        self.inner.commit_ops(|_| Ok(vec![op]))
    }

    /// Reads the current value of `key`, or `None` if absent or expired.
    ///
    /// The value can change as soon as this returns. To update a key based on its current
    /// value, hold an application lock around get → compute → put; see the
    /// [type-level docs](Self#read-modify-write).
    pub fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        ReadApi::get(self, cf, key)
    }

    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        self.inner.store.read_snapshot(self.inner.now_ms())
    }

    /// Starts a new epoch (same db_uuid; seq continues). Use after restoring from a backup.
    pub fn bump_epoch(&self) -> Result<DbIdentity> {
        self.inner.bump_epoch()
    }

    /// Stops background work and waits for it to finish.
    pub async fn shutdown(mut self) -> Result<()> {
        self.inner.shutdown_tx.send_replace(true);
        if let Some(task) = self.task.take() {
            let _ = task.await;
        }
        Ok(())
    }
}

impl Drop for Master {
    fn drop(&mut self) {
        self.inner.shutdown_tx.send_replace(true);
    }
}

impl ReadApi for Master {
    fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).get(&cf, key)
    }

    fn iter(&self, cf: &str, mode: IterMode<'_>) -> Result<DbIter<'_>> {
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).iter(cf, mode, None)
    }

    fn prefix_iter(&self, cf: &str, prefix: &[u8]) -> Result<DbIter<'_>> {
        let cf = self.inner.store.live_cf(cf)?;
        self.inner.store.live_ctx(self.inner.now_ms()).iter(cf, IterMode::From(prefix, Direction::Forward), Some(prefix))
    }

    fn list_cfs(&self) -> Vec<CfSpec> {
        self.inner.store.cfs.live_entries().into_iter().map(|e| e.spec).collect()
    }
}

impl LogApi for Master {
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

Note: `entries_since` borrows the DB from `&self`. A `Master` held in an `Arc` works as `&*m`.

- [ ] **Step 7: Wire up `src/lib.rs` with the crate docs**

Replace the top of `src/lib.rs` (the `//!` line) with:

```rust
//! Single-master, async log-shipping replication for RocksDB.
//!
//! A [`Master`] is the only writer. Every write becomes one fsynced, checksummed log entry
//! of absolute operations (`Put`, `Delete`, `DeleteRange`, `CreateCf`, `DropCf`), which
//! read-only slaves replay in order. See `DESIGN.md` and `PROTOCOL.md`.
//!
//! # Read-modify-write is the application's job
//!
//! This crate has no `update`, compare-and-set or merge. The writer lock serialises
//! individual write calls only, so **between a `get` and a `put`, another writer can change
//! the key, and that change will be lost.** To derive a new value from an old one (counters,
//! appends, "insert if absent"), hold your own lock around the whole sequence:
//!
//! ```no_run
//! # use replicated_rocksdb::{Master, Result};
//! # use std::sync::Mutex;
//! # fn f(master: &Master, lock: &Mutex<()>) -> Result<()> {
//! let _guard = lock.lock().unwrap();               // app-level lock: global, per key, or striped
//! let old = master.get("counters", b"hits")?;
//! let n = old.map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap())) + 1;
//! master.put("counters", b"hits", &n.to_be_bytes())?; // logged as an absolute Put
//! # Ok(()) }
//! ```
//!
//! Every writer of those keys must take the same lock. Because there is exactly one master
//! process, an in-process lock is enough.
```

Add modules and re-exports:

```rust
pub mod config;
pub mod log;
mod master;

pub use config::{MasterConfig, ReaperConfig, Retention};
pub use log::{EntryIter, LogApi};
pub use master::Master;
```

- [ ] **Step 8: Run all tests**

Run: `cargo test`
Expected: PASS, including the 11 tests in `tests/master_write.rs` and the crate-level doctest (`no_run`, compiled only).

- [ ] **Step 9: Commit**

```bash
git add src tests
git commit -m "feat: master with fsynced write path, epochs, log API and RMW docs

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 9: TTL index and reaper

**Files:**
- Create: `src/store/ttl.rs`, `src/master/reaper.rs`, `tests/master_ttl.rs`
- Modify: `src/store/mod.rs` (remove the `add_ttl_index` stub, add the module), `src/master/mod.rs`, `src/lib.rs`, `DESIGN.md` (§3.1)

**Interfaces:**
- Consumes: `Inner::{lock_writer, commit_locked, now_ms}`, `Store::{live_cf, live_ctx, cf}`, `envelope::decode`.
- Produces:
  - `ttl::index_key(expire_at: u64, cf: &CfEntry, key: &[u8]) -> Vec<u8>`, `ttl::IndexEntry { raw_key, expire_at, cf_name, create_seq, key }`, `ttl::parse_index_key(&[u8]) -> Result<IndexEntry>`.
  - `Store::add_ttl_index(&self, &mut WriteBatch, &[Op]) -> Result<()>`, `Store::ttl_due(&self, now_ms, limit) -> Result<Vec<IndexEntry>>`, `Store::ttl_entry_is_live(&self, &IndexEntry) -> Result<bool>`, `Store::remove_ttl_index(&self, &mut WriteBatch, &[IndexEntry])`.
  - `pub struct ReapStats { pub deleted: usize, pub stale: usize, pub entries: usize }`.
  - `Inner::reap_round(&self) -> Result<ReapStats>`; `pub fn Master::reap_expired(&self) -> Result<ReapStats>`.

- [ ] **Step 1: Write the failing tests**

Create `tests/master_ttl.rs`:

```rust
mod common;

use std::time::Duration;

use common::{clock, config, log};
use replicated_rocksdb::{Batch, CfSpec, LogApi, Master, Op, ReadApi, ReapStats};

async fn master(dir: &std::path::Path, c: &std::sync::Arc<replicated_rocksdb::ManualClock>) -> Master {
    let m = Master::open(config(dir, c)).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    m
}

#[tokio::test]
async fn expired_values_are_hidden_then_reaped_by_a_logged_delete() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.put_with_ttl("c", b"k", b"v", Duration::from_secs(10)).unwrap();
    assert_eq!(m.get("c", b"k").unwrap().unwrap(), b"v");
    assert_eq!(m.reap_expired().unwrap(), ReapStats::default(), "nothing due yet");

    c.advance(Duration::from_secs(10));
    assert_eq!(m.get("c", b"k").unwrap(), None, "hidden before reaping");
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 1, stale: 0, entries: 1 });
    let last = log(&m).pop().unwrap();
    assert_eq!(last.decode_ops().unwrap(), vec![Op::Delete { cf: "c".into(), key: b"k".to_vec() }]);
    assert_eq!(m.reap_expired().unwrap(), ReapStats::default(), "index entry removed");
}

#[tokio::test]
async fn overwrite_clears_ttl_and_leaves_a_stale_index_entry() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.put_with_ttl("c", b"k", b"v", Duration::from_secs(1)).unwrap();
    m.put("c", b"k", b"forever").unwrap();
    let seq = m.last_seq();
    c.advance(Duration::from_secs(5));
    assert_eq!(m.get("c", b"k").unwrap().unwrap(), b"forever");
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 0, stale: 1, entries: 0 });
    assert_eq!(m.last_seq(), seq, "stale cleanup is not logged");
    assert_eq!(m.get("c", b"k").unwrap().unwrap(), b"forever");
}

#[tokio::test]
async fn range_deleted_and_dropped_cf_entries_are_stale() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.create_cf(CfSpec::new("d")).unwrap();
    m.put_with_ttl("c", b"a", b"v", Duration::from_secs(1)).unwrap();
    m.put_with_ttl("d", b"a", b"v", Duration::from_secs(1)).unwrap();
    m.delete_range("c", b"a", b"b").unwrap();
    m.drop_cf("d").unwrap();
    m.create_cf(CfSpec::new("d")).unwrap();
    m.put("d", b"a", b"new").unwrap();
    c.advance(Duration::from_secs(2));
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 0, stale: 2, entries: 0 });
    assert_eq!(m.get("d", b"a").unwrap().unwrap(), b"new");
}

#[tokio::test]
async fn large_rounds_split_into_bounded_entries() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.reaper.max_deletes_per_entry = 3;
    cfg.reaper.max_deletes_per_round = 7;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut b = Batch::new();
    for i in 0..10u8 {
        b.put_with_ttl("c", &[i], b"v", Duration::from_secs(1));
    }
    m.write(b).unwrap();
    c.advance(Duration::from_secs(1));
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 7, stale: 0, entries: 3 });
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 3, stale: 0, entries: 1 });
    let sizes: Vec<usize> = log(&m).iter().skip(2).map(|e| e.decode_ops().unwrap().len()).collect();
    assert_eq!(sizes, vec![3, 3, 1, 3]);
}

#[tokio::test]
async fn ttl_on_empty_and_nul_keys_is_reaped() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.put_with_ttl("c", b"", b"", Duration::from_secs(1)).unwrap();
    m.put_with_ttl("c", b"\0", b"\0", Duration::from_secs(1)).unwrap();
    m.put("c", b"\0\0", b"keep").unwrap();
    c.advance(Duration::from_secs(1));
    assert_eq!(m.reap_expired().unwrap(), ReapStats { deleted: 2, stale: 0, entries: 1 });
    assert_eq!(m.get("c", b"\0\0").unwrap().unwrap(), b"keep");
}

#[tokio::test]
async fn extreme_ttls() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let m = master(dir.path(), &c).await;
    m.put_with_ttl("c", b"zero", b"v", Duration::ZERO).unwrap();
    m.put_with_ttl("c", b"max", b"v", Duration::MAX).unwrap();
    assert_eq!(m.get("c", b"zero").unwrap(), None, "expire_at == now is expired");
    let (_, meta) = m.get_with_meta("c", b"max").unwrap().unwrap();
    assert_eq!(meta.expire_at_ms, Some(u64::MAX));
    c.set(u64::MAX - 1);
    assert!(m.get("c", b"max").unwrap().is_some());
    assert_eq!(m.reap_expired().unwrap().deleted, 1);
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --test master_ttl`
Expected: FAIL to compile: `no method named reap_expired`, `unresolved import ReapStats`.

- [ ] **Step 3: Implement `src/store/ttl.rs`**

```rust
//! `__ttl_index` (master only): `BE64(expire_at) ‖ BE64(create_seq) ‖ BE16(len(name)) ‖ name ‖ key`.
//! Including `create_seq` keeps a dropped-and-recreated CF from matching old entries.

use rocksdb::{IteratorMode, WriteBatch};

use super::meta::CF_TTL_INDEX;
use super::Store;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::types::{CfEntry, Op, Seq};

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct IndexEntry {
    pub raw_key: Vec<u8>,
    pub expire_at: u64,
    pub cf_name: String,
    pub create_seq: Seq,
    pub key: Vec<u8>,
}

pub(crate) fn index_key(expire_at: u64, cf: &CfEntry, key: &[u8]) -> Vec<u8> {
    let name = cf.spec.name.as_bytes();
    let mut k = Vec::with_capacity(18 + name.len() + key.len());
    k.extend_from_slice(&expire_at.to_be_bytes());
    k.extend_from_slice(&cf.create_seq.to_be_bytes());
    k.extend_from_slice(&(name.len() as u16).to_be_bytes());
    k.extend_from_slice(name);
    k.extend_from_slice(key);
    k
}

pub(crate) fn parse_index_key(raw: &[u8]) -> Result<IndexEntry> {
    let bad = || Error::corrupt("malformed __ttl_index key");
    if raw.len() < 18 {
        return Err(bad());
    }
    let expire_at = u64::from_be_bytes(raw[0..8].try_into().map_err(|_| bad())?);
    let create_seq = u64::from_be_bytes(raw[8..16].try_into().map_err(|_| bad())?);
    let name_len = u16::from_be_bytes(raw[16..18].try_into().map_err(|_| bad())?) as usize;
    let name_end = 18 + name_len;
    if raw.len() < name_end {
        return Err(bad());
    }
    let cf_name = String::from_utf8(raw[18..name_end].to_vec()).map_err(|_| bad())?;
    Ok(IndexEntry { raw_key: raw.to_vec(), expire_at, cf_name, create_seq, key: raw[name_end..].to_vec() })
}

impl Store {
    pub(crate) fn add_ttl_index(&self, batch: &mut WriteBatch, ops: &[Op]) -> Result<()> {
        let cf = self.cf(CF_TTL_INDEX);
        for op in ops {
            if let Op::Put { cf: name, key, expire_at_ms: Some(at), .. } = op {
                let phys = self.live_cf(name)?;
                batch.put_cf(&cf, index_key(*at, &phys.entry, key), []);
            }
        }
        Ok(())
    }

    /// Up to `limit` index entries with `expire_at <= now_ms`, oldest first.
    pub(crate) fn ttl_due(&self, now_ms: u64, limit: usize) -> Result<Vec<IndexEntry>> {
        let cf = self.cf(CF_TTL_INDEX);
        let mut due = Vec::new();
        for item in self.db().iterator_cf(&cf, IteratorMode::Start) {
            if due.len() >= limit {
                break;
            }
            let entry = parse_index_key(&item?.0)?;
            if entry.expire_at > now_ms {
                break;
            }
            due.push(entry);
        }
        Ok(due)
    }

    /// True if the key still holds the value this index entry was written for.
    pub(crate) fn ttl_entry_is_live(&self, e: &IndexEntry) -> Result<bool> {
        let Some(phys) = self.cfs.get(&e.cf_name) else { return Ok(false) };
        if phys.entry.create_seq != e.create_seq {
            return Ok(false);
        }
        let Some(raw) = self.live_ctx(0).get_raw(&phys, &e.key)? else { return Ok(false) };
        Ok(envelope::decode(&raw)?.meta.expire_at_ms == Some(e.expire_at))
    }

    pub(crate) fn remove_ttl_index(&self, batch: &mut WriteBatch, entries: &[IndexEntry]) {
        let cf = self.cf(CF_TTL_INDEX);
        for e in entries {
            batch.delete_cf(&cf, &e.raw_key);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::CfSpec;

    #[test]
    fn index_key_round_trips_and_sorts_by_expiry() {
        let cf = CfEntry { spec: CfSpec::new("a:b"), create_seq: 7 };
        let k1 = index_key(5, &cf, b"");
        let k2 = index_key(6, &cf, b"\x00");
        assert!(k1 < k2);
        let e = parse_index_key(&k2).unwrap();
        assert_eq!((e.expire_at, e.create_seq, e.cf_name.as_str(), e.key.as_slice()), (6, 7, "a:b", &b"\x00"[..]));
        assert!(parse_index_key(&k2[..17]).is_err());
    }
}
```

Remove the `add_ttl_index` stub from `src/store/mod.rs` and add `pub(crate) mod ttl;` to its module list.

- [ ] **Step 4: Implement `src/master/reaper.rs`**

```rust
//! TTL reaper (master only, DESIGN.md §8).

use rocksdb::{WriteBatch, WriteOptions};

use super::writer::Inner;
use crate::error::Result;
use crate::types::Op;

/// What one reaper round did.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct ReapStats {
    /// Keys deleted through logged `Delete` ops.
    pub deleted: usize,
    /// Index entries removed because the key was overwritten, deleted or its CF dropped.
    pub stale: usize,
    /// Log entries written.
    pub entries: usize,
}

impl Inner {
    pub(crate) fn reap_round(&self) -> Result<ReapStats> {
        let cfg = &self.cfg.reaper;
        let mut stats = ReapStats::default();
        while stats.deleted < cfg.max_deletes_per_round {
            let limit = cfg.max_deletes_per_entry.min(cfg.max_deletes_per_round - stats.deleted);
            // The writer lock is held per entry and released between entries, so the reaper
            // never blocks application writes for long.
            let mut w = self.lock_writer()?;
            let due = self.store.ttl_due(self.now_ms(), limit)?;
            if due.is_empty() {
                break;
            }
            let mut ops = Vec::new();
            for e in &due {
                if self.store.ttl_entry_is_live(e)? {
                    ops.push(Op::Delete { cf: e.cf_name.clone(), key: e.key.clone() });
                }
            }
            stats.stale += due.len() - ops.len();
            if ops.is_empty() {
                // Master-local cleanup; __ttl_index is derived and never replicated.
                let mut batch = WriteBatch::default();
                self.store.remove_ttl_index(&mut batch, &due);
                self.store.db().write_opt(batch, &WriteOptions::default())?;
            } else {
                let n = ops.len();
                self.commit_locked(&mut w, |_| Ok(ops), |batch, store| {
                    store.remove_ttl_index(batch, &due);
                    Ok(())
                })?;
                stats.deleted += n;
                stats.entries += 1;
            }
        }
        Ok(stats)
    }
}
```

In `src/master/mod.rs`, add `mod reaper;` and `pub use reaper::ReapStats;`, and add to `impl Master`:

```rust
    /// Runs one reaper round now: logs `Delete`s for expired keys, in entries of at most
    /// `reaper.max_deletes_per_entry`, up to `reaper.max_deletes_per_round` deletes.
    pub fn reap_expired(&self) -> Result<ReapStats> {
        self.inner.reap_round()
    }
```

In `src/lib.rs`, change the master re-export to `pub use master::{Master, ReapStats};`.

- [ ] **Step 5: Run the tests to confirm they pass**

Run: `cargo test`
Expected: PASS, including the 6 tests in `tests/master_ttl.rs` and the `index_key_round_trips_and_sorts_by_expiry` unit test.

- [ ] **Step 6: Update DESIGN.md §3.1**

Replace the bullet starting `` - `__ttl_index` is a derived structure`` with:

```
- `__ttl_index` is a derived structure that only the master reads. Key: `BE64(expire_at_ms) ‖ BE64(create_seq) ‖ BE16(len(name)) ‖ name ‖ key`, empty value. Including `create_seq` stops a dropped and re-created CF from matching old index entries. Slaves never reap, so they do not need it. It can be rebuilt from a full scan of the envelopes, for example on a future promotion.
```

- [ ] **Step 7: Commit**

```bash
git add src tests DESIGN.md
git commit -m "feat: TTL index and reaper that logs bounded Delete entries

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 10: Changelog retention, guards and the maintenance task

**Files:**
- Create: `src/store/retention.rs`, `src/master/maintenance.rs`, `tests/master_retention.rs`
- Modify: `src/store/mod.rs` (`guards` field, module), `src/master/mod.rs` (spawn task in `open`), `src/master/writer.rs` (`trim_changelog` wrapper)

**Interfaces:**
- Consumes: `Store::{entries_after, first_retained_seq, set_first_retained_seq, position, cf}`, `LogEntry::decode`, `Retention`, `Inner::{reap_round, now_ms}`, `Store::drop_unreferenced_cfs`.
- Produces:
  - `pub(crate) struct RetentionGuard` (RAII; drop releases), `Store::guard(&self, seq: Seq) -> RetentionGuard` (entries with seq > `seq` are kept while held).
  - `Store::trim_changelog(&self, now_ms: u64, r: &Retention) -> Result<u64>` (returns entries removed).
  - `Inner::trim_changelog(&self) -> Result<u64>`.
  - `master::maintenance::spawn(Arc<Inner>) -> JoinHandle<()>`, started by `Master::open`.

- [ ] **Step 1: Write the failing tests**

Unit tests in a new `src/store/retention.rs` (tests only for now):

```rust
#[cfg(test)]
mod tests {
    use std::time::Duration;

    use crate::config::Retention;
    use crate::error::Error;
    use crate::store::test_support::{apply, open_store};
    use crate::types::{CfSpec, Op};

    const HOUR: u64 = 3_600_000;

    fn retention(max_age_ms: Option<u64>, max_entries: Option<u64>) -> Retention {
        Retention { max_age: max_age_ms.map(Duration::from_millis), max_entries, check_interval: Duration::from_secs(1) }
    }

    fn put(i: u8) -> Vec<Op> {
        vec![Op::Put { cf: "c".into(), key: vec![i], value: vec![], expire_at_ms: None }]
    }

    // test_support::apply stamps ts = 1000 × seq.
    fn store_with(n: u8) -> (tempfile::TempDir, crate::store::Store) {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        for i in 0..n {
            apply(&store, put(i)).unwrap();
        }
        (dir, store)
    }

    #[test]
    fn trims_by_age_from_the_front() {
        let (_d, store) = store_with(4); // seqs 1..=5, ts 1000..=5000
        assert_eq!(store.trim_changelog(3_500 + HOUR, &retention(Some(HOUR), None)).unwrap(), 3);
        assert_eq!(store.first_retained_seq(), 4);
        assert!(matches!(store.entries_after(2), Err(Error::Gap { requested: 3, first_retained: 4 })));
        assert_eq!(store.entries_after(3).unwrap().count(), 2);
    }

    #[test]
    fn trims_by_count() {
        let (_d, store) = store_with(4);
        assert_eq!(store.trim_changelog(0, &retention(None, Some(2))).unwrap(), 3);
        assert_eq!(store.first_retained_seq(), 4);
    }

    #[test]
    fn quiet_store_can_trim_everything_but_keeps_position() {
        let (_d, store) = store_with(2);
        assert_eq!(store.trim_changelog(u64::MAX, &retention(Some(1), None)).unwrap(), 3);
        assert_eq!(store.position().last_seq, 3);
        assert_eq!(store.first_retained_seq(), 4);
        assert_eq!(apply(&store, put(9)).unwrap(), 4);
        assert_eq!(store.first_retained_seq(), 4);
        assert_eq!(store.entries_after(3).unwrap().next().unwrap().unwrap().seq, 4);
    }

    #[test]
    fn guard_keeps_entries_after_its_seq() {
        let (_d, store) = store_with(4);
        let g = store.guard(2);
        assert_eq!(store.trim_changelog(u64::MAX, &retention(Some(1), None)).unwrap(), 2);
        assert_eq!(store.first_retained_seq(), 3);
        drop(g);
        assert_eq!(store.trim_changelog(u64::MAX, &retention(Some(1), None)).unwrap(), 3);
    }

    #[test]
    fn first_retained_survives_reopen() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = open_store(dir.path());
            apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
            apply(&store, put(1)).unwrap();
            store.trim_changelog(u64::MAX, &retention(None, Some(1))).unwrap();
        }
        let store = open_store(dir.path());
        assert_eq!(store.first_retained_seq(), 2);
    }
}
```

Add `pub(crate) mod retention;` to the `src/store/mod.rs` module list.

Integration test `tests/master_retention.rs`:

```rust
mod common;

use std::time::Duration;

use common::{clock, config};
use replicated_rocksdb::{CfSpec, LogApi, Master};

/// The maintenance task trims the log and physically drops released CFs on its own.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn maintenance_runs_in_the_background() {
    let dir = tempfile::tempdir().unwrap();
    let c = clock();
    let mut cfg = config(dir.path(), &c);
    cfg.retention.max_entries = Some(1);
    cfg.retention.check_interval = Duration::from_millis(20);
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while m.first_retained_seq() != 6 {
        assert!(tokio::time::Instant::now() < deadline, "retention did not run");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(m.last_seq(), 6);
    m.shutdown().await.unwrap();

    // Shutdown released the DB: it can be reopened at once.
    let m = Master::open(config(dir.path(), &c)).await.unwrap();
    assert_eq!(m.first_retained_seq(), 6);
}
```

- [ ] **Step 2: Run the tests to confirm they fail**

Run: `cargo test --lib store::retention && cargo test --test master_retention`
Expected: FAIL to compile: `no method named trim_changelog`, `no method named guard`.

- [ ] **Step 3: Implement `src/store/retention.rs`** (above the test module)

```rust
//! Changelog retention (DESIGN.md §9). Runs on every node.

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError};

use rocksdb::{IteratorMode, WriteBatch, WriteOptions};

use super::meta::{self, CF_CHANGELOG};
use super::Store;
use crate::config::Retention;
use crate::error::Result;
use crate::types::{LogEntry, Seq};

#[derive(Debug, Default)]
pub(crate) struct GuardSet {
    next_id: u64,
    held: BTreeMap<u64, Seq>,
}

/// While held, retention keeps every entry with seq greater than the guarded seq.
#[derive(Debug)]
pub(crate) struct RetentionGuard {
    set: Arc<Mutex<GuardSet>>,
    id: u64,
}

impl Drop for RetentionGuard {
    fn drop(&mut self) {
        self.set.lock().unwrap_or_else(PoisonError::into_inner).held.remove(&self.id);
    }
}

impl Store {
    pub(crate) fn guard(&self, seq: Seq) -> RetentionGuard {
        let mut set = self.guards.lock().unwrap_or_else(PoisonError::into_inner);
        let id = set.next_id;
        set.next_id += 1;
        set.held.insert(id, seq);
        RetentionGuard { set: self.guards.clone(), id }
    }

    /// Deletes entries from the front of the changelog that are older than `max_age`
    /// (by the master's `ts_ms`) or beyond `max_entries`, never past a guard.
    /// `last_seq` is untouched. Returns how many entries were removed.
    pub(crate) fn trim_changelog(&self, now_ms: u64, r: &Retention) -> Result<u64> {
        let first = self.first_retained_seq();
        let last = self.position().last_seq;
        if first > last {
            return Ok(0);
        }
        // Entries in [first, cut) are deleted.
        let guard_limit = self
            .guards
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .held
            .values()
            .min()
            .map_or(last + 1, |s| s.saturating_add(1));
        let by_count = r.max_entries.map_or(first, |m| (last + 1).saturating_sub(m));
        let mut cut = by_count.max(first);
        if let Some(age) = r.max_age {
            let age_ms = u64::try_from(age.as_millis()).unwrap_or(u64::MAX);
            let start = cut.to_be_bytes();
            let cf = self.cf(CF_CHANGELOG);
            let iter = self.db().iterator_cf(&cf, IteratorMode::From(&start, rocksdb::Direction::Forward));
            for item in iter {
                let (k, v) = item?;
                let seq = meta::be64(&k)?;
                if seq >= guard_limit || LogEntry::decode(&v)?.ts_ms.saturating_add(age_ms) > now_ms {
                    break;
                }
                cut = seq + 1;
            }
        }
        let cut = cut.min(guard_limit).min(last + 1);
        if cut <= first {
            return Ok(0);
        }
        let cf = self.cf(CF_CHANGELOG);
        let mut batch = WriteBatch::default();
        batch.delete_range_cf(&cf, first.to_be_bytes(), cut.to_be_bytes());
        self.db().write_opt(batch, &WriteOptions::default())?;
        self.set_first_retained_seq(cut);
        self.db().compact_range_cf(&cf, None::<&[u8]>, Some(cut.to_be_bytes()));
        Ok(cut - first)
    }
}
```

In `src/store/mod.rs`, add the field `pub(crate) guards: Arc<Mutex<retention::GuardSet>>,` to `Store`, initialise it in `open` with `guards: Arc::new(Mutex::new(retention::GuardSet::default())),`, and add `Mutex` to the `std::sync` import.

- [ ] **Step 4: Implement the maintenance task**

Add to `impl Inner` in `src/master/writer.rs`:

```rust
    pub(crate) fn trim_changelog(&self) -> Result<u64> {
        self.store.trim_changelog(self.now_ms(), &self.cfg.retention)
    }
```

Create `src/master/maintenance.rs`:

```rust
//! Background work: retention, TTL reaping and physical CF drops.

use std::sync::Arc;
use std::time::Duration;

use tokio::task::JoinHandle;
use tokio::time::{interval_at, Instant, MissedTickBehavior};

use super::writer::Inner;
use crate::error::Result;

const CF_DROP_INTERVAL: Duration = Duration::from_secs(5);

pub(crate) fn spawn(inner: Arc<Inner>) -> JoinHandle<()> {
    let mut shutdown = inner.shutdown_tx.subscribe();
    tokio::spawn(async move {
        let every = |period: Duration| {
            let mut t = interval_at(Instant::now() + period, period);
            t.set_missed_tick_behavior(MissedTickBehavior::Delay);
            t
        };
        let mut retention = every(inner.cfg.retention.check_interval);
        let mut reaper = every(inner.cfg.reaper.interval);
        let mut cf_drops = every(CF_DROP_INTERVAL);
        loop {
            tokio::select! {
                _ = shutdown.changed() => break,
                _ = retention.tick() => run(&inner, "retention", |i| i.trim_changelog().map(drop)).await,
                _ = reaper.tick() => run(&inner, "reaper", |i| i.reap_round().map(drop)).await,
                _ = cf_drops.tick() => run(&inner, "cf-drop", |i| i.store.drop_unreferenced_cfs().map(drop)).await,
            }
        }
    })
}

async fn run(inner: &Arc<Inner>, task: &'static str, f: fn(&Inner) -> Result<()>) {
    let i = inner.clone();
    match tokio::task::spawn_blocking(move || f(&i)).await {
        Ok(Ok(())) => {}
        Ok(Err(e)) => tracing::warn!(task, error = %e, "maintenance task failed"),
        Err(e) => tracing::error!(task, error = %e, "maintenance task panicked"),
    }
}
```

In `src/master/mod.rs`, add `mod maintenance;`, and in `Master::open` replace `Ok(Master { inner, task: None })` with:

```rust
        let task = maintenance::spawn(inner.clone());
        Ok(Master { inner, task: Some(task) })
```

- [ ] **Step 5: Run the whole suite**

Run: `cargo test`
Expected: PASS: all unit, integration and doc tests, including 5 new retention unit tests and `maintenance_runs_in_the_background`.

- [ ] **Step 6: Lint and document**

Run: `cargo clippy --all-targets -- -D warnings && cargo doc --no-deps`
Expected: no warnings, docs build. Fix anything flagged (typically unused imports left over from earlier stubs).

- [ ] **Step 7: Commit**

```bash
git add src tests
git commit -m "feat: changelog retention with guards and background maintenance

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Out of scope for this plan (Plans 2 and 3)

These spec items are deliberately not in Plan 1. Their foundations are, as noted:

| Spec item | Plan | Foundation built here |
|---|---|---|
| Replication server: Handshake decision table, Tail, Bootstrap with lease, error model | 2 | `Store::{entries_after, read_snapshot, guard}`, `LogApi::subscribe`, `Digest::finish_bootstrap` |
| Auth (token, mTLS), TLS, per-method authorisation, auth rate limit | 2 | — |
| `Slave`: applier, epoch/CRC checks, GapPolicy, bootstrap install, persisted `node_state`, `clear_halt`, listener | 2 | `Store::{prepare, commit}` (same path), `LogEntry::{verify_crc, decode_ops}`, `Role::Slave` |
| `bump_epoch` ending streams with `UPSTREAM_RESET` | 2 | `Master::bump_epoch` |
| Verify sessions, range hashing, `ListRange` narrowing, `VerifyReport` | 3 | `Digest::{record, finish_range}`, `envelope::Envelope::header_len`, `OwnedSnapshot` |
| Observability (status APIs, spans) | 2–3 | `tracing` already in use |
