# replicated-rocksdb Replication Protocol

| | |
|---|---|
| Protocol version | **1** (`PROTOCOL_VERSION = 1`) |
| Envelope version | **1** |
| Protobuf package | `rrdb.replication.v1` |
| Status | Draft for implementation, 2026-10-09 |
| Companion document | [DESIGN.md](DESIGN.md) (storage, write path, crate API) |

## Abstract

This document specifies the wire protocol that replicated-rocksdb nodes use to replicate a key-value database from a single writable **master** to read-only **replicas**, either directly or through chains of replicas. The protocol transfers an ordered log of absolute operations, full-state snapshots for new or lagging replicas, and range digests for checking integrity. It runs over gRPC on HTTP/2, with TLS and bearer-token or mutual-TLS authentication.

This document is enough to build an interoperable client or server. It does not cover how a node stores data locally, except where that storage is visible on the wire: the value envelope, the digests and the log entry checksum.

## Contents

1. [Conventions and terminology](#1-conventions-and-terminology)
2. [Overview](#2-overview)
3. [Transport, metadata and authentication](#3-transport-metadata-and-authentication)
4. [Versioning and compatibility](#4-versioning-and-compatibility)
5. [Data formats](#5-data-formats)
6. [Handshake](#6-handshake)
7. [Tail](#7-tail)
8. [Bootstrap](#8-bootstrap)
9. [Verify](#9-verify)
10. [Error model](#10-error-model)
11. [Client behaviour](#11-client-behaviour)
12. [Timing and limits](#12-timing-and-limits)
13. [Security considerations](#13-security-considerations)
14. [Complete protobuf definition](#14-complete-protobuf-definition)
15. [Test vectors](#15-test-vectors)
16. [Example exchanges](#16-example-exchanges)

---

## 1. Conventions and terminology

The key words **MUST**, **MUST NOT**, **REQUIRED**, **SHALL**, **SHALL NOT**, **SHOULD**, **SHOULD NOT**, **RECOMMENDED**, **MAY** and **OPTIONAL** are to be read as described in BCP 14 ([RFC 2119], [RFC 8174]) when, and only when, they appear in all capitals.

| Term | Meaning |
|---|---|
| **Master** | The single node that accepts writes and assigns sequence numbers. It is the *origin* of the log. |
| **Replica** | A read-only node that applies the log. A replica MAY also serve other replicas. The crate API calls it a *slave*. |
| **Server** | The node answering an RPC: either the master or a replica that serves others. |
| **Client** | The replica that makes the RPC. It is the server's *downstream*, and the server is its *upstream*. |
| **Seq** | Unsigned 64-bit sequence number of a log entry. It is assigned by the master, starts at 1 and increases by exactly 1 per entry. Seq 0 means "nothing applied". |
| **Identity** | `(db_uuid, epoch)` of the origin master. It is the same on every node in a topology. |
| **Node id** | A node's configured `identifier`. It MUST be unique within a topology. |
| **Envelope** | The bytes a node stores for each user value (§5.3). |
| **`last_seq`** | The highest seq a node has committed (master) or applied (replica). |
| **`first_retained_seq`** | The lowest seq still in a node's log, or `last_seq + 1` if the log is empty. |
| **`BE64(x)`** | `x` as 8 bytes, unsigned big-endian. |
| **`‖`** | Byte concatenation. |

All integers in this document are unsigned. All timestamps are milliseconds since the Unix epoch (UTC).

## 2. Overview

```
              Handshake ─► decide RESUME / BOOTSTRAP / REJECT
                 │
       ┌─────────┴──────────┐
       ▼                    ▼
   Bootstrap            Tail (from last_seq + 1)
 (snapshot at S) ──────► Tail (from S + 1)
                            │
                            ▼  periodically, on a second connection
                  VerifyBegin ► VerifyRange* ► VerifyEnd
```

The service has seven RPCs:

| RPC | Shape | Purpose |
|---|---|---|
| `Handshake` | unary | Compare versions, identity and position, and tell the client what to do next. |
| `Tail` | bidi stream | Stream log entries from a given seq, with heartbeats; the client acknowledges what it has committed. Does not end on its own. |
| `Bootstrap` | server stream | Stream a consistent full snapshot of user data at a seq `S`. |
| `VerifyBegin` | unary | Open a verify session pinned to a snapshot at seq `S`. |
| `VerifyRange` | bidi stream | Compare digests of key ranges and list keys in ranges that differ. |
| `VerifyEnd` | unary | Close a verify session. |
| `Status` | unary | Report the server's identity, position and replicas. Read-only; for operators and promotion tooling. |

Properties the protocol relies on:

- **Absolute operations only.** Every operation sets state to a given value and never depends on prior state, so applying the log in order is deterministic.
- **One origin.** Seqs, epochs, timestamps and checksums are assigned by the master and forwarded byte for byte by every replica. A seq means the same thing on every hop.
- **Self-validating streams.** `Tail`, `Bootstrap` and `VerifyBegin` repeat the checks they depend on, so a server whose state changed after the `Handshake` is caught without extra coordination.

## 3. Transport, metadata and authentication

### 3.1 Transport

- The protocol runs over gRPC on HTTP/2. The service is `rrdb.replication.v1.Replication`.
- Servers SHOULD use TLS. TLS 1.3 is RECOMMENDED and TLS 1.2 is the minimum.
- A server MUST refuse to start with bearer-token authentication on a plaintext listener unless the operator explicitly allows it (`allow_plaintext_token`). That option is intended for loopback and tests only.
- Clients SHOULD enable HTTP/2 keepalive pings (RECOMMENDED: every 30 s, 10 s timeout).

### 3.2 Request metadata

A client MUST send these gRPC metadata entries on **every** call:

| Key | Value | Notes |
|---|---|---|
| `x-rrdb-protocol-version` | Decimal `PROTOCOL_VERSION`, e.g. `1` | The server MUST reject a mismatch with `FAILED_PRECONDITION` / `PROTOCOL_MISMATCH` on every RPC except `Handshake`, which reports it in the response (§6). |
| `x-rrdb-node-id` | The client's node id | 1–128 characters from `[A-Za-z0-9._-]`. Self-asserted; see §3.3. |
| `authorization` | `Bearer <token>` | Required when the server uses token authentication. |

A token MUST consist only of characters allowed by `b64token` in [RFC 6750] §2.1. Tokens SHOULD carry at least 256 bits of entropy.

### 3.3 Authentication and authorisation

- The server authenticates **each call** on its own. There is no session state, and a successful `Handshake` does not authorise later calls.
- Token comparison MUST run in constant time. A server MAY accept several tokens at once so they can be rotated.
- With mutual TLS, the peer's identity is taken from the client certificate. Otherwise `x-rrdb-node-id` is only a label for logs and status. It MUST NOT be used as the sole basis for authorisation.
- A server SHOULD authorise per RPC. `Bootstrap` and `VerifyRange` with `ListRange` expose every key, and `Bootstrap` and `VerifyRange` with `FetchKeys` or `FetchRange` expose every value. The method being called is available to the server's authenticator.
- `Status` exposes node ids, positions and epoch histories, but no keys or values.
- Failures: missing or invalid credentials → `UNAUTHENTICATED`. Valid credentials that are not allowed this RPC → `PERMISSION_DENIED`. The server MUST NOT say which part of the credentials was wrong.

### 3.4 Message compression

Message compression is optional and uses gRPC's own mechanism (`grpc-encoding`, `grpc-accept-encoding`).

- An implementation MUST accept `identity` and `zstd` on every RPC, for requests and responses alike. It MAY send `zstd`.
- A server compresses a response only when the client advertised `zstd` in `grpc-accept-encoding`. Request compression is not negotiated by gRPC: a client that sends a `zstd` request to a server that does not accept it fails.
- Upgrade order: upgrade every node to an implementation that accepts `zstd` before configuring any node to send it. Sending `identity` (no compression) is always safe.
- Compression happens below the protobuf layer and never changes message bytes after decompression, so CRCs, `ops` bytes and verbatim forwarding by slaves are unaffected. The two directions are independent, and chained nodes need not use the same setting.

## 4. Versioning and compatibility

- `PROTOCOL_VERSION` (a uint32) MUST match **exactly** between client and server. There is no negotiation and no range of supported versions. To upgrade, stop all nodes, upgrade them, and start them again.
- `envelope_version` MUST also match exactly, because envelopes are compared byte for byte (§5.3, §9).
- The software version of an implementation is never compared.
- **Frozen fields.** So that any two versions can always report a mismatch cleanly, these fields keep their number, name and type in every future protocol version:
  - `HandshakeRequest` fields 1–3 (`protocol_version`, `envelope_version`, `client_id`);
  - `HandshakeResponse` fields 1–6 (`protocol_version`, `envelope_version`, `server_id`, `decision`, `reason`, `detail`);
  - the values `REJECT = 3`, `PROTOCOL_MISMATCH = 4` and `ENVELOPE_MISMATCH = 5`.
- Field numbers MUST NOT be reused across versions. Removed fields MUST be marked `reserved`.
- Because versions match exactly, a peer never legitimately sends unknown fields or enum values. A receiver MUST treat an unset `oneof` or an unknown enum value in a `LogEntry`, `Op` or bootstrap chunk as corruption (`CORRUPT`). Elsewhere it SHOULD reject the message with `INVALID_ARGUMENT`.

## 5. Data formats

### 5.1 Log entry

```proto
message LogEntry {
  uint64  seq    = 1;
  uint64  epoch  = 2;
  uint64  ts_ms  = 3;   // master commit time; non-decreasing in seq
  bytes   ops    = 4;   // serialised OpList, opaque to forwarders
  fixed32 crc32c = 5;
}
```

- `ops` holds the bytes of a serialised `OpList` exactly as the master produced them. Protobuf encoding is not canonical across implementations, so **no node may re-encode `ops`**. Replicas store these bytes and forward them unchanged, together with `seq`, `epoch`, `ts_ms` and `crc32c`.
- `crc32c` is CRC-32C (Castagnoli, polynomial `0x1EDC6F41` in normal form, reflected, initial value and final XOR `0xFFFFFFFF`; the same CRC as [RFC 3720] and SSE4.2). It is computed over:

  ```
  BE64(seq) ‖ BE64(epoch) ‖ BE64(ts_ms) ‖ ops
  ```

- A receiver MUST check `crc32c` before decoding `ops`. A server SHOULD check it when reading an entry from local storage before forwarding it. On a mismatch the server fails the stream with `DATA_LOSS` / `CORRUPT` and does not forward the entry.
- `ts_ms` MUST NOT decrease as `seq` increases. Nodes use it for log retention, and the master uses it as the time base for TTL.

### 5.2 Operations

```proto
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
message DeleteRange { string cf = 1; bytes start = 2; bytes end = 3; }   // [start, end)
message CreateCf    { string name = 1; optional string comparator = 2; }
message DropCf      { string name = 1; }
```

Rules (a receiver that finds a violation MUST treat the entry as `CORRUPT`):

1. `ops` is non-empty.
2. An entry that contains `CreateCf` or `DropCf` contains **exactly one** op.
3. `cf` and `name` are logical column family (CF) names. They are 1–255 bytes of valid UTF-8 and MUST NOT start with `__`.
4. `cf` names a CF that exists at this seq. For `CreateCf`, `name` does not exist; for `DropCf`, it does.
5. For `DeleteRange`, `start` sorts strictly before `end` under the CF's comparator.

Apply semantics. All ops in one entry are applied atomically, in order:

| Op | Effect |
|---|---|
| `Put` | Store an envelope (§5.3) with `write_seq = entry.seq`, the TTL flag set if and only if `expire_at_ms` is present, and `value`. |
| `Delete` | Remove `key`. |
| `DeleteRange` | Remove every key `k` with `start ≤ k < end` under the CF's comparator. |
| `CreateCf` | Create CF `name` with `create_seq = entry.seq` and the given comparator. |
| `DropCf` | Remove CF `name` and all its data. |

`comparator` names a key ordering that every node MUST have registered under the same name. If none is given, the ordering is bytewise lexicographic. A node that receives an unknown comparator name MUST stop applying and halt with `SpecMismatch`.

There is no merge, increment, append, compare-and-set or any other operation whose result depends on prior state.

### 5.3 Value envelope

Every stored user value is wrapped in an envelope. Envelopes appear on the wire in `Bootstrap` records and are hashed by `Verify`.

```
offset  size  field
0       1     ver           = 1 (envelope_version)
1       1     flags         bit 0 = HAS_TTL; bits 1–7 reserved, MUST be 0
2       8     write_seq     BE64; seq of the entry that wrote this value
10      8     expire_at_ms  BE64; present if and only if HAS_TTL
10|18   …     value         user bytes, to the end
```

- A receiver MUST treat an envelope with an unknown `ver`, a reserved flag bit set, or a length shorter than its header as `CORRUPT`.
- The **header** is the first 10 bytes, or 18 with TTL. Quick verification (§9) hashes only the header.
- A value with `expire_at_ms ≤ now` is *expired*. Expired values are hidden from application reads, but they still exist, and they are still transferred and hashed until the master logs their `Delete`.

### 5.4 Digest framing

`Bootstrap` and `Verify` use BLAKE3 (32-byte output, default unkeyed mode) over a sequence of framed items:

| Item | Encoding |
|---|---|
| CF marker | `0x01 ‖ BE64(len(name)) ‖ name ‖ BE64(create_seq)` |
| Record | `0x02 ‖ BE64(len(key)) ‖ key ‖ BE64(len(payload)) ‖ payload` |
| Trailer | `0x03 ‖ BE64(snapshot_seq) ‖ BE64(record_count)` |

`name` is UTF-8. Records follow the CF's key order (its comparator).

- **Bootstrap digest** = BLAKE3 over, for each CF in `BootstrapHeader.cf_specs` order: its CF marker, then a record for every key in that CF with `payload = envelope`. Then one trailer. A CF with no keys still contributes its marker.
- **Range hash** (Verify) = BLAKE3 over the record items for the keys in the range, with `payload` set by the session mode (§9.3). There are no markers or trailer. An empty range hashes the empty input.

### 5.5 Shared messages

```proto
message Identity   { bytes db_uuid = 1; uint64 epoch = 2; }   // db_uuid: 16 bytes, RFC 9562 UUID
message CfSpec     { string name = 1; optional string comparator = 2; uint64 create_seq = 3; }
message EpochStart { uint64 epoch = 1; uint64 start_seq = 2; uint64 nonce = 3; }
```

Two identities are equal when both `db_uuid` and `epoch` are equal. In every message that carries a list of `CfSpec`, the list is sorted by `name`, compared as bytes in ascending order.

#### Epoch history

The **epoch history** of a database is the list of `EpochStart { epoch, start_seq, nonce }` records, one per epoch, sorted by `epoch` ascending. The first record is always `{1, 1, n}`. A new epoch starts **every time a master opens an existing database**, when the master bumps its epoch, and when a replica is promoted to master (§6.4). The new record's `start_seq` is the seq of the first entry written in the new epoch, which is the node's `last_seq + 1` at that moment; an epoch that took no writes shares its `start_seq` with the next one. Within one epoch there is exactly one writer process, so two histories can only differ from the start of an epoch on.

`nonce` is a random 64-bit value chosen when the epoch starts. It never takes part in ordering: which of two nodes is later is decided by the epoch number, then `last_seq`. It only decides **identity**: two records with the same `epoch` are the same epoch if and only if their nonces are equal (their `start_seq`s are then equal too). Two histories that hold the same epoch number with different nonces forked at the lower of the two `start_seq`s. (A restored backup that opens and starts epoch `e` again, or two promotions that chose the same number, are told apart this way.)

- `epoch_for(seq)` is the `epoch` of the last record whose `start_seq ≤ seq`. Every entry MUST satisfy `entry.epoch == epoch_for(entry.seq)`.
- The **fork point after epoch e**, `fork(e)`, is the `start_seq` of the first record whose `epoch > e`, or "none" if there is no such record. Entries with seq `< fork(e)` are shared by every history that contains epoch `e`. Entries at or after `fork(e)` may differ.
- Every node persists the epoch history together with its identity, and forwards it unchanged.

## 6. Handshake

```proto
rpc Handshake(HandshakeRequest) returns (HandshakeResponse);
```

`Handshake` has no side effects. A client MUST call it before each connection attempt, and SHOULD call it again after any stream ends.

### 6.1 Request

| Field | Meaning |
|---|---|
| `protocol_version`, `envelope_version` | The client's versions. |
| `client_id` | The client's node id. MUST equal `x-rrdb-node-id`. |
| `known_identity` | The origin identity the client has data for. Absent if the client has no data. |
| `last_seq` | The client's `last_seq` (0 if it has no data). |
| `last_crc32c` | The `crc32c` of the client's entry at `last_seq`, if known. Used to detect divergence. |
| `known_epochs` | The client's newest epoch records (`epoch`, `start_seq`, `nonce`; §5.5), oldest first and ending with its record of `known_identity.epoch`: the last 5, or all of them when its history is shorter. Empty if the client has no data. A client with data SHOULD send them: they let the server tell apart two epochs with the same number (§6.3) and prove continuity across an epoch change from the history alone, without any entry. A list that does not end with `known_identity.epoch` is ignored. Field 7 (formerly `known_epoch_start`) is reserved. |

### 6.2 Response

| Field | Meaning |
|---|---|
| `protocol_version`, `envelope_version`, `server_id` | The server's values. |
| `decision`, `reason`, `detail` | What the client must do, why, and human-readable text. |
| `upstream_chain` | Node ids between this server and the master, closest first. Empty on the master. |
| `identity` | The server's origin identity. Present unless `reason = NOT_READY`. |
| `epoch_history` | The server's epoch history (§5.5). |
| `cf_specs` | The server's current CFs. Informational only. |
| `first_retained_seq`, `last_seq` | The server's log window. |
| `origin_last_seq` | The highest master seq the server knows of. On the master it equals `last_seq`. |
| `max_entry_bytes` | The master's maximum encoded `LogEntry` size, forwarded down the chain. |
| `heartbeat_ms` | The interval at which the server sends Tail heartbeats. |

### 6.3 Decision table

Two derived values are used by rules 8–10:

- The client's epoch is **changed** when `known_identity.epoch < identity.epoch`, or when `known_identity.epoch = identity.epoch` and one of the records in `known_epochs` is not in the server's history (another `start_seq` or another `nonce`, or a number the server never had). In the second case the two histories share the epoch number but not the epoch (a restored backup that started the number again, or another promotion that chose it), and the client is treated exactly like one of an older epoch.
- The **client fork** `cfork` is `fork(known_identity.epoch)` in the server's history, lowered, for every record of `known_epochs` that is not in the server's history, to `min(its start_seq, start_seq of the server's record of the same number)` (if the server has no record of that number, the server's side of the minimum is omitted). Entries before `cfork` are shared; entries at or after it may differ. `cfork` is "none" only when the epoch is changed and the server's history has no later epoch and no differing record, which is an inconsistent history.
- **Continuity is proven by the history** when every record of `known_epochs` is in the server's history (same `epoch`, `start_seq` and `nonce`): every entry before `cfork` was written by the same processes on both sides.

The server evaluates these rules **in order** (rule 10a before rule 9) and applies the first that matches:

| # | Condition | `decision` | `reason` |
|---|---|---|---|
| 1 | `protocol_version` differs | `REJECT` | `PROTOCOL_MISMATCH` |
| 2 | `envelope_version` differs | `REJECT` | `ENVELOPE_MISMATCH` |
| 3 | `client_id` equals `server_id` or appears in `upstream_chain` | `REJECT` | `LOOP_DETECTED` |
| 4 | The server is bootstrapping itself, or replaying a repair (§11.2; it has no consistent state) | `REJECT` | `NOT_READY` |
| 5 | `known_identity` absent | `BOOTSTRAP` | `NO_LOCAL_STATE` |
| 6 | `known_identity.db_uuid ≠ identity.db_uuid` | `BOOTSTRAP` | `IDENTITY_CHANGED` |
| 7 | `known_identity.epoch > identity.epoch` | `REJECT` | `STALE_EPOCH` |
| 8 | The client's epoch is changed and `last_seq ≥ cfork` (or `cfork` is none) | `REJECT` | `DIVERGED` |
| 9 | The client's epoch is changed and continuity is proven: by the history (every record of `known_epochs` is in the server's history), or because the server holds entry `last_seq` and its `crc32c` equals `last_crc32c` | `RESUME` | `EPOCH_ADVANCED` |
| 10a | The client's epoch is changed, `last_crc32c` is present, the server holds entry `last_seq`, and its `crc32c` differs (checked before rule 9) | `REJECT` | `DIVERGED` |
| 10b | The client's epoch is changed and rule 9 did not prove continuity (neither the history nor the CRC does) | `BOOTSTRAP` | `IDENTITY_CHANGED` |
| 11 | `last_seq > server.last_seq` and the server is the master | `REJECT` | `DIVERGED` |
| 12 | `last_crc32c` present, the server still holds entry `last_seq`, and its `crc32c` differs | `REJECT` | `DIVERGED` |
| 13 | `last_seq + 1 < first_retained_seq` | `BOOTSTRAP` | `LOG_GAP` |
| 14 | otherwise | `RESUME` | `REASON_UNSPECIFIED` |

Notes:

- **Rule 7.** The client follows a newer epoch than the server. Following the server would mix histories. Either the server is a replica that has not yet learned the new epoch (temporary), or it is an old master that came back after a promotion (permanent). The client backs off and retries (§11.2).
- **Rules 8–10, epoch continuity.** After a master restart, an epoch bump or a promotion, a replica of an older epoch can keep its data if its history is a prefix of the server's. That holds when its last entry comes before the fork point and its newest records (`known_epochs`), nonces included, are in the server's history: both sides then hold entries written by the same processes. Such a client continues without a bootstrap (`EPOCH_ADVANCED`), even when the server no longer holds the client's last entry: it adopts the server's identity and history, which replaces its own record of a same-numbered epoch. Entry CRCs are a second line of defence: a client whose last entry is held by the server with a different `crc32c` is `DIVERGED` (rule 10a, checked first), and a matching `crc32c` of the client's last entry proves continuity by itself (rule 9), whether or not `known_epochs` was sent. Only records of epochs that hold entries on the client (`start_seq <= last_seq`) count for the history proof: an epoch that took no writes yet contains nothing the server could contradict. A client that has entries at or past the fork point holds writes the new history does not have (`DIVERGED`). Any other client of a changed epoch, one that rule 9 proves neither by history nor by CRC (for example: it sent no usable records, and the server does not hold its last entry or it sent no `last_crc32c`), is treated as having a changed identity: `BOOTSTRAP` / `IDENTITY_CHANGED` (rule 10b), never `RESUME`. A malformed `known_epochs` (epochs not strictly increasing, or not ending with `known_identity.epoch`) is `INVALID_ARGUMENT`.
- **Same epoch number, another epoch (`known_epochs`).** Epoch numbers are not globally coordinated: a master restored from a backup starts again a number its replicas may already have followed past the restore point, a promotion that was undone (`demote`) may have handed its number to replicas that followed it, and another node may later start the same number. Without `known_epochs` such a client would get a plain `RESUME` (equal identities) and then receive entries whose epoch contradicts its history. With them, the server sees that a record differs (start seq or nonce) and applies rules 8–10 with the fork at the lower of the two start seqs; comparing up to five records also catches histories that forked at an older epoch and later reached the same numbers. A server that receives no `known_epochs` compares identities only.
- **Rule 11.** The master commits with fsync, so a replica can never be ahead of it legitimately. If one is, the master has lost history, typically because it was restored from a backup. Since every master open starts a new epoch, rule 8 normally catches this first; rule 11 remains as a backstop. Accepting the replica would let the same seq carry different writes on different nodes.
- **Rule 14 with a replica server.** When the server is a replica, a client can legitimately be ahead of it: the server may have crashed after forwarding entries it had not yet flushed. The answer is still `RESUME`, and the client's Tail waits until the server catches up (§7.3).
- **Rules 9, 10a and 12, and Tail's `prev_crc32c` check.** "The server holds entry `n`" includes the case where `n` is the server's own `last_seq` and the server knows that entry's `crc32c` from its own position, even if its log no longer contains the entry (right after the server bootstrapped, or after retention emptied its log).
- `BOOTSTRAP` for `IDENTITY_CHANGED` or `LOG_GAP`, and `REJECT` for `DIVERGED`, are subject to the client's local policies (§11.2). The client may halt or bootstrap.
- A server that is itself halted still answers normally. Its data is consistent but no longer advances.

### 6.4 Promotion (informative)

A replica is promoted to master by an operator, after the old master has been stopped. The promoted node starts epoch `e' = 1 + max(highest epoch in its history, highest epoch it gave up by demoting, highest epoch reported by the peers it checked)`, appends `{e', last_seq + 1, fresh nonce}` to its history, and begins accepting writes at `last_seq + 1`. Its identity keeps the same `db_uuid`. An epoch number SHOULD never be reused: a node that undoes a promotion remembers the number it gave up (`max_burned_epoch`, reported by `Status`), and promotion tooling chooses a number above every epoch any reachable peer reports (identity, every history record and `max_burned_epoch`). A promotion that checks no peers can still choose a number that some unreachable replica already holds; `known_epochs` (§6.3) then detects the clash by its nonce.

Other replicas are then pointed at the new master, directly or through a chain. Under §6.3:

- Replicas at or behind the promoted node's `last_seq`, with matching entries, continue at once (rule 9).
- Replicas that had received entries the promoted node never saw are at or past the fork point and get `DIVERGED` (rule 8). Their divergence policy decides whether they halt or bootstrap. When an operator explicitly re-points a replica, an implementation SHOULD NOT discard its divergent entries without an explicit confirmation.
- The old master, if restarted, starts an epoch of its own (§5.5). A new-epoch replica that asks it gets `STALE_EPOCH` (rule 7) when the old master's number is lower, and `DIVERGED` otherwise (its record of that number has another nonce, or the old master never had the replica's epoch). It can rejoin only as a new replica with an empty directory.

The protocol cannot stop an operator from running two masters at once. If two nodes both start the same epoch number, their records differ by nonce (and possibly start seq), and `known_epochs` makes the handshake treat them as different epochs (§6.3), wherever the conflicting entries are, retained or not.

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
| `node_state` | `"ready"`, `"bootstrapping"`, `"halted:<Reason>"` or `"repairing:<seq>"` (a repair is replaying up to `seq`; the node answers `NOT_READY` until then). |
| `upstream` | The address the node replicates from; empty on the master. |
| `replicas` | One `ReplicaSummary { node_id, applied_seq, in_sync }` per open Tail stream (§7.6). |
| `max_burned_epoch` | The highest epoch number this node started as a master and then gave up by undoing the promotion or by rejoining as a slave with empty trailing epochs (0 if none). Replicas that followed that promotion may hold the number, so a node being promoted SHOULD choose a higher one (§6.4). |
| `probe_crc` | Present only if the request set `probe_seq`: the `crc32c` of the server's entry at `probe_seq`, if it holds it (its own `last_entry_crc` counts for `probe_seq = last_seq`, as in §6.3). Absent when the entry is not held (trimmed, before the server's bootstrap, or past its `last_seq`). |

Comparing the `Status` of several replicas shows which one holds the most of the log (newest epoch, then highest `last_seq`) and which ones have diverged from it. Two nodes are compatible only if no seq both hold lies at or past a point where their epoch histories differ, **and** the longer node holds the shorter node's last entry with the same `crc32c` — which a tool learns by asking the longer node with `probe_seq` = the shorter node's `last_seq`. Only a proven CRC mismatch shows divergence; when the longer node no longer holds that entry (no `probe_crc`), compatibility is *unverifiable*, and tools SHOULD NOT treat such a node as safe to ignore. Nothing in the protocol acts on it automatically.

## 7. Tail

```proto
rpc Tail(stream TailClientMessage) returns (stream TailMessage);
```

Tail is a bidirectional stream. The client's first message is `start`, carrying a `TailRequest` (§7.1); every later client message is an `ack` (§7.6). The server sends entry batches and heartbeats (§7.3).

**The client owns its position.** The server keeps no per-client cursor, offset or session between calls. Every resume, whether after a restart, a dropped connection or a bootstrap, is the client stating where it is:

1. `Handshake` with `last_seq` (its durable `last_seq`) and `last_crc32c` (the CRC of that entry).
2. If the decision is `RESUME`: `Tail` with `from_seq = last_seq + 1` and `prev_crc32c = last_crc32c`.

Because the client reads `last_seq` from durable storage, which is written atomically with the applied entries (§7.4), a resume never skips an entry. If it re-receives entries, they are discarded by the `entry.seq ≤ last_seq` check (§7.4). After a bootstrap at `S`, the client's `last_seq` is `S`, so the same rule gives `from_seq = S + 1`.

### 7.1 Request

```proto
message TailClientMessage { oneof kind { TailRequest start = 1; TailAck ack = 2; } }
```

The first client message MUST be `start`. The server ends the call with `INVALID_ARGUMENT` if it is anything else, and SHOULD do so if it has not arrived within 10 s. The fields of `start`:

| Field | Meaning |
|---|---|
| `client_id` | As in the handshake. |
| `identity` | The origin identity the client follows. REQUIRED. |
| `from_seq` | The first seq the client wants. Normally its `last_seq + 1`. |
| `prev_crc32c` | The `crc32c` of the client's entry at `from_seq − 1`, if known. |
| `bootstrap_lease` | The `lease_id` from a `Bootstrap` this client just completed, if any (§8.4). |
| `min_batch_bytes` | Fetch batching (§7.5). The server delays sending until at least this many bytes of entries are ready. 0 (default) = send each entry as soon as it is available. |
| `max_wait_ms` | Fetch batching (§7.5). The longest the server delays a ready entry while waiting for `min_batch_bytes`. Ignored when `min_batch_bytes = 0`. |
| `max_batch_bytes` | Fetch batching (§7.5). The most entry bytes the server puts in one `EntryBatch`. 0 = server default. |

### 7.2 Server validation

Before sending anything, the server MUST check the following and fail the call on the first that matches:

| Condition | Status / reason |
|---|---|
| The server is bootstrapping itself, or replaying a repair | `UNAVAILABLE` / `NOT_READY` |
| `identity ≠ server identity` | `FAILED_PRECONDITION` / `IDENTITY_CHANGED` |
| The server is the master and `from_seq > last_seq + 1` | `FAILED_PRECONDITION` / `DIVERGED` |
| `prev_crc32c` present, the server holds entry `from_seq − 1`, and its `crc32c` differs | `FAILED_PRECONDITION` / `DIVERGED` |
| `from_seq < first_retained_seq` | `OUT_OF_RANGE` / `LOG_GAP` |

If `bootstrap_lease` matches a lease this server issued to the same `client_id`, and `from_seq ≤ snapshot_seq + 1`, the stream takes the lease over and the server releases it as soon as the stream has read the entries following the snapshot from its log (or when the stream ends). Releasing it earlier would let aggressive retention remove entry `snapshot_seq + 1` before the stream reads it.

### 7.3 Stream

```proto
message TailMessage { oneof kind { EntryBatch batch = 1; Heartbeat heartbeat = 2; } }
message EntryBatch  { repeated LogEntry entries = 1; }
message Heartbeat   { uint64 upstream_last_seq = 1; uint64 origin_last_seq = 2; uint64 ts_ms = 3; }
```

- Entries are sent in `EntryBatch` messages. Across all batches of a stream, entries are in strictly consecutive seq order, starting at `from_seq`, with no gaps or duplicates. A batch is never empty. When batches are formed is governed by §7.5.
- If `from_seq > server.last_seq + 1` (only possible when the server is a replica), the server sends heartbeats and no entries until its own `last_seq` reaches `from_seq − 1`, then continues normally. The **junction** is checked first: if the client sent `prev_crc32c` and the server did not hold entry `from_seq − 1` at validation, the server compares that entry's `crc32c` with `prev_crc32c` as soon as it holds it, and ends the stream with `FAILED_PRECONDITION` / `DIVERGED` on a mismatch, before sending `from_seq`.
- The server sends a `Heartbeat` whenever it has sent no message for `heartbeat_ms`. It SHOULD also send one at least every `heartbeat_ms` while it is streaming entries, so that `origin_last_seq` stays current.
- `upstream_last_seq` is the server's `last_seq`. `origin_last_seq` is the master's `last_seq` as last known to the server. `ts_ms` is the server's wall clock and is informational only.
- The stream does not end on its own. The server ends it:
  - with `OUT_OF_RANGE` / `LOG_GAP` if the next entry to send has been removed by retention (slow consumer);
  - with `UNAVAILABLE` / `UPSTREAM_RESET` if the server starts bootstrapping itself, commits a repair (§11.2), or its identity changes (for example an epoch bump on the master);
  - with `UNAVAILABLE` / `SHUTTING_DOWN` on a graceful shutdown;
  - with `DATA_LOSS` / `CORRUPT` if a stored entry fails its CRC;
  - with `FAILED_PRECONDITION` / `DIVERGED` if the junction check above fails;
  - with `INVALID_ARGUMENT` if a client message breaks the rules of §7.6.
- A stream that ends without a status (a clean end of stream) carries no decision. The client MUST treat it as transient: back off and handshake again (§11.4).
- Flow control is HTTP/2 flow control. A server MUST NOT drop entries to keep up with a slow consumer. It applies back-pressure until retention forces `LOG_GAP`.

### 7.4 Client checks on each entry

For each `entry` of each batch, in order, the client MUST check, in order:

1. `crc32c` (§5.1). Failure → `Corrupt`.
2. `entry.epoch == epoch_for(entry.seq)` under the client's epoch history (§5.5). On failure the client ends the stream and handshakes again **after its reconnect backoff** (§11.4), surfacing the mismatch in its status. A server ends its streams when its own identity changes (`UPSTREAM_RESET`), and the handshake has already reconciled both histories, so such an entry contradicts the shared history; retrying at once would loop against a misbehaving upstream.
3. If `entry.seq ≤ last_seq`, skip the entry. This happens only right after a reconnect.
4. If `entry.seq ≠ last_seq + 1` → `LogGap`.
5. Decode `ops` and check the rules in §5.2. Failure → `Corrupt`.

Then it applies the entry atomically, together with `last_seq` and a copy of the entry (§11.1). An entry is never split across writes. Consecutive entries of one batch MAY share one atomic write, whose `last_seq` is that of the last of them; a failing check ends such a write before the failing entry. Either way, a crash always leaves the client at a clean entry boundary.

### 7.5 Fetch batching

The client chooses the trade-off between latency and batching per stream, in the spirit of Kafka's `fetch.min.bytes`, `fetch.max.wait.ms` and `fetch.max.bytes`. The size of an entry is its encoded `LogEntry` size; a batch's size includes the per-entry framing within `EntryBatch` (field tag and length prefix).

The server keeps a *pending* batch of entries that are available (committed on the server, at or after the next seq to send) and not yet sent. It sends the pending batch as soon as any of these holds:

1. `min_batch_bytes = 0` and the pending batch is non-empty (real-time mode);
2. the pending batch's size is at least `min_batch_bytes`;
3. `max_wait_ms` has elapsed since the oldest pending entry became available to the stream;
4. adding the next available entry would exceed `max_batch_bytes`.

A batch never exceeds `max_batch_bytes`, except that a single entry larger than it is sent alone. When the client is far behind, every available entry is "ready" immediately, so batches are full-size and catch-up is not slowed by `max_wait_ms`.

The server clamps the client's values to its own limits: `max_wait_ms` to at most `heartbeat_ms`, and `max_batch_bytes` to at most `max_tail_batch_bytes` (RECOMMENDED default 4 MiB). If `max_batch_bytes` is 0, it uses 1 MiB. Heartbeats are unaffected: a stream with nothing pending still gets one every `heartbeat_ms`.

### 7.6 Acknowledgements

```proto
message TailAck { uint64 applied_seq = 1; }
```

After `start`, the client sends `ack` messages on the same stream. `applied_seq` means: every entry with seq ≤ `applied_seq` is committed in the client's store (written to its write-ahead log; also fsynced if the client fsyncs its writes). Until the first ack the server assumes `applied_seq = from_seq − 1`.

- The client SHOULD send an ack after applying each `EntryBatch`, coalescing acks that back up (only the latest `applied_seq` matters), and MUST send one at least once per `heartbeat_ms`, so that a replica with nothing to apply stays in sync.
- `applied_seq` MUST NOT decrease and MUST NOT exceed the last seq the server sent on this stream. A second `start` is not allowed either. The server ends a stream that breaks one of these rules with `INVALID_ARGUMENT`.
- Acks never change what the server sends: it does not resend or skip entries because of them. On a reconnect the client's position is still its own durable `last_seq` (§7).
- Per open stream the server tracks the client's node id and address, whether the id was verified (mutual TLS), the epoch the client follows, when the stream opened, the last seq sent, the last seq acknowledged, the last time `applied_seq` reached the server's `last_seq`, and the stream's fetch settings.
- A replica is **caught up** as of the moment the server sent it a batch that held everything the server had (the batch ended at the server's `last_seq` when it was sent), once an ack covers that batch; also when an ack reaches the server's `last_seq` directly. This is Kafka's "caught up to the end offset at fetch time": a replica that keeps acknowledging what it is sent stays caught up even while writes keep the server ahead of its latest ack.
- A replica is **in sync** while its stream is open, it follows the server's current identity, and it was caught up within the last `replica_lag_time_max` (RECOMMENDED 10 s). A server MUST keep `heartbeat_ms` at most half of `replica_lag_time_max`, so that an idle replica acknowledging once per heartbeat stays in sync. A master MAY make a write wait until a number of in-sync replicas have acknowledged it; it SHOULD count each replica node id once, by its best open stream.

## 8. Bootstrap

```proto
rpc Bootstrap(BootstrapRequest) returns (stream BootstrapChunk);
```

### 8.1 Server procedure

1. If the server is bootstrapping itself or replaying a repair, fail with `UNAVAILABLE` / `NOT_READY`. If it is already serving `max_concurrent_bootstraps` bootstraps, fail with `RESOURCE_EXHAUSTED` / `BUSY`.
2. Take a consistent point-in-time snapshot of the database, without blocking writers.
3. Read through that snapshot: `S = last_seq`, the identity, the CF list, and the `crc32c` of entry `S` if known.
4. Register a **retention guard**: no entry with seq greater than `S` may be removed while the guard is held. Then issue a lease (§8.4).
5. Send exactly one `BootstrapHeader`, then zero or more `BootstrapRecords`, then exactly one `BootstrapFooter`. Every value comes from the snapshot.

### 8.2 Messages

```proto
message BootstrapRequest { string client_id = 1; }

message BootstrapChunk {
  oneof kind { BootstrapHeader header = 1; BootstrapRecords records = 2; BootstrapFooter footer = 3; }
}
message BootstrapHeader {
  Identity identity                = 1;
  uint64   snapshot_seq            = 2;   // S
  optional fixed32 snapshot_crc32c = 3;   // crc32c of entry S, if known
  repeated CfSpec cf_specs         = 4;   // sorted by name
  uint64   max_entry_bytes         = 5;
  uint32   envelope_version        = 6;
  bytes    lease_id                = 7;   // 16 random bytes
  uint64   lease_ttl_ms            = 8;
  repeated EpochStart epoch_history = 9;
}
message Record           { bytes key = 1; bytes envelope = 2; }
message BootstrapRecords { string cf = 1; repeated Record records = 2; }
message BootstrapFooter  { uint64 record_count = 1; bytes digest = 2; }   // digest: 32 bytes
```

Ordering and size:

- CFs are sent in `cf_specs` order, and the records of one CF are contiguous. Within a CF, keys are in strictly ascending comparator order.
- A `BootstrapRecords` message SHOULD be about `bootstrap_chunk_bytes` (RECOMMENDED: 1 MiB) and MUST contain at least one record. A single record may make it larger.
- Every user key in the snapshot is sent, including expired values that have not been deleted yet (§5.3).
- `record_count` is the total number of records. `digest` is the bootstrap digest (§5.4).

### 8.3 Client procedure

1. Persist that it is bootstrapping. From then on it MUST answer reads with "not ready", and as a server it answers with `NOT_READY` (§6.3 rule 4).
2. End every stream it serves downstream with `UNAVAILABLE` / `UPSTREAM_RESET`.
3. Delete all local user data and its local log.
4. Create the CFs from `cf_specs` and write records as they arrive, checking the order and that every `cf` appears in `cf_specs`. Check each envelope (§5.3).
5. When the footer arrives, check `record_count` and `digest`. On a mismatch, discard everything and start again.
6. In one atomic write, commit: identity, epoch history, CF list, `last_seq = S`, `last_crc32c = snapshot_crc32c` (if present) and state *ready*.
7. Call `Tail` with `from_seq = S + 1` and `bootstrap_lease = lease_id`.

If the client crashes in any step before 6, it MUST start the bootstrap again from the beginning.

### 8.4 Retention guard and lease

The guard registered in §8.1 step 4 MUST be kept until the first of these happens:

- the stream fails before the footer is sent;
- a `Tail` from the same `client_id` that presented the `lease_id` with `from_seq ≤ S + 1` has read the entries after `S` from the log, or has ended (§7.2);
- `lease_ttl_ms` passes after the footer is sent (RECOMMENDED: 15 min).

Without the lease, retention could remove entry `S + 1` in the window between bootstrap and tail, and the client would have to bootstrap again, possibly forever.

## 9. Verify

Verify lets a replica check that its user data equals its upstream's at the same seq, without transferring the data.

### 9.1 Session

```proto
rpc VerifyBegin(VerifyBeginRequest) returns (VerifyBeginResponse);
rpc VerifyEnd(VerifyEndRequest)     returns (VerifyEndResponse);

enum VerifyMode { VERIFY_MODE_UNSPECIFIED = 0; QUICK = 1; FULL = 2; }

message VerifyBeginRequest  { string client_id = 1; VerifyMode mode = 2; }
message VerifyBeginResponse { bytes session_id = 1; Identity identity = 2; uint64 snapshot_seq = 3;
                              repeated CfSpec cf_specs = 4; uint64 session_timeout_ms = 5;
                              repeated EpochStart epoch_history = 6; }
message VerifyEndRequest    { bytes session_id = 1; }
message VerifyEndResponse   {}
```

1. The server takes a snapshot, reads `S`, its identity, its epoch history and the CF list through it, and returns them with a random 16-byte `session_id`. (A repair stores that epoch history, the one the data at `S` belongs to; it falls back to a bootstrap if it differs from the handshake's.) It fails with `RESOURCE_EXHAUSTED` / `BUSY` if it already has `max_verify_sessions` open.
2. If the client's identity differs from `identity`, the client ends the session. Otherwise the client captures a snapshot of its own state **at exactly seq `S`**: it applies entries up to `S` and takes the snapshot before applying `S + 1`. If it is already past `S`, it calls `VerifyEnd` and retries later.
3. The client compares the CF lists. Any difference is a mismatch, and only CFs present on both sides are compared further.
4. The client sends range requests (§9.2) on a `VerifyRange` stream.
5. The client calls `VerifyEnd`. The server also closes a session that has been idle for `session_timeout_ms`. Any later use of that session fails with `NOT_FOUND` / `SESSION_NOT_FOUND`.

### 9.2 Range requests

```proto
rpc VerifyRange(stream VerifyRangeRequest) returns (stream VerifyRangeResponse);

message KeyRange  { string cf = 1; optional bytes lower = 2; optional bytes upper = 3; }  // [lower, upper)
message HashRange { KeyRange range = 1; uint64 count = 2; bytes hash = 3; }
message ListRange { KeyRange range = 1; uint32 limit = 2; }

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

- A `KeyRange` is half-open, `[lower, upper)`, under the CF's comparator. An absent `lower` means unbounded below, and an absent `upper` means unbounded above. An empty byte string is a valid key and is **not** the same as absent.
- **The client MUST cover each CF with contiguous ranges:** the first range has no `lower`, each range's `upper` is the next range's `lower`, and the last range has no `upper`. A key that exists only on the server can then never fall between two ranges.
- `HashRange`: the client sends its own `count` and range hash (§5.4). The server computes both over the same range in its snapshot and replies with `matched = (count and hash both equal)`, plus its own `count` and `hash`.
- `ListRange`: the server returns up to `limit` keys from the range, in order, each with its `write_seq` and `payload_hash = BLAKE3(payload)`. `truncated` is set if there are more. `limit` MUST be between 1 and 1024. The server MAY return fewer than `limit` keys to bound the size of the answer (RECOMMENDED: stop once the keys add up to 1 MiB), but it MUST return at least two keys when the range holds two or more.
- To continue a truncated listing, the client sends the same range with `lower` set to the last key it received, and skips that key when the answer repeats it first. This works under any comparator (appending a byte to a key gives its successor only under bytewise order), and the two-key rule guarantees progress.
- A request whose `cf` is not in the session's snapshot, whose `range` or `kind` is missing, or whose `limit` is out of bounds fails with `INVALID_ARGUMENT`. A `VerifyBegin` with `mode = VERIFY_MODE_UNSPECIFIED` fails the same way.
- When its state is replaced (it starts bootstrapping itself, commits a repair, or its identity changes), the server ends every `VerifyRange` stream with `UNAVAILABLE` / `UPSTREAM_RESET`, as it does for `Tail` (§7.3), and drops every session. A later request on a dropped session fails with `UPSTREAM_RESET` or `SESSION_NOT_FOUND`; a client MUST treat both as "start a new run".
- `FetchKeys` (repair): the server returns, in the order asked, a `FetchedRecord` per key with its whole envelope as of the session's snapshot, or without `envelope` when the key does not exist. `keys` MUST NOT be empty and MUST NOT hold more than 1024 keys (`INVALID_ARGUMENT` otherwise). The server MAY answer only a prefix of the keys (setting `truncated`) to bound the answer (RECOMMENDED: stop once ~1 MiB of envelopes is reached), but answers at least one; the client asks again for the rest.
- `FetchRange` (repair): like `ListRange`, but each record carries the whole envelope. The same limits, paging rule and two-record minimum apply.
- `EntryCrcs` (repair): `from_seq ≤ to_seq ≤ snapshot_seq` is required (`INVALID_ARGUMENT` otherwise). The server returns `(seq, crc32c)` for every entry with `from_seq ≤ seq ≤ min(to_seq, from_seq + 1023)` that it held at the snapshot (entry `snapshot_seq` is known whenever the server knows its CRC: from the log, or from `last_entry_crc` after retention emptied the log or a bootstrap that carried one; it is not known for `snapshot_seq = 0`, nor after a bootstrap without a CRC, and is then omitted). They are read through the session's snapshot, so they never change during the session; a client compares them with its own entries (repair checks the entry before the fork its epoch history shows).
- Envelopes expired but not yet reaped are returned like any other: repair copies what is stored.
- The client MAY pipeline up to `verify_window` requests (RECOMMENDED: 16). The server answers in request order, and `request_id` echoes the request.

### 9.3 Modes

| Mode | `payload` per record | Detects |
|---|---|---|
| `QUICK` | Envelope header (bytes 0–9, or 0–17 with TTL) | Missing, extra or stale keys; TTL differences |
| `FULL` | Whole envelope | All of the above, plus corrupted values |

### 9.4 Narrowing a mismatch (informative)

The client chooses the range boundaries, for example every `chunk_keys` keys or `chunk_bytes` bytes of its own data. When a range does not match, it splits the range at its median local key and hashes both halves. Once a range holds at most `min_chunk_keys` local keys, or cannot be split further, it sends `ListRange` and compares the keys directly. That last step also finds keys that exist only on the server. Clients SHOULD limit their own read rate (`max_bytes_per_sec`), and servers MAY limit theirs. A client SHOULD NOT use very long keys as range boundaries, so that every request stays far below the server's request size limit; this implementation never uses a key longer than 1 MiB as a boundary, and continues a truncated listing only from a key of at most 2 MiB (otherwise it reports the rest of the range as not compared).

## 10. Error model

RPC failures use standard gRPC status codes. The status MUST carry a `google.rpc.ErrorInfo` detail with `domain = "rrdb.replication.v1"` and `reason` set to the name of a `Reason` value.

| `Reason` | gRPC status | Retry? | Meaning |
|---|---|---|---|
| `PROTOCOL_MISMATCH` | `FAILED_PRECONDITION` | no | Protocol version differs (non-Handshake RPCs). |
| `ENVELOPE_MISMATCH` | `FAILED_PRECONDITION` | no | Envelope version differs. |
| `NOT_READY` | `UNAVAILABLE` | yes, with backoff | The server is bootstrapping itself, or replaying a repair. |
| `IDENTITY_CHANGED` | `FAILED_PRECONDITION` | after a new handshake | The client's identity is not the server's. |
| `DIVERGED` | `FAILED_PRECONDITION` | per divergence policy | The client's history disagrees with the server's. |
| `STALE_EPOCH` | `FAILED_PRECONDITION` | yes, with backoff | The server is behind the client's epoch: a replica that has not caught up yet, or a demoted master. |
| `LOG_GAP` | `OUT_OF_RANGE` | after a new handshake | The requested entries are no longer retained. |
| `LOOP_DETECTED` | `FAILED_PRECONDITION` | no | The client is upstream of the server. |
| `BUSY` | `RESOURCE_EXHAUSTED` | yes, with backoff | Concurrency limit reached, or the caller's address is throttled after too many failed authentications. |
| `SESSION_NOT_FOUND` | `NOT_FOUND` | start a new session | Unknown or expired verify session. |
| `UPSTREAM_RESET` | `UNAVAILABLE` | after a new handshake | The server's state was replaced. |
| `SHUTTING_DOWN` | `UNAVAILABLE` | yes, with backoff | Graceful shutdown. |
| `CORRUPT` | `DATA_LOSS` | client: yes, with backoff | Stored data failed an integrity check on the server. The client's own data is intact, so it backs off and retries; the server's operator must repair it. |

Authentication failures use `UNAUTHENTICATED` or `PERMISSION_DENIED` without `ErrorInfo` (§3.3). A server that throttles an address after repeated authentication failures MUST answer the throttled calls with `RESOURCE_EXHAUSTED` / `BUSY`, not `UNAUTHENTICATED`: their credentials were not checked, and a client halts on `UNAUTHENTICATED`, so a flood of bad credentials from a shared address would otherwise stop the good replicas behind it. A malformed request uses `INVALID_ARGUMENT`, and so does a Tail client that breaks the message order or the ack rules (§7.1, §7.6).

## 11. Client behaviour

### 11.1 Persistent state

A conforming client keeps, and updates atomically with every applied entry (or run of consecutive entries applied in one write, §7.4) or completed bootstrap:

- `identity`, epoch history, `last_seq`, `last_crc32c`;
- the CF list;
- a copy of every applied entry (the exact `LogEntry` fields), kept for retention and for serving downstream;
- its node state: *ready*, *bootstrapping*, or *halted(reason)*;
- while a repair replays, its repair target `S`, persisted apart from the node state (a halt does not clear it): the client is not ready, whatever its node state, until `last_seq` reaches `S`.

### 11.2 Acting on a decision

| Outcome | Client action |
|---|---|
| `RESUME` | `Tail(from_seq = last_seq + 1)`. |
| `RESUME` / `EPOCH_ADVANCED` | Persist the server's identity and epoch history atomically, then `Tail(from_seq = last_seq + 1)`. |
| An epoch history (handshake or bootstrap header) that is empty, does not start with `{1, 1}`, has non-increasing epochs or decreasing start seqs, or does not end with the identity's epoch; or a local failure to apply or commit a received entry (an unknown comparator → `SpecMismatch`) | Halt with `Corrupt` (persisted). Exceptions, where nothing was committed: a storage I/O failure (disk full, a device error), and `CfInUse` (a reader's snapshot still holds a CF with that physical name and a different spec): back off, retry, report in `last_error`. |
| `BOOTSTRAP` / `NO_LOCAL_STATE` | Bootstrap (§8.3). |
| `BOOTSTRAP` / `IDENTITY_CHANGED` | Identity policy *Refuse* (default) → halt `IdentityChanged`. *Rebootstrap* → bootstrap. |
| `BOOTSTRAP` / `LOG_GAP` | Gap policy *Rebootstrap* (default) → bootstrap. *Halt* → halt `LogGap`. |
| `REJECT` / `DIVERGED` | Divergence policy *Halt* (default) → halt `Diverged`. *Repair* → repair from a verify session on the same server (the fork `F` from the epoch histories, `EntryCrcs` to check entry `F − 1`, `FetchKeys` / `FetchRange` for the keys the local entries `F..=L` touched, all at the session's `snapshot_seq` `S`; §9.2), then rewind to `F − 1` in one atomic write and `Tail` from `F`; the client is not ready until `last_seq` reaches `S`. Anything it cannot prove falls back to a bootstrap. *Rebootstrap* → bootstrap. |
| `RESUME` while a repair replays up to `S`, with a server epoch history that leaves the client's at or below `S` | Treat as `REJECT` / `DIVERGED`: the keys the repair took hold that server's state at `S` of a history the server no longer has, so replaying it cannot complete them. |
| `REJECT` / `STALE_EPOCH` | Back off and retry the handshake. An upstream replica may still be catching up; a master (empty `upstream_chain`) never will, and the operator must re-point the replica. The client SHOULD surface the condition in its status. When the operator explicitly re-points the client at that master **and consents to discard data**, the client MAY treat the answer as `DIVERGED` (its newer epoch holds history the master lacks) and resolve it as consented; it MUST NOT do so automatically. |
| `REJECT` / `PROTOCOL_MISMATCH`, `ENVELOPE_MISMATCH`, `LOOP_DETECTED` | Halt with that reason. Not persisted: re-evaluated on restart. |
| `REJECT` / `NOT_READY` | Back off and retry the handshake. |
| A call or stream fails with `CORRUPT` (the server's stored data) | Back off and retry. The client's data is not affected. |
| A stream ends with a retryable error or without any status, or the connection is lost | Back off and handshake again. |

A halted client keeps serving its last consistent data to readers and to downstream replicas. Halts caused by data (`IdentityChanged`, `Diverged`, `LogGap`, `Corrupt`, `SpecMismatch`) MUST be persisted and survive a restart until an operator clears them.

### 11.3 State machine

```
                   ┌─ RESUME ──────────────────────────────► Streaming ─┐
open ─► Connecting ┼─ BOOTSTRAP ─► Bootstrapping ──(Tail)──► Streaming  │ retryable error
          ▲        └─ REJECT, or policy says halt ─────────► Halted     │ or disconnect
          │                                                    │        │
          ├────────────── operator clears the halt ────────────┘        │
          └────────────── backoff ◄─────────────────────────────────────┘
```

A failed bootstrap (bad footer, lost connection) also returns to `Connecting` through backoff.

### 11.4 Idle detection and reconnection

- If no `TailMessage` arrives for `3 × heartbeat_ms`, the client MUST cancel the stream and reconnect.
- A Tail stream that ends cleanly (end of stream without a status) MUST be treated like a lost connection: back off and handshake again. It never means that the server has nothing more to send.
- Reconnect backoff SHOULD be exponential with full jitter (RECOMMENDED: initial 100 ms, factor 2, maximum 30 s), reset after 60 s of successful streaming.
- The client sets its maximum decode size for `Tail` to `max(max_entry_bytes, max_batch_bytes) + 64 KiB`, and for `Bootstrap` to `max(max_entry_bytes, 16 MiB) + 64 KiB`. The client does not know the server's `bootstrap_chunk_bytes`, so servers MUST keep it at most 16 MiB.

## 12. Timing and limits

| Parameter | Set by | Recommended default |
|---|---|---|
| `heartbeat_ms` | server, sent in handshake | 5 000 |
| Client idle timeout | client | 3 × `heartbeat_ms` |
| `max_entry_bytes` | master, sent down the chain | 16 MiB |
| `bootstrap_chunk_bytes` | server | 1 MiB (at most 16 MiB) |
| `min_batch_bytes` / `max_wait_ms` / `max_batch_bytes` (Tail) | client, per stream | 0 / 100 / 1 MiB |
| `max_tail_batch_bytes` (cap on the client's `max_batch_bytes`) | server | 4 MiB |
| Tail acks | client | after each batch, and at least every `heartbeat_ms` |
| `replica_lag_time_max` (in-sync window, §7.6) | server | 10 000 ms |
| `lease_ttl_ms` | server | 900 000 (15 min) |
| `max_concurrent_bootstraps` | server | 2 |
| `max_verify_sessions` | server | 4 |
| `session_timeout_ms` (verify) | server | 300 000 (5 min) |
| `verify_window` | client | 16 |
| `ListRange.limit` | client | ≤ 1024 |
| `ListResult` size | server | stop after ~1 MiB of keys, but never fewer than 2 keys |
| `FetchKeys.keys` | client | 1..=1024 keys, ≤ 1 MiB of keys per request (a single larger key alone) |
| `FetchRange.limit` | client | 1..=1024 |
| `FetchResult` size | server | stop after ~1 MiB of keys and envelopes, but never fewer than 1 record (`FetchKeys`) or 2 records (`FetchRange`) |
| `EntryCrcs` span | server | at most 1024 entries per request, none after `snapshot_seq` (a client never asks past it) |
| HTTP/2 keepalive | client | 30 s interval, 10 s timeout |
| Reconnect backoff | client | 100 ms → 30 s, ×2, full jitter |

Only the master enforces `max_entry_bytes`, at write time. Replicas accept and forward whatever the master committed.

## 13. Security considerations

- **Confidentiality.** `Bootstrap` and `Tail` carry every key and value. Without TLS they are exposed to anyone on the network path. Plaintext SHOULD be limited to loopback.
- **Authorisation scope.** A credential that can call `Bootstrap`, or `VerifyRange` with `FetchKeys` or `FetchRange`, can read the whole database. Deployments SHOULD issue replication credentials only to replica nodes and SHOULD NOT reuse them elsewhere.
- **Tokens.** Tokens MUST be compared in constant time, and MUST NOT be logged or included in error details. Support at least two concurrently valid tokens so they can be rotated without downtime.
- **Node ids are not credentials.** `client_id`, `server_id` and `upstream_chain` are self-asserted. They are used for loop detection and diagnostics only.
- **Integrity.** CRC-32C protects against accidental corruption only. Against active tampering, rely on TLS. The BLAKE3 digests detect divergence between honest nodes. They do not authenticate the data.
- **Denial of service.** Servers bound concurrent bootstraps and verify sessions, expire idle sessions and leases, and SHOULD limit the rate of failed authentication attempts per peer address. Throttled calls are answered `BUSY` (retryable, §10), so the limit cannot be used to halt good replicas that share the address.
- **Divergence.** The epoch history, the `DIVERGED` and `STALE_EPOCH` checks and the master's fsync together keep different histories from mixing under the same seq. An operator who restores a master from a backup MUST bump its epoch before it serves replicas, and an operator who promotes a replica MUST stop the old master first.

## 14. Complete protobuf definition

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
message EpochStart { uint64 epoch = 1; uint64 start_seq = 2; uint64 nonce = 3; }

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
  reserved 7;                              // was known_epoch_start
  repeated EpochStart known_epochs = 8;   // the client's newest epoch records (up to 5), oldest first
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
  repeated EpochStart epoch_history = 6;   // as of the snapshot, like identity and cf_specs
}

message KeyRange  { string cf = 1; optional bytes lower = 2; optional bytes upper = 3; }
message HashRange { KeyRange range = 1; uint64 count = 2; bytes hash = 3; }
message ListRange { KeyRange range = 1; uint32 limit = 2; }

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

message VerifyEndRequest  { bytes session_id = 1; }
message VerifyEndResponse {}

// ---------- status (operator tooling) ----------

// probe_seq: also report the crc32c of the server's entry at this seq (probe_crc).
message StatusRequest  { string client_id = 1; optional uint64 probe_seq = 2; }
message ReplicaSummary { string node_id = 1; uint64 applied_seq = 2; bool in_sync = 3; }
message StatusResponse {
  string   node_id                  = 1;
  string   role                     = 2;   // "master" or "slave"
  optional Identity identity        = 3;
  repeated EpochStart epoch_history = 4;
  uint64   last_seq                 = 5;
  optional fixed32 last_entry_crc   = 6;
  uint64   last_ts_ms               = 7;   // ts of the entry at last_seq; 0 if unknown
  string   node_state               = 8;   // "ready", "bootstrapping", "halted:<Reason>" or "repairing:<seq>"
  string   upstream                 = 9;   // empty on the master
  repeated ReplicaSummary replicas  = 10;
  optional fixed32 probe_crc        = 11;  // crc32c of the entry at probe_seq, if held
  uint64   max_burned_epoch         = 12;  // highest epoch the node gave up by demoting; 0 = none
}
```

## 15. Test vectors

All bytes are in hexadecimal. CRC-32C sanity check: `crc32c("123456789") = 0xE3069283`.

**V1. Log entry checksum.** `seq = 1`, `epoch = 1`, `ts_ms = 1700000000000`, one op `Put{cf: "users", key: "k1", value: "v1"}`:

```
ops    = 0a 11 0a 0f 0a 05 75 73 65 72 73 12 02 6b 31 1a 02 76 31      (19 bytes)
crc32c = 0x73717118
```

**V2. Envelopes** for the value `"v1"` written at seq 1:

```
no TTL                         01 00 00 00 00 00 00 00 00 01 76 31
expire_at_ms = 1700000060000   01 01 00 00 00 00 00 00 00 01 00 00 01 8b cf e6 52 60 76 31
```

**V3. Digests.** One CF `users` (`create_seq = 1`) holding one key `k1` with the V2 no-TTL envelope, snapshot at seq 1:

```
bootstrap digest       3aa1d6c512a91d54c3da5ef18a6efef531d61b7d3ec9b37c1ba907832fb22729
range hash, QUICK      2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3
range hash, FULL       14e556f28a8301390e9be56ecb4ec7ff3ceea3cd48316ab7f7f0488117e53ecd
range hash, empty      af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262
```

## 16. Example exchanges

**New replica C joins through replica B.**

```
C → B  Handshake{client_id:"C", known_identity: –, last_seq: 0}
B → C  {decision: BOOTSTRAP, reason: NO_LOCAL_STATE, identity: (U,1), upstream_chain: ["A"], …}
C → B  Bootstrap{client_id:"C"}
B → C  Header{S: 4210, cf_specs:[…], lease_id: L} · Records… · Footer{count, digest}
C      verify footer, commit last_seq = 4210
C → B  Tail start{identity:(U,1), from_seq: 4211, bootstrap_lease: L}
B → C  batch[4211, 4212] · heartbeat · …                  (B releases the guard for L)
C → B  ack{applied_seq: 4212}                             (after applying the batch; again
                                                          at least once per heartbeat)
```

**Replica restarts ahead of its upstream.** B crashed before flushing entries it had already forwarded to C.

```
C → B  Handshake{last_seq: 900, last_crc32c: X}           B.last_seq = 880
B → C  {decision: RESUME}                                  (rule 14; B is not the master)
C → B  Tail start{from_seq: 901} · ack{applied_seq: 900} …
B → C  heartbeat · heartbeat · …                           (B re-fetches 881–900 from A)
B → C  entry 901 · …
```

**Master restored from backup without an epoch bump.** Its open started epoch 2 at 4801.

```
B → A  Handshake{known_identity:(U,1), last_seq: 5000, known_epochs:[{1,1,n1}]}     A.last_seq = 4800
A → B  {decision: REJECT, reason: DIVERGED}               (rule 8: 5000 ≥ fork(1) = 4801)
B      halts Diverged; keeps serving reads at seq 5000
```

**Promotion.** A fails. B (at seq 700) is promoted, so the history becomes `[{1,1},{2,701}]`. C had reached seq 690 and D had reached seq 705.

```
C → B  Handshake{known_identity:(U,1), last_seq: 690, last_crc32c: X}
B → C  {decision: RESUME, reason: EPOCH_ADVANCED, identity:(U,2), epoch_history:[{1,1},{2,701}]}
C      persists identity (U,2) + history, then Tail{from_seq: 691}     (rule 9)
D → B  Handshake{known_identity:(U,1), last_seq: 705}
B → D  {decision: REJECT, reason: DIVERGED}                (rule 8: 705 ≥ fork(1) = 701)
A → …  restarted old master: its open starts its own epoch 2 (another nonce); a replica of
       B's epoch 2 asking A gets DIVERGED (rule 8), one of a later epoch STALE_EPOCH (rule 7)
```

[RFC 2119]: https://www.rfc-editor.org/rfc/rfc2119
[RFC 8174]: https://www.rfc-editor.org/rfc/rfc8174
[RFC 3720]: https://www.rfc-editor.org/rfc/rfc3720
[RFC 6750]: https://www.rfc-editor.org/rfc/rfc6750
