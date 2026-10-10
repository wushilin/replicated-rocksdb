# replicated-rocksdb

Rust crate: single-master, async log-shipping replication for RocksDB. Master is the only writer; slaves are read-only and can be chained (a slave can serve other slaves). Full design: [DESIGN.md](DESIGN.md). Read it before changing anything in the write path, apply path, bootstrap or verification. Wire protocol: [PROTOCOL.md](PROTOCOL.md) is normative; `proto/replication.proto` must stay identical to its §14, and the codec must pass its §15 test vectors.

## Invariants (do not break)

- **Absolute ops only.** The log may contain only `Put`, `Delete`, `DeleteRange`, `CreateCf`, `DropCf`. There is no merge or any other relative op. Merge operators set through `RocksTuning` must be rejected.
- **Atomicity.** Mutation, `__changelog[seq]` and `__meta.last_seq` are always in the same `WriteBatch`, on the master and on slaves. A `LogEntry` is never split across batches.
- **`__meta.last_seq` is the source of truth for position**, never the last key of `__changelog`, which retention can empty completely.
- **Master always fsyncs.** Losing acknowledged entries would cause seqs to be reused and replicas to diverge.
- **Slaves forward entries unchanged** (seq, epoch, ts, crc, and the `ops` bytes verbatim; never re-encode `ops`). Identity always refers to the origin master.
- **Divergence is never silent.** The master rejects a replica ahead of it; mismatched `last_entry_crc` / `prev_crc32c` → `DIVERGED` halt.
- **Consent is scoped.** Only `clear_halt(Rebootstrap)`, a repoint with `AcceptDataLoss` (including the first handshake of `Slave::rejoin` / `Node::rejoin_as_slave`), or a configured `DivergencePolicy` discards divergent entries. Automatic and verify requests are bound to (repoint generation, `db_uuid`) and never override `IDENTITY_CHANGED`, nor `DIVERGED` except `AutoCause::Diverged`, set only under a configured Rebootstrap policy or repoint mode; repair fallbacks are pinned to the consented database and fork.
- **Epoch history is part of identity.** Every entry satisfies `entry.epoch == epoch_for(entry.seq)`; history and `origin_identity` are always written in the same batch. Promotion (`Slave::promote`) and `bump_epoch` append to it; seq numbering never restarts.
- **Every master open starts an epoch** (except the open after a promotion, which already started one) with a fresh nonce, persisted before the port opens; a repair's rewind rewrites identity and epoch history in the same batch.
- **A stored master reopens fenced** through `Node`: no epoch, no port, no reads or writes until a peer check clears it; only `unchecked_master: AcceptSplitBrainRisk` skips the check (`unfence` consent waives only unreachable peers); a former master becomes a slave only via the explicit `rejoin`.
- **The client owns its replication position.** It resumes by sending its durable `last_seq` (Handshake) and `from_seq = last_seq + 1` (Tail). Servers keep no per-client cursor.
- **All user values are wrapped in an envelope** `[ver][flags][write_seq][expire_at?][value]`. `ReadApi` strips it; bootstrap and verify use raw envelopes.
- **TTL is master-driven.** `expire_at` is absolute and computed on the master. Only the master reaps, and it does so by logging `Delete`. Reads filter expired values on every node. RocksDB native TTL and compaction filters are not used. `__ttl_index` is master-local.
- **CF ops** are standalone entries. Physical CF name is `u:{name}:{create_seq}`. Drop is logical, and the physical drop is deferred while snapshots reference the CF.
- Bootstrap and verify read `S`, `cf_specs` and state **through the same RocksDB snapshot** as the data.
- Repair fetches, `EntryCrcs` and the epoch history sent with them are read through that same snapshot too.
- **Repair readiness.** `__meta.repair_until > last_seq` means not ready on every surface (reads, snapshots, serving, verify capture, promotion, rechecked on the stopped store). It is persisted apart from `node_state`, and the replay commit that reaches it clears it before `S` is acknowledged.
- `PROTOCOL_VERSION` must match exactly between nodes (crate version is not compared).
- User CF names must not start with `__`.
- **No read-modify-write API** (no `update`, CAS or merge). Applications must hold their own lock around get → compute → put (DESIGN.md §5.1). This must stay clearly documented in the rustdoc for `Master::put`, `Master::write` and `Master::get`, and in the crate-level docs.

## Stack

rocksdb, tokio, tonic + prost (TLS), uuid, blake3, crc32c, subtle, thiserror, tracing, async-trait.
