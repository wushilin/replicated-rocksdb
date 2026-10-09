# replicated-rocksdb

Rust crate: single-master, async log-shipping replication for RocksDB. Master is the only writer; slaves are read-only and can be chained (a slave can serve other slaves). Full design: [DESIGN.md](DESIGN.md). Read it before changing anything in the write path, apply path, bootstrap or verification. Wire protocol: [PROTOCOL.md](PROTOCOL.md) is normative; `proto/replication.proto` must stay identical to its §14, and the codec must pass its §15 test vectors.

## Invariants (do not break)

- **Absolute ops only.** The log may contain only `Put`, `Delete`, `DeleteRange`, `CreateCf`, `DropCf`. There is no merge or any other relative op. Merge operators set through `RocksTuning` must be rejected.
- **Atomicity.** Mutation, `__changelog[seq]` and `__meta.last_seq` are always in the same `WriteBatch`, on the master and on slaves. A `LogEntry` is never split across batches.
- **`__meta.last_seq` is the source of truth for position**, never the last key of `__changelog`, which retention can empty completely.
- **Master always fsyncs.** Losing acknowledged entries would cause seqs to be reused and replicas to diverge.
- **Slaves forward entries unchanged** (seq, epoch, ts, crc, and the `ops` bytes verbatim; never re-encode `ops`). Identity always refers to the origin master.
- **Divergence is never silent.** The master rejects a replica ahead of it; mismatched `last_entry_crc` / `prev_crc32c` → `DIVERGED` halt.
- **Epoch history is part of identity.** Every entry satisfies `entry.epoch == epoch_for(entry.seq)`; history and `origin_identity` are always written in the same batch. Promotion (`Slave::promote`) and `bump_epoch` append to it; seq numbering never restarts.
- **The client owns its replication position.** It resumes by sending its durable `last_seq` (Handshake) and `from_seq = last_seq + 1` (Tail). Servers keep no per-client cursor.
- **All user values are wrapped in an envelope** `[ver][flags][write_seq][expire_at?][value]`. `ReadApi` strips it; bootstrap and verify use raw envelopes.
- **TTL is master-driven.** `expire_at` is absolute and computed on the master. Only the master reaps, and it does so by logging `Delete`. Reads filter expired values on every node. RocksDB native TTL and compaction filters are not used. `__ttl_index` is master-local.
- **CF ops** are standalone entries. Physical CF name is `u:{name}:{create_seq}`. Drop is logical, and the physical drop is deferred while snapshots reference the CF.
- Bootstrap and verify read `S`, `cf_specs` and state **through the same RocksDB snapshot** as the data.
- `PROTOCOL_VERSION` must match exactly between nodes (crate version is not compared).
- User CF names must not start with `__`.
- **No read-modify-write API** (no `update`, CAS or merge). Applications must hold their own lock around get → compute → put (DESIGN.md §5.1). This must stay clearly documented in the rustdoc for `Master::put`, `Master::write` and `Master::get`, and in the crate-level docs.

## Stack

rocksdb, tokio, tonic + prost (TLS), uuid, blake3, crc32c, subtle, thiserror, tracing, async-trait.
