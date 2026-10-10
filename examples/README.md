# Examples

Each example is self-contained (temp directories, ports on `127.0.0.1`), runs in a few
seconds, prints what it is doing and exits 0. Run one with `cargo run --example <name>`.

| Example | What it shows | Output to expect |
|---|---|---|
| `embedded_master` | A `Master` as a plain local store: CFs, put/get/delete/delete_range, `Batch`, TTL, snapshot, iteration, and the **application-lock read-modify-write pattern** (the crate has no `update`/CAS/merge). | A line per operation; the locked counter ends at exactly `200`. |
| `master_and_replica` | Master serving replication, a `Slave` following it, `wait_for_seq` for read-your-writes, `Master::replicas()` / `Slave::status()`, real-time vs batched `FetchConfig`. | Replica reads, lag/in_sync numbers, and the batched replica lagging by about its `max_wait` (~500 ms). |
| `write_concern` | `WriteConcern` in config and per call; `NotEnoughReplicas` (nothing written) vs `AckTimeout` (committed, do not blindly retry); `spawn_blocking`. | A refused write, an acked write, and a timeout whose entry still reaches the slow replica. |
| `failover` | The operator runbook: stop A, `admin::gather` / `choose_promotion_candidate`, `promote_checked` (incl. `PromoteRefused`), `repoint`, then the `Diverged` / `AcceptDataLoss` path. | Two scenarios: a clean failover, then a deliberately wrong promotion showing `Diverged` and the explicit data-loss opt-in. |
| `secure_replication` | TLS (runtime `rcgen` CA and server cert) plus bearer-token auth; mTLS allow-list explained in comments. | A good-token replica replicates; a bad-token replica halts with `AuthRejected`. |
