# Examples

Each example is self-contained (temp directories, ports on `127.0.0.1`), runs in a few
seconds, prints what it is doing and exits 0. Run one with `cargo run --example <name>`.

| Example | What it shows | Output to expect |
|---|---|---|
| `embedded_master` | A `Master` as a plain local store: CFs, put/get/delete/delete_range, `Batch`, TTL, snapshot, iteration, and the **application-lock read-modify-write pattern** (the crate has no `update`/CAS/merge). | A line per operation; the locked counter ends at exactly `200`. |
| `master_and_replica` | Master serving replication, a `Slave` following it, `wait_streaming` / `wait_in_sync` instead of polling, `wait_for_seq` for read-your-writes, `Master::replicas()` / `Slave::status()`, real-time vs batched `FetchConfig`. | Replica reads, lag/in_sync numbers, and the batched replica lagging by about its `max_wait` (~500 ms). |
| `write_concern` | `WriteConcern` in config and per call (CF operations are exempt); `NotEnoughReplicas` (nothing written) vs `AckTimeout` (committed, do not blindly retry); `Master::wait_in_sync`; `spawn_blocking`. | A refused write, an acked write, and a timeout whose entry still reaches the slow replica. |
| `failover` | The operator runbook: stop A, `admin::gather` / `choose_promotion_candidate`, `promote_checked` (incl. `PromoteRefused::into_parts`), `MasterConfig::from_slave_config`, `repoint`, then the `Diverged` / `AcceptDataLoss` path. | Two scenarios: a clean failover, then a deliberately wrong promotion showing `Diverged` and the explicit data-loss opt-in. |
| `secure_replication` | TLS (runtime `rcgen` CA and server cert) plus bearer-token auth; mTLS allow-list explained in comments. | A good-token replica replicates; a bad-token replica halts with `AuthRejected`. |
| `verify` | `Slave::verify_now` in `Quick` and `Full` mode and the `VerifyReport`; damage injected into a stopped replica with plain RocksDB. | Two clean runs, then `Quick` still clean and `Full` reporting the one corrupted key. |
| `repair` | A divergence refused by the default `repoint`, `diverged_entries` to salvage, then `repoint(.., RepointOptions { on_divergence: RepointDivergence::Repair { accept_loss: AcceptDataLoss } })` returning `Repaired { discarded }`; and `MismatchPolicy::Repair` restoring a key a verify found missing. | The refusal, the salvageable entries, `Repaired { discarded: 3..=4 }` and a clean `Full` verify; then the lost key back and a clean verify. |
| `node` | The `Node` handle through a failover: `create_master` / `create_slave`, `promote_checked` on the shared handle, the old master reopening **fenced** (`PeerIsMaster`), `rejoin_as_slave` refusing the divergence, `diverged_entries`, then `repoint(.., Repair { accept_loss: AcceptDataLoss })`. | Six numbered steps; A fenced by B, the refused rejoin with its one salvageable entry, `Repaired { .. }`, and A's final reads (`k2` absent, `k3` from B). |

## Interactive playground

`cargo run --example playground`, then open <http://127.0.0.1:8080>. Unlike the examples
above it keeps running (Ctrl-C to stop).

Four `Node`s run in one process as a daisy chain A → B → C → D on 127.0.0.1:7101..7104. From
the browser you can:

- **Write** on any node (a slave refuses), single keys or bursts, with an optional write concern
  (`min in-sync replicas`). **Read** one key on all four nodes side by side, or scan a node.
- **Transitions** offered per state: promote (checked / unchecked), demote, repoint, rejoin as
  slave, unfence (optionally accepting split-brain risk), stop / start, clear halt, rebootstrap,
  verify, show divergent entries. Data-loss steps need the "accept data loss" tick
  (`AcceptDataLoss`).
- **Status per node**, pushed live over a WebSocket: a light (green = up to date, blinking
  green = replicating or catching up, red = fenced, failed, halted or retrying with an error,
  grey = stopped), progress (applied / master seq), epoch and epochs behind the master, entries
  behind, lag, last contact, last error with its `UpstreamErrorKind`, and each node's direct
  downstream replicas (`Node::replicas`: in sync or not).
- **Live events**: every slave's `ReplicationListener` (`on_entry`, `on_state_change`,
  bootstrap, rewind, verify) and every `Node::watch_state` role change.

Options: `--http ADDR`, `--port-base N`, `--dir PATH` (keep the data; reopening it brings stored
masters back fenced until a peer check clears them).

Try: stop A, promote B (checked), start A (fenced by B), rejoin A as slave of B. Or stop a
slave, write on the master, stop the master, promote the stale slave **unchecked**, write on it,
and repair the others with "accept data loss".
