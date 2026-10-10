# Plan 4: `Node` Handle, Fenced Open and Rejoin — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add `Slave::rejoin` (a former master becomes a slave without wiping its directory) and a shareable `Node` handle that holds either role, switches roles explicitly inside itself, opens a stored master *fenced* until a peer check clears it, and exposes its exact state through `state()` / `watch_state()`.

**Architecture:** `Slave::rejoin` is a store-level rewrite batch (role, trimmed epoch history, burned epochs, identity, master-only keys) followed by a normal `Slave` open whose first handshake is pre-armed as a repoint carrying the caller's consent. `Node` wraps a `tokio::sync::RwLock<Current>` (`Master | Slave | Fenced | Switching | Failed | Stopped`); sync calls use `try_read` and fail fast with `Err(Switching)`, switches take the write lock, move the old handle out, call the existing consuming core operation, and put the result (or the handed-back handle) back. The fence check reads the node's own `NodeInfo` from the closed store through a read-only RocksDB open and asks every peer for `Status`, deciding with `admin::compare`.

**Tech Stack:** Rust 2024, rocksdb 0.25, tokio 1.53 (`RwLock`, `watch`, `JoinSet`), tonic (existing `Status` RPC), `fail` (feature `failpoints`).

**Spec:** `docs/superpowers/specs/2026-10-11-node-handle.md` (binding). Read it, `CLAUDE.md` and DESIGN.md §2, §3.2, §13, §13.1, §16 before starting any task.

## Global Constraints

- Every invariant in `CLAUDE.md` is binding; in particular: atomicity of identity + epoch history (same batch), `__meta.last_seq` is the position, divergence is never silent, consent is scoped, every master open starts an epoch, no read-modify-write API (the RMW warning must appear in rustdoc of `Node::put`, `Node::write`, `Node::get` too).
- The user's standing rule: every operation that can lose data or create two writers is an explicit call, refuses by default, and needs a typed consent token (`AcceptDataLoss`, `AcceptSplitBrainRisk`).
- `Master` and `Slave` keep their public API and behaviour; only additions.
- Never commit with `--no-verify`.
- Every commit message ends with the line `Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>`.
- Build and test with `CARGO_BUILD_JOBS=4` in the environment (prefix every cargo command with it).
- Integration-test ports come only from `support::reserved_port()` (through `Cluster::serve()` / `master_config` / `slave_config`); never bind port 0 in tests.
- `proto/replication.proto` is not changed (it must stay identical to PROTOCOL.md §14).
- Final gate (all must pass, run in this order at the end of Task 7):
  - `CARGO_BUILD_JOBS=4 cargo test`
  - `CARGO_BUILD_JOBS=4 cargo test --features failpoints`
  - `CARGO_BUILD_JOBS=4 cargo clippy --all-targets --all-features -- -D warnings`
  - `cargo fmt --check`
  - `CARGO_BUILD_JOBS=4 cargo doc --no-deps`
  - every example exits 0: `for e in embedded_master master_and_replica write_concern failover secure_replication verify repair node; do CARGO_BUILD_JOBS=4 cargo run --example $e || exit 1; done`

## Implementer decisions (where the spec left a choice)

These are fixed by this plan; do not re-decide them.

1. **`Slave::rejoin` return type:** `Result<(Slave, Result<RepointOutcome>)>`. Outer `Err`: the slave was never opened (refusal before the rewrite with the directory unchanged, or a failure during/after the rewrite, in which case the directory is a slave and `Slave::open` reopens it). Inner `Err` (e.g. `Diverged`): the first handshake's result, with the slave running and returned. No new error/refusal type.
2. **First handshake consent:** the slave is opened with a repoint pre-armed at generation 0 (`PendingRepoint` + `repoint_divergence = Some(opts.on_divergence)`) *before* its driver is spawned, so the very first handshake follows `RejoinOptions`, never `SlaveConfig::on_divergence`, and settles exactly like `Slave::repoint` (single-use consent, then `Refuse` until the next repoint or restart).
3. **`NodeState::Slave` representation:** `Slave { state: SlaveState, upstream: String }` where `upstream` is the URI as in `SlaveStatus::upstream` (`UpstreamConfig` is neither `Debug` nor `PartialEq`).
4. **Extra state `NodeState::Stopped`** after `Node::shutdown`; calls then return `Err(NodeFailed("the node was shut down; call Node::reopen"))` and `reopen` is allowed from `Failed` and `Stopped`.
5. **Iteration on `Node`:** `Node` does not implement `ReadApi` (an iterator would borrow a lock guard). It offers `get`, `get_with_meta`, `multi_get`, `list_cfs` (→ `Result<Vec<CfSpec>>`) and `snapshot()`; iterate through the returned `ReadSnapshot`.
6. **Errors for wrong-role calls:** writes on a slave → `NotMaster`; slave-admin calls and `promote*` on a master → `NotSlave`; `rejoin_as_slave` on a slave → `NotMaster`; `demote` on a slave → `NotMaster`; `unfence` when not fenced and `reopen` when not `Failed`/`Stopped` → `Error::Config`. Any call on `Fenced` → `Fenced { reason }`, on `Failed`/`Stopped` → `NodeFailed`, while a switch holds or waits for the lock → `Switching`.
7. **`FenceReason::peer`:** the peer's `node_id` when it answered `Status`, its URI otherwise (`PeerUnreachable`, `PeerError` from a failed call).
8. **Fence decision details:** a peer of another database (`Comparison::DifferentDatabase`) blocks as `PeerError { error: "a different database" }`. The epoch rule ("a peer reports a `max_burned_epoch` or epoch `>=` this node's newest epoch number with a different nonce") blocks as `PeerAhead`. When several peers block, the reason reported is the most severe — `PeerIsMaster` > `PeerDiverged` > `PeerAhead` > `PeerError` > `PeerUnreachable` — first in `cfg.peers` order within a class. With `allow_unreachable`, unreachable peers are skipped (all peers unreachable then clears); `PeerError` is never skipped.
9. **Expected rejoin outcome after a failover is `EpochAdvanced`, not `Resumed`:** the new master always started a newer epoch than the old master's newest remaining record, so the handshake answers `EPOCH_ADVANCED`. Tests assert `EpochAdvanced` for the spec's "resume" cases.
10. **Master-only keys removed by the rewrite:** every `__ttl_index` entry (one `delete_range` in the same batch), `promoted_from_len`, `last_ts`. Also removed so the slave starts ready: `node_state`, `repair_until`, `diverged_at`, `max_entry_bytes`, `bootstrap_cfs`. `cf_create_intent` is left to `Store::open`'s recovery.
11. **A switch future dropped before it finishes** (e.g. the caller's `timeout`) leaves the node `Failed { error: "a role switch was cancelled before it finished; call Node::reopen" }` (a drop guard), never stuck in `Switching`.
12. **`rejoin_as_slave` from `Master`:** the master is shut down first and must release the database (a live `ReadSnapshot` → `Failed` with a clear `Error::Config`; `reopen` then fences). If `Slave::rejoin` fails and the stored role is still `master`, a node that came from `Fenced` returns to `Fenced` with its previous reason; otherwise it becomes `Failed`.
13. **`Node::promote` (unchecked)** uses a new crate-private `Slave::promote_keeping` that hands the slave back on refusal (plain `Slave::promote` shuts it down, which `Node` must not do).
14. **`Node::from_master` / `from_slave` with a mismatched or invalid config** shut the handle down gracefully and return `Err(Config)`.

## Review Focus

1. A caller wraps a switch (`promote`, `rejoin_as_slave`, …) in `tokio::time::timeout` and the future is dropped mid-switch → the node must end `Failed` (never stuck `Switching`) and `reopen` must restore it. Test: Task 6, `a_cancelled_switch_leaves_the_node_failed_and_reopenable` (failpoints).
2. `Slave::rejoin` on a directory that a `Master` in the same process still holds open → refused, directory unchanged, the master keeps writing. Test: Task 3, `rejoin_refuses_a_directory_that_is_still_open`.
3. Misconfigured peers: a peer of another database in `cfg.peers` must never clear the fence. Test: Task 4, unit test `a_peer_of_another_database_never_clears`.
4. `Node::repoint` with an invalid address must not replace the upstream that later reopens of the slave role use. Test: Task 5, inside `watch_state_forwards_slave_states_and_repoint_updates_the_upstream`.
5. A `ReadSnapshot` from `node.snapshot()` still alive during `promote` → `Failed` with an actionable error, and after dropping it `reopen` brings back the slave (directory unchanged). Test: Task 6, `a_lost_handle_fails_the_node_and_reopen_recovers`.

---

## File Structure

| File | Responsibility | Task |
|---|---|---|
| `src/node/mod.rs` (create) | module root, re-exports | 1, 4, 5, 6 |
| `src/node/types.rs` (create) | `AcceptSplitBrainRisk`, `FenceReason`, `NodeState`, `UnfenceOptions` | 1 |
| `src/slave/rejoin.rs` (create) | `RejoinOptions`, `Slave::rejoin` | 1, 3 |
| `src/error.rs` (modify) | `NotMaster`, `NotSlave`, `Fenced`, `Switching`, `NodeFailed` | 1 |
| `src/lib.rs` (modify) | module + exports, crate docs | 1, 5, 7 |
| `src/store/rejoin.rs` (create) | rewrite batch `Store::rejoin_as_slave`, `trim_trailing_empty` | 2 |
| `src/store/offline.rs` (create) | `stored_meta`, `OfflineLog` (read-only view of a closed store) | 2, 4 |
| `src/store/mod.rs` (modify) | `mod offline; mod rejoin;` | 2 |
| `src/store/meta.rs`, `src/master/writer.rs` (modify) | comments: burned by demote *or rejoin* | 2 |
| `src/slave/mod.rs` (modify) | `Slave::start` with pre-armed repoint; `path()` | 3, 5 |
| `src/failpoint.rs` (modify) | document `rejoin-after-rewrite` | 3 |
| `src/node/fence.rs` (create) | `PeerAnswer`, `decide`, `check` | 4 |
| `src/node/config.rs` (create) | `NodeConfig` | 5 |
| `src/node/handle.rs` (create) | `Node`, `Current`, constructors, reads, writes, slave admin, state publishing | 5 |
| `src/master/mod.rs` (modify) | `path()`, `shutdown_released()` | 5, 6 |
| `src/node/switch.rs` (create) | `Switch` guard and every role switch | 6 |
| `src/slave/promote.rs` (modify) | `promote_keeping` | 6 |
| `tests/rejoin.rs` (create) | `Slave::rejoin` end to end | 3 |
| `tests/node.rs` (create) | `Node` end to end | 5, 6 |
| `tests/failpoints.rs` (modify) | rejoin crash, watch `Switching`, cancelled switch | 3, 6 |
| `DESIGN.md`, `CLAUDE.md`, `PROTOCOL.md` (§6.5 prose only), `examples/node.rs`, `examples/README.md` | docs and example | 7 |

---

### Task 1: Errors, public types and the consent token

**Files:**
- Create: `src/node/mod.rs`, `src/node/types.rs`, `src/slave/rejoin.rs`
- Modify: `src/error.rs` (add variants after `VerifyAborted`), `src/slave/mod.rs` (add `mod rejoin; pub use rejoin::RejoinOptions;`), `src/lib.rs` (add `mod node;` and exports)

**Interfaces:**
- Consumes: `crate::slave::{SlaveState, RepointDivergence}`, `crate::types::DbIdentity`.
- Produces (all re-exported from the crate root):
  - `pub struct AcceptSplitBrainRisk;` — `#[derive(Clone, Copy, Debug, PartialEq, Eq)]`
  - `#[non_exhaustive] pub enum FenceReason { NoPeers, PeerUnreachable { peer: String }, PeerIsMaster { peer: String }, PeerAhead { peer: String }, PeerDiverged { peer: String }, PeerError { peer: String, error: String } }` — `Clone, Debug, PartialEq, Eq`, `impl Display`
  - `#[non_exhaustive] pub enum NodeState { Master { identity: DbIdentity }, Fenced { reason: FenceReason }, Slave { state: SlaveState, upstream: String }, Switching, Failed { error: String }, Stopped }` — `Clone, Debug, PartialEq, Eq`
  - `pub struct UnfenceOptions { pub allow_unreachable: Option<AcceptSplitBrainRisk> }` — `Clone, Copy, Debug, Default, PartialEq, Eq`
  - `pub struct RejoinOptions { pub on_divergence: RepointDivergence }` — `Clone, Copy, Debug, Default, PartialEq, Eq`
  - `Error::NotMaster`, `Error::NotSlave`, `Error::Fenced { reason: FenceReason }`, `Error::Switching`, `Error::NodeFailed(String)`

- [ ] **Step 1: Write the failing tests**

Append to the `tests` module of `src/error.rs`:

```rust
    #[test]
    fn node_errors_say_what_to_do() {
        use crate::node::FenceReason;
        let fenced = Error::Fenced {
            reason: FenceReason::PeerIsMaster { peer: "b".into() },
        };
        assert_eq!(fenced.to_string(), "the node is fenced: peer b is a master");
        assert_eq!(
            Error::NodeFailed("boom".into()).to_string(),
            "the node failed: boom; call Node::reopen"
        );
        assert!(Error::Switching.to_string().contains("retry"));
        assert!(!Error::Switching.is_transient_io());
    }
```

Create `src/node/types.rs` with only the test module first (the implementation follows in Step 3):

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::slave::{AcceptDataLoss, RejoinOptions, RepointDivergence};

    #[test]
    fn defaults_refuse() {
        assert_eq!(UnfenceOptions::default().allow_unreachable, None);
        assert_eq!(
            RejoinOptions::default().on_divergence,
            RepointDivergence::Refuse
        );
        let repair = RejoinOptions {
            on_divergence: RepointDivergence::Repair {
                accept_loss: AcceptDataLoss,
            },
        };
        assert_ne!(repair, RejoinOptions::default());
    }

    #[test]
    fn fence_reasons_display_the_peer() {
        let cases = [
            (FenceReason::NoPeers, "no peers are configured to check against"),
            (
                FenceReason::PeerUnreachable { peer: "http://h:1".into() },
                "peer http://h:1 did not answer",
            ),
            (FenceReason::PeerIsMaster { peer: "b".into() }, "peer b is a master"),
            (
                FenceReason::PeerAhead { peer: "b".into() },
                "peer b is ahead of this node or knows a newer epoch",
            ),
            (
                FenceReason::PeerDiverged { peer: "b".into() },
                "peer b holds a history that diverges from this node's",
            ),
            (
                FenceReason::PeerError { peer: "b".into(), error: "denied".into() },
                "peer b answered with an error: denied",
            ),
        ];
        for (reason, text) in cases {
            assert_eq!(reason.to_string(), text);
        }
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CARGO_BUILD_JOBS=4 cargo test --lib node_errors_say_what_to_do`
Expected: compile error (`crate::node` / `Error::Fenced` not found).

- [ ] **Step 3: Implement**

`src/node/types.rs` (above the test module):

```rust
//! Public types of the `Node` handle (DESIGN.md §13.2).

use crate::slave::SlaveState;
use crate::types::DbIdentity;

/// Acknowledges that an operation can leave the database with two writers (split brain): a
/// node opens as master without proving that no other master exists. A caller constructs it
/// explicitly (`AcceptSplitBrainRisk`); nothing in the crate supplies one by default.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct AcceptSplitBrainRisk;

/// Why a stored master stays fenced (DESIGN.md §13.2). `peer` is the peer's node id when it
/// answered `Status`, its address otherwise. More reasons may be added, hence
/// `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum FenceReason {
    /// `NodeConfig::peers` is empty: nothing can prove there is no other master.
    NoPeers,
    /// The peer did not answer `Status` within `admin::STATUS_TIMEOUT`.
    PeerUnreachable { peer: String },
    /// The peer reports `role == Master`.
    PeerIsMaster { peer: String },
    /// The peer holds more of a compatible history, or knows an epoch at or above this
    /// node's newest one (in its history, identity or `max_burned_epoch`).
    PeerAhead { peer: String },
    /// The peer's history and this node's hold different entries.
    PeerDiverged { peer: String },
    /// The peer answered `Status` with an error, or belongs to another database.
    PeerError { peer: String, error: String },
}

impl std::fmt::Display for FenceReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            FenceReason::NoPeers => write!(f, "no peers are configured to check against"),
            FenceReason::PeerUnreachable { peer } => write!(f, "peer {peer} did not answer"),
            FenceReason::PeerIsMaster { peer } => write!(f, "peer {peer} is a master"),
            FenceReason::PeerAhead { peer } => {
                write!(f, "peer {peer} is ahead of this node or knows a newer epoch")
            }
            FenceReason::PeerDiverged { peer } => {
                write!(f, "peer {peer} holds a history that diverges from this node's")
            }
            FenceReason::PeerError { peer, error } => {
                write!(f, "peer {peer} answered with an error: {error}")
            }
        }
    }
}

/// What a `Node` is right now (`Node::state`, `Node::watch_state`). More states may be
/// added, hence `non_exhaustive`.
#[derive(Clone, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum NodeState {
    /// Running as the master; `identity` is its current epoch.
    Master { identity: DbIdentity },
    /// A stored master that has not been cleared by a peer check: no epoch started, no port
    /// served, reads and writes refused.
    Fenced { reason: FenceReason },
    /// Running as a slave; `state` follows the slave's own state machine live, `upstream` is
    /// the address it replicates from (as `SlaveStatus::upstream`).
    Slave { state: SlaveState, upstream: String },
    /// A role switch is in progress; calls return `Error::Switching`.
    Switching,
    /// A switch lost the running handle; `Node::reopen` opens the directory again.
    Failed { error: String },
    /// `Node::shutdown` completed; `Node::reopen` opens the directory again.
    Stopped,
}

/// Options of `Node::unfence`. The default refuses while any peer is unreachable.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct UnfenceOptions {
    /// Clear the fence although some peers did not answer: they could be a live master.
    pub allow_unreachable: Option<AcceptSplitBrainRisk>,
}
```

`src/node/mod.rs`:

```rust
//! A handle that holds either role and switches it explicitly (DESIGN.md §13.2).

mod types;

pub use types::{AcceptSplitBrainRisk, FenceReason, NodeState, UnfenceOptions};
```

`src/slave/rejoin.rs`:

```rust
//! A former master becomes a slave without wiping its directory (DESIGN.md §13.2).

use super::repoint::RepointDivergence;

/// Options of [`Slave::rejoin`](crate::Slave::rejoin). The default refuses divergence: the
/// slave halts `Diverged` with its data intact.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RejoinOptions {
    /// Decides a `DIVERGED` answer to the first handshake, exactly like
    /// `RepointOptions::on_divergence` (single-use consent).
    pub on_divergence: RepointDivergence,
}
```

In `src/slave/mod.rs`: add `mod rejoin;` to the module list and `pub use rejoin::RejoinOptions;` next to the other `pub use` lines.

In `src/error.rs`, after `VerifyAborted(..)`:

```rust
    /// A write (or `bump_epoch`, `reap_expired`, ...) on a `Node` that is not the master.
    #[error("this node is not the master")]
    NotMaster,
    /// A slave-only call (`repoint`, `clear_halt`, `promote`, ...) on a `Node` that is not a
    /// slave.
    #[error("this node is not a slave")]
    NotSlave,
    /// The `Node` is a fenced stored master: nothing is served until `Node::unfence` clears
    /// it or `Node::rejoin_as_slave` turns it into a slave.
    #[error("the node is fenced: {reason}")]
    Fenced { reason: crate::node::FenceReason },
    /// The `Node` is switching roles (or a switch waits for the lock); retry shortly.
    #[error("the node is switching roles; retry")]
    Switching,
    /// The `Node` lost its running handle (or was shut down); `Node::reopen` opens it again.
    #[error("the node failed: {0}; call Node::reopen")]
    NodeFailed(String),
```

In `src/lib.rs` add `mod node;` after `mod master;` and:

```rust
pub use node::{AcceptSplitBrainRisk, FenceReason, NodeState, UnfenceOptions};
```

and add `RejoinOptions` to the `pub use slave::{...}` list (alphabetical, after `PromotionCheck`).

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CARGO_BUILD_JOBS=4 cargo test --lib -- node_errors_say_what_to_do defaults_refuse fence_reasons_display_the_peer`
Expected: 3 passed. Then `CARGO_BUILD_JOBS=4 cargo clippy --all-targets --all-features -- -D warnings` clean.

- [ ] **Step 5: Commit**

```bash
git add src/node src/slave/rejoin.rs src/slave/mod.rs src/error.rs src/lib.rs
git commit -m "feat: Node state types, FenceReason, AcceptSplitBrainRisk, RejoinOptions and Node errors

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 2: Store-level rejoin rewrite batch

**Files:**
- Create: `src/store/rejoin.rs`, `src/store/offline.rs`
- Modify: `src/store/mod.rs` (add `pub(crate) mod offline;` and `pub(crate) mod rejoin;`), `src/store/meta.rs:29-31` (doc of `MAX_BURNED_EPOCH`), `src/master/writer.rs:161-163` (comment in `bump_epoch`)

**Interfaces:**
- Consumes: `Store::load_meta`, `Store::cf`, `Store::db`, `meta::{key, encode_identity, encode_epoch_history, CF_META, CF_TTL_INDEX}`, private `store::read_meta_read_only(path)`.
- Produces:
  - `pub(crate) fn trim_trailing_empty(history: Vec<EpochStart>, last_seq: Seq) -> (Vec<EpochStart>, Option<u64>)` in `store::rejoin`
  - `pub(crate) struct Rejoined { pub identity: Option<DbIdentity>, pub burned: Option<u64> }` in `store::rejoin`
  - `impl Store { pub(crate) fn rejoin_as_slave(&self) -> Result<Rejoined> }`
  - `pub(crate) fn stored_meta(path: &Path) -> Result<Option<MetaSnapshot>>` in `store::offline` (read-only; `None` = no stored role)

- [ ] **Step 1: Write the failing tests**

`src/store/rejoin.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use rocksdb::IteratorMode;

    use super::*;
    use crate::store::meta::{CF_TTL_INDEX, key};
    use crate::store::test_support::{apply, open_store};
    use crate::store::ttl::index_key;
    use crate::types::{CfEntry, CfSpec, Op, Role};

    fn rec(epoch: u64, start_seq: Seq) -> EpochStart {
        EpochStart { epoch, start_seq, nonce: epoch * 100 }
    }

    #[test]
    fn trailing_empty_epochs_are_trimmed_and_burned() {
        // Empty epochs after the last entry are dropped; the highest is burned.
        let h = vec![rec(1, 1), rec(2, 3), rec(3, 4), rec(4, 4)];
        assert_eq!(
            trim_trailing_empty(h, 3),
            (vec![rec(1, 1), rec(2, 3)], Some(4))
        );
        // An empty epoch in the middle stays (a later one holds entries).
        let h = vec![rec(1, 1), rec(2, 5), rec(3, 5)];
        assert_eq!(trim_trailing_empty(h.clone(), 7), (h, None));
        // last_seq 0 keeps the first record.
        assert_eq!(
            trim_trailing_empty(vec![rec(1, 1), rec(2, 1), rec(3, 1)], 0),
            (vec![rec(1, 1)], Some(3))
        );
        // Nothing to trim.
        assert_eq!(trim_trailing_empty(vec![rec(1, 1)], 9), (vec![rec(1, 1)], None));
        assert_eq!(trim_trailing_empty(vec![], 0), (vec![], None));
    }

    /// A master store with 3 entries in epochs 1..2, two empty per-open epochs after them, a
    /// TTL index, a promotion record and `last_ts`.
    fn master_with_trailing_epochs(dir: &std::path::Path, burned_before: u64) -> uuid::Uuid {
        let store = open_store(dir);
        let db_uuid = uuid::Uuid::from_u128(7);
        apply(&store, vec![Op::CreateCf { spec: CfSpec::new("c") }]).unwrap();
        for k in [b"a", b"b"] {
            let put = Op::Put { cf: "c".into(), key: k.to_vec(), value: vec![1], expire_at_ms: Some(50) };
            apply(&store, vec![put]).unwrap();
        }
        let history = vec![rec(1, 1), rec(2, 3), rec(3, 4), rec(4, 4)];
        let id = DbIdentity { db_uuid, epoch: 4 };
        store
            .write_meta(
                &[
                    (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                    (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
                    (key::MAX_BURNED_EPOCH, &burned_before.to_be_bytes()),
                    (key::PROMOTED_FROM_LEN, &1u64.to_be_bytes()),
                    (key::LAST_TS, &3_000u64.to_be_bytes()),
                ],
                true,
            )
            .unwrap();
        let c = CfEntry { spec: CfSpec::new("c"), create_seq: 1 };
        let ttl = store.cf(CF_TTL_INDEX);
        for k in [b"a", b"b"] {
            store.db().put_cf(&ttl, index_key(50, &c, k), []).unwrap();
        }
        db_uuid
    }

    #[test]
    fn rejoin_rewrites_a_master_into_a_ready_slave_in_one_batch() {
        let dir = tempfile::tempdir().unwrap();
        let db_uuid = master_with_trailing_epochs(dir.path(), 0);
        {
            let (store, _) = Store::open(dir.path(), Role::Master, Default::default()).unwrap();
            let r = store.rejoin_as_slave().unwrap();
            assert_eq!(r.identity, Some(DbIdentity { db_uuid, epoch: 2 }));
            assert_eq!(r.burned, Some(4));
        }
        assert!(
            matches!(
                Store::open(dir.path(), Role::Master, Default::default()),
                Err(Error::WrongRole { .. })
            ),
            "the role is persisted"
        );
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(m.role, Role::Slave);
        assert_eq!(m.identity, Some(DbIdentity { db_uuid, epoch: 2 }));
        assert_eq!(m.epoch_history, vec![rec(1, 1), rec(2, 3)]);
        assert_eq!(m.max_burned_epoch, 4);
        assert_eq!(m.last_seq, 3, "no entry is touched");
        assert_eq!((m.promoted_from_len, m.last_ts, m.node_state), (None, 0, None));
        assert!(
            store
                .db()
                .iterator_cf(&store.cf(CF_TTL_INDEX), IteratorMode::Start)
                .next()
                .is_none(),
            "slaves keep no TTL index"
        );
        assert_eq!(store.repair_until().unwrap(), None);
    }

    #[test]
    fn an_older_burn_is_kept_when_higher() {
        let dir = tempfile::tempdir().unwrap();
        master_with_trailing_epochs(dir.path(), 9);
        let (store, _) = Store::open(dir.path(), Role::Master, Default::default()).unwrap();
        store.rejoin_as_slave().unwrap();
        assert_eq!(store.load_meta().unwrap().unwrap().max_burned_epoch, 9);
    }

    /// Review focus: a master that never took a write (restarted a few times) keeps only its
    /// first record and becomes a slave without data loss.
    #[test]
    fn a_master_without_entries_keeps_its_first_epoch() {
        let dir = tempfile::tempdir().unwrap();
        let store = open_store(dir.path());
        let history = vec![rec(1, 1), rec(2, 1), rec(3, 1)];
        let id = DbIdentity { db_uuid: uuid::Uuid::from_u128(8), epoch: 3 };
        store
            .write_meta(
                &[
                    (key::ORIGIN_IDENTITY, &meta::encode_identity(&id)),
                    (key::EPOCH_HISTORY, &meta::encode_epoch_history(&history)),
                ],
                true,
            )
            .unwrap();
        let r = store.rejoin_as_slave().unwrap();
        assert_eq!((r.identity.unwrap().epoch, r.burned), (1, Some(3)));
        let m = store.load_meta().unwrap().unwrap();
        assert_eq!(m.epoch_history, vec![rec(1, 1)]);
    }

    #[test]
    fn stored_meta_reads_without_opening_for_write() {
        let dir = tempfile::tempdir().unwrap();
        assert_eq!(crate::store::offline::stored_meta(dir.path()).unwrap(), None);
        assert_eq!(
            crate::store::offline::stored_meta(&dir.path().join("missing")).unwrap(),
            None
        );
        let store = open_store(dir.path());
        // Works while the store is open read-write in this process (no LOCK needed).
        let m = crate::store::offline::stored_meta(dir.path()).unwrap().unwrap();
        assert_eq!(m.role, Role::Master);
        drop(store);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CARGO_BUILD_JOBS=4 cargo test --lib store::rejoin`
Expected: compile error (`trim_trailing_empty`, `rejoin_as_slave`, `offline` not found).

- [ ] **Step 3: Implement**

`src/store/offline.rs`:

```rust
//! Reading a closed store without opening it for writing (DESIGN.md §13.2): the stored role
//! of a directory, and (Task 4) a fenced master's own position.

use std::path::Path;

use rocksdb::{DB, Options};

use super::meta::{CF_META, MetaSnapshot};
use crate::error::Result;

/// The stored `__meta` of the database at `path`, read through a read-only open (no LOCK, no
/// write). `None` when there is no database or it never stored a role (missing directory,
/// empty directory, or a crash during first creation).
pub(crate) fn stored_meta(path: &Path) -> Result<Option<MetaSnapshot>> {
    if !path.join("CURRENT").exists() {
        return Ok(None);
    }
    if !DB::list_cf(&Options::default(), path)?.iter().any(|n| n == CF_META) {
        return Ok(None);
    }
    super::read_meta_read_only(path)
}
```

`src/store/rejoin.rs` (above the tests):

```rust
//! Store side of `Slave::rejoin` (DESIGN.md §13.2): one synced batch turns a stopped master
//! into a ready slave without touching its entries.

use rocksdb::{IteratorMode, WriteBatch, WriteOptions};

use super::Store;
use super::meta::{self, CF_META, CF_TTL_INDEX, key};
use crate::error::{Error, Result};
use crate::types::{DbIdentity, EpochStart, Role, Seq};

/// What the rewrite did.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) struct Rejoined {
    /// The identity the slave now follows (`None`: the master never had one, no data).
    pub identity: Option<DbIdentity>,
    /// The highest epoch number dropped from the history, if any (burned).
    pub burned: Option<u64>,
}

/// Drops the trailing epochs that hold no entry (`start_seq > last_seq`: per-open epochs of
/// restarts without writes, an empty promotion epoch) and returns the highest dropped number.
/// At least one record stays: the one covering `last_seq` (with `last_seq == 0`, the first).
/// Records are sorted by epoch with non-decreasing `start_seq`, so the empty ones are a suffix.
pub(crate) fn trim_trailing_empty(
    mut history: Vec<EpochStart>,
    last_seq: Seq,
) -> (Vec<EpochStart>, Option<u64>) {
    let keep = history
        .iter()
        .rposition(|e| e.start_seq <= last_seq)
        .map_or(1, |i| i + 1)
        .min(history.len());
    if keep >= history.len() {
        return (history, None);
    }
    let burned = history[keep..].iter().map(|e| e.epoch).max();
    history.truncate(keep);
    (history, burned)
}

impl Store {
    /// One synced batch: role = slave; the trailing empty epochs dropped from the history and
    /// their highest number burned into `max_burned_epoch` (max with the stored value), like
    /// `demote_to_slave`; `origin_identity = (db_uuid, newest remaining epoch)` with the
    /// history; master-only keys removed (`__ttl_index`, `promoted_from_len`, `last_ts`); and
    /// the slave-state keys removed so the slave starts ready (`node_state`, `repair_until`,
    /// `diverged_at`, `max_entry_bytes`, `bootstrap_cfs`). Entries and user data are untouched.
    /// `Err(WrongRole)` unless the stored role is `master`; nothing is written then.
    pub(crate) fn rejoin_as_slave(&self) -> Result<Rejoined> {
        let m = self
            .load_meta()?
            .ok_or_else(|| Error::corrupt("__meta is empty"))?;
        if m.role != Role::Master {
            return Err(Error::WrongRole {
                expected: Role::Master,
                found: m.role,
            });
        }
        if m.identity.is_some() && m.epoch_history.is_empty() {
            return Err(Error::corrupt(
                "origin_identity without epoch_history in __meta",
            ));
        }
        let (history, burned) = trim_trailing_empty(m.epoch_history, m.last_seq);
        let identity = m.identity.map(|id| DbIdentity {
            db_uuid: id.db_uuid,
            epoch: history.last().map_or(id.epoch, |e| e.epoch),
        });
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ROLE, Role::Slave.as_bytes());
        if let Some(id) = &identity {
            // Identity and history in the same batch (CLAUDE.md invariant).
            batch.put_cf(&meta_cf, key::ORIGIN_IDENTITY, meta::encode_identity(id));
            batch.put_cf(
                &meta_cf,
                key::EPOCH_HISTORY,
                meta::encode_epoch_history(&history),
            );
        }
        if let Some(b) = burned {
            batch.put_cf(
                &meta_cf,
                key::MAX_BURNED_EPOCH,
                m.max_burned_epoch.max(b).to_be_bytes(),
            );
        }
        for k in [
            key::PROMOTED_FROM_LEN,
            key::LAST_TS,
            key::NODE_STATE,
            key::REPAIR_UNTIL,
            key::DIVERGED_AT,
            key::MAX_ENTRY_BYTES,
            key::BOOTSTRAP_CFS,
        ] {
            batch.delete_cf(&meta_cf, k);
        }
        // Every `__ttl_index` key sorts below `last ‖ 0x00`.
        let ttl = self.cf(CF_TTL_INDEX);
        let last = self
            .db()
            .iterator_cf(&ttl, IteratorMode::End)
            .next()
            .transpose()?;
        if let Some((last, _)) = last {
            let mut end = last.to_vec();
            end.push(0);
            batch.delete_range_cf(&ttl, Vec::new(), end);
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        Ok(Rejoined { identity, burned })
    }
}
```

In `src/store/mod.rs` add `pub(crate) mod offline;` and `pub(crate) mod rejoin;` to the module list.

In `src/store/meta.rs` change the doc of `MAX_BURNED_EPOCH` to: `/// The highest epoch number this node ever started and then gave up (`Master::demote`, or the empty trailing epochs `Slave::rejoin` drops). A later promotion of this node never reuses it.`

In `src/master/writer.rs` (`bump_epoch`), change the comment to: `// `max_burned_epoch` needs no check here: a number is burned only by `demote` or `Slave::rejoin`, which turn the node into a slave, and a slave becomes a master again only through a promotion, which does honour it (`Store::promote_to_master`).`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CARGO_BUILD_JOBS=4 cargo test --lib store::`
Expected: all store tests pass, including the 5 new ones.

- [ ] **Step 5: Commit**

```bash
git add src/store/rejoin.rs src/store/offline.rs src/store/mod.rs src/store/meta.rs src/master/writer.rs
git commit -m "feat(store): rejoin rewrite batch and read-only stored_meta

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 3: `Slave::rejoin`

**Files:**
- Modify: `src/slave/rejoin.rs` (add `Slave::rejoin`), `src/slave/mod.rs:61-78` (`Slave::open` → `Slave::start`), `src/failpoint.rs` (doc list)
- Create: `tests/rejoin.rs`
- Modify: `tests/failpoints.rs` (crash test)

**Interfaces:**
- Consumes: `store::offline::stored_meta`, `Store::rejoin_as_slave`, `Rejoined` (Task 2); `RejoinOptions` (Task 1); `repoint::PendingRepoint { generation, halt_error, reply }`, `Inner::pending_repoint()`, `Inner::repoint_divergence`, `Inner::repoint_tx`; `crate::failpoint::io_error`; `crate::background::blocking`.
- Produces:
  - `pub async fn Slave::rejoin(cfg: SlaveConfig, opts: RejoinOptions) -> Result<(Slave, Result<RepointOutcome>)>`
  - private `async fn Slave::start(cfg: SlaveConfig, settle: Option<RepointDivergence>) -> Result<(Slave, Option<oneshot::Receiver<Result<RepointOutcome>>>)>` (`Slave::open(cfg)` = `start(cfg, None)`)
  - failpoint `rejoin-after-rewrite` (`return`: the rewrite is committed, the slave is not opened, `Err(Io)`).

- [ ] **Step 1: Write the failing tests**

Create `tests/rejoin.rs`:

```rust
//! `Slave::rejoin`: a former master becomes a slave without wiping its directory
//! (DESIGN.md §13.2).

mod support;

use replicated_rocksdb::{
    AcceptDataLoss, CfSpec, Error, HaltReason, Master, MasterConfig, ReadApi, RejoinOptions,
    RepointDivergence, RepointOutcome, Role, Seq, Slave, SlaveState,
};
use support::{Cluster, assert_converged, wait_state};

/// A (master) → B (slave), in sync. With `a_ahead`, B stops first and A takes two entries B
/// never sees. A stops; B reopens and is promoted (plain `promote`: A is down). Returns A's
/// master config, B as the new master, and the first seq B does not share with A.
async fn failover(c: &Cluster, a_ahead: bool) -> (MasterConfig, Master, Seq) {
    let a_cfg = c.master_config("a");
    let a = Master::open(a_cfg.clone()).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    b.shutdown().await.unwrap();
    if a_ahead {
        a.put("c", b"only on a", b"2").unwrap();
        a.put("c", b"also only on a", b"3").unwrap();
    }
    a.shutdown().await.unwrap();
    let b = Slave::open(b_cfg).await.unwrap();
    let fork = b.last_seq() + 1;
    let b = b.promote(c.master_config("b")).await.unwrap();
    (a_cfg, b, fork)
}

async fn rejoin(
    c: &Cluster,
    b: &Master,
    opts: RejoinOptions,
) -> (Slave, Result<RepointOutcome, Error>, std::sync::Arc<support::RecordingListener>) {
    let (cfg, listener) = c.slave_config("a", b.serve_addr().unwrap());
    let (slave, outcome) = tokio::time::timeout(support::WAIT, Slave::rejoin(cfg, opts))
        .await
        .expect("rejoin settles")
        .expect("the slave is opened");
    (slave, outcome, listener)
}

/// The old master is behind the new one (the new master wrote after the promotion): it
/// follows the new master's epoch without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_former_master_behind_the_new_one_advances_without_a_bootstrap() {
    let c = Cluster::new();
    let (_, b, _) = failover(&c, false).await;
    b.put("c", b"after failover", b"x").unwrap();
    let (a, outcome, la) = rejoin(&c, &b, RejoinOptions::default()).await;
    assert_eq!(outcome.unwrap(), RepointOutcome::EpochAdvanced);
    assert_converged(&b, &[("a", &a)]).await;
    assert_eq!(la.bootstraps(), 0, "no bootstrap");
    let history = a.promotion_info().epoch_history;
    assert_eq!(history[..], b.epoch_history()[..history.len()], "a prefix of B's");
    assert_eq!(a.promotion_info().role, Role::Slave);
    b.put("c", b"later", b"y").unwrap();
    assert_converged(&b, &[("a", &a)]).await;
}

/// The old master restarted after the failover without writes: its per-open epoch (same
/// number as B's promotion epoch, another nonce) is dropped and burned, and it still follows
/// B without a bootstrap.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_trailing_empty_epoch_is_burned_and_the_old_master_advances() {
    let c = Cluster::new();
    let (a_cfg, b, _) = failover(&c, false).await;
    let restarted = Master::open(a_cfg).await.unwrap();
    let restart_epoch = restarted.identity().epoch;
    assert_eq!(restart_epoch, b.identity().epoch, "the same number, another nonce");
    restarted.shutdown().await.unwrap();
    let (a, outcome, la) = rejoin(&c, &b, RejoinOptions::default()).await;
    assert_eq!(outcome.unwrap(), RepointOutcome::EpochAdvanced);
    assert_eq!(a.promotion_info().max_burned_epoch, restart_epoch);
    assert_converged(&b, &[("a", &a)]).await;
    assert_eq!(la.bootstraps(), 0);
}

/// The old master holds entries B never saw: refused by default, halted with its data intact,
/// the suffix salvageable, and the halt survives a restart.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn divergence_is_refused_by_default_and_nothing_is_deleted() {
    let c = Cluster::new();
    let (_, b, fork) = failover(&c, true).await;
    let (a, outcome, _) = rejoin(&c, &b, RejoinOptions::default()).await;
    let a_last = a.last_seq();
    match outcome {
        Err(Error::Diverged { fork_seq, local_last_seq, divergent_entries }) => {
            assert_eq!((fork_seq, local_last_seq, divergent_entries), (fork, a_last, 2))
        }
        other => panic!("expected Diverged, got {other:?}"),
    }
    assert_eq!(a.status().state, SlaveState::Halted(HaltReason::Diverged));
    assert_eq!(a.get("c", b"only on a").unwrap(), Some(b"2".to_vec()));
    assert_eq!(
        a.diverged_entries().unwrap().iter().map(|e| e.seq).collect::<Vec<_>>(),
        (fork..=a_last).collect::<Vec<_>>()
    );
    let (cfg, _) = c.slave_config("a", b.serve_addr().unwrap());
    let a = c.restart_slave(a, cfg).await;
    assert_eq!(a.status().state, SlaveState::Halted(HaltReason::Diverged), "persisted");
    assert_eq!(a.last_seq(), a_last, "nothing deleted");
}

/// With `Repair { AcceptDataLoss }` only the divergent suffix is discarded and the old master
/// ends up equal to the new one.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repair_with_consent_discards_only_the_divergent_suffix() {
    let c = Cluster::new();
    let (_, b, fork) = failover(&c, true).await;
    let opts = RejoinOptions {
        on_divergence: RepointDivergence::Repair { accept_loss: AcceptDataLoss },
    };
    let (a, outcome, la) = rejoin(&c, &b, opts).await;
    assert_eq!(
        outcome.unwrap(),
        RepointOutcome::Repaired { discarded: fork..=fork + 1 }
    );
    assert_converged(&b, &[("a", &a)]).await;
    assert_eq!(a.get("c", b"only on a").unwrap(), None);
    assert_eq!(la.bootstraps(), 0, "repaired, not bootstrapped");
}

/// Refusals before the rewrite leave the directory unchanged.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_refuses_a_slave_directory_an_empty_one_and_an_invalid_config() {
    let c = Cluster::new();
    let (_, b, _) = failover(&c, false).await;
    let b_addr = b.serve_addr().unwrap();
    // A slave directory.
    let (s, _) = c.slave("s", b_addr).await;
    s.shutdown().await.unwrap();
    let (cfg, _) = c.slave_config("s", b_addr);
    assert!(matches!(
        Slave::rejoin(cfg, RejoinOptions::default()).await,
        Err(Error::WrongRole { expected: Role::Master, found: Role::Slave })
    ));
    // No database.
    let (cfg, _) = c.slave_config("empty", b_addr);
    assert!(matches!(
        Slave::rejoin(cfg, RejoinOptions::default()).await,
        Err(Error::Config(_))
    ));
    // Invalid config: the stored master stays a master.
    let (mut cfg, _) = c.slave_config("a", b_addr);
    cfg.identifier = "bad id".into();
    assert!(matches!(
        Slave::rejoin(cfg, RejoinOptions::default()).await,
        Err(Error::Config(_))
    ));
    let m = Master::open(c.master_config("a")).await.expect("still a master");
    m.shutdown().await.unwrap();
}

/// Review focus: the directory is still open by a running master in this process.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn rejoin_refuses_a_directory_that_is_still_open() {
    let c = Cluster::new();
    let (a_cfg, b, _) = failover(&c, false).await;
    let a = Master::open(a_cfg).await.unwrap();
    let id = a.identity();
    let (cfg, _) = c.slave_config("a", b.serve_addr().unwrap());
    assert!(Slave::rejoin(cfg, RejoinOptions::default()).await.is_err());
    assert_eq!(a.identity(), id);
    a.put("c", b"still writing", b"1").expect("the master is untouched");
    a.shutdown().await.unwrap();
    let a = Master::open(c.master_config("a")).await.expect("still stored as master");
    a.shutdown().await.unwrap();
}
```

Note: `Master::epoch_history()` and `Slave::promotion_info()` exist (`src/master/mod.rs`, `src/slave/promote.rs`); `Role` is re-exported from the crate root. If `RecordingListener` is private to `support`, use `support::RecordingListener` as written (it is `pub`).

Append to `tests/failpoints.rs` (add `Master, MasterConfig, RejoinOptions, Slave` etc. to its `use replicated_rocksdb::{...}` list as needed):

```rust
/// A crash right after the rewrite batch (before the slave opens) leaves a slave directory
/// that reopens as a slave and still refuses the divergence (consent is never persisted).
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_crash_after_the_rejoin_rewrite_reopens_as_a_halting_slave() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = Master::open(c.master_config("a")).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let (b_cfg, _) = c.slave_config("b", a.serve_addr().unwrap());
    let b = Slave::open(b_cfg.clone()).await.unwrap();
    a.put("c", b"k", b"1").unwrap();
    assert_converged(&a, &[("b", &b)]).await;
    b.shutdown().await.unwrap();
    a.put("c", b"only on a", b"2").unwrap();
    a.shutdown().await.unwrap();
    let b = Slave::open(b_cfg).await.unwrap().promote(c.master_config("b")).await.unwrap();

    fail::cfg("rejoin-after-rewrite", "1*return").unwrap();
    let (cfg, _) = c.slave_config("a", b.serve_addr().unwrap());
    let err = Slave::rejoin(
        cfg.clone(),
        RejoinOptions {
            on_divergence: replicated_rocksdb::RepointDivergence::Repair {
                accept_loss: replicated_rocksdb::AcceptDataLoss,
            },
        },
    )
    .await
    .err()
    .expect("the crash");
    assert!(matches!(err, Error::Io(_)), "{err}");
    fail::remove("rejoin-after-rewrite");
    assert_eq!(
        support::RawDb::open(&c.path("a")).meta(b"role"),
        Some(b"slave".to_vec())
    );
    let a = Slave::open(cfg).await.unwrap();
    wait_until("a halts Diverged", || {
        a.status().state == SlaveState::Halted(HaltReason::Diverged)
    })
    .await;
    assert_eq!(a.get("c", b"only on a").unwrap(), Some(b"2".to_vec()));
    scenario.teardown();
}
```

(If `support::RawDb` keeps the DB open, wrap it: `{ let raw = support::RawDb::open(..); assert_eq!(raw.meta(..), ..); }` so it is dropped before `Slave::open`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CARGO_BUILD_JOBS=4 cargo test --test rejoin`
Expected: compile error (`Slave::rejoin` not found).

- [ ] **Step 3: Implement**

In `src/slave/mod.rs` replace the body of `Slave::open` and add `start` (add `use tokio::sync::oneshot;` and `use std::sync::PoisonError;`; `RepointDivergence`/`RepointOutcome` are already in scope through `pub use repoint::...`):

```rust
    pub async fn open(cfg: SlaveConfig) -> Result<Self> {
        Self::start(cfg, None).await.map(|(slave, _)| slave)
    }

    /// Opens the slave. With `settle`, the first handshake is armed as a repoint (generation 0)
    /// with that divergence mode before the driver starts, so it never follows
    /// `cfg.on_divergence`; the receiver gets that handshake's outcome, exactly as
    /// `Slave::repoint` returns it (`Slave::rejoin`).
    async fn start(
        cfg: SlaveConfig,
        settle: Option<RepointDivergence>,
    ) -> Result<(Self, Option<oneshot::Receiver<Result<RepointOutcome>>>)> {
        cfg.validate()?;
        let (store, meta) = Store::open(&cfg.path, Role::Slave, cfg.rocksdb.clone())?;
        let inner = Arc::new(Inner::new(store, cfg, &meta)?);
        let settled = settle.map(|mode| {
            let (reply, settled) = oneshot::channel();
            let mut pending = inner.pending_repoint();
            *inner
                .repoint_divergence
                .lock()
                .unwrap_or_else(PoisonError::into_inner) = Some(mode);
            *pending = Some(repoint::PendingRepoint {
                generation: *inner.repoint_tx.borrow(),
                halt_error: None,
                reply,
            });
            settled
        });
        let server =
            crate::server::start_for(&inner, inner.cfg.serve.clone(), inner.replicas.clone())
                .await?;
        let driver = driver::spawn(&inner);
        let maintenance = maintenance::spawn(&inner);
        let verifier = verifier::spawn(&inner);
        Ok((
            Slave {
                inner,
                driver: Some(driver),
                maintenance: Some(maintenance),
                verifier,
                server,
            },
            settled,
        ))
    }
```

Append to `src/slave/rejoin.rs`:

```rust
use super::{RepointOutcome, Slave, SlaveConfig};
use crate::error::{Error, Result};
use crate::store::Store;
use crate::store::offline::stored_meta;
use crate::types::Role;

impl Slave {
    /// Turns a stopped master's directory into a slave of `cfg.upstream` **without wiping
    /// it** (DESIGN.md §13.2). Explicit operator action; nothing calls it automatically.
    ///
    /// 1. Refuses, leaving the directory unchanged, unless the stored role is `master`
    ///    (`Err(WrongRole)`; no database: `Err(Config)`), or when `cfg` is invalid
    ///    (`Err(Config)`) or the directory cannot be opened (for example a `Master` of this
    ///    process still holds it).
    /// 2. One synced batch: `role = slave`; trailing epochs without entries (restarts that took
    ///    no writes, an empty promotion epoch) are dropped and their highest number burned into
    ///    `max_burned_epoch`, as `Master::demote` does; `origin_identity` follows the newest
    ///    remaining record (same batch); master-only keys (`__ttl_index`, `promoted_from_len`,
    ///    `last_ts`) are removed. The role is persisted before the handshake: a slave never
    ///    writes and divergence halts by default, and consent is never persisted, so a crash
    ///    after the batch reopens as a slave that halts `Diverged` if it diverged.
    /// 3. Opens the slave and returns once its first handshake with `cfg.upstream` settles,
    ///    under `opts.on_divergence` (never `cfg.on_divergence`) and with the outcomes of
    ///    [`Slave::repoint`]: `Ok(EpochAdvanced)` when the old master's log is a prefix of the
    ///    new master's (after a failover the new master always runs a newer epoch, so this is
    ///    the usual "resumed" answer), `Resumed`, `Bootstrapped`, `Repaired { discarded }`,
    ///    `Retrying { .. }`, or `Err(Diverged { .. })` under `RepointDivergence::Refuse` (the
    ///    default) with the slave halted `Diverged`, its data intact and
    ///    [`diverged_entries`](Self::diverged_entries) available. The consent in `opts` is
    ///    single-use, as for `repoint`.
    ///
    /// **Return value:** the outer `Err` means no slave was opened: a refusal of step 1 (the
    /// directory is unchanged and still a master), or a failure after step 2 (the directory
    /// is now a slave; open it with `Slave::open`). Otherwise the slave is returned with the
    /// first handshake's result, which may itself be an error: a halted slave is never lost.
    pub async fn rejoin(
        cfg: SlaveConfig,
        opts: RejoinOptions,
    ) -> Result<(Slave, Result<RepointOutcome>)> {
        cfg.validate()?;
        let (path, tuning, node) = (cfg.path.clone(), cfg.rocksdb.clone(), cfg.identifier.clone());
        let rejoined = crate::background::blocking(move || {
            match stored_meta(&path)? {
                None => {
                    return Err(Error::Config(format!(
                        "{} holds no database to rejoin",
                        path.display()
                    )));
                }
                Some(m) if m.role != Role::Master => {
                    return Err(Error::WrongRole {
                        expected: Role::Master,
                        found: m.role,
                    });
                }
                Some(_) => {}
            }
            let (store, _) = Store::open(&path, Role::Master, tuning)?;
            store.rejoin_as_slave()
        })
        .await?;
        tracing::info!(
            node = %node,
            epoch = rejoined.identity.map(|i| i.epoch),
            burned = rejoined.burned,
            "former master rewritten as a slave"
        );
        // Test hook: the rewrite is committed, the slave is not opened yet (a crash here).
        crate::failpoint::io_error("rejoin-after-rewrite")?;
        let (slave, settled) = Slave::start(cfg, Some(opts.on_divergence)).await?;
        let settled = settled.expect("armed above");
        // The sender is dropped only when the slave shuts down before the handshake settles.
        let outcome = settled.await.unwrap_or(Err(Error::RepointSuperseded));
        Ok((slave, outcome))
    }
}
```

In `src/failpoint.rs` add to the doc list (before `slave-snapshot-after-check`):
`//! `rejoin-after-rewrite` (`Slave::rejoin` committed its rewrite batch, before the slave opens; `return` makes it fail with `Err(Io)`, as a crash there would),`

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CARGO_BUILD_JOBS=4 cargo test --test rejoin`
Expected: 6 passed.
Run: `CARGO_BUILD_JOBS=4 cargo test --features failpoints --test failpoints a_crash_after_the_rejoin_rewrite`
Expected: 1 passed.
Run: `CARGO_BUILD_JOBS=4 cargo test --test repoint --test promotion` (the refactored `Slave::open` must not change behaviour)
Expected: all pass.

- [ ] **Step 5: Commit**

```bash
git add src/slave/rejoin.rs src/slave/mod.rs src/failpoint.rs tests/rejoin.rs tests/failpoints.rs
git commit -m "feat: Slave::rejoin turns a former master into a slave without wiping it

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 4: Fence check

**Files:**
- Modify: `src/store/offline.rs` (add `OfflineLog`)
- Create: `src/node/fence.rs`
- Modify: `src/node/mod.rs` (add `pub(crate) mod fence;`)

**Interfaces:**
- Consumes: `stored_meta` (Task 2); `FenceReason` (Task 1); `admin::{compare, Comparison, Side, NodeInfo}`, `admin::status_classified(&UpstreamConfig, &str, Option<Seq>) -> Result<NodeInfo, StatusFailure>`, `admin::StatusFailure::{Unreachable { reason, source }, Answered(String)}`, `UpstreamConfig::uri()`, `background::blocking`.
- Produces:
  - `pub(crate) struct OfflineLog` with `pub(crate) fn open(path: &Path) -> Result<Option<OfflineLog>>`, `pub(crate) fn meta(&self) -> &MetaSnapshot`, `pub(crate) fn node_info(&self, node_id: &str) -> NodeInfo`, `pub(crate) fn entry_crc(&self, seq: Seq) -> Result<Option<u32>>`
  - `pub(crate) enum PeerAnswer { Answered { info: NodeInfo, local_probe: Option<(Seq, Option<u32>)> }, Unreachable, Error(String) }`
  - `pub(crate) fn decide(me: &NodeInfo, answers: &[(String, PeerAnswer)], allow_unreachable: bool) -> Option<FenceReason>` (`None` = cleared)
  - `pub(crate) async fn check(path: &Path, node_id: &str, peers: &[UpstreamConfig], allow_unreachable: bool) -> Result<Option<FenceReason>>` (writes nothing, starts no epoch; `Err` only when the local store cannot be read)

- [ ] **Step 1: Write the failing tests**

`src/node/fence.rs` test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DbIdentity, EpochStart};

    const DB: u128 = 1;

    fn rec(epoch: u64, start_seq: Seq, nonce: u64) -> EpochStart {
        EpochStart { epoch, start_seq, nonce }
    }

    fn node(id: &str, role: Role, db: u128, history: Vec<EpochStart>, last_seq: Seq) -> NodeInfo {
        let mut n = NodeInfo::new(id, role);
        n.identity = Some(DbIdentity {
            db_uuid: uuid::Uuid::from_u128(db),
            epoch: history.last().map_or(1, |e| e.epoch),
        });
        n.epoch_history = history;
        n.last_seq = last_seq;
        n
    }

    /// The fenced old master: epochs 1 and 2 (its last open), 10 entries.
    fn me() -> NodeInfo {
        node("a", Role::Master, DB, vec![rec(1, 1, 11), rec(2, 6, 22)], 10)
    }

    fn answered(info: NodeInfo) -> PeerAnswer {
        PeerAnswer::Answered { info, local_probe: None }
    }

    #[test]
    fn no_peers_never_clears() {
        assert_eq!(decide(&me(), &[], true), Some(FenceReason::NoPeers));
    }

    #[test]
    fn equal_and_behind_slaves_clear() {
        let equal = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 22)], 10);
        let behind = node("d", Role::Slave, DB, vec![rec(1, 1, 11)], 4);
        let answers = [("u1".to_string(), answered(equal)), ("u2".to_string(), answered(behind))];
        assert_eq!(decide(&me(), &answers, false), None);
    }

    #[test]
    fn unreachable_blocks_unless_accepted_but_errors_always_block() {
        let ok = node("c", Role::Slave, DB, vec![rec(1, 1, 11)], 4);
        let answers = [
            ("u1".to_string(), answered(ok.clone())),
            ("http://dead:1".to_string(), PeerAnswer::Unreachable),
        ];
        assert_eq!(
            decide(&me(), &answers, false),
            Some(FenceReason::PeerUnreachable { peer: "http://dead:1".into() })
        );
        assert_eq!(decide(&me(), &answers, true), None);
        let all_dead = [("http://dead:1".to_string(), PeerAnswer::Unreachable)];
        assert_eq!(decide(&me(), &all_dead, true), None, "the consent covers them all");
        let error = [("u3".to_string(), PeerAnswer::Error("denied".into()))];
        assert_eq!(
            decide(&me(), &error, true),
            Some(FenceReason::PeerError { peer: "u3".into(), error: "denied".into() })
        );
    }

    #[test]
    fn a_master_peer_wins_over_every_other_reason() {
        let master = node("b", Role::Master, DB, vec![rec(1, 1, 11), rec(2, 6, 99)], 12);
        let answers = [
            ("http://dead:1".to_string(), PeerAnswer::Unreachable),
            ("u3".to_string(), PeerAnswer::Error("denied".into())),
            ("u2".to_string(), answered(master)),
        ];
        assert_eq!(
            decide(&me(), &answers, false),
            Some(FenceReason::PeerIsMaster { peer: "b".into() })
        );
    }

    #[test]
    fn a_later_or_diverged_peer_blocks() {
        let later = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 22)], 15);
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(later))], false),
            Some(FenceReason::PeerAhead { peer: "c".into() })
        );
        // Promoted elsewhere at seq 6 with another nonce, holding entries past it.
        let other = node("d", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 33)], 8);
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(other))], false),
            Some(FenceReason::PeerDiverged { peer: "d".into() })
        );
    }

    #[test]
    fn a_newer_epoch_anywhere_blocks_as_ahead() {
        // Behind and compatible by position, but it holds another epoch 2 (another nonce,
        // starting after both positions): someone else started this node's newest epoch number.
        let knows = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 11, 33)], 4);
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(knows))], false),
            Some(FenceReason::PeerAhead { peer: "c".into() })
        );
        // Burned an epoch at or above this node's newest (a promotion undone elsewhere).
        let mut burned = node("c", Role::Slave, DB, vec![rec(1, 1, 11)], 4);
        burned.max_burned_epoch = 2;
        assert_eq!(
            decide(&me(), &[("u".to_string(), answered(burned))], false),
            Some(FenceReason::PeerAhead { peer: "c".into() })
        );
    }

    /// Review focus: a peer of another database never clears the fence.
    #[test]
    fn a_peer_of_another_database_never_clears() {
        let foreign = node("x", Role::Slave, 2, vec![rec(1, 1, 5)], 1);
        assert!(matches!(
            decide(&me(), &[("u".to_string(), answered(foreign))], true),
            Some(FenceReason::PeerError { peer, .. }) if peer == "x"
        ));
    }

    #[test]
    fn a_crc_mismatch_at_the_peers_last_entry_is_divergence() {
        let mut me = me();
        me.last_entry_crc = Some(1);
        let mut peer = node("c", Role::Slave, DB, vec![rec(1, 1, 11), rec(2, 6, 22)], 8);
        peer.last_entry_crc = Some(7);
        let answers = [(
            "u".to_string(),
            PeerAnswer::Answered { info: peer, local_probe: Some((8, Some(9))) },
        )];
        assert_eq!(
            decide(&me, &answers, false),
            Some(FenceReason::PeerDiverged { peer: "c".into() })
        );
    }

    #[tokio::test]
    async fn check_reads_the_closed_store_without_writing() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = crate::store::test_support::open_store(dir.path());
            let id = DbIdentity { db_uuid: uuid::Uuid::from_u128(DB), epoch: 1 };
            store
                .write_meta(
                    &[
                        (crate::store::meta::key::ORIGIN_IDENTITY, &crate::store::meta::encode_identity(&id)),
                        (
                            crate::store::meta::key::EPOCH_HISTORY,
                            &crate::store::meta::encode_epoch_history(&[rec(1, 1, 11)]),
                        ),
                    ],
                    true,
                )
                .unwrap();
        }
        let before = crate::store::offline::stored_meta(dir.path()).unwrap();
        assert_eq!(check(dir.path(), "a", &[], false).await.unwrap(), Some(FenceReason::NoPeers));
        // Port 1 on loopback: nothing listens, the connection is refused at once.
        let dead = UpstreamConfig::new("127.0.0.1", 1);
        assert_eq!(
            check(dir.path(), "a", std::slice::from_ref(&dead), false).await.unwrap(),
            Some(FenceReason::PeerUnreachable { peer: dead.uri() })
        );
        assert_eq!(check(dir.path(), "a", &[dead], true).await.unwrap(), None);
        assert_eq!(crate::store::offline::stored_meta(dir.path()).unwrap(), before, "nothing written");
        let log = crate::store::offline::OfflineLog::open(dir.path()).unwrap().unwrap();
        let info = log.node_info("a");
        assert_eq!((info.role, info.last_seq, info.node_state.as_str()), (Role::Master, 0, "fenced"));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CARGO_BUILD_JOBS=4 cargo test --lib node::fence`
Expected: compile error (`decide`, `check`, `OfflineLog` not found).

- [ ] **Step 3: Implement**

Append to `src/store/offline.rs` (extend imports: `use rocksdb::ColumnFamilyDescriptor; use super::meta::CF_CHANGELOG; use crate::admin::NodeInfo; use crate::error::Error; use crate::types::{LogEntry, Seq};`):

```rust
/// A read-only view of a closed store: its `__meta` and `__changelog`. Opening it takes no
/// LOCK and writes nothing, so a fenced master's directory stays exactly as it was. Drop it
/// before opening the directory for writing.
pub(crate) struct OfflineLog {
    db: DB,
    meta: MetaSnapshot,
}

impl OfflineLog {
    /// `None` when `path` holds no stored role (see [`stored_meta`]).
    pub(crate) fn open(path: &Path) -> Result<Option<OfflineLog>> {
        let Some(meta) = stored_meta(path)? else {
            return Ok(None);
        };
        let db = DB::open_cf_descriptors_read_only(
            &Options::default(),
            path,
            [
                ColumnFamilyDescriptor::new(CF_META, Options::default()),
                ColumnFamilyDescriptor::new(CF_CHANGELOG, Options::default()),
            ],
            false,
        )?;
        Ok(Some(OfflineLog { db, meta }))
    }

    pub(crate) fn meta(&self) -> &MetaSnapshot {
        &self.meta
    }

    /// What this node would report in `Status` (no replicas, no probes, no upstream), with
    /// `node_state = "fenced"`.
    pub(crate) fn node_info(&self, node_id: &str) -> NodeInfo {
        let m = &self.meta;
        let mut info = NodeInfo::new(node_id, m.role);
        info.identity = m.identity;
        info.epoch_history = m.epoch_history.clone();
        info.last_seq = m.last_seq;
        info.last_entry_crc = m.last_entry_crc;
        info.node_state = "fenced".into();
        info.max_burned_epoch = m.max_burned_epoch;
        info
    }

    /// The CRC of the entry at `seq` (as `Store::entry_crc`): from the changelog, or
    /// `last_entry_crc` at `last_seq`; `None` when not held.
    pub(crate) fn entry_crc(&self, seq: Seq) -> Result<Option<u32>> {
        if seq == 0 || seq > self.meta.last_seq {
            return Ok(None);
        }
        let cf = self
            .db
            .cf_handle(CF_CHANGELOG)
            .ok_or_else(|| Error::corrupt("__changelog missing in read-only open"))?;
        if let Some(v) = self.db.get_cf(&cf, seq.to_be_bytes())? {
            return Ok(Some(LogEntry::decode(&v)?.crc32c));
        }
        Ok((seq == self.meta.last_seq).then_some(self.meta.last_entry_crc).flatten())
    }
}
```

`src/node/fence.rs` (above the tests):

```rust
//! The fence check of a stored master (DESIGN.md §13.2): may this directory open as the
//! master? It reads the node's own position from the closed store and every peer's `Status`,
//! and writes nothing.

use std::path::Path;
use std::sync::Arc;

use tokio::task::JoinSet;

use super::types::FenceReason;
use crate::admin::{self, Comparison, NodeInfo, Side, StatusFailure, compare};
use crate::background::blocking;
use crate::error::{Error, Result};
use crate::slave::UpstreamConfig;
use crate::store::offline::OfflineLog;
use crate::types::{Role, Seq};

/// One peer's answer to `Status`.
pub(crate) enum PeerAnswer {
    /// `local_probe`: this node's CRC at the peer's `last_seq`, when the peer is shorter.
    Answered {
        info: NodeInfo,
        local_probe: Option<(Seq, Option<u32>)>,
    },
    Unreachable,
    Error(String),
}

/// Severity ranks: lower wins (decision 8 of the plan).
const MASTER: u8 = 0;
const DIVERGED: u8 = 1;
const AHEAD: u8 = 2;
const ERROR: u8 = 3;
const UNREACHABLE: u8 = 4;

/// `None` when the fence clears: at least one peer, every peer answered (or
/// `allow_unreachable`), none is a master, none is later than or diverged from `me`, none is
/// of another database, and none knows an epoch at or above `me`'s newest one other than
/// that very record. Otherwise the most severe reason, first in `answers` order.
pub(crate) fn decide(
    me: &NodeInfo,
    answers: &[(String, PeerAnswer)],
    allow_unreachable: bool,
) -> Option<FenceReason> {
    if answers.is_empty() {
        return Some(FenceReason::NoPeers);
    }
    let mut worst: Option<(u8, FenceReason)> = None;
    for (uri, answer) in answers {
        let verdict = match answer {
            PeerAnswer::Unreachable if allow_unreachable => None,
            PeerAnswer::Unreachable => Some((
                UNREACHABLE,
                FenceReason::PeerUnreachable { peer: uri.clone() },
            )),
            PeerAnswer::Error(error) => Some((
                ERROR,
                FenceReason::PeerError {
                    peer: uri.clone(),
                    error: error.clone(),
                },
            )),
            PeerAnswer::Answered { info, local_probe } => judge(me, info, *local_probe),
        };
        if let Some((rank, reason)) = verdict {
            if worst.as_ref().is_none_or(|(w, _)| rank < *w) {
                worst = Some((rank, reason));
            }
        }
    }
    worst.map(|(_, reason)| reason)
}

fn judge(
    me: &NodeInfo,
    info: &NodeInfo,
    local_probe: Option<(Seq, Option<u32>)>,
) -> Option<(u8, FenceReason)> {
    let peer = info.node_id.clone();
    if info.role == Role::Master {
        return Some((MASTER, FenceReason::PeerIsMaster { peer }));
    }
    let mut mine = me.clone();
    if let Some((seq, crc)) = local_probe {
        mine.probes.insert(seq, crc);
    }
    match compare(&mine, info) {
        Comparison::DifferentDatabase => {
            return Some((
                ERROR,
                FenceReason::PeerError {
                    peer,
                    error: "a different database".into(),
                },
            ));
        }
        Comparison::Diverged { .. } => return Some((DIVERGED, FenceReason::PeerDiverged { peer })),
        Comparison::Later(Side::B) => return Some((AHEAD, FenceReason::PeerAhead { peer })),
        Comparison::Later(Side::A) | Comparison::Equal => {}
    }
    let newest = me.epoch_history.last().copied()?;
    let knows_newer = info.max_burned_epoch >= newest.epoch
        || info.identity.is_some_and(|id| id.epoch > newest.epoch)
        || info
            .epoch_history
            .iter()
            .any(|r| r.epoch >= newest.epoch && *r != newest);
    knows_newer.then_some((AHEAD, FenceReason::PeerAhead { peer }))
}

/// Runs the fence check for the closed store at `path` (DESIGN.md §13.2). Every peer is asked
/// for `Status` concurrently (each bounded by `admin::STATUS_TIMEOUT`), probed at this node's
/// `last_seq`; this node's CRC at a shorter peer's `last_seq` is read from the closed store.
/// `Err` only when the local store cannot be read; nothing is written and no epoch starts.
pub(crate) async fn check(
    path: &Path,
    node_id: &str,
    peers: &[UpstreamConfig],
    allow_unreachable: bool,
) -> Result<Option<FenceReason>> {
    if peers.is_empty() {
        return Ok(Some(FenceReason::NoPeers));
    }
    let p = path.to_path_buf();
    let log = Arc::new(
        blocking(move || {
            OfflineLog::open(&p)?.ok_or_else(|| {
                Error::Config(format!("{} holds no database", p.display()))
            })
        })
        .await?,
    );
    let me = log.node_info(node_id);
    let probe = Some(me.last_seq).filter(|&s| s > 0);
    let mut calls = JoinSet::new();
    for (i, peer) in peers.iter().cloned().enumerate() {
        let id = node_id.to_string();
        calls.spawn(async move {
            let r = admin::status_classified(&peer, &id, probe).await;
            (i, peer.uri(), r)
        });
    }
    let mut answers: Vec<Option<(String, PeerAnswer)>> = (0..peers.len()).map(|_| None).collect();
    while let Some(joined) = calls.join_next().await {
        let (i, uri, r) = joined.map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
        let answer = match r {
            Ok(info) => {
                let local_probe = if info.last_seq > 0 && info.last_seq < me.last_seq {
                    let (l, seq) = (log.clone(), info.last_seq);
                    Some((seq, blocking(move || l.entry_crc(seq)).await?))
                } else {
                    None
                };
                PeerAnswer::Answered { info, local_probe }
            }
            Err(StatusFailure::Unreachable { reason, .. }) => {
                tracing::warn!(peer = %uri, reason = %reason, "fence check: peer unreachable");
                PeerAnswer::Unreachable
            }
            Err(StatusFailure::Answered(e)) => {
                tracing::warn!(peer = %uri, error = %e, "fence check: peer answered with an error");
                PeerAnswer::Error(e)
            }
        };
        answers[i] = Some((uri, answer));
    }
    drop(log);
    let answers: Vec<_> = answers.into_iter().flatten().collect();
    let verdict = decide(&me, &answers, allow_unreachable);
    match &verdict {
        None => tracing::info!(node = %node_id, "fence check cleared"),
        Some(reason) => tracing::warn!(node = %node_id, %reason, "fence check: still fenced"),
    }
    Ok(verdict)
}
```

`admin::StatusFailure` and `admin::status_classified` are `pub(crate)` already. If the compiler complains that the `JoinSet` future is not `Send` because of `StatusFailure`'s boxed source, keep it as is: `Box<dyn Error + Send + Sync>` is `Send`. Add `pub(crate) mod fence;` to `src/node/mod.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CARGO_BUILD_JOBS=4 cargo test --lib node::fence`
Expected: 9 passed.

- [ ] **Step 5: Commit**

```bash
git add src/node/fence.rs src/node/mod.rs src/store/offline.rs
git commit -m "feat: fence check of a stored master against its peers

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 5: `Node` core

**Files:**
- Create: `src/node/config.rs`, `src/node/handle.rs`, `tests/node.rs`
- Modify: `src/node/mod.rs`, `src/lib.rs` (exports), `src/master/mod.rs` (`path()`), `src/slave/mod.rs` (`path()`)

**Interfaces:**
- Consumes: Tasks 1-4 (`NodeState`, `FenceReason`, `AcceptSplitBrainRisk`, `fence::check(path, node_id, peers, allow_unreachable)`, `offline::stored_meta`); `Master::{open, shutdown, identity, serve_addr, last_seq, snapshot, put, put_with_ttl, put_with, delete, delete_range, write, write_with, create_cf, drop_cf, reap_expired, bump_epoch}`; `Slave::{open, shutdown, status, watch_state, serve_addr, last_seq, snapshot, repoint, clear_halt, verify_now, diverged_entries, wait_streaming, wait_for_seq}`; `ReadApi`.
- Produces:
  - `pub struct NodeConfig { pub master: MasterConfig, pub slave: SlaveConfig, pub peers: Vec<UpstreamConfig>, pub unchecked_master: Option<AcceptSplitBrainRisk> }`, `NodeConfig::new(master: MasterConfig, slave: SlaveConfig) -> NodeConfig`, `pub(crate) fn validate(&self) -> Result<()>`
  - `pub struct Node` (`Send + Sync`); `pub(super) enum Current { Master(Master), Slave(Slave), Fenced(FenceReason), Switching, Failed(String), Stopped }`; `Current::unavailable(&self) -> Error`
  - Node fields (`pub(super)` for Task 6): `cfg: NodeConfig`, `upstream: Arc<Mutex<UpstreamConfig>>`, `role: tokio::sync::RwLock<Current>`; methods `pub(super) fn publish(&self, cur: &Current)`, `pub(super) fn slave_config(&self) -> SlaveConfig`, `pub(super) async fn open_current(cfg: &NodeConfig, upstream: &UpstreamConfig) -> Result<Current>` (free fn in `handle.rs`)
  - Public: `async fn create_master(cfg: NodeConfig) -> Result<Node>`, `async fn create_slave(cfg) -> Result<Node>`, `async fn open(cfg) -> Result<Node>`, `async fn from_master(master: Master, cfg: NodeConfig) -> Result<Node>`, `async fn from_slave(slave: Slave, cfg: NodeConfig) -> Result<Node>`; `fn state(&self) -> NodeState`; `fn watch_state(&self) -> watch::Receiver<NodeState>`; `fn serve_addr(&self) -> Option<SocketAddr>`; `fn last_seq(&self) -> Result<Seq>`; reads `get`, `get_with_meta`, `multi_get`, `list_cfs(&self) -> Result<Vec<CfSpec>>`, `snapshot(&self) -> Result<ReadSnapshot>`; writes with the `Master` signatures; slave admin `async repoint(&self, UpstreamConfig, RepointOptions) -> Result<RepointOutcome>`, `async clear_halt(&self, HaltAction) -> Result<()>`, `async verify_now(&self, VerifyMode) -> Result<VerifyReport>`, `async wait_streaming(&self, Duration) -> Result<()>`, `async wait_for_seq(&self, Seq, Duration) -> Result<()>`, `fn diverged_entries(&self) -> Result<Vec<LogEntry>>`, `fn status(&self) -> Result<SlaveStatus>`
  - `pub(crate) fn Master::path(&self) -> &Path`, `pub(crate) fn Slave::path(&self) -> &Path`
- Test helpers in `tests/node.rs` used again by Task 6: `fn up(SocketAddr) -> UpstreamConfig`, `fn dead_addr() -> SocketAddr`, `fn node_cfg(c: &Cluster, name: &str, upstream: SocketAddr, peers: &[SocketAddr]) -> NodeConfig`, `async fn wait_node(n: &Node, what: &str, f: impl Fn(&NodeState) -> bool)`, `fn streaming(s: &NodeState) -> bool`.

- [ ] **Step 1: Write the failing tests**

Create `tests/node.rs`:

```rust
//! The `Node` handle (DESIGN.md §13.2), with real nodes over 127.0.0.1.

mod support;

use std::net::SocketAddr;

use replicated_rocksdb::{
    AcceptSplitBrainRisk, CfSpec, Error, FenceReason, Master, Node, NodeConfig, NodeState,
    RepointOptions, RepointOutcome, Slave, SlaveState, UpstreamConfig,
};
use support::{Cluster, WAIT, reserved_port, wait_until};

fn up(a: SocketAddr) -> UpstreamConfig {
    UpstreamConfig::from_addr(a)
}

/// An address nothing listens on.
fn dead_addr() -> SocketAddr {
    SocketAddr::from(([127, 0, 0, 1], reserved_port()))
}

/// Node `name`: master and slave share path, identifier and serve port, so the node keeps its
/// address in both roles.
fn node_cfg(c: &Cluster, name: &str, upstream: SocketAddr, peers: &[SocketAddr]) -> NodeConfig {
    let master = c.master_config(name);
    let (mut slave, _) = c.slave_config(name, upstream);
    slave.serve = master.serve.clone();
    let mut cfg = NodeConfig::new(master, slave);
    cfg.peers = peers.iter().copied().map(up).collect();
    cfg
}

async fn wait_node(n: &Node, what: &str, f: impl Fn(&NodeState) -> bool) {
    wait_until(what, || f(&n.state())).await;
}

fn streaming(s: &NodeState) -> bool {
    matches!(s, NodeState::Slave { state: SlaveState::Streaming, .. })
}

fn is_master(s: &NodeState) -> bool {
    matches!(s, NodeState::Master { .. })
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_and_open_each_role() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    assert!(is_master(&a.state()));
    a.create_cf(CfSpec::new("c")).unwrap();
    a.put("c", b"k", b"v").unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[])).await.unwrap();
    wait_node(&b, "b streaming", streaming).await;
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    assert_eq!(b.get("c", b"k").unwrap(), Some(b"v".to_vec()));
    assert_eq!(b.list_cfs().unwrap().len(), 1);
    assert!(matches!(b.put("c", b"k", b"x"), Err(Error::NotMaster)));
    assert!(matches!(b.bump_epoch(), Err(Error::NotMaster)));
    assert!(matches!(a.status(), Err(Error::NotSlave)));
    assert!(matches!(a.diverged_entries(), Err(Error::NotSlave)));

    b.shutdown().await.unwrap();
    assert_eq!(b.state(), NodeState::Stopped);
    assert!(matches!(b.get("c", b"k"), Err(Error::NodeFailed(_))));
    let b = Node::open(node_cfg(&c, "b", a_addr, &[])).await.unwrap();
    wait_node(&b, "b streaming after open", streaming).await;

    let NodeState::Master { identity } = a.state() else { panic!() };
    a.shutdown().await.unwrap();
    let mut cfg = node_cfg(&c, "a", dead_addr(), &[]);
    cfg.unchecked_master = Some(AcceptSplitBrainRisk);
    let a = Node::open(cfg).await.unwrap();
    match a.state() {
        NodeState::Master { identity: now } => assert_eq!(now.epoch, identity.epoch + 1),
        s => panic!("expected Master, got {s:?}"),
    }
    let id = a.bump_epoch().unwrap();
    assert_eq!(a.state(), NodeState::Master { identity: id });
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn create_needs_an_empty_directory_and_open_a_stored_role() {
    let c = Cluster::new();
    assert!(matches!(
        Node::open(node_cfg(&c, "a", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.shutdown().await.unwrap();
    assert!(matches!(
        Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    assert!(matches!(
        Node::create_slave(node_cfg(&c, "a", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    let mut cfg = node_cfg(&c, "x", dead_addr(), &[]);
    cfg.slave.path = c.path("y");
    assert!(matches!(Node::create_master(cfg).await, Err(Error::Config(_))));
    let mut cfg = node_cfg(&c, "x", dead_addr(), &[]);
    cfg.slave.identifier = "y".into();
    assert!(matches!(Node::create_slave(cfg).await, Err(Error::Config(_))));
}

/// A stored master opened without peers stays fenced: no epoch, no port, no reads or writes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_stored_master_without_peers_opens_fenced() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    a.put("c", b"k", b"v").unwrap();
    let NodeState::Master { identity } = a.state() else { panic!() };
    a.shutdown().await.unwrap();

    let a = Node::open(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    assert_eq!(a.state(), NodeState::Fenced { reason: FenceReason::NoPeers });
    assert!(matches!(
        a.get("c", b"k"),
        Err(Error::Fenced { reason: FenceReason::NoPeers })
    ));
    assert!(matches!(a.put("c", b"k", b"x"), Err(Error::Fenced { .. })));
    assert!(matches!(a.snapshot(), Err(Error::Fenced { .. })));
    assert!(matches!(a.last_seq(), Err(Error::Fenced { .. })));
    assert_eq!(a.serve_addr(), None, "nothing is served");
    a.shutdown().await.unwrap();

    let mut cfg = node_cfg(&c, "a", dead_addr(), &[]);
    cfg.unchecked_master = Some(AcceptSplitBrainRisk);
    let a = Node::open(cfg).await.unwrap();
    match a.state() {
        NodeState::Master { identity: now } => {
            assert_eq!(now.epoch, identity.epoch + 1, "the fenced open started no epoch")
        }
        s => panic!("expected Master, got {s:?}"),
    }
    assert_eq!(a.get("c", b"k").unwrap(), Some(b"v".to_vec()));
}

/// A → B. A dies, B is promoted; A restarts through `Node` and is fenced by B.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_old_master_restarts_fenced_by_the_new_one() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let (b, _) = c.slave("b", a_addr).await;
    a.put("c", b"k", b"v").unwrap();
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    a.shutdown().await.unwrap();
    let b = b.promote(c.master_config("b")).await.unwrap();
    let b_addr = b.serve_addr().unwrap();

    let a = Node::open(node_cfg(&c, "a", b_addr, &[b_addr])).await.unwrap();
    assert_eq!(
        a.state(),
        NodeState::Fenced { reason: FenceReason::PeerIsMaster { peer: "b".into() } }
    );
    assert!(matches!(a.get("c", b"k"), Err(Error::Fenced { .. })));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn running_handles_can_be_wrapped() {
    let c = Cluster::new();
    let m = Master::open(c.master_config("a")).await.unwrap();
    let a = Node::from_master(m, node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let s = Slave::open(c.slave_config("b", a_addr).0).await.unwrap();
    let b = Node::from_slave(s, node_cfg(&c, "b", a_addr, &[])).await.unwrap();
    wait_node(&b, "b streaming", streaming).await;

    let other = Master::open(c.master_config("z")).await.unwrap();
    assert!(matches!(
        Node::from_master(other, node_cfg(&c, "y", dead_addr(), &[])).await,
        Err(Error::Config(_))
    ));
    // The refused handle was shut down: its directory opens again.
    Master::open(c.master_config("z")).await.unwrap().shutdown().await.unwrap();
}

/// Slave sub-states are forwarded live; `repoint` updates the upstream used by later reopens;
/// review focus: a refused repoint (invalid address) does not.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_state_forwards_slave_states_and_repoint_updates_the_upstream() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", dead_addr(), &[])).await.unwrap();
    let mut rx = b.watch_state();
    assert!(matches!(b.state(), NodeState::Slave { .. }));
    assert_eq!(
        b.repoint(up(a_addr), RepointOptions::default()).await.unwrap(),
        RepointOutcome::Bootstrapped
    );
    tokio::time::timeout(WAIT, rx.wait_for(streaming)).await.unwrap().unwrap();
    match b.state() {
        NodeState::Slave { upstream, .. } => assert_eq!(upstream, format!("http://{a_addr}")),
        s => panic!("expected Slave, got {s:?}"),
    }
    assert!(matches!(
        b.repoint(UpstreamConfig::new("127.0.0.1", 0), RepointOptions::default()).await,
        Err(Error::Config(_))
    ));
    b.shutdown().await.unwrap();
    b.reopen().await.unwrap();
    wait_node(&b, "b streams from a after reopen", streaming).await;
}
```

Note: `b.reopen()` is implemented in Task 6. Until then, mark that one test `#[ignore = "needs Node::reopen (Task 6)"]` and remove the attribute in Task 6 Step 3; every other test above must pass at the end of this task.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CARGO_BUILD_JOBS=4 cargo test --test node`
Expected: compile error (`Node`, `NodeConfig` not found).

- [ ] **Step 3: Implement**

Add to `Master` (`src/master/mod.rs`, inside `impl Master`):

```rust
    /// The directory this master runs on.
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.inner.cfg.path
    }
```

Add to `Slave` (`src/slave/mod.rs`, inside `impl Slave`):

```rust
    /// The directory this slave runs on.
    pub(crate) fn path(&self) -> &std::path::Path {
        &self.inner.cfg.path
    }
```

`src/node/config.rs`:

```rust
//! `NodeConfig`.

use crate::config::MasterConfig;
use crate::error::{Error, Result};
use crate::slave::{SlaveConfig, UpstreamConfig};

use super::types::AcceptSplitBrainRisk;

/// Configuration of a [`Node`](crate::Node). `master` is used whenever the node runs as master,
/// `slave` whenever it runs as slave (`slave.upstream` is the default upstream). Both must
/// name the same directory and identifier.
#[derive(Clone)]
pub struct NodeConfig {
    pub master: MasterConfig,
    pub slave: SlaveConfig,
    /// The other nodes of the topology, asked by `Node::promote_checked` and by the fence
    /// check of a stored master.
    pub peers: Vec<UpstreamConfig>,
    /// Skip the fence check: a stored master opens as master at once, even if another master
    /// is running. Explicit consent; `None` by default.
    pub unchecked_master: Option<AcceptSplitBrainRisk>,
}

impl NodeConfig {
    /// No peers and no consent to skip the fence check.
    pub fn new(master: MasterConfig, slave: SlaveConfig) -> NodeConfig {
        NodeConfig {
            master,
            slave,
            peers: Vec::new(),
            unchecked_master: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if self.master.path != self.slave.path {
            return Err(Error::Config(
                "NodeConfig: master.path and slave.path must be the same directory".into(),
            ));
        }
        if self.master.identifier != self.slave.identifier {
            return Err(Error::Config(
                "NodeConfig: master.identifier and slave.identifier must be the same".into(),
            ));
        }
        Ok(())
    }
}
```

`src/node/handle.rs`:

```rust
//! The `Node` handle: either role behind one `RwLock`, with its state published on a watch
//! channel (DESIGN.md §13.2).

use std::net::SocketAddr;
use std::sync::{Arc, Mutex, PoisonError};
use std::time::Duration;

use tokio::sync::{RwLock, RwLockReadGuard, watch};
use tokio::task::JoinHandle;

use super::config::NodeConfig;
use super::fence;
use super::types::{FenceReason, NodeState};
use crate::background::blocking;
use crate::config::WriteConcern;
use crate::error::{Error, Result};
use crate::master::{Master, ReapStats};
use crate::read::{ReadApi, ReadSnapshot};
use crate::slave::{
    HaltAction, RepointOptions, RepointOutcome, Slave, SlaveConfig, SlaveState, SlaveStatus,
    UpstreamConfig,
};
use crate::store::offline::stored_meta;
use crate::types::{Batch, CfSpec, DbIdentity, LogEntry, Role, Seq, ValueMeta};
use crate::verify::{VerifyMode, VerifyReport};

pub(super) const STOPPED: &str = "the node was shut down; call Node::reopen";

/// What the node holds.
pub(super) enum Current {
    Master(Master),
    Slave(Slave),
    Fenced(FenceReason),
    Switching,
    Failed(String),
    Stopped,
}

impl Current {
    /// The error for a call this role cannot serve: a write on a slave is `NotMaster`, a
    /// slave-only call on a master is `NotSlave`.
    pub(super) fn unavailable(&self) -> Error {
        match self {
            Current::Master(_) => Error::NotSlave,
            Current::Slave(_) => Error::NotMaster,
            Current::Fenced(reason) => Error::Fenced {
                reason: reason.clone(),
            },
            Current::Switching => Error::Switching,
            Current::Failed(e) => Error::NodeFailed(e.clone()),
            Current::Stopped => Error::NodeFailed(STOPPED.into()),
        }
    }
}

/// The task forwarding a slave's states, and the generation it was started for: a forwarder
/// of an older generation never publishes (checked under this mutex).
#[derive(Default)]
struct Forward {
    generation: u64,
    task: Option<JoinHandle<()>>,
}

/// One database directory that runs as master or slave and changes role only through explicit
/// calls (DESIGN.md §13.2). Share it as `Arc<Node>`: every caller sees a role change on its
/// next call.
///
/// Sync calls never wait behind a role switch: while a switch holds or waits for the lock they
/// return `Error::Switching`. Async slave-admin calls (`repoint`, `clear_halt`, `verify_now`,
/// `wait_*`) hold the node across their await, and a switch waits for them. A stored master
/// opens **fenced** (no epoch, no port, no reads or writes) until a peer check clears it or
/// `NodeConfig::unchecked_master` is given.
///
/// # Read-modify-write
///
/// There is no `update`, compare-and-set or merge. **Between your `get` and your `put`, another
/// writer can change the key, and that change will be lost.** Hold your own lock around
/// get → compute → put (DESIGN.md §5.1), as with [`Master`].
pub struct Node {
    pub(super) cfg: NodeConfig,
    /// The slave role's upstream: `cfg.slave.upstream` until `repoint` or `rejoin_as_slave`
    /// replaces it.
    pub(super) upstream: Arc<Mutex<UpstreamConfig>>,
    pub(super) role: RwLock<Current>,
    state_tx: watch::Sender<NodeState>,
    forward: Arc<Mutex<Forward>>,
}

const _: () = {
    fn assert<T: Send + Sync + 'static>() {}
    let _ = assert::<Node>;
};

/// Opens the stored role of `cfg`'s directory: a slave as a slave (with `upstream`); a master
/// directly under `unchecked_master`, else after the fence check (`Fenced` when it does not
/// clear). `Err(Config)` without a stored role.
pub(super) async fn open_current(cfg: &NodeConfig, upstream: &UpstreamConfig) -> Result<Current> {
    let path = cfg.master.path.clone();
    let stored = blocking(move || stored_meta(&path)).await?;
    match stored.map(|m| m.role) {
        None => Err(Error::Config(format!(
            "{} holds no database; use Node::create_master or Node::create_slave",
            cfg.master.path.display()
        ))),
        Some(Role::Slave) => {
            let mut s = cfg.slave.clone();
            s.upstream = upstream.clone();
            Ok(Current::Slave(Slave::open(s).await?))
        }
        Some(Role::Master) if cfg.unchecked_master.is_some() => {
            tracing::warn!(node = %cfg.master.identifier, "opening a stored master without the fence check (AcceptSplitBrainRisk)");
            Ok(Current::Master(Master::open(cfg.master.clone()).await?))
        }
        Some(Role::Master) => {
            match fence::check(&cfg.master.path, &cfg.master.identifier, &cfg.peers, false).await? {
                None => Ok(Current::Master(Master::open(cfg.master.clone()).await?)),
                Some(reason) => Ok(Current::Fenced(reason)),
            }
        }
    }
}

impl Node {
    /// Creates a new master database. Only for a directory with no stored role (missing or
    /// empty); otherwise `Err(Config)`. No fence check.
    pub async fn create_master(cfg: NodeConfig) -> Result<Node> {
        cfg.validate()?;
        refuse_existing(&cfg).await?;
        let master = Master::open(cfg.master.clone()).await?;
        Ok(Node::with(cfg, Current::Master(master)))
    }

    /// Creates a new slave database that bootstraps from `cfg.slave.upstream`. Only for a
    /// directory with no stored role; otherwise `Err(Config)`.
    pub async fn create_slave(cfg: NodeConfig) -> Result<Node> {
        cfg.validate()?;
        refuse_existing(&cfg).await?;
        let slave = Slave::open(cfg.slave.clone()).await?;
        Ok(Node::with(cfg, Current::Slave(slave)))
    }

    /// Opens an existing database in its stored role. A stored slave opens as a slave. A
    /// stored master runs the fence check once (`cfg.peers`, DESIGN.md §13.2): when it clears,
    /// the master opens (starting its epoch); otherwise the node is returned **fenced**
    /// (`NodeState::Fenced { reason }`) and nothing re-checks automatically: call
    /// [`unfence`](Self::unfence) or [`rejoin_as_slave`](Self::rejoin_as_slave). With
    /// `cfg.unchecked_master` the master opens without the check. `Err(Config)` when the
    /// directory holds no database (use `create_*`).
    pub async fn open(cfg: NodeConfig) -> Result<Node> {
        cfg.validate()?;
        let current = open_current(&cfg, &cfg.slave.upstream).await?;
        Ok(Node::with(cfg, current))
    }

    /// Wraps a running master. `Err(Config)` (the master is shut down) if `cfg` is invalid or
    /// names another directory.
    pub async fn from_master(master: Master, cfg: NodeConfig) -> Result<Node> {
        let checked = cfg.validate().and_then(|()| same_dir(master.path(), &cfg));
        if let Err(e) = checked {
            let _ = master.shutdown().await;
            return Err(e);
        }
        Ok(Node::with(cfg, Current::Master(master)))
    }

    /// Wraps a running slave; later reopens of the slave role use `cfg.slave.upstream`.
    /// `Err(Config)` (the slave is shut down) if `cfg` is invalid or names another directory.
    pub async fn from_slave(slave: Slave, cfg: NodeConfig) -> Result<Node> {
        let checked = cfg.validate().and_then(|()| same_dir(slave.path(), &cfg));
        if let Err(e) = checked {
            let _ = slave.shutdown().await;
            return Err(e);
        }
        Ok(Node::with(cfg, Current::Slave(slave)))
    }

    fn with(cfg: NodeConfig, current: Current) -> Node {
        let node = Node {
            upstream: Arc::new(Mutex::new(cfg.slave.upstream.clone())),
            cfg,
            role: RwLock::new(Current::Switching),
            state_tx: watch::channel(NodeState::Switching).0,
            forward: Arc::default(),
        };
        node.publish(&current);
        *node.role.try_write().expect("not shared yet") = current;
        node
    }

    /// The exact current state.
    pub fn state(&self) -> NodeState {
        self.state_tx.borrow().clone()
    }

    /// Every state change, including the slave's own states (forwarded live).
    pub fn watch_state(&self) -> watch::Receiver<NodeState> {
        self.state_tx.subscribe()
    }

    /// Publishes the state of `cur`; for a slave, starts forwarding its states. Stops the
    /// previous forwarder first (under the same mutex, so none publishes after this).
    pub(super) fn publish(&self, cur: &Current) {
        let mut fwd = self.forward.lock().unwrap_or_else(PoisonError::into_inner);
        fwd.generation += 1;
        if let Some(task) = fwd.task.take() {
            task.abort();
        }
        let state = match cur {
            Current::Master(m) => NodeState::Master {
                identity: m.identity(),
            },
            Current::Slave(s) => {
                let mut rx = s.watch_state();
                let first = rx.borrow_and_update().clone();
                fwd.task = Some(forward_slave_states(
                    rx,
                    self.state_tx.clone(),
                    self.forward.clone(),
                    self.upstream.clone(),
                    fwd.generation,
                ));
                NodeState::Slave {
                    state: first,
                    upstream: self.upstream_uri(),
                }
            }
            Current::Fenced(reason) => NodeState::Fenced {
                reason: reason.clone(),
            },
            Current::Switching => NodeState::Switching,
            Current::Failed(error) => NodeState::Failed {
                error: error.clone(),
            },
            Current::Stopped => NodeState::Stopped,
        };
        self.state_tx.send_replace(state);
    }

    /// Re-sends the slave state (after the upstream changed).
    fn refresh_slave(&self, s: &Slave) {
        let _fwd = self.forward.lock().unwrap_or_else(PoisonError::into_inner);
        self.state_tx.send_replace(NodeState::Slave {
            state: s.status().state,
            upstream: self.upstream_uri(),
        });
    }

    fn upstream_uri(&self) -> String {
        self.upstream
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .uri()
    }

    /// `cfg.slave` with the current upstream.
    pub(super) fn slave_config(&self) -> SlaveConfig {
        let mut s = self.cfg.slave.clone();
        s.upstream = self
            .upstream
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .clone();
        s
    }

    fn current(&self) -> Result<RwLockReadGuard<'_, Current>> {
        self.role.try_read().map_err(|_| Error::Switching)
    }

    fn with_reader<T>(&self, f: impl FnOnce(&dyn ReadApi) -> Result<T>) -> Result<T> {
        let g = self.current()?;
        match &*g {
            Current::Master(m) => f(m),
            Current::Slave(s) => f(s),
            other => Err(other.unavailable()),
        }
    }

    fn with_master<T>(&self, f: impl FnOnce(&Master) -> Result<T>) -> Result<T> {
        let g = self.current()?;
        match &*g {
            Current::Master(m) => f(m),
            other => Err(other.unavailable()),
        }
    }

    fn with_slave<T>(&self, f: impl FnOnce(&Slave) -> Result<T>) -> Result<T> {
        let g = self.current()?;
        match &*g {
            Current::Slave(s) => f(s),
            other => Err(other.unavailable()),
        }
    }

    /// For async slave-admin calls: waits behind a running switch, then holds the node.
    async fn slave(&self) -> Result<RwLockReadGuard<'_, Slave>> {
        let g = self.role.read().await;
        RwLockReadGuard::try_map(g, |c| match c {
            Current::Slave(s) => Some(s),
            _ => None,
        })
        .map_err(|g| g.unavailable())
    }

    /// The replication address of the running role; `None` when fenced, failed, switching or
    /// not serving.
    pub fn serve_addr(&self) -> Option<SocketAddr> {
        match &*self.current().ok()? {
            Current::Master(m) => m.serve_addr(),
            Current::Slave(s) => s.serve_addr(),
            _ => None,
        }
    }

    /// The last committed (master) or applied (slave) seq.
    pub fn last_seq(&self) -> Result<Seq> {
        let g = self.current()?;
        match &*g {
            Current::Master(m) => Ok(m.last_seq()),
            Current::Slave(s) => Ok(s.last_seq()),
            other => Err(other.unavailable()),
        }
    }

    // ---- reads: master or slave ----

    /// Reads the current value of `key`, or `None` if absent or expired. The value can change
    /// as soon as this returns: to update a key from its current value, hold an application
    /// lock around get → compute → put (see the [type-level docs](Self#read-modify-write)).
    pub fn get(&self, cf: &str, key: &[u8]) -> Result<Option<Vec<u8>>> {
        self.with_reader(|r| r.get(cf, key))
    }

    pub fn get_with_meta(&self, cf: &str, key: &[u8]) -> Result<Option<(Vec<u8>, ValueMeta)>> {
        self.with_reader(|r| r.get_with_meta(cf, key))
    }

    pub fn multi_get(&self, cf: &str, keys: &[&[u8]]) -> Result<Vec<Option<Vec<u8>>>> {
        self.with_reader(|r| r.multi_get(cf, keys))
    }

    pub fn list_cfs(&self) -> Result<Vec<CfSpec>> {
        self.with_reader(|r| Ok(r.list_cfs()))
    }

    /// A consistent view; iterate through it (`ReadApi::iter`). Drop it before a role switch:
    /// a live snapshot keeps the database open and makes the switch fail.
    pub fn snapshot(&self) -> Result<ReadSnapshot> {
        let g = self.current()?;
        match &*g {
            Current::Master(m) => m.snapshot(),
            Current::Slave(s) => s.snapshot(),
            other => Err(other.unavailable()),
        }
    }

    // ---- writes: master only (a slave answers `NotMaster`) ----

    /// [`Master::put`]. Not safe to compute `value` from a previous `get` without an
    /// application lock around both calls (see the [type-level docs](Self#read-modify-write)).
    /// Blocks until fsynced; async callers use `spawn_blocking`.
    pub fn put(&self, cf: &str, key: &[u8], value: &[u8]) -> Result<Seq> {
        self.with_master(|m| m.put(cf, key, value))
    }

    /// [`Master::put_with_ttl`].
    pub fn put_with_ttl(&self, cf: &str, key: &[u8], value: &[u8], ttl: Duration) -> Result<Seq> {
        self.with_master(|m| m.put_with_ttl(cf, key, value, ttl))
    }

    /// [`Master::put_with`].
    pub fn put_with(&self, cf: &str, key: &[u8], value: &[u8], concern: WriteConcern) -> Result<Seq> {
        self.with_master(|m| m.put_with(cf, key, value, concern))
    }

    /// [`Master::delete`].
    pub fn delete(&self, cf: &str, key: &[u8]) -> Result<Seq> {
        self.with_master(|m| m.delete(cf, key))
    }

    /// [`Master::delete_range`].
    pub fn delete_range(&self, cf: &str, start: &[u8], end: &[u8]) -> Result<Seq> {
        self.with_master(|m| m.delete_range(cf, start, end))
    }

    /// [`Master::write`]: one atomic entry. A batch is not a transaction over reads; values
    /// computed from earlier reads can be stale unless the application holds its own lock (see
    /// the [type-level docs](Self#read-modify-write)).
    pub fn write(&self, batch: Batch) -> Result<Seq> {
        self.with_master(|m| m.write(batch))
    }

    /// [`Master::write_with`].
    pub fn write_with(&self, batch: Batch, concern: WriteConcern) -> Result<Seq> {
        self.with_master(|m| m.write_with(batch, concern))
    }

    /// [`Master::create_cf`].
    pub fn create_cf(&self, spec: CfSpec) -> Result<Seq> {
        self.with_master(|m| m.create_cf(spec))
    }

    /// [`Master::drop_cf`].
    pub fn drop_cf(&self, name: &str) -> Result<Seq> {
        self.with_master(|m| m.drop_cf(name))
    }

    /// [`Master::reap_expired`].
    pub fn reap_expired(&self) -> Result<ReapStats> {
        self.with_master(|m| m.reap_expired())
    }

    /// [`Master::bump_epoch`]; the new identity is published in `NodeState::Master`.
    pub fn bump_epoch(&self) -> Result<DbIdentity> {
        self.with_master(|m| {
            let id = m.bump_epoch()?;
            self.state_tx
                .send_replace(NodeState::Master { identity: id });
            Ok(id)
        })
    }

    // ---- slave admin: slave only (a master answers `NotSlave`) ----

    /// [`Slave::repoint`]. The new upstream is also the one later reopens of the slave role
    /// use (unless the address is refused with `Err(Config)`).
    pub async fn repoint(&self, upstream: UpstreamConfig, opts: RepointOptions) -> Result<RepointOutcome> {
        let s = self.slave().await?;
        let result = s.repoint(upstream.clone(), opts).await;
        if !matches!(result, Err(Error::Config(_))) {
            *self.upstream.lock().unwrap_or_else(PoisonError::into_inner) = upstream;
            self.refresh_slave(&s);
        }
        result
    }

    /// [`Slave::clear_halt`].
    pub async fn clear_halt(&self, action: HaltAction) -> Result<()> {
        self.slave().await?.clear_halt(action).await
    }

    /// [`Slave::verify_now`].
    pub async fn verify_now(&self, mode: VerifyMode) -> Result<VerifyReport> {
        self.slave().await?.verify_now(mode).await
    }

    /// [`Slave::wait_streaming`]. A switch waits for it.
    pub async fn wait_streaming(&self, timeout: Duration) -> Result<()> {
        self.slave().await?.wait_streaming(timeout).await
    }

    /// [`Slave::wait_for_seq`]. A switch waits for it.
    pub async fn wait_for_seq(&self, seq: Seq, timeout: Duration) -> Result<()> {
        self.slave().await?.wait_for_seq(seq, timeout).await
    }

    /// [`Slave::diverged_entries`].
    pub fn diverged_entries(&self) -> Result<Vec<LogEntry>> {
        self.with_slave(|s| s.diverged_entries())
    }

    /// [`Slave::status`].
    pub fn status(&self) -> Result<SlaveStatus> {
        self.with_slave(|s| Ok(s.status()))
    }
}

async fn refuse_existing(cfg: &NodeConfig) -> Result<()> {
    let path = cfg.master.path.clone();
    if blocking(move || stored_meta(&path)).await?.is_some() {
        return Err(Error::Config(format!(
            "{} already holds a database; use Node::open",
            cfg.master.path.display()
        )));
    }
    Ok(())
}

fn same_dir(running: &std::path::Path, cfg: &NodeConfig) -> Result<()> {
    if running == cfg.master.path {
        Ok(())
    } else {
        Err(Error::Config(
            "the handle runs on another directory than NodeConfig names".into(),
        ))
    }
}

fn forward_slave_states(
    mut rx: watch::Receiver<SlaveState>,
    tx: watch::Sender<NodeState>,
    forward: Arc<Mutex<Forward>>,
    upstream: Arc<Mutex<UpstreamConfig>>,
    generation: u64,
) -> JoinHandle<()> {
    tokio::spawn(async move {
        while rx.changed().await.is_ok() {
            let state = rx.borrow_and_update().clone();
            let fwd = forward.lock().unwrap_or_else(PoisonError::into_inner);
            if fwd.generation != generation {
                return;
            }
            let upstream = upstream
                .lock()
                .unwrap_or_else(PoisonError::into_inner)
                .uri();
            tx.send_replace(NodeState::Slave { state, upstream });
        }
    })
}
```

(Lock order everywhere: `forward` before `upstream`; `repoint` releases `upstream` before `refresh_slave`.)

`src/node/mod.rs` becomes:

```rust
//! A handle that holds either role and switches it explicitly (DESIGN.md §13.2).

mod config;
pub(crate) mod fence;
mod handle;
mod types;

pub use config::NodeConfig;
pub use handle::Node;
pub use types::{AcceptSplitBrainRisk, FenceReason, NodeState, UnfenceOptions};
```

In `src/lib.rs` extend the export: `pub use node::{AcceptSplitBrainRisk, FenceReason, Node, NodeConfig, NodeState, UnfenceOptions};`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CARGO_BUILD_JOBS=4 cargo test --test node`
Expected: 5 passed, 1 ignored (the repoint/reopen test).
Run: `CARGO_BUILD_JOBS=4 cargo clippy --all-targets --all-features -- -D warnings`
Expected: clean.

- [ ] **Step 5: Commit**

```bash
git add src/node src/lib.rs src/master/mod.rs src/slave/mod.rs tests/node.rs
git commit -m "feat: Node handle with fenced open, reads, writes, slave admin and live state

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 6: Role switches on `Node`

**Files:**
- Create: `src/node/switch.rs`
- Modify: `src/node/mod.rs` (`mod switch;`), `src/slave/promote.rs` (add `promote_keeping`), `src/master/mod.rs` (add `shutdown_released`), `tests/node.rs` (remove the `#[ignore]`, add tests), `tests/failpoints.rs` (two tests)

**Interfaces:**
- Consumes: Task 5 (`Node { cfg, upstream, role }`, `Current`, `Current::unavailable`, `Node::publish`, `Node::slave_config`, `open_current(&NodeConfig, &UpstreamConfig)`, `STOPPED`); Task 4 (`fence::check(path, node_id, peers, allow_unreachable)`); Task 3 (`Slave::rejoin`); `Slave::{promote_checked, promote_refusal (private), promote_now (private)}`, `PromoteRefused { error, slave }`, `Master::demote(cfg) -> Result<Slave, DemoteRefused>`, `DemoteRefused { error, master }`; `offline::stored_meta`; test helpers of `tests/node.rs` (`up`, `dead_addr`, `node_cfg`, `wait_node`, `streaming`, `is_master`).
- Produces:
  - `pub(crate) async fn Slave::promote_keeping(self, cfg: MasterConfig) -> Result<Master, PromoteRefused>`
  - `pub(crate) async fn Master::shutdown_released(self) -> Result<()>`
  - `Node`: `pub async fn promote(&self) -> Result<()>`, `pub async fn promote_checked(&self) -> Result<()>`, `pub async fn demote(&self) -> Result<()>`, `pub async fn rejoin_as_slave(&self, upstream: Option<UpstreamConfig>, opts: RejoinOptions) -> Result<RepointOutcome>`, `pub async fn unfence(&self, opts: UnfenceOptions) -> Result<()>`, `pub async fn reopen(&self) -> Result<()>`, `pub async fn shutdown(&self) -> Result<()>`

- [ ] **Step 1: Write the failing tests**

In `tests/node.rs`: remove `#[ignore]` from `watch_state_forwards_slave_states_and_repoint_updates_the_upstream`, extend the `use` list with `std::sync::Arc`, `std::sync::atomic::{AtomicBool, Ordering}`, `std::time::Duration`, `AcceptDataLoss, RejoinOptions, RepointDivergence, UnfenceOptions, HaltReason`, and append:

```rust
/// Writers hammer `put` while the node is promoted: each put succeeds as master or fails with
/// `NotMaster`/`Switching`, nothing else; after the promotion, writes succeed.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn promotion_under_concurrent_writers() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Arc::new(Node::create_slave(node_cfg(&c, "b", a_addr, &[])).await.unwrap());
    wait_node(&b, "b streaming", streaming).await;
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    a.shutdown().await.unwrap();

    let stop = Arc::new(AtomicBool::new(false));
    let writers: Vec<_> = (0..4)
        .map(|w| {
            let (b, stop) = (b.clone(), stop.clone());
            tokio::task::spawn_blocking(move || {
                let (mut ok, mut refused, mut i) = (0u64, 0u64, 0u64);
                while !stop.load(Ordering::SeqCst) {
                    match b.put("c", format!("{w}-{i}").as_bytes(), b"v") {
                        Ok(_) => ok += 1,
                        Err(Error::NotMaster | Error::Switching) => {
                            refused += 1;
                            std::thread::sleep(Duration::from_millis(1));
                        }
                        Err(e) => panic!("writer {w}: unexpected error {e}"),
                    }
                    i += 1;
                }
                (ok, refused)
            })
        })
        .collect();
    tokio::time::sleep(Duration::from_millis(50)).await;
    b.promote().await.unwrap();
    assert!(is_master(&b.state()));
    tokio::time::sleep(Duration::from_millis(200)).await;
    stop.store(true, Ordering::SeqCst);
    let (mut ok, mut refused) = (0, 0);
    for w in writers {
        let (o, r) = w.await.unwrap();
        ok += o;
        refused += r;
    }
    assert!(refused > 0, "writes before the promotion were refused");
    assert!(ok > 0, "writes after the promotion succeeded");
    b.put("c", b"after", b"1").unwrap();
}

/// A refused checked promotion (a master is reachable) leaves the slave running and following.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_promotion_keeps_the_slave_running() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[a_addr])).await.unwrap();
    wait_node(&b, "b streaming", streaming).await;
    assert!(matches!(b.promote_checked().await, Err(Error::MasterReachable { .. })));
    assert!(streaming(&b.state()), "still a streaming slave");
    let seq = a.put("c", b"k", b"v").unwrap();
    b.wait_for_seq(seq, WAIT).await.unwrap();
    assert!(matches!(a.promote().await, Err(Error::NotSlave)));
    // Plain promote refuses a slave without data and keeps it too.
    let e = Node::create_slave(node_cfg(&c, "e", dead_addr(), &[])).await.unwrap();
    assert!(matches!(e.promote().await, Err(Error::Config(_))));
    assert!(matches!(e.state(), NodeState::Slave { .. }));
}

/// Demote undoes an unused promotion; once written, it is refused and the master keeps running.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn demote_through_the_node() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[])).await.unwrap();
    wait_node(&b, "b streaming", streaming).await;
    b.promote().await.unwrap();
    b.demote().await.unwrap();
    assert!(matches!(b.state(), NodeState::Slave { .. }));
    b.promote().await.unwrap();
    b.put("c", b"k", b"v").unwrap();
    assert!(matches!(b.demote().await, Err(Error::Config(_))));
    assert!(is_master(&b.state()));
    b.put("c", b"k2", b"v").expect("still the master");
}

/// The full runbook through `Node`: A dies, B is promoted, A restarts fenced by B and rejoins.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn the_old_master_rejoins_through_the_node() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[a_addr])).await.unwrap();
    a.put("c", b"k", b"1").unwrap();
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    a.shutdown().await.unwrap();
    b.promote_checked().await.unwrap();
    b.put("c", b"on b", b"2").unwrap();
    let b_addr = b.serve_addr().unwrap();

    let a = Node::open(node_cfg(&c, "a", b_addr, &[b_addr])).await.unwrap();
    assert_eq!(
        a.state(),
        NodeState::Fenced { reason: FenceReason::PeerIsMaster { peer: "b".into() } }
    );
    assert_eq!(
        a.rejoin_as_slave(None, RejoinOptions::default()).await.unwrap(),
        RepointOutcome::EpochAdvanced
    );
    a.wait_for_seq(b.last_seq().unwrap(), WAIT).await.unwrap();
    assert_eq!(a.get("c", b"on b").unwrap(), Some(b"2".to_vec()));
    assert!(matches!(a.rejoin_as_slave(None, RejoinOptions::default()).await, Err(Error::NotMaster)));
}

/// A diverged rejoin through the node halts, keeps the suffix readable, and repairs only with
/// `AcceptDataLoss`.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_diverged_rejoin_halts_then_repairs_with_consent() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[a_addr])).await.unwrap();
    a.put("c", b"k", b"1").unwrap();
    b.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    b.shutdown().await.unwrap();
    a.put("c", b"only on a", b"x").unwrap();
    a.shutdown().await.unwrap();
    b.reopen().await.unwrap();
    b.promote_checked().await.unwrap();
    let b_addr = b.serve_addr().unwrap();

    let a = Node::open(node_cfg(&c, "a", b_addr, &[b_addr])).await.unwrap();
    assert!(matches!(
        a.rejoin_as_slave(None, RejoinOptions::default()).await,
        Err(Error::Diverged { divergent_entries: 1, .. })
    ));
    assert!(matches!(
        a.state(),
        NodeState::Slave { state: SlaveState::Halted(HaltReason::Diverged), .. }
    ));
    assert_eq!(a.diverged_entries().unwrap().len(), 1);
    assert_eq!(a.get("c", b"only on a").unwrap(), Some(b"x".to_vec()));
    let repair = RepointOptions {
        on_divergence: RepointDivergence::Repair { accept_loss: AcceptDataLoss },
    };
    assert!(matches!(
        a.repoint(up(b_addr), repair).await.unwrap(),
        RepointOutcome::Repaired { .. }
    ));
    a.wait_for_seq(b.last_seq().unwrap(), WAIT).await.unwrap();
    assert_eq!(a.get("c", b"only on a").unwrap(), None);
}

/// Unreachable peers keep the node fenced unless the operator accepts the risk; peers that are
/// slaves behind this node clear it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn unfence_clears_with_behind_slaves_or_explicit_consent() {
    let c = Cluster::new();
    let a_cfg = node_cfg(&c, "a", dead_addr(), &[]);
    let a = Node::create_master(a_cfg.clone()).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let (s, _) = c.slave("s", a_addr).await;
    a.put("c", b"k", b"1").unwrap();
    s.wait_for_seq(a.last_seq().unwrap(), WAIT).await.unwrap();
    a.shutdown().await.unwrap();
    let s_addr = s.serve_addr().unwrap();
    let dead = dead_addr();

    let mut cfg = a_cfg.clone();
    cfg.peers = vec![up(s_addr), up(dead)];
    let a = Node::open(cfg).await.unwrap();
    assert_eq!(
        a.state(),
        NodeState::Fenced { reason: FenceReason::PeerUnreachable { peer: format!("http://{dead}") } }
    );
    assert!(matches!(a.unfence(UnfenceOptions::default()).await, Err(Error::Fenced { .. })));
    a.unfence(UnfenceOptions { allow_unreachable: Some(AcceptSplitBrainRisk) })
        .await
        .unwrap();
    assert!(is_master(&a.state()));
    assert!(matches!(a.unfence(UnfenceOptions::default()).await, Err(Error::Config(_))));
    a.shutdown().await.unwrap();

    // Only the behind slave as peer: the open itself clears.
    let mut cfg = a_cfg;
    cfg.peers = vec![up(s_addr)];
    let a = Node::open(cfg).await.unwrap();
    assert!(is_master(&a.state()), "{:?}", a.state());
}

/// Review focus: a live `ReadSnapshot` makes the promotion lose the handle; the node is
/// `Failed`, and after dropping the snapshot `reopen` brings back the (unchanged) slave.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_lost_handle_fails_the_node_and_reopen_recovers() {
    let c = Cluster::new();
    let a = Node::create_master(node_cfg(&c, "a", dead_addr(), &[])).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let a_addr = a.serve_addr().unwrap();
    let b = Node::create_slave(node_cfg(&c, "b", a_addr, &[])).await.unwrap();
    wait_node(&b, "b streaming", streaming).await;
    let snap = b.snapshot().unwrap();
    let err = b.promote().await.unwrap_err();
    assert!(err.to_string().contains("ReadSnapshot"), "{err}");
    assert!(matches!(b.state(), NodeState::Failed { .. }));
    assert!(matches!(b.get("c", b"k"), Err(Error::NodeFailed(_))));
    drop(snap);
    b.reopen().await.unwrap();
    wait_node(&b, "b streaming again", streaming).await;
}
```

Append to `tests/failpoints.rs` (add `Node, NodeConfig, NodeState, SlaveState` to the imports; define the helper locally because each test binary has its own helpers):

```rust
fn node_cfg(c: &Cluster, name: &str, upstream: std::net::SocketAddr) -> replicated_rocksdb::NodeConfig {
    let master = c.master_config(name);
    let (mut slave, _) = c.slave_config(name, upstream);
    slave.serve = master.serve.clone();
    replicated_rocksdb::NodeConfig::new(master, slave)
}

/// `watch_state` shows Slave → Switching → Master, and calls fail fast with `Switching`
/// while the switch runs.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn watch_state_sees_the_switch() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = Master::open(c.master_config("a")).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let b = std::sync::Arc::new(
        replicated_rocksdb::Node::create_slave(node_cfg(&c, "b", a.serve_addr().unwrap()))
            .await
            .unwrap(),
    );
    wait_until("b streaming", || {
        matches!(b.state(), replicated_rocksdb::NodeState::Slave { state: SlaveState::Streaming, .. })
    })
    .await;
    a.shutdown().await.unwrap();
    let mut rx = b.watch_state();
    let seen = tokio::spawn(async move {
        let mut seen = Vec::new();
        while rx.changed().await.is_ok() {
            let s = rx.borrow_and_update().clone();
            let done = matches!(s, replicated_rocksdb::NodeState::Master { .. });
            seen.push(s);
            if done {
                break;
            }
        }
        seen
    });
    fail::cfg("slave-promote-before-shutdown", "pause").unwrap();
    let promoting = tokio::spawn({
        let b = b.clone();
        async move { b.promote().await }
    });
    wait_until("switching", || b.state() == replicated_rocksdb::NodeState::Switching).await;
    assert!(matches!(b.put("c", b"k", b"v"), Err(Error::Switching)));
    assert!(matches!(b.get("c", b"k"), Err(Error::Switching)));
    fail::remove("slave-promote-before-shutdown");
    promoting.await.unwrap().unwrap();
    let seen = seen.await.unwrap();
    let switching = seen
        .iter()
        .position(|s| *s == replicated_rocksdb::NodeState::Switching)
        .expect("Switching seen");
    assert!(matches!(seen.last(), Some(replicated_rocksdb::NodeState::Master { .. })));
    assert!(switching < seen.len() - 1);
    assert!(!seen.iter().any(|s| matches!(s, replicated_rocksdb::NodeState::Failed { .. })));
    scenario.teardown();
}

/// Review focus: a switch future dropped mid-way leaves the node `Failed`, never `Switching`,
/// and `reopen` restores it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_cancelled_switch_leaves_the_node_failed_and_reopenable() {
    let (_serial, scenario) = serial().await;
    let c = Cluster::new();
    let a = Master::open(c.master_config("a")).await.unwrap();
    a.create_cf(CfSpec::new("c")).unwrap();
    let b = replicated_rocksdb::Node::create_slave(node_cfg(&c, "b", a.serve_addr().unwrap()))
        .await
        .unwrap();
    wait_until("b streaming", || {
        matches!(b.state(), replicated_rocksdb::NodeState::Slave { state: SlaveState::Streaming, .. })
    })
    .await;
    fail::cfg("slave-promote-before-shutdown", "pause").unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), b.promote())
            .await
            .is_err(),
        "the promotion is held at the pause point"
    );
    assert!(matches!(b.state(), replicated_rocksdb::NodeState::Failed { .. }));
    fail::remove("slave-promote-before-shutdown");
    // The dropped slave winds down asynchronously; its LOCK is released shortly.
    let deadline = tokio::time::Instant::now() + support::WAIT;
    loop {
        match b.reopen().await {
            Ok(()) => break,
            Err(e) if tokio::time::Instant::now() < deadline => {
                tracing::debug!(error = %e, "reopen not yet possible");
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(e) => panic!("reopen failed: {e}"),
        }
    }
    assert!(matches!(b.state(), replicated_rocksdb::NodeState::Slave { .. }));
    scenario.teardown();
}
```

(If `tracing` is not a dev-dependency usable from tests, replace the `tracing::debug!` line with `let _ = e;`.)

- [ ] **Step 2: Run the tests to verify they fail**

Run: `CARGO_BUILD_JOBS=4 cargo test --test node`
Expected: compile error (`promote`, `demote`, `rejoin_as_slave`, `unfence`, `reopen`, `shutdown` not found on `Node`).

- [ ] **Step 3: Implement**

`src/slave/promote.rs`, inside the second `impl Slave` block (after `promote_checked`):

```rust
    /// `promote` that hands the slave back, still running, on every refusal before it changes
    /// anything (the checks of `promote_refusal`), like `promote_checked` without the peer
    /// check. Used by `Node::promote`, which must never shut a refused slave down.
    pub(crate) async fn promote_keeping(self, cfg: MasterConfig) -> Result<Master, PromoteRefused> {
        if let Some(msg) = self.promote_refusal(&cfg) {
            return Err(PromoteRefused {
                error: Error::Config(msg),
                slave: Some(Box::new(self)),
            });
        }
        self.promote_now(cfg, 0, true).await
    }
```

`src/master/mod.rs`, inside `impl Master`:

```rust
    /// `shutdown`, then `Err(Config)` if a `ReadSnapshot` still holds the database (a role
    /// switch could not reopen the directory). Used by `Node::rejoin_as_slave`.
    pub(crate) async fn shutdown_released(self) -> Result<()> {
        let db = Arc::downgrade(self.inner.store.db());
        self.shutdown().await?;
        if db.strong_count() > 0 {
            return Err(crate::Error::Config(
                "a ReadSnapshot of this master is still open and keeps the database open; drop it, then call Node::reopen".into(),
            ));
        }
        Ok(())
    }
```

`src/node/switch.rs`:

```rust
//! Role switches of a `Node` (DESIGN.md §13.2). Each takes the write lock, moves the running
//! handle out (the role reads `Switching`), runs the consuming core operation, and stores what
//! it returned: the new role, the handed-back old one on a refusal, or `Failed` when the
//! handle was lost. A switch future dropped before it finishes leaves `Failed` (never
//! `Switching`).

use tokio::sync::RwLockWriteGuard;

use super::fence;
use super::handle::{Current, Node, open_current};
use super::types::UnfenceOptions;
use crate::background::blocking;
use crate::error::{Error, Result};
use crate::master::{DemoteRefused, Master};
use crate::slave::{PromoteRefused, RejoinOptions, RepointOutcome, Slave, UpstreamConfig};
use crate::store::offline::stored_meta;
use crate::types::Role;

const CANCELLED: &str = "a role switch was cancelled before it finished; call Node::reopen";

struct Switch<'a> {
    node: &'a Node,
    guard: RwLockWriteGuard<'a, Current>,
    done: bool,
}

impl Switch<'_> {
    fn finish<T>(mut self, new: Current, result: Result<T>) -> Result<T> {
        *self.guard = new;
        self.node.publish(&self.guard);
        self.done = true;
        result
    }
}

impl Drop for Switch<'_> {
    fn drop(&mut self) {
        if !self.done {
            tracing::error!(node = %self.node.cfg.master.identifier, "{CANCELLED}");
            *self.guard = Current::Failed(CANCELLED.into());
            self.node.publish(&self.guard);
        }
    }
}

fn slave_only(c: &Current) -> Result<()> {
    match c {
        Current::Slave(_) => Ok(()),
        other => Err(other.unavailable()),
    }
}

fn master_only(c: &Current) -> Result<()> {
    match c {
        Current::Master(_) => Ok(()),
        // `unavailable` says `NotSlave` for a master; for a slave it says `NotMaster`.
        other => Err(other.unavailable()),
    }
}

impl Node {
    /// Takes the write lock (waiting for running slave-admin calls), checks the role, and
    /// moves it out, publishing `Switching`. A refused check changes nothing.
    async fn begin_switch(
        &self,
        check: impl FnOnce(&Current) -> Result<()>,
    ) -> Result<(Switch<'_>, Current)> {
        let mut guard = self.role.write().await;
        check(&guard)?;
        let old = std::mem::replace(&mut *guard, Current::Switching);
        self.publish(&guard);
        Ok((
            Switch {
                node: self,
                guard,
                done: false,
            },
            old,
        ))
    }

    /// [`Slave::promote_checked`] with `cfg.peers` and `cfg.master`. **Refused by default**
    /// (a later peer, a reachable master, a peer that would lose entries, ...): the slave keeps
    /// running. Only a failure after the slave began shutting down (an I/O error, a live
    /// `ReadSnapshot` from [`snapshot`](Self::snapshot)) leaves the node `Failed`.
    pub async fn promote_checked(&self) -> Result<()> {
        let (sw, old) = self.begin_switch(slave_only).await?;
        let slave = match old {
            Current::Slave(s) => s,
            other => return sw.finish(other, Err(Error::NotSlave)),
        };
        let result = slave
            .promote_checked(self.cfg.master.clone(), &self.cfg.peers)
            .await;
        self.after_promotion(sw, result)
    }

    /// Promotes without asking any peer, like [`Slave::promote`]: **the operator must stop or
    /// fence the old master first**. A refusal (bootstrapping, no data, a repair replaying,
    /// halted `Corrupt`/`SpecMismatch`) keeps the slave running.
    pub async fn promote(&self) -> Result<()> {
        let (sw, old) = self.begin_switch(slave_only).await?;
        let slave = match old {
            Current::Slave(s) => s,
            other => return sw.finish(other, Err(Error::NotSlave)),
        };
        let result = slave.promote_keeping(self.cfg.master.clone()).await;
        self.after_promotion(sw, result)
    }

    fn after_promotion(
        &self,
        sw: Switch<'_>,
        result: std::result::Result<Master, PromoteRefused>,
    ) -> Result<()> {
        match result {
            Ok(master) => sw.finish(Current::Master(master), Ok(())),
            Err(PromoteRefused { error, slave: Some(slave) }) => {
                sw.finish(Current::Slave(*slave), Err(error))
            }
            Err(PromoteRefused { error, slave: None }) => {
                sw.finish(Current::Failed(error.to_string()), Err(error))
            }
        }
    }

    /// [`Master::demote`] with the node's slave config: only undoes a promotion that has taken
    /// no writes. A refusal keeps the master running.
    pub async fn demote(&self) -> Result<()> {
        let (sw, old) = self.begin_switch(master_only).await?;
        let master = match old {
            Current::Master(m) => m,
            other => return sw.finish(other, Err(Error::NotMaster)),
        };
        match master.demote(self.slave_config()).await {
            Ok(slave) => sw.finish(Current::Slave(slave), Ok(())),
            Err(DemoteRefused { error, master: Some(m) }) => {
                sw.finish(Current::Master(*m), Err(error))
            }
            Err(DemoteRefused { error, master: None }) => {
                sw.finish(Current::Failed(error.to_string()), Err(error))
            }
        }
    }

    /// Turns this master (running or fenced) into a slave of `upstream` (`None`: the node's
    /// slave upstream) with [`Slave::rejoin`]: no wipe; `Ok(EpochAdvanced | Resumed | ...)`
    /// when its log is a prefix of the upstream's, `Err(Diverged)` under the default
    /// `RepointDivergence::Refuse` with the node then a slave halted `Diverged` (data intact,
    /// [`diverged_entries`](Self::diverged_entries); resolve with
    /// [`repoint`](Self::repoint) and `AcceptDataLoss`). A running master is shut down first.
    /// A refusal before the rewrite keeps a fenced node fenced; a master that was shut down,
    /// or any failure after the rewrite, leaves the node `Failed` (`reopen` opens the stored
    /// role again).
    pub async fn rejoin_as_slave(
        &self,
        upstream: Option<UpstreamConfig>,
        opts: RejoinOptions,
    ) -> Result<RepointOutcome> {
        let mut cfg = self.slave_config();
        if let Some(u) = &upstream {
            cfg.upstream = u.clone();
        }
        cfg.validate()?;
        let (sw, old) = self
            .begin_switch(|c| match c {
                Current::Master(_) | Current::Fenced(_) => Ok(()),
                other => Err(other.unavailable()),
            })
            .await?;
        let fenced = match old {
            Current::Master(master) => {
                if let Err(e) = master.shutdown_released().await {
                    return sw.finish(Current::Failed(e.to_string()), Err(e));
                }
                None
            }
            Current::Fenced(reason) => Some(reason),
            other => return sw.finish(other, Err(Error::NotMaster)),
        };
        match Slave::rejoin(cfg, opts).await {
            Ok((slave, outcome)) => {
                if let Some(u) = upstream {
                    *self
                        .upstream
                        .lock()
                        .unwrap_or_else(std::sync::PoisonError::into_inner) = u;
                }
                sw.finish(Current::Slave(slave), outcome)
            }
            Err(e) => {
                let path = self.cfg.master.path.clone();
                let still_master = blocking(move || stored_meta(&path))
                    .await
                    .ok()
                    .flatten()
                    .is_some_and(|m| m.role == Role::Master);
                match (fenced, still_master) {
                    (Some(reason), true) => sw.finish(Current::Fenced(reason), Err(e)),
                    _ => sw.finish(Current::Failed(e.to_string()), Err(e)),
                }
            }
        }
    }

    /// Runs the fence check again (DESIGN.md §13.2). When it clears, the master opens (its
    /// epoch starts) and this returns `Ok`. Otherwise `Err(Fenced { reason })` and the node
    /// stays fenced with that reason. `opts.allow_unreachable` (explicit
    /// `AcceptSplitBrainRisk`) skips peers that do not answer. `Err(Config)` if the node is
    /// not fenced. Calls answer `Switching` while the check runs.
    pub async fn unfence(&self, opts: UnfenceOptions) -> Result<()> {
        let not_fenced = || Error::Config("the node is not fenced".into());
        let (sw, old) = self
            .begin_switch(|c| match c {
                Current::Fenced(_) => Ok(()),
                _ => Err(not_fenced()),
            })
            .await?;
        let reason = match old {
            Current::Fenced(reason) => reason,
            other => return sw.finish(other, Err(not_fenced())),
        };
        let c = &self.cfg;
        let checked = fence::check(
            &c.master.path,
            &c.master.identifier,
            &c.peers,
            opts.allow_unreachable.is_some(),
        )
        .await;
        match checked {
            Err(e) => sw.finish(Current::Fenced(reason), Err(e)),
            Ok(Some(reason)) => sw.finish(
                Current::Fenced(reason.clone()),
                Err(Error::Fenced { reason }),
            ),
            Ok(None) => match Master::open(c.master.clone()).await {
                Ok(master) => sw.finish(Current::Master(master), Ok(())),
                Err(e) => sw.finish(Current::Failed(e.to_string()), Err(e)),
            },
        }
    }

    /// Opens the stored role again after `Failed` or `shutdown`, exactly like [`Node::open`]
    /// (a stored master is fenced-checked). `Ok` does not say which role: see
    /// [`state`](Self::state). On `Err` the node stays as it was. `Err(Config)` in any other
    /// state.
    pub async fn reopen(&self) -> Result<()> {
        let (sw, old) = self
            .begin_switch(|c| match c {
                Current::Failed(_) | Current::Stopped => Ok(()),
                _ => Err(Error::Config(
                    "only a failed or shut-down node can be reopened".into(),
                )),
            })
            .await?;
        let upstream = self.slave_config().upstream;
        match open_current(&self.cfg, &upstream).await {
            Ok(current) => sw.finish(current, Ok(())),
            Err(e) => sw.finish(old, Err(e)),
        }
    }

    /// Shuts the running role down (`Master::shutdown` / `Slave::shutdown`); the node is then
    /// `Stopped` (also when the shutdown reports an error, which is returned).
    pub async fn shutdown(&self) -> Result<()> {
        let (sw, old) = self.begin_switch(|_| Ok(())).await?;
        let result = match old {
            Current::Master(m) => m.shutdown().await,
            Current::Slave(s) => s.shutdown().await,
            _ => Ok(()),
        };
        sw.finish(Current::Stopped, result)
    }
}
```

Add `mod switch;` to `src/node/mod.rs`. `Current`, `open_current` and the `Node` fields used here are `pub(super)` from Task 5. `crate::master::DemoteRefused` is `pub` in `src/master/mod.rs`; `crate::slave::{PromoteRefused, RejoinOptions}` are re-exported by the slave module.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `CARGO_BUILD_JOBS=4 cargo test --test node`
Expected: 13 passed, 0 ignored.
Run: `CARGO_BUILD_JOBS=4 cargo test --features failpoints --test failpoints -- watch_state_sees_the_switch a_cancelled_switch`
Expected: 2 passed.
Run: `CARGO_BUILD_JOBS=4 cargo test --test promotion`
Expected: all pass (`promote_keeping` and `shutdown_released` are additions).

- [ ] **Step 5: Commit**

```bash
git add src/node src/slave/promote.rs src/master/mod.rs tests/node.rs tests/failpoints.rs
git commit -m "feat: Node role switches: promote, demote, rejoin_as_slave, unfence, reopen, shutdown

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

### Task 7: Docs, invariant and example

**Files:**
- Modify: `DESIGN.md` (§2, §3.2 `max_burned_epoch` row, §13.1 last paragraph, new §13.2, §16, §20, §21, §22), `CLAUDE.md` (invariants), `PROTOCOL.md` (§6.5 table row prose only; **not** §14), `src/lib.rs` (crate docs), `src/admin.rs` (doc of `NodeInfo::max_burned_epoch`)
- Create: `examples/node.rs`
- Modify: `examples/README.md`

**Interfaces:**
- Consumes: the whole public API of Tasks 1-6.
- Produces: documentation only, plus `examples/node.rs` (exit 0).

- [ ] **Step 1: Write the example (it is the test of this task)**

`examples/node.rs`:

```rust
//! The failover runbook with the `Node` handle (DESIGN.md section 13.2), in one process.
//!
//!   1. A is the master, B its replica, each behind one `Node`.
//!   2. B drops out; A takes one write B never sees; then A dies.
//!   3. B comes back and is promoted through the same handle (`promote_checked`).
//!   4. A restarts: its stored role is master, so it opens FENCED (B is a master).
//!   5. A rejoins as a slave of B: refused (diverged), its extra entry is shown.
//!   6. The operator accepts the loss explicitly: `Repair { accept_loss: AcceptDataLoss }`.
//!
//! Run with: `cargo run --example node`

use std::error::Error as StdError;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    AcceptDataLoss, AllowAll, CfSpec, Error, MasterConfig, Node, NodeConfig, NodeState,
    RejoinOptions, RepointDivergence, RepointOptions, ServeConfig, SlaveConfig, UpstreamConfig,
};

type Res<T> = Result<T, Box<dyn StdError>>;
const WAIT: Duration = Duration::from_secs(10);

fn serve() -> ServeConfig {
    ServeConfig::new(IpAddr::V4(Ipv4Addr::LOCALHOST), 0, Arc::new(AllowAll))
}

/// One node: the same directory and identifier in both roles. `upstream` is the default
/// upstream of the slave role, `peers` the nodes the fence check and `promote_checked` ask.
fn node_cfg(dir: &Path, id: &str, upstream: SocketAddr, peers: &[SocketAddr]) -> NodeConfig {
    let mut slave = SlaveConfig::new(id, dir.join(id), UpstreamConfig::from_addr(upstream));
    slave.serve = Some(serve());
    let master = MasterConfig::from_slave_config(&slave, Some(serve()));
    let mut cfg = NodeConfig::new(master, slave);
    cfg.peers = peers.iter().copied().map(UpstreamConfig::from_addr).collect();
    cfg
}

fn show(node: &Node, key: &[u8]) -> String {
    match node.get("kv", key) {
        Ok(Some(v)) => String::from_utf8_lossy(&v).into_owned(),
        Ok(None) => "(absent)".into(),
        Err(e) => format!("(error: {e})"),
    }
}

#[tokio::main]
async fn main() -> Res<()> {
    let tmp = tempfile::tempdir()?;
    let dir = tmp.path();
    let nowhere: SocketAddr = ([127, 0, 0, 1], 1).into(); // A starts as master: no upstream yet

    println!("1. A is the master, B its replica");
    let a = Node::create_master(node_cfg(dir, "A", nowhere, &[])).await?;
    a.create_cf(CfSpec::new("kv"))?;
    let a_addr = a.serve_addr().expect("A serves");
    let b = Arc::new(Node::create_slave(node_cfg(dir, "B", a_addr, &[a_addr])).await?);
    let seq = a.put("kv", b"k1", b"everyone has this")?;
    b.wait_for_seq(seq, WAIT).await?;
    println!("   B state: {:?}", b.state());

    println!("2. B drops out, A writes k2 alone, then A dies");
    b.shutdown().await?;
    a.put("kv", b"k2", b"only A has this")?;
    a.shutdown().await?;

    println!("3. B comes back and is promoted (A is unreachable: reported, not blocking)");
    b.reopen().await?;
    b.promote_checked().await?;
    b.put("kv", b"k3", b"written on B")?;
    let b_addr = b.serve_addr().expect("B serves");
    println!("   B state: {:?}", b.state());

    println!("4. A restarts: a stored master opens fenced until a peer check clears it");
    let a = Node::open(node_cfg(dir, "A", b_addr, &[b_addr])).await?;
    match a.state() {
        NodeState::Fenced { reason } => println!("   A is fenced: {reason}"),
        other => return Err(format!("expected A fenced, got {other:?}").into()),
    }
    match a.get("kv", b"k1") {
        Err(Error::Fenced { .. }) => println!("   reads on A are refused while fenced"),
        other => return Err(format!("expected Fenced, got {other:?}").into()),
    }

    println!("5. A rejoins as a slave of B (no wipe); divergence is refused by default");
    match a.rejoin_as_slave(None, RejoinOptions::default()).await {
        Err(Error::Diverged { fork_seq, divergent_entries, .. }) => {
            println!("   refused: {divergent_entries} entr(y/ies) from seq {fork_seq} exist only on A");
            for e in a.diverged_entries()? {
                println!("   salvageable entry: seq {} (epoch {})", e.seq, e.epoch);
            }
            println!("   A still reads its own data: k2 = {}", show(&a, b"k2"));
        }
        Ok(outcome) => println!("   A follows B: {outcome:?}"),
        Err(e) => return Err(e.into()),
    }

    println!("6. The operator accepts the loss of A's extra entry, explicitly");
    let repair = RepointOptions {
        on_divergence: RepointDivergence::Repair { accept_loss: AcceptDataLoss },
    };
    let outcome = a.repoint(UpstreamConfig::from_addr(b_addr), repair).await?;
    println!("   {outcome:?}");
    a.wait_for_seq(b.last_seq()?, WAIT).await?;
    println!("   A: k1 = {}, k2 = {}, k3 = {}", show(&a, b"k1"), show(&a, b"k2"), show(&a, b"k3"));
    assert_eq!(a.get("kv", b"k2")?, None);
    assert_eq!(a.get("kv", b"k3")?, Some(b"written on B".to_vec()));

    a.shutdown().await?;
    b.shutdown().await?;
    println!("done");
    Ok(())
}
```

- [ ] **Step 2: Run it**

Run: `CARGO_BUILD_JOBS=4 cargo run --example node`
Expected: the six steps print, A is fenced with `peer B is a master`, the rejoin is refused with 1 entry, `Repaired { discarded: .. }`, then `k2 = (absent)`, `k3 = written on B`, `done`, exit status 0.

- [ ] **Step 3: Write the docs**

`DESIGN.md`:

- §2, the role bullet becomes: "The role is persisted in `__meta`. Opening a slave directory as `Master` (or vice versa) is refused; the only role changes are the explicit `Slave::promote*`, `Master::demote` and `Slave::rejoin` (§13.1, §13.2)."
- §3.2, `max_burned_epoch` row: "the highest epoch number this node started and gave up: with `Master::demote`, or the empty trailing epochs `Slave::rejoin` drops (absent = 0). …" (rest unchanged).
- §13.1, replace the last bullet ("The old master can never rejoin as master. … which bootstraps.") with: "The old master can never rejoin as master. A restart starts an epoch of its own; replicas of the new history refuse to follow it (`STALE_EPOCH` while its number is lower, retried and reported; `DIVERGED` otherwise). Opened through `Node`, a stored master starts **fenced** and a peer check finds the new master (§13.2). To reuse the machine, turn it into a slave of the new master with `Node::rejoin_as_slave` (or `Slave::rejoin`), which keeps its data when its log is a prefix of the new master's and refuses (`Diverged`) otherwise; wiping its directory and starting a new slave remains possible."
- New §13.2 after §13.1:

```markdown
### 13.2 `Node`: one handle, explicit role switches, fenced open, rejoin

`Node` holds one database directory in either role and is shared as `Arc<Node>`. Role changes happen only through explicit calls on it, and every caller sees the new role on its next call. `NodeConfig { master, slave, peers, unchecked_master }` names the same directory and identifier in both role configs.

- **State.** `Node::state()` / `watch_state()`: `Master { identity }`, `Fenced { reason }`, `Slave { state, upstream }` (the slave's own states, forwarded live), `Switching`, `Failed { error }`, `Stopped`.
- **Calls.** Reads (`get`, `get_with_meta`, `multi_get`, `list_cfs`, `snapshot`) work in both roles; writes are master-only (`NotMaster`); slave admin (`repoint`, `clear_halt`, `verify_now`, `diverged_entries`, `status`, `wait_*`) is slave-only (`NotSlave`). `Fenced` answers `Fenced { reason }`, `Failed`/`Stopped` `NodeFailed`. `repoint` also replaces the upstream used by later reopens of the slave role.
- **Concurrency.** A `tokio::sync::RwLock` holds the role. Sync calls use `try_read` and return `Switching` while a switch holds or waits for the lock: no caller ever blocks behind a switch. Async slave-admin calls hold the read lock across their await, so a switch waits for them. A switch takes the write lock, moves the running handle out, calls the consuming core operation (`promote_checked`, `promote`, `demote`, `rejoin`), and stores the result; a refusal hands the old handle back and it is put back running. A lost handle (a failure after shutdown began, a live `ReadSnapshot`, a switch future dropped before it finished) makes the node `Failed`; `reopen` opens the stored role again.
- **Fenced open.** `Node::open` of a directory whose stored role is `master` opens nothing at first: no epoch, no port, no reads or writes (the data of a returning old master may be divergent; nothing reads it as current). It runs the fence check once: read this node's `NodeInfo` from the closed store (read-only open; nothing written), ask every peer for `Status` (5 s each, concurrently), and clear only if at least one peer is configured, every peer answered (unless `UnfenceOptions::allow_unreachable: Some(AcceptSplitBrainRisk)`), no peer is a master, no peer is later than or diverged from this node (`admin::compare`, with CRC probes), no peer is of another database, and no peer knows an epoch at or above this node's newest one other than that record (history, identity or `max_burned_epoch`). Cleared → the master opens (and starts its epoch); otherwise the node stays `Fenced { reason }` (`NoPeers`, `PeerUnreachable`, `PeerIsMaster`, `PeerAhead`, `PeerDiverged`, `PeerError`) until the application calls `unfence` or `rejoin_as_slave`. Nothing re-checks automatically. `NodeConfig::unchecked_master: Some(AcceptSplitBrainRisk)` skips the check.
- **Rejoin.** `Slave::rejoin(cfg, RejoinOptions)` (and `Node::rejoin_as_slave` from `Master` or `Fenced`) turns a stopped master into a slave without a wipe. One synced batch: `role = slave`; the trailing epoch records with `start_seq > last_seq` (restarts without writes, an empty promotion epoch) are dropped and their highest number burned into `max_burned_epoch`; `origin_identity` follows the newest remaining record (same batch); the master-only keys (`__ttl_index`, `promoted_from_len`, `last_ts`) are removed and the slave starts ready. The role is persisted before the first handshake: a slave never writes and divergence halts by default, and consent is never persisted, so a crash after the batch reopens as a slave that halts `Diverged` if it diverged. The first handshake is armed as a repoint with `RejoinOptions::on_divergence` (default `Refuse`) and settles with the outcomes and single-use consent of `repoint` (§13.1): after a failover the new master runs a newer epoch, so a prefix log answers `EpochAdvanced`; a log with entries the new master lacks is `Diverged` and halts with its data intact (`diverged_entries`), resolved only with `AcceptDataLoss` (`Repair` or `Rebootstrap`, at rejoin or by a later `repoint`/`clear_halt`).
```

- §16 (Public API), add after the `Slave` block:

```rust
pub struct NodeConfig { pub master: MasterConfig, pub slave: SlaveConfig,
                        pub peers: Vec<UpstreamConfig>,
                        pub unchecked_master: Option<AcceptSplitBrainRisk> }
pub struct Node { .. }   // Send + Sync; share as Arc<Node>
impl Node {
    async fn create_master(cfg) / create_slave(cfg) / open(cfg) -> Result<Node>;
    async fn from_master(Master, cfg) / from_slave(Slave, cfg) -> Result<Node>;
    fn state(&self) -> NodeState; fn watch_state(&self) -> watch::Receiver<NodeState>;
    // reads (both roles), writes (master), slave admin (slave) as on Master/Slave
    async fn promote_checked(&self) / promote(&self) / demote(&self) -> Result<()>;
    async fn rejoin_as_slave(&self, Option<UpstreamConfig>, RejoinOptions) -> Result<RepointOutcome>;
    async fn unfence(&self, UnfenceOptions) / reopen(&self) / shutdown(&self) -> Result<()>;
}
impl Slave { async fn rejoin(cfg: SlaveConfig, opts: RejoinOptions)
                 -> Result<(Slave, Result<RepointOutcome>)>; }
```

- §20: add `    node/             Node handle: role switches, fence check, state forwarding` under `slave/`.
- §21: add a bullet: "**Node:** create/open per role; fenced open (no peers, unreachable peer and `allow_unreachable`, old master fenced by the new one, `unchecked_master`, unfence with behind slaves); promotion under concurrent writers (`NotMaster`/`Switching` only); refused promote keeps the slave; demote; `watch_state` sees `Switching`; a cancelled switch and a lost handle leave `Failed` and `reopen` recovers; rejoin resume, trailing empty epoch burned, diverged refused, repair with consent, crash after the rewrite."
- §22 Decided: add "A stored master opened through `Node` is fenced until a peer check clears it; a former master becomes a slave only through the explicit `rejoin`, which never wipes and never discards entries without `AcceptDataLoss`."

`CLAUDE.md`, add to "Invariants (do not break)" after the "Every master open starts an epoch" bullet, verbatim from the spec:

```markdown
- **A stored master reopens fenced** through `Node`: no epoch, no port, no reads or writes until a peer check clears it or `AcceptSplitBrainRisk` is given; a former master becomes a slave only via the explicit `rejoin`.
```

`PROTOCOL.md` §6.5 `max_burned_epoch` row (not §14): "The highest epoch number this node started as a master and then gave up by undoing the promotion or by rejoining as a slave with empty trailing epochs (0 if none). …" (rest unchanged).

`src/admin.rs` `NodeInfo::max_burned_epoch` doc: "The highest epoch number this node started as a master and gave up (`Master::demote`, or the empty trailing epochs `Slave::rejoin` dropped; 0 if none). …".

`src/lib.rs` crate docs, add after the read-modify-write section:

```rust
//! # One handle for either role: `Node`
//!
//! [`Node`] holds a database directory as master or slave and changes role only through
//! explicit calls (`promote_checked`, `demote`, `rejoin_as_slave`, ...), so an `Arc<Node>`
//! shared by the application always points at the current role. A directory whose stored role
//! is `master` opens **fenced** — no epoch, no port, no reads or writes — until a peer check
//! proves no other master exists ([`NodeConfig::peers`]) or the caller passes
//! [`AcceptSplitBrainRisk`]. A former master rejoins as a slave of the new master without a
//! wipe ([`Slave::rejoin`]); entries only it holds are discarded only with [`AcceptDataLoss`].
//!
//! ```no_run
//! # use replicated_rocksdb::{Node, NodeConfig, NodeState, RejoinOptions, Result};
//! # async fn f(cfg: NodeConfig) -> Result<()> {
//! let node = std::sync::Arc::new(Node::open(cfg).await?);
//! if let NodeState::Fenced { reason } = node.state() {
//!     eprintln!("not serving: {reason}");
//!     node.rejoin_as_slave(None, RejoinOptions::default()).await?; // refuses divergence
//! }
//! # Ok(()) }
//! ```
```

`examples/README.md`, add a row after `repair`:

```markdown
| `node` | The `Node` handle through a failover: `create_master` / `create_slave`, `promote_checked` on the shared handle, the old master reopening **fenced** (`PeerIsMaster`), `rejoin_as_slave` refusing the divergence, `diverged_entries`, then `repoint(.., Repair { accept_loss: AcceptDataLoss })`. | Six numbered steps; A fenced by B, the refused rejoin with its one salvageable entry, `Repaired { .. }`, and A's final reads (`k2` absent, `k3` from B). |
```

- [ ] **Step 4: Run the final gate**

Run each, in order; all must succeed:

```bash
CARGO_BUILD_JOBS=4 cargo test
CARGO_BUILD_JOBS=4 cargo test --features failpoints
CARGO_BUILD_JOBS=4 cargo clippy --all-targets --all-features -- -D warnings
cargo fmt --check
CARGO_BUILD_JOBS=4 cargo doc --no-deps
for e in embedded_master master_and_replica write_concern failover secure_replication verify repair node; do CARGO_BUILD_JOBS=4 cargo run --example $e || exit 1; done
```

Expected: all tests pass (including `proto_sync`, which proves `proto/replication.proto` is untouched), no clippy warnings, no fmt diff, docs build without broken intra-doc links, every example exits 0.

- [ ] **Step 5: Commit**

```bash
git add DESIGN.md CLAUDE.md PROTOCOL.md src/lib.rs src/admin.rs examples/node.rs examples/README.md
git commit -m "docs: Node handle, fenced open and rejoin; node example

Co-Authored-By: Claude Opus 5.5 <noreply@anthropic.com>"
```

---

## Spec coverage map

| Spec item | Task |
|---|---|
| §2 `Slave::rejoin`, steps 1-4, failpoint | 2 (batch), 3 (open + settle + failpoint) |
| §3 `NodeConfig` + validation, constructors, `NodeState`, `state`/`watch_state` | 1, 5 |
| §3 reads / writes / slave admin, `repoint` updates upstream | 5 |
| §3 switches, `try_read` fail-fast, refused switch hands back, `Failed` | 6 |
| §3 new `Error` variants | 1 |
| §4 fenced open, fence check, `FenceReason`, `unfence`, `unchecked_master` | 4, 5, 6 |
| §5 DESIGN section + §13 sentence, CLAUDE.md invariant, crate docs, example, README | 7 |
| §5 test list | 3 (rejoin cases, crash), 5 (create/open, empty/non-empty, NotMaster, fenced reads, NoPeers, PeerIsMaster, unchecked, from_*), 6 (writers, demote, refused promote, watch Switching, unreachable + allow, unfence, Failed → reopen) |
