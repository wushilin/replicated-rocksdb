# Plan 4 spec: `Node` handle, fenced open, rejoin of a former master

Status: approved in conversation on 2026-10-11 (the user designed this with Claude and said
"work until completion"). The user's standing rule applies throughout: **every operation that can
lose data or create two writers is an explicit call, refuses by default, and needs a typed
consent token to proceed** (`AcceptDataLoss`, and the new `AcceptSplitBrainRisk`).

## 1. Goals

1. A `Node` handle that applications share as `Arc<Node>` and that holds either role. Role
   changes happen *inside* it, explicitly, so every caller sees the new role on its next call
   without swapping references.
2. An exact, observable state API: `node.state()` and `node.watch_state()`.
3. **Fenced open**: a directory whose stored role is `master` opened through `Node` does not
   accept writes until a peer check clears it (prevents an old master that restarts after a
   failover from taking writes next to the new one).
4. **Rejoin**: a former master that has taken writes can become a slave of the new master
   without wiping its directory: `Resumed` when its log is a prefix of the new master's,
   `DIVERGED` halt otherwise, resolved only with `AcceptDataLoss` (same rules as `repoint`).

Non-goals: no merging of change logs; no automatic role change of any kind; `Master` and
`Slave` keep their current public API and behaviour (except additions below).

## 2. Core addition: `Slave::rejoin`

```rust
impl Slave {
    /// Opens a directory whose stored role is `master` (closed, not running) as a slave of
    /// `cfg.upstream`. Explicit; see DESIGN.md (new section).
    pub async fn rejoin(cfg: SlaveConfig, opts: RejoinOptions) -> Result<(Slave, RepointOutcome)>;
}
pub struct RejoinOptions { pub on_divergence: RepointDivergence } // Default: Refuse
```

Steps:

1. Refuse with `WrongRole` unless the stored role is `master`; refuse on invalid `cfg` like
   `Slave::open` does. Refusals leave the directory unchanged.
2. **Rewrite batch** (one synced `WriteBatch`):
   - `role = slave`;
   - drop **trailing empty epochs** from the epoch history: records with `start_seq > last_seq`
     (per-open epochs of restarts that took no writes, or an empty promotion epoch). Their
     highest epoch number is **burned** into `__meta.max_burned_epoch` (max with the existing
     value), exactly like `Master::demote` does. At least one record always remains (the one
     covering `last_seq`; a database with `last_seq == 0` keeps its first record);
   - `origin_identity = (db_uuid, epoch of the newest remaining record)`; history and identity
     in the same batch (invariant);
   - remove master-only keys: every `__ttl_index` entry and `promoted_from_len` (and any other
     key DESIGN.md §2 marks as master-only; the implementer lists them);
   - `node_state` absent (= ready); no `repair_until`.
   Ruling: the role is persisted in this batch (before the handshake). This is safe because a
   slave never writes and divergence halts by default; consent is never persisted. A crash
   after the batch reopens as a slave that halts `DIVERGED` if it diverged.
3. Open the slave and settle the first handshake against `cfg.upstream` with the consent in
   `opts` and the same semantics and return values as `Slave::repoint` (`Resumed`,
   `EpochAdvanced`, `Bootstrapped`, `Repaired { discarded }`, `Retrying { .. }`;
   `Err(Halted(Diverged))` under `Refuse`, in which case the slave is still returned to the
   caller — design the return type so the halted slave is not lost, e.g.
   `Result<(Slave, Result<RepointOutcome>)>` or a `RejoinRefused { error, slave }` like
   `PromoteRefused`; pick one and document it).
4. Failpoint `rejoin-after-rewrite` between steps 2 and 3 for a crash test.

## 3. `Node`

```rust
pub struct NodeConfig {
    pub master: MasterConfig,          // used whenever the node runs as master
    pub slave: SlaveConfig,            // used whenever it runs as slave (upstream = default upstream)
    pub peers: Vec<UpstreamConfig>,    // for promote_checked and the fenced-open check
    pub unchecked_master: Option<AcceptSplitBrainRisk>, // skip the fenced-open check
}
```
Validation: `master.path == slave.path` and `master.identifier == slave.identifier`, else
`Error::Config`.

Constructors (all async, all return `Result<Node>`):
- `Node::create_master(cfg)` / `Node::create_slave(cfg)`: only for a directory with **no stored
  role** (missing or empty); otherwise `Error::Config`. No fencing on create.
- `Node::open(cfg)`: reads the stored role. No role → `Error::Config` ("use create_*").
  `slave` → opens `Slave` with `cfg.slave`. `master` → **fenced open** (§4).
- `Node::from_master(Master, cfg)` / `Node::from_slave(Slave, cfg)`: wrap a running handle.

State:
```rust
#[non_exhaustive]
pub enum NodeState {
    Master { identity: DbIdentity },
    Fenced { reason: FenceReason },
    Slave { state: SlaveState, upstream: UpstreamConfig },  // or a display string; implementer's call
    Switching,
    Failed { error: String },
}
pub fn state(&self) -> NodeState;
pub fn watch_state(&self) -> watch::Receiver<NodeState>;  // slave sub-states forwarded live
```

Calls (all `&self`):
- Reads (`ReadApi`-equivalent: `get`, `get_with_meta`, `multi_get`, iteration, `snapshot`)
  work as master or slave; `Fenced` → `Err(Fenced)`, `Switching` → `Err(Switching)`,
  `Failed` → `Err(NodeFailed)`.
- Writes (`put`, `put_with_ttl`, `put_with`, `delete`, `delete_range`, `write`, `write_with`,
  `create_cf`, `drop_cf`, `reap_expired`, `bump_epoch`) → master only; slave → `Err(NotMaster)`.
- Slave admin (`repoint`, `clear_halt`, `verify_now`, `diverged_entries`, `status`) → slave only.
  `repoint` also updates the upstream the node uses for later reopens of the slave role.
- Switches (async, explicit, each returns an outcome or an error; a refusal leaves the previous
  role in place and running):
  - `promote_checked(&self) -> Result<()>` (uses `cfg.peers`, `cfg.master`), and
    `promote(&self)` (unchecked, same as `Slave::promote`);
  - `demote(&self)` (`Master::demote` semantics: only undoes an unused promotion);
  - `rejoin_as_slave(&self, upstream: Option<UpstreamConfig>, opts: RejoinOptions)` from
    `Master` or `Fenced` (§2); `None` = `cfg.slave.upstream`;
  - `unfence(&self, opts: UnfenceOptions)` from `Fenced` (§4);
  - `reopen(&self)` from `Failed` (runs `open` logic again, including fencing);
  - `shutdown(&self)`.
- Concurrency: `tokio::sync::RwLock<Role>`. Sync calls use `try_read()` and fail fast with
  `Err(Switching)` when a switch holds or waits for the write lock (never block a caller
  behind a switch). Async slave-admin calls hold the read guard across their await (a switch
  waits for them). Switches take the write lock, `mem::replace` the role with `Switching`,
  call the consuming core operation, and store the result. A refused core switch hands the
  old handle back (`PromoteRefused::slave`, `DemoteRefused::master`) and it is put back. If the
  old handle is lost, the role becomes `Failed { error }`.
- New `Error` variants: `NotMaster`, `NotSlave`, `Fenced { reason }`, `Switching`,
  `NodeFailed(String)` (names may be adjusted; `Error` is `non_exhaustive`).

## 4. Fenced open

When `Node::open` finds stored role `master`:
- If `cfg.unchecked_master` is `Some(AcceptSplitBrainRisk)`: `Master::open(cfg.master)` directly.
- Otherwise the node starts **Fenced**: no master is opened, no epoch is started, no port is
  served, reads and writes return `Err(Fenced)`. Ruling: fenced reads are refused (the data of
  a returning old master may be divergent; nothing should read it as current).
- `open` then runs the fence check once (same as `unfence(UnfenceOptions::default())`); if it
  clears, it opens the master; if not, `open` still returns `Ok(node)` in `Fenced { reason }`.
  Nothing re-checks automatically; the application calls `unfence` or `rejoin_as_slave`.

Fence check (reuse `Slave::check_promotion`/`admin::compare` machinery where possible):
- Read this node's `NodeInfo` (identity, history, last_seq, max_burned_epoch) from the closed
  store (open read-only or open-and-close; must not start an epoch or write anything).
- Ask every peer in `cfg.peers` for `Status` (5 s timeout each, concurrently).
- Clear only if: at least one peer is configured; every peer answered (unless
  `UnfenceOptions { allow_unreachable: Some(AcceptSplitBrainRisk) }`); no peer reports
  `role == Master`; no peer is ahead of or diverged from this node (by `admin::compare`: this
  node must be equal or later and compatible); no peer reports a `max_burned_epoch` or epoch
  `>=` this node's newest epoch number with a different nonce.
- `FenceReason` (non_exhaustive): `NoPeers`, `PeerUnreachable { peer }`, `PeerIsMaster { peer }`,
  `PeerAhead { peer }`, `PeerDiverged { peer }`, `PeerError { peer, error }`.

## 5. Docs, examples, tests

- DESIGN.md: new section for `Node`, fenced open and rejoin; update the §13 sentence "The old
  master can never rejoin as master… wipe its directory" to point at `rejoin_as_slave`.
- CLAUDE.md invariant: "**A stored master reopens fenced** through `Node`: no epoch, no port,
  no reads or writes until a peer check clears it or `AcceptSplitBrainRisk` is given; a
  former master becomes a slave only via the explicit `rejoin`."
- README/crate docs: mention `Node`. `examples/node.rs`: full failover runbook (A master, B
  slave; A dies; B promoted via `Node`; A restarts → Fenced(PeerIsMaster); A
  `rejoin_as_slave` → Resumed or Diverged → shows `diverged_entries` and repairs with
  `AcceptDataLoss`). Must exit 0; add to examples/README.md.
- Tests (real multi-node, ports from `support::reserved_port()`):
  create/open per role; create on a non-empty dir refused; open on empty dir refused;
  writes on slave → `NotMaster`; reads on fenced → `Fenced`; promote through `Node` while
  writer tasks hammer `put` (each `put` either succeeds as master or fails with
  `NotMaster`/`Switching`; no other error; after promote, writes succeed); demote; refused
  promote keeps the slave running; `watch_state` sees Slave → Switching → Master;
  fenced open after failover (old master → `PeerIsMaster`); fenced open with no peers →
  `NoPeers`; unreachable peer → `PeerUnreachable`, and `allow_unreachable` clears it;
  `unchecked_master` opens directly; unfence clears when peers are behind slaves;
  rejoin resume (old master behind/equal → `Resumed`); rejoin with a trailing empty epoch
  (old master restarted after the failover without writes) → `Resumed` and the epoch is burned;
  rejoin diverged under `Refuse` → halted, `diverged_entries` lists them, nothing deleted;
  rejoin with `Repair { AcceptDataLoss }` → `Repaired`, data equals new master;
  crash at `rejoin-after-rewrite` (failpoints) → reopen as slave works and still halts on
  divergence; `Failed` → `reopen`; `from_master`/`from_slave`.
- All existing tests keep passing; clippy (`-D warnings`), fmt, doc and every example clean.
