# Plan 4 execution rulings

Decisions made during subagent-driven execution of Plan 4 (`2026-10-11-plan4-node-handle.md`, Tasks 1-7), in order. Each line: what was decided, why, and the cost if wrong.

## Pre-flight (F1-F13)

- F1: Task 5 owns `src/node/switch.rs` with `Switch`, `begin_switch`, `Node::shutdown` and `Node::reopen`; Task 6 adds the other switches and the `#[ignore]` is dropped. Why: Task 5's tests call both, and each task must leave a green build and clean clippy. Cost: a task boundary moved.
- F2: delete the unused `OfflineLog::meta()`. Why: its `dead_code` warning would fail `clippy -D warnings`. Cost: none.
- F3: `PeerAnswer::Answered` holds `Box<NodeInfo>`. Why: `clippy::large_enum_variant` (about 232 B vs 24 B). Cost: one allocation per peer check.
- F4: Task 3 adds only `RejoinOptions` to `tests/failpoints.rs` imports; Task 6 adds only `NodeState` (or nothing). Why: the plan's lists duplicate existing imports (E0252) or leave unused ones. Cost: none.
- F5: remove the unused `wait_state` import from `tests/rejoin.rs`. Why: clippy gate. Cost: none.
- F6: `STOPPED = "the node was shut down"`, `CANCELLED = "a role switch was cancelled before it finished"`. Why: `Error::NodeFailed` already appends "; call Node::reopen". Cost: none.
- F7: CLAUDE.md "Consent is scoped" also lists the first handshake of `Slave::rejoin` / `Node::rejoin_as_slave` (armed as a generation-0 repoint). Why: a reviewer checks every consent path against that bullet. Cost: a stale invariant if omitted.
- F8: `node_cfg` and `failover` live in `tests/support/mod.rs` and are shared by both test binaries; the store-level duplication with `demote_to_slave` is optional. Why: avoid copies drifting. Cost: some test churn.
- F9: assert `Slave` precedes `Switching` in `watch_state`; add a default-options `unfence` success test (peer restarted after `PeerUnreachable`). Why: spec coverage. Cost: two tests.
- F10: `NodeState::Slave.upstream` is published from the slave's `status().upstream`; `from_slave` refuses with `Config` on a mismatch with `cfg.slave.upstream`. Why: the slave is the truth about what it follows. Cost: `from_slave` is stricter.
- F11: accept `EpochAdvanced` for the spec's "Resumed"; `Slave::rejoin` on a directory with no database returns `Config` (not `WrongRole`); add `NodeState::Stopped`; `Node` has no `ReadApi` impl (iterate through `snapshot()`). Why: each is correct against the handshake rules and weakens no consent rule. Cost: small spec drift.
- F12: leave the `max_burned_epoch` comment in `proto/replication.proto` / PROTOCOL.md section 14 ("gave up by demoting") untouched. Why: the proto must stay identical to section 14 (`tests/proto_sync.rs`); section 6.5 and DESIGN carry the full wording. Cost: a slightly narrow comment.
- F13: all `use` lines at the top of `slave/rejoin.rs`; plan bookkeeping omissions need no code change. Why: style only. Cost: none.

## During execution

- Task 1: the private `slave::state::NodeState` name clash with the public `node::NodeState` is left as is. Why: private, no conflict. Cost: reader confusion only.
- Task 2: no scoped re-review of fix round 1 (four doc/test-only minors). Why: store tests and clippy cover them. Cost: the final review catches any miss. Carry: the rewrite runs on a plain `Store::open` (no per-open epoch) and the master store is dropped before the slave reopen.
- Task 3: repoint-mode shadowing of `cfg.on_divergence` is accepted. Why: matches `repoint` and only suppresses data loss. Cost: none. No re-review of the doc/test-only fix round. Cost: the final review catches any miss.
- Task 4 (a): any peer history record absent from ours blocks clearing at any epoch number, even under `AcceptSplitBrainRisk`. Why: it is evidence of another writer. Cost: more fenced opens that need rejoin or an override.
- Task 4 (b): local config/TLS rejections are answered errors, not "unreachable" (shared with `promote_checked`, which gets stricter: `PromotionUnverifiable`). Why: consent must never skip a peer that was not contacted. Cost: `promote_checked` refuses on a misconfigured peer list.
- Task 4 (c): a fence-check `Err` makes `Node::open` fail rather than leave `Fenced`. Why: `Err` only means the local store is unreadable, so there is nothing to fence or serve. Cost: none.
- Task 5: the slave is the source of truth for the upstream (captured when the handle is taken out). Why: a dropped or superseded repoint made `Node.upstream` drift. Cost: none.
- Task 6 (a): fix the core so a slave leaves `Bootstrapping` once its bootstrap commits (reports `Connecting`). Why: promotion was refused for up to about 40 s after a finished bootstrap. Cost: a small core change.
- Task 6 (b): a refused promotion reopens the slave with the upstream it currently follows (public behaviour change). Why: safer than the original config. Cost: doc change.
- Task 6 (c): a failed open after `unfence` leaves the node `Failed`; `reopen` re-checks without consent. Why: consent is never carried over. Cost: one more `unfence` call.

## Follow-ups (not done; for the maintainer)

- TLS classification: with TLS 1.3 a missing-client-certificate rejection surfaces as `BrokenPipe` on first use and is still treated as unreachable. A peer that rejects our certificate can therefore be waived by `AcceptSplitBrainRisk` and does not block `promote_checked`.
- `Master::open` cancel-safety when an `unfence` / `open` / `reopen` future is dropped mid-open is unverified. The node ends `Failed` and `reopen` recovers, but a half-started epoch or port was not examined.
- Plan 3 follow-ups remain in `2026-10-09-plan3-rulings.md`.
