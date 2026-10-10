# Spec addendum: acks, write concern, failover tooling and divergence repair

Status: agreed with the user, 2026-10-09. To be merged into DESIGN.md and PROTOCOL.md by the owning tasks (Plan 2 amendment for §1–§4, Plan 3 for §5). Where this addendum and DESIGN/PROTOCOL disagree, this addendum wins until the merge.

## 1. Replica acks and replica status

- `Tail` becomes a bidirectional stream: `rpc Tail(stream TailClientMessage) returns (stream TailMessage)`.
  - `TailClientMessage { oneof kind { TailRequest start = 1; TailAck ack = 2; } }`. The first message MUST be `start`; later messages MUST be `ack`.
  - `TailAck { uint64 applied_seq = 1; }`. Non-decreasing; never above the last seq the server sent; a violation ends the stream with `INVALID_ARGUMENT`.
  - Meaning: every entry with seq ≤ `applied_seq` is committed in the replica's store (written to its WAL; also fsynced when the replica sets `sync_writes`).
  - The client sends an ack after applying each batch (coalesced if acks back up) and at least once per heartbeat interval.
- Servers track, per open Tail stream: node id, remote address, `peer_id_verified`, identity epoch, connected-since, last sent seq, last acked seq, last time the replica was fully caught up, and the stream's fetch settings.
- `Master::replicas()` and `Slave::replicas()` return `Vec<ReplicaStatus>` for direct downstream replicas: `{ node_id, remote_addr, peer_verified, epoch, connected_since, sent_seq, applied_seq, lag_entries, lag_ms, in_sync, fetch }`. `lag_ms` is derived from entry `ts_ms` (now − ts of the first unacked entry; 0 when caught up).

## 2. In-sync replicas and write concern (master)

- A direct replica is **in sync** when its Tail stream is open, it follows the master's current identity, and it has been fully caught up (`applied_seq == master last_seq` at some moment) within the last `replica_lag_time_max` (default 10 s). Only direct replicas of the master count.
- `MasterConfig.write_concern: WriteConcern { min_insync_replicas: usize /* 0 = async (default) */, ack_timeout: Duration /* 5 s */ }`; per-call override via `Master::write_with(batch, WriteConcern)` (and `put_with`/… helpers as needed).
- With `min_insync_replicas = N > 0`:
  1. Before committing: if fewer than N replicas are in sync → `Error::NotEnoughReplicas { required, in_sync }`. Nothing written; no seq consumed.
  2. Commit locally (fsync) as today; release the writer lock.
  3. Wait until N in-sync replicas have acked ≥ seq → `Ok(seq)`.
  4. On timeout → `Error::AckTimeout { seq, acked, required }`. The entry is committed on the master and will still replicate; the error only says N acks were not confirmed in time. Documented prominently (Kafka semantics).
- Waiting writes do not hold the writer lock (pipelining). Write methods stay synchronous; async callers use `spawn_blocking`.

## 3. Repoint

- `Slave::repoint(upstream: UpstreamConfig, opts: RepointOptions) -> Result<RepointOutcome>` switches upstream at runtime without reopening. The slave keeps serving reads.
- It closes the current upstream stream (aborting an in-progress bootstrap, which restarts from scratch against the new upstream) and handshakes the new upstream with all normal rules (loop detection, STALE_EPOCH, epoch continuity).
- **Divergence is never resolved implicitly by repoint.** `RepointOptions { on_divergence: Refuse /* default */ | Repair { accept_loss: AcceptDataLoss } | Rebootstrap { accept_loss: AcceptDataLoss } }`:
  - `Refuse` (default): if the handshake answers DIVERGED, `repoint` returns `Error::Diverged { fork_seq, local_last_seq, divergent_entries }` and the slave stays halted `Diverged` with its data intact and readable. The configured `on_divergence` policy is NOT applied on an explicit repoint.
  - `Repair` / `Rebootstrap` require an explicit `AcceptDataLoss` token (a unit type constructed by the caller) acknowledging that the divergent entries are discarded.
- `RepointOutcome { resumed | epoch_advanced | bootstrapped | repaired { discarded: Seq range } }`.
- The upstream is not persisted; after restart the application opens the slave with the new upstream.

## 4. Promotion safety tooling

- Read-only `Status` RPC on the replication service (authenticated like the others; `Method::Status`): node id, role, identity, epoch history, last_seq, last_entry_crc, last_ts_ms, node state, upstream, replicas.
- `Slave::promotion_info() -> NodeInfo` (same fields, in-process).
- `admin::compare(a, b) -> Comparison { Later(a|b) | Equal | Diverged { fork } | DifferentDatabase }`: later = same db_uuid, compatible histories (the shorter node's last entry is present in the longer with the same CRC, or provable via epoch history fork points), then higher newest epoch, then higher last_seq.
- `admin::choose_promotion_candidate(&[NodeInfo]) -> Result<Choice>` picks the most advanced compatible node and lists diverged/foreign nodes.
- `Slave::promote_checked(cfg, peers: &[UpstreamConfig]) -> Result<Master>` queries each reachable peer's `Status`; refuses with `Error::NotMostAdvanced { leader, seq }` if any is later. Unreachable peers are reported in the error/log but do not block (operator's call). Plain `promote` remains.
- `Master::demote(cfg: SlaveConfig) -> Result<Slave>`: allowed only while the current epoch has no entries (`last_seq < epoch_start_seq`); persists role=slave and restores the previous identity/history (the empty epoch record is removed). Lets an operator undo promoting the wrong node before it takes writes.
- Failover runbook documented in DESIGN.md: fence old master → gather status → `promote_checked` the most advanced → `repoint` the others. With `min_insync_replicas ≥ 1`, the most advanced in-sync replica holds every acknowledged write.

## 5. Divergence repair (Plan 3, with verify sessions)

- `DivergencePolicy::{Halt /* default */, Repair, Rebootstrap}` for automatic reconnects; explicit `repoint` follows §3 instead.
- `Slave::diverged_entries() -> Result<Vec<LogEntry>>` on a node halted `Diverged`: the local entries past the fork (if still retained), for salvage.
- Repair algorithm (requires the divergent suffix F..L still in the local changelog, and upstream retaining F):
  1. From local entries F..L, collect the affected set: keys written/deleted, ranges deleted. If any divergent entry is a CF op → fall back to full bootstrap.
  2. Open a session on the upstream pinned to one snapshot at its seq S; fetch current envelopes (or absence) for the affected keys and all keys in the affected ranges (new RPC on the session stream, alongside HashRange/ListRange).
  3. One WriteBatch: write/delete affected keys with the fetched envelopes; delete local changelog entries F..L; set last_seq = F−1, last_entry_crc = CRC of local entry F−1, origin identity + epoch history = upstream's. Node is NotReady (reads refused, downstream streams reset) until replay passes S.
  4. Tail from F; replaying F..S over the repaired keys yields byte-identical state at S (absolute ops).
  - Fallbacks to full bootstrap: suffix trimmed locally, upstream LOG_GAP at F, CF op in the suffix, repair fetch exceeds a size bound.
- The same fetch mechanism repairs keys reported by a verify mismatch (`MismatchPolicy::Repair`), replacing "automatic range repair" in DESIGN §22's future list.
- Tests: promote a behind replica → explicit repoint refuses; `repoint(.., Repair{AcceptDataLoss})` repairs without bootstrap (listener), Full verify clean; deletes and range deletes in the suffix; crash mid-repair (failpoint) then converge; fallbacks; verify-mismatch repair after injected corruption; `demote` undo path with zero loss; `promote_checked` refusal; chooser picks highest / flags diverged / rejects foreign db.

## 6. Tests required for §1–§2 (Plan 2)

- Acks: `replicas()` reports applied_seq/lag accurately while a replica lags and catches up; ack monotonicity violations rejected.
- Write concern: N=1 with no replica → NotEnoughReplicas (no seq consumed); with a replica → returns after its ack; replica killed (failpoint/kill) → AckTimeout and the entry is still replicated after the replica returns; lagging replica drops out of ISR and rejoins; master killed after an acked write → promote_checked the in-sync replica → write present.

## 7. Epoch nonces and per-open epochs (agreed 2026-10-10)

Goal: decide compatibility of two histories exactly from the epoch history alone, so a peer whose log entries were trimmed is never "unverifiable".

- **History record** becomes `EpochStart { epoch: u64, start_seq: u64, nonce: u64 }`. `nonce` is a random 64-bit value created when the epoch starts (fresh master, `bump_epoch`, promotion, and every master open — below). It is persisted with the history and carried wherever the history travels (handshake response, bootstrap header, Status RPC).
- **Ordering is decided by the u64s only:** newest epoch number, then `last_seq`. The nonce never influences which node is later.
- **The nonce only verifies identity:** two records with the same `epoch` are the same epoch iff their nonces are equal (start_seq must then also be equal). A nonce mismatch proves the histories forked at that epoch's start; the fork is the smaller of the two `start_seq`s. Collision probability 2⁻⁶⁴ per comparison.
- **Every master open starts a new epoch** (fresh nonce, `start_seq = last_seq + 1`), like a Raft term / Kafka leader epoch. Consequences:
  - Within one epoch there is exactly one writer process, so "same seq, same epoch, different content" is impossible; divergence can only begin at an epoch boundary.
  - Restoring a master from a backup no longer needs `bump_epoch_on_open`: replicas holding entries at or after the restored node's new epoch start (in an epoch the restored history lacks) are `DIVERGED` by history alone. `bump_epoch_on_open` is kept as a no-op-compatible option (documented as redundant).
  - Replicas at or behind the restart point move to the new epoch via `EPOCH_ADVANCED` without a bootstrap. History grows by one record per master open.
- **Comparisons** (`decide` rules 6–10, `admin::compare`, `check_promotion`, repair fork search) first compare histories record by record: the first epoch record whose number matches but nonce differs, or that exists in one history and not the other below both nodes' positions, determines the fork exactly. Entry-CRC probes remain as a second line of defence, never a requirement.
- **`promote_checked`:** a behind peer is compatible iff its history (with nonces) is a prefix of the candidate's up to its position; otherwise it is diverged with an exact fork, which blocks via `PromotionWouldDiscard` when the peer holds entries at or past the fork. `PromotionUnverifiable` remains only for peers whose Status could not be read (answered with an error) — still blocking.
- **Wire/format:** `EpochStart.nonce` (proto field 3), local meta history encoding; `HandshakeRequest.known_epoch_start` is superseded by sending the client's newest record `{epoch, start_seq, nonce}`. PROTOCOL_VERSION stays 1 (pre-release).
- Lands as Plan 3 Task 0b (before verify and repair), with handshake unit tests, promotion e2e (trimmed behind peer: compatible → not blocking; diverged → blocking), restore-without-bump e2e (detected DIVERGED with no bump), master restart e2e (followers EPOCH_ADVANCED, no bootstrap).
