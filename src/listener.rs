//! Callbacks from a slave's apply loop (DESIGN.md §15).

use crate::slave::SlaveState;
use crate::types::{LogEntry, Seq};
use crate::verify::VerifyReport;

/// Observes a slave's replication. Called inline in the apply loop, after commit and
/// before the new seq is published to `wait_for_seq` and `subscribe`: a slow listener
/// slows replication down. Must not wait on the slave's own progress
/// (for example `wait_for_seq`), which would deadlock. A panic is caught and logged; the
/// listener is then not called again until the process restarts.
///
/// Consecutive entries may be committed together (group commit): when `on_entry(k)` runs,
/// later entries of the same group may already be committed.
///
/// Delivery is at-most-once across crashes. Within a process, `on_entry` seqs are strictly
/// increasing between bootstraps and rewinds; after `on_bootstrap_complete(S)` or
/// `on_rewind(S)` delivery continues at `S + 1` (state was replaced wholesale, or in part, so
/// rebuild derived state). For at-least-once delivery keep your own cursor and use
/// `LogApi::entries_since`.
pub trait ReplicationListener: Send + Sync + 'static {
    fn on_entry(&self, _entry: &LogEntry) {}
    fn on_bootstrap_begin(&self, _snapshot_seq: Seq) {}
    fn on_bootstrap_complete(&self, _snapshot_seq: Seq) {}
    fn on_state_change(&self, _state: &SlaveState) {}
    /// After every verify run that produced a report (scheduled or `Slave::verify_now`).
    /// Mismatches are in `report.mismatches`; under `MismatchPolicy::Rebootstrap` the
    /// re-bootstrap has already been requested.
    fn on_verify_complete(&self, _report: &VerifyReport) {}
    /// A repair (DESIGN.md §12.1) rewrote some keys and moved the applied position back to
    /// `last_seq`. Entries after it are delivered again (`on_entry` continues at
    /// `last_seq + 1`); derived state of the keys involved should be rebuilt from reads once
    /// the slave is ready again.
    fn on_rewind(&self, _last_seq: Seq) {}
}
