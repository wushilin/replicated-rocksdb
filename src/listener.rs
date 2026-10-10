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
/// Delivery is at-most-once across crashes. Within a process, `on_entry` seqs are strictly
/// increasing between bootstraps; after `on_bootstrap_complete(S)` delivery continues at
/// `S + 1` (state was replaced wholesale, so rebuild derived state). For at-least-once
/// delivery keep your own cursor and use `LogApi::entries_since`.
pub trait ReplicationListener: Send + Sync + 'static {
    fn on_entry(&self, _entry: &LogEntry) {}
    fn on_bootstrap_begin(&self, _snapshot_seq: Seq) {}
    fn on_bootstrap_complete(&self, _snapshot_seq: Seq) {}
    fn on_state_change(&self, _state: &SlaveState) {}
    /// After every verify run that produced a report (scheduled or `Slave::verify_now`).
    /// Mismatches are in `report.mismatches`; under `MismatchPolicy::Rebootstrap` the
    /// re-bootstrap has already been requested.
    fn on_verify_complete(&self, _report: &VerifyReport) {}
}
