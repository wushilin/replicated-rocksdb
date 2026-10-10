//! A handle that holds either role and switches it explicitly (DESIGN.md §13.2).

#[allow(dead_code)] // consumed by the Node fenced open (Plan 4 Task 5); remove then
pub(crate) mod fence;
mod types;

pub use types::{AcceptSplitBrainRisk, FenceReason, NodeState, UnfenceOptions};
