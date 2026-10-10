//! A handle that holds either role and switches it explicitly (DESIGN.md §13.2).

mod config;
pub(crate) mod fence;
mod handle;
mod switch;
mod types;

pub use config::NodeConfig;
pub use handle::Node;
pub use types::{AcceptSplitBrainRisk, FenceReason, NodeState, UnfenceOptions};
