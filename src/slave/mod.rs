//! A read-only replica.

mod config;
mod policy;
mod state;

pub use config::{Backoff, DivergencePolicy, FetchConfig, GapPolicy, IdentityPolicy, SlaveConfig, UpstreamConfig};
pub use state::{HaltAction, HaltReason, SlaveState, SlaveStatus};
