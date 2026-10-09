//! Single-master, async log-shipping replication for RocksDB.

#![forbid(unsafe_op_in_unsafe_fn)]

#[allow(dead_code)] // generated types are unused until later tasks
pub(crate) mod proto;

#[allow(unused_imports)]
pub(crate) use proto::replication::v1 as pb;

pub mod clock;
pub mod error;
pub mod tuning;
pub mod types;

pub use clock::{Clock, ManualClock, SystemClock};
pub use error::{Error, Result};
pub use types::{Batch, CfSpec, DbIdentity, Direction, EpochStart, IterMode, LogEntry, Op, Role, Seq, ValueMeta};

pub mod codec;
#[allow(dead_code)] // consumed by the master and slave in later tasks
pub(crate) mod store;
pub use codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
pub use tuning::{CfKind, ComparatorRegistry, Compression, RocksTuning, Tunable};
