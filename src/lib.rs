//! Single-master, async log-shipping replication for RocksDB.
//!
//! A [`Master`] is the only writer. Every write becomes one fsynced, checksummed log entry
//! of absolute operations (`Put`, `Delete`, `DeleteRange`, `CreateCf`, `DropCf`), which
//! read-only slaves replay in order. See `DESIGN.md` and `PROTOCOL.md`.
//!
//! # Read-modify-write is the application's job
//!
//! This crate has no `update`, compare-and-set or merge. The writer lock serialises
//! individual write calls only, so **between a `get` and a `put`, another writer can change
//! the key, and that change will be lost.** To derive a new value from an old one (counters,
//! appends, "insert if absent"), hold your own lock around the whole sequence:
//!
//! ```no_run
//! # use replicated_rocksdb::{Master, Result};
//! # use std::sync::Mutex;
//! # fn f(master: &Master, lock: &Mutex<()>) -> Result<()> {
//! let _guard = lock.lock().unwrap();               // app-level lock: global, per key, or striped
//! let old = master.get("counters", b"hits")?;
//! let n = old.map_or(0, |v| u64::from_be_bytes(v.try_into().unwrap())) + 1;
//! master.put("counters", b"hits", &n.to_be_bytes())?; // logged as an absolute Put
//! # Ok(()) }
//! ```
//!
//! Every writer of those keys must take the same lock. Because there is exactly one master
//! process, an in-process lock is enough.

#![forbid(unsafe_op_in_unsafe_fn)]

#[allow(dead_code)] // generated types are unused until later tasks
pub(crate) mod proto;

pub(crate) use proto::replication::v1 as pb;

pub mod clock;
pub mod config;
pub mod log;
mod master;
pub mod error;
pub mod tuning;
pub mod read;
pub mod types;

pub use clock::{Clock, ManualClock, SystemClock};
pub use error::{Error, Result};
pub use types::{Batch, CfSpec, DbIdentity, Direction, EpochStart, IterMode, LogEntry, Op, Role, Seq, ValueMeta};

#[doc(hidden)]
pub mod codec;
pub(crate) mod store;
pub use codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
pub use tuning::{CfKind, ComparatorRegistry, Compression, RocksTuning, Tunable};
pub use read::{ReadApi, ReadSnapshot};
pub use store::read::DbIter;

pub use config::{MasterConfig, ReaperConfig, Retention};
pub use log::{EntryIter, LogApi};
pub use master::{Master, ReapStats};
