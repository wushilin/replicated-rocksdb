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
//!
//! # One handle for either role: `Node`
//!
//! [`Node`] holds a database directory as master or slave and changes role only through
//! explicit calls (`promote_checked`, `demote`, `rejoin_as_slave`, ...), so an `Arc<Node>`
//! shared by the application always points at the current role. A directory whose stored role
//! is `master` opens **fenced** (no epoch, no port, no reads or writes) until a peer check
//! proves no other master exists ([`NodeConfig::peers`]) or the caller passes
//! [`AcceptSplitBrainRisk`]. A former master rejoins as a slave of the new master without a
//! wipe ([`Slave::rejoin`]); entries only it holds are discarded only with [`AcceptDataLoss`].
//!
//! ```no_run
//! # use replicated_rocksdb::{Node, NodeConfig, NodeState, RejoinOptions, Result};
//! # async fn f(cfg: NodeConfig) -> Result<()> {
//! let node = std::sync::Arc::new(Node::open(cfg).await?);
//! if let NodeState::Fenced { reason } = node.state() {
//!     eprintln!("not serving: {reason}");
//!     node.rejoin_as_slave(None, RejoinOptions::default()).await?; // refuses divergence
//! }
//! # Ok(()) }
//! ```

#![forbid(unsafe_op_in_unsafe_fn)]

pub(crate) mod net;
pub(crate) mod proto;

pub(crate) use proto::replication::v1 as pb;

pub mod admin;
pub mod auth;
pub(crate) mod background;
pub mod clock;
pub mod config;
pub mod error;
pub mod log;
mod master;
mod node;
pub mod read;
pub mod tuning;
pub mod types;

pub use clock::{Clock, ManualClock, SystemClock};
pub use error::{Error, Result};
pub use types::{
    Batch, CfSpec, DbIdentity, Direction, EpochStart, IterMode, LogEntry, Op, Role, Seq, ValueMeta,
};

#[doc(hidden)]
pub mod codec;
pub(crate) mod source;
pub(crate) mod store;
pub(crate) mod verify;
pub use codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
pub use read::{ReadApi, ReadSnapshot};
pub use store::read::DbIter;
pub use tuning::{CfKind, ComparatorRegistry, Compression, RocksTuning, Tunable};
pub use verify::{
    CfVersion, Mismatch, MismatchPolicy, VerifyAbort, VerifyConfig, VerifyMode, VerifyReport,
};

pub use auth::{
    AllowAll, AllowMethods, AuthError, Authenticator, CredentialsProvider, Method,
    MtlsSubjectAllowList, NoCredentials, PeerInfo, TokenAuthenticator, TokenCredentials,
};
pub use config::{
    MAX_BOOTSTRAP_CHUNK_BYTES, MasterConfig, ReaperConfig, Retention, ServeConfig, ServeLimits,
    TlsClientConfig, TlsIdentity, TlsServerConfig, WriteConcern,
};
pub use log::{EntryIter, LogApi};
pub use master::{DemoteRefused, Master, ReapStats};
pub use server::replicas::{FetchSettings, ReplicaStatus};
pub mod listener;
mod slave;

pub use listener::ReplicationListener;
pub use node::{AcceptSplitBrainRisk, FenceReason, Node, NodeConfig, NodeState, UnfenceOptions};
pub use slave::{
    AcceptDataLoss, Backoff, DivergencePolicy, FetchConfig, GapPolicy, HaltAction, HaltReason,
    IdentityPolicy, PromoteRefused, PromotionCheck, RejoinOptions, RepointDivergence,
    RepointOptions, RepointOutcome, Slave, SlaveConfig, SlaveState, SlaveStatus, UpstreamConfig,
    UpstreamErrorKind, WaitProgress,
};
pub(crate) mod failpoint;
pub(crate) mod server;
