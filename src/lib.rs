//! Single-master, async log-shipping replication for RocksDB.

#![forbid(unsafe_op_in_unsafe_fn)]

#[allow(dead_code)] // generated types are unused until later tasks
pub(crate) mod proto;

#[allow(unused_imports)]
pub(crate) use proto::replication::v1 as pb;
