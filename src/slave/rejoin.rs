//! A former master becomes a slave without wiping its directory (DESIGN.md §13.2).

use super::repoint::RepointDivergence;
use super::{RepointOutcome, Slave, SlaveConfig};
use crate::error::{Error, Result};
use crate::store::Store;
use crate::store::offline::stored_meta;
use crate::types::Role;

/// Options of [`Slave::rejoin`](crate::Slave::rejoin). The default refuses divergence: the
/// slave halts `Diverged` with its data intact.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RejoinOptions {
    /// Decides a `DIVERGED` answer to the first handshake, exactly like
    /// `RepointOptions::on_divergence` (single-use consent).
    pub on_divergence: RepointDivergence,
}

impl Slave {
    /// Turns a stopped master's directory into a slave of `cfg.upstream` **without wiping
    /// it** (DESIGN.md §13.2). Explicit operator action; nothing calls it automatically.
    ///
    /// 1. Refuses, leaving the directory unchanged, unless the stored role is `master`
    ///    (`Err(WrongRole)`; no database: `Err(Config)`), or when `cfg` is invalid
    ///    (`Err(Config)`) or the directory cannot be opened (for example a `Master` of this
    ///    process still holds it).
    /// 2. One synced batch: `role = slave`; trailing epochs without entries (restarts that took
    ///    no writes, an empty promotion epoch) are dropped and their highest number burned into
    ///    `max_burned_epoch`, as `Master::demote` does; `origin_identity` follows the newest
    ///    remaining record (same batch); master-only keys (`__ttl_index`, `promoted_from_len`,
    ///    `last_ts`) are removed. The role is persisted before the handshake: a slave never
    ///    writes and divergence halts by default, and consent is never persisted, so a crash
    ///    after the batch reopens as a slave that halts `Diverged` if it diverged.
    /// 3. Opens the slave and returns once its first handshake with `cfg.upstream` settles,
    ///    under `opts.on_divergence` (never `cfg.on_divergence`) and with the outcomes of
    ///    [`Slave::repoint`]: `Ok(EpochAdvanced)` when the old master's log is a prefix of the
    ///    new master's (after a failover the new master always runs a newer epoch, so this is
    ///    the usual "resumed" answer), `Resumed`, `Bootstrapped`, `Repaired { discarded }`,
    ///    `Retrying { .. }`, or `Err(Diverged { .. })` under `RepointDivergence::Refuse` (the
    ///    default) with the slave halted `Diverged`, its data intact and
    ///    [`diverged_entries`](Self::diverged_entries) available. The consent in `opts` is
    ///    single-use, as for `repoint`.
    ///
    /// **Return value:** the outer `Err` means no slave was opened: a refusal of step 1 (the
    /// directory is unchanged and still a master), or a failure after step 2 (the directory
    /// is now a slave; open it with `Slave::open`). Otherwise the slave is returned with the
    /// first handshake's result, which may itself be an error: a halted slave is never lost.
    pub async fn rejoin(
        cfg: SlaveConfig,
        opts: RejoinOptions,
    ) -> Result<(Slave, Result<RepointOutcome>)> {
        cfg.validate()?;
        let (path, tuning, node) = (
            cfg.path.clone(),
            cfg.rocksdb.clone(),
            cfg.identifier.clone(),
        );
        let rejoined = crate::background::blocking(move || {
            match stored_meta(&path)? {
                None => {
                    return Err(Error::Config(format!(
                        "{} holds no database to rejoin",
                        path.display()
                    )));
                }
                Some(m) if m.role != Role::Master => {
                    return Err(Error::WrongRole {
                        expected: Role::Master,
                        found: m.role,
                    });
                }
                Some(_) => {}
            }
            // A plain store open: `Master::open` would start a per-open epoch. The master
            // store is dropped (closed) here; the directory is reopened as a slave below.
            let (store, _) = Store::open(&path, Role::Master, tuning)?;
            store.rejoin_as_slave()
        })
        .await?;
        tracing::info!(
            node = %node,
            epoch = rejoined.identity.map(|i| i.epoch),
            burned = rejoined.burned,
            "former master rewritten as a slave"
        );
        // Test hook: the rewrite is committed, the slave is not opened yet (a crash here).
        crate::failpoint::io_error("rejoin-after-rewrite")?;
        let (slave, settled) = Slave::start(cfg, Some(opts.on_divergence)).await?;
        let settled = settled.expect("armed above");
        // The sender is dropped only when the slave shuts down before the handshake settles.
        let outcome = settled.await.unwrap_or(Err(Error::RepointSuperseded));
        Ok((slave, outcome))
    }
}
