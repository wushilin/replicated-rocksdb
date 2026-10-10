//! Store side of promoting a slave to master (DESIGN.md §13.1).

use rocksdb::{IteratorMode, WriteBatch, WriteOptions};

use super::Store;
use super::meta::{self, CF_CHANGELOG, CF_META, CF_TTL_INDEX, MetaSnapshot, key};
use super::ttl::index_key;
use crate::codec::envelope;
use crate::error::{Error, Result};
use crate::types::{DbIdentity, EpochStart, LogEntry, Role};

const BATCH: usize = 1_000;

impl Store {
    /// Why this (stopped) slave's directory must not become a master, read from the store
    /// itself: the final check of a promotion, which nothing can race. A persisted repair
    /// target above `last_seq` means some keys hold an upstream's state at that target and the
    /// rest an older state (DESIGN.md §12.1): a state no node ever had.
    pub(crate) fn promotion_blocker(&self) -> Result<Option<String>> {
        let last = self.position().last_seq;
        Ok(self.repair_until()?.filter(|&until| until > last).map(|until| {
            format!(
                "cannot promote while a repair replays up to seq {until} (at {last}): the data is not a consistent state yet"
            )
        }))
    }

    /// Rebuilds the derived `__ttl_index` from a full scan of the envelopes: cleared, then one
    /// entry per stored value that has an expiry. Returns how many entries were added.
    pub(crate) fn rebuild_ttl_index(&self) -> Result<u64> {
        let ttl = self.cf(CF_TTL_INDEX);
        self.delete_all_keys(&ttl)?;
        let mut batch = WriteBatch::default();
        let mut added = 0u64;
        let live: Vec<_> = self.cfs.read().values().cloned().collect();
        for phys in live {
            let handle = self.handle(&phys)?;
            for item in self.db().iterator_cf(&handle, IteratorMode::Start) {
                let (k, v) = item?;
                if let Some(at) = envelope::decode(&v)?.meta.expire_at_ms {
                    batch.put_cf(&ttl, index_key(at, &phys.entry, &k), []);
                    added += 1;
                    if batch.len() >= BATCH {
                        self.db().write(std::mem::take(&mut batch))?;
                    }
                }
            }
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        Ok(added)
    }

    /// One synced batch: role = master, a new epoch starting at `last_seq + 1`, `last_ts`
    /// from the entry at `last_seq` (0 if it is no longer retained), slave-only keys removed.
    ///
    /// The new epoch number is `1 + max(own history, own identity, max_burned_epoch, floor)`:
    /// it never reuses a number this node gave up by demoting, nor one at or below `floor`
    /// (the highest epoch the caller saw on its peers, 0 for none).
    pub(crate) fn promote_to_master(&self, floor: u64) -> Result<DbIdentity> {
        if let Some(why) = self.promotion_blocker()? {
            return Err(Error::Config(why));
        }
        let m = self
            .load_meta()?
            .ok_or_else(|| Error::corrupt("__meta is empty"))?;
        let old = m
            .identity
            .ok_or_else(|| Error::Config("this slave has no data to promote".into()))?;
        let mut history = m.epoch_history;
        let history_len = history.len();
        let own = history
            .iter()
            .map(|e| e.epoch)
            .max()
            .unwrap_or(old.epoch)
            .max(old.epoch);
        let epoch = own.max(m.max_burned_epoch).max(floor) + 1;
        history.push(EpochStart::fresh(epoch, m.last_seq + 1));
        let identity = DbIdentity {
            db_uuid: old.db_uuid,
            epoch,
        };
        let last_ts = match self
            .db()
            .get_cf(&self.cf(CF_CHANGELOG), m.last_seq.to_be_bytes())?
        {
            Some(v) => LogEntry::decode(&v)?.ts_ms,
            None => 0,
        };
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ROLE, Role::Master.as_bytes());
        batch.put_cf(
            &meta_cf,
            key::ORIGIN_IDENTITY,
            meta::encode_identity(&identity),
        );
        batch.put_cf(
            &meta_cf,
            key::EPOCH_HISTORY,
            meta::encode_epoch_history(&history),
        );
        batch.put_cf(&meta_cf, key::LAST_TS, last_ts.to_be_bytes());
        // Where `demote` returns to, however many epochs (per-open ones) are added later.
        batch.put_cf(
            &meta_cf,
            key::PROMOTED_FROM_LEN,
            (history_len as u64).to_be_bytes(),
        );
        // A repair target at or below `last_seq` was reached; a fork point is the slave's.
        for k in [
            key::NODE_STATE,
            key::MAX_ENTRY_BYTES,
            key::BOOTSTRAP_CFS,
            key::REPAIR_UNTIL,
            key::DIVERGED_AT,
        ] {
            batch.delete_cf(&meta_cf, k);
        }
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        Ok(identity)
    }
}

/// How many epoch records a demotion keeps. A promoted master returns to the history it had
/// before the promotion, removing every record added since (the promotion's own and the ones
/// per-open epochs added), provided no entry was written after the promotion started
/// (`last_seq <` the first removed record's `start_seq`). Without a promotion record (a
/// master that was never promoted) only the newest epoch is undone.
pub(crate) fn demotion_keep(m: &MetaSnapshot) -> Result<usize> {
    let len = m.epoch_history.len();
    let keep = match m.promoted_from_len {
        Some(n) if n >= 1 && n < len => n,
        _ => len.saturating_sub(1),
    };
    if keep == 0 || len < 2 {
        return Err(Error::Config(
            "nothing to undo: this is the database's first epoch".into(),
        ));
    }
    let first_removed = m.epoch_history[keep];
    if m.last_seq >= first_removed.start_seq {
        return Err(Error::Config(format!(
            "epoch {} already has entries ({}..={}); demoting would discard them",
            first_removed.epoch, first_removed.start_seq, m.last_seq
        )));
    }
    Ok(keep)
}

impl Store {
    /// Whether a demotion would be allowed now (same rule as [`Store::demote_to_slave`]).
    pub(crate) fn check_demote(&self) -> Result<()> {
        let m = self
            .load_meta()?
            .ok_or_else(|| Error::corrupt("__meta is empty"))?;
        demotion_keep(&m).map(|_| ())
    }

    /// `__meta.max_burned_epoch`: the highest epoch number this node gave up by demoting
    /// (0 if none). Reported by `Status` so promotion tooling never reuses it.
    pub(crate) fn max_burned_epoch(&self) -> Result<u64> {
        match self.db().get_cf(&self.cf(CF_META), key::MAX_BURNED_EPOCH)? {
            Some(v) => meta::be64(&v),
            None => Ok(0),
        }
    }

    /// Undoes a promotion that has not taken writes (addendum §4): one synced batch sets
    /// role = slave and restores the identity and history from before the promotion
    /// (`promoted_from_len`), removing every record added since, also the ones that restarts
    /// of the master added (see `demotion_keep`). The highest removed epoch number is
    /// **burned**: it is kept in `max_burned_epoch` (same batch), and a later promotion of
    /// this node never reuses it, because replicas that followed the undone promotion may
    /// still hold it. Refused (`Error::Config`, nothing written) if an entry was written
    /// since the promotion started, or if there is no earlier epoch to return to.
    pub(crate) fn demote_to_slave(&self) -> Result<DbIdentity> {
        let m = self
            .load_meta()?
            .ok_or_else(|| Error::corrupt("__meta is empty"))?;
        let id = m
            .identity
            .ok_or_else(|| Error::corrupt("a master without identity"))?;
        let keep = demotion_keep(&m)?;
        let mut history = m.epoch_history;
        // Every removed epoch number is burned (the highest one covers them all).
        let burned = history[keep..].iter().map(|e| e.epoch).max().unwrap_or(0);
        history.truncate(keep);
        let previous = DbIdentity {
            db_uuid: id.db_uuid,
            epoch: history.last().map_or(1, |e| e.epoch),
        };
        let meta_cf = self.cf(CF_META);
        let mut batch = WriteBatch::default();
        batch.put_cf(&meta_cf, key::ROLE, Role::Slave.as_bytes());
        batch.put_cf(
            &meta_cf,
            key::ORIGIN_IDENTITY,
            meta::encode_identity(&previous),
        );
        batch.put_cf(
            &meta_cf,
            key::EPOCH_HISTORY,
            meta::encode_epoch_history(&history),
        );
        batch.put_cf(
            &meta_cf,
            key::NODE_STATE,
            crate::store::bootstrap::NODE_READY,
        );
        batch.put_cf(
            &meta_cf,
            key::MAX_BURNED_EPOCH,
            m.max_burned_epoch.max(burned).to_be_bytes(),
        );
        batch.delete_cf(&meta_cf, key::PROMOTED_FROM_LEN);
        batch.delete_cf(&meta_cf, key::LAST_TS);
        let mut wo = WriteOptions::default();
        wo.set_sync(true);
        self.db().write_opt(batch, &wo)?;
        // Slaves keep no TTL index. Cleared after the role switch: a crash in between leaves a
        // slave with a stale index, which nothing reads and the next promotion rebuilds.
        self.delete_all_keys(&self.cf(CF_TTL_INDEX))?;
        Ok(previous)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::bootstrap::BootstrapFinish;
    use crate::store::test_support::apply;
    use crate::types::{CfEntry, CfSpec, Op};

    #[test]
    fn promotion_rewrites_meta_and_rebuilds_the_ttl_index() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path(), Role::Slave, Default::default())
                .unwrap()
                .0;
            let id = DbIdentity {
                db_uuid: uuid::Uuid::from_u128(5),
                epoch: 2,
            };
            let history = vec![
                EpochStart {
                    epoch: 1,
                    start_seq: 1,
                    nonce: 0,
                },
                EpochStart {
                    epoch: 2,
                    start_seq: 1,
                    nonce: 0,
                },
            ];
            store
                .finish_bootstrap(&BootstrapFinish {
                    identity: id,
                    epoch_history: history,
                    cf_specs: vec![],
                    snapshot_seq: 0,
                    snapshot_crc: None,
                    max_entry_bytes: 1 << 20,
                })
                .unwrap();
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new("c"),
                }],
            )
            .unwrap();
            let put = |k: &[u8], at| Op::Put {
                cf: "c".into(),
                key: k.to_vec(),
                value: vec![],
                expire_at_ms: at,
            };
            apply(
                &store,
                vec![put(b"a", Some(50)), put(b"", None), put(b"\0", Some(70))],
            )
            .unwrap();
            assert_eq!(store.rebuild_ttl_index().unwrap(), 2);
            assert_eq!(store.rebuild_ttl_index().unwrap(), 2, "idempotent");
            let due = store.ttl_due(60, 10, None).unwrap();
            assert_eq!(
                due.iter().map(|e| e.key.clone()).collect::<Vec<_>>(),
                vec![b"a".to_vec()]
            );
            let c = CfEntry {
                spec: CfSpec::new("c"),
                create_seq: 1,
            };
            assert!(
                due.iter()
                    .all(|e| e.cf_name == c.spec.name && e.create_seq == c.create_seq)
            );

            let promoted = store.promote_to_master(0).unwrap();
            assert_eq!(
                promoted,
                DbIdentity {
                    db_uuid: id.db_uuid,
                    epoch: 3
                }
            );
        }
        let (_, m) = Store::open(dir.path(), Role::Master, Default::default()).unwrap();
        assert_eq!(m.role, Role::Master);
        assert_eq!(m.identity.unwrap().epoch, 3);
        assert_eq!(
            m.epoch_history.last().map(|e| (e.epoch, e.start_seq)),
            Some((3, 3))
        );
        assert_eq!(
            m.last_ts, 2_000,
            "ts of entry 2 (test_support stamps 1000 × seq)"
        );
        assert_eq!((m.node_state, m.max_entry_bytes), (None, None));
    }

    #[test]
    fn demotion_undoes_an_unused_promotion_only() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path(), Role::Slave, Default::default())
                .unwrap()
                .0;
            let id = DbIdentity {
                db_uuid: uuid::Uuid::from_u128(5),
                epoch: 1,
            };
            store
                .finish_bootstrap(&BootstrapFinish {
                    identity: id,
                    epoch_history: vec![EpochStart {
                        epoch: 1,
                        start_seq: 1,
                        nonce: 0,
                    }],
                    cf_specs: vec![],
                    snapshot_seq: 0,
                    snapshot_crc: None,
                    max_entry_bytes: 1 << 20,
                })
                .unwrap();
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new("c"),
                }],
            )
            .unwrap();
            store.promote_to_master(0).unwrap();
        }
        {
            let store = Store::open(dir.path(), Role::Master, Default::default())
                .unwrap()
                .0;
            assert_eq!(
                store.demote_to_slave().unwrap(),
                DbIdentity {
                    db_uuid: uuid::Uuid::from_u128(5),
                    epoch: 1
                }
            );
        }
        let (store, m) = Store::open(dir.path(), Role::Slave, Default::default()).unwrap();
        assert_eq!(
            m.epoch_history,
            vec![EpochStart {
                epoch: 1,
                start_seq: 1,
                nonce: 0,
            }]
        );
        assert_eq!(m.last_seq, 1);
        assert_eq!(m.max_burned_epoch, 2, "the undone epoch number is burned");
        assert!(
            matches!(store.demote_to_slave(), Err(Error::Config(_))),
            "first epoch: nothing to undo"
        );
    }

    /// Epoch numbers are never reused: a promotion after a demotion skips the burned number,
    /// and a caller-supplied floor (the peers' highest epoch) is honoured too.
    #[test]
    fn a_burned_epoch_and_the_peer_floor_are_never_reused() {
        let dir = tempfile::tempdir().unwrap();
        {
            let store = Store::open(dir.path(), Role::Slave, Default::default())
                .unwrap()
                .0;
            let id = DbIdentity {
                db_uuid: uuid::Uuid::from_u128(5),
                epoch: 1,
            };
            store
                .finish_bootstrap(&BootstrapFinish {
                    identity: id,
                    epoch_history: vec![EpochStart {
                        epoch: 1,
                        start_seq: 1,
                        nonce: 0,
                    }],
                    cf_specs: vec![],
                    snapshot_seq: 0,
                    snapshot_crc: None,
                    max_entry_bytes: 1 << 20,
                })
                .unwrap();
            apply(
                &store,
                vec![Op::CreateCf {
                    spec: CfSpec::new("c"),
                }],
            )
            .unwrap();
            assert_eq!(store.promote_to_master(0).unwrap().epoch, 2);
            assert_eq!(store.demote_to_slave().unwrap().epoch, 1);
            assert_eq!(
                store.promote_to_master(0).unwrap().epoch,
                3,
                "epoch 2 is burned"
            );
            assert_eq!(store.demote_to_slave().unwrap().epoch, 1);
            assert_eq!(store.load_meta().unwrap().unwrap().max_burned_epoch, 3);
            assert_eq!(
                store.promote_to_master(7).unwrap().epoch,
                8,
                "above every epoch a peer reported"
            );
        }
    }

    #[test]
    fn a_slave_without_data_cannot_be_promoted() {
        let dir = tempfile::tempdir().unwrap();
        let store = Store::open(dir.path(), Role::Slave, Default::default())
            .unwrap()
            .0;
        assert!(matches!(store.promote_to_master(0), Err(Error::Config(_))));
    }
}
