//! Registry of live and pending-drop user CFs.
//!
//! Every reader that may touch a physical CF (snapshots, iterators, sessions) holds an
//! `Arc<PhysCf>`. A dropped CF is physically removed only when the registry's own
//! reference is the last one (DESIGN.md §7).

use std::collections::BTreeMap;
use std::sync::{Arc, Mutex, PoisonError, RwLock, RwLockReadGuard, RwLockWriteGuard};

use super::meta::{self, key};
use super::Store;
use crate::error::Result;
use crate::types::CfEntry;

#[derive(Debug)]
pub(crate) struct PhysCf {
    pub entry: CfEntry,
    pub physical: String,
}

impl PhysCf {
    pub(crate) fn new(entry: CfEntry) -> Arc<PhysCf> {
        let physical = entry.physical_name();
        Arc::new(PhysCf { entry, physical })
    }
}

#[derive(Debug, Default)]
pub(crate) struct CfRegistry {
    live: RwLock<BTreeMap<String, Arc<PhysCf>>>,
    pub(crate) pending: Mutex<Vec<Arc<PhysCf>>>,
    /// Pending CFs taken over by a running bootstrap. They keep their original `Arc`, so
    /// readers that still hold the CF keep it from being dropped (DESIGN.md §7).
    pub(crate) held: Mutex<Vec<Arc<PhysCf>>>,
}

impl CfRegistry {
    pub(crate) fn load(live: &[CfEntry], pending: &[CfEntry]) -> Self {
        let live = live.iter().map(|e| (e.spec.name.clone(), PhysCf::new(e.clone()))).collect();
        let pending = pending.iter().cloned().map(PhysCf::new).collect();
        CfRegistry { live: RwLock::new(live), pending: Mutex::new(pending), held: Mutex::default() }
    }

    pub(crate) fn read(&self) -> RwLockReadGuard<'_, BTreeMap<String, Arc<PhysCf>>> {
        self.live.read().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn write(&self) -> RwLockWriteGuard<'_, BTreeMap<String, Arc<PhysCf>>> {
        self.live.write().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn get(&self, name: &str) -> Option<Arc<PhysCf>> {
        self.read().get(name).cloned()
    }

    /// Live CFs sorted by name.
    pub(crate) fn live_entries(&self) -> Vec<CfEntry> {
        self.read().values().map(|p| p.entry.clone()).collect()
    }

    #[cfg(test)]
    pub(crate) fn push_pending(&self, cf: Arc<PhysCf>) {
        self.pending.lock().unwrap_or_else(PoisonError::into_inner).push(cf);
    }
}

impl Store {
    /// Physically drops pending CFs that nothing references any more. Returns how many.
    pub(crate) fn drop_unreferenced_cfs(&self) -> Result<usize> {
        let mut pending = self.cfs.pending.lock().unwrap_or_else(PoisonError::into_inner);
        let mut dropped = 0;
        let mut i = 0;
        while i < pending.len() {
            // Pending CFs are not in the live map, so no new references can appear;
            // a count of 1 (ours) means no reader holds it.
            if Arc::strong_count(&pending[i]) == 1 {
                // A CF recorded but never created (crash or failure before `create_cf`)
                // is already gone.
                if self.db().cf_handle(&pending[i].physical).is_some() {
                    self.db().drop_cf(&pending[i].physical)?;
                }
                pending.swap_remove(i);
                dropped += 1;
            } else {
                i += 1;
            }
        }
        if dropped > 0 {
            let remaining: Vec<CfEntry> = pending.iter().map(|p| p.entry.clone()).collect();
            self.write_meta(&[(key::PENDING_CF_DROPS, &meta::encode_cf_list(&remaining))], true)?;
        }
        Ok(dropped)
    }
}
