//! Reading a closed store without opening it for writing (DESIGN.md §13.2): the stored role
//! of a directory, and (Task 4) a fenced master's own position.

use std::path::Path;

use rocksdb::{DB, Options};

use super::meta::{CF_META, MetaSnapshot};
use crate::error::Result;

/// The stored `__meta` of the database at `path`, read through a read-only open (no LOCK, no
/// write). `None` when there is no database or it never stored a role (missing directory,
/// empty directory, or a crash during first creation).
pub(crate) fn stored_meta(path: &Path) -> Result<Option<MetaSnapshot>> {
    if !path.join("CURRENT").exists() {
        return Ok(None);
    }
    if !DB::list_cf(&Options::default(), path)?
        .iter()
        .any(|n| n == CF_META)
    {
        return Ok(None);
    }
    super::read_meta_read_only(path)
}
