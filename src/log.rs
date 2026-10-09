//! Public access to a node's changelog.

use tokio::sync::watch;

use crate::error::Result;
use crate::types::Seq;

pub use crate::store::log::EntryIter;

pub trait LogApi {
    /// Last committed (master) or applied (slave) seq.
    fn last_seq(&self) -> Seq;
    /// Lowest seq still in the changelog, or `last_seq + 1` if it is empty.
    fn first_retained_seq(&self) -> Seq;
    /// Entries with seq greater than `seq`, in order. `Err(Gap)` if any were trimmed;
    /// resync with a full read in that case.
    fn entries_since(&self, seq: Seq) -> Result<EntryIter<'_>>;
    /// Receives the latest committed seq after each commit.
    fn subscribe(&self) -> watch::Receiver<Seq>;
}
