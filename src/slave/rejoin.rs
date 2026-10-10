//! A former master becomes a slave without wiping its directory (DESIGN.md §13.2).

use super::repoint::RepointDivergence;

/// Options of [`Slave::rejoin`](crate::Slave::rejoin). The default refuses divergence: the
/// slave halts `Diverged` with its data intact.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct RejoinOptions {
    /// Decides a `DIVERGED` answer to the first handshake, exactly like
    /// `RepointOptions::on_divergence` (single-use consent).
    pub on_divergence: RepointDivergence,
}
