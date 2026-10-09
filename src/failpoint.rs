//! Test-only fault injection (feature `failpoints`). Without the feature every check is a
//! constant `false` and compiles away.
//!
//! Points: `slave-after-apply` (after an entry commits, before the listener),
//! `slave-bootstrap-after-chunk` (after a bootstrap records message is written),
//! `server-tail-after-entry` (the server ends the Tail stream after sending an entry),
//! `master-bump-epoch-write` (the synced write of `bump_epoch` fails).
//!
//! Configure with `fail::cfg(name, "1*return")` to make the point fire once, or
//! `fail::cfg(name, "pause")` to block the thread there until `fail::remove(name)`.

/// True when the named point is configured to fire (`return`). A `pause` action blocks the
/// calling thread until the point is removed, then returns false.
#[cfg(feature = "failpoints")]
pub(crate) fn fire(name: &str) -> bool {
    fail::eval(name, |_| ()).is_some()
}

#[cfg(not(feature = "failpoints"))]
#[inline(always)]
pub(crate) fn fire(_name: &str) -> bool {
    false
}
