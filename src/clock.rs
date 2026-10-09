//! Time source. Injected so tests can control TTL expiry and retention.

use std::sync::atomic::{AtomicU64, Ordering};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub trait Clock: Send + Sync + 'static {
    /// Milliseconds since the Unix epoch.
    fn now_ms(&self) -> u64;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SystemClock;

impl Clock for SystemClock {
    fn now_ms(&self) -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| u64::try_from(d.as_millis()).unwrap_or(u64::MAX))
            .unwrap_or(0)
    }
}

/// A clock that changes only when told to. For tests.
#[derive(Debug, Default)]
pub struct ManualClock(AtomicU64);

impl ManualClock {
    pub fn new(now_ms: u64) -> Self {
        ManualClock(AtomicU64::new(now_ms))
    }

    pub fn set(&self, now_ms: u64) {
        self.0.store(now_ms, Ordering::SeqCst);
    }

    pub fn advance(&self, by: Duration) {
        let by = u64::try_from(by.as_millis()).unwrap_or(u64::MAX);
        let mut cur = self.0.load(Ordering::SeqCst);
        loop {
            match self.0.compare_exchange_weak(cur, cur.saturating_add(by), Ordering::SeqCst, Ordering::SeqCst) {
                Ok(_) => break,
                Err(actual) => cur = actual,
            }
        }
    }
}

impl Clock for ManualClock {
    fn now_ms(&self) -> u64 {
        self.0.load(Ordering::SeqCst)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    #[test]
    fn manual_clock_moves_only_when_told() {
        let c = ManualClock::new(10);
        assert_eq!(c.now_ms(), 10);
        c.advance(Duration::from_millis(5));
        assert_eq!(c.now_ms(), 15);
        c.set(3);
        assert_eq!(c.now_ms(), 3);
    }

    #[test]
    fn advance_saturates_instead_of_overflowing() {
        let c = ManualClock::new(7);
        c.advance(Duration::MAX);
        assert_eq!(c.now_ms(), u64::MAX);
    }

    #[test]
    fn system_clock_is_after_2020() {
        assert!(SystemClock.now_ms() > 1_577_836_800_000);
    }
}
