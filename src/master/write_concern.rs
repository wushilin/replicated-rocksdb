//! Write concern: waiting for in-sync replicas to acknowledge a write (addendum §2).

use std::time::Duration;

use super::writer::Inner;
use crate::config::WriteConcern;
use crate::error::{Error, Result};
use crate::types::{Op, Seq};

impl Inner {
    /// How long a replica may go without being fully caught up and still count as in sync;
    /// zero (no replica is ever in sync) when this master does not serve replication.
    fn replica_lag_time_max(&self) -> Duration {
        self.cfg.serve.as_ref().map_or(Duration::ZERO, |s| s.limits.replica_lag_time_max)
    }

    /// In-sync replicas of the current epoch, and how many of them acknowledged `seq`.
    fn in_sync(&self, seq: Seq) -> (usize, usize) {
        let acks = self.replicas.acks(self.identity().epoch, self.replica_lag_time_max());
        let in_sync = acks.iter().filter(|(_, ins)| *ins).count();
        let acked = acks.iter().filter(|(applied, ins)| *ins && *applied >= seq).count();
        (in_sync, acked)
    }

    /// Commits one entry under `wc` (addendum §2). With `min_insync_replicas = N > 0`:
    /// refuses with `NotEnoughReplicas` (nothing written, no seq consumed) if fewer than N
    /// replicas are in sync; otherwise commits as usual (fsynced, writer lock released) and
    /// then waits for N in-sync replicas to acknowledge the seq, or fails with `AckTimeout`
    /// — the entry stays committed and keeps replicating.
    ///
    /// The in-sync check comes last, just before the commit and under the writer lock: a
    /// fenced writer (`WriterFenced`) and invalid input (`NoSuchCf`, `EntryTooLarge`, an
    /// invalid range, ...) report their own error, never `NotEnoughReplicas`.
    pub(crate) fn commit_with(&self, wc: &WriteConcern, build: impl FnOnce(u64) -> Result<Vec<Op>>) -> Result<Seq> {
        let required = wc.min_insync_replicas;
        if required == 0 {
            return self.commit_ops(build);
        }
        let seq = {
            let mut w = self.lock_writer()?;
            self.commit_locked(&mut w, build, |_, _| {
                let (in_sync, _) = self.in_sync(Seq::MAX);
                if in_sync < required { Err(Error::NotEnoughReplicas { required, in_sync }) } else { Ok(()) }
            })?
        };
        if self.replicas.wait_until(wc.ack_timeout, |_| self.in_sync(seq).1 >= required) {
            Ok(seq)
        } else {
            Err(Error::AckTimeout { seq, acked: self.in_sync(seq).1, required })
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::time::Duration;

    use crate::config::WriteConcern;
    use crate::error::Error;
    use crate::log::LogApi;
    use crate::server::test_client::{ack, client, eventually, next_batch, serving_master, tail_req, TailExt};
    use crate::types::CfSpec;

    fn concern(n: usize, timeout_ms: u64) -> WriteConcern {
        WriteConcern { min_insync_replicas: n, ack_timeout: Duration::from_millis(timeout_ms) }
    }

    /// A replica that stops acknowledging: the write is committed but the caller gets
    /// `AckTimeout`; the late ack still lands.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn ack_timeout_leaves_the_write_committed() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        let m = Arc::new(m);
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
        next_batch(&mut s).await;
        acks.send(ack(1)).await.unwrap();
        eventually("in sync", || m.replicas().iter().any(|r| r.in_sync)).await;

        let m2 = m.clone();
        let r = tokio::task::spawn_blocking(move || m2.put_with("c", b"k", b"v", concern(1, 300))).await.unwrap();
        assert!(matches!(r, Err(Error::AckTimeout { seq: 2, acked: 0, required: 1 })), "{r:?}");
        assert_eq!(m.last_seq(), 2, "committed despite the timeout");
        assert_eq!(next_batch(&mut s).await[0].seq, 2, "and still replicated");
        acks.send(ack(2)).await.unwrap();
        eventually("the late ack", || m.replicas()[0].applied_seq == 2).await;
    }

    /// A write with N = 1 returns as soon as the in-sync replica acknowledges it.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_returns_after_the_in_sync_ack() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        let m = Arc::new(m);
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
        next_batch(&mut s).await;
        acks.send(ack(1)).await.unwrap();
        eventually("in sync", || m.replicas().iter().any(|r| r.in_sync)).await;

        let m2 = m.clone();
        let write = tokio::task::spawn_blocking(move || m2.put_with("c", b"k", b"v", concern(1, 10_000)));
        let batch = next_batch(&mut s).await;
        assert!(!write.is_finished(), "waits for the ack");
        acks.send(ack(batch[0].seq)).await.unwrap();
        assert_eq!(write.await.unwrap().unwrap(), 2);
    }

    /// Waiting for acks does not hold the writer lock: while one write waits for its ack,
    /// another write commits and returns under its own (asynchronous) concern.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_write_commits_while_another_waits_for_acks() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        let m = Arc::new(m);
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
        next_batch(&mut s).await;
        acks.send(ack(1)).await.unwrap();
        eventually("in sync", || m.replicas().iter().any(|r| r.in_sync)).await;

        let m2 = m.clone();
        let waiting = tokio::task::spawn_blocking(move || m2.put_with("c", b"waits", b"v", concern(1, 10_000)));
        assert_eq!(next_batch(&mut s).await[0].seq, 2, "the waiting write is committed and sent");
        let m3 = m.clone();
        let other = tokio::task::spawn_blocking(move || m3.put("c", b"proceeds", b"v")).await.unwrap().unwrap();
        assert_eq!(other, 3, "committed while seq 2 waits for its ack");
        assert!(!waiting.is_finished(), "seq 2 still waits");
        acks.send(ack(2)).await.unwrap();
        assert_eq!(waiting.await.unwrap().unwrap(), 2);
    }

    /// A replica that stops being caught up leaves the ISR after `replica_lag_time_max`,
    /// then `NotEnoughReplicas` refuses writes without consuming a seq; catching up rejoins.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn a_lagging_replica_leaves_the_isr_and_rejoins() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| {
            cfg.serve.as_mut().unwrap().limits.replica_lag_time_max = Duration::from_millis(300);
        })
        .await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let mut c = client(m.serve_addr().unwrap(), "b");
        let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
        next_batch(&mut s).await;
        acks.send(ack(1)).await.unwrap();
        eventually("in sync", || m.replicas().iter().any(|r| r.in_sync)).await;

        m.put("c", b"unacked", b"v").unwrap();
        eventually("out of sync after 300 ms behind", || !m.replicas()[0].in_sync).await;
        let last = m.last_seq();
        let m = Arc::new(m);
        let m2 = m.clone();
        let r = tokio::task::spawn_blocking(move || m2.put_with("c", b"refused", b"v", concern(1, 1_000))).await.unwrap();
        assert!(matches!(r, Err(Error::NotEnoughReplicas { required: 1, in_sync: 0 })), "{r:?}");
        assert_eq!(m.last_seq(), last, "no seq consumed");

        assert_eq!(next_batch(&mut s).await[0].seq, last);
        acks.send(ack(last)).await.unwrap();
        eventually("back in sync", || m.replicas()[0].in_sync).await;
    }

    #[tokio::test]
    async fn without_replicas_a_write_concern_is_refused_and_consumes_no_seq() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let r = m.put_with("c", b"k", b"v", WriteConcern::in_sync(1));
        assert!(matches!(r, Err(Error::NotEnoughReplicas { required: 1, in_sync: 0 })), "{r:?}");
        assert_eq!(m.last_seq(), 1);
        let (embedded, _) = serving_master(tempfile::tempdir().unwrap().path(), |cfg| cfg.serve = None).await;
        embedded.create_cf(CfSpec::new("c")).unwrap();
        assert!(matches!(embedded.put_with("c", b"k", b"v", WriteConcern::in_sync(1)), Err(Error::NotEnoughReplicas { .. })));
        assert_eq!(embedded.put_with("c", b"k", b"v", WriteConcern::default()).unwrap(), 2, "N = 0 is asynchronous");
    }

    /// M4: invalid input reports its own error before the replica check.
    #[tokio::test]
    async fn invalid_input_is_reported_before_not_enough_replicas() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| cfg.max_entry_bytes = 1 << 10).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        let wc = WriteConcern::in_sync(1);
        assert!(matches!(m.put_with("nope", b"k", b"v", wc), Err(Error::NoSuchCf(_))));
        assert!(matches!(m.put_with("c", b"k", &[0; 2 << 10], wc), Err(Error::EntryTooLarge { .. })));
        let mut batch = crate::types::Batch::new();
        batch.delete_range("c", b"z", b"a");
        assert!(matches!(m.write_with(batch, wc), Err(Error::InvalidRange)));
        assert!(matches!(m.put_with("c", b"k", b"v", wc), Err(Error::NotEnoughReplicas { required: 1, in_sync: 0 })));
        assert_eq!(m.last_seq(), 1, "nothing written");
    }
}
