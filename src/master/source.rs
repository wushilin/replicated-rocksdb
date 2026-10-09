//! The master as a replication source.

use std::time::Duration;

use tokio::sync::watch;

use super::writer::Inner;
use crate::source::LogSource;
use crate::store::Store;
use crate::types::{DbIdentity, EpochStart, Role, Seq};

impl LogSource for Inner {
    fn node_id(&self) -> &str {
        &self.cfg.identifier
    }

    fn role(&self) -> Role {
        Role::Master
    }

    fn is_ready(&self) -> bool {
        true
    }

    fn identity(&self) -> Option<(DbIdentity, Vec<EpochStart>)> {
        Some(self.identity_and_history())
    }

    fn store(&self) -> &Store {
        &self.store
    }

    fn origin_last_seq(&self) -> Seq {
        self.store.position().last_seq
    }

    fn upstream_chain(&self) -> Vec<String> {
        Vec::new()
    }

    fn max_entry_bytes(&self) -> u64 {
        self.cfg.max_entry_bytes
    }

    fn heartbeat(&self) -> Duration {
        self.cfg.heartbeat
    }

    fn subscribe(&self) -> watch::Receiver<Seq> {
        self.seq_tx.subscribe()
    }

    fn resets(&self) -> watch::Receiver<u64> {
        self.reset_tx.subscribe()
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use tonic::Code;

    use crate::config::Retention;
    use crate::log::LogApi;
    use crate::net::reason_of;
    use crate::pb;
    use crate::pb::tail_message::Kind;
    use crate::server::test_client::{client, next_batch, serving_master, tail_req, within, TailExt};
    use crate::types::CfSpec;

    /// Retention overtakes a reader that stopped consuming: the stream ends with LOG_GAP
    /// once the entries it already read are delivered.
    #[tokio::test]
    async fn slow_reader_gets_log_gap_when_retention_overtakes_it() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        // 50 MiB of log: far more than socket and HTTP/2 buffers, so the producer blocks.
        for i in 0..200u8 {
            m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
        }
        let req = tail_req(&m.identity(), 1);
        let mut s = client(m.serve_addr().unwrap(), "b").tail_start(req).await.unwrap().into_inner();
        // The producer has read ahead once the first batch arrives.
        let first = next_batch(&mut s).await;
        let mut last = first.last().unwrap().seq;
        assert_eq!(first[0].seq, 1);
        let keep_one = Retention { max_age: None, max_entries: Some(1), check_interval: Duration::from_secs(1) };
        m.inner.store.trim_changelog(0, &keep_one).unwrap();

        let err = within("the slow reader's LOG_GAP", async {
            loop {
                match s.message().await {
                    Ok(Some(pb::TailMessage { kind: Some(Kind::Batch(b)) })) => {
                        for e in b.entries {
                            assert_eq!(e.seq, last + 1);
                            last = e.seq;
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => panic!("stream ended without a status"),
                    Err(e) => return e,
                }
            }
        })
        .await;
        // Entries the producer had already read are delivered; the next read hits the gap.
        // Reads stop at 64 entries or at the default 1 MiB batch size, whichever comes first;
        // the first read also holds the small CreateCf entry 1, so reads end at 1 + k * per_read.
        let per_read = ((1usize << 20).div_ceil(first.last().unwrap().encoded_len())).min(crate::server::READ_BATCH) as u64;
        assert!(last < 200 && (last - 1) % per_read == 0, "last delivered: {last} (reads of {per_read})");
        assert_eq!((err.code(), reason_of(&err)), (Code::OutOfRange, Some(pb::Reason::LogGap)));
    }

    /// The lease protects `S + 1` until the stream has actually read it, even when the Tail
    /// starts far before the snapshot.
    #[tokio::test]
    async fn lease_is_kept_until_the_entry_after_the_snapshot_is_read() {
        use crate::pb::bootstrap_chunk::Kind as Chunk;
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        m.create_cf(CfSpec::new("c")).unwrap();
        // 130 entries of 256 KiB: the producer blocks long before it reads past entry 128.
        for i in 0..130u8 {
            m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
        }
        let mut c = client(m.serve_addr().unwrap(), "b");
        let mut boot = c.bootstrap(pb::BootstrapRequest { client_id: "b".into() }).await.unwrap().into_inner();
        let (mut lease, mut s_seq) = (Vec::new(), 0);
        within("the bootstrap", async {
            loop {
                match boot.message().await.unwrap().unwrap().kind.unwrap() {
                    Chunk::Header(h) => (lease, s_seq) = (h.lease_id, h.snapshot_seq),
                    Chunk::Records(_) => {}
                    Chunk::Footer(_) => break,
                }
            }
        })
        .await;
        for i in 0..5u8 {
            m.put("c", &[i], b"v").unwrap();
        }
        let mut req = tail_req(&m.identity(), 1);
        req.bootstrap_lease = Some(lease);
        let mut s = client(m.serve_addr().unwrap(), "b").tail_start(req).await.unwrap().into_inner();
        next_batch(&mut s).await; // the first block is read; entry S + 1 is not
        tokio::time::sleep(Duration::from_millis(200)).await;
        let keep_one = Retention { max_age: None, max_entries: Some(1), check_interval: Duration::from_secs(1) };
        m.inner.store.trim_changelog(0, &keep_one).unwrap();
        assert_eq!(m.first_retained_seq(), s_seq + 1, "the lease still holds S + 1");
    }
}
