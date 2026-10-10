//! Server behaviour against a real master, through a bare protocol client.

use std::sync::Arc;
use std::time::{Duration, Instant};

use tonic::Code;

use super::test_client::{
    Meta, TailExt, ack, client, client_with, eventually, hello, next_batch, serving_master,
    stream_error, tail_req, within,
};
use crate::auth::{AllowMethods, Method, TokenAuthenticator};
use crate::codec::digest::Digest;
use crate::codec::{PROTOCOL_VERSION, envelope};
use crate::log::LogApi;
use crate::net::convert::{epochs_to_pb, identity_to_pb};
use crate::net::reason_of;
use crate::pb;
use crate::pb::bootstrap_chunk::Kind as Chunk;
use crate::pb::tail_message::Kind as Msg;
use crate::types::{CfSpec, DbIdentity, LogEntry};

/// The next entry of a real-time stream: one entry per batch when entries are written one
/// at a time after the stream caught up.
async fn next_entry(s: &mut tonic::Streaming<pb::TailMessage>) -> LogEntry {
    let mut batch = next_batch(s).await;
    assert_eq!(
        batch.len(),
        1,
        "real-time batches carry the one entry that became available"
    );
    batch.remove(0)
}

#[tokio::test]
async fn handshake_reports_the_master_state() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let r = c.handshake(hello("b")).await.unwrap().into_inner();
    assert_eq!(r.server_id, "a");
    assert_eq!(
        (r.decision(), r.reason()),
        (pb::Decision::Bootstrap, pb::Reason::NoLocalState)
    );
    assert_eq!(r.identity, Some(identity_to_pb(&m.identity())));
    assert_eq!(r.epoch_history, epochs_to_pb(&m.epoch_history()));
    assert_eq!(
        (r.epoch_history[0].epoch, r.epoch_history[0].start_seq),
        (1, 1)
    );
    assert_eq!(
        (r.first_retained_seq, r.last_seq, r.origin_last_seq),
        (1, 2, 2)
    );
    assert_eq!((r.max_entry_bytes, r.heartbeat_ms), (16 << 20, 100));
    assert!(r.upstream_chain.is_empty());
    assert_eq!(r.cf_specs.len(), 1);

    let mut known = hello("b");
    known.known_identity = Some(identity_to_pb(&m.identity()));
    known.last_seq = 2;
    known.last_crc32c = m
        .entries_since(1)
        .unwrap()
        .next()
        .unwrap()
        .ok()
        .map(|e| e.crc32c);
    let r = c.handshake(known).await.unwrap().into_inner();
    assert_eq!(
        (r.decision(), r.reason()),
        (pb::Decision::Resume, pb::Reason::Unspecified)
    );
}

#[tokio::test]
async fn version_mismatch_is_answered_in_the_handshake_body() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    // No protocol metadata at all: Handshake still answers.
    let mut c = client_with(
        m.serve_addr().unwrap(),
        Meta {
            node_id: "b",
            token: None,
            version: None,
        },
    );
    let mut req = hello("b");
    req.protocol_version = PROTOCOL_VERSION + 1;
    let r = c.handshake(req).await.unwrap().into_inner();
    assert_eq!(
        (r.decision(), r.reason()),
        (pb::Decision::Reject, pb::Reason::ProtocolMismatch)
    );
    assert_eq!(r.protocol_version, PROTOCOL_VERSION);
    // Every other RPC fails without the metadata.
    let err = c.tail_start(tail_req(&m.identity(), 1)).await.unwrap_err();
    assert_eq!(
        (err.code(), reason_of(&err)),
        (Code::FailedPrecondition, Some(pb::Reason::ProtocolMismatch))
    );
}

#[tokio::test]
async fn client_id_must_match_the_node_id_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let err = client(m.serve_addr().unwrap(), "b")
        .handshake(hello("someone-else"))
        .await
        .unwrap_err();
    assert_eq!(err.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn malformed_handshake_input_is_invalid_argument() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let addr = m.serve_addr().unwrap();
    let mut c = client(addr, "b");

    let mut short_uuid = hello("b");
    short_uuid.known_identity = Some(pb::Identity {
        db_uuid: vec![1; 15],
        epoch: 1,
    });
    let e = c.handshake(short_uuid).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "15-byte db_uuid");
    assert_eq!(
        reason_of(&e),
        Some(pb::Reason::Unspecified),
        "INVALID_ARGUMENT carries ErrorInfo"
    );

    // known_epochs: not increasing, or not ending with known_identity's epoch.
    let ep = |epoch, start_seq| pb::EpochStart {
        epoch,
        start_seq,
        nonce: 1,
    };
    for (bad, what) in [
        (vec![ep(2, 5), ep(1, 1)], "unsorted"),
        (vec![ep(1, 1), ep(1, 1)], "duplicate"),
        (vec![ep(1, 1), ep(2, 5)], "not the identity's epoch"),
    ] {
        let mut r = hello("b");
        r.known_identity = Some(identity_to_pb(&m.identity()));
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.known_epochs = bad;
        let e = c.handshake(r).await.unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "known_epochs {what}");
    }

    // An empty client_id (the node-id metadata is "b", so it would also mismatch).
    assert_eq!(
        c.handshake(hello("")).await.unwrap_err().code(),
        Code::InvalidArgument,
        "empty client_id"
    );

    // client_id and node id agree but are not valid ids.
    let long: &'static str = Box::leak("x".repeat(129).into_boxed_str());
    for bad in ["has space", "semi;colon", "a/b", long] {
        let mut c = client(addr, bad);
        let e = c.handshake(hello(bad)).await.unwrap_err();
        assert_eq!(e.code(), Code::InvalidArgument, "client_id {bad:?}");
    }

    // The node-id metadata is checked on every RPC.
    let mut c = client(addr, "bad id");
    let e = c.tail_start(tail_req(&m.identity(), 1)).await.unwrap_err();
    assert_eq!(
        e.code(),
        Code::InvalidArgument,
        "tail with a bad x-rrdb-node-id"
    );
    let e = c
        .bootstrap(pb::BootstrapRequest {
            client_id: "bad id".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Code::InvalidArgument,
        "bootstrap with a bad x-rrdb-node-id"
    );

    // Tail and Bootstrap also require client_id to equal the metadata.
    let mut c = client(addr, "b");
    let mut req = tail_req(&m.identity(), 1);
    req.client_id = "other".into();
    assert_eq!(
        c.tail_start(req).await.unwrap_err().code(),
        Code::InvalidArgument,
        "tail client_id mismatch"
    );
    let e = c
        .bootstrap(pb::BootstrapRequest {
            client_id: "other".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Code::InvalidArgument,
        "bootstrap client_id mismatch"
    );
}

#[tokio::test]
async fn tail_streams_entries_in_order_then_heartbeats_then_new_entries() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"", b"").unwrap();
    m.put("c", b"\0", b"\0").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let mut s = c
        .tail_start(tail_req(&m.identity(), 1))
        .await
        .unwrap()
        .into_inner();
    // The backlog arrives as one batch.
    let backlog = next_batch(&mut s).await;
    assert_eq!(
        backlog.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![1, 2, 3]
    );
    backlog.iter().for_each(|e| e.verify_crc().unwrap());
    // Idle: heartbeats only, carrying the server's position.
    match within("a heartbeat", s.message())
        .await
        .unwrap()
        .unwrap()
        .kind
        .unwrap()
    {
        Msg::Heartbeat(h) => assert_eq!((h.upstream_last_seq, h.origin_last_seq), (3, 3)),
        Msg::Batch(b) => panic!("unexpected batch of {}", b.entries.len()),
    }
    m.put("c", b"k", b"v").unwrap();
    assert_eq!(next_entry(&mut s).await.seq, 4);
}

#[tokio::test]
async fn tail_validation_follows_the_spec() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    eventually("retention", || m.first_retained_seq() == 6).await;
    let id = m.identity();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let reason = |e: tonic::Status| (e.code(), reason_of(&e));

    let other = DbIdentity {
        db_uuid: uuid::Uuid::new_v4(),
        epoch: 1,
    };
    let e = c.tail_start(tail_req(&other, 6)).await.unwrap_err();
    assert_eq!(
        reason(e),
        (Code::FailedPrecondition, Some(pb::Reason::IdentityChanged))
    );

    let e = c.tail_start(tail_req(&id, 8)).await.unwrap_err();
    assert_eq!(
        reason(e),
        (Code::FailedPrecondition, Some(pb::Reason::Diverged)),
        "past master last_seq + 1"
    );

    let mut bad_prev = tail_req(&id, 7);
    bad_prev.prev_crc32c = Some(0xDEAD_BEEF);
    let e = c.tail_start(bad_prev).await.unwrap_err();
    assert_eq!(
        reason(e),
        (Code::FailedPrecondition, Some(pb::Reason::Diverged))
    );

    let e = c.tail_start(tail_req(&id, 2)).await.unwrap_err();
    assert_eq!(reason(e), (Code::OutOfRange, Some(pb::Reason::LogGap)));

    let e = c.tail_start(tail_req(&id, 0)).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);

    // from_seq = last_seq + 1 is valid: the stream waits for the next write.
    let mut s = c.tail_start(tail_req(&id, 7)).await.unwrap().into_inner();
    m.put("c", b"x", b"v").unwrap();
    assert_eq!(next_entry(&mut s).await.seq, 7);
}

#[tokio::test]
async fn bump_epoch_ends_streams_with_upstream_reset() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(tail_req(&m.identity(), 1))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next_entry(&mut s).await.seq, 1);
    m.bump_epoch().unwrap();
    let err = stream_error(&mut s).await;
    assert_eq!(
        (err.code(), reason_of(&err)),
        (Code::Unavailable, Some(pb::Reason::UpstreamReset))
    );
}

#[tokio::test]
async fn shutdown_ends_streams_and_releases_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(tail_req(&m.identity(), 1))
        .await
        .unwrap()
        .into_inner();
    assert_eq!(next_entry(&mut s).await.seq, 1);
    m.shutdown().await.unwrap();
    let err = stream_error(&mut s).await;
    assert_eq!(
        (err.code(), reason_of(&err)),
        (Code::Unavailable, Some(pb::Reason::ShuttingDown))
    );
    // The directory can be reopened immediately.
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    assert_eq!(m.last_seq(), 1);
}

#[tokio::test]
async fn embedded_master_does_not_serve() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.serve = None).await;
    assert!(m.serve_addr().is_none());
}

#[tokio::test]
async fn failed_authentications_are_rate_limited_per_address() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let serve = cfg.serve.as_mut().unwrap();
        serve.authenticator = Arc::new(TokenAuthenticator::new(["good"]));
        serve.allow_plaintext_token = true;
        serve.limits.max_auth_failures_per_minute = 3;
    })
    .await;
    let addr = m.serve_addr().unwrap();
    let meta = |token| Meta {
        node_id: "b",
        token: Some(token),
        version: Some(PROTOCOL_VERSION),
    };
    client_with(addr, meta("good"))
        .handshake(hello("b"))
        .await
        .unwrap();
    for _ in 0..3 {
        let e = client_with(addr, meta("bad"))
            .handshake(hello("b"))
            .await
            .unwrap_err();
        assert_eq!(e.code(), Code::Unauthenticated);
        assert_eq!(reason_of(&e), None, "auth failures carry no ErrorInfo");
    }
    // Throttled without checking the token, with a retryable BUSY: never UNAUTHENTICATED, which
    // would halt a correctly configured replica behind the same address.
    let e = client_with(addr, meta("good"))
        .handshake(hello("b"))
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Code::ResourceExhausted,
        "blocked without checking the token"
    );
    assert_eq!(reason_of(&e), Some(pb::Reason::Busy));
}

#[tokio::test]
async fn methods_can_be_denied_per_credential() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let serve = cfg.serve.as_mut().unwrap();
        let token = Arc::new(TokenAuthenticator::new(["t"]));
        serve.authenticator =
            Arc::new(AllowMethods::new(token, &[Method::Handshake, Method::Tail]));
        serve.allow_plaintext_token = true;
    })
    .await;
    let mut c = client_with(
        m.serve_addr().unwrap(),
        Meta {
            node_id: "b",
            token: Some("t"),
            version: Some(PROTOCOL_VERSION),
        },
    );
    c.handshake(hello("b")).await.unwrap();
    let e = c
        .bootstrap(pb::BootstrapRequest {
            client_id: "b".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::PermissionDenied);
    let e = c
        .status(pb::StatusRequest {
            client_id: "b".into(),
            probe_seq: None,
        })
        .await
        .unwrap_err();
    assert_eq!(
        e.code(),
        Code::PermissionDenied,
        "Status is a method of its own"
    );
}

#[tokio::test]
async fn status_and_tail_reject_a_client_id_that_differs_from_the_metadata() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let mut c = client(m.serve_addr().unwrap(), "b");
    let e = c
        .status(pb::StatusRequest {
            client_id: "x".into(),
            probe_seq: None,
        })
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    let mut req = tail_req(&m.identity(), 1);
    req.client_id = "x".into();
    let e = c.tail_start(req).await.unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    assert_eq!(
        c.status(pb::StatusRequest {
            client_id: "b".into(),
            probe_seq: None
        })
        .await
        .unwrap()
        .into_inner()
        .node_id,
        "a"
    );
}

/// Reads a whole bootstrap stream: (header, records per message, footer).
async fn read_bootstrap(
    c: &mut super::test_client::Client,
) -> (
    pb::BootstrapHeader,
    Vec<pb::BootstrapRecords>,
    pb::BootstrapFooter,
) {
    within("a whole bootstrap stream", async {
        let mut s = c
            .bootstrap(pb::BootstrapRequest {
                client_id: "b".into(),
            })
            .await
            .unwrap()
            .into_inner();
        let Chunk::Header(h) = s.message().await.unwrap().unwrap().kind.unwrap() else {
            panic!("header first")
        };
        let mut msgs = Vec::new();
        loop {
            match s.message().await.unwrap().unwrap().kind.unwrap() {
                Chunk::Records(r) => msgs.push(r),
                Chunk::Footer(f) => return (h, msgs, f),
                Chunk::Header(_) => panic!("second header"),
            }
        }
    })
    .await
}

#[tokio::test]
async fn bootstrap_streams_a_consistent_snapshot_with_a_valid_digest() {
    let dir = tempfile::tempdir().unwrap();
    let (m, clock) = serving_master(dir.path(), |cfg| {
        cfg.serve.as_mut().unwrap().limits.bootstrap_chunk_bytes = 40;
    })
    .await;
    m.create_cf(CfSpec::new("b-empty")).unwrap();
    m.create_cf(CfSpec::new("a")).unwrap();
    m.put("a", b"", b"").unwrap();
    m.put("a", b"\0", b"\0\0").unwrap();
    m.put_with_ttl("a", b"ttl", b"gone", Duration::from_secs(1))
        .unwrap();
    for i in 0..10u8 {
        m.put("a", &[b'k', i], &[i; 20]).unwrap();
    }
    clock.advance(Duration::from_secs(5)); // "ttl" is expired but not reaped: still sent
    let mut c = client(m.serve_addr().unwrap(), "b");
    let (h, msgs, f) = read_bootstrap(&mut c).await;

    assert_eq!(h.snapshot_seq, m.last_seq());
    assert_eq!(
        h.snapshot_crc32c,
        m.entries_since(m.last_seq() - 1)
            .unwrap()
            .next()
            .unwrap()
            .ok()
            .map(|e| e.crc32c)
    );
    assert_eq!(
        h.cf_specs
            .iter()
            .map(|c| c.name.as_str())
            .collect::<Vec<_>>(),
        vec!["a", "b-empty"]
    );
    assert_eq!(h.identity, Some(identity_to_pb(&m.identity())));
    assert_eq!(h.lease_id.len(), 16);
    assert_eq!(h.lease_ttl_ms, 15 * 60 * 1000);
    assert!(msgs.len() > 1, "small chunk size splits the CF");
    assert!(msgs.iter().all(|r| r.cf == "a" && !r.records.is_empty()));

    let mut d = Digest::new();
    for spec in &h.cf_specs {
        d.cf_marker(&spec.name, spec.create_seq);
        for r in msgs.iter().filter(|r| r.cf == spec.name) {
            for rec in &r.records {
                d.record(&rec.key, &rec.envelope);
            }
        }
    }
    let (count, digest) = d.finish_bootstrap(h.snapshot_seq);
    assert_eq!(
        (f.record_count, f.digest.as_slice()),
        (13, digest.as_slice())
    );
    assert_eq!(count, 13);
    let keys: Vec<Vec<u8>> = msgs
        .iter()
        .flat_map(|r| r.records.iter().map(|x| x.key.clone()))
        .collect();
    assert!(keys.windows(2).all(|w| w[0] < w[1]), "strictly ascending");
    let ttl = msgs
        .iter()
        .flat_map(|r| &r.records)
        .find(|x| x.key == b"ttl")
        .expect("expired value is sent");
    assert_eq!(envelope::decode(&ttl.envelope).unwrap().value, b"gone");
}

#[tokio::test]
async fn bootstrap_lease_holds_retention_until_tail_presents_it() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");
    let (h, _, _) = read_bootstrap(&mut c).await;
    let s = h.snapshot_seq;
    for i in 0..5u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    eventually("retention to trim up to the lease", || {
        m.first_retained_seq() == s + 1
    })
    .await;
    tokio::time::sleep(Duration::from_millis(100)).await; // five more retention rounds
    assert_eq!(m.first_retained_seq(), s + 1, "the lease keeps S + 1");

    let mut req = tail_req(&m.identity(), s + 1);
    req.bootstrap_lease = Some(h.lease_id.clone());
    let mut stream = c.tail_start(req).await.unwrap().into_inner();
    assert_eq!(next_batch(&mut stream).await[0].seq, s + 1);
    eventually("retention after the lease is released", || {
        m.first_retained_seq() == m.last_seq()
    })
    .await;
}

#[tokio::test]
async fn an_unclaimed_lease_expires() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.retention.max_entries = Some(1);
        cfg.retention.check_interval = Duration::from_millis(20);
        cfg.serve.as_mut().unwrap().limits.bootstrap_lease_ttl = Duration::from_millis(1500);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let (h, _, _) = read_bootstrap(&mut client(m.serve_addr().unwrap(), "b")).await;
    for i in 0..3u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    // Held by the unclaimed lease first, then released when it expires (1 s sweep).
    eventually("retention to stop at the lease", || {
        m.first_retained_seq() == h.snapshot_seq + 1
    })
    .await;
    eventually("lease expiry", || m.first_retained_seq() == m.last_seq()).await;
}

#[tokio::test]
async fn concurrent_bootstraps_are_limited() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let limits = &mut cfg.serve.as_mut().unwrap().limits;
        limits.max_concurrent_bootstraps = 1;
        limits.bootstrap_chunk_bytes = 1;
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    // 32 MiB, one record per message: far more than the channel, socket and HTTP/2 buffers
    // hold, so the unread first bootstrap keeps its producer, and its permit, busy.
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    let mut c = client(m.serve_addr().unwrap(), "b");
    let _held = c
        .bootstrap(pb::BootstrapRequest {
            client_id: "b".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let err = client(m.serve_addr().unwrap(), "c")
        .bootstrap(pb::BootstrapRequest {
            client_id: "c".into(),
        })
        .await
        .unwrap_err();
    assert_eq!(
        (err.code(), reason_of(&err)),
        (Code::ResourceExhausted, Some(pb::Reason::Busy))
    );
}

/// Clients that stop reading must not keep the server's producers, and so the database,
/// alive: shutdown ends them and the directory can be reopened at once.
#[tokio::test]
async fn shutdown_with_unread_streams_releases_the_database() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    // 32 MiB of data: far more than socket and HTTP/2 buffers, so both producers block.
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    let mut c = client(m.serve_addr().unwrap(), "b");
    // A Tail that never sends `start`: its handler holds the source while it waits. It is
    // opened first, while nothing else holds the source: a Tail producer also clones the
    // source for each blocking log read, so with streams open the count is not quiescent.
    let base = Arc::strong_count(&m.log_source());
    let (_silent_tx, silent_rx) = tokio::sync::mpsc::channel::<pb::TailClientMessage>(1);
    let mut c2 = c.clone();
    let silent = tokio::spawn(async move {
        c2.tail(tokio_stream::wrappers::ReceiverStream::new(silent_rx))
            .await
    });
    eventually("the silent Tail holds the source", || {
        Arc::strong_count(&m.log_source()) > base
    })
    .await;
    let mut tail = c
        .tail_start(tail_req(&m.identity(), 1))
        .await
        .unwrap()
        .into_inner();
    assert!(
        within("the first tail message", tail.message())
            .await
            .unwrap()
            .is_some()
    );
    let mut boot = c
        .bootstrap(pb::BootstrapRequest {
            client_id: "b".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let first = within("the bootstrap header", boot.message())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first.kind, Some(Chunk::Header(_))));
    tokio::time::timeout(Duration::from_secs(20), m.shutdown())
        .await
        .expect("shutdown hangs")
        .unwrap();
    let silent = within("the silent Tail's answer", silent).await.unwrap();
    assert_eq!(
        silent.err().map(|s| reason_of(&s)),
        Some(Some(pb::Reason::ShuttingDown))
    );
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    assert_eq!(m.last_seq(), 129);
}

// ---------------- Fetch batching (PROTOCOL.md §7.5) ----------------

fn fetch_req(
    id: &DbIdentity,
    from_seq: u64,
    min_batch_bytes: u32,
    max_wait_ms: u32,
    max_batch_bytes: u32,
) -> pb::TailRequest {
    pb::TailRequest {
        min_batch_bytes,
        max_wait_ms,
        max_batch_bytes,
        ..tail_req(id, from_seq)
    }
}

fn batch_bytes(batch: &[LogEntry]) -> usize {
    batch.iter().map(LogEntry::encoded_len).sum()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn real_time_mode_delivers_each_entry_promptly_one_batch_at_a_time() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_secs(1)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(tail_req(&m.identity(), 2))
        .await
        .unwrap()
        .into_inner();
    for i in 0..5u8 {
        let seq = m.put("c", &[i], b"v").unwrap();
        let t0 = Instant::now(); // after the fsynced put returned
        let batch = next_batch(&mut s).await;
        let latency = t0.elapsed();
        assert_eq!(
            batch.iter().map(|e| e.seq).collect::<Vec<_>>(),
            vec![seq],
            "one entry per batch"
        );
        // Real time: nothing waits for more data (the generous bound only guards against a
        // regression to the 1 s heartbeat or a max_wait; it is not a latency benchmark).
        assert!(
            latency < Duration::from_millis(900),
            "entry {seq} took {latency:?}"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn small_writes_wait_for_max_wait_and_arrive_together() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_secs(3)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let req = fetch_req(&m.identity(), 2, 1 << 20, 1500, 0);
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(req)
        .await
        .unwrap()
        .into_inner();
    m.put("c", &[0], b"v").unwrap();
    let t0 = Instant::now(); // entry 2 became available no later than this
    for i in 1..3u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(
        batch.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![2, 3, 4],
        "one batch"
    );
    assert!(
        waited >= Duration::from_millis(1000),
        "sent before max_wait (1500 ms): {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(6),
        "sent long after max_wait: {waited:?}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn reaching_min_batch_bytes_sends_before_max_wait() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.heartbeat = Duration::from_secs(30);
        cfg.serve.as_mut().unwrap().limits.replica_lag_time_max = Duration::from_secs(60);
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", &[0], &[7; 100]).unwrap();
    let one = m
        .entries_since(1)
        .unwrap()
        .next()
        .unwrap()
        .unwrap()
        .encoded_len();
    let req = fetch_req(&m.identity(), 3, u32::try_from(3 * one).unwrap(), 20_000, 0);
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(req)
        .await
        .unwrap()
        .into_inner();
    for i in 1..=3u8 {
        m.put("c", &[i], &[7; 100]).unwrap();
    }
    let t0 = Instant::now(); // min_batch_bytes is reached no later than this
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(
        batch.iter().map(|e| e.seq).collect::<Vec<_>>(),
        vec![3, 4, 5]
    );
    assert!(batch_bytes(&batch) >= 3 * one);
    assert!(
        waited < Duration::from_secs(5),
        "waited {waited:?} (max_wait is 20 s) although min_batch_bytes was reached"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn catch_up_is_split_at_max_batch_bytes_without_gaps() {
    let dir = tempfile::tempdir().unwrap();
    // The server caps the client's 1 MiB request at 64 KiB.
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.serve.as_mut().unwrap().limits.max_tail_batch_bytes = 64 << 10;
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..1000u32 {
        m.put("c", &i.to_be_bytes(), &[1; 1024]).unwrap();
    }
    // min_batch_bytes is huge and max_wait long: catch-up must still not wait.
    let req = fetch_req(&m.identity(), 1, u32::MAX, 100, 1 << 20);
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(req)
        .await
        .unwrap()
        .into_inner();
    let mut batches = Vec::new();
    let mut seqs = Vec::new();
    while seqs.len() < 1001 {
        let b = next_batch(&mut s).await;
        assert!(
            batch_bytes(&b) <= 64 << 10,
            "a batch of {} bytes exceeds the cap",
            batch_bytes(&b)
        );
        seqs.extend(b.iter().map(|e| e.seq));
        batches.push(b);
    }
    assert_eq!(
        seqs,
        (1..=1001).collect::<Vec<_>>(),
        "consecutive, no gaps or duplicates"
    );
    assert!(
        batches.len() >= 16,
        "1 MiB of backlog needs at least 16 batches of 64 KiB, got {}",
        batches.len()
    );
    // Catch-up is not paced by max_wait: with 100 ms per batch it would take over 1.6 s.
    // (Each `next_batch` is bounded at 10 s; the shape assertions above carry the test.)
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn an_entry_larger_than_max_batch_bytes_is_sent_alone() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"small-1", b"v").unwrap();
    let big = m.put("c", b"big", &[9; 4096]).unwrap();
    m.put("c", b"small-2", b"v").unwrap();
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(fetch_req(&m.identity(), 2, 0, 0, 1024))
        .await
        .unwrap()
        .into_inner();
    let mut got = Vec::new();
    while got.iter().map(Vec::len).sum::<usize>() < 3 {
        got.push(next_batch(&mut s).await);
    }
    let seqs: Vec<Vec<u64>> = got
        .iter()
        .map(|b| b.iter().map(|e| e.seq).collect())
        .collect();
    assert_eq!(seqs, vec![vec![2], vec![big], vec![4]]);
    assert!(batch_bytes(&got[1]) > 1024);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn max_wait_is_clamped_to_the_heartbeat() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| cfg.heartbeat = Duration::from_millis(200)).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    let req = fetch_req(&m.identity(), 2, 1 << 20, 30_000, 0);
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(req)
        .await
        .unwrap()
        .into_inner();
    m.put("c", b"k", b"v").unwrap();
    let t0 = Instant::now();
    let batch = next_batch(&mut s).await;
    let waited = t0.elapsed();
    assert_eq!(batch.len(), 1);
    assert!(
        waited >= Duration::from_millis(100),
        "sent before the clamped max_wait: {waited:?}"
    );
    assert!(
        waited < Duration::from_secs(5),
        "max_wait (30 s) was not clamped to the 200 ms heartbeat: {waited:?}"
    );
}

/// Large log: both producers block on a client that does not read.
async fn big_log(dir: &std::path::Path) -> crate::Master {
    let (m, _) = serving_master(dir, |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    m
}

/// A reset while the channel is full must still reach the client as a status, not a clean EOF.
#[tokio::test]
async fn tail_reset_under_backpressure_ends_with_upstream_reset() {
    let dir = tempfile::tempdir().unwrap();
    let m = big_log(dir.path()).await;
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(tail_req(&m.identity(), 1))
        .await
        .unwrap()
        .into_inner();
    assert!(
        within("the first tail message", s.message())
            .await
            .unwrap()
            .is_some()
    );
    tokio::time::sleep(Duration::from_millis(300)).await; // the producer is blocked, channel full
    m.bump_epoch().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let err = stream_error(&mut s).await;
    assert_eq!(
        (err.code(), reason_of(&err)),
        (Code::Unavailable, Some(pb::Reason::UpstreamReset))
    );
}

#[tokio::test]
async fn bootstrap_reset_under_backpressure_ends_with_upstream_reset() {
    let dir = tempfile::tempdir().unwrap();
    let m = big_log(dir.path()).await;
    let mut s = client(m.serve_addr().unwrap(), "b")
        .bootstrap(pb::BootstrapRequest {
            client_id: "b".into(),
        })
        .await
        .unwrap()
        .into_inner();
    let first = within("the bootstrap header", s.message())
        .await
        .unwrap()
        .unwrap();
    assert!(matches!(first.kind, Some(Chunk::Header(_))));
    tokio::time::sleep(Duration::from_millis(300)).await; // the job is blocked, channel full
    m.bump_epoch().unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    let err = stream_error(&mut s).await;
    assert_eq!(
        (err.code(), reason_of(&err)),
        (Code::Unavailable, Some(pb::Reason::UpstreamReset))
    );
}

/// A batch never exceeds `max_batch_bytes` on the wire, per-entry framing included.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn batches_fit_max_batch_bytes_including_framing() {
    use prost::Message;
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..200u32 {
        m.put("c", &i.to_be_bytes(), &[1; 60]).unwrap();
    }
    let mut s = client(m.serve_addr().unwrap(), "b")
        .tail_start(fetch_req(&m.identity(), 1, 0, 0, 1000))
        .await
        .unwrap()
        .into_inner();
    let mut seen = 0;
    while seen < 201 {
        let batch = next_batch(&mut s).await;
        let wire = pb::EntryBatch {
            entries: batch
                .iter()
                .cloned()
                .map(crate::net::convert::entry_to_pb)
                .collect(),
        }
        .encoded_len();
        assert!(
            wire <= 1000,
            "batch of {} entries is {wire} bytes on the wire",
            batch.len()
        );
        seen += batch.len();
    }
}

// ---------------- Acks and replica status (addendum §1) ----------------

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn acks_drive_replica_status_and_in_sync() {
    let dir = tempfile::tempdir().unwrap();
    let (m, clock) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    for i in 0..3u8 {
        m.put("c", &[i], b"v").unwrap();
    }
    let mut c = client(m.serve_addr().unwrap(), "b");
    let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
    let batch = next_batch(&mut s).await;
    assert_eq!(batch.last().unwrap().seq, 4);
    eventually("the replica is registered", || m.replicas().len() == 1).await;
    let r = &m.replicas()[0];
    assert_eq!(
        (r.node_id.as_str(), r.sent_seq, r.applied_seq, r.lag_entries),
        ("b", 4, 0, 4)
    );
    assert!(!r.in_sync, "not caught up yet");
    assert!(r.lag_ms.is_some());
    assert_eq!(r.epoch, 1);
    assert_eq!(r.fetch.max_batch_bytes, 1 << 20);

    acks.send(ack(2)).await.unwrap();
    eventually("the partial ack", || m.replicas()[0].applied_seq == 2).await;
    assert_eq!(m.replicas()[0].lag_entries, 2);
    assert!(!m.replicas()[0].in_sync);
    // lag_ms is now − ts_ms of the first unacknowledged entry (3, written at T0)...
    clock.advance(Duration::from_secs(1));
    assert_eq!(m.replicas()[0].lag_ms, Some(1000));
    // ...and None once that entry is no longer retained here.
    let keep_one = crate::config::Retention {
        max_age: None,
        max_entries: Some(1),
        check_interval: Duration::from_secs(1),
    };
    m.log_source().store().trim_changelog(0, &keep_one).unwrap();
    assert_eq!(m.first_retained_seq(), 4);
    assert_eq!(m.replicas()[0].lag_ms, None);
    acks.send(ack(4)).await.unwrap();
    eventually("caught up and in sync", || m.replicas()[0].in_sync).await;
    let r = &m.replicas()[0];
    assert_eq!((r.applied_seq, r.lag_entries, r.lag_ms), (4, 0, Some(0)));

    // A new write is sent but not acknowledged: lag grows, the replica stays in sync for
    // replica_lag_time_max.
    m.put("c", b"k", b"v").unwrap();
    assert_eq!(next_batch(&mut s).await[0].seq, 5);
    eventually("sent", || m.replicas()[0].sent_seq == 5).await;
    let r = &m.replicas()[0];
    assert_eq!((r.applied_seq, r.lag_entries), (4, 1));
    assert!(r.in_sync);

    drop(acks);
    drop(s);
    eventually("the closed stream leaves the registry", || {
        m.replicas().is_empty()
    })
    .await;
}

#[tokio::test]
async fn ack_violations_end_the_stream_with_invalid_argument() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let mut c = client(m.serve_addr().unwrap(), "b");

    // Above the last sent seq.
    let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 3)).await.unwrap();
    acks.send(ack(3)).await.unwrap();
    let e = stream_error(&mut s).await;
    assert_eq!(e.code(), Code::InvalidArgument, "{e:?}");

    // Backwards.
    let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
    next_batch(&mut s).await;
    eventually("sent", || m.replicas().iter().any(|r| r.sent_seq == 2)).await;
    acks.send(ack(2)).await.unwrap();
    acks.send(ack(1)).await.unwrap();
    assert_eq!(stream_error(&mut s).await.code(), Code::InvalidArgument);

    // A second `start`.
    let (mut s, acks) = c.tail_with_acks(tail_req(&m.identity(), 1)).await.unwrap();
    acks.send(pb::TailClientMessage {
        kind: Some(pb::tail_client_message::Kind::Start(tail_req(
            &m.identity(),
            1,
        ))),
    })
    .await
    .unwrap();
    assert_eq!(stream_error(&mut s).await.code(), Code::InvalidArgument);
}

#[tokio::test]
async fn the_first_tail_message_must_be_start() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |_| {}).await;
    let mut c = client(m.serve_addr().unwrap(), "b");
    let e = c
        .tail(tokio_stream::iter([super::test_client::ack(0)]))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument);
    let e = c
        .tail(tokio_stream::iter(Vec::<pb::TailClientMessage>::new()))
        .await
        .unwrap_err();
    assert_eq!(e.code(), Code::InvalidArgument, "an empty request stream");
}

/// A reset while a bootstrap client has stopped reading: the job releases its snapshot and its
/// bootstrap slot at once, and only then waits to deliver `UPSTREAM_RESET`.
#[tokio::test]
async fn a_reset_releases_a_stalled_bootstraps_snapshot_and_slot() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        cfg.serve.as_mut().unwrap().limits.max_concurrent_bootstraps = 1;
    })
    .await;
    m.create_cf(CfSpec::new("c")).unwrap();
    // 32 MiB: far more than the channel and transport buffers hold.
    for i in 0..128u8 {
        m.put("c", &[i], &vec![i; 256 << 10]).unwrap();
    }
    let src = m.log_source();
    let db_refs = Arc::strong_count(src.store().db());
    let mut stalled = client(m.serve_addr().unwrap(), "b");
    let mut boot = stalled
        .bootstrap(pb::BootstrapRequest {
            client_id: "b".into(),
        })
        .await
        .unwrap()
        .into_inner();
    assert!(matches!(
        within("the header", boot.message())
            .await
            .unwrap()
            .unwrap()
            .kind,
        Some(Chunk::Header(_))
    ));
    assert!(
        Arc::strong_count(src.store().db()) > db_refs,
        "the job holds a snapshot"
    );
    m.bump_epoch().unwrap();
    eventually("the snapshot is released", || {
        Arc::strong_count(src.store().db()) == db_refs
    })
    .await;
    let mut other = client(m.serve_addr().unwrap(), "c");
    let next = other
        .bootstrap(pb::BootstrapRequest {
            client_id: "c".into(),
        })
        .await;
    assert!(next.is_ok(), "the slot is free again: {:?}", next.err());
}

/// Final review T4-M6: the verify RPCs are a method of their own. A credential without
/// `Method::Verify` gets `PERMISSION_DENIED` on `VerifyBegin`, `VerifyRange` and `VerifyEnd`;
/// a call without a token gets `UNAUTHENTICATED`.
#[tokio::test]
async fn the_verify_rpcs_require_the_verify_method() {
    let dir = tempfile::tempdir().unwrap();
    let (m, _) = serving_master(dir.path(), |cfg| {
        let serve = cfg.serve.as_mut().unwrap();
        let token = Arc::new(TokenAuthenticator::new(["t"]));
        serve.authenticator =
            Arc::new(AllowMethods::new(token, &[Method::Handshake, Method::Tail]));
        serve.allow_plaintext_token = true;
    })
    .await;
    m.create_cf(crate::types::CfSpec::new("c")).unwrap();
    let addr = m.serve_addr().unwrap();
    let with = |token| {
        client_with(
            addr,
            Meta {
                node_id: "b",
                token,
                version: Some(PROTOCOL_VERSION),
            },
        )
    };
    for (token, code) in [
        (Some("t"), Code::PermissionDenied),
        (None, Code::Unauthenticated),
    ] {
        let mut c = with(token);
        let e = c
            .verify_begin(pb::VerifyBeginRequest {
                client_id: "b".into(),
                mode: pb::VerifyMode::Quick as i32,
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), code, "VerifyBegin with {token:?}");
        let req = pb::VerifyRangeRequest {
            session_id: vec![0; 16],
            request_id: 1,
            kind: None,
        };
        let e = c.verify_range(tokio_stream::iter([req])).await.unwrap_err();
        assert_eq!(e.code(), code, "VerifyRange with {token:?}");
        let e = c
            .verify_end(pb::VerifyEndRequest {
                session_id: vec![0; 16],
            })
            .await
            .unwrap_err();
        assert_eq!(e.code(), code, "VerifyEnd with {token:?}");
    }
}
