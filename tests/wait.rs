//! `Slave::wait_streaming` / `wait_for_seq` timeouts say where the slave stood
//! (`Error::WaitTimeout`), and `SlaveStatus::last_error_kind` tells a wrong address or a wrong
//! endpoint from an upstream that is only slow.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, Error, RepointOptions, RepointOutcome, Slave, SlaveState, TlsClientConfig,
    UpstreamConfig, UpstreamErrorKind, WaitProgress,
};
use support::{Cluster, WAIT, wait_until};
use tokio::io::{AsyncReadExt, AsyncWriteExt};

const SHORT: Duration = Duration::from_millis(300);

/// A TCP server that answers every connection with `reply` and closes it.
async fn raw_server(reply: &'static [u8]) -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(async move {
        while let Ok((mut s, _)) = l.accept().await {
            tokio::spawn(async move {
                let mut b = [0u8; 1024];
                let _ = s.read(&mut b).await;
                let _ = s.write_all(reply).await;
                // Closing at once races the client's parse of the reply (it may see only the
                // close); servers usually linger a moment.
                tokio::time::sleep(Duration::from_secs(2)).await;
                let _ = s.shutdown().await;
            });
        }
    });
    addr
}

/// A gRPC server that has no replication service.
async fn other_grpc_server() -> SocketAddr {
    let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = l.local_addr().unwrap();
    tokio::spawn(
        tonic::transport::Server::builder()
            .add_routes(tonic::service::Routes::default())
            .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(l)),
    );
    addr
}

fn timeout_progress(r: replicated_rocksdb::Result<()>) -> Box<WaitProgress> {
    match r {
        Err(Error::WaitTimeout(p)) => p,
        other => panic!("expected WaitTimeout, got {other:?}"),
    }
}

/// Waits until the slave reported a problem, then checks a wait on it: no progress, never
/// reached, and the expected kind, in the status and in the error.
async fn assert_stuck(s: &Slave, kind: UpstreamErrorKind) {
    wait_until("a classified error", || {
        s.status().last_error_kind == Some(kind)
    })
    .await;
    let p = timeout_progress(s.wait_streaming(SHORT).await);
    assert!(!p.progressed, "{p}");
    assert_eq!(p.target_seq, None);
    assert_eq!(p.status.state, SlaveState::Connecting);
    assert_eq!(p.status.last_contact, None, "{p}");
    assert_eq!(p.status.last_error_kind, Some(kind), "{p}");
    assert!(p.status.last_error.is_some());
    let text = p.to_string();
    assert!(
        text.contains("no progress") && text.contains("never reached"),
        "{text}"
    );
    let p = timeout_progress(s.wait_for_seq(1, SHORT).await);
    assert_eq!((p.target_seq, p.progressed), (Some(1), false));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_refused_connection_is_unreachable() {
    let c = Cluster::new();
    let nobody = SocketAddr::from(([127, 0, 0, 1], support::reserved_port()));
    let (b, _) = c.slave("b", nobody).await;
    assert_stuck(&b, UpstreamErrorKind::Unreachable).await;
    let e = b.status().last_error.unwrap();
    assert!(e.contains("refused"), "the root cause is shown: {e}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_endpoint_that_is_not_http2_is_not_a_replication_service() {
    let c = Cluster::new();
    let http1 = raw_server(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n").await;
    let (b, _) = c.slave("b", http1).await;
    assert_stuck(&b, UpstreamErrorKind::NotReplicationService).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn another_grpc_server_is_not_a_replication_service() {
    let c = Cluster::new();
    let grpc = other_grpc_server().await;
    let (b, _) = c.slave("b", grpc).await;
    assert_stuck(&b, UpstreamErrorKind::NotReplicationService).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_tls_client_at_a_plaintext_master_is_a_tls_problem() {
    let c = Cluster::new();
    let m = c.master("a").await;
    let key = rcgen::KeyPair::generate().unwrap();
    let mut ca = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
    ca.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    let (mut cfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    cfg.upstream.tls = Some(TlsClientConfig {
        ca_pem: ca.self_signed(&key).unwrap().pem().into_bytes(),
        domain_name: Some("localhost".into()),
        identity: None,
    });
    let b = Slave::open(cfg).await.unwrap();
    assert_stuck(&b, UpstreamErrorKind::Tls).await;
}

/// A slave catching up under a stream of writes is not stuck: its wait times out with
/// `progressed`, no error, and a contact time.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_moving_slave_times_out_with_progress() {
    let c = Cluster::new();
    let m = Arc::new(c.master("a").await);
    m.create_cf(CfSpec::new("c")).unwrap();
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    b.wait_streaming(WAIT).await.unwrap();
    let stop = Arc::new(AtomicBool::new(false));
    let writer = {
        let (m, stop) = (m.clone(), stop.clone());
        tokio::task::spawn_blocking(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::SeqCst) {
                m.put("c", &i.to_be_bytes(), b"v").unwrap();
                i += 1;
                std::thread::sleep(Duration::from_millis(5));
            }
        })
    };
    let p = timeout_progress(b.wait_for_seq(u64::MAX, Duration::from_millis(500)).await);
    stop.store(true, Ordering::SeqCst);
    writer.await.unwrap();
    assert!(p.progressed, "{p}");
    assert_eq!(p.target_seq, Some(u64::MAX));
    assert!(p.status.applied_seq > 1);
    assert!(p.status.last_contact.is_some());
    assert_eq!(p.status.last_error_kind, None);
    assert!(p.to_string().contains("progressing"), "{p}");
}

/// A repoint at a wrong address settles `Retrying` with the kind, and a repoint back clears it.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_repoint_to_a_wrong_address_says_so() {
    let c = Cluster::new();
    let m = c.master("a").await;
    let (b, _) = c.slave("b", m.serve_addr().unwrap()).await;
    b.wait_streaming(WAIT).await.unwrap();
    let http1 = raw_server(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n").await;
    match b
        .repoint(UpstreamConfig::from_addr(http1), RepointOptions::default())
        .await
        .unwrap()
    {
        RepointOutcome::Retrying { kind, last_error } => {
            assert_eq!(
                kind,
                UpstreamErrorKind::NotReplicationService,
                "{last_error}"
            )
        }
        other => panic!("{other:?}"),
    }
    let back = b
        .repoint(
            UpstreamConfig::from_addr(m.serve_addr().unwrap()),
            RepointOptions::default(),
        )
        .await
        .unwrap();
    assert_eq!(back, RepointOutcome::Resumed);
    b.wait_streaming(WAIT).await.unwrap();
    let s = b.status();
    assert_eq!((s.last_error, s.last_error_kind), (None, None));
}
