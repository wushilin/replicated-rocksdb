//! The HTTP status endpoints (`ServeConfig::http_status`): served on the replication port,
//! behind the same TLS, token and failure limiter as gRPC, while replication keeps working.

mod support;

use std::net::SocketAddr;
use std::sync::Arc;

use http_body_util::{BodyExt, Full};
use hyper::body::Bytes;
use hyper::{Method, Request, StatusCode};
use hyper_util::rt::{TokioExecutor, TokioIo};
use replicated_rocksdb::{
    CfSpec, HaltReason, Master, Slave, SlaveState, TlsServerConfig, TokenAuthenticator,
    TokenCredentials,
};
use support::{Cluster, assert_converged, wait_state};
use tokio::net::TcpStream;
use tokio_rustls::rustls;

/// A CA and a server certificate for `localhost`, made by rcgen.
struct Pki {
    ca_der: Vec<u8>,
    ca_pem: Vec<u8>,
    issuer: rcgen::Issuer<'static, rcgen::KeyPair>,
}

impl Pki {
    fn new() -> Self {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, "rrdb test CA");
        let cert = params.self_signed(&key).unwrap();
        Pki {
            ca_der: cert.der().to_vec(),
            ca_pem: cert.pem().into_bytes(),
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    fn server(&self) -> TlsServerConfig {
        let key = rcgen::KeyPair::generate().unwrap();
        let params = rcgen::CertificateParams::new(vec!["localhost".to_string()]).unwrap();
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        TlsServerConfig {
            identity: replicated_rocksdb::TlsIdentity {
                cert_pem: cert.pem().into_bytes(),
                key_pem: key.serialize_pem().into_bytes(),
            },
            client_ca_pem: None,
        }
    }

    fn client(&self) -> replicated_rocksdb::TlsClientConfig {
        replicated_rocksdb::TlsClientConfig {
            ca_pem: self.ca_pem.clone(),
            domain_name: Some("localhost".into()),
            identity: None,
        }
    }
}

struct Reply {
    status: StatusCode,
    headers: hyper::HeaderMap,
    body: String,
}

impl Reply {
    fn json(&self) -> serde_json::Value {
        serde_json::from_str(&self.body).unwrap_or_else(|e| panic!("{e}: {}", self.body))
    }
}

fn request(method: Method, path: &str, token: Option<&str>) -> Request<Full<Bytes>> {
    with_body(method, path, token, "")
}

fn with_body(
    method: Method,
    path: &str,
    token: Option<&str>,
    body: &'static str,
) -> Request<Full<Bytes>> {
    let mut b = Request::builder()
        .method(method)
        .uri(format!("http://localhost{path}"));
    if let Some(t) = token {
        b = b.header("authorization", format!("Bearer {t}"));
    }
    b.body(Full::new(Bytes::from_static(body.as_bytes())))
        .unwrap()
}

async fn read(resp: hyper::Response<hyper::body::Incoming>) -> Reply {
    let (parts, body) = resp.into_parts();
    let body = body.collect().await.unwrap().to_bytes();
    Reply {
        status: parts.status,
        headers: parts.headers,
        body: String::from_utf8(body.to_vec()).unwrap(),
    }
}

/// One request over TLS (HTTP/2, the only ALPN protocol the server offers), as `curl
/// --cacert ca.pem` would send it.
async fn https(
    addr: SocketAddr,
    pki: &Pki,
    method: Method,
    path: &str,
    token: Option<&str>,
) -> Reply {
    send_tls(addr, pki, request(method, path, token)).await
}

async fn send_tls(addr: SocketAddr, pki: &Pki, req: Request<Full<Bytes>>) -> Reply {
    let mut roots = rustls::RootCertStore::empty();
    roots.add(pki.ca_der.clone().into()).unwrap();
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut cfg = rustls::ClientConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_root_certificates(roots)
        .with_no_client_auth();
    cfg.alpn_protocols = vec![b"h2".to_vec(), b"http/1.1".to_vec()];
    let tcp = TcpStream::connect(addr).await.unwrap();
    let tls = tokio_rustls::TlsConnector::from(Arc::new(cfg))
        .connect("localhost".try_into().unwrap(), tcp)
        .await
        .unwrap();
    let (mut send, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tls))
            .await
            .unwrap();
    tokio::spawn(conn);
    read(send.send_request(req).await.unwrap()).await
}

/// One plaintext HTTP/1.1 request.
async fn http1(addr: SocketAddr, path: &str) -> hyper::Result<Reply> {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let (mut send, conn) = hyper::client::conn::http1::handshake(TokioIo::new(tcp)).await?;
    tokio::spawn(conn);
    Ok(read(send.send_request(request(Method::GET, path, None)).await?).await)
}

/// One plaintext HTTP/2 request (prior knowledge, as a gRPC client speaks).
async fn h2c(addr: SocketAddr, path: &str) -> Reply {
    let tcp = TcpStream::connect(addr).await.unwrap();
    let (mut send, conn) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(tcp))
            .await
            .unwrap();
    tokio::spawn(conn);
    read(
        send.send_request(request(Method::GET, path, None))
            .await
            .unwrap(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn status_endpoints_share_the_replication_port_tls_and_token() {
    let c = Cluster::new();
    let pki = Pki::new();
    let mut cfg = c.master_config("a");
    let serve = cfg.serve.as_mut().unwrap();
    serve.tls = Some(pki.server());
    serve.authenticator = Arc::new(TokenAuthenticator::new(["t0k"]));
    serve.http_status = true;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let addr = m.serve_addr().unwrap();

    let (mut scfg, _) = c.slave_config("b", addr);
    scfg.serve = None;
    scfg.upstream.tls = Some(pki.client());
    scfg.upstream.credentials = Arc::new(TokenCredentials::new("t0k"));
    let b = Slave::open(scfg).await.unwrap();
    assert_converged(&m, &[("b", &b)]).await;

    let r = https(addr, &pki, Method::GET, "/v1/status", Some("t0k")).await;
    assert_eq!(r.status, StatusCode::OK, "{}", r.body);
    assert_eq!(r.headers["content-type"], "application/json");
    let v = r.json();
    assert_eq!(v["last_seq"], m.last_seq());
    assert_eq!(v["node_id"], "a");
    assert_eq!(v["role"], "master");
    assert_eq!(v["node_state"], "ready");
    assert_eq!(
        v["identity"]["db_uuid"],
        m.identity().db_uuid.hyphenated().to_string()
    );
    assert_eq!(v["identity"]["epoch"], m.identity().epoch);
    assert_eq!(v["epoch_history"][0]["start_seq"], 1);
    assert!(v["last_entry_crc"].as_str().unwrap().starts_with("0x"));
    assert_eq!(v["replicas"][0]["node_id"], "b");

    let r = https(addr, &pki, Method::GET, "/v1/status", None).await;
    assert_eq!(r.status, StatusCode::UNAUTHORIZED, "{}", r.body);
    assert_eq!(r.headers["www-authenticate"], "Bearer");
    let r = https(addr, &pki, Method::GET, "/v1/health", None).await;
    assert_eq!(
        r.status,
        StatusCode::UNAUTHORIZED,
        "health needs the token too"
    );

    let r = https(addr, &pki, Method::GET, "/v1/health", Some("t0k")).await;
    assert_eq!(
        (r.status, r.json()),
        (StatusCode::OK, serde_json::json!({"ok": true}))
    );
    let r = https(addr, &pki, Method::GET, "/v1/ready", Some("t0k")).await;
    assert_eq!(
        (r.status, r.json()),
        (
            StatusCode::OK,
            serde_json::json!({"ready": true, "node_state": "ready"})
        )
    );

    let r = https(addr, &pki, Method::GET, "/metrics", Some("t0k")).await;
    assert_eq!(r.status, StatusCode::OK);
    assert_eq!(
        r.headers["content-type"],
        "text/plain; version=0.0.4; charset=utf-8"
    );
    assert!(
        r.body
            .contains(&format!("\nrrdb_last_seq {}\n", m.last_seq())),
        "{}",
        r.body
    );
    assert!(
        r.body.contains("rrdb_role{role=\"master\"} 1\n"),
        "{}",
        r.body
    );
    assert!(r.body.contains("rrdb_node_ready 1\n"), "{}", r.body);
    assert!(
        r.body.contains("rrdb_replica_applied_seq{node_id=\"b\"} "),
        "{}",
        r.body
    );
    assert!(
        r.body.contains("rrdb_replica_in_sync{node_id=\"b\"} "),
        "{}",
        r.body
    );
    // Every sample line parses as `name{labels} value`.
    for line in r.body.lines().filter(|l| !l.starts_with('#')) {
        let (name, value) = line.rsplit_once(' ').unwrap();
        assert!(name.starts_with("rrdb_"), "{line}");
        value.parse::<u64>().unwrap();
    }

    let r = https(addr, &pki, Method::GET, "/v1/nope", Some("t0k")).await;
    assert_eq!(r.status, StatusCode::NOT_FOUND);
    let r = https(addr, &pki, Method::POST, "/v1/status", Some("t0k")).await;
    assert_eq!(r.status, StatusCode::METHOD_NOT_ALLOWED);
    assert_eq!(r.headers["allow"], "GET");
    let req = with_body(Method::GET, "/v1/status", Some("t0k"), "unread");
    let r = send_tls(addr, &pki, req).await;
    assert_eq!(r.status, StatusCode::BAD_REQUEST, "{}", r.body);

    // gRPC on the same port is unaffected.
    m.put("c", b"k2", b"v2").unwrap();
    assert_converged(&m, &[("b", &b)]).await;
    let v = https(addr, &pki, Method::GET, "/v1/status", Some("t0k"))
        .await
        .json();
    assert_eq!(v["last_seq"], m.last_seq());
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn repeated_wrong_tokens_over_http_are_throttled() {
    let c = Cluster::new();
    let pki = Pki::new();
    let mut cfg = c.master_config("a");
    let serve = cfg.serve.as_mut().unwrap();
    serve.tls = Some(pki.server());
    serve.authenticator = Arc::new(TokenAuthenticator::new(["good"]));
    serve.limits.max_auth_failures_per_minute = 3;
    serve.http_status = true;
    let m = Master::open(cfg).await.unwrap();
    let addr = m.serve_addr().unwrap();
    for _ in 0..3 {
        let r = https(addr, &pki, Method::GET, "/v1/status", Some("guess")).await;
        assert_eq!(r.status, StatusCode::UNAUTHORIZED);
    }
    let r = https(addr, &pki, Method::GET, "/v1/status", Some("good")).await;
    assert_eq!(r.status, StatusCode::TOO_MANY_REQUESTS, "{}", r.body);
    // And gRPC from the same address is throttled by the same limiter.
    let mut up = replicated_rocksdb::UpstreamConfig::from_addr(addr);
    up.tls = Some(pki.client());
    up.credentials = Arc::new(TokenCredentials::new("good"));
    let err = replicated_rocksdb::admin::node_status(&up, "probe")
        .await
        .unwrap_err();
    assert!(
        err.to_string().contains("too many failed authentication"),
        "{err}"
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_halted_slave_is_not_ready() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    let serve = cfg.serve.as_mut().unwrap();
    serve.authenticator = Arc::new(TokenAuthenticator::new(["good"]));
    serve.allow_plaintext_token = true;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();

    let (mut scfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    scfg.upstream.credentials = Arc::new(TokenCredentials::new("wrong"));
    scfg.serve.as_mut().unwrap().http_status = true;
    let b = Slave::open(scfg).await.unwrap();
    wait_state(&b, "AuthRejected", |s| {
        *s == SlaveState::Halted(HaltReason::AuthRejected)
    })
    .await;
    // The slave serves plaintext with `AllowAll`: plain HTTP/1.1, no token.
    let r = http1(b.serve_addr().unwrap(), "/v1/ready").await.unwrap();
    assert_eq!(r.status, StatusCode::SERVICE_UNAVAILABLE, "{}", r.body);
    let v = r.json();
    assert_eq!(v["ready"], false);
    assert!(
        v["node_state"].as_str().unwrap().starts_with("halted:"),
        "{v}"
    );
    let r = http1(b.serve_addr().unwrap(), "/metrics").await.unwrap();
    assert!(r.body.contains("rrdb_node_ready 0\n"), "{}", r.body);
    assert!(
        r.body.contains("rrdb_role{role=\"slave\"} 1\n"),
        "{}",
        r.body
    );
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn without_http_status_the_port_serves_only_grpc() {
    let c = Cluster::new();
    let m = c.master("a").await;
    let addr = m.serve_addr().unwrap();
    // HTTP/1.1 is refused at the connection level (the listener speaks HTTP/2 only).
    assert!(http1(addr, "/v1/status").await.is_err());
    // Over HTTP/2 the path is not a gRPC method: tonic's own `UNIMPLEMENTED`, not a status.
    let r = h2c(addr, "/v1/status").await;
    assert_eq!(r.headers["grpc-status"], "12", "{:?}", r.headers);
    assert!(r.body.is_empty());
}
