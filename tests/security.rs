//! TLS, mutual TLS and token authentication, end to end, with certificates made by rcgen.

mod support;

use std::sync::Arc;

use replicated_rocksdb::{
    AllowMethods, CfSpec, Error, HaltReason, Master, Method, MtlsSubjectAllowList, Slave,
    SlaveState, TlsClientConfig, TlsIdentity, TlsServerConfig, TokenAuthenticator,
    TokenCredentials,
};
use support::{Cluster, assert_converged, wait_state, wait_until};

/// A CA and the certificates it signs.
struct Pki {
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
            ca_pem: cert.pem().into_bytes(),
            issuer: rcgen::Issuer::new(params, key),
        }
    }

    /// A certificate for `localhost` (server) or with subject CN `cn` (client).
    fn identity(&self, sans: &[&str], cn: &str) -> TlsIdentity {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params =
            rcgen::CertificateParams::new(sans.iter().map(|s| s.to_string()).collect::<Vec<_>>())
                .unwrap();
        params
            .distinguished_name
            .push(rcgen::DnType::CommonName, cn);
        let cert = params.signed_by(&key, &self.issuer).unwrap();
        TlsIdentity {
            cert_pem: cert.pem().into_bytes(),
            key_pem: key.serialize_pem().into_bytes(),
        }
    }

    fn server(&self, mutual: bool) -> TlsServerConfig {
        TlsServerConfig {
            identity: self.identity(&["localhost"], "localhost"),
            client_ca_pem: mutual.then(|| self.ca_pem.clone()),
        }
    }

    fn client(&self, identity: Option<TlsIdentity>) -> TlsClientConfig {
        TlsClientConfig {
            ca_pem: self.ca_pem.clone(),
            domain_name: Some("localhost".into()),
            identity,
        }
    }
}

async fn tls_master(
    c: &Cluster,
    pki: &Pki,
    mutual: bool,
    auth: Arc<dyn replicated_rocksdb::Authenticator>,
) -> Master {
    let mut cfg = c.master_config("a");
    let serve = cfg.serve.as_mut().unwrap();
    serve.tls = Some(pki.server(mutual));
    serve.authenticator = auth;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    m
}

async fn tls_slave(
    c: &Cluster,
    m: &Master,
    name: &str,
    tls: TlsClientConfig,
    token: Option<&str>,
) -> Slave {
    let (mut cfg, _) = c.slave_config(name, m.serve_addr().unwrap());
    cfg.serve = None;
    cfg.upstream.tls = Some(tls);
    if let Some(t) = token {
        cfg.upstream.credentials = Arc::new(TokenCredentials::new(t));
    }
    Slave::open(cfg).await.unwrap()
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn tokens_over_tls_accept_any_valid_secret_and_reject_others() {
    let c = Cluster::new();
    let pki = Pki::new();
    let auth = Arc::new(TokenAuthenticator::new(["old-token", "new-token"]));
    let m = tls_master(&c, &pki, false, auth).await;
    let old = tls_slave(&c, &m, "old", pki.client(None), Some("old-token")).await;
    let new = tls_slave(&c, &m, "new", pki.client(None), Some("new-token")).await;
    assert_converged(&m, &[("old", &old), ("new", &new)]).await;

    let bad = tls_slave(&c, &m, "bad", pki.client(None), Some("wrong-token")).await;
    wait_state(&bad, "AuthRejected", |s| {
        *s == SlaveState::Halted(HaltReason::AuthRejected)
    })
    .await;
    let none = tls_slave(&c, &m, "none", pki.client(None), None).await;
    wait_state(&none, "AuthRejected", |s| {
        *s == SlaveState::Halted(HaltReason::AuthRejected)
    })
    .await;
    assert!(matches!(
        bad.wait_for_seq(1, support::WAIT).await,
        Err(Error::Halted(HaltReason::AuthRejected))
    ));
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bootstrap_can_be_denied_per_method() {
    let c = Cluster::new();
    let pki = Pki::new();
    let token = Arc::new(TokenAuthenticator::new(["t"]));
    let auth = Arc::new(AllowMethods::new(token, &[Method::Handshake, Method::Tail]));
    let m = tls_master(&c, &pki, false, auth).await;
    let s = tls_slave(&c, &m, "b", pki.client(None), Some("t")).await;
    wait_state(&s, "AuthRejected", |st| {
        *st == SlaveState::Halted(HaltReason::AuthRejected)
    })
    .await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn token_authentication_on_plaintext_is_refused_unless_allowed() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    cfg.serve.as_mut().unwrap().authenticator = Arc::new(TokenAuthenticator::new(["t"]));
    assert!(matches!(
        Master::open(cfg.clone()).await,
        Err(Error::Config(_))
    ));

    cfg.serve.as_mut().unwrap().allow_plaintext_token = true;
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    let (mut scfg, _) = c.slave_config("b", m.serve_addr().unwrap());
    scfg.serve = None;
    scfg.upstream.credentials = Arc::new(TokenCredentials::new("t"));
    let s = Slave::open(scfg).await.unwrap();
    assert_converged(&m, &[("b", &s)]).await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn mutual_tls_allow_list_by_certificate_subject() {
    let c = Cluster::new();
    let pki = Pki::new();
    let allow = MtlsSubjectAllowList::new()
        .allow_all_methods("node-b")
        .allow("node-ro", &[Method::Handshake, Method::Tail]);
    let m = tls_master(&c, &pki, true, Arc::new(allow)).await;

    let b = tls_slave(
        &c,
        &m,
        "b",
        pki.client(Some(pki.identity(&[], "node-b"))),
        None,
    )
    .await;
    assert_converged(&m, &[("b", &b)]).await;

    let intruder = tls_slave(
        &c,
        &m,
        "x",
        pki.client(Some(pki.identity(&[], "intruder"))),
        None,
    )
    .await;
    wait_state(&intruder, "AuthRejected", |s| {
        *s == SlaveState::Halted(HaltReason::AuthRejected)
    })
    .await;

    let read_only = tls_slave(
        &c,
        &m,
        "ro",
        pki.client(Some(pki.identity(&[], "node-ro"))),
        None,
    )
    .await;
    wait_state(&read_only, "AuthRejected on Bootstrap", |s| {
        *s == SlaveState::Halted(HaltReason::AuthRejected)
    })
    .await;

    // Without a client certificate the TLS handshake itself fails: a transport error, retried.
    let anonymous = tls_slave(&c, &m, "anon", pki.client(None), None).await;
    wait_until("a transport error", || {
        anonymous.status().last_error.is_some()
    })
    .await;
    assert_eq!(anonymous.status().state, SlaveState::Connecting);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_wrong_ca_is_a_retried_transport_error() {
    let c = Cluster::new();
    let pki = Pki::new();
    let other = Pki::new();
    let m = tls_master(&c, &pki, false, Arc::new(replicated_rocksdb::AllowAll)).await;
    let s = tls_slave(&c, &m, "b", other.client(None), None).await;
    wait_until("a transport error", || s.status().last_error.is_some()).await;
    assert_eq!(s.status().state, SlaveState::Connecting);
    let ok = tls_slave(&c, &m, "ok", pki.client(None), None).await;
    assert_converged(&m, &[("ok", &ok)]).await;
}

/// I2: a flood of bad tokens from 127.0.0.1 throttles the address, but a correctly configured
/// replica behind the same address is answered `BUSY` (retryable), not `UNAUTHENTICATED`: it
/// keeps retrying with backoff, never halts `AuthRejected`, and converges once the window passes.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn an_auth_failure_flood_throttles_but_never_halts_a_good_replica_on_the_same_address() {
    let c = Cluster::new();
    let mut cfg = c.master_config("a");
    let serve = cfg.serve.as_mut().unwrap();
    serve.authenticator = Arc::new(TokenAuthenticator::new(["good"]));
    serve.allow_plaintext_token = true;
    serve.limits.max_auth_failures_per_minute = 3;
    serve.limits.auth_failure_window = std::time::Duration::from_secs(2);
    let m = Master::open(cfg).await.unwrap();
    m.create_cf(CfSpec::new("c")).unwrap();
    m.put("c", b"k", b"v").unwrap();
    let addr = m.serve_addr().unwrap();

    let mut bad = replicated_rocksdb::UpstreamConfig::from_addr(addr);
    bad.credentials = Arc::new(TokenCredentials::new("bad"));
    for _ in 0..10 {
        assert!(
            replicated_rocksdb::admin::node_status(&bad, "attacker")
                .await
                .is_err()
        );
    }

    let (mut cfg, listener) = c.slave_config("b", addr);
    cfg.serve = None;
    cfg.upstream.credentials = Arc::new(TokenCredentials::new("good"));
    let b = Slave::open(cfg).await.unwrap();
    wait_until("throttled with a retryable error", || {
        b.status()
            .last_error
            .as_deref()
            .is_some_and(|e| e.contains("too many failed authentication"))
    })
    .await;
    assert!(!matches!(b.status().state, SlaveState::Halted(_)));
    assert_converged(&m, &[("b", &b)]).await;
    assert!(
        listener
            .states()
            .iter()
            .all(|s| !matches!(s, SlaveState::Halted(_))),
        "never halted: {:?}",
        listener.states()
    );
}
