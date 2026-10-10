//! Replication over TLS with a bearer-token check.
//!
//! The master presents a server certificate (the slave verifies it against a CA it trusts)
//! and requires a secret token from every replica. All certificates are generated at
//! runtime with `rcgen`, so the example is self-contained. Do not do this in production:
//! use certificates from your own PKI and load tokens from a secret store.
//!
//! Run with: `cargo run --example secure_replication`

use std::error::Error;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use replicated_rocksdb::{
    CfSpec, HaltReason, Master, MasterConfig, ReadApi, ServeConfig, Slave, SlaveConfig,
    TlsClientConfig, TlsIdentity, TlsServerConfig, TokenAuthenticator, TokenCredentials,
    UpstreamConfig,
};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let dir = tempfile::tempdir()?;

    // ---- 1. A throw-away CA, and a server certificate for "localhost" signed by it. -------
    let ca_key = rcgen::KeyPair::generate()?;
    let mut ca_params = rcgen::CertificateParams::new(Vec::<String>::new())?;
    ca_params.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
    ca_params
        .distinguished_name
        .push(rcgen::DnType::CommonName, "example CA");
    let ca_cert = ca_params.self_signed(&ca_key)?;
    let ca_pem = ca_cert.pem().into_bytes();
    let issuer = rcgen::Issuer::new(ca_params, ca_key);

    let server_key = rcgen::KeyPair::generate()?;
    let server_cert = rcgen::CertificateParams::new(vec!["localhost".to_string()])?
        .signed_by(&server_key, &issuer)?;

    // ---- 2. Master: TLS + token authentication. -------------------------------------------
    let mut serve = ServeConfig::new(
        IpAddr::V4(Ipv4Addr::LOCALHOST),
        0,
        // Several secrets may be valid at once, which allows zero-downtime rotation:
        // add the new token, move the replicas over, then remove the old one.
        Arc::new(TokenAuthenticator::new(["s3cret-token"])),
    );
    serve.tls = Some(TlsServerConfig {
        identity: TlsIdentity {
            cert_pem: server_cert.pem().into_bytes(),
            key_pem: server_key.serialize_pem().into_bytes(),
        },
        // `None`: clients are not asked for a certificate (the token authenticates them).
        //
        // For MUTUAL TLS set this to `Some(ca_pem)` so replicas must present a client
        // certificate signed by that CA, give each replica `TlsClientConfig::identity`, and
        // swap the authenticator for `MtlsSubjectAllowList::new().allow_all_methods("replica-1")`
        // (or `.allow("monitor", &[Method::Handshake, Method::Tail])` for a read-only peer).
        // Then the certificate subject, not a shared secret, decides who may replicate.
        client_ca_pem: None,
    });
    // Without TLS, token auth is refused at open() unless `allow_plaintext_token` is set,
    // because the token would travel in the clear.
    let mut mcfg = MasterConfig::new("secure-master", dir.path().join("master"));
    mcfg.serve = Some(serve);
    let master = Master::open(mcfg).await?;
    let addr = master.serve_addr().unwrap();
    master.create_cf(CfSpec::new("secrets"))?;
    let seq = master.put("secrets", b"k", b"replicated over TLS")?;
    println!("master serving TLS on {addr}");

    // ---- 3. A replica that trusts the CA and knows the token. -----------------------------
    let upstream_with = |token: &str| {
        let mut up = UpstreamConfig::from_addr(addr);
        up.tls = Some(TlsClientConfig {
            ca_pem: ca_pem.clone(),
            // We dial 127.0.0.1 but the certificate says "localhost": name what to verify.
            domain_name: Some("localhost".into()),
            identity: None, // client certificate, only needed for mutual TLS
        });
        up.credentials = Arc::new(TokenCredentials::new(token));
        up
    };
    let good = Slave::open(SlaveConfig::new(
        "good-replica",
        dir.path().join("good"),
        upstream_with("s3cret-token"),
    ))
    .await?;
    good.wait_for_seq(seq, Duration::from_secs(10)).await?;
    println!(
        "replica with the right token has: {:?}",
        good.get("secrets", b"k")?
            .map(|v| String::from_utf8_lossy(&v).into_owned())
    );

    // ---- 4. A replica with the wrong token is rejected and halts (no retry storm). --------
    let bad = Slave::open(SlaveConfig::new(
        "bad-replica",
        dir.path().join("bad"),
        upstream_with("wrong-token"),
    ))
    .await?;
    // `wait_streaming` reports the halt instead of waiting forever.
    match bad.wait_streaming(Duration::from_secs(10)).await {
        Err(replicated_rocksdb::Error::Halted(HaltReason::AuthRejected)) => {}
        other => panic!("expected an AuthRejected halt, got {other:?}"),
    }
    println!("replica with the wrong token: {:?}", bad.status().state);

    bad.shutdown().await?;
    good.shutdown().await?;
    master.shutdown().await?;
    println!("done");
    Ok(())
}
