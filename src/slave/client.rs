//! The upstream connection: a lazily connected channel whose interceptor adds the protocol
//! metadata and credentials to every call (PROTOCOL.md §3.2).

use std::sync::Arc;
use std::time::Duration;

use tonic::codec::CompressionEncoding;
use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use super::config::UpstreamConfig;
use super::inner::Inner;
use crate::auth::CredentialsProvider;
use crate::codec::PROTOCOL_VERSION;
use crate::config::WireCompression;
use crate::error::{Error, Result};
use crate::net::{MD_AUTHORIZATION, MD_NODE_ID, MD_PROTOCOL_VERSION};
use crate::pb::replication_client::ReplicationClient;

#[derive(Clone)]
pub(crate) struct CallMeta {
    node_id: AsciiMetadataValue,
    credentials: Arc<dyn CredentialsProvider>,
}

impl Interceptor for CallMeta {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let md = req.metadata_mut();
        md.insert(
            MD_PROTOCOL_VERSION,
            AsciiMetadataValue::from(PROTOCOL_VERSION),
        );
        md.insert(MD_NODE_ID, self.node_id.clone());
        if let Some(token) = self.credentials.token() {
            // An invalid token is refused locally as UNAUTHENTICATED, which halts the slave
            // with `AuthRejected` (not persisted).
            if crate::auth::validate_token(&token).is_err() {
                return Err(Status::unauthenticated(
                    "the configured token has characters outside RFC 6750 b64token",
                ));
            }
            let mut v = b"Bearer ".to_vec();
            v.extend_from_slice(&token);
            let value = AsciiMetadataValue::try_from(v.as_slice()).map_err(|_| {
                Status::unauthenticated("the configured token is not valid header text")
            })?;
            md.insert(MD_AUTHORIZATION, value);
        }
        Ok(req)
    }
}

pub(crate) type Client = ReplicationClient<InterceptedService<Channel, CallMeta>>;

/// A client for the slave's current upstream. Connects on first use and reconnects by itself.
pub(crate) fn connect(inner: &Inner) -> Result<Client> {
    connect_upstream(
        &inner.upstream(),
        &inner.cfg.identifier,
        inner.cfg.connect_timeout,
    )
}

/// A client for the current upstream whose connection is already established (bounded by
/// `connect_timeout`). A verify connects this way before it holds the applier.
pub(crate) async fn connect_now(inner: &Inner) -> Result<Client> {
    let up = inner.upstream();
    let (endpoint, meta) = endpoint_for(&up, &inner.cfg.identifier, inner.cfg.connect_timeout)?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
    Ok(new_client(channel, meta, up.compression))
}

/// A client for `up`, identifying itself as `identifier`.
pub(crate) fn connect_upstream(
    up: &UpstreamConfig,
    identifier: &str,
    connect_timeout: Duration,
) -> Result<Client> {
    let (endpoint, meta) = endpoint_for(up, identifier, connect_timeout)?;
    Ok(new_client(endpoint.connect_lazy(), meta, up.compression))
}

/// The one place a `ReplicationClient` is built: always accepts zstd, sends it when configured.
fn new_client(channel: Channel, meta: CallMeta, compression: WireCompression) -> Client {
    let mut client = ReplicationClient::with_interceptor(channel, meta)
        .accept_compressed(CompressionEncoding::Zstd);
    if let Some(e) = compression.encoding() {
        client = client.send_compressed(e);
    }
    client
}

fn endpoint_for(
    up: &UpstreamConfig,
    identifier: &str,
    connect_timeout: Duration,
) -> Result<(Endpoint, CallMeta)> {
    let mut endpoint = Endpoint::from_shared(up.uri())
        .map_err(|e| Error::Config(format!("upstream address {}: {e}", up.uri())))?
        .connect_timeout(connect_timeout)
        .http2_keep_alive_interval(Duration::from_secs(30))
        .keep_alive_timeout(Duration::from_secs(10))
        .keep_alive_while_idle(true);
    if let Some(tls) = &up.tls {
        endpoint = endpoint
            .tls_config(tls.to_tonic(&up.server))
            .map_err(|e| Error::Config(format!("upstream TLS: {e}")))?;
    }
    crate::config::validate_identifier(identifier)?;
    let node_id = AsciiMetadataValue::try_from(identifier)
        .map_err(|_| Error::Config("identifier is not valid header text".into()))?;
    let meta = CallMeta {
        node_id,
        credentials: up.credentials.clone(),
    };
    Ok((endpoint, meta))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::TlsClientConfig;
    use crate::slave::UpstreamErrorKind as Kind;
    use crate::slave::state::Problem;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};

    /// The problem a handshake with `up` ends in.
    async fn problem(up: UpstreamConfig) -> Problem {
        let mut c = connect_upstream(&up, "probe", Duration::from_secs(2)).unwrap();
        let r = tokio::time::timeout(
            Duration::from_secs(10),
            c.handshake(crate::pb::HandshakeRequest::default()),
        )
        .await
        .expect("an answer within 10 s");
        Problem::from_status(&r.expect_err("not a replication server"))
    }

    /// A TCP server that reads once, writes `reply` and closes.
    async fn raw_server(reply: &'static [u8]) -> u16 {
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = l.local_addr().unwrap().port();
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
        port
    }

    fn tls(port: u16) -> UpstreamConfig {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut p = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        p.is_ca = rcgen::IsCa::Ca(rcgen::BasicConstraints::Unconstrained);
        let mut up = UpstreamConfig::new("127.0.0.1", port);
        up.tls = Some(TlsClientConfig {
            ca_pem: p.self_signed(&key).unwrap().pem().into_bytes(),
            domain_name: Some("localhost".into()),
            identity: None,
        });
        up
    }

    #[tokio::test]
    async fn failed_connections_are_classified() {
        let closed = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let closed_port = closed.local_addr().unwrap().port();
        drop(closed);
        let p = problem(UpstreamConfig::new("127.0.0.1", closed_port)).await;
        assert_eq!(p.kind, Kind::Unreachable, "{p}");
        assert!(p.message.contains("refused"), "the root cause is kept: {p}");

        let hung_up = raw_server(b"").await;
        let p = problem(UpstreamConfig::new("127.0.0.1", hung_up)).await;
        assert_eq!(p.kind, Kind::Unreachable, "{p}");

        let http1 = raw_server(b"HTTP/1.1 400 Bad Request\r\nContent-Length: 0\r\n\r\n").await;
        let p = problem(UpstreamConfig::new("127.0.0.1", http1)).await;
        assert_eq!(p.kind, Kind::NotReplicationService, "{p}");
        let other = raw_server(b"-ERR unknown command\r\n").await;
        let p = problem(UpstreamConfig::new("127.0.0.1", other)).await;
        assert_eq!(p.kind, Kind::NotReplicationService, "{p}");
        // A plaintext client at a TLS port: the server answers with a TLS alert.
        let tls_port = raw_server(&[0x15, 0x03, 0x03, 0x00, 0x02, 0x02, 0x46]).await;
        let p = problem(UpstreamConfig::new("127.0.0.1", tls_port)).await;
        assert_eq!(p.kind, Kind::NotReplicationService, "{p}");

        // A gRPC server without the replication service.
        let l = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let grpc = l.local_addr().unwrap().port();
        tokio::spawn(
            tonic::transport::Server::builder()
                .add_routes(tonic::service::Routes::default())
                .serve_with_incoming(tokio_stream::wrappers::TcpListenerStream::new(l)),
        );
        let p = problem(UpstreamConfig::new("127.0.0.1", grpc)).await;
        assert_eq!(p.kind, Kind::NotReplicationService, "{p}");

        // A TLS client at a plaintext port.
        let p = problem(tls(grpc)).await;
        assert_eq!(p.kind, Kind::Tls, "{p}");
        let p = problem(tls(http1)).await;
        assert_eq!(p.kind, Kind::Tls, "{p}");
    }

    #[test]
    fn replication_answers_are_upstream_problems() {
        let p = Problem::from_status(&Status::unavailable("x").clone());
        assert_eq!(p.kind, Kind::Unreachable);
        for s in [
            Status::failed_precondition("STALE_EPOCH"),
            Status::resource_exhausted("busy"),
            Status::internal("oops"),
        ] {
            assert_eq!(Problem::from_status(&s).kind, Kind::Upstream, "{s:?}");
        }
        assert_eq!(
            Problem::from_status(&Status::internal("oops")).message,
            "Internal: oops"
        );
    }
}
