//! The upstream connection: a lazily connected channel whose interceptor adds the protocol
//! metadata and credentials to every call (PROTOCOL.md §3.2).

use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::AsciiMetadataValue;
use tonic::service::Interceptor;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::{Channel, Endpoint};
use tonic::{Request, Status};

use super::config::UpstreamConfig;
use super::inner::Inner;
use crate::auth::CredentialsProvider;
use crate::codec::PROTOCOL_VERSION;
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
    let (endpoint, meta) = endpoint_for(
        &inner.upstream(),
        &inner.cfg.identifier,
        inner.cfg.connect_timeout,
    )?;
    let channel = endpoint
        .connect()
        .await
        .map_err(|e| Error::Io(std::io::Error::other(e.to_string())))?;
    Ok(ReplicationClient::with_interceptor(channel, meta))
}

/// A client for `up`, identifying itself as `identifier`.
pub(crate) fn connect_upstream(
    up: &UpstreamConfig,
    identifier: &str,
    connect_timeout: Duration,
) -> Result<Client> {
    let (endpoint, meta) = endpoint_for(up, identifier, connect_timeout)?;
    Ok(ReplicationClient::with_interceptor(
        endpoint.connect_lazy(),
        meta,
    ))
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
