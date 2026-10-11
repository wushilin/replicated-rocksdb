//! Read-only HTTP endpoints on the replication port (`ServeConfig::http_status`, DESIGN.md
//! §15 and §19): `/v1/status`, `/v1/health`, `/v1/ready` and `/metrics`.
//!
//! They share the listener, the TLS configuration and the authorization path of the gRPC
//! service: every request is checked as [`Method::Status`] through
//! [`ServerState::check_access`] (same authenticator, same per-address failure limiter). They
//! never read a request body, and answer from the same [`status::node_info`] as the `Status`
//! RPC.

use std::convert::Infallible;
use std::sync::Arc;

use tonic::codegen::http::header::{ALLOW, CACHE_CONTROL, CONTENT_TYPE, WWW_AUTHENTICATE};
use tonic::codegen::http::{HeaderValue, Method as HttpMethod, Request, Response, StatusCode};
use tonic::codegen::{Body as _, Context, Future, Pin, Poll, Service};
use tonic::metadata::MetadataMap;
use tonic::service::{AxumBody, AxumRouter, Routes};

use super::{Denied, ServerState, Svc, peer_info, status};
use crate::admin::NodeInfo;
use crate::auth::{Method, PeerInfo};
use crate::config::validate_identifier;
use crate::net::{self, MD_NODE_ID};
use crate::pb::replication_server::ReplicationServer;
use crate::source::LogSource;

/// The peer id an HTTP caller gets when it sends no valid `x-rrdb-node-id` (and has no
/// client certificate). Unverified, like every self-asserted id.
const ANONYMOUS_PEER: &str = "http";

/// The gRPC service plus the status endpoints, which answer every path the service does not.
pub(super) fn routes(state: Arc<ServerState>, grpc: ReplicationServer<Svc>) -> Routes {
    Routes::from(AxumRouter::new().fallback_service(Endpoints(state))).add_service(grpc)
}

/// Why an HTTP request was not authorized.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum HttpAuthError {
    /// 401: no token, or not a valid one.
    Unauthenticated,
    /// 403: the authenticator does not allow `Status` to this caller.
    PermissionDenied,
    /// 429: too many failed attempts from this address; the credentials were not checked.
    Throttled,
    /// 503: the node is gone (shutting down).
    Unavailable,
}

impl HttpAuthError {
    fn status_code(self) -> StatusCode {
        match self {
            HttpAuthError::Unauthenticated => StatusCode::UNAUTHORIZED,
            HttpAuthError::PermissionDenied => StatusCode::FORBIDDEN,
            HttpAuthError::Throttled => StatusCode::TOO_MANY_REQUESTS,
            HttpAuthError::Unavailable => StatusCode::SERVICE_UNAVAILABLE,
        }
    }

    fn message(self) -> &'static str {
        match self {
            HttpAuthError::Unauthenticated => "authentication failed",
            HttpAuthError::PermissionDenied => "Status is not allowed",
            HttpAuthError::Throttled => {
                "too many failed authentication attempts from this address; retry later"
            }
            HttpAuthError::Unavailable => "node is shutting down",
        }
    }
}

/// Authorizes an HTTP request as the `Status` method: the bearer token of `authorization`
/// (PROTOCOL.md §3.2), `peer` as gRPC sees it, and the same failure limiter.
pub(crate) async fn authorize_http(
    state: &ServerState,
    peer: PeerInfo,
    authorization: Option<&HeaderValue>,
) -> Result<Arc<dyn LogSource>, HttpAuthError> {
    let token = authorization.and_then(|v| net::parse_bearer(v.as_bytes()));
    state
        .check_access(&peer, token.as_deref(), Method::Status)
        .await
        .map_err(|denied| match denied {
            Denied::Throttled => HttpAuthError::Throttled,
            Denied::Unauthenticated => HttpAuthError::Unauthenticated,
            Denied::Forbidden => HttpAuthError::PermissionDenied,
            Denied::Gone(_) => HttpAuthError::Unavailable,
        })
}

/// The tower service behind every non-gRPC path.
#[derive(Clone)]
struct Endpoints(Arc<ServerState>);

impl Service<Request<AxumBody>> for Endpoints {
    type Response = Response<AxumBody>;
    type Error = Infallible;
    type Future = Pin<Box<dyn Future<Output = Result<Response<AxumBody>, Infallible>> + Send>>;

    fn poll_ready(&mut self, _cx: &mut Context<'_>) -> Poll<Result<(), Infallible>> {
        Poll::Ready(Ok(()))
    }

    fn call(&mut self, req: Request<AxumBody>) -> Self::Future {
        let state = self.0.clone();
        Box::pin(async move { Ok(handle(&state, req).await) })
    }
}

#[derive(Clone, Copy)]
enum Endpoint {
    Status,
    Health,
    Ready,
    Metrics,
}

async fn handle(state: &ServerState, req: Request<AxumBody>) -> Response<AxumBody> {
    let (mut parts, body) = req.into_parts();
    // The body is never read: only whether there is one, before it is dropped.
    let has_body = !body.is_end_stream();
    drop(body);
    let claimed = parts
        .headers
        .get(MD_NODE_ID)
        .and_then(|v| v.to_str().ok())
        .filter(|id| validate_identifier(id).is_ok())
        .unwrap_or(ANONYMOUS_PEER)
        .to_string();
    // Connection info (remote address, client certificate) is in the request extensions,
    // where tonic put it for the gRPC service too.
    let conn = tonic::Request::from_parts(
        MetadataMap::new(),
        std::mem::take(&mut parts.extensions),
        (),
    );
    let peer = peer_info(&conn, claimed);
    let src = match authorize_http(state, peer, parts.headers.get("authorization")).await {
        Ok(src) => src,
        Err(e) => {
            let mut r = error(e.status_code(), e.message());
            if e == HttpAuthError::Unauthenticated {
                r.headers_mut()
                    .insert(WWW_AUTHENTICATE, HeaderValue::from_static("Bearer"));
            }
            return r;
        }
    };
    let endpoint = match parts.uri.path() {
        "/v1/status" => Endpoint::Status,
        "/v1/health" => Endpoint::Health,
        "/v1/ready" => Endpoint::Ready,
        "/metrics" => Endpoint::Metrics,
        _ => return error(StatusCode::NOT_FOUND, "not found"),
    };
    if parts.method != HttpMethod::GET {
        let mut r = error(StatusCode::METHOD_NOT_ALLOWED, "only GET is allowed");
        r.headers_mut()
            .insert(ALLOW, HeaderValue::from_static("GET"));
        return r;
    }
    if has_body {
        return error(StatusCode::BAD_REQUEST, "a request body is not allowed");
    }
    if let Endpoint::Health = endpoint {
        return json(StatusCode::OK, &serde_json::json!({ "ok": true }));
    }
    // `node_info` reads the log and `__meta`: a blocking thread, as for the `Status` RPC.
    let replicas = state.replicas.clone();
    let lag_max = state.cfg.limits.replica_lag_time_max;
    let info: NodeInfo = match tokio::task::spawn_blocking(move || {
        status::node_info(&*src, &replicas, lag_max, &[])
    })
    .await
    {
        Ok(info) => info,
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "status task failed"),
    };
    match endpoint {
        Endpoint::Status => json(StatusCode::OK, &status::to_json(&info)),
        Endpoint::Ready => {
            let ready = status::is_ready(&info);
            let code = if ready {
                StatusCode::OK
            } else {
                StatusCode::SERVICE_UNAVAILABLE
            };
            json(
                code,
                &serde_json::json!({ "ready": ready, "node_state": info.node_state }),
            )
        }
        Endpoint::Metrics => respond(
            StatusCode::OK,
            "text/plain; version=0.0.4; charset=utf-8",
            status::to_prometheus(&info),
        ),
        Endpoint::Health => unreachable!("answered above"),
    }
}

fn respond(code: StatusCode, content_type: &'static str, body: String) -> Response<AxumBody> {
    let mut r = Response::new(AxumBody::from(body));
    *r.status_mut() = code;
    let h = r.headers_mut();
    h.insert(CONTENT_TYPE, HeaderValue::from_static(content_type));
    h.insert(CACHE_CONTROL, HeaderValue::from_static("no-store"));
    r
}

fn json(code: StatusCode, value: &impl serde::Serialize) -> Response<AxumBody> {
    match serde_json::to_string(value) {
        Ok(body) => respond(code, "application/json", body),
        Err(_) => error(StatusCode::INTERNAL_SERVER_ERROR, "encoding failed"),
    }
}

fn error(code: StatusCode, message: &str) -> Response<AxumBody> {
    let body = serde_json::json!({ "error": message }).to_string();
    respond(code, "application/json", body)
}

#[cfg(test)]
mod tests {
    use std::net::SocketAddr;
    use std::sync::Arc;

    use super::*;
    use crate::auth::{AllowMethods, TokenAuthenticator};
    use crate::server::test_client::serving_master;

    fn peer(port: u16) -> PeerInfo {
        PeerInfo {
            peer_id: ANONYMOUS_PEER.into(),
            peer_id_verified: false,
            remote_addr: SocketAddr::from(([127, 0, 0, 1], port)),
            client_cert: None,
        }
    }

    fn bearer(t: &str) -> HeaderValue {
        HeaderValue::from_str(&format!("Bearer {t}")).unwrap()
    }

    async fn result(
        m: &crate::Master,
        auth: Option<HeaderValue>,
    ) -> Result<Arc<dyn LogSource>, HttpAuthError> {
        authorize_http(m.server_state(), peer(1), auth.as_ref()).await
    }

    #[tokio::test]
    async fn tokens_and_the_failure_limiter_are_shared_with_grpc() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| {
            let serve = cfg.serve.as_mut().unwrap();
            serve.authenticator = Arc::new(TokenAuthenticator::new(["good"]));
            serve.allow_plaintext_token = true;
            serve.limits.max_auth_failures_per_minute = 3;
        })
        .await;
        assert!(result(&m, Some(bearer("good"))).await.is_ok());
        assert_eq!(
            result(&m, None).await.err(),
            Some(HttpAuthError::Unauthenticated)
        );
        // Not the `Bearer` scheme: no token.
        assert_eq!(
            result(&m, Some(HeaderValue::from_static("Basic Z29vZA==")))
                .await
                .err(),
            Some(HttpAuthError::Unauthenticated)
        );
        assert_eq!(
            result(&m, Some(bearer("bad"))).await.err(),
            Some(HttpAuthError::Unauthenticated)
        );
        // Three failures from 127.0.0.1: even the good token is throttled now.
        assert_eq!(
            result(&m, Some(bearer("good"))).await.err(),
            Some(HttpAuthError::Throttled)
        );
        // The same limiter blocks gRPC from that address.
        let state = m.server_state();
        let call = crate::server::CallInfo {
            peer: peer(2),
            token: Some(b"good".to_vec()),
            node_id: "b".into(),
        };
        let err = state.authorize(&call, Method::Status).await.err().unwrap();
        assert_eq!(
            crate::net::reason_of(&err),
            Some(crate::pb::Reason::Busy),
            "{err:?}"
        );
    }

    #[tokio::test]
    async fn grpc_failures_count_toward_the_http_limit() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| {
            let serve = cfg.serve.as_mut().unwrap();
            serve.authenticator = Arc::new(TokenAuthenticator::new(["good"]));
            serve.allow_plaintext_token = true;
            serve.limits.max_auth_failures_per_minute = 2;
        })
        .await;
        let state = m.server_state();
        for _ in 0..2 {
            let call = crate::server::CallInfo {
                peer: peer(2),
                token: Some(b"bad".to_vec()),
                node_id: "b".into(),
            };
            assert!(state.authorize(&call, Method::Tail).await.is_err());
        }
        assert_eq!(
            result(&m, Some(bearer("good"))).await.err(),
            Some(HttpAuthError::Throttled)
        );
    }

    #[tokio::test]
    async fn a_method_restriction_without_status_is_forbidden() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| {
            let serve = cfg.serve.as_mut().unwrap();
            let token = Arc::new(TokenAuthenticator::new(["good"]));
            serve.authenticator =
                Arc::new(AllowMethods::new(token, &[Method::Handshake, Method::Tail]));
            serve.allow_plaintext_token = true;
        })
        .await;
        assert_eq!(
            result(&m, Some(bearer("good"))).await.err(),
            Some(HttpAuthError::PermissionDenied)
        );
        // A forbidden method is not an authentication failure: never throttled.
        for _ in 0..20 {
            assert_eq!(
                result(&m, Some(bearer("good"))).await.err(),
                Some(HttpAuthError::PermissionDenied)
            );
        }
    }

    #[tokio::test]
    async fn the_mtls_peer_identity_reaches_the_authenticator() {
        use crate::auth::MtlsSubjectAllowList;
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |cfg| {
            let serve = cfg.serve.as_mut().unwrap();
            serve.authenticator =
                Arc::new(MtlsSubjectAllowList::new().allow("ops", &[Method::Status]));
        })
        .await;
        let state = m.server_state();
        // `peer_info` turns a verified certificate's CN into the peer id; here the
        // authenticator sees the same `PeerInfo` gRPC would build.
        let verified = PeerInfo {
            peer_id: "ops".into(),
            peer_id_verified: true,
            ..peer(1)
        };
        assert!(authorize_http(state, verified, None).await.is_ok());
        let claimed = PeerInfo {
            peer_id: "ops".into(),
            ..peer(1)
        };
        assert_eq!(
            authorize_http(state, claimed, None).await.err(),
            Some(HttpAuthError::Unauthenticated)
        );
    }

    #[tokio::test]
    async fn a_gone_node_is_unavailable() {
        let dir = tempfile::tempdir().unwrap();
        let (m, _) = serving_master(dir.path(), |_| {}).await;
        let state = m.server_state().clone();
        m.shutdown().await.unwrap();
        assert_eq!(
            authorize_http(&state, peer(1), None).await.err(),
            Some(HttpAuthError::Unavailable)
        );
    }
}
