//! The replication gRPC server (PROTOCOL.md §6–§8), shared by the master and by slaves that
//! serve downstream replicas. It runs over a [`LogSource`] and holds it weakly, so a running
//! server never keeps a closed node's database open.

pub(crate) mod bootstrap;
pub(crate) mod handshake;
pub(crate) mod replicas;
pub(crate) mod status;
mod tail;
pub(crate) mod verify;
pub(crate) use tail::DEFAULT_MAX_BATCH_BYTES;
#[cfg(test)]
pub(crate) use tail::READ_BATCH;
#[cfg(test)]
pub(crate) mod test_client;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, PoisonError, Weak};
use std::time::{Duration, Instant};

use tokio::net::TcpListener;
use tokio::sync::{Semaphore, mpsc, watch};
use tokio::task::JoinHandle;
use tokio_stream::wrappers::{ReceiverStream, TcpListenerStream};
use tonic::transport::Server;
use tonic::{Request, Response, Status};

use crate::auth::{AuthError, FailureLimiter, Method, PeerInfo, subject_common_name};
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::config::{ServeConfig, validate_identifier};
use crate::error::{Error, Result};
use crate::net::convert::{cf_to_pb, epochs_to_pb, identity_to_pb};
use crate::net::{self, reason_status};
use crate::pb;
use crate::pb::replication_server::{Replication, ReplicationServer};
use crate::source::LogSource;
use crate::store::retention::RetentionGuard;
use crate::types::{Role, Seq};
use replicas::ReplicaRegistry;

/// A bootstrap's retention guard, kept after the footer until the client's Tail presents
/// the lease or the lease expires (PROTOCOL.md §8.4).
pub(crate) struct Lease {
    pub client_id: String,
    pub snapshot_seq: Seq,
    pub expires_at: Instant,
    _guard: RetentionGuard,
}

pub(crate) struct ServerState {
    source: Weak<dyn LogSource>,
    pub(crate) cfg: ServeConfig,
    leases: Mutex<HashMap<[u8; 16], Lease>>,
    bootstraps: Arc<Semaphore>,
    pub(crate) shutdown: watch::Receiver<bool>,
    auth_failures: FailureLimiter,
    /// The node's downstream replicas (shared with the node, which reports and waits on them).
    pub(crate) replicas: Arc<ReplicaRegistry>,
    /// Open verify sessions; each owns a snapshot (PROTOCOL.md §9).
    pub(crate) verify_sessions: verify::Sessions,
    /// Permits for running verify computations; `shutdown` takes them all.
    pub(crate) compute_permits: Arc<Semaphore>,
    /// Set on shutdown: running verify scans stop at their next key.
    pub(crate) cancel: Arc<AtomicBool>,
}

impl ServerState {
    fn source(&self) -> Result<Arc<dyn LogSource>, Status> {
        self.source
            .upgrade()
            .ok_or_else(|| reason_status(pb::Reason::ShuttingDown, "node is shutting down"))
    }

    pub(crate) fn is_shutting_down(&self) -> bool {
        *self.shutdown.borrow()
    }

    fn leases(&self) -> std::sync::MutexGuard<'_, HashMap<[u8; 16], Lease>> {
        self.leases.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub(crate) fn insert_lease(
        &self,
        id: [u8; 16],
        client_id: String,
        snapshot_seq: Seq,
        guard: RetentionGuard,
    ) {
        let expires_at = Instant::now() + self.cfg.limits.bootstrap_lease_ttl;
        self.leases().insert(
            id,
            Lease {
                client_id,
                snapshot_seq,
                expires_at,
                _guard: guard,
            },
        );
    }

    pub(crate) fn remove_lease(&self, id: &[u8; 16]) {
        self.leases().remove(id);
    }

    /// Takes the lease presented by `client_id`'s Tail from `from_seq` (PROTOCOL.md §7.2),
    /// if it matches. The Tail producer keeps it until it has read the entries after the
    /// snapshot, so aggressive retention cannot trim `S + 1` in between.
    pub(crate) fn take_lease(
        &self,
        lease_id: &[u8],
        client_id: &str,
        from_seq: Seq,
    ) -> Option<Lease> {
        let id = <[u8; 16]>::try_from(lease_id).ok()?;
        let mut leases = self.leases();
        let matches = leases
            .get(&id)
            .is_some_and(|l| l.client_id == client_id && from_seq <= l.snapshot_seq + 1);
        if matches { leases.remove(&id) } else { None }
    }

    pub(crate) fn expire_leases(&self, now: Instant) {
        self.leases().retain(|_, l| l.expires_at > now);
    }

    /// Authenticates one call (PROTOCOL.md §3.3) and returns the node it may read.
    async fn authorize(
        &self,
        call: &CallInfo,
        method: Method,
    ) -> Result<Arc<dyn LogSource>, Status> {
        let ip = call.peer.remote_addr.ip();
        if self.auth_failures.is_blocked(ip, Instant::now()) {
            // Throttled, not rejected: the credentials were not even checked. A retryable
            // answer, so a good replica behind the same address backs off instead of halting
            // `AuthRejected` (which would let anyone with network access stop it).
            return Err(reason_status(
                pb::Reason::Busy,
                "too many failed authentication attempts from this address; retry later",
            ));
        }
        match self
            .cfg
            .authenticator
            .authenticate(&call.peer, method, call.token.as_deref())
            .await
        {
            Ok(()) => self.source(),
            Err(AuthError::Unauthenticated) => {
                self.auth_failures.record_failure(ip, Instant::now());
                Err(Status::unauthenticated("authentication failed"))
            }
            Err(AuthError::PermissionDenied) => Err(Status::permission_denied(format!(
                "{method:?} is not allowed"
            ))),
        }
    }
}

/// What a handler extracts from the request before any `.await`.
pub(crate) struct CallInfo {
    pub peer: PeerInfo,
    pub token: Option<Vec<u8>>,
    /// `x-rrdb-node-id`, validated.
    pub node_id: String,
}

impl CallInfo {
    /// The request's `client_id` must be a valid id and equal the `x-rrdb-node-id` metadata.
    pub(crate) fn check_client_id(&self, client_id: &str) -> Result<(), Status> {
        validate_identifier(client_id)
            .map_err(|e| net::invalid_argument(format!("client_id: {e}")))?;
        if client_id != self.node_id {
            return Err(net::invalid_argument(
                "client_id must equal the x-rrdb-node-id metadata",
            ));
        }
        Ok(())
    }
}

/// Checks the protocol version metadata (except for Handshake), validates the node id
/// metadata and collects peer info.
pub(crate) fn call_info<T>(req: &Request<T>, method: Method) -> Result<CallInfo, Status> {
    if method != Method::Handshake {
        net::check_protocol_version(req.metadata())?;
    }
    let node_id = net::node_id(req.metadata())
        .ok_or_else(|| net::invalid_argument("x-rrdb-node-id is required"))?;
    validate_identifier(&node_id)
        .map_err(|e| net::invalid_argument(format!("x-rrdb-node-id: {e}")))?;
    let remote_addr = req
        .remote_addr()
        .unwrap_or_else(|| SocketAddr::from(([0, 0, 0, 0], 0)));
    let client_cert = req
        .peer_certs()
        .and_then(|certs| certs.first().map(|c| c.as_ref().to_vec()));
    let peer = match client_cert.as_deref().and_then(subject_common_name) {
        Some(cn) => PeerInfo {
            peer_id: cn,
            peer_id_verified: true,
            remote_addr,
            client_cert,
        },
        None => PeerInfo {
            peer_id: node_id.clone(),
            peer_id_verified: false,
            remote_addr,
            client_cert,
        },
    };
    Ok(CallInfo {
        peer,
        token: net::bearer_token(req.metadata()),
        node_id,
    })
}

/// Maps a store error to a status. Corruption and gaps keep their protocol reasons.
pub(crate) fn store_status(e: Error) -> Status {
    match e {
        Error::Corrupt(m) => reason_status(pb::Reason::Corrupt, m),
        e @ Error::Gap { .. } => reason_status(pb::Reason::LogGap, e.to_string()),
        other => net::internal_status(other.to_string()),
    }
}

struct Svc(Arc<ServerState>);

pub(crate) type TailStream = ReceiverStream<Result<pb::TailMessage, Status>>;
pub(crate) type BootstrapStream = ReceiverStream<Result<pb::BootstrapChunk, Status>>;
pub(crate) type VerifyRangeStream = ReceiverStream<Result<pb::VerifyRangeResponse, Status>>;

#[tonic::async_trait]
impl Replication for Svc {
    async fn handshake(
        &self,
        req: Request<pb::HandshakeRequest>,
    ) -> Result<Response<pb::HandshakeResponse>, Status> {
        let call = call_info(&req, Method::Handshake)?;
        let src = self.0.authorize(&call, Method::Handshake).await?;
        let r = req.into_inner();
        call.check_client_id(&r.client_id)?;
        if let Some(known) = &r.known_identity {
            crate::net::convert::identity_from_pb(known)
                .map_err(|_| net::invalid_argument("known_identity.db_uuid must be 16 bytes"))?;
        }
        handshake::check_known_epochs(&r).map_err(net::invalid_argument)?;
        // `server_view` reads the log (the CRC at the client's last seq): a blocking thread.
        let (s, client_last) = (src.clone(), r.last_seq);
        let view = tokio::task::spawn_blocking(move || server_view(&*s, client_last))
            .await
            .map_err(|e| net::internal_status(format!("handshake task failed: {e}")))??;
        let v = handshake::decide(&view, &r);
        let span = tracing::info_span!(
            "handshake",
            node_id = %view.server_id,
            upstream = view.upstream_chain.first().map(String::as_str),
            client = %r.client_id,
            seq = view.last_seq,
            epoch = view.identity.map(|id| id.epoch),
        );
        span.in_scope(|| {
            tracing::info!(
                client_last_seq = r.last_seq,
                decision = v.decision.as_str_name(),
                reason = v.reason.as_str_name(),
                detail = %v.detail,
                "handshake"
            )
        });
        let identity = view.identity.filter(|_| v.reason != pb::Reason::NotReady);
        Ok(Response::new(pb::HandshakeResponse {
            protocol_version: PROTOCOL_VERSION,
            envelope_version: u32::from(ENVELOPE_VERSION),
            server_id: view.server_id,
            decision: v.decision as i32,
            reason: v.reason as i32,
            detail: v.detail,
            upstream_chain: view.upstream_chain,
            identity: identity.as_ref().map(identity_to_pb),
            cf_specs: src
                .store()
                .cfs
                .live_entries()
                .iter()
                .map(cf_to_pb)
                .collect(),
            first_retained_seq: view.first_retained_seq,
            last_seq: view.last_seq,
            origin_last_seq: src.origin_last_seq(),
            max_entry_bytes: src.max_entry_bytes(),
            heartbeat_ms: u32::try_from(src.heartbeat().as_millis()).unwrap_or(u32::MAX),
            epoch_history: epochs_to_pb(&view.epoch_history),
        }))
    }

    type TailStream = TailStream;

    async fn tail(
        &self,
        req: Request<tonic::Streaming<pb::TailClientMessage>>,
    ) -> Result<Response<TailStream>, Status> {
        tail::tail(&self.0, req).await
    }

    type BootstrapStream = BootstrapStream;

    async fn bootstrap(
        &self,
        req: Request<pb::BootstrapRequest>,
    ) -> Result<Response<BootstrapStream>, Status> {
        bootstrap::bootstrap(&self.0, req).await
    }

    async fn verify_begin(
        &self,
        req: Request<pb::VerifyBeginRequest>,
    ) -> Result<Response<pb::VerifyBeginResponse>, Status> {
        verify::begin(&self.0, req).await
    }

    type VerifyRangeStream = VerifyRangeStream;

    async fn verify_range(
        &self,
        req: Request<tonic::Streaming<pb::VerifyRangeRequest>>,
    ) -> Result<Response<VerifyRangeStream>, Status> {
        verify::range(&self.0, req).await
    }

    async fn status(
        &self,
        req: Request<pb::StatusRequest>,
    ) -> Result<Response<pb::StatusResponse>, Status> {
        let call = call_info(&req, Method::Status)?;
        let src = self.0.authorize(&call, Method::Status).await?;
        let r = req.into_inner();
        call.check_client_id(&r.client_id)?;
        let probes: Vec<Seq> = r.probe_seq.into_iter().collect();
        // `node_info` reads the log and `__meta`: a blocking thread.
        let replicas = self.0.replicas.clone();
        let lag_max = self.0.cfg.limits.replica_lag_time_max;
        let info = tokio::task::spawn_blocking(move || {
            status::node_info(&*src, &replicas, lag_max, &probes)
        })
        .await
        .map_err(|e| net::internal_status(format!("status task failed: {e}")))?;
        Ok(Response::new(status::to_pb(&info, r.probe_seq)))
    }

    async fn verify_end(
        &self,
        req: Request<pb::VerifyEndRequest>,
    ) -> Result<Response<pb::VerifyEndResponse>, Status> {
        verify::end(&self.0, req).await
    }
}

fn server_view(src: &dyn LogSource, client_last_seq: Seq) -> Result<handshake::ServerView, Status> {
    let ident = src.identity();
    let store = src.store();
    Ok(handshake::ServerView {
        server_id: src.node_id().to_string(),
        is_master: src.role() == Role::Master,
        ready: src.is_ready(),
        identity: ident.as_ref().map(|(id, _)| *id),
        epoch_history: ident.map(|(_, h)| h).unwrap_or_default(),
        upstream_chain: src.upstream_chain(),
        last_seq: store.position().last_seq,
        first_retained_seq: store.first_retained_seq(),
        crc_at_client_last: store.entry_crc(client_last_seq).map_err(store_status)?,
    })
}

/// A running server. Dropping it signals shutdown without waiting.
pub(crate) struct ServerHandle {
    addr: SocketAddr,
    state: Arc<ServerState>,
    shutdown_tx: watch::Sender<bool>,
    task: Option<JoinHandle<std::result::Result<(), tonic::transport::Error>>>,
    sweep: Option<JoinHandle<()>>,
}

impl ServerHandle {
    pub(crate) fn addr(&self) -> SocketAddr {
        self.addr
    }

    /// How long a replica may go without being fully caught up and still count as in sync.
    pub(crate) fn replica_lag_time_max(&self) -> Duration {
        self.state.cfg.limits.replica_lag_time_max
    }

    /// Ends every stream with `SHUTTING_DOWN`, stops accepting calls, and waits (up to 5 s
    /// each) until every bootstrap job has returned its permit (and dropped its snapshot),
    /// every verify computation has stopped, and the server task has finished. `Err(Timeout)`
    /// if a bootstrap job or a verify computation did not stop within 5 s: it may still hold
    /// the database (the rest of the shutdown still completes).
    pub(crate) async fn shutdown(mut self) -> Result<()> {
        self.shutdown_tx.send_replace(true);
        self.state.cancel.store(true, Ordering::SeqCst);
        // A bootstrap job owns a snapshot, and with it the database.
        let mut stuck = false;
        let all =
            u32::try_from(self.state.cfg.limits.max_concurrent_bootstraps).unwrap_or(u32::MAX);
        if tokio::time::timeout(
            Duration::from_secs(5),
            self.state.bootstraps.acquire_many(all),
        )
        .await
        .is_err()
        {
            tracing::warn!(addr = %self.addr, "bootstrap jobs did not stop in 5 s");
            stuck = true;
        }
        // Verify computations stop at their next key; wait until none is left, because each
        // one references the database through its session's snapshot.
        let computations = u32::try_from(verify::MAX_COMPUTATIONS).unwrap_or(u32::MAX);
        let held = tokio::time::timeout(
            Duration::from_secs(5),
            self.state.compute_permits.acquire_many(computations),
        )
        .await;
        if held.is_err() {
            tracing::warn!(addr = %self.addr, "verify computations did not stop in 5 s");
            stuck = true;
        }
        // Close while still holding the permits, so no request slips in between.
        self.state.compute_permits.close();
        drop(held);
        // A Bootstrap that arrives later on an existing connection is refused.
        self.state.bootstraps.close();
        self.state.leases().clear();
        self.state.verify_sessions.clear();
        if let Some(sweep) = self.sweep.take() {
            let _ = sweep.await;
        }
        if let Some(mut task) = self.task.take() {
            if tokio::time::timeout(Duration::from_secs(5), &mut task)
                .await
                .is_err()
            {
                tracing::warn!(addr = %self.addr, "replication server did not stop in 5 s; aborting it");
                task.abort();
            }
        }
        if stuck {
            return Err(Error::Timeout);
        }
        Ok(())
    }

    /// Stops at once, without any graceful step (emulates a crash), then waits (up to 5 s)
    /// until the blocking work that still references the database is gone: bootstrap jobs
    /// and verify computations stop at their next key once `cancel` is set. `Err(Timeout)` if
    /// one did not stop in time.
    #[cfg_attr(not(feature = "failpoints"), allow(dead_code))] // used by Slave::kill
    pub(crate) async fn abort(mut self) -> Result<()> {
        self.shutdown_tx.send_replace(true);
        self.state.cancel.store(true, Ordering::SeqCst);
        self.state.leases().clear();
        self.state.verify_sessions.clear();
        if let Some(t) = self.sweep.take() {
            t.abort();
        }
        if let Some(t) = self.task.take() {
            t.abort();
        }
        let bootstraps =
            u32::try_from(self.state.cfg.limits.max_concurrent_bootstraps).unwrap_or(u32::MAX);
        let computations = u32::try_from(verify::MAX_COMPUTATIONS).unwrap_or(u32::MAX);
        let drained = tokio::time::timeout(Duration::from_secs(5), async {
            let b = self.state.bootstraps.acquire_many(bootstraps).await;
            let c = self.state.compute_permits.acquire_many(computations).await;
            (b, c)
        })
        .await;
        // Closed while the permits are held: nothing new starts.
        self.state.compute_permits.close();
        self.state.bootstraps.close();
        match drained {
            Ok(_) => Ok(()),
            Err(_) => Err(Error::Timeout),
        }
    }
}

impl Drop for ServerHandle {
    fn drop(&mut self) {
        self.shutdown_tx.send_replace(true);
    }
}

/// Binds `cfg.bind:cfg.port` (0 picks a free port) and serves `source`. Its Tail streams
/// register in `replicas`.
pub(crate) async fn start(
    source: Weak<dyn LogSource>,
    cfg: ServeConfig,
    replicas: Arc<ReplicaRegistry>,
) -> Result<ServerHandle> {
    cfg.validate()?;
    let listener = TcpListener::bind((cfg.bind, cfg.port)).await?;
    let addr = listener.local_addr()?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let state = Arc::new(ServerState {
        source,
        bootstraps: Arc::new(Semaphore::new(cfg.limits.max_concurrent_bootstraps)),
        auth_failures: FailureLimiter::new(
            cfg.limits.max_auth_failures_per_minute,
            cfg.limits.auth_failure_window,
        ),
        leases: Mutex::new(HashMap::new()),
        shutdown: shutdown_rx.clone(),
        verify_sessions: verify::Sessions::new(
            cfg.limits.max_verify_sessions,
            cfg.limits.verify_session_timeout,
        ),
        compute_permits: Arc::new(Semaphore::new(verify::MAX_COMPUTATIONS)),
        cancel: Arc::new(AtomicBool::new(false)),
        cfg,
        replicas,
    });
    let mut builder = Server::builder()
        .http2_keepalive_interval(Some(Duration::from_secs(30)))
        .http2_keepalive_timeout(Some(Duration::from_secs(10)));
    if let Some(tls) = &state.cfg.tls {
        builder = builder
            .tls_config(tls.to_tonic())
            .map_err(|e| Error::Config(format!("server TLS: {e}")))?;
    }
    let service = ReplicationServer::new(Svc(state.clone()))
        .max_decoding_message_size(net::MAX_REQUEST_BYTES);
    let mut signal = shutdown_rx.clone();
    let task = tokio::spawn(
        builder
            .add_service(service)
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                stopped(&mut signal).await
            }),
    );
    let sweep = tokio::spawn(sweep_leases(state.clone(), shutdown_rx));
    tracing::info!(%addr, "replication server listening");
    Ok(ServerHandle {
        addr,
        state,
        shutdown_tx,
        task: Some(task),
        sweep: Some(sweep),
    })
}

async fn sweep_leases(state: Arc<ServerState>, mut shutdown: watch::Receiver<bool>) {
    let mut tick = tokio::time::interval(Duration::from_secs(1));
    // Subscribe once and never upgrade the weak source here: this task must not become the
    // last owner of a dropped node (closing RocksDB on an async worker).
    let resets = state.source.upgrade().map(|s| s.resets());
    loop {
        tokio::select! {
            _ = tick.tick() => {
                let now = Instant::now();
                state.expire_leases(now);
                // Sessions idle too long, or opened before a reset of this node's state.
                let reset_gen = resets.as_ref().map(|r| *r.borrow());
                state.verify_sessions.expire(now, reset_gen);
            }
            r = shutdown.changed() => if r.is_err() || *shutdown.borrow() { return },
        }
    }
}

/// Ends a stream with `status`. If the channel is full it waits for room, so a slow client
/// still learns why (for example `LOG_GAP`), but gives up on shutdown or reset.
pub(crate) async fn end_stream<T>(
    tx: &mpsc::Sender<Result<T, Status>>,
    status: Status,
    shutdown: &mut watch::Receiver<bool>,
    resets: &mut watch::Receiver<u64>,
) {
    if let Err(mpsc::error::TrySendError::Full(msg)) = tx.try_send(Err(status)) {
        tokio::select! {
            biased;
            _ = tx.send(msg) => {}
            _ = stopped(shutdown) => {}
            _ = resets.changed() => {}
        }
    }
}

/// Starts serving `source` if `serve` is configured.
pub(crate) async fn start_for<S: LogSource>(
    source: &Arc<S>,
    serve: Option<ServeConfig>,
    replicas: Arc<ReplicaRegistry>,
) -> Result<Option<ServerHandle>> {
    match serve {
        Some(cfg) => {
            let weak: Weak<dyn LogSource> = Arc::downgrade(source) as Weak<dyn LogSource>;
            Ok(Some(start(weak, cfg, replicas).await?))
        }
        None => Ok(None),
    }
}

/// Resolves once `rx` holds `true` (or its sender is gone).
pub(crate) async fn stopped(rx: &mut watch::Receiver<bool>) {
    let _ = rx.wait_for(|stop| *stop).await;
}

/// Waits until `arc` is the only strong reference left (background tasks that held a clone
/// have finished), so the database it owns closes when the caller drops it.
pub(crate) async fn await_sole_owner<T: ?Sized>(arc: &Arc<T>, timeout: Duration) -> bool {
    let deadline = tokio::time::Instant::now() + timeout;
    while Arc::strong_count(arc) > 1 {
        if tokio::time::Instant::now() >= deadline {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
    true
}
