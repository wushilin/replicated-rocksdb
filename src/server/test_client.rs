//! A bare protocol client for server unit tests.

use std::net::SocketAddr;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use tonic::metadata::AsciiMetadataValue;
use tonic::service::interceptor::InterceptedService;
use tonic::service::Interceptor;
use tonic::transport::{Channel, Endpoint};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;
use tonic::{Request, Response, Status};

use crate::auth::AllowAll;
use crate::clock::ManualClock;
use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::config::{MasterConfig, ServeConfig};
use crate::master::Master;
use crate::pb;
use crate::net::convert::{entry_from_pb, identity_to_pb};
use crate::pb::replication_client::ReplicationClient;
use crate::pb::tail_client_message::Kind as ClientKind;
use crate::pb::tail_message::Kind;
use crate::types::{DbIdentity, LogEntry};

pub(crate) const T0: u64 = 1_700_000_000_000;

#[derive(Clone)]
pub(crate) struct Meta {
    pub node_id: &'static str,
    pub token: Option<&'static str>,
    pub version: Option<u32>,
}

impl Interceptor for Meta {
    fn call(&mut self, mut req: Request<()>) -> Result<Request<()>, Status> {
        let md = req.metadata_mut();
        if let Some(v) = self.version {
            md.insert("x-rrdb-protocol-version", AsciiMetadataValue::from(v));
        }
        md.insert("x-rrdb-node-id", AsciiMetadataValue::from_static(self.node_id));
        if let Some(t) = self.token {
            md.insert("authorization", AsciiMetadataValue::try_from(format!("Bearer {t}")).unwrap());
        }
        Ok(req)
    }
}

pub(crate) type Client = ReplicationClient<InterceptedService<Channel, Meta>>;

pub(crate) fn client_with(addr: SocketAddr, meta: Meta) -> Client {
    let channel = Endpoint::from_shared(format!("http://{addr}")).unwrap().connect_lazy();
    ReplicationClient::with_interceptor(channel, meta).max_decoding_message_size(64 << 20)
}

pub(crate) fn client(addr: SocketAddr, node_id: &'static str) -> Client {
    client_with(addr, Meta { node_id, token: None, version: Some(PROTOCOL_VERSION) })
}

/// A real-time Tail request (`min_batch_bytes = 0`, server-default batch size).
pub(crate) fn tail_req(id: &DbIdentity, from_seq: u64) -> pb::TailRequest {
    pb::TailRequest {
        client_id: "b".into(),
        identity: Some(identity_to_pb(id)),
        from_seq,
        prev_crc32c: None,
        bootstrap_lease: None,
        min_batch_bytes: 0,
        max_wait_ms: 0,
        max_batch_bytes: 0,
    }
}

/// Awaits `fut` for at most 10 s, so a regression fails the test instead of hanging it.
pub(crate) async fn within<T>(what: &str, fut: impl std::future::Future<Output = T>) -> T {
    tokio::time::timeout(Duration::from_secs(10), fut).await.unwrap_or_else(|_| panic!("timed out after 10 s: {what}"))
}

/// The next entry batch of a Tail stream, skipping heartbeats.
pub(crate) async fn next_batch(s: &mut tonic::Streaming<pb::TailMessage>) -> Vec<LogEntry> {
    within("the next entry batch", async {
        loop {
            match s.message().await.unwrap().unwrap().kind.unwrap() {
                Kind::Batch(b) => return b.entries.into_iter().map(entry_from_pb).collect(),
                Kind::Heartbeat(_) => {}
            }
        }
    })
    .await
}

/// The status a stream ends with, skipping its remaining messages.
pub(crate) async fn stream_error<T>(s: &mut tonic::Streaming<T>) -> Status {
    within("the stream to end with a status", async {
        loop {
            match s.message().await {
                Ok(Some(_)) => continue,
                Ok(None) => panic!("stream ended without a status"),
                Err(e) => return e,
            }
        }
    })
    .await
}

/// Opens Tail streams (PROTOCOL.md §7: the request stream starts with `start`).
pub(crate) trait TailExt {
    /// A Tail that sends only `start`, then half-closes its request stream (no acks).
    async fn tail_start(&mut self, req: pb::TailRequest) -> Result<Response<tonic::Streaming<pb::TailMessage>>, Status>;
    /// A Tail whose acks the test sends through the returned channel.
    async fn tail_with_acks(
        &mut self,
        req: pb::TailRequest,
    ) -> Result<(tonic::Streaming<pb::TailMessage>, mpsc::Sender<pb::TailClientMessage>), Status>;
}

impl TailExt for Client {
    async fn tail_start(&mut self, req: pb::TailRequest) -> Result<Response<tonic::Streaming<pb::TailMessage>>, Status> {
        let start = pb::TailClientMessage { kind: Some(ClientKind::Start(req)) };
        self.tail(tokio_stream::iter([start])).await
    }

    async fn tail_with_acks(
        &mut self,
        req: pb::TailRequest,
    ) -> Result<(tonic::Streaming<pb::TailMessage>, mpsc::Sender<pb::TailClientMessage>), Status> {
        let (tx, rx) = mpsc::channel(16);
        tx.send(pb::TailClientMessage { kind: Some(ClientKind::Start(req)) }).await.unwrap();
        let s = self.tail(ReceiverStream::new(rx)).await?.into_inner();
        Ok((s, tx))
    }
}

/// An ack message.
pub(crate) fn ack(applied_seq: u64) -> pb::TailClientMessage {
    pb::TailClientMessage { kind: Some(ClientKind::Ack(pb::TailAck { applied_seq })) }
}

/// A handshake from a client with no data.
pub(crate) fn hello(client_id: &str) -> pb::HandshakeRequest {
    pb::HandshakeRequest {
        protocol_version: PROTOCOL_VERSION,
        envelope_version: u32::from(ENVELOPE_VERSION),
        client_id: client_id.into(),
        known_identity: None,
        last_seq: 0,
        last_crc32c: None,
        known_epoch_start: None,
    }
}

/// A master serving on 127.0.0.1:0 with `AllowAll`, a manual clock and a 100 ms heartbeat.
pub(crate) async fn serving_master(dir: &Path, tweak: impl FnOnce(&mut MasterConfig)) -> (Master, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(T0));
    let mut cfg = MasterConfig::new("a", dir);
    cfg.clock = clock.clone();
    cfg.heartbeat = Duration::from_millis(100);
    cfg.retention.check_interval = Duration::from_secs(3600);
    cfg.reaper.interval = Duration::from_secs(3600);
    cfg.serve = Some(ServeConfig::new([127, 0, 0, 1].into(), 0, Arc::new(AllowAll)));
    tweak(&mut cfg);
    (Master::open(cfg).await.unwrap(), clock)
}

/// Polls `f` every 10 ms for up to 10 s.
pub(crate) async fn eventually(what: &str, mut f: impl FnMut() -> bool) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !f() {
        assert!(tokio::time::Instant::now() < deadline, "timed out waiting for {what}");
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}
