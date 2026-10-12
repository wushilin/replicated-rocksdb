//! Slave configuration (DESIGN.md §16).

use std::net::SocketAddr;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::auth::{CredentialsProvider, NoCredentials};
use crate::clock::{Clock, SystemClock};
use crate::config::{
    Retention, ServeConfig, TlsClientConfig, WireCompression, validate_identifier,
};
use crate::error::{Error, Result};
use crate::listener::ReplicationListener;
use crate::tuning::RocksTuning;
use crate::verify::VerifyConfig;

/// The node this slave replicates from: the master or another slave.
#[derive(Clone)]
#[non_exhaustive]
pub struct UpstreamConfig {
    /// Host name or IP address.
    pub server: String,
    pub port: u16,
    pub tls: Option<TlsClientConfig>,
    pub credentials: Arc<dyn CredentialsProvider>,
    /// What the slave compresses its requests with. Default `None`; see [`WireCompression`]
    /// for the upgrade-order rule. Responses are compressed per the server's setting.
    pub compression: WireCompression,
}

impl UpstreamConfig {
    pub fn new(server: impl Into<String>, port: u16) -> Self {
        UpstreamConfig {
            server: server.into(),
            port,
            tls: None,
            credentials: Arc::new(NoCredentials),
            compression: WireCompression::None,
        }
    }

    pub fn from_addr(addr: SocketAddr) -> Self {
        Self::new(addr.ip().to_string(), addr.port())
    }

    pub(crate) fn uri(&self) -> String {
        let scheme = if self.tls.is_some() { "https" } else { "http" };
        if self.server.contains(':') && !self.server.starts_with('[') {
            format!("{scheme}://[{}]:{}", self.server, self.port)
        } else {
            format!("{scheme}://{}:{}", self.server, self.port)
        }
    }
}

/// What to do when the upstream's database identity differs, or continuity across an
/// epoch change cannot be proven.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IdentityPolicy {
    /// Halt with `IdentityChanged` (persisted).
    #[default]
    Refuse,
    Rebootstrap,
}

/// What to do when the upstream reports `DIVERGED` on a reconnect (an explicit
/// `Slave::repoint` uses `RepointOptions::on_divergence` instead). Both non-default choices
/// are configured consent to discard the divergent entries.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum DivergencePolicy {
    /// Halt with `Diverged` (persisted). The data stays readable; `Slave::diverged_entries`
    /// returns the divergent suffix for salvage.
    #[default]
    Halt,
    /// Discard only the divergent suffix (DESIGN.md §12.1): the keys it touched are taken
    /// from the upstream, the suffix is dropped, and the slave replays the upstream's history
    /// (reads answer `NotReady` until it has replayed up to the upstream's snapshot seq).
    /// Falls back to a bootstrap from the same upstream when that cannot be proven safe.
    Repair,
    /// Discard all local data and bootstrap.
    Rebootstrap,
}

/// What to do when the entries this slave needs are no longer retained upstream.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum GapPolicy {
    #[default]
    Rebootstrap,
    /// Halt with `LogGap` (persisted).
    Halt,
}

/// Tail fetch batching (PROTOCOL.md §7.5), like Kafka's `fetch.min.bytes`,
/// `fetch.max.wait.ms` and `fetch.max.bytes`.
// Deliberately exhaustive: users build it with a struct literal; a new field is a breaking change.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct FetchConfig {
    /// The upstream waits until this many bytes of entries are ready. 0 = real-time push.
    pub min_batch_bytes: u32,
    /// The longest a ready entry waits for `min_batch_bytes` (the upstream clamps it to its
    /// heartbeat). Ignored when `min_batch_bytes` is 0.
    pub max_wait: Duration,
    /// The most entry bytes per batch (the upstream clamps it to its `max_tail_batch_bytes`).
    pub max_batch_bytes: u32,
}

impl Default for FetchConfig {
    fn default() -> Self {
        FetchConfig {
            min_batch_bytes: 0,
            max_wait: Duration::from_millis(100),
            max_batch_bytes: 1 << 20,
        }
    }
}

/// Reconnect backoff: exponential with full jitter.
#[derive(Clone, Debug)]
pub struct Backoff {
    pub initial: Duration,
    pub max: Duration,
    pub factor: u32,
}

impl Default for Backoff {
    fn default() -> Self {
        Backoff {
            initial: Duration::from_millis(100),
            max: Duration::from_secs(30),
            factor: 2,
        }
    }
}

/// Backoff progress of one driver.
#[derive(Debug)]
pub(crate) struct BackoffState {
    cfg: Backoff,
    cap: Duration,
}

impl BackoffState {
    pub(crate) fn new(cfg: Backoff) -> Self {
        let cap = cfg.initial;
        BackoffState { cfg, cap }
    }

    pub(crate) fn reset(&mut self) {
        self.cap = self.cfg.initial;
    }

    /// A random delay in `[0, cap]`; the cap then grows by `factor` up to `max`.
    pub(crate) fn next_delay(&mut self) -> Duration {
        let cap_ms = u64::try_from(self.cap.as_millis()).unwrap_or(u64::MAX);
        let random = uuid::Uuid::new_v4().as_u128() as u64;
        let delay = Duration::from_millis(random % cap_ms.saturating_add(1));
        self.cap = self.cap.saturating_mul(self.cfg.factor).min(self.cfg.max);
        delay
    }
}

#[derive(Clone)]
#[non_exhaustive]
pub struct SlaveConfig {
    /// Unique within the topology: 1–128 characters of `[A-Za-z0-9._-]`.
    pub identifier: String,
    pub path: PathBuf,
    pub upstream: UpstreamConfig,
    /// `Some` = this slave also serves downstream replicas.
    pub serve: Option<ServeConfig>,
    pub retention: Retention,
    /// Integrity verification against the upstream (DESIGN.md §12). Scheduled runs are off
    /// unless `interval` is set; `Slave::verify_now` always works.
    pub verify: VerifyConfig,
    pub listener: Option<Arc<dyn ReplicationListener>>,
    pub on_identity_change: IdentityPolicy,
    pub on_divergence: DivergencePolicy,
    pub on_log_gap: GapPolicy,
    pub reconnect: Backoff,
    /// Tail fetch batching. Default: real-time push.
    pub fetch: FetchConfig,
    /// A repair (DESIGN.md §12.1) that would read, fetch or delete more than this many bytes
    /// of entries, keys and envelopes falls back to a bootstrap. Default 64 MiB; must be
    /// positive.
    pub repair_max_bytes: u64,
    /// Fsync every applied entry before acknowledging it. Default `false`: an ack then means
    /// "in the replica's WAL", which survives a process crash but not a machine crash.
    pub sync_writes: bool,
    /// Heartbeat interval when serving downstream replicas. Default 5 s.
    pub heartbeat: Duration,
    /// TCP connect timeout to the upstream. Default 5 s.
    pub connect_timeout: Duration,
    pub rocksdb: RocksTuning,
    /// Time source for TTL read filtering and retention.
    pub clock: Arc<dyn Clock>,
}

impl SlaveConfig {
    pub fn new(
        identifier: impl Into<String>,
        path: impl Into<PathBuf>,
        upstream: UpstreamConfig,
    ) -> Self {
        SlaveConfig {
            identifier: identifier.into(),
            path: path.into(),
            upstream,
            serve: None,
            retention: Retention::default(),
            verify: VerifyConfig::default(),
            listener: None,
            on_identity_change: IdentityPolicy::default(),
            on_divergence: DivergencePolicy::default(),
            on_log_gap: GapPolicy::default(),
            reconnect: Backoff::default(),
            fetch: FetchConfig::default(),
            repair_max_bytes: 64 << 20,
            sync_writes: false,
            heartbeat: Duration::from_secs(5),
            connect_timeout: Duration::from_secs(5),
            rocksdb: RocksTuning::default(),
            clock: Arc::new(SystemClock),
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_identifier(&self.identifier)?;
        if self.upstream.server.is_empty() || self.upstream.port == 0 {
            return Err(Error::Config(
                "upstream server and port are required".into(),
            ));
        }
        if self.reconnect.initial.is_zero()
            || self.reconnect.max < self.reconnect.initial
            || self.reconnect.factor == 0
        {
            return Err(Error::Config(
                "reconnect backoff needs 0 < initial <= max and factor >= 1".into(),
            ));
        }
        if self.fetch.max_batch_bytes == 0 {
            return Err(Error::Config(
                "fetch.max_batch_bytes must be positive".into(),
            ));
        }
        if self.repair_max_bytes == 0 {
            return Err(Error::Config("repair_max_bytes must be positive".into()));
        }
        if self.heartbeat.is_zero()
            || self.connect_timeout.is_zero()
            || self.retention.check_interval.is_zero()
        {
            return Err(Error::Config(
                "heartbeat, connect_timeout and retention.check_interval must be positive".into(),
            ));
        }
        self.verify.validate()?;
        if let Some(serve) = &self.serve {
            serve.validate()?;
            serve.check_heartbeat(self.heartbeat)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn backoff_grows_with_full_jitter_and_resets() {
        let mut b = BackoffState::new(Backoff {
            initial: Duration::from_millis(100),
            max: Duration::from_millis(400),
            factor: 2,
        });
        for cap in [100, 200, 400, 400] {
            let d = b.next_delay();
            assert!(d <= Duration::from_millis(cap), "{d:?} > {cap} ms");
        }
        b.reset();
        assert!(b.next_delay() <= Duration::from_millis(100));
    }

    #[test]
    fn upstream_uri_brackets_ipv6() {
        assert_eq!(
            UpstreamConfig::new("10.0.0.1", 7000).uri(),
            "http://10.0.0.1:7000"
        );
        assert_eq!(
            UpstreamConfig::from_addr("[::1]:7000".parse().unwrap()).uri(),
            "http://[::1]:7000"
        );
    }

    #[test]
    fn config_is_validated() {
        let up = UpstreamConfig::new("127.0.0.1", 7000);
        SlaveConfig::new("b", "/tmp/x", up.clone())
            .validate()
            .unwrap();
        assert!(
            SlaveConfig::new("bad id", "/tmp/x", up.clone())
                .validate()
                .is_err()
        );
        assert!(
            SlaveConfig::new("b", "/tmp/x", UpstreamConfig::new("h", 0))
                .validate()
                .is_err()
        );
        let mut cfg = SlaveConfig::new("b", "/tmp/x", up.clone());
        cfg.reconnect.max = Duration::from_millis(1);
        assert!(cfg.validate().is_err());
        let mut cfg = SlaveConfig::new("b", "/tmp/x", up.clone());
        cfg.repair_max_bytes = 0;
        assert!(matches!(cfg.validate(), Err(Error::Config(m)) if m.contains("repair_max_bytes")));
        assert_eq!(
            SlaveConfig::new("b", "/tmp/x", up.clone()).repair_max_bytes,
            64 << 20
        );
        // Serving: the heartbeat must be at most half of the in-sync window.
        let mut cfg = SlaveConfig::new("b", "/tmp/x", up);
        cfg.serve = Some(ServeConfig::new(
            std::net::IpAddr::from([127, 0, 0, 1]),
            0,
            Arc::new(crate::auth::AllowAll),
        ));
        cfg.heartbeat = Duration::from_secs(5);
        cfg.validate().unwrap();
        cfg.serve.as_mut().unwrap().limits.replica_lag_time_max = Duration::from_millis(9_999);
        assert!(
            matches!(cfg.validate(), Err(Error::Config(m)) if m.contains("replica_lag_time_max"))
        );
    }
}
