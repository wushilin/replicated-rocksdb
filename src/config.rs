//! Configuration shared by master and slave.

use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use crate::clock::{Clock, SystemClock};
use crate::error::{Error, Result};
use crate::tuning::RocksTuning;

/// Changelog retention. Runs on every node; age is measured from the master's `ts_ms`.
#[derive(Clone, Debug)]
pub struct Retention {
    pub max_age: Option<Duration>,
    pub max_entries: Option<u64>,
    pub check_interval: Duration,
}

impl Default for Retention {
    fn default() -> Self {
        Retention {
            max_age: Some(Duration::from_secs(14 * 24 * 3600)),
            max_entries: None,
            check_interval: Duration::from_secs(600),
        }
    }
}

/// TTL reaper (master only).
#[derive(Clone, Debug)]
pub struct ReaperConfig {
    pub interval: Duration,
    pub max_deletes_per_entry: usize,
    pub max_deletes_per_round: usize,
}

impl Default for ReaperConfig {
    fn default() -> Self {
        ReaperConfig { interval: Duration::from_secs(30), max_deletes_per_entry: 1_000, max_deletes_per_round: 100_000 }
    }
}

#[derive(Clone)]
pub struct MasterConfig {
    /// Unique within the topology: 1–128 characters of `[A-Za-z0-9._-]`.
    pub identifier: String,
    pub path: PathBuf,
    pub retention: Retention,
    pub reaper: ReaperConfig,
    /// Maximum encoded size of one log entry. Default 16 MiB.
    pub max_entry_bytes: u64,
    pub heartbeat: Duration,
    /// Set when opening a database restored from a backup (DESIGN.md §13).
    pub bump_epoch_on_open: bool,
    pub rocksdb: RocksTuning,
    pub clock: Arc<dyn Clock>,
    /// Replication server. `None` runs an embedded, non-replicating master.
    pub serve: Option<ServeConfig>,
}

impl MasterConfig {
    pub fn new(identifier: impl Into<String>, path: impl Into<PathBuf>) -> Self {
        MasterConfig {
            identifier: identifier.into(),
            path: path.into(),
            retention: Retention::default(),
            reaper: ReaperConfig::default(),
            max_entry_bytes: 16 << 20,
            heartbeat: Duration::from_secs(5),
            bump_epoch_on_open: false,
            rocksdb: RocksTuning::default(),
            clock: Arc::new(SystemClock),
            serve: None,
        }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        validate_identifier(&self.identifier)?;
        if self.max_entry_bytes == 0 {
            return Err(Error::Config("max_entry_bytes must be positive".into()));
        }
        if self.reaper.max_deletes_per_entry == 0 || self.reaper.max_deletes_per_round == 0 {
            return Err(Error::Config("reaper limits must be positive".into()));
        }
        if self.heartbeat.is_zero() {
            return Err(Error::Config("heartbeat must be positive".into()));
        }
        if self.retention.check_interval.is_zero() || self.reaper.interval.is_zero() {
            return Err(Error::Config("maintenance intervals must be positive".into()));
        }
        if let Some(serve) = &self.serve {
            serve.validate()?;
        }
        Ok(())
    }
}

pub(crate) fn validate_identifier(id: &str) -> Result<()> {
    let ok = (1..=128).contains(&id.len())
        && id.bytes().all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'));
    if ok {
        Ok(())
    } else {
        Err(Error::Config(format!("identifier {id:?} must be 1-128 characters of [A-Za-z0-9._-]")))
    }
}

// ---------------- Replication serving and TLS (Plan 2) ----------------

/// PEM-encoded certificate chain and private key.
#[derive(Clone)]
pub struct TlsIdentity {
    pub cert_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

impl std::fmt::Debug for TlsIdentity {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("TlsIdentity(..)")
    }
}

/// TLS for a replication server. With `client_ca_pem`, clients must present a certificate
/// signed by that CA (mutual TLS); its subject CommonName becomes the peer id.
#[derive(Clone, Debug)]
pub struct TlsServerConfig {
    pub identity: TlsIdentity,
    pub client_ca_pem: Option<Vec<u8>>,
}

impl TlsServerConfig {
    pub(crate) fn to_tonic(&self) -> tonic::transport::ServerTlsConfig {
        use tonic::transport::{Certificate, Identity, ServerTlsConfig};
        let mut t = ServerTlsConfig::new()
            .identity(Identity::from_pem(&self.identity.cert_pem, &self.identity.key_pem));
        if let Some(ca) = &self.client_ca_pem {
            t = t.client_ca_root(Certificate::from_pem(ca));
        }
        t
    }
}

/// TLS for connecting to an upstream.
#[derive(Clone, Debug)]
pub struct TlsClientConfig {
    /// PEM of the CA that signed the upstream's certificate.
    pub ca_pem: Vec<u8>,
    /// Name to verify the upstream certificate against; defaults to `UpstreamConfig::server`.
    pub domain_name: Option<String>,
    /// Client certificate for mutual TLS.
    pub identity: Option<TlsIdentity>,
}

impl TlsClientConfig {
    #[allow(dead_code)] // used by the replication client (later Plan 2 task)
    pub(crate) fn to_tonic(&self, default_domain: &str) -> tonic::transport::ClientTlsConfig {
        use tonic::transport::{Certificate, ClientTlsConfig, Identity};
        let mut t = ClientTlsConfig::new()
            .ca_certificate(Certificate::from_pem(&self.ca_pem))
            .domain_name(self.domain_name.clone().unwrap_or_else(|| default_domain.to_string()));
        if let Some(id) = &self.identity {
            t = t.identity(Identity::from_pem(&id.cert_pem, &id.key_pem));
        }
        t
    }
}

/// Server-side limits (PROTOCOL.md §12).
#[derive(Clone, Debug)]
pub struct ServeLimits {
    pub max_concurrent_bootstraps: usize,
    pub max_verify_sessions: usize,
    /// Soft size of one `BootstrapRecords` message. At most [`MAX_BOOTSTRAP_CHUNK_BYTES`].
    pub bootstrap_chunk_bytes: usize,
    pub bootstrap_lease_ttl: Duration,
    pub verify_session_timeout: Duration,
    /// Failed authentications per remote address per minute before calls are refused unchecked.
    pub max_auth_failures_per_minute: u32,
    /// Cap on a Tail client's `max_batch_bytes` (PROTOCOL.md §7.5).
    pub max_tail_batch_bytes: usize,
}

/// Upper bound for `bootstrap_chunk_bytes`; clients size their Bootstrap decode limit with it.
pub const MAX_BOOTSTRAP_CHUNK_BYTES: usize = 16 << 20;

impl Default for ServeLimits {
    fn default() -> Self {
        ServeLimits {
            max_concurrent_bootstraps: 2,
            max_verify_sessions: 4,
            bootstrap_chunk_bytes: 1 << 20,
            bootstrap_lease_ttl: Duration::from_secs(15 * 60),
            verify_session_timeout: Duration::from_secs(5 * 60),
            max_auth_failures_per_minute: 10,
            max_tail_batch_bytes: 4 << 20,
        }
    }
}

/// Where and how a node serves replication to downstream replicas.
#[derive(Clone)]
pub struct ServeConfig {
    pub bind: std::net::IpAddr,
    /// 0 picks a free port; read it back with `serve_addr()`.
    pub port: u16,
    pub tls: Option<TlsServerConfig>,
    pub authenticator: Arc<dyn crate::auth::Authenticator>,
    /// Allows token authentication without TLS. Loopback and tests only.
    pub allow_plaintext_token: bool,
    pub limits: ServeLimits,
}

impl ServeConfig {
    pub fn new(bind: std::net::IpAddr, port: u16, authenticator: Arc<dyn crate::auth::Authenticator>) -> Self {
        ServeConfig { bind, port, tls: None, authenticator, allow_plaintext_token: false, limits: ServeLimits::default() }
    }

    pub(crate) fn validate(&self) -> Result<()> {
        self.authenticator.validate()?;
        if self.allow_plaintext_token && self.tls.is_none() && !self.bind.is_loopback() {
            tracing::warn!(bind = %self.bind, "allow_plaintext_token is set on a non-loopback address; tokens travel unencrypted");
        }
        if self.tls.is_none() && self.authenticator.uses_tokens() && !self.allow_plaintext_token {
            return Err(Error::Config(
                "token authentication without TLS is refused; configure tls or set allow_plaintext_token".into(),
            ));
        }
        let l = &self.limits;
        if l.max_concurrent_bootstraps == 0
            || l.max_verify_sessions == 0
            || l.max_auth_failures_per_minute == 0
            || l.max_tail_batch_bytes == 0
        {
            return Err(Error::Config("serve limits must be positive".into()));
        }
        if l.bootstrap_chunk_bytes == 0 || l.bootstrap_chunk_bytes > MAX_BOOTSTRAP_CHUNK_BYTES {
            return Err(Error::Config(format!("bootstrap_chunk_bytes must be 1..={MAX_BOOTSTRAP_CHUNK_BYTES}")));
        }
        if l.bootstrap_lease_ttl.is_zero() || l.verify_session_timeout.is_zero() {
            return Err(Error::Config("serve timeouts must be positive".into()));
        }
        Ok(())
    }
}

#[cfg(test)]
mod serve_tests {
    use super::*;
    use crate::auth::{AllowAll, TokenAuthenticator};

    #[test]
    fn token_auth_on_plaintext_is_refused_unless_allowed() {
        let localhost = std::net::IpAddr::from([127, 0, 0, 1]);
        let mut cfg = ServeConfig::new(localhost, 0, Arc::new(TokenAuthenticator::new(["t"])));
        assert!(matches!(cfg.validate(), Err(Error::Config(m)) if m.contains("without TLS")));
        cfg.allow_plaintext_token = true;
        cfg.validate().unwrap();
        ServeConfig::new(localhost, 0, Arc::new(AllowAll)).validate().unwrap();
    }

    #[test]
    fn limits_are_validated() {
        let mut cfg = ServeConfig::new(std::net::IpAddr::from([127, 0, 0, 1]), 0, Arc::new(AllowAll));
        cfg.limits.bootstrap_chunk_bytes = MAX_BOOTSTRAP_CHUNK_BYTES + 1;
        assert!(cfg.validate().is_err());
        cfg.limits.bootstrap_chunk_bytes = 1;
        cfg.validate().unwrap();
        cfg.limits.max_concurrent_bootstraps = 0;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn tls_identity_debug_hides_key_and_timeouts_are_validated() {
        let id = TlsIdentity { cert_pem: b"CERT".to_vec(), key_pem: b"SECRETKEY".to_vec() };
        let d = format!("{id:?} {:?}", TlsServerConfig { identity: id.clone(), client_ca_pem: None });
        assert!(!d.contains("SECRETKEY") && !d.contains("CERT"));
        let new = || ServeConfig::new(std::net::IpAddr::from([127, 0, 0, 1]), 0, Arc::new(AllowAll));
        let mut cfg = new();
        cfg.limits.max_auth_failures_per_minute = 0;
        assert!(cfg.validate().is_err());
        let mut cfg = new();
        cfg.limits.bootstrap_lease_ttl = Duration::ZERO;
        assert!(cfg.validate().is_err());
        let mut cfg = new();
        cfg.limits.verify_session_timeout = Duration::ZERO;
        assert!(cfg.validate().is_err());
    }

    #[test]
    fn invalid_token_secrets_are_refused() {
        let mut cfg = ServeConfig::new(std::net::IpAddr::from([127, 0, 0, 1]), 0, Arc::new(TokenAuthenticator::new([""])));
        cfg.allow_plaintext_token = true;
        assert!(matches!(cfg.validate(), Err(Error::Config(m)) if m.contains("b64token")));
    }
}
