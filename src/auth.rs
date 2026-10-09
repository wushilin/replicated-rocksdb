//! Authentication and authorisation of replication calls (DESIGN.md §14, PROTOCOL.md §3.3).
//!
//! Every call is authenticated on its own; there is no session state. Tokens travel as
//! `authorization: Bearer <token>` and are compared in constant time. Tokens are never logged.

use std::collections::{HashMap, VecDeque};
use std::fmt;
use std::net::{IpAddr, SocketAddr};
use std::sync::{Arc, Mutex, PoisonError};
use std::time::{Duration, Instant};

use subtle::ConstantTimeEq;

/// The RPC being authorised. `Bootstrap` and `Verify` expose the whole database.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Method {
    Handshake,
    Tail,
    Bootstrap,
    Verify,
}

/// Who is calling.
#[derive(Clone, Debug)]
pub struct PeerInfo {
    /// The client certificate's subject CommonName under mutual TLS, else `x-rrdb-node-id`.
    pub peer_id: String,
    /// True only when `peer_id` comes from a client certificate the TLS layer verified.
    pub peer_id_verified: bool,
    pub remote_addr: SocketAddr,
    /// DER of the client certificate, under mutual TLS.
    pub client_cert: Option<Vec<u8>>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, thiserror::Error)]
pub enum AuthError {
    /// Missing or invalid credentials (`UNAUTHENTICATED`).
    #[error("unauthenticated")]
    Unauthenticated,
    /// Valid credentials that may not make this call (`PERMISSION_DENIED`).
    #[error("permission denied")]
    PermissionDenied,
}

#[async_trait::async_trait]
pub trait Authenticator: Send + Sync + 'static {
    async fn authenticate(&self, peer: &PeerInfo, method: Method, token: Option<&[u8]>) -> Result<(), AuthError>;

    /// True if this authenticator relies on bearer tokens. A server refuses to start with
    /// such an authenticator on a plaintext listener unless `allow_plaintext_token` is set.
    fn uses_tokens(&self) -> bool {
        false
    }
}

/// Supplies the bearer token a client sends on every call.
pub trait CredentialsProvider: Send + Sync + 'static {
    fn token(&self) -> Option<Vec<u8>>;
}

/// Accepts every call. For tests and trusted loopback setups.
#[derive(Clone, Copy, Debug, Default)]
pub struct AllowAll;

#[async_trait::async_trait]
impl Authenticator for AllowAll {
    async fn authenticate(&self, _: &PeerInfo, _: Method, _: Option<&[u8]>) -> Result<(), AuthError> {
        Ok(())
    }
}

/// Accepts calls carrying any of the configured secrets. Several secrets can be valid at
/// once so they can be rotated.
pub struct TokenAuthenticator {
    secrets: Vec<Vec<u8>>,
}

impl TokenAuthenticator {
    pub fn new(secrets: impl IntoIterator<Item = impl Into<Vec<u8>>>) -> Self {
        TokenAuthenticator { secrets: secrets.into_iter().map(Into::into).collect() }
    }
}

impl fmt::Debug for TokenAuthenticator {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "TokenAuthenticator({} secrets)", self.secrets.len())
    }
}

#[async_trait::async_trait]
impl Authenticator for TokenAuthenticator {
    async fn authenticate(&self, _: &PeerInfo, _: Method, token: Option<&[u8]>) -> Result<(), AuthError> {
        let Some(token) = token else { return Err(AuthError::Unauthenticated) };
        // Compare against every secret without early exit.
        let mut ok = subtle::Choice::from(0);
        for s in &self.secrets {
            ok |= s.as_slice().ct_eq(token);
        }
        if bool::from(ok) { Ok(()) } else { Err(AuthError::Unauthenticated) }
    }

    fn uses_tokens(&self) -> bool {
        true
    }
}

/// Sends a fixed bearer token.
pub struct TokenCredentials {
    secret: Vec<u8>,
}

impl TokenCredentials {
    /// `secret` must consist of RFC 6750 `b64token` characters.
    pub fn new(secret: impl Into<Vec<u8>>) -> Self {
        TokenCredentials { secret: secret.into() }
    }
}

impl fmt::Debug for TokenCredentials {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("TokenCredentials(..)")
    }
}

impl CredentialsProvider for TokenCredentials {
    fn token(&self) -> Option<Vec<u8>> {
        Some(self.secret.clone())
    }
}

/// Sends no credentials.
#[derive(Clone, Copy, Debug, Default)]
pub struct NoCredentials;

impl CredentialsProvider for NoCredentials {
    fn token(&self) -> Option<Vec<u8>> {
        None
    }
}

/// Mutual TLS: accepts a call only from a verified client certificate whose subject
/// CommonName is listed, and only for the methods listed for it.
#[derive(Clone, Debug, Default)]
pub struct MtlsSubjectAllowList {
    allowed: HashMap<String, Vec<Method>>,
}

impl MtlsSubjectAllowList {
    pub fn new() -> Self {
        Self::default()
    }

    /// Allows `subject` (certificate CommonName) to call `methods`.
    pub fn allow(mut self, subject: impl Into<String>, methods: &[Method]) -> Self {
        self.allowed.insert(subject.into(), methods.to_vec());
        self
    }

    /// Allows `subject` to call every method.
    pub fn allow_all_methods(self, subject: impl Into<String>) -> Self {
        self.allow(subject, &[Method::Handshake, Method::Tail, Method::Bootstrap, Method::Verify])
    }
}

#[async_trait::async_trait]
impl Authenticator for MtlsSubjectAllowList {
    async fn authenticate(&self, peer: &PeerInfo, method: Method, _: Option<&[u8]>) -> Result<(), AuthError> {
        if !peer.peer_id_verified {
            return Err(AuthError::Unauthenticated);
        }
        match self.allowed.get(&peer.peer_id) {
            Some(methods) if methods.contains(&method) => Ok(()),
            _ => Err(AuthError::PermissionDenied),
        }
    }
}

/// Wraps another authenticator and additionally restricts which methods may be called,
/// for example to keep `Bootstrap` and `Verify` away from some credentials.
pub struct AllowMethods {
    inner: Arc<dyn Authenticator>,
    methods: Vec<Method>,
}

impl AllowMethods {
    pub fn new(inner: Arc<dyn Authenticator>, methods: &[Method]) -> Self {
        AllowMethods { inner, methods: methods.to_vec() }
    }
}

#[async_trait::async_trait]
impl Authenticator for AllowMethods {
    async fn authenticate(&self, peer: &PeerInfo, method: Method, token: Option<&[u8]>) -> Result<(), AuthError> {
        self.inner.authenticate(peer, method, token).await?;
        if self.methods.contains(&method) { Ok(()) } else { Err(AuthError::PermissionDenied) }
    }

    fn uses_tokens(&self) -> bool {
        self.inner.uses_tokens()
    }
}

/// Limits failed authentications per remote address: after `max` failures within one
/// minute, calls from that address are refused without checking credentials.
#[derive(Debug)]
pub(crate) struct FailureLimiter {
    max: usize,
    window: Duration,
    failures: Mutex<HashMap<IpAddr, VecDeque<Instant>>>,
}

impl FailureLimiter {
    pub(crate) fn new(max_per_minute: u32) -> Self {
        FailureLimiter {
            max: max_per_minute as usize,
            window: Duration::from_secs(60),
            failures: Mutex::new(HashMap::new()),
        }
    }

    pub(crate) fn is_blocked(&self, ip: IpAddr, now: Instant) -> bool {
        let mut map = self.failures.lock().unwrap_or_else(PoisonError::into_inner);
        let Some(q) = map.get_mut(&ip) else { return false };
        while q.front().is_some_and(|t| now.saturating_duration_since(*t) >= self.window) {
            q.pop_front();
        }
        if q.is_empty() {
            map.remove(&ip);
            return false;
        }
        q.len() >= self.max
    }

    pub(crate) fn record_failure(&self, ip: IpAddr, now: Instant) {
        self.failures.lock().unwrap_or_else(PoisonError::into_inner).entry(ip).or_default().push_back(now);
    }
}

/// The subject CommonName of a DER-encoded X.509 certificate, if it has one.
/// A minimal DER walk: Certificate → tbsCertificate → subject → first CN attribute.
pub(crate) fn subject_common_name(cert_der: &[u8]) -> Option<String> {
    const SEQUENCE: u8 = 0x30;
    const SET: u8 = 0x31;
    const OID: u8 = 0x06;
    const CONTEXT_0: u8 = 0xA0;
    const CN_OID: &[u8] = &[0x55, 0x04, 0x03]; // 2.5.4.3

    let (tag, cert, _) = der_tlv(cert_der)?;
    if tag != SEQUENCE {
        return None;
    }
    let (tag, tbs, _) = der_tlv(cert)?;
    if tag != SEQUENCE {
        return None;
    }
    // [0] version (optional), serialNumber, signature, issuer, validity, subject
    let (tag, _, after) = der_tlv(tbs)?;
    let mut rest = if tag == CONTEXT_0 { der_tlv(after)?.2 } else { after };
    for _ in 0..3 {
        rest = der_tlv(rest)?.2;
    }
    let (tag, mut subject, _) = der_tlv(rest)?;
    if tag != SEQUENCE {
        return None;
    }
    while !subject.is_empty() {
        let (tag, mut set, r) = der_tlv(subject)?;
        subject = r;
        if tag != SET {
            return None;
        }
        while !set.is_empty() {
            let (tag, atv, r) = der_tlv(set)?;
            set = r;
            if tag != SEQUENCE {
                return None;
            }
            let (tag, oid, value) = der_tlv(atv)?;
            if tag == OID && oid == CN_OID {
                let (_, s, _) = der_tlv(value)?;
                return String::from_utf8(s.to_vec()).ok();
            }
        }
    }
    None
}

/// One DER TLV at the start of `b`: `(tag, contents, rest)`.
fn der_tlv(b: &[u8]) -> Option<(u8, &[u8], &[u8])> {
    let (&tag, b) = b.split_first()?;
    let (&first, b) = b.split_first()?;
    let (len, b) = if first < 0x80 {
        (usize::from(first), b)
    } else {
        let n = usize::from(first & 0x7F);
        if n == 0 || n > 4 || b.len() < n {
            return None;
        }
        (b[..n].iter().fold(0usize, |acc, &x| (acc << 8) | usize::from(x)), &b[n..])
    };
    if b.len() < len {
        return None;
    }
    Some((tag, &b[..len], &b[len..]))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn peer(id: &str, verified: bool) -> PeerInfo {
        PeerInfo {
            peer_id: id.into(),
            peer_id_verified: verified,
            remote_addr: "127.0.0.1:9".parse().unwrap(),
            client_cert: None,
        }
    }

    #[tokio::test]
    async fn token_authenticator_accepts_any_configured_secret() {
        let a = TokenAuthenticator::new([b"old-secret".to_vec(), b"new-secret".to_vec()]);
        let p = peer("b", false);
        assert!(a.uses_tokens());
        a.authenticate(&p, Method::Tail, Some(b"old-secret")).await.unwrap();
        a.authenticate(&p, Method::Tail, Some(b"new-secret")).await.unwrap();
        for bad in [Some(&b"new-secre"[..]), Some(b"new-secret!"), Some(b""), None] {
            assert_eq!(a.authenticate(&p, Method::Tail, bad).await, Err(AuthError::Unauthenticated));
        }
    }

    #[tokio::test]
    async fn allow_methods_restricts_after_authenticating() {
        let a = AllowMethods::new(Arc::new(TokenAuthenticator::new(["t"])), &[Method::Handshake, Method::Tail]);
        let p = peer("b", false);
        assert!(a.uses_tokens());
        a.authenticate(&p, Method::Tail, Some(b"t")).await.unwrap();
        assert_eq!(a.authenticate(&p, Method::Bootstrap, Some(b"t")).await, Err(AuthError::PermissionDenied));
        assert_eq!(a.authenticate(&p, Method::Bootstrap, Some(b"x")).await, Err(AuthError::Unauthenticated));
    }

    #[tokio::test]
    async fn mtls_allow_list_needs_a_verified_listed_subject() {
        let a = MtlsSubjectAllowList::new().allow_all_methods("node-b").allow("node-c", &[Method::Handshake]);
        assert!(!a.uses_tokens());
        a.authenticate(&peer("node-b", true), Method::Bootstrap, None).await.unwrap();
        a.authenticate(&peer("node-c", true), Method::Handshake, None).await.unwrap();
        assert_eq!(a.authenticate(&peer("node-c", true), Method::Tail, None).await, Err(AuthError::PermissionDenied));
        assert_eq!(a.authenticate(&peer("node-x", true), Method::Tail, None).await, Err(AuthError::PermissionDenied));
        assert_eq!(a.authenticate(&peer("node-b", false), Method::Tail, None).await, Err(AuthError::Unauthenticated));
    }

    #[tokio::test]
    async fn allow_all_and_credentials() {
        AllowAll.authenticate(&peer("x", false), Method::Verify, None).await.unwrap();
        assert_eq!(TokenCredentials::new("abc").token(), Some(b"abc".to_vec()));
        assert_eq!(NoCredentials.token(), None);
        assert!(!format!("{:?}", TokenCredentials::new("abc")).contains("abc"), "tokens are never printed");
    }

    #[test]
    fn failure_limiter_blocks_after_max_within_a_minute() {
        let l = FailureLimiter::new(3);
        let ip: IpAddr = "10.0.0.1".parse().unwrap();
        let other: IpAddr = "10.0.0.2".parse().unwrap();
        let t0 = Instant::now();
        for i in 0..3 {
            assert!(!l.is_blocked(ip, t0));
            l.record_failure(ip, t0 + Duration::from_secs(i));
        }
        assert!(l.is_blocked(ip, t0 + Duration::from_secs(10)));
        assert!(!l.is_blocked(other, t0 + Duration::from_secs(10)));
        assert!(!l.is_blocked(ip, t0 + Duration::from_secs(61)), "the oldest failure left the window");
        assert!(!l.is_blocked(ip, t0 + Duration::from_secs(120)));
    }

    #[test]
    fn common_name_is_read_from_a_real_certificate() {
        let key = rcgen::KeyPair::generate().unwrap();
        let mut params = rcgen::CertificateParams::new(Vec::<String>::new()).unwrap();
        params.distinguished_name.push(rcgen::DnType::OrganizationName, "rrdb");
        params.distinguished_name.push(rcgen::DnType::CommonName, "node-b");
        let cert = params.self_signed(&key).unwrap();
        assert_eq!(subject_common_name(cert.der()).as_deref(), Some("node-b"));
        assert_eq!(subject_common_name(&[0x30, 0x82, 0xFF]), None);
        assert_eq!(subject_common_name(&[]), None);
    }
}
