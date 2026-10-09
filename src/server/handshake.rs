//! The handshake decision table (PROTOCOL.md §6.3). Pure: the server gathers a
//! [`ServerView`] and this module decides, so every rule is unit-tested in isolation.

use crate::codec::{ENVELOPE_VERSION, PROTOCOL_VERSION};
use crate::pb;
use crate::types::{fork_after, DbIdentity, EpochStart, Seq};

/// What the server knows about itself when answering one handshake.
#[derive(Clone, Debug)]
pub(crate) struct ServerView {
    pub server_id: String,
    pub is_master: bool,
    /// False while the server bootstraps itself or before it has any data.
    pub ready: bool,
    pub identity: Option<DbIdentity>,
    pub epoch_history: Vec<EpochStart>,
    pub upstream_chain: Vec<String>,
    pub last_seq: Seq,
    pub first_retained_seq: Seq,
    /// `crc32c` of the server's entry at the client's `last_seq`, if the server holds it.
    pub crc_at_client_last: Option<u32>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct Verdict {
    pub decision: pb::Decision,
    pub reason: pb::Reason,
    pub detail: String,
}

fn verdict(decision: pb::Decision, reason: pb::Reason, detail: impl Into<String>) -> Verdict {
    Verdict { decision, reason, detail: detail.into() }
}

/// Evaluates PROTOCOL.md §6.3 rules 1–14 in order; the first match wins.
pub(crate) fn decide(v: &ServerView, r: &pb::HandshakeRequest) -> Verdict {
    use pb::Decision::{Bootstrap, Reject, Resume};
    use pb::Reason as R;

    // 1, 2: exact version match.
    if r.protocol_version != PROTOCOL_VERSION {
        let d = format!("client protocol {} != server protocol {PROTOCOL_VERSION}", r.protocol_version);
        return verdict(Reject, R::ProtocolMismatch, d);
    }
    if r.envelope_version != u32::from(ENVELOPE_VERSION) {
        let d = format!("client envelope {} != server envelope {ENVELOPE_VERSION}", r.envelope_version);
        return verdict(Reject, R::EnvelopeMismatch, d);
    }
    // 3: the client is this server or one of its upstreams.
    if r.client_id == v.server_id || v.upstream_chain.contains(&r.client_id) {
        let d = format!("{} is upstream of {} (chain {:?})", r.client_id, v.server_id, v.upstream_chain);
        return verdict(Reject, R::LoopDetected, d);
    }
    // 4: no consistent state to serve.
    let Some(server_id) = v.identity.filter(|_| v.ready) else {
        return verdict(Reject, R::NotReady, format!("{} is bootstrapping", v.server_id));
    };
    // 5
    let Some(known) = &r.known_identity else {
        return verdict(Bootstrap, R::NoLocalState, "client has no data");
    };
    // 6
    if known.db_uuid.as_slice() != server_id.db_uuid.as_bytes() {
        return verdict(Bootstrap, R::IdentityChanged, "database uuid differs");
    }
    let client_crc_matches = r.last_crc32c.is_some() && r.last_crc32c == v.crc_at_client_last;
    // 7
    if known.epoch > server_id.epoch {
        let d = format!("client epoch {} is newer than server epoch {}", known.epoch, server_id.epoch);
        return verdict(Reject, R::StaleEpoch, d);
    }
    if known.epoch < server_id.epoch {
        // 8: the client holds entries at or past the fork point after its epoch.
        if let Some(fork) = fork_after(&v.epoch_history, known.epoch) {
            if r.last_seq >= fork {
                let d = format!("client last_seq {} is at or past fork point {fork} of epoch {}", r.last_seq, known.epoch);
                return verdict(Reject, R::Diverged, d);
            }
        }
        // 9: continuity proven by the CRC of the client's last entry.
        if client_crc_matches {
            return verdict(Resume, R::EpochAdvanced, format!("epoch {} -> {}", known.epoch, server_id.epoch));
        }
        // 10
        return verdict(Bootstrap, R::IdentityChanged, "continuity across the epoch change cannot be proven");
    }
    // 11: nobody can be ahead of the fsyncing master.
    if v.is_master && r.last_seq > v.last_seq {
        let d = format!("client last_seq {} is ahead of master last_seq {}", r.last_seq, v.last_seq);
        return verdict(Reject, R::Diverged, d);
    }
    // 12
    if r.last_crc32c.is_some() && v.crc_at_client_last.is_some() && !client_crc_matches {
        return verdict(Reject, R::Diverged, format!("entry {} differs", r.last_seq));
    }
    // 13
    if r.last_seq.saturating_add(1) < v.first_retained_seq {
        let d = format!("client needs {} but first retained is {}", r.last_seq + 1, v.first_retained_seq);
        return verdict(Bootstrap, R::LogGap, d);
    }
    // 14
    verdict(Resume, R::Unspecified, "")
}

#[cfg(test)]
mod tests {
    use super::*;
    use pb::Decision::{Bootstrap, Reject, Resume};
    use pb::Reason as R;

    fn uuid() -> uuid::Uuid {
        uuid::Uuid::from_u128(0x1234)
    }

    /// A master at epoch 2 (fork after epoch 1 at seq 101), last_seq 200, log from 50.
    fn master() -> ServerView {
        ServerView {
            server_id: "a".into(),
            is_master: true,
            ready: true,
            identity: Some(DbIdentity { db_uuid: uuid(), epoch: 2 }),
            epoch_history: vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 2, start_seq: 101 }],
            upstream_chain: vec![],
            last_seq: 200,
            first_retained_seq: 50,
            crc_at_client_last: Some(0xAAAA),
        }
    }

    fn replica() -> ServerView {
        ServerView { server_id: "b".into(), is_master: false, upstream_chain: vec!["a".into()], ..master() }
    }

    /// A client of epoch 2 at seq 150 whose last entry matches.
    fn req() -> pb::HandshakeRequest {
        pb::HandshakeRequest {
            protocol_version: PROTOCOL_VERSION,
            envelope_version: u32::from(ENVELOPE_VERSION),
            client_id: "c".into(),
            known_identity: Some(pb::Identity { db_uuid: uuid().as_bytes().to_vec(), epoch: 2 }),
            last_seq: 150,
            last_crc32c: Some(0xAAAA),
        }
    }

    fn check(v: &ServerView, r: &pb::HandshakeRequest, decision: pb::Decision, reason: pb::Reason) {
        let got = decide(v, r);
        assert_eq!((got.decision, got.reason), (decision, reason), "detail: {}", got.detail);
    }

    #[test]
    fn rule_01_protocol_mismatch() {
        let mut r = req();
        r.protocol_version += 1;
        r.envelope_version += 1;
        check(&master(), &r, Reject, R::ProtocolMismatch);
    }

    #[test]
    fn rule_02_envelope_mismatch() {
        let mut r = req();
        r.envelope_version += 1;
        r.client_id = "a".into();
        check(&master(), &r, Reject, R::EnvelopeMismatch);
    }

    #[test]
    fn rule_03_loop_detected_for_self_and_chain() {
        let mut r = req();
        r.client_id = "b".into();
        check(&replica(), &r, Reject, R::LoopDetected);
        r.client_id = "a".into();
        let mut v = replica();
        v.ready = false;
        check(&v, &r, Reject, R::LoopDetected);
    }

    #[test]
    fn rule_04_not_ready() {
        let mut v = replica();
        v.ready = false;
        let mut r = req();
        r.known_identity = None;
        check(&v, &r, Reject, R::NotReady);
        let mut v = replica();
        v.identity = None;
        check(&v, &r, Reject, R::NotReady);
    }

    #[test]
    fn rule_05_no_local_state() {
        let mut r = req();
        r.known_identity = None;
        r.last_seq = 0;
        r.last_crc32c = None;
        check(&master(), &r, Bootstrap, R::NoLocalState);
    }

    #[test]
    fn rule_06_identity_changed_uuid() {
        let mut r = req();
        r.known_identity = Some(pb::Identity { db_uuid: uuid::Uuid::from_u128(9).as_bytes().to_vec(), epoch: 5 });
        check(&master(), &r, Bootstrap, R::IdentityChanged);
    }

    #[test]
    fn rule_07_stale_epoch() {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 3;
        r.last_seq = 999;
        check(&master(), &r, Reject, R::StaleEpoch);
    }

    #[test]
    fn rule_08_diverged_at_or_past_fork() {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.last_seq = 101;
        check(&master(), &r, Reject, R::Diverged);
        r.last_seq = 100;
        r.last_crc32c = Some(0xBBBB);
        check(&master(), &r, Bootstrap, R::IdentityChanged);
    }

    #[test]
    fn rule_09_epoch_advanced_when_crc_proves_continuity() {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.last_seq = 100;
        check(&master(), &r, Resume, R::EpochAdvanced);
        check(&replica(), &r, Resume, R::EpochAdvanced);
    }

    #[test]
    fn rule_10_epoch_change_without_proof() {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.last_seq = 30;
        r.last_crc32c = Some(0xAAAA);
        let mut v = master();
        v.crc_at_client_last = None; // entry 30 was trimmed
        check(&v, &r, Bootstrap, R::IdentityChanged);
        r.last_crc32c = None;
        check(&master(), &r, Bootstrap, R::IdentityChanged);
    }

    #[test]
    fn rule_11_client_ahead_of_master() {
        let mut r = req();
        r.last_seq = 201;
        let mut v = master();
        v.crc_at_client_last = None;
        check(&v, &r, Reject, R::Diverged);
    }

    #[test]
    fn rule_12_crc_differs() {
        let mut r = req();
        r.last_crc32c = Some(0xBBBB);
        check(&master(), &r, Reject, R::Diverged);
        check(&replica(), &r, Reject, R::Diverged);
    }

    #[test]
    fn rule_13_log_gap() {
        let mut r = req();
        r.last_seq = 48;
        let mut v = master();
        v.crc_at_client_last = None;
        check(&v, &r, Bootstrap, R::LogGap);
        r.last_seq = 49; // needs 50 = first retained
        check(&v, &r, Resume, R::Unspecified);
    }

    #[test]
    fn rule_14_resume_including_replica_behind_client() {
        check(&master(), &req(), Resume, R::Unspecified);
        let mut r = req();
        r.last_seq = 250; // ahead of a replica that crashed unsynced: allowed
        let mut v = replica();
        v.crc_at_client_last = None;
        check(&v, &r, Resume, R::Unspecified);
        // An empty client log with identity (bootstrapped at seq 0) resumes from 1.
        let mut r = req();
        r.last_seq = 0;
        r.last_crc32c = None;
        let mut v = master();
        v.first_retained_seq = 1;
        v.crc_at_client_last = None;
        check(&v, &r, Resume, R::Unspecified);
    }
}
