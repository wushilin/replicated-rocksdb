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
    /// Includes the server's own `last_entry_crc` when `seq == last_seq` (PROTOCOL §6.3 note);
    /// `None` for `seq == 0`, `seq > last_seq`, or a trimmed entry.
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

/// Evaluates PROTOCOL.md §6.3 rules 1–14 (with 10a/10b) in order; the first match wins.
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
    let Some(ident) = v.identity.filter(|_| v.ready) else {
        return verdict(Reject, R::NotReady, format!("{} is bootstrapping", v.server_id));
    };
    // 5
    let Some(known) = &r.known_identity else {
        return verdict(Bootstrap, R::NoLocalState, "client has no data");
    };
    // 6
    if known.db_uuid.as_slice() != ident.db_uuid.as_bytes() {
        return verdict(Bootstrap, R::IdentityChanged, "database uuid differs");
    }
    let client_crc_matches = r.last_crc32c.is_some() && r.last_crc32c == v.crc_at_client_last;
    // 7
    if known.epoch > ident.epoch {
        let d = format!("client epoch {} is newer than server epoch {}", known.epoch, ident.epoch);
        return verdict(Reject, R::StaleEpoch, d);
    }
    if let Some(cfork) = changed_epoch_fork(v, known.epoch, ident.epoch, r.known_epoch_start) {
        // 8: the client holds entries at or past the point where its history leaves the server's.
        match cfork {
            Some(fork) if r.last_seq >= fork => {
                let d = format!("client last_seq {} is at or past fork point {fork} of epoch {}", r.last_seq, known.epoch);
                return verdict(Reject, R::Diverged, d);
            }
            Some(_) => {}
            None => {
                let d = format!("epoch history is inconsistent: no epoch after {} although server is at {}", known.epoch, ident.epoch);
                return verdict(Reject, R::Diverged, d);
            }
        }
        // 9: continuity proven by the CRC of the client's last entry.
        if client_crc_matches {
            return verdict(Resume, R::EpochAdvanced, format!("epoch {} -> {}", known.epoch, ident.epoch));
        }
        // 10a: a proven mismatch below the fork.
        if r.last_crc32c.is_some() && v.crc_at_client_last.is_some() {
            return verdict(Reject, R::Diverged, format!("entry {} differs", r.last_seq));
        }
        // 10b
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

/// `Some(cfork)` when the client's epoch is *changed* (PROTOCOL.md §6.3): an older epoch number,
/// or the same number with another start seq (`known_epoch_start`) than the server's record of
/// it. `cfork` is where the client's history leaves the server's (`None`: inconsistent).
/// `None` when the client follows the server's epoch.
fn changed_epoch_fork(v: &ServerView, known_epoch: u64, server_epoch: u64, known_start: Option<Seq>) -> Option<Option<Seq>> {
    let server_start = v.epoch_history.iter().find(|e| e.epoch == known_epoch).map(|e| e.start_seq);
    // The client's epoch began at another seq than the server's epoch of the same number (or
    // the server never had that epoch): the histories differ from the lower start on.
    let start_fork = known_start.filter(|&k| server_start != Some(k)).map(|k| server_start.map_or(k, |s| s.min(k)));
    if known_epoch == server_epoch && start_fork.is_none() {
        return None;
    }
    let fork = match (fork_after(&v.epoch_history, known_epoch), start_fork) {
        (Some(a), Some(b)) => Some(a.min(b)),
        (a, b) => a.or(b),
    };
    Some(fork)
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
            known_epoch_start: Some(101),
        }
    }

    /// `req()` at an older epoch: the client follows epoch 1 (start 1).
    fn epoch_1(last_seq: Seq) -> pb::HandshakeRequest {
        let mut r = req();
        r.known_identity.as_mut().unwrap().epoch = 1;
        r.known_epoch_start = Some(1);
        r.last_seq = last_seq;
        r
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
        let r = epoch_1(101);
        check(&master(), &r, Reject, R::Diverged);
        // Without known_epoch_start (the field is optional) the fork is the same.
        let mut r = epoch_1(101);
        r.known_epoch_start = None;
        check(&master(), &r, Reject, R::Diverged);
    }

    /// M1: below the fork, a CRC mismatch the server can prove is DIVERGED, not
    /// IDENTITY_CHANGED (which would invite a bootstrap under a Rebootstrap identity policy).
    #[test]
    fn rule_10a_proven_mismatch_below_the_fork_is_diverged() {
        let mut r = epoch_1(100);
        r.last_crc32c = Some(0xBBBB);
        check(&master(), &r, Reject, R::Diverged);
        check(&replica(), &r, Reject, R::Diverged);
    }

    #[test]
    fn rule_09_epoch_advanced_when_crc_proves_continuity() {
        let r = epoch_1(100);
        check(&master(), &r, Resume, R::EpochAdvanced);
        check(&replica(), &r, Resume, R::EpochAdvanced);
    }

    #[test]
    fn rule_10b_epoch_change_without_proof() {
        let mut r = epoch_1(30);
        r.last_crc32c = Some(0xAAAA);
        let mut v = master();
        v.crc_at_client_last = None; // entry 30 was trimmed
        check(&v, &r, Bootstrap, R::IdentityChanged);
        r.last_crc32c = None;
        check(&master(), &r, Bootstrap, R::IdentityChanged);
        // None == None must not count as proof.
        check(&v, &r, Bootstrap, R::IdentityChanged);
    }

    #[test]
    fn rule_08_inconsistent_history_fails_closed() {
        let mut v = master();
        v.epoch_history = vec![EpochStart { epoch: 1, start_seq: 1 }];
        let r = epoch_1(100);
        check(&v, &r, Reject, R::Diverged);
    }

    /// C1: the same epoch number started at another seq is another epoch. The server's epoch 2
    /// starts at 101; the client's epoch 2 at 81 (a promotion that was undone) or at 121.
    #[test]
    fn same_epoch_number_with_another_start_is_an_epoch_change() {
        // The client has nothing at or past min(81, 101) = 81, and the server holds its last
        // entry with the same CRC: continuity is proven, the client adopts the server's epoch.
        let mut r = req();
        r.known_epoch_start = Some(81);
        r.last_seq = 80;
        check(&master(), &r, Resume, R::EpochAdvanced);
        check(&replica(), &r, Resume, R::EpochAdvanced);
        // Entries at or past the lower start: diverged, whichever side started first.
        r.last_seq = 81;
        check(&master(), &r, Reject, R::Diverged);
        let mut r = req();
        r.known_epoch_start = Some(121);
        r.last_seq = 101;
        check(&master(), &r, Reject, R::Diverged);
        r.last_seq = 100;
        check(&master(), &r, Resume, R::EpochAdvanced);
        // Below the fork, a proven CRC mismatch is DIVERGED; no proof is IDENTITY_CHANGED.
        r.last_crc32c = Some(0xBBBB);
        check(&master(), &r, Reject, R::Diverged);
        let mut v = master();
        v.crc_at_client_last = None;
        check(&v, &r, Bootstrap, R::IdentityChanged);
        // The same start (or none sent) is the same epoch: plain resume.
        check(&master(), &req(), Resume, R::Unspecified);
        let mut r = req();
        r.known_epoch_start = None;
        check(&master(), &r, Resume, R::Unspecified);
    }

    /// An epoch the server never had (it skipped the number) forks at the client's start, even
    /// when the server's next epoch starts later.
    #[test]
    fn an_epoch_the_server_never_had_forks_at_the_clients_start() {
        let mut v = master();
        v.identity = Some(DbIdentity { db_uuid: uuid(), epoch: 3 });
        v.epoch_history = vec![EpochStart { epoch: 1, start_seq: 1 }, EpochStart { epoch: 3, start_seq: 101 }];
        let mut r = req(); // epoch 2 from 81
        r.known_epoch_start = Some(81);
        r.last_seq = 90;
        check(&v, &r, Reject, R::Diverged);
        r.last_seq = 80;
        check(&v, &r, Resume, R::EpochAdvanced);
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
        // Rule 12 needs the client CRC.
        r.last_crc32c = None;
        check(&master(), &r, Resume, R::Unspecified);
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
        // Rule 13 must not overflow.
        let mut r = req();
        r.last_seq = u64::MAX;
        r.last_crc32c = None;
        check(&replica(), &r, Resume, R::Unspecified);
    }
}
