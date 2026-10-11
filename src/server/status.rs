//! The read-only `Status` RPC (addendum §4), the in-process equivalent, and the views of the
//! same [`NodeInfo`] the HTTP status endpoints render (JSON and Prometheus text).

use std::fmt::Write as _;
use std::time::Duration;

use serde::Serialize;

use super::replicas::ReplicaRegistry;
use crate::admin::{NodeInfo, ReplicaSummary};
use crate::net::convert::{epochs_to_pb, identity_to_pb};
use crate::pb;
use crate::source::LogSource;
use crate::types::{Role, Seq};

/// What this node reports about itself.
/// `probes`: seqs whose entry CRC to report as well (`NodeInfo::probes`).
pub(crate) fn node_info(
    src: &dyn LogSource,
    replicas: &ReplicaRegistry,
    lag_max: Duration,
    probes: &[Seq],
) -> NodeInfo {
    let ident = src.identity();
    let pos = src.store().position();
    let last_ts_ms = src
        .store()
        .read_entries(pos.last_seq, 1, usize::MAX)
        .ok()
        .and_then(|v| v.first().filter(|e| e.seq == pos.last_seq).map(|e| e.ts_ms))
        .unwrap_or(0);
    let epoch = ident.as_ref().map_or(0, |(id, _)| id.epoch);
    NodeInfo {
        node_id: src.node_id().to_string(),
        role: src.role(),
        identity: ident.as_ref().map(|(id, _)| *id),
        epoch_history: ident.map(|(_, h)| h).unwrap_or_default(),
        last_seq: pos.last_seq,
        last_entry_crc: pos.last_entry_crc,
        last_ts_ms,
        node_state: src.node_state(),
        upstream: src.upstream(),
        replicas: replicas
            .summary(epoch, lag_max)
            .into_iter()
            .map(|(node_id, applied_seq, in_sync)| ReplicaSummary {
                node_id,
                applied_seq,
                in_sync,
            })
            .collect(),
        // `entry_crc` includes this node's own `last_entry_crc` at `last_seq`; a read error
        // reports the entry as not held.
        probes: probes
            .iter()
            .map(|&seq| (seq, src.store().entry_crc(seq).ok().flatten()))
            .collect(),
        // A read error reports none; promotion tooling also reads every history record.
        max_burned_epoch: src.store().max_burned_epoch().unwrap_or(0),
    }
}

/// The response for a request that asked about `probe_seq`.
pub(crate) fn to_pb(i: &NodeInfo, probe_seq: Option<Seq>) -> pb::StatusResponse {
    pb::StatusResponse {
        node_id: i.node_id.clone(),
        role: role_name(i.role).into(),
        identity: i.identity.as_ref().map(identity_to_pb),
        epoch_history: epochs_to_pb(&i.epoch_history),
        last_seq: i.last_seq,
        last_entry_crc: i.last_entry_crc,
        last_ts_ms: i.last_ts_ms,
        node_state: i.node_state.clone(),
        upstream: i.upstream.clone().unwrap_or_default(),
        replicas: i
            .replicas
            .iter()
            .map(|r| pb::ReplicaSummary {
                node_id: r.node_id.clone(),
                applied_seq: r.applied_seq,
                in_sync: r.in_sync,
            })
            .collect(),
        probe_crc: probe_seq.and_then(|seq| i.probes.get(&seq).copied().flatten()),
        max_burned_epoch: i.max_burned_epoch,
    }
}

fn role_name(role: Role) -> &'static str {
    match role {
        Role::Master => "master",
        Role::Slave => "slave",
    }
}

/// `/v1/status`: the `Status` RPC's data as readable JSON (UUIDs canonical, CRCs and nonces as
/// `0x` hex strings; a nonce is a full 64-bit random number, which JSON numbers do not carry
/// exactly in many parsers).
#[derive(Serialize)]
pub(crate) struct StatusJson {
    node_id: String,
    role: &'static str,
    identity: Option<IdentityJson>,
    epoch_history: Vec<EpochJson>,
    last_seq: Seq,
    last_entry_crc: Option<String>,
    last_ts_ms: u64,
    node_state: String,
    upstream: Option<String>,
    replicas: Vec<ReplicaJson>,
    max_burned_epoch: u64,
}

#[derive(Serialize)]
struct IdentityJson {
    db_uuid: String,
    epoch: u64,
}

#[derive(Serialize)]
struct EpochJson {
    epoch: u64,
    start_seq: Seq,
    nonce: String,
}

#[derive(Serialize)]
struct ReplicaJson {
    node_id: String,
    applied_seq: Seq,
    in_sync: bool,
}

pub(crate) fn to_json(i: &NodeInfo) -> StatusJson {
    StatusJson {
        node_id: i.node_id.clone(),
        role: role_name(i.role),
        identity: i.identity.map(|id| IdentityJson {
            db_uuid: id.db_uuid.to_string(),
            epoch: id.epoch,
        }),
        epoch_history: i
            .epoch_history
            .iter()
            .map(|e| EpochJson {
                epoch: e.epoch,
                start_seq: e.start_seq,
                nonce: format!("0x{:016x}", e.nonce),
            })
            .collect(),
        last_seq: i.last_seq,
        last_entry_crc: i.last_entry_crc.map(|c| format!("0x{c:08x}")),
        last_ts_ms: i.last_ts_ms,
        node_state: i.node_state.clone(),
        upstream: i.upstream.clone(),
        replicas: i
            .replicas
            .iter()
            .map(|r| ReplicaJson {
                node_id: r.node_id.clone(),
                applied_seq: r.applied_seq,
                in_sync: r.in_sync,
            })
            .collect(),
        max_burned_epoch: i.max_burned_epoch,
    }
}

/// Whether `/v1/ready` reports the node ready: its `node_state` is `ready`.
pub(crate) fn is_ready(i: &NodeInfo) -> bool {
    i.node_state == "ready"
}

/// `/metrics`: Prometheus text exposition format 0.0.4.
pub(crate) fn to_prometheus(i: &NodeInfo) -> String {
    let mut out = String::new();
    let mut gauge = |name: &str, help: &str, samples: &[(String, u64)]| {
        let _ = writeln!(out, "# HELP {name} {help}");
        let _ = writeln!(out, "# TYPE {name} gauge");
        for (labels, value) in samples {
            let _ = writeln!(out, "{name}{labels} {value}");
        }
    };
    let epoch = i.identity.map_or(0, |id| id.epoch);
    gauge(
        "rrdb_last_seq",
        "Seq of the last entry this node holds.",
        &[(String::new(), i.last_seq)],
    );
    gauge(
        "rrdb_last_ts_ms",
        "Master timestamp (ms) of the entry at rrdb_last_seq; 0 if no longer retained.",
        &[(String::new(), i.last_ts_ms)],
    );
    gauge(
        "rrdb_role",
        "This node's role (1 for the current one).",
        &[Role::Master, Role::Slave].map(|r| {
            let name = role_name(r);
            (format!("{{role=\"{name}\"}}"), u64::from(r == i.role))
        }),
    );
    gauge(
        "rrdb_node_ready",
        "1 when node_state is ready.",
        &[(String::new(), u64::from(is_ready(i)))],
    );
    gauge(
        "rrdb_epoch",
        "Current epoch of the origin identity; 0 without data.",
        &[(String::new(), epoch)],
    );
    let label = |r: &ReplicaSummary| format!("{{node_id=\"{}\"}}", escape_label(&r.node_id));
    gauge(
        "rrdb_replica_applied_seq",
        "Last seq each direct downstream replica acknowledged.",
        &i.replicas
            .iter()
            .map(|r| (label(r), r.applied_seq))
            .collect::<Vec<_>>(),
    );
    gauge(
        "rrdb_replica_in_sync",
        "1 while a direct downstream replica is in sync.",
        &i.replicas
            .iter()
            .map(|r| (label(r), u64::from(r.in_sync)))
            .collect::<Vec<_>>(),
    );
    out
}

/// A label value in the Prometheus text format: `\`, `"` and newline escaped.
fn escape_label(v: &str) -> String {
    let mut out = String::with_capacity(v.len());
    for c in v.chars() {
        match c {
            '\\' => out.push_str("\\\\"),
            '"' => out.push_str("\\\""),
            '\n' => out.push_str("\\n"),
            c => out.push(c),
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{DbIdentity, EpochStart};

    fn info() -> NodeInfo {
        let mut i = NodeInfo::new("a", Role::Master);
        i.identity = Some(DbIdentity {
            db_uuid: uuid::Uuid::from_u128(0x0123_4567_89ab_cdef_0123_4567_89ab_cdef),
            epoch: 3,
        });
        i.epoch_history = vec![EpochStart {
            epoch: 3,
            start_seq: 1,
            nonce: u64::MAX,
        }];
        i.last_seq = 42;
        i.last_entry_crc = Some(0xab);
        i.last_ts_ms = 7;
        i.replicas = vec![ReplicaSummary {
            node_id: "b\"\\\nx".into(),
            applied_seq: 40,
            in_sync: true,
        }];
        i
    }

    #[test]
    fn json_view_has_readable_fields() {
        let v = serde_json::to_value(to_json(&info())).unwrap();
        assert_eq!(v["role"], "master");
        assert_eq!(
            v["identity"]["db_uuid"],
            "01234567-89ab-cdef-0123-456789abcdef"
        );
        assert_eq!(v["identity"]["epoch"], 3);
        assert_eq!(v["epoch_history"][0]["nonce"], "0xffffffffffffffff");
        assert_eq!(v["epoch_history"][0]["start_seq"], 1);
        assert_eq!(v["last_seq"], 42);
        assert_eq!(v["last_entry_crc"], "0x000000ab");
        assert_eq!(v["upstream"], serde_json::Value::Null);
        assert_eq!(v["replicas"][0]["applied_seq"], 40);
        assert_eq!(v["max_burned_epoch"], 0);
        let mut empty = NodeInfo::new("b", Role::Slave);
        empty.node_state = "bootstrapping".into();
        let v = serde_json::to_value(to_json(&empty)).unwrap();
        assert_eq!(v["identity"], serde_json::Value::Null);
        assert_eq!(v["last_entry_crc"], serde_json::Value::Null);
    }

    #[test]
    fn prometheus_view_escapes_labels() {
        let text = to_prometheus(&info());
        assert!(text.contains("\nrrdb_last_seq 42\n"), "{text}");
        assert!(text.contains("rrdb_role{role=\"master\"} 1\n"), "{text}");
        assert!(text.contains("rrdb_role{role=\"slave\"} 0\n"), "{text}");
        assert!(text.contains("rrdb_node_ready 1\n"), "{text}");
        assert!(text.contains("rrdb_epoch 3\n"), "{text}");
        assert!(
            text.contains("rrdb_replica_applied_seq{node_id=\"b\\\"\\\\\\nx\"} 40\n"),
            "{text}"
        );
        assert!(
            text.contains("# TYPE rrdb_replica_in_sync gauge\n"),
            "{text}"
        );
    }
}
