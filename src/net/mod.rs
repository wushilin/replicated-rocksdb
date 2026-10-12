//! Wire helpers shared by the replication server and client: request metadata keys and the
//! error model (PROTOCOL.md §3.2, §10).

pub(crate) mod convert;

use std::collections::HashMap;

use tonic::metadata::MetadataMap;
use tonic::{Code, Status};
use tonic_types::{ErrorDetails, StatusExt};

use crate::codec::PROTOCOL_VERSION;
use crate::pb;

pub(crate) const MD_PROTOCOL_VERSION: &str = "x-rrdb-protocol-version";
pub(crate) const MD_NODE_ID: &str = "x-rrdb-node-id";
pub(crate) const MD_AUTHORIZATION: &str = "authorization";
/// `google.rpc.ErrorInfo.domain` of every replication error.
pub(crate) const ERROR_DOMAIN: &str = "rrdb.replication.v1";
/// Extra room on top of the payload size for protobuf framing (PROTOCOL.md §11.4).
pub(crate) const DECODE_SLACK: usize = 64 << 10;
/// Requests are small; the server never accepts larger ones.
pub(crate) const MAX_REQUEST_BYTES: usize = 4 << 20;

/// The gRPC status code that carries `reason` (PROTOCOL.md §10).
pub(crate) fn code_for(reason: pb::Reason) -> Code {
    use pb::Reason as R;
    match reason {
        R::NotReady | R::UpstreamReset | R::ShuttingDown => Code::Unavailable,
        R::LogGap => Code::OutOfRange,
        R::Busy => Code::ResourceExhausted,
        R::SessionNotFound => Code::NotFound,
        R::Corrupt => Code::DataLoss,
        R::Unspecified => Code::Unknown,
        R::NoLocalState
        | R::IdentityChanged
        | R::ProtocolMismatch
        | R::EnvelopeMismatch
        | R::LoopDetected
        | R::Diverged
        | R::EpochAdvanced
        | R::StaleEpoch => Code::FailedPrecondition,
    }
}

/// A failed RPC carrying `ErrorInfo { reason, domain: "rrdb.replication.v1" }`.
pub(crate) fn reason_status(reason: pb::Reason, message: impl Into<String>) -> Status {
    let details = ErrorDetails::with_error_info(reason.as_str_name(), ERROR_DOMAIN, HashMap::new());
    Status::with_error_details(code_for(reason), message, details)
}

/// An unexpected server-side failure: `INTERNAL`, still with `ErrorInfo`
/// (`REASON_UNSPECIFIED`), so every failure carries the error domain.
pub(crate) fn internal_status(message: impl Into<String>) -> Status {
    unspecified_status(Code::Internal, message)
}

/// A failed RPC with `code` and `ErrorInfo` (`REASON_UNSPECIFIED`), for failures that have no
/// protocol reason of their own (`INVALID_ARGUMENT`, `UNIMPLEMENTED`).
fn unspecified_status(code: Code, message: impl Into<String>) -> Status {
    let details = ErrorDetails::with_error_info(
        pb::Reason::Unspecified.as_str_name(),
        ERROR_DOMAIN,
        HashMap::new(),
    );
    Status::with_error_details(code, message, details)
}

/// `INVALID_ARGUMENT` with `ErrorInfo` (`REASON_UNSPECIFIED`).
pub(crate) fn invalid_argument(message: impl Into<String>) -> Status {
    unspecified_status(Code::InvalidArgument, message)
}

/// The protocol reason carried by `status`, if any.
pub(crate) fn reason_of(status: &Status) -> Option<pb::Reason> {
    let details = status.get_error_details();
    let info = details.error_info()?;
    if info.domain != ERROR_DOMAIN {
        return None;
    }
    pb::Reason::from_str_name(&info.reason)
}

/// Fails with `FAILED_PRECONDITION / PROTOCOL_MISMATCH` unless the call carries our
/// `x-rrdb-protocol-version` (every RPC except Handshake, PROTOCOL.md §3.2).
pub(crate) fn check_protocol_version(md: &MetadataMap) -> Result<(), Status> {
    let sent = md
        .get(MD_PROTOCOL_VERSION)
        .and_then(|v| v.to_str().ok())
        .and_then(|v| v.parse::<u32>().ok());
    match sent {
        Some(v) if v == PROTOCOL_VERSION => Ok(()),
        other => Err(reason_status(
            pb::Reason::ProtocolMismatch,
            format!("protocol version {other:?} sent, this server speaks {PROTOCOL_VERSION}"),
        )),
    }
}

/// The bearer token from `authorization: Bearer <token>`, if present.
pub(crate) fn bearer_token(md: &MetadataMap) -> Option<Vec<u8>> {
    parse_bearer(md.get(MD_AUTHORIZATION)?.as_bytes())
}

/// The token of an `authorization` header value `Bearer <token>` (PROTOCOL.md §3.2), shared by
/// gRPC metadata and the HTTP status endpoints.
pub(crate) fn parse_bearer(value: &[u8]) -> Option<Vec<u8>> {
    value.strip_prefix(b"Bearer ").map(<[u8]>::to_vec)
}

/// The self-asserted node id from `x-rrdb-node-id`.
pub(crate) fn node_id(md: &MetadataMap) -> Option<String> {
    md.get(MD_NODE_ID)
        .and_then(|v| v.to_str().ok())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::AsciiMetadataValue;

    #[test]
    fn every_reason_round_trips_through_a_status() {
        for n in 0..=15 {
            let reason = pb::Reason::try_from(n).unwrap();
            let s = reason_status(reason, "msg");
            assert_eq!(reason_of(&s), Some(reason), "{reason:?}");
            assert_eq!(s.code(), code_for(reason));
            assert_eq!(s.message(), "msg");
        }
    }

    #[test]
    fn codes_follow_the_error_table() {
        use pb::Reason as R;
        assert_eq!(code_for(R::ProtocolMismatch), Code::FailedPrecondition);
        assert_eq!(code_for(R::NotReady), Code::Unavailable);
        assert_eq!(code_for(R::LogGap), Code::OutOfRange);
        assert_eq!(code_for(R::Busy), Code::ResourceExhausted);
        assert_eq!(code_for(R::SessionNotFound), Code::NotFound);
        assert_eq!(code_for(R::UpstreamReset), Code::Unavailable);
        assert_eq!(code_for(R::ShuttingDown), Code::Unavailable);
        assert_eq!(code_for(R::Corrupt), Code::DataLoss);
        assert_eq!(code_for(R::StaleEpoch), Code::FailedPrecondition);
    }

    #[test]
    fn internal_failures_carry_error_info() {
        let s = internal_status("disk on fire");
        assert_eq!(s.code(), Code::Internal);
        assert_eq!(reason_of(&s), Some(pb::Reason::Unspecified));
    }

    #[test]
    fn plain_status_has_no_reason() {
        assert_eq!(reason_of(&Status::unavailable("x")), None);
        let foreign = Status::with_error_details(
            Code::Unavailable,
            "x",
            ErrorDetails::with_error_info("BUSY", "other.domain", HashMap::new()),
        );
        assert_eq!(reason_of(&foreign), None);
    }

    #[test]
    fn metadata_helpers() {
        let mut md = MetadataMap::new();
        assert!(check_protocol_version(&md).is_err());
        md.insert(
            MD_PROTOCOL_VERSION,
            AsciiMetadataValue::from(PROTOCOL_VERSION),
        );
        check_protocol_version(&md).unwrap();
        md.insert(
            MD_PROTOCOL_VERSION,
            AsciiMetadataValue::from(PROTOCOL_VERSION + 1),
        );
        let err = check_protocol_version(&md).unwrap_err();
        assert_eq!(reason_of(&err), Some(pb::Reason::ProtocolMismatch));

        assert_eq!(bearer_token(&md), None);
        md.insert(
            MD_AUTHORIZATION,
            AsciiMetadataValue::from_static("Bearer s3cr.et-_~+/="),
        );
        assert_eq!(bearer_token(&md).unwrap(), b"s3cr.et-_~+/=");
        md.insert(
            MD_AUTHORIZATION,
            AsciiMetadataValue::from_static("Basic abc"),
        );
        assert_eq!(bearer_token(&md), None);
        md.insert(MD_NODE_ID, AsciiMetadataValue::from_static("node-b"));
        assert_eq!(node_id(&md).as_deref(), Some("node-b"));
    }
}
