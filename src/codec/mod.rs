//! Wire and storage encodings shared by every node (PROTOCOL.md §5).

pub mod digest;
pub mod entry;
pub mod envelope;

pub use entry::{entry_crc, validate_cf_name};
#[allow(unused_imports)] // used by later tasks
pub(crate) use entry::{check_structure, decode_ops, encode_ops};

/// Wire protocol version. Must match exactly between nodes.
pub const PROTOCOL_VERSION: u32 = 1;
/// Value envelope format version. Must match exactly between nodes.
pub const ENVELOPE_VERSION: u8 = 1;
/// Maximum length of a user CF name, in bytes.
pub const MAX_CF_NAME_BYTES: usize = 255;
