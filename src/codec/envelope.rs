//! Value envelope (PROTOCOL.md §5.3).

use super::ENVELOPE_VERSION;
use crate::error::{Error, Result};
use crate::types::{Seq, ValueMeta};

pub const FLAG_TTL: u8 = 0x01;
const BASE_HEADER: usize = 10;
const TTL_HEADER: usize = 18;

pub fn encode(write_seq: Seq, expire_at_ms: Option<u64>, value: &[u8]) -> Vec<u8> {
    let header = if expire_at_ms.is_some() {
        TTL_HEADER
    } else {
        BASE_HEADER
    };
    let mut out = Vec::with_capacity(header + value.len());
    out.push(ENVELOPE_VERSION);
    out.push(if expire_at_ms.is_some() { FLAG_TTL } else { 0 });
    out.extend_from_slice(&write_seq.to_be_bytes());
    if let Some(at) = expire_at_ms {
        out.extend_from_slice(&at.to_be_bytes());
    }
    out.extend_from_slice(value);
    out
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Envelope<'a> {
    pub meta: ValueMeta,
    /// 10, or 18 with TTL. `raw[..header_len]` is the Quick-verify payload.
    pub header_len: usize,
    pub value: &'a [u8],
}

impl Envelope<'_> {
    pub fn is_expired(&self, now_ms: u64) -> bool {
        self.meta.expire_at_ms.is_some_and(|at| at <= now_ms)
    }
}

pub fn decode(raw: &[u8]) -> Result<Envelope<'_>> {
    if raw.len() < BASE_HEADER {
        return Err(Error::corrupt("value envelope is shorter than its header"));
    }
    if raw[0] != ENVELOPE_VERSION {
        return Err(Error::corrupt(format!(
            "unknown envelope version {}",
            raw[0]
        )));
    }
    let flags = raw[1];
    if flags & !FLAG_TTL != 0 {
        return Err(Error::corrupt(format!(
            "reserved envelope flag bits set: {flags:#04x}"
        )));
    }
    let write_seq = be64(&raw[2..10]);
    let (expire_at_ms, header_len) = if flags & FLAG_TTL != 0 {
        if raw.len() < TTL_HEADER {
            return Err(Error::corrupt(
                "value envelope is shorter than its TTL header",
            ));
        }
        (Some(be64(&raw[10..18])), TTL_HEADER)
    } else {
        (None, BASE_HEADER)
    };
    Ok(Envelope {
        meta: ValueMeta {
            write_seq,
            expire_at_ms,
        },
        header_len,
        value: &raw[header_len..],
    })
}

fn be64(b: &[u8]) -> u64 {
    let mut a = [0u8; 8];
    a.copy_from_slice(b);
    u64::from_be_bytes(a)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::entry::tests::hex;

    #[test]
    fn vector_v2_envelopes() {
        assert_eq!(
            encode(1, None, b"v1"),
            hex("01 00 00 00 00 00 00 00 00 01 76 31")
        );
        assert_eq!(
            encode(1, Some(1_700_000_060_000), b"v1"),
            hex("01 01 00 00 00 00 00 00 00 01 00 00 01 8b cf e6 52 60 76 31")
        );
    }

    #[test]
    fn decode_round_trips() {
        let raw = encode(9, Some(50), b"");
        let env = decode(&raw).unwrap();
        assert_eq!(
            env.meta,
            ValueMeta {
                write_seq: 9,
                expire_at_ms: Some(50)
            }
        );
        assert_eq!(env.header_len, 18);
        assert_eq!(env.value, b"");
        assert!(!env.is_expired(49));
        assert!(env.is_expired(50));
        assert!(!decode(&encode(9, None, b"x")).unwrap().is_expired(u64::MAX));
    }

    #[test]
    fn decode_rejects_bad_envelopes() {
        let short = &encode(1, None, b"")[..9];
        let mut wrong_ver = encode(1, None, b"");
        wrong_ver[0] = 2;
        let mut reserved = encode(1, None, b"");
        reserved[1] = 0x02;
        let ttl_short = &encode(1, Some(5), b"")[..17];
        for raw in [short, &wrong_ver[..], &reserved[..], ttl_short] {
            assert!(matches!(decode(raw), Err(Error::Corrupt(_))));
        }
    }
}
