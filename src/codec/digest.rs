//! BLAKE3 digest framing for Bootstrap and Verify (PROTOCOL.md §5.4).

use crate::types::Seq;

/// Incremental BLAKE3 digest over CF markers and `(key, payload)` records, used by Bootstrap
/// and Verify so both sides hash identical framing.
pub struct Digest {
    hasher: blake3::Hasher,
    records: u64,
}

impl Default for Digest {
    fn default() -> Self {
        Self::new()
    }
}

impl Digest {
    /// An empty digest.
    pub fn new() -> Self {
        Digest {
            hasher: blake3::Hasher::new(),
            records: 0,
        }
    }

    /// `0x01 ‖ BE64(len(name)) ‖ name ‖ BE64(create_seq)`
    pub fn cf_marker(&mut self, name: &str, create_seq: Seq) {
        self.hasher.update(&[0x01]);
        self.hasher.update(&(name.len() as u64).to_be_bytes());
        self.hasher.update(name.as_bytes());
        self.hasher.update(&create_seq.to_be_bytes());
    }

    /// `0x02 ‖ BE64(len(key)) ‖ key ‖ BE64(len(payload)) ‖ payload`
    pub fn record(&mut self, key: &[u8], payload: &[u8]) {
        self.hasher.update(&[0x02]);
        self.hasher.update(&(key.len() as u64).to_be_bytes());
        self.hasher.update(key);
        self.hasher.update(&(payload.len() as u64).to_be_bytes());
        self.hasher.update(payload);
        self.records += 1;
    }

    /// Number of records hashed so far (CF markers are not counted).
    pub fn records(&self) -> u64 {
        self.records
    }

    /// Range hash for Verify: records only.
    pub fn finish_range(self) -> (u64, [u8; 32]) {
        (self.records, *self.hasher.finalize().as_bytes())
    }

    /// Bootstrap digest: appends `0x03 ‖ BE64(snapshot_seq) ‖ BE64(record_count)`.
    pub fn finish_bootstrap(mut self, snapshot_seq: Seq) -> (u64, [u8; 32]) {
        self.hasher.update(&[0x03]);
        self.hasher.update(&snapshot_seq.to_be_bytes());
        self.hasher.update(&self.records.to_be_bytes());
        (self.records, *self.hasher.finalize().as_bytes())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::codec::envelope;

    fn hex32(s: &str) -> [u8; 32] {
        let v: Vec<u8> = (0..32)
            .map(|i| u8::from_str_radix(&s[2 * i..2 * i + 2], 16).unwrap())
            .collect();
        v.try_into().unwrap()
    }

    #[test]
    fn vector_v3_digests() {
        let env = envelope::encode(1, None, b"v1");

        let mut boot = Digest::new();
        boot.cf_marker("users", 1);
        boot.record(b"k1", &env);
        assert_eq!(
            boot.finish_bootstrap(1),
            (
                1,
                hex32("3aa1d6c512a91d54c3da5ef18a6efef531d61b7d3ec9b37c1ba907832fb22729")
            )
        );

        let mut quick = Digest::new();
        quick.record(b"k1", &env[..10]);
        assert_eq!(
            quick.finish_range(),
            (
                1,
                hex32("2fe3008b6cde35de0a02bc7e782ab96808ae937eac33d5433182e9cae37c1bb3")
            )
        );

        let mut full = Digest::new();
        full.record(b"k1", &env);
        assert_eq!(
            full.finish_range().1,
            hex32("14e556f28a8301390e9be56ecb4ec7ff3ceea3cd48316ab7f7f0488117e53ecd")
        );

        assert_eq!(
            Digest::new().finish_range(),
            (
                0,
                hex32("af1349b9f5f9a1a6a0404dea36dcc9499bcb25c9adc112b7cc9a93cae41f3262")
            )
        );
    }
}
