//! A cache-local bloom filter, one per table file.
//!
//! Each key sets all its probe bits inside one 64-byte line (RocksDB's
//! "FastLocalBloom" layout), so a lookup touches one cache line. At 10 bits
//! per key with 6 probes it answers "maybe" for about 1% of absent keys.
//!
//! ```text
//! filter := line[64 bytes] * n | num_probes u8
//! ```

use xxhash_rust::xxh3::xxh3_64;

const LINE_BITS: u32 = 512;
const LINE_BYTES: usize = 64;
pub const BITS_PER_KEY: usize = 10;
const NUM_PROBES: u8 = 6;

/// The hash a key is filtered on: column family and user key, so every
/// version of a key maps to the same bits.
pub fn hash(cf_and_user_key: &[u8]) -> u64 {
    xxh3_64(cf_and_user_key)
}

fn probes(hash: u64, lines: usize) -> (usize, impl Iterator<Item = u32>) {
    // Upper half picks the line (multiply-shift, no modulo bias to speak of),
    // lower half walks the bits inside it.
    let line = (((hash >> 32) * lines as u64) >> 32) as usize;
    let mut h = hash as u32;
    let bits = (0..NUM_PROBES).map(move |_| {
        let bit = h >> (32 - 9);
        h = h.wrapping_mul(0x9e37_79b9);
        bit
    });
    (line, bits)
}

pub fn build(hashes: &[u64]) -> Vec<u8> {
    let lines = (hashes.len() * BITS_PER_KEY)
        .div_ceil(LINE_BITS as usize)
        .max(1);
    let mut filter = vec![0u8; lines * LINE_BYTES + 1];
    for &hash in hashes {
        let (line, bits) = probes(hash, lines);
        let base = line * LINE_BYTES;
        for bit in bits {
            filter[base + (bit / 8) as usize] |= 1 << (bit % 8);
        }
    }
    filter[lines * LINE_BYTES] = NUM_PROBES;
    filter
}

/// `false` means the key is certainly absent. A filter this crate did not
/// write (wrong length or probe count) answers `true`: no false negatives.
pub fn may_contain(filter: &[u8], hash: u64) -> bool {
    let Some((&num_probes, lines)) = filter.split_last() else {
        return true;
    };
    if num_probes != NUM_PROBES || lines.is_empty() || lines.len() % LINE_BYTES != 0 {
        return true;
    }
    let (line, mut bits) = probes(hash, lines.len() / LINE_BYTES);
    let base = line * LINE_BYTES;
    bits.all(|bit| lines[base + (bit / 8) as usize] & (1 << (bit % 8)) != 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn no_false_negatives_and_about_one_percent_false_positives() {
        let present: Vec<u64> = (0..10_000u32).map(|i| hash(&i.to_be_bytes())).collect();
        let filter = build(&present);
        assert!(present.iter().all(|h| may_contain(&filter, *h)));
        let false_positives = (10_000..110_000u32)
            .filter(|i| may_contain(&filter, hash(&i.to_be_bytes())))
            .count();
        let rate = false_positives as f64 / 100_000.0;
        assert!(rate < 0.02, "false positive rate {rate}");
    }

    #[test]
    fn an_empty_table_excludes_everything_and_a_foreign_filter_nothing() {
        assert!(
            !may_contain(&build(&[]), hash(b"x")),
            "a table with no keys excludes every key"
        );
        assert!(may_contain(&[], 1));
        assert!(may_contain(&[0; 10], 1));
    }
}
