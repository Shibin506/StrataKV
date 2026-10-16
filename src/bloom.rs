//! A Bloom filter: a small bit array that answers "definitely not here" or "maybe here".
//! With 10 bits per key the "maybe" is wrong about 1% of the time, so 99% of lookups for a key that
//! a table does not contain skip reading the table's data entirely.
//!
//! The hash is defined here (not `std`'s, whose algorithm may change between Rust versions) because
//! filters are stored on disk and must read the same forever.

/// FNV-1a followed by a SplitMix64 finalizer: stable, fast, and well spread.
pub fn hash64(key: &[u8]) -> u64 {
    let mut h: u64 = 0xcbf2_9ce4_8422_2325;
    for &b in key {
        h ^= b as u64;
        h = h.wrapping_mul(0x0000_0100_0000_01b3);
    }
    mix(h)
}

fn mix(mut z: u64) -> u64 {
    z = (z ^ (z >> 30)).wrapping_mul(0xbf58_476d_1ce4_e5b9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94d0_49bb_1331_11eb);
    z ^ (z >> 31)
}

/// Kirsch–Mitzenmacher: k bit positions from two hashes, `h1 + i*h2`.
fn positions(k: u8, h: u64, m: u64) -> impl Iterator<Item = u64> {
    let h2 = mix(h ^ 0x9e37_79b9_7f4a_7c15) | 1;
    (0..k as u64).map(move |i| h.wrapping_add(i.wrapping_mul(h2)) % m)
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Bloom {
    k: u8,
    bits: Vec<u8>,
}

impl Bloom {
    /// Builds a filter for keys given by their `hash64`.
    pub fn build(hashes: &[u64], bits_per_key: u32) -> Self {
        let nbits = (hashes.len() as u64 * bits_per_key as u64).max(64);
        let nbytes = nbits.div_ceil(8) as usize;
        let k = ((bits_per_key as f64) * std::f64::consts::LN_2).round().clamp(1.0, 30.0) as u8;
        let mut f = Bloom { k, bits: vec![0; nbytes] };
        let m = (nbytes * 8) as u64;
        for &h in hashes {
            for bit in positions(k, h, m) {
                f.bits[(bit / 8) as usize] |= 1 << (bit % 8);
            }
        }
        f
    }

    pub fn may_contain(&self, key: &[u8]) -> bool {
        let m = (self.bits.len() * 8) as u64;
        if m == 0 {
            return true;
        }
        positions(self.k, hash64(key), m).all(|bit| self.bits[(bit / 8) as usize] & (1 << (bit % 8)) != 0)
    }

    /// `k u8` then the bit array.
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(1 + self.bits.len());
        out.push(self.k);
        out.extend_from_slice(&self.bits);
        out
    }

    pub fn decode(buf: &[u8]) -> Option<Self> {
        let (&k, bits) = buf.split_first()?;
        (1..=30).contains(&k).then(|| Bloom { k, bits: bits.to_vec() })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn keys(prefix: &str, n: usize) -> Vec<Vec<u8>> {
        (0..n).map(|i| format!("{prefix}{i}").into_bytes()).collect()
    }

    #[test]
    fn never_says_no_for_a_key_it_holds() {
        let present = keys("in-", 10_000);
        let f = Bloom::build(&present.iter().map(|k| hash64(k)).collect::<Vec<_>>(), 10);
        assert!(present.iter().all(|k| f.may_contain(k)));
    }

    #[test]
    fn false_positive_rate_is_about_one_percent_at_ten_bits_per_key() {
        let present = keys("in-", 10_000);
        let f = Bloom::build(&present.iter().map(|k| hash64(k)).collect::<Vec<_>>(), 10);
        let absent = keys("out-", 100_000);
        let fp = absent.iter().filter(|k| f.may_contain(k)).count() as f64 / absent.len() as f64;
        assert!(fp < 0.02, "false positive rate {fp}");
    }

    #[test]
    fn round_trips_and_the_hash_never_changes() {
        let f = Bloom::build(&[hash64(b"a"), hash64(b"b")], 10);
        assert_eq!(Bloom::decode(&f.encode()).unwrap(), f);
        // Golden values: filters on disk depend on them, so they must never change.
        assert_eq!(hash64(b"stratakv"), 0xc83f_11e6_174c_afaa);
        assert_eq!(hash64(b""), 0xf52a_15e9_a9b5_e89b);
        assert!(Bloom::decode(&[0]).is_none() && Bloom::decode(&[]).is_none());
    }

    #[test]
    fn an_empty_filter_is_valid() {
        let f = Bloom::build(&[], 10);
        assert!(!f.bits.is_empty());
        let _ = f.may_contain(b"anything");
    }
}
