use crate::engine::sst::hash64;

pub trait FilterPolicy: Send + Sync {
    fn may_match(&self, key: &[u8], filter: &[u8]) -> bool;
}

pub struct BloomFilterPolicy {
    k: u8,
}

impl BloomFilterPolicy {
    pub fn new(bits_per_key: usize) -> Self {
        let mut k = (bits_per_key as f64 * 0.69) as i32;
        if k < 1 { k = 1; }
        if k > 30 { k = 30; }
        Self { k: k as u8 }
    }
}

impl FilterPolicy for BloomFilterPolicy {
    fn may_match(&self, key: &[u8], filter: &[u8]) -> bool {
        if filter.len() < 2 {
            return true;
        }
        let bits = (filter.len() - 1) * 8;
        let k = filter[filter.len()-1];
        if k > 30 {
            return true;
        }

        let mut h = hash(key);
        let delta = (h >> 17) | (h << 15);

        for _ in 0..k {
            let bitpos = (h as usize) % bits;
            if (filter[bitpos/8] & (1 << (bitpos % 8))) == 0 {
                return false;
            }
            h = h.wrapping_add(delta);
        }
        true
    }
}

fn hash(key: &[u8]) -> u32 {
    // 可替换为 murmur
    let mut h: u32 = 2166136261;
    for &b in key {
        h ^= b as u32;
        h = h.wrapping_mul(16777619);
    }
    h
}

pub struct BloomFilterBuilder {
    bits_per_key: usize,
    k: u8,
    keys: Vec<Vec<u8>>,
}

impl BloomFilterBuilder {
    pub(crate) fn new(bits_per_key: usize) -> Self {
        // Use k ~ ln2 * bits_per_key (clamped)
        let mut k = ((bits_per_key as f64) * 0.69).round() as i32;
        if k < 1 {
            k = 1;
        } else if k > 30 {
            k = 30;
        }
        Self {
            bits_per_key,
            k: k as u8,
            keys: Vec::new(),
        }
    }

    pub(crate) fn add_key(&mut self, key: &[u8]) {
        self.keys.push(key.to_vec());
    }

    pub(crate) fn finish(&self) -> Vec<u8> {
        // Bit array size
        let n_keys = self.keys.len().max(1);
        let mut bits = n_keys * self.bits_per_key;
        // minimum bits
        if bits < 64 {
            bits = 64;
        }
        // round up to bytes
        let bytes = (bits + 7) / 8;
        let bits = bytes * 8;

        let mut filter = vec![0u8; bytes];
        for key in &self.keys {
            let h1 = hash64(key, 0x243F_6A88_85A3_08D3);
            let h2 = hash64(key, 0x1319_8A2E_0370_7344) | 1; // odd step

            let mut h = h1;
            for _ in 0..self.k {
                let bitpos = (h % (bits as u64)) as usize;
                filter[bitpos / 8] |= 1u8 << (bitpos % 8);
                h = h.wrapping_add(h2);
            }
        }

        // Append k (like LevelDB filter block stores k at end)
        filter.push(self.k);
        filter
    }
}
