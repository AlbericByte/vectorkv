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
