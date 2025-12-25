use std::hash::{Hash, Hasher};
use std::sync::{Arc, Mutex};
use crate::engine::sst::block::Shard;

/// 缓存 Key：唯一定位一个 block
#[derive(Clone, Debug, Eq)]
pub struct BlockCacheKey {
    pub file_number: u64,
    pub block_offset: u64,
}

impl PartialEq for BlockCacheKey {
    fn eq(&self, other: &Self) -> bool {
        self.file_number == other.file_number && self.block_offset == other.block_offset
    }
}

impl Hash for BlockCacheKey {
    fn hash<H: Hasher>(&self, state: &mut H) {
        state.write_u64(self.file_number);
        state.write_u64(self.block_offset);
    }
}

/// Sharded LRU Block Cache
pub struct BlockCache<V> {
    shards: Vec<Mutex<Shard<V>>>,
    shard_mask: usize, // 如果 shards 数是 2^n，mask 更快
}

impl<V> BlockCache<V>
where
    V: Send + Sync + 'static,
{
    /// shards 建议 16/32/64；capacity_bytes 总容量，自动均分到各 shard
    pub fn new(capacity_bytes: usize, shards: usize) -> Self {
        assert!(shards > 0);
        let shards_pow2 = shards.next_power_of_two();
        let per = capacity_bytes / shards_pow2;

        let mut v = Vec::with_capacity(shards_pow2);
        for _ in 0..shards_pow2 {
            v.push(Mutex::new(Shard::new(per)));
        }

        Self {
            shards: v,
            shard_mask: shards_pow2 - 1,
        }
    }

    #[inline]
    fn shard_index(&self, key: &BlockCacheKey) -> usize {
        // 一个简单、够用的混合 hash（你也可换成更强的）
        let x = key.file_number ^ key.block_offset.rotate_left(17);
        (x as usize) & self.shard_mask
    }

    /// 获取一个 block（命中则 move-to-front）
    pub fn get(&self, key: &BlockCacheKey) -> Option<Arc<V>> {
        let idx = self.shard_index(key);
        let mut g = self.shards[idx].lock().unwrap();
        g.get(key)
    }

    /// 插入/更新一个 block
    ///
    /// charge：该 block 占用字节（通常 = block_bytes.len() + overhead）
    pub fn insert(&self, key: BlockCacheKey, value: Arc<V>, charge: usize) {
        let idx = self.shard_index(&key);
        let mut g = self.shards[idx].lock().unwrap();
        g.insert(key, value, charge);
    }

    /// 删除一个 block（如果存在）
    pub fn erase(&self, key: &BlockCacheKey) {
        let idx = self.shard_index(key);
        let mut g = self.shards[idx].lock().unwrap();
        g.erase(key);
    }

    /// 当前使用字节（总和）
    pub fn usage_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|m| m.lock().unwrap().usage)
            .sum()
    }

    /// 总容量（总和）
    pub fn capacity_bytes(&self) -> usize {
        self.shards
            .iter()
            .map(|m| m.lock().unwrap().capacity)
            .sum()
    }
}


