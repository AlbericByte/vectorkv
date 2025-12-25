use std::collections::HashMap;
use std::ptr::NonNull;
use std::sync::Arc;
use crate::engine::sst::block::{LruList, Node};
use crate::engine::sst::block::BlockCacheKey;

pub struct Shard<V> {
    pub(crate) map: HashMap<BlockCacheKey, NonNull<Node<V>>>,
    pub(crate) lru: LruList<V>,
    pub(crate) usage: usize,
    pub(crate) capacity: usize,
}

impl<V> Shard<V> {
    pub fn new(capacity: usize) -> Self {
        Self {
            map: HashMap::new(),
            lru: LruList::new(),
            usage: 0,
            capacity,
        }
    }

    pub fn get(&mut self, key: &BlockCacheKey) -> Option<Arc<V>> {
        let ptr = *self.map.get(key)?;
        // SAFETY: ptr 始终指向我们分配的 Node，且在 map 删除前不会释放
        let node = unsafe { ptr.as_ref() };

        // move-to-front（最近使用）
        self.lru.move_to_front(ptr);

        Some(Arc::clone(&node.value))
    }

    pub fn insert(&mut self, key: BlockCacheKey, value: Arc<V>, charge: usize) {
        // 如果已存在：更新 value/charge，并 move-to-front
        if let Some(&ptr) = self.map.get(&key) {
            let mut ptr = ptr;
            // SAFETY: 同上
            let node = unsafe { ptr.as_ref() };

            // usage 修正：先减旧 charge
            self.usage = self.usage.saturating_sub(node.charge);

            // SAFETY: 我们需要可变引用来更新 node 字段
            let node_mut = unsafe { ptr.as_mut() };
            node_mut.value = value;
            node_mut.charge = charge;

            self.usage += charge;

            self.lru.move_to_front(ptr);
            self.evict_if_needed();
            return;
        }

        // 新建 node
        let node = Box::new(Node {
            key: key.clone(),
            value,
            charge,
            prev: None,
            next: None,
        });

        let ptr = unsafe { NonNull::new_unchecked(Box::into_raw(node)) };

        self.lru.push_front(ptr);
        self.map.insert(key, ptr);
        self.usage += charge;

        self.evict_if_needed();
    }

    pub fn erase(&mut self, key: &BlockCacheKey) {
        if let Some(ptr) = self.map.remove(key) {
            // 从 LRU 链表移除
            self.lru.remove(ptr);

            // 回收 node
            // SAFETY: ptr 来自 Box::into_raw，且我们已经从 list/map 去掉它
            let boxed = unsafe { Box::from_raw(ptr.as_ptr()) };
            self.usage = self.usage.saturating_sub(boxed.charge);
            // drop(boxed) 自动释放
        }
    }

    pub fn evict_if_needed(&mut self) {
        if self.usage <= self.capacity {
            return;
        }

        // 从 LRU 尾部开始淘汰（最久未使用）
        // 注意：如果 block 仍被外部持有（Arc strong_count > 1），我们不淘汰它
        // 为避免死循环，我们允许扫描有限次数
        let mut scans = 0usize;
        let max_scans = self.map.len().max(8);

        while self.usage > self.capacity && scans < max_scans {
            scans += 1;

            let victim_ptr = match self.lru.back() {
                Some(p) => p,
                None => break,
            };

            // SAFETY: victim_ptr 有效
            let victim = unsafe { victim_ptr.as_ref() };

            // pinned: 外部还持有引用，不淘汰
            if Arc::strong_count(&victim.value) > 1 {
                // 这个对象很热但被 pin 住了；我们把它先移到 front，避免一直卡在尾部
                self.lru.move_to_front(victim_ptr);
                continue;
            }

            let victim_key = victim.key.clone();
            self.erase(&victim_key);
        }
    }
}