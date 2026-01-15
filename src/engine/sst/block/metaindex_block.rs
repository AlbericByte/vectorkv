use crate::error::DBError;
use crate::engine::sst::block::{BlockBuilder, DataBlock, FilterPolicy};
use crate::engine::sst::BlockHandle;
use crate::engine::sst::iterator::{DataBlockIter, InternalIterator};

/// 读 SST 时的 metaindex block
pub struct MetaIndexBlock {
    block: DataBlock,
}

impl MetaIndexBlock {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, DBError> {
        Ok(Self {
            block: DataBlock::from_bytes(bytes)?,
        })
    }

    /// 查找 block handle：name 精确匹配（其实用 lower_bound + 比较即可）
    pub fn find(&self, name: &str) -> Result<Option<BlockHandle>, DBError> {
        let target = name.as_bytes();

        let v = match self.block.lower_bound_value(target) {
            Some(v) => v,
            None => return Ok(None),
        };

        // lower_bound 返回的是第一条 >= target
        // 需要确认 key == target 才算命中
        // 工业级做法：lower_bound 返回 (key,value)，这里为了简化，只用 value，
        // 所以建议把 lower_bound 扩展成返回 key+value。
        //
        // 这里给“最低改动”的方案：你把 DataBlock 再加一个方法 lower_bound_kv() 更好。
        //
        // 临时方案：直接用 DataBlock::get 等值查找（如果你实现了 get）
        // 更严谨：实现 lower_bound_kv。
        //
        // ✅ 推荐：用 get(name) 来保证等值命中。
        if let Some(exact) = self.block.get(target) {
            return Ok(Some(BlockHandle::decode_from_bytes(&exact)?));
        }

        Ok(None)
    }

    /// 约定：找 filter.<name>
    pub fn find_filter(&self, filter_name: &str) -> Result<Option<BlockHandle>, DBError> {
        let key = format!("filter.{}", filter_name);
        self.find(&key)
    }

    pub fn raw_block(&self) -> &DataBlock {
        &self.block
    }

    pub fn get_filter_handle(
        &self,
        policy: &dyn FilterPolicy,
    ) -> Result<Option<BlockHandle>, DBError> {
        // RocksDB 约定 filter block key =  "filter.<policy-name>"
        let key_str = format!("filter.{}", policy.name());
        let target = key_str.as_bytes();

        let mut iter = DataBlockIter::new(self.block);
        iter.seek(target);

        if iter.valid() && iter.key() == target {
            // value bytes = encoded BlockHandle
            let mut pos = 0usize;
            let h = BlockHandle::decode_from(iter.value(), &mut pos)
                .ok_or_else(|| DBError::Corruption("bad filter block handle".into()))?;

            return Ok(Some(h));
        }

        Ok(None)
    }

}

/// 写 SST 时构建 metaindex block
pub struct MetaIndexBlockBuilder {
    builder: BlockBuilder,
    last_key: Vec<u8>,
}

impl MetaIndexBlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        Self {
            builder: BlockBuilder::new(restart_interval),
            last_key: Vec::new(),
        }
    }

    /// 通用添加：name -> BlockHandle
    pub fn add(&mut self, name: &str, handle: BlockHandle) {
        let k = name.as_bytes();

        if !self.last_key.is_empty() && k <= self.last_key.as_slice() {
            // debug assert/panic 或改成 Result
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(k);

        let mut v = Vec::with_capacity(20);
        handle.encode_to(&mut v);
        self.builder.add(k, &v);
    }

    /// 约定项：filter block
    pub fn add_filter_block(&mut self, filter_name: &str, handle: BlockHandle) {
        // RocksDB 通常是 "filter.<policy_name>"
        let key = format!("filter.{}", filter_name);
        self.add(&key, handle);
    }

    /// 约定项：properties
    pub fn add_properties_block(&mut self, handle: BlockHandle) {
        self.add("properties", handle);
    }

    pub fn finish(&mut self) -> Vec<u8> {
        self.builder.finish()
    }

    pub fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }

    pub fn reset(&mut self) {
        self.builder.reset();
    }
}
