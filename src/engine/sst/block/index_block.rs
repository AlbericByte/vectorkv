use crate::DBError;
use crate::engine::sst::block::{BlockBuilder, DataBlock};
use crate::engine::sst::format::BlockHandle;
use crate::engine::sst::iterator::DataBlockIter;

/// 读 SST 时的 IndexBlock
pub struct IndexBlock {
    block: DataBlock,
}

impl IndexBlock {
    pub fn from_bytes(bytes: Vec<u8>) -> Result<Self, DBError> {
        Ok(Self {
            block: DataBlock::from_bytes(bytes)?,
        })
    }

    /// 给定 user_key/内部 key，找到对应 DataBlock 的 handle
    ///
    /// 约定：index entry key 是 data block 的 largest_key，
    /// 所以要找 "第一个 >= target_key 的 entry"
    pub fn find_data_block(&self, target_key: &[u8]) -> Result<Option<BlockHandle>, DBError> {
        // 关键：这里不能用 DataBlock::get（等值），
        // 必须做 "lower_bound"（seek 到 >= target_key 的第一条 index entry）。
        //
        // 工业级做法：用 BlockIter.seek，然后读当前 entry。
        //
        // 如果你还没实现 BlockIter.seek，
        // 这里给一个“最小可用”的降级方案：扫描 restart array 做 lower_bound。
        //
        // ✅ 推荐：实现 DataBlock::lower_bound(...)，这里就一行调用。
        //
        let v = self
            .block
            .lower_bound_value(target_key)  // <-- 你需要实现这个（下面给实现）
            .ok_or_else(|| DBError::Corruption("index lower_bound failed".into()))?;

        Ok(Some(BlockHandle::decode_from_bytes(&v)?))
    }

    pub fn raw_block(&self) -> &DataBlock {
        &self.block
    }

    pub fn iter(&self) -> DataBlockIter<'_> {
        DataBlockIter {
            block: &self.block,
            offset: 0,
            key_buf:Vec::new(),
            value_range: 0..0,
            valid: false,
        }
    }
}

/// 写 SST 时构建 index block
pub struct IndexBlockBuilder {
    builder: BlockBuilder,
    last_key: Vec<u8>, // 保证 key 单调递增（可选校验）
}

impl IndexBlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        Self {
            builder: BlockBuilder::new(restart_interval),
            last_key: Vec::new(),
        }
    }

    /// 向 index block 添加：largest_key -> BlockHandle
    pub fn add(&mut self, largest_key_in_data_block: &[u8], handle: BlockHandle) {
        // （可选）校验递增：index keys 必须严格递增
        if !self.last_key.is_empty() && largest_key_in_data_block <= self.last_key.as_slice() {
            // 工业级一般是 debug assert；你也可返回 Result
            // panic!("index key not increasing");
        }
        self.last_key.clear();
        self.last_key.extend_from_slice(largest_key_in_data_block);

        let mut v = Vec::with_capacity(20);
        handle.encode_to(&mut v);
        self.builder.add(largest_key_in_data_block, &v);
    }

    /// 结束 index block 构建，返回 bytes（写入 SST 文件）
    pub fn finish(& mut self) -> Vec<u8> {
        self.builder.finish()
    }

    pub fn is_empty(&self) -> bool {
        self.builder.is_empty()
    }
}