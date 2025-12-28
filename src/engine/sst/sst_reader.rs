// sst/table.rs
use std::fs::File;
use std::io::{BufReader, Read, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use crate::error::DBError;
use crate::engine::sst::format::{Footer, BlockHandle};
use crate::engine::sst::block::{DataBlock, FilterBlock, FilterPolicy, IndexBlock, MetaIndexBlock, BLOCK_TRAILER_SIZE};
use crate::engine::sst::block::{BlockCache, BlockCacheKey};
use crate::engine::sst::iterator::{InternalIterator, TwoLevelIterator};

pub struct SstReader {
    file_number: u64,
    path: PathBuf,

    // 常驻
    index_block: Arc<IndexBlock>,      // 简化：用 DataBlock 表示 index（你也可以单独 IndexBlock）
    filter_block: Option<Arc<FilterBlock>>,
    filter_policy: Option<Arc<dyn FilterPolicy>>,

    // 共享 cache
    block_cache: Arc<BlockCache<DataBlock>>,
}

impl SstReader {
    pub fn open(
        file_number: u64,
        path: PathBuf,
        block_cache: Arc<BlockCache<DataBlock>>,
        filter_policy: Option<Arc<dyn FilterPolicy>>,
    ) -> Result<Self, DBError> {
        let mut f = BufReader::new(File::open(&path).map_err(DBError::Io)?);
        let file_len = f.get_ref().metadata().map_err(DBError::Io)?.len();
        let footer = Footer::read_from_file(&mut f, file_len)?;

        // 1) 读 index block
        let index_bytes = read_block_raw(&mut f, footer.index_handle)?;
        // TODO: 做 decode_block + CRC + 解压，这里先假设 DataBlock::from_bytes 里已经处理了
        let index_block = Arc::new(IndexBlock::from_bytes(index_bytes)?);

        // 2) 读 metaindex block → 找 filter block handle → 再读 filter block
        let mut filter_block: Option<Arc<FilterBlock>> = None;

        if let Some(policy) = &filter_policy {
            // 2.1 先读 metaindex block
            let meta_bytes_raw = read_block_raw(&mut f, footer.metaindex_handle)?;
            let meta_block = MetaIndexBlock::from_bytes(meta_bytes_raw)?;

            // 2.2 从 metaindex 找 filter block handle
            if let Some(filter_handle) =
                MetaIndexBlock::get_filter_handle(&meta_block, policy.as_ref())?
            {
                // 2.3 读 filter block
                let filter_bytes_raw = read_block_raw(&mut f, filter_handle)?;
                let fb = FilterBlock::from_bytes(filter_bytes_raw);
                filter_block = Some(Arc::new(fb?));
            }
        }

        Ok(Self {
            file_number,
            path,
            index_block,
            filter_block,
            filter_policy,
            block_cache,
        })
    }

    /// 点查：index → data block → entry
    pub fn get(&self, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        // 0) 可选 bloom：先用 index 找到 data block offset，再查 filter
        let (data_handle, data_block_offset) = self.find_data_block(key)?;

        if let (Some(fb), Some(policy)) = (&self.filter_block, &self.filter_policy) {
            if let Some(filter) = fb.filter_for_data_block(data_block_offset) {
                if !policy.may_match(key, filter) {
                    return Ok(None);
                }
            }
        }

        let block = self.read_data_block_cached(data_handle)?;
        Ok(block.get(key))
    }

    /// 迭代器：TwoLevel（index iter → data iter）
    pub fn iter<'a>(self: &Arc<Self>)
                -> TwoLevelIterator<'a, impl Fn(BlockHandle) -> Box<dyn InternalIterator + 'a>+'a> {
        let index_iter = self.index_block.iter();
        let reader = Arc::clone(self);
        TwoLevelIterator::new(
            Box::new(index_iter),
            move |h|{
                Box::new(reader.read_data_block_cached(h).iter())
            },
        )
    }

    fn find_data_block(&self, key: &[u8]) -> Result<(BlockHandle, u64), DBError> {
        let handle_opt = self.index_block.find_data_block(key)?;

        // If found, return the BlockHandle and use its offset as the sequence/snapshot marker
        if let Some(h) = handle_opt {
            return Ok((h.clone(), h.offset));
        }

        // Key not found in index is treated as an error for this API
        Err(DBError::NotFound(format!(
            "Data block not found for key {:?}",
            String::from_utf8_lossy(key)
        )))
    }

    fn read_data_block_cached(&self, h: BlockHandle) -> Result<Arc<DataBlock>, DBError> {
        let k = BlockCacheKey { file_number: self.file_number, block_offset: h.offset };
        if let Some(b) = self.block_cache.get(&k) {
            return Ok(b);
        }

        let mut f = BufReader::new(File::open(&self.path).map_err(DBError::Io)?);
        let bytes = read_block_raw(&mut f, h)?;
        let b = Arc::new(DataBlock::from_bytes(bytes)?);

        // 估算 charge（工业级：用 bytes.len() + overhead）
        self.block_cache.insert(k, Arc::clone(&b), 0);
        Ok(b)
    }
}

pub fn read_block_raw<R: Read + Seek>(
    r: &mut R,
    h: BlockHandle,
) -> Result<Vec<u8>, DBError> {

    let block_size = h.size as usize + BLOCK_TRAILER_SIZE;

    let mut buf = vec![0u8; block_size];
    // TODO: 校验 crc / 解压缩
    // seek to offset
    r.seek(SeekFrom::Start(h.offset))
        .map_err(|e| DBError::Io(e))?;

    r.read_exact(&mut buf)
        .map_err(|e| DBError::Io(e))?;

    Ok(buf)
}
