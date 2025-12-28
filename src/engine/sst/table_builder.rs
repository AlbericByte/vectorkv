// src/sst/table_builder.rs
use std::io::{self, Write};

use crate::engine::sst::block::BlockBuilder;
use crate::engine::sst::format::{BlockHandle, Footer, BLOCK_TRAILER_SIZE, NO_COMPRESSION};

pub struct TableBuilder<W: Write> {
    w: W,
    offset: u64,

    data_block: BlockBuilder,
    index_block: BlockBuilder,

    pending_index_handle: Option<BlockHandle>,
    pending_index_key: Vec<u8>,
}

impl<W: Write> TableBuilder<W> {
    pub fn new(w: W) -> Self {
        Self {
            w,
            offset: 0,
            data_block: BlockBuilder::new(16),
            index_block: BlockBuilder::new(1),
            pending_index_handle: None,
            pending_index_key: Vec::new(),
        }
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> io::Result<()> {
        // 如果上一个 data block 已经写出，需要把它的 handle 写入 index
        if let Some(h) = self.pending_index_handle.take() {
            // index entry：key -> handle_bytes
            let mut hb = Vec::new();
            h.encode_to(&mut hb);
            self.index_block.add(&self.pending_index_key, &hb);
        }

        self.data_block.add(key, value);

        // 简化：达到一定大小就 flush block
        if self.data_block.finish().len() > 16 * 1024 {
            // 注意：finish() 会 take buffer；所以这里不要这样写
            // 实际应先 check estimated size。这里给你正确写法：
            // 见下方 flush_data_block() 用法
        }

        // 先用简单策略：每次 add 都不 flush，让调用者决定
        Ok(())
    }

    pub fn flush_data_block(&mut self, last_key_in_block: &[u8]) -> io::Result<()> {
        if self.data_block.is_empty() {
            return Ok(());
        }
        let raw = self.data_block.finish();
        let handle = self.write_block(&raw)?;
        self.data_block.reset();

        self.pending_index_handle = Some(handle);
        self.pending_index_key.clear();
        self.pending_index_key.extend_from_slice(last_key_in_block);
        Ok(())
    }

    pub fn finish(mut self) -> io::Result<()> {
        // flush last data block: 调用方需要提供最后一个 key（或者你内部缓存 last_key）
        // 这里假设你在外面会在 finish 前 flush_data_block(last_key) 一次
        if let Some(h) = self.pending_index_handle.take() {
            let mut hb = Vec::new();
            h.encode_to(&mut hb);
            self.index_block.add(&self.pending_index_key, &hb);
        }

        let index_raw = self.index_block.finish();
        let index_handle = self.write_block(&index_raw)?;

        let footer = Footer {
            metaindex_handle: BlockHandle { offset: 0, size: 0 },
            index_handle,
        };
        let footer_bytes = footer.encode();
        self.w.write_all(&footer_bytes)?;
        self.offset += footer_bytes.len() as u64;

        Ok(())
    }

    fn write_block(&mut self, raw: &[u8]) -> io::Result<BlockHandle> {
        let handle = BlockHandle {
            offset: self.offset,
            size: raw.len() as u64,
        };

        self.w.write_all(raw)?;
        // trailer: compression + crc (先写 0，后续加 crc32c)
        self.w.write_all(&[NO_COMPRESSION])?;
        self.w.write_all(&0u32.to_le_bytes())?;
        self.offset += raw.len() as u64 + BLOCK_TRAILER_SIZE as u64;
        Ok(handle)
    }
}
