// src/sst/table_reader.rs
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::path::Path;

use crate::engine::sst::format::{Footer, BlockHandle, BLOCK_TRAILER_SIZE};

pub struct TableReader {
    file: File,
    index_block: Vec<u8>, // 最小版：直接把 index block 整块读入内存
}

impl TableReader {
    pub fn open(path: &Path) -> io::Result<Self> {
        let mut file = File::open(path)?;
        let file_len = file.metadata()?.len();
        if file_len < Footer::ENCODED_LEN as u64 {
            return Err(io::Error::new(io::ErrorKind::InvalidData, "sst too small"));
        }

        // read footer
        file.seek(SeekFrom::End(-(Footer::ENCODED_LEN as i64)))?;
        let mut footer_buf = vec![0u8; Footer::ENCODED_LEN];
        file.read_exact(&mut footer_buf)?;
        let footer = Footer::decode(&footer_buf)
            .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidData, "bad footer"))?;

        // read index block
        let index_block = read_block(&mut file, footer.index_handle)?;

        Ok(Self { file, index_block })
    }

    pub fn get(&mut self, key: &[u8]) -> io::Result<Option<Vec<u8>>> {
        // 1) 在 index_block 里定位 data block handle（TODO：实现 index seek）
        let data_handle = match seek_index(&self.index_block, key) {
            None => return Ok(None),
            Some(h) => h,
        };

        // 2) 读 data block
        let data_block = read_block(&mut self.file, data_handle)?;

        // 3) 在 data_block 里 seek key（TODO：实现 block seek）
        Ok(seek_data_block(&data_block, key))
    }
}

fn read_block(file: &mut File, h: BlockHandle) -> io::Result<Vec<u8>> {
    file.seek(SeekFrom::Start(h.offset))?;
    let mut buf = vec![0u8; h.size as usize + BLOCK_TRAILER_SIZE];
    file.read_exact(&mut buf)?;

    // buf[..h.size] 是 block 内容
    // buf[h.size] 是 compression_type
    // buf[h.size+1..] 是 crc32c（可校验）
    Ok(buf[..h.size as usize].to_vec())
}

// ---- TODO：你需要实现的两个 seek ----

// index entry: key -> encoded BlockHandle bytes
fn seek_index(_index_block: &[u8], _key: &[u8]) -> Option<BlockHandle> {
    // 这里要用 BlockIter 解码 index_block
    // 找到第一个 >= key 的 entry
    // 然后 decode value 里的 BlockHandle
    None
}

fn seek_data_block(_data_block: &[u8], _key: &[u8]) -> Option<Vec<u8>> {
    // 同样用 BlockIter seek 到 key
    // 如果相等返回 value
    None
}
