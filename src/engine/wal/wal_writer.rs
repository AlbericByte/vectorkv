use std::io::{self, Write};
use crate::engine::wal::format::{BLOCK_SIZE, HEADER_SIZE, RecordType, record_crc32c};

pub struct WalWriter<W: Write> {
    w: W,
    block_offset: usize,
}

impl<W: Write> WalWriter<W> {
    pub fn new(w: W) -> Self {
        Self { w, block_offset: 0 }
    }

    pub fn into_inner(self) -> W { self.w }

    /// append 一条“逻辑 record”（可能会被拆成多个 fragment 写入多个 block）
    pub fn append(&mut self, payload: &[u8]) -> io::Result<()> {
        let mut left = payload;
        let mut first = true;

        while !left.is_empty() {
            // block 剩余空间
            let avail = BLOCK_SIZE - self.block_offset;

            // 如果连 header 都放不下，则 padding 到下一个 block
            if avail < HEADER_SIZE {
                self.pad_to_block_end(avail)?;
            }

            let avail_payload = BLOCK_SIZE - self.block_offset - HEADER_SIZE;
            let frag_len = avail_payload.min(left.len());

            let typ = match (first, frag_len == left.len()) {
                (true, true) => RecordType::Full,
                (true, false) => RecordType::First,
                (false, true) => RecordType::Last,
                (false, false) => RecordType::Middle,
            };

            self.write_fragment(typ, &left[..frag_len])?;

            left = &left[frag_len..];
            first = false;
        }

        Ok(())
    }

    fn pad_to_block_end(&mut self, bytes: usize) -> io::Result<()> {
        if bytes > 0 {
            // 这里 pad 0 是 LevelDB/RocksDB 兼容做法
            self.w.write_all(&vec![0u8; bytes])?;
        }
        self.block_offset = 0;
        Ok(())
    }

    fn write_fragment(&mut self, typ: RecordType, frag: &[u8]) -> io::Result<()> {
        let crc = record_crc32c(typ, frag);
        let len = frag.len() as u16;

        // header: crc32c, len, type
        self.w.write_all(&crc.to_le_bytes())?;
        self.w.write_all(&len.to_le_bytes())?;
        self.w.write_all(&[typ as u8])?;

        // payload
        self.w.write_all(frag)?;

        self.block_offset += HEADER_SIZE + frag.len();
        Ok(())
    }

    pub fn flush(&mut self) -> io::Result<()> {
        self.w.flush()
    }
}
