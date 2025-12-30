use std::io::{self, Read};
use crate::error::DBError;
use crate::engine::wal::format::{BLOCK_SIZE, HEADER_SIZE, RecordType, record_crc32c};


pub type WalReadResult<T> = std::result::Result<T, DBError>;

pub struct WalReader<R: Read> {
    r: R,
    block: [u8; BLOCK_SIZE],
    block_len: usize,
    block_pos: usize,

    assembling: Vec<u8>,
    assembling_active: bool,
}

impl<R: Read> WalReader<R> {
    pub fn new(r: R) -> Self {
        Self {
            r,
            block: [0u8; BLOCK_SIZE],
            block_len: 0,
            block_pos: 0,
            assembling: Vec::new(),
            assembling_active: false,
        }
    }

    /// 读取下一条完整 record 的 payload（已拼接 FIRST/MIDDLE/LAST）
    pub fn next_record(&mut self) -> WalReadResult<Option<Vec<u8>>> {
        loop {
            if self.block_pos >= self.block_len {
                if !self.read_next_block()? {
                    // EOF：如果还在 assembling，按 corruption 处理或忽略（这里选择报错）
                    if self.assembling_active {
                        return Err(DBError::Corruption("EOF in fragmented record".into()));
                    }
                    return Ok(None);
                }
            }

            // 如果剩余不足 header，跳到下个 block
            if self.block_len - self.block_pos < HEADER_SIZE {
                self.block_pos = self.block_len;
                continue;
            }

            let hdr = &self.block[self.block_pos..self.block_pos + HEADER_SIZE];
            let crc = u32::from_le_bytes([hdr[0], hdr[1], hdr[2], hdr[3]]);
            let len = u16::from_le_bytes([hdr[4], hdr[5]]) as usize;
            let typ_u8 = hdr[6];

            // padding 区域可能是 0：len=0,type=0（LevelDB 里可能出现）
            if crc == 0 && len == 0 && typ_u8 == 0 {
                // 认为剩下都是 padding，跳到 block end
                self.block_pos = self.block_len;
                continue;
            }

            let Some(typ) = RecordType::from_u8(typ_u8) else {
                // 坏 type：跳过当前 block（更稳）
                self.skip_rest_of_block();
                self.reset_assembling();
                continue;
            };

            // payload 是否完整在 block 中
            let payload_start = self.block_pos + HEADER_SIZE;
            let payload_end = payload_start + len;
            if payload_end > self.block_len {
                // 截断：跳过当前 block
                self.skip_rest_of_block();
                self.reset_assembling();
                continue;
            }

            let frag = &self.block[payload_start..payload_end];

            // CRC 校验
            if record_crc32c(typ, frag) != crc {
                self.skip_rest_of_block();
                self.reset_assembling();
                continue;
            }

            // 消费该 fragment
            self.block_pos = payload_end;

            match typ {
                RecordType::Full => {
                    self.reset_assembling();
                    return Ok(Some(frag.to_vec()));
                }
                RecordType::First => {
                    self.assembling.clear();
                    self.assembling.extend_from_slice(frag);
                    self.assembling_active = true;
                }
                RecordType::Middle => {
                    if !self.assembling_active {
                        // 中间段但没开始：当 corruption 处理
                        self.skip_rest_of_block();
                        continue;
                    }
                    self.assembling.extend_from_slice(frag);
                }
                RecordType::Last => {
                    if !self.assembling_active {
                        self.skip_rest_of_block();
                        continue;
                    }
                    self.assembling.extend_from_slice(frag);
                    self.assembling_active = false;
                    let out = std::mem::take(&mut self.assembling);
                    return Ok(Some(out));
                }
            }
        }
    }

    fn read_next_block(&mut self) -> WalReadResult<bool> {
        self.block_pos = 0;
        self.block_len = 0;

        // 尝试读满一个 block；最后一个 block 可能不足
        let mut off = 0;
        while off < BLOCK_SIZE {
            let n = self.r.read(&mut self.block[off..])
                .map_err(|_| DBError::Corruption("read error".to_string()))?;
            if n == 0 { break; }
            off += n;
            // 小优化：如果底层是文件，read 往往一次就能读很多；不强求读满
            if off > 0 && off < BLOCK_SIZE { break; }
        }

        self.block_len = off;
        Ok(self.block_len > 0)
    }

    fn skip_rest_of_block(&mut self) {
        self.block_pos = self.block_len;
    }

    fn reset_assembling(&mut self) {
        self.assembling.clear();
        self.assembling_active = false;
    }
}
