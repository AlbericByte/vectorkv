// src/sst/block.rs
use crate::engine::sst::put_varint64;

pub const K_NO_COMPRESSION: u8 = 0;
pub const K_SNAPPY_COMPRESSION: u8 = 1;

pub const BLOCK_TRAILER_SIZE: usize = 5; // 1 byte type + 4 byte crc
pub const FOOTER_SIZE: usize = 48 + 48 + 8; // RocksDB footer layout

pub struct BlockBuilder {
    restart_interval: usize,
    buf: Vec<u8>,
    restarts: Vec<u32>,
    counter: usize,
    last_key: Vec<u8>,
}

impl BlockBuilder {
    pub fn new(restart_interval: usize) -> Self {
        Self {
            restart_interval,
            buf: Vec::new(),
            restarts: vec![0],
            counter: 0,
            last_key: Vec::new(),
        }
    }

    pub fn counter(&self) -> usize {
        self.counter
    }

    pub fn is_empty(&self) -> bool {
        self.buf.is_empty()
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        let mut shared = 0usize;
        if self.counter < self.restart_interval {
            let min_len = self.last_key.len().min(key.len());
            while shared < min_len && self.last_key[shared] == key[shared] {
                shared += 1;
            }
        } else {
            self.restarts.push(self.buf.len() as u32);
            self.counter = 0;
            shared = 0;
        }

        let non_shared = key.len() - shared;
        // 这里为了省事用 varint64（实际 rocksdb 用 varint32）
        put_varint64(&mut self.buf, shared as u64);
        put_varint64(&mut self.buf, non_shared as u64);
        put_varint64(&mut self.buf, value.len() as u64);

        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    pub fn finish(&mut self) -> Vec<u8> {
        // append restarts
        for &r in &self.restarts {
            self.buf.extend_from_slice(&(r as u32).to_le_bytes());
        }
        self.buf.extend_from_slice(&(self.restarts.len() as u32).to_le_bytes());
        std::mem::take(&mut self.buf)
    }

    pub fn reset(&mut self) {
        self.buf.clear();
        self.restarts.clear();
        self.restarts.push(0);
        self.counter = 0;
        self.last_key.clear();
    }

    pub fn current_size_estimate(&self) -> usize {
        self.buf.len() + self.restarts.len() * 4
    }

    pub fn encoded_block_size(&self) -> usize {
        let body = self.buf.len();
        let restarts = self.restarts.len() * 4;
        let footer = 4 + 4; // restart_count(u32) + first_restart(u32)
        body + restarts + footer
    }
}
