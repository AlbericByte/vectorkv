use crate::engine::sst::block::{get_varint32, put_varint32};
use crate::engine::sst::iterator::DataBlockIter;
use crate::error::DBError;

pub enum BlockType {
    Data,
    Index,
    Filter,
}

pub trait BlockTrait: Send + Sync {
    fn size(&self) -> usize;
    fn block_type(&self) -> BlockType;
}

pub struct DataBlock {
    pub(crate) data: Vec<u8>,
    pub(crate) restart_offsets: Vec<u32>,
}

impl BlockTrait for DataBlock {
    fn size(&self) -> usize {
        self.data.len()
    }

    fn block_type(&self) -> BlockType {
        BlockType::Data
    }
}

impl DataBlock {
    pub fn from_bytes(mut data: Vec<u8>) -> Result<Self, DBError> {
        if data.len() < 4 {
            return Err(DBError::Corruption("block too small".into()));
        }

        let n = u32::from_le_bytes(data[data.len()-4..].try_into().unwrap()) as usize;
        let restarts_start = data.len() - 4 - n * 4;
        if restarts_start > data.len() {
            return Err(DBError::Corruption("bad restart array".into()));
        }

        let mut restart_offsets = Vec::with_capacity(n);
        for i in 0..n {
            let off = u32::from_le_bytes(
                data[restarts_start + i*4 .. restarts_start + (i+1)*4]
                    .try_into().unwrap()
            );
            restart_offsets.push(off);
        }

        Ok(Self { data, restart_offsets })
    }


    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // 1️⃣ 二分 restart array
        let mut left = 0;
        let mut right = self.restart_offsets.len();

        while left < right {
            let mid = (left + right) / 2;
            let off = self.restart_offsets[mid] as usize;

            let mut cur = off;
            let (_, first_key) = read_entry_key(&self.data, &mut cur).ok()?;

            if first_key.as_slice() < key {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        // 2️⃣ 从 restart 点线性 scan
        if left > 0 {
            left -= 1;
        }
        let mut offset = self.restart_offsets[left] as usize;
        let mut last_key = Vec::new();

        while offset < self.data.len() {
            let (shared, unshared, value_len, key_delta, value) =
                read_entry(&self.data, &mut offset).ok()?;

            last_key.truncate(shared);
            last_key.extend_from_slice(&key_delta);

            match last_key.as_slice().cmp(key) {
                std::cmp::Ordering::Equal => return Some(value),
                std::cmp::Ordering::Greater => return None,
                std::cmp::Ordering::Less => {}
            }
        }
        None
    }

    /// 返回 “第一条 key >= target”的 value（Vec<u8>）
    /// 找不到则返回 None。
    ///
    /// 这是 IndexBlock / MetaIndexBlock 的核心能力。
    pub fn lower_bound_value(&self, target: &[u8]) -> Option<Vec<u8>> {
        // 1) 二分 restart array，找到可能包含 target 的 restart 区间
        let mut left = 0usize;
        let mut right = self.restart_offsets.len();

        while left < right {
            let mid = (left + right) / 2;
            let off = self.restart_offsets[mid] as usize;

            let mut cur = off;
            let (_, first_key) = read_entry_key(&self.data, &mut cur).ok()?;

            if first_key.as_slice() < target {
                left = mid + 1;
            } else {
                right = mid;
            }
        }

        if left > 0 {
            left -= 1;
        }

        // 2) 从该 restart 点线性 scan，找第一条 >= target
        let mut offset = self.restart_offsets[left] as usize;
        let mut last_key = Vec::new();

        while offset < self.data_entries_end() {
            let (shared, _unshared, _vlen, key_delta, value) = read_entry(&self.data, &mut offset).ok()?;
            last_key.truncate(shared);
            last_key.extend_from_slice(&key_delta);

            match last_key.as_slice().cmp(target) {
                std::cmp::Ordering::Less => continue,
                std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => return Some(value),
            }
        }

        None
    }

    #[inline]
    fn data_entries_end(&self) -> usize {
        // entries 的结束位置 = restart array 开始位置
        let n = self.restart_offsets.len();
        self.data.len() - 4 - n * 4
    }

    pub fn iter(&self) -> DataBlockIter<'_> {
        DataBlockIter {
            block: self,
            offset: 0,
            key_buf:Vec::new(),
            value_range: 0..0,
            valid: false,
        }
    }
}

fn read_entry(
    data: &[u8],
    pos: &mut usize,
) -> Result<(usize, usize, usize, Vec<u8>, Vec<u8>), DBError> {
    let shared = get_varint32(data, pos) as usize;
    let unshared = get_varint32(data, pos) as usize;
    let value_len = get_varint32(data, pos) as usize;

    let key_delta = data[*pos .. *pos + unshared].to_vec();
    *pos += unshared;

    let value = data[*pos .. *pos + value_len].to_vec();
    *pos += value_len;

    Ok((shared, unshared, value_len, key_delta, value))
}

fn read_entry_key(data: &[u8], pos: &mut usize) -> Result<(usize, Vec<u8>), DBError> {
    let shared = get_varint32(data, pos) as usize;
    let unshared = get_varint32(data, pos) as usize;
    let _value_len = get_varint32(data, pos) as usize;

    let key = data[*pos .. *pos + unshared].to_vec();
    *pos += unshared;

    Ok((shared, key))
}


/// restart 间隔
pub const DEFAULT_RESTART_INTERVAL: usize = 16;

/// 用于写 SST datablock
pub struct DataBlockBuilder {
    buf: Vec<u8>,
    restarts: Vec<u32>,
    last_key: Vec<u8>,
    counter: usize,
    restart_interval: usize,
    finished: bool,
}

impl DataBlockBuilder {
    pub fn new() -> Self {
        Self {
            buf: Vec::new(),
            restarts: vec![0],               // 第一条 restart = offset 0
            last_key: Vec::new(),
            counter: 0,
            restart_interval: DEFAULT_RESTART_INTERVAL,
            finished: false,
        }
    }

    /// append key-value，保证 key 按字典序递增
    pub fn add(&mut self, key: &[u8], value: &[u8]) {
        assert!(!self.finished);

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

        put_varint32(&mut self.buf, shared as u32);
        put_varint32(&mut self.buf, non_shared as u32);
        put_varint32(&mut self.buf, value.len() as u32);

        self.buf.extend_from_slice(&key[shared..]);
        self.buf.extend_from_slice(value);

        self.last_key.clear();
        self.last_key.extend_from_slice(key);
        self.counter += 1;
    }

    /// 输出 block bytes
    pub fn finish(mut self) -> Vec<u8> {
        assert!(!self.finished);

        for &r in &self.restarts {
            self.buf.extend_from_slice(&r.to_le_bytes());
        }

        let num = self.restarts.len() as u32;
        self.buf.extend_from_slice(&num.to_le_bytes());

        self.finished = true;
        self.buf
    }

    pub fn current_size(&self) -> usize {
        self.buf.len()
    }
}
