use std::cmp::Ordering;
use crate::engine::sst::block::{get_varint32, DataBlock};
use crate::engine::sst::iterator::InternalIterator;

/// DataBlock 内部迭代器（prefix 解码 + 顺序/seek）
pub struct DataBlockIter<'a> {
    pub(crate) block: &'a DataBlock,
    /// 当前 entry 在 data 中的偏移
    pub(crate) offset: usize,
    /// 当前完整 key
    pub(crate) key_buf: Vec<u8>,
    /// 当前 value 在 data 中的切片范围
    pub(crate) value_range: std::ops::Range<usize>,
    /// 是否有效
    pub(crate) valid: bool,
}

impl<'a> DataBlockIter<'a> {
    pub fn new(block: &'a DataBlock) -> Self {
        let mut it = Self {
            block,
            offset: 0,
            key_buf: Vec::new(),
            value_range: 0..0,
            valid: false,
        };
        it
    }

    /// 解析当前 offset 对应的 entry，更新 key_buf / value_range
    fn parse_current(&mut self) {
        let data = &self.block.data;
        let mut pos = self.offset;
        if pos >= data.len() {
            self.valid = false;
            return;
        }

        let shared = match get_varint32(data, &mut pos) {
            Some(v) => v as usize,
            None => {
                self.valid = false;
                return;
            }
        };
        let non_shared = match get_varint32(data, &mut pos) {
            Some(v) => v as usize,
            None => {
                self.valid = false;
                return;
            }
        };
        let vlen = match get_varint32(data, &mut pos) {
            Some(v) => v as usize,
            None => {
                self.valid = false;
                return;
            }
        };

        if pos + non_shared + vlen > data.len() {
            self.valid = false;
            return;
        }

        // key = key_prefix(shared) + key_suffix
        self.key_buf.truncate(shared);
        self.key_buf
            .extend_from_slice(&data[pos..pos + non_shared]);
        pos += non_shared;

        let vstart = pos;
        let vend = vstart + vlen;
        self.value_range = vstart..vend;
        self.offset = vend;
        self.valid = true;
    }

    /// 只在从某个 restart offset 开始 scan 时用
    fn seek_to_restart_point(&mut self, restart_idx: usize) {
        assert!(restart_idx < self.block.restart_offsets.len());
        self.offset = self.block.restart_offsets[restart_idx] as usize;
        self.key_buf.clear();
        self.value_range = 0..0;
        self.valid = false;
        self.parse_current();
    }

    /// 二分 search restart array，找到包含 target 的 restart 区间
    fn find_restart_point(&self, target: &[u8]) -> usize {
        let restarts = &self.block.restart_offsets;
        let data = &self.block.data;

        let mut left = 0usize;
        let mut right = restarts.len();

        while left + 1 < right {
            let mid = (left + right) / 2;
            let mut pos = restarts[mid] as usize;

            // restart 开始的 entry 总是 shared=0
            let shared = get_varint32(data, &mut pos);
            assert_eq!(shared, 0);
            let non_shared = get_varint32(data, &mut pos)as usize;

            if pos + non_shared > data.len() {
                break;
            }
            let key = &data[pos..pos + non_shared];

            match key.cmp(target) {
                Ordering::Less => left = mid,
                Ordering::Equal | Ordering::Greater => right = mid,
            }
        }

        left
    }
}

impl<'a> InternalIterator for DataBlockIter<'a> {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.offset = 0;
        self.key_buf.clear();
        self.value_range = 0..0;
        self.valid = false;
        self.parse_current();
    }

    fn seek(&mut self, target: &[u8]) {
        if self.block.data.is_empty() {
            self.valid = false;
            return;
        }

        let r = self.find_restart_point(target);
        self.seek_to_restart_point(r);

        // 从 restart 线性 scan，找 >= target 的第一条
        while self.valid() {
            match self.key().cmp(target) {
                Ordering::Less => self.next(),
                Ordering::Equal | Ordering::Greater => break,
            }
        }
    }

    fn next(&mut self) {
        if !self.valid {
            return;
        }
        self.parse_current();
    }

    fn key(&self) -> &[u8] {
        &self.key_buf
    }

    fn value(&self) -> &[u8] {
        let data = &self.block.data;
        &data[self.value_range.clone()]
    }
}