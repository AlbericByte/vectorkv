use crate::engine::sst::iterator::InternalIterator;

/// TwoLevelIterator：
///   外层 index_iter：指向某个 data block 的 index entry
///   内层 data_iter：当前 data block 内的迭代
pub struct TwoLevelIterator<'a, F> {
    index_iter: Box<dyn InternalIterator + 'a>,
    data_iter: Option<Box<dyn InternalIterator + 'a>>,
    /// 由 index value -> data block iterator 的工厂函数
    ///
    /// 比如：value 是 BlockHandle 编码，factory 负责 decode + 读 block + 构造 DataBlockIter。
    block_reader: F,
    valid: bool,
}

impl<'a, F> TwoLevelIterator<'a, F>
where
    F: Fn(&[u8]) -> Box<dyn InternalIterator + 'a>,
{
    pub fn new(
        index_iter: Box<dyn InternalIterator + 'a>,
        block_reader: F,
    ) -> Self {
        Self {
            index_iter,
            data_iter: None,
            block_reader,
            valid: false,
        }
    }

    /// 确保当前 data_iter 指向 index_iter 当前 entry 对应的 block
    fn init_data_block(&mut self) {
        if !self.index_iter.valid() {
            self.data_iter = None;
            self.valid = false;
            return;
        }

        let v = self.index_iter.value(); // 一般是 BlockHandle 编码
        let mut it = (self.block_reader)(v);
        it.seek_to_first();
        if it.valid() {
            self.data_iter = Some(it);
            self.valid = true;
        } else {
            // 当前 block 没数据，尝试下一个 index entry
            self.data_iter = None;
            self.valid = false;
        }
    }

    /// 前进到下一个非空的 data block 的第一条
    fn skip_empty_data_blocks(&mut self) {
        loop {
            match self.data_iter.as_mut() {
                Some(di) if di.valid() => {
                    self.valid = true;
                    return;
                }
                _ => {
                    self.index_iter.next();
                    if !self.index_iter.valid() {
                        self.data_iter = None;
                        self.valid = false;
                        return;
                    }
                    self.init_data_block();
                    if self.valid {
                        return;
                    }
                }
            }
        }
    }
}

impl<'a, F> InternalIterator for TwoLevelIterator<'a, F>
where
    F: Fn(&[u8]) -> Box<dyn InternalIterator + 'a>,
{
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.index_iter.seek_to_first();
        if !self.index_iter.valid() {
            self.data_iter = None;
            self.valid = false;
            return;
        }
        self.init_data_block();
        if !self.valid {
            self.skip_empty_data_blocks();
        }
    }

    fn seek(&mut self, target: &[u8]) {
        // 粗略实现：直接在所有 block 上 binary seek：
        // 更优的是先在 index 上 seek，找包含 target 的 block，再 data 上 seek。
        // 这里给一个典型模式：index 按 key 上界，先 seek index，再构 block，再 seek data。
        self.index_iter.seek(target);
        if !self.index_iter.valid() {
            self.data_iter = None;
            self.valid = false;
            return;
        }
        self.init_data_block();
        if !self.valid {
            self.skip_empty_data_blocks();
            return;
        }

        // data block 里进一步 seek
        if let Some(di) = self.data_iter.as_mut() {
            di.seek(target);
            if !di.valid() {
                self.skip_empty_data_blocks();
            } else {
                self.valid = true;
            }
        }
    }

    fn next(&mut self) {
        if !self.valid {
            return;
        }
        if let Some(di) = self.data_iter.as_mut() {
            di.next();
        }
        if self.data_iter.as_ref().map_or(true, |di| !di.valid()) {
            self.skip_empty_data_blocks();
        }
    }

    fn key(&self) -> &[u8] {
        self.data_iter.as_ref().unwrap().key()
    }

    fn value(&self) -> &[u8] {
        self.data_iter.as_ref().unwrap().value()
    }
}
