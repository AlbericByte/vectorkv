use std::sync::Arc;
use crate::engine::mem::{mvcc_comparator, raw_mvcc_compare};
use crate::engine::sst::iterator::{InternalIterator, MergingIterator, TwoLevelIterator, DBIterator, SnapshotIterator};
use crate::engine::sst::{BlockHandle, TableCache};
use crate::engine::version::{FileMetaData, VersionEdit};
use crate::util::NUM_LEVELS;

#[derive(Clone)]
pub struct Version {
    levels: [Vec<Arc<FileMetaData>>; NUM_LEVELS],
    table_cache: Arc<TableCache>,
}

impl Version {
    pub fn new_empty(table_cache: Arc<TableCache>) -> Self {
        Self {
            levels: std::array::from_fn(|_| Vec::new()),
            table_cache,
        }
    }

    /// 根据 VersionEdit 更新自己
    ///
    /// 注意：Version 是不可变语义，一般做法是：
    /// - 先 clone 当前 Version
    /// - 然后在 clone 上调用 apply_edit
    /// - VersionSet 再把 Arc<new_version> 设为 current
    pub fn apply_edit(&mut self, edit: &VersionEdit, _tc: &TableCache) {
        // 1) 删除文件
        for (level, file_number) in &edit.delete_files {
            if *level >= NUM_LEVELS {
                continue;
            }
            let files = &mut self.levels[*level];
            files.retain(|f| f.file_number != *file_number);
        }

        // 2) 增加新文件
        for (level, meta) in &edit.add_files {
            if *level >= NUM_LEVELS {
                continue;
            }
            self.levels[*level].push(Arc::new(meta.clone()));

            // L0 可以重叠，不需要排序
            if *level > 0 {
                self.levels[*level].sort_by(|a, b| a.smallest_key.cmp(&b.smallest_key));
            }
        }
    }

    pub fn get(&self, key: &[u8]) -> Option<Vec<u8>> {
        // ---------- 1️⃣ 查 L0 ----------
        // L0 文件可能重叠，必须按“最新 → 最旧”查
        // 通常 file_number 越大越新
        let l0 = &self.levels[0];

        for f in l0.iter().rev() {
            if f.contains_key(key) {
                if let Some(v) = self.get_from_sst(f, key) {
                    return Some(v);
                }
            }
        }

        // ---------- 2️⃣ 查 L1 ~ Ln ----------
        for level in 1..NUM_LEVELS {
            let files = &self.levels[level];

            // 二分查找定位 candidate SST
            let mut left = 0;
            let mut right = files.len();

            while left < right {
                let mid = (left + right) / 2;
                let f = &files[mid];

                if key < f.smallest_key.as_slice() {
                    right = mid;
                } else if key > f.largest_key.as_slice() {
                    left = mid + 1;
                } else {
                    // 命中区间
                    if let Some(v) = self.get_from_sst(f, key) {
                        return Some(v);
                    } else {
                        return None;
                    }
                }
            }
        }

        None
    }

    /// 为当前 Version 中所有 SST 创建 iterator 列表（内部 iterator）
    ///
    /// 一般用法：
    /// - L0: 对每个文件建一个 iterator
    /// - L1+: 每层文件合并成一个“LevelIterator”（按 key merge）
    /// - 最后再把所有 level iterator 丢给一个 MergingIterator
    ///
    /// 这里先返回“每个文件一个 iterator”，方便你后面自己组合。
    pub fn new_sst_iterators<'a>(
        &'a self,
        table_cache: &'a TableCache,
    ) -> Vec<Box<dyn InternalIterator + 'a>> {
        // ⚠️ 这里签名可以按照你自己的 iterator 体系调整，
        // 我先给一个“思路版”代码：遍历所有文件，拿到 SstReader，再调用 reader.iter()
        //
        // 实际中你可能会用：
        //   type I = Box<dyn InternalIterator + 'a>;
        //   fn new_sst_iterators(&self, tc: &TableCache) -> Vec<I>
        //
        // 下面的代码写成伪实现（需要你根据自己的类型名改一改）：

        let mut iters = Vec::new();

        for level in 0..NUM_LEVELS {
            for f in &self.levels[level] {
                let reader = match table_cache.find_table_by_number(f.file_number){
                    Some(reader) => reader,
                    None => continue,
                };
                // 假设 SstReader::iter() 返回实现了 InternalIterator 的 TwoLevelIterator
                let it = reader.iter();
                iters.push(Box::new(it) as Box<dyn InternalIterator + 'a>);
            }
        }

        iters
    }

    // 如果你已经有 MergingIterator，可以给一个更“高级”的 new_iterator：
    pub fn new_iterator(
        &self,
        snapshot_seq: u64,
    ) -> Box<dyn DBIterator> {
        let internal_iters = self.new_sst_iterators(&self.table_cache);
        let merging =MergingIterator::new(internal_iters, raw_mvcc_compare);
        let snap_iter =Box::new(SnapshotIterator::new(merging, snapshot_seq));
        Box::new(snap_iter)
    }


    fn get_from_sst(
        &self,
        file: &Arc<FileMetaData>,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        let reader = self.table_cache.find_table(file)?;
        reader.get(key).ok()?
    }

    pub fn levels(&self) -> [Vec<Arc<FileMetaData>>; NUM_LEVELS] {
        self.levels.clone()
    }

    pub fn table_cache(&self) -> Arc<TableCache> {
        self.table_cache.clone()
    }
}
