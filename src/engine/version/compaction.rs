use std::cmp::Ordering;
use std::collections::{BTreeMap, BinaryHeap};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use std::thread;
use crate::engine::mem::{InternalKey, ValueType};
use crate::engine::sst::SstReader;
use crate::engine::sst::table_builder::TableBuilder;
use crate::engine::version::version_set::{ColumnFamilyData, VersionBuilder};
use crate::engine::version::{VersionEdit, VersionSet};
use crate::util::{DbConfig, NUM_LEVELS};

pub trait MergeOperator {
    fn merge(&self, key: &[u8], existing: Option<&[u8]>, value: &[u8]) -> Vec<u8>;
}

struct HeapItem<'a> {
    key: InternalKey, // InternalKey 包含 user_key + seq + value_type
    value: Vec<u8>,
    iter_index: usize,
    iter: Box<dyn Iterator<Item = (InternalKey, Vec<u8>)> + 'a>,
}

// PartialEq / Eq
impl<'a> PartialEq for HeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}
impl<'a> Eq for HeapItem<'a> {}

// PartialOrd / Ord
impl<'a> PartialOrd for HeapItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl<'a> Ord for HeapItem<'a> {
    fn cmp(&self, other: &Self) -> Ordering {
        // BinaryHeap 默认是最大堆，如果你希望最小 key 在堆顶，反转 cmp
        other.key.cmp(&self.key)
    }
}
pub struct Compactor {
    db_config: Arc<DbConfig>,
    version_set: Arc<Mutex<VersionSet>>,
    cf: Arc<ColumnFamilyData>,
    merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>,
}

impl Compactor {
    pub fn new(db_config: Arc<DbConfig>, version_set: Arc<Mutex<VersionSet>>, cf: Arc<ColumnFamilyData>, merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>) -> Self {
        Self { db_config, version_set, cf, merge_operator }
    }

    /// 自动触发所有层级 compact（多线程）
    pub fn auto_compact(&self) {
        for level in 0..NUM_LEVELS-1 {
            let cf = Arc::clone(&self.cf);
            let op = self.merge_operator.clone();
            thread::spawn(move || {
                let comp = SingleLevelCompaction::new(self.db_config, self.version_set, cf, op);
                let _ = comp.compact_level(level, None, None);
            });
        }
    }
}

pub struct SingleLevelCompaction {
    db_config: Arc<DbConfig>,
    version_set: Arc<Mutex<VersionSet>>,
    cf: Arc<ColumnFamilyData>,
    merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>,
}

impl SingleLevelCompaction  {
    pub fn new(db_config: Arc<DbConfig>, version_set: Arc<Mutex<VersionSet>>, cf: Arc<ColumnFamilyData>, merge_operator: Option<Arc<dyn MergeOperator + Send + Sync>>) -> Self {
        Self { db_config, version_set, cf, merge_operator }
    }

    pub fn compact_level(&self, level_num: usize, begin: Option<&[u8]>, end: Option<&[u8]>) -> Result<(), String> {
        if level_num >= NUM_LEVELS - 1 {
            return Err("Already top level".into());
        }

        // 1️⃣ 获取当前 Version
        let current_version = self.cf.current.as_ref().clone();

        // 2️⃣ 构造 VersionBuilder
        let mut builder = VersionBuilder::new_from_version(&current_version);

        let level_files = &builder.levels[level_num];

        // 3️⃣ 选择文件
        let files_to_compact: Vec<_> = level_files.iter()
            .filter(|f| (begin.map_or(true, |b| f.largest_key.as_slice() >= b)) &&
                (end.map_or(true, |e| f.smallest_key.as_slice() < e)))
            .cloned()
            .collect();

        if files_to_compact.is_empty() { return Ok(()); }

        // 4️⃣ 打开 reader & iterator
        let mut iters = Vec::new();
        for file in &files_to_compact {
            let reader = SstReader::open(
                file.file_number,
                self.db_config.sst_path(file.file_number),
                self.cf.current.table_cache().block_cache(),
                self.db_config.get_filter_policy(self.cf.cf_type).clone(),
            )?;

            iters.push(reader.iter());
        }

        // 5️⃣ init heap
        let mut heap = BinaryHeap::new();
        for (idx, iter) in iters.iter_mut().enumerate() {
            if let Some(entry) = iter.next() {
                heap.push(HeapItem {
                    key: entry.key,
                    value: entry.value,
                    iter_index: idx,
                    iter: Box::new(iter.by_ref().map(|(k, v)| (k, v))),
                });
            }
        }

        // 6️⃣ 输出新 SST
        let cf_opts = &self.db_config.get_column_family_options(self.cf.cf_type);
        let file_number = {
            let vs = self.version_set.lock().unwrap();
            vs.new_file_number()
        };
        let mut builder = TableBuilder::from_options(file_number, self.new_sst_path(level_num + 1, file_number), cf_opts);

        let mut last_user_key: Option<Vec<u8>> = None;

        while let Some(item) = heap.pop() {
            let HeapItem { key, value, iter_index, mut iter } = item;

            let is_new_key = last_user_key
                .as_ref()
                .map(|k| k != &key.user_key)
                .unwrap_or(true);

            if is_new_key {
                if key.value_type == ValueType::Put {
                    builder.add(&key, &value)?;
                }
                last_user_key = Some(key.user_key.clone());
            }

            iter.next();
            if iter.valid() {
                heap.push(HeapItem {
                    key: InternalKey::decode(iter.key()),
                    value: iter.value().to_vec(),
                    iter_index,
                    iter,
                });
            }
        }

        let new_file = builder.finish()?;

        // 7️⃣ Version edit
        let mut edit = VersionEdit::new(self.cf.cf_id, self.cf.cf_type);
        for f in files_to_compact {
            edit.delete_file(level_num, f.file_number);
        }
        edit.add_file(
            level_num + 1,
            new_file.file_number,
            new_file.file_size,
            new_file.smallest_key.clone(),
            new_file.largest_key.clone(),
        );


        self.version_set.lock().unwrap().log_and_apply(edit)?;

        Ok(())
    }

    pub fn new_sst_path(&self, level: usize, file_number: usize) -> PathBuf {
        let level_dir = self.db_config.sst_dir.join(format!("L{}", level));
        let file_name = format!("{:06}.sst", file_number);
        level_dir.join(file_name)
    }
}
