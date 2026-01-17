use std::collections::{HashMap, VecDeque};
use std::sync::{Arc, Mutex};
use crate::engine::mem::ColumnFamilyId;
use crate::error::DBError;
use crate::engine::mem::{MemTable, SkipListMemTable, ValueType};
use crate::engine::mem::SequenceNumber;
use crate::engine::wal::write_batch::{WriteBatch, WriteBatchEntry};

/// 等价于 RocksDB 的 MemTableList / MemTableSet
struct CfMemTables {
    /// 当前可写的 memtable
    active: Arc<dyn MemTable>,

    /// 不再写入，等待 flush 的 memtable
    immutables: VecDeque<Arc<dyn MemTable>>,

    /// 正在 flush 到 SST 的 memtable（后台线程使用）
    flushing: Vec<Arc<dyn MemTable>>,
}

pub struct MemTableSet {
    pub(crate) cfs: HashMap<ColumnFamilyId, CfMemTables>,
}

impl MemTableSet {
    /// 创建一个新的 MemTableSet（DB 启动时）
    pub fn new(seq: u64, cfs: &[ColumnFamilyId]) -> Self {

        let mut map = HashMap::new();
        for cf in cfs{
            let active = Arc::new(SkipListMemTable::new(seq));
            map.insert(
                *cf,
                CfMemTables {
                    active,
                    immutables: VecDeque::new(),
                    flushing: Vec::new(),
                }
            );
        }
        Self {
            cfs: map
        }
    }

    // ========== 写入路径 ==========

    pub fn apply(&self, base_seq: SequenceNumber, batch: WriteBatch) -> Result<(), DBError> {
        let mut seq = base_seq;

        for entry in batch.entries {
            match entry {
                WriteBatchEntry::Put { cf, key, value } => {
                    self.insert(cf, seq, &key, &value, ValueType::Put)?;
                }

                WriteBatchEntry::Delete { cf, key } => {
                    // Delete = value_type=Delete, value=null
                    self.insert(cf, seq, &key, &[], ValueType::Delete)?;
                }
            }
            seq += 1;
        }
        Ok(())
    }

    /// 向当前活跃 memtable 写入
    pub fn insert(
        &self,
        cf: ColumnFamilyId,
        seq: SequenceNumber,
        key: &[u8],
        value: &[u8],
        value_type: ValueType,
    ) -> Result<(), DBError> {
        let cf_tables = self.cfs.get(&cf)
            .ok_or(DBError::UnknownColumnFamily(format!(
                "Unknown column family id: {}",
                cf)))?;
        cf_tables.active.insert(seq, key, value, value_type)
    }

    /// 冻结当前 memtable（切换 active → immutable）
    pub fn freeze_active(&mut self, cf: ColumnFamilyId, new_seq: SequenceNumber) -> Result<VecDeque<Arc<dyn MemTable>>, DBError>{
        let cf_tables = self.cfs.get_mut(&cf)
            .ok_or(DBError::UnknownColumnFamily(format!(
                "Unknown column family id: {}",
                cf)))?;
        let old = std::mem::replace(
            &mut cf_tables.active,
            Arc::new(SkipListMemTable::new(new_seq)),
        );
        cf_tables.immutables.push_back(old);
        Ok(cf_tables.immutables)
    }

    // ========== 读取路径 ==========

    /// 按最新版本查询（active → immutables 逆序）
    pub fn get(
        &self,
        cf: ColumnFamilyId,
        seq: SequenceNumber,
        key: &[u8],
    ) -> Option<Vec<u8>> {
        let cf_tables = self.cfs.get(&cf)?;
        if let Some(v) = cf_tables.active.get(seq, key) {
            return Some(v);
        }

        for table in cf_tables.immutables.iter().rev() {
            if let Some(v) = table.get(seq, key) {
                return Some(v);
            }
        }
        None
    }

    // ========== flush 相关 ==========

    /// 取出一个 immutable 交给后台 flush
    pub fn pick_flush_candidate(&mut self, cf: ColumnFamilyId) -> Option<Arc<dyn MemTable>> {
        let Some(cf_tables) = self.cfs.get_mut(&cf)?;
        if let Some(t) = cf_tables.immutables.pop_front() {
            cf_tables.flushing.push(t.clone());
            Some(t)
        } else {
            None
        }
    }

    /// flush 完成后回收
    pub fn finish_flush(&mut self, cf: ColumnFamilyId, table: &Arc<dyn MemTable>) {
        if let Some(cf_tables) = self.cfs.get_mut(&cf) {
            cf_tables.flushing.retain(|x| !Arc::ptr_eq(x, table));
        }
    }

    // ========== 状态辅助 ==========

    pub fn num_immutables(&self, cf: ColumnFamilyId) -> usize {
        self.cfs.get(&cf)
            .map(|cf_tables| cf_tables.immutables.len())
            .unwrap_or(0)
    }

    pub fn has_flush_candidate(&self, cf: ColumnFamilyId) -> bool {
        self.cfs.get(&cf)
            .map(|cf_tables| !cf_tables.immutables.is_empty())
            .unwrap_or(false)
    }
}
