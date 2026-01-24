use std::sync::{Arc, Weak, Mutex};
use std::collections::VecDeque;
use crate::{DBImpl, DB};
use crate::engine::mem::{ColumnFamilyId, MemTable};


pub trait Command: Send + 'static {
    fn execute(&self);
}

pub struct FlushMemTableCommand {
    db: Weak<DBImpl>,
    memtables: VecDeque<Arc<dyn MemTable>>,
}

impl FlushMemTableCommand {
    pub fn new(db: &Arc<DBImpl>, memtables: VecDeque<Arc<dyn MemTable>>) -> Self {
        Self {
            db: Arc::downgrade(db),
            memtables,
        }
    }
}

impl Command for FlushMemTableCommand {
    fn execute(&self) {
        if let Some(db) = self.db.upgrade() {
            for mem in &self.memtables {
                if let Err(e) = db.flush_memtable(Arc::clone(mem)) {
                    eprintln!("Flush error: {:?}", e);
                }
            }
        }
    }
}

pub struct CompactionCommand {
    db: Weak<DBImpl>,
    cf: ColumnFamilyId,
    begin: Option<Vec<u8>>,
    end: Option<Vec<u8>>,
}

impl Command for CompactionCommand {
    fn execute(&self) {
        if let Some(db) = self.db.upgrade() {
            // 调用 DBImpl 的 compaction 内部方法
            let _ = db.run_compaction(self.cf, self.begin.as_deref(), self.end.as_deref());
        }
    }
}

