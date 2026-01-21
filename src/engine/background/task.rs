use std::sync::{Arc, Weak, Mutex};
use std::collections::VecDeque;
use crate::{DBImpl, DB};
use crate::engine::mem::MemTable;


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

