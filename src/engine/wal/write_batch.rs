use crate::engine::mem::ColumnFamilyId;

#[derive(Debug)]
pub enum WriteBatchEntry {
    Put {
        cf: u32,
        key: Vec<u8>,
        value: Vec<u8>,
    },
    Delete {
        cf: u32,
        key: Vec<u8>,
    },
}

#[derive(Debug, Default)]
pub struct WriteBatch {
    pub entries: Vec<WriteBatchEntry>,
    pub involved_cfs: Vec<ColumnFamilyId>,
}

impl WriteBatch {
    pub fn new() -> Self {
        Self { entries: Vec::new(),
            involved_cfs: Vec::new(),
        }
    }

    pub fn put(&mut self, cf: u32, key: &[u8], value: &[u8]) {
        if !self.involved_cfs.contains(&cf) {
            self.involved_cfs.push(cf);
        }
        self.entries.push(WriteBatchEntry::Put {
            cf,
            key: key.to_vec(),
            value: value.to_vec(),
        });
    }

    pub fn delete(&mut self, cf: u32, key: &[u8]) {
        if !self.involved_cfs.contains(&cf) {
            self.involved_cfs.push(cf);
        }
        self.entries.push(WriteBatchEntry::Delete {
            cf,
            key: key.to_vec(),
        });
    }

    pub fn is_empty(&self) -> bool {
        self.entries.is_empty()
    }

    pub fn len(&self) -> usize {
        self.entries.len()
    }

    pub fn involved_cfs(&self) -> &[ColumnFamilyId] {
        &self.involved_cfs
    }
}
