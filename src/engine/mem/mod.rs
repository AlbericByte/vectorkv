pub type SequenceNumber = u64;
pub type ColumnFamilyId = u32;

pub mod skiplist;
pub mod storage;
pub mod memtable_set;
pub mod memtable;
#[cfg(test)]
pub mod skiplist_test;


pub use memtable::{mvcc_comparator,raw_mvcc_compare,MemTable,SkipListMemTable,ValueType,InternalKey};
pub use memtable_set::{MemTableSet};
pub use storage::Storage;
