use crate::db::db_iterator::DBIterator;
use crate::db::snapshot::Snapshot;
use crate::DBError;
use crate::engine::mem::ColumnFamilyId;
use crate::engine::wal::write_batch::WriteBatch;

pub trait DB: Send + Sync {
    fn put(&self, cf: ColumnFamilyId, key: &[u8], value: &[u8]) -> Result<(),DBError>;

    fn delete(&self, cf: ColumnFamilyId, key: &[u8]) -> Result<(),DBError>;

    fn write(&self, batch: WriteBatch) -> Result<(),DBError>;

    fn get(&self, cf: ColumnFamilyId, key: &[u8]) -> Result<Option<Vec<u8>>,DBError>;

    fn new_iterator(&self, cf: ColumnFamilyId) -> Box<dyn DBIterator>;

    fn flush(&self, cf: ColumnFamilyId) -> Result<(),DBError>;

    fn compact_range(
        &self,
        cf: ColumnFamilyId,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<(),DBError>;

    fn get_snapshot(&self) -> Snapshot;

    fn release_snapshot(&self, snapshot: Snapshot);
}
