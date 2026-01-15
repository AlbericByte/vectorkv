use crate::engine::mem::ColumnFamilyId;

pub const FIRST_MANIFEST: &str = "MANIFEST-000001";
pub const SYSTEM_COLUMN_FAMILY: &str = "cf_system";
pub const USER_COLUMN_FAMILY: &str = "cf_user";
pub const USER_COLUMN_FAMILY_ID: ColumnFamilyId = 0;
pub const SYSTEM_COLUMN_FAMILY_ID: ColumnFamilyId = 1;
pub const NUM_LEVELS: usize = 7;
pub const MIN_BLOCK_SIZE: usize = 1024;
pub const BLOCK_TRAILER_SIZE: usize = 5;
pub const NO_COMPRESSION: u8 = 0;
// RocksDB/LevelDB magic（不同实现可能不同；你可以先用固定 magic）
// 这里用 LevelDB 的 classic magic 示例；你也可以换成 RocksDB 的。
pub const TABLE_MAGIC: u64 = 0xdb4775248b80fb57;