use crate::engine::mem::ColumnFamilyId;

pub const FIRST_MANIFEST: &str = "MANIFEST-000001";
pub const SYSTEM_COLUMN_FAMILY: &str = "cf_system";
pub const USER_COLUMN_FAMILY: &str = "cf_user";
pub const USER_COLUMN_FAMILY_ID: ColumnFamilyId = 0;
pub const SYSTEM_COLUMN_FAMILY_ID: ColumnFamilyId = 1;
pub const NUM_LEVELS: usize = 7;