use config::{Config, File, FileFormat};
use std::path::{Path, PathBuf};
use serde::Deserialize;
use crate::DBError;

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DbConfig {
    // DB level options
    pub create_if_missing: bool,
    pub max_manifest_file_size: u64,

    pub system_cf: ColumnFamilyOptions,
    pub user_cf: ColumnFamilyOptions,

    pub write: WriteOptions,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct DBOptions {
    /// Create the DB if missing on open.
    pub create_if_missing: bool,

    /// Maximum size of the manifest file before rotating.
    pub max_manifest_file_size: u64,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct WriteOptions {
    /// Require strong consistency WAL writes (wait for sync thread to advance).
    pub sync: bool,
}

#[derive(Debug, Clone, Default)]
pub struct ColumnFamilyOptions {
    /// Enable dynamic level-based compaction file growth.
    pub level_compaction_dynamic_size: bool,

    /// Target file size for SST flush.
    pub target_file_size: u64,
}

pub fn load_db_config(db_path: &PathBuf) -> Result<DbConfig, DBError> {
    let mut cfg = Config::builder();

    let yaml = db_path.join("config.yaml");
    if yaml.exists() {
        cfg = cfg.add_source(File::new(yaml.to_str().unwrap(), FileFormat::Yaml));
    } else {
        let json = db_path.join("config.json");
        if json.exists() {
            cfg = cfg.add_source(File::new(json.to_str().unwrap(), FileFormat::Json));
        } else {
            let ini = db_path.join("config.ini");
            if ini.exists() {
                cfg = cfg.add_source(File::new(ini.to_str().unwrap(), FileFormat::Ini));
            }
        }
    }

    let cfg = cfg.build().map_err(|e| DBError::Io(e.to_string()))?;
    cfg.try_deserialize().map_err(|e| DBError::Io(e.to_string()))
}