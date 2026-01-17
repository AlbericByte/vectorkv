use std::{fs, io};
use std::io::{Read, Write};
use config::{Config, File, FileFormat};
use std::path::{PathBuf};
use serde::Deserialize;
use crate::DBError;
use crate::util::options::{OpenOptions, OptionsFile};

#[derive(Debug, Deserialize, Default)]
pub struct DbConfigFile {
    // open 行为
    pub create_if_missing: Option<bool>,

    // 路径
    pub wal_dir: Option<PathBuf>,
    pub sst_dir: Option<PathBuf>,
    pub manifest_dir: Option<PathBuf>,

    // Options 覆盖
    pub options: Option<OptionsFile>,

    // CF 覆盖
    pub system_cf: Option<ColumnFamilyOptions>,
    pub user_cf: Option<ColumnFamilyOptions>,

    // 写策略
    pub write: Option<WriteOptions>,
}


#[derive(Debug, Clone)]
pub struct DbConfig {
    /// DB 根目录
    pub db_path: PathBuf,

    /// WAL 文件目录
    pub wal_dir: PathBuf,

    /// SST 文件目录
    pub sst_dir: PathBuf,

    /// Manifest 文件目录
    pub manifest_dir: PathBuf,
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

pub fn load_db_config(db_path: &PathBuf) -> Result<DbConfigFile, DBError> {
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

    let cfg = cfg.build().map_err(|e| DBError::Config(e))?;
    cfg.try_deserialize().map_err(|e| DBError::Config(e))
}

impl DbConfigFile {
    pub fn to_open_options(self) -> OpenOptions {
        let mut open = OpenOptions::default();

        if let Some(v) = self.create_if_missing {
            open.create_if_missing = v;
        }

        open.wal_dir = self.wal_dir;
        open.sst_dir = self.sst_dir;
        open.manifest_dir = self.manifest_dir;

        if let Some(w) = self.write {
            open.write = w;
        }

        if let Some(opts) = self.options {
            let o = &mut open.options;

            macro_rules! apply {
                ($f:ident) => {
                    if let Some(v) = opts.$f {
                        o.$f = v;
                    }
                };
            }

            apply!(write_buffer_size);
            apply!(max_write_buffer_number);
            apply!(allow_concurrent_memtable_write);
            apply!(level0_file_num_compaction_trigger);
            apply!(max_background_compactions);
            apply!(max_background_flushes);
            apply!(compression);
            apply!(block_cache_size);
            apply!(optimize_filters_for_hits);
            apply!(enable_write_ahead_log);
            apply!(max_open_files);
            apply!(max_manifest_file_size);
        }

        if let Some(cf) = self.system_cf {
            open.options.system_cf = cf;
        }
        if let Some(cf) = self.user_cf {
            open.options.user_cf = cf;
        }

        open
    }
}

impl DbConfig {
    pub fn from_open_options(
        db_path: PathBuf,
        open: &OpenOptions,
    ) -> Self {
        let wal_dir = open
            .wal_dir
            .clone()
            .unwrap_or_else(|| db_path.join("wal"));

        let sst_dir = open
            .sst_dir
            .clone()
            .unwrap_or_else(|| db_path.join("sst"));

        let manifest_dir = open
            .manifest_dir
            .clone()
            .unwrap_or_else(|| db_path.join("manifest"));

        Self {
            db_path,
            wal_dir,
            sst_dir,
            manifest_dir,
        }
    }

    pub fn create_dirs(&self) -> Result<(), DBError> {
        fs::create_dir_all(&self.db_path)?;
        fs::create_dir_all(&self.wal_dir)?;
        fs::create_dir_all(&self.sst_dir)?;
        fs::create_dir_all(&self.manifest_dir)?;
        Ok(())
    }

    pub fn wal_path(&self, log_number: u64) -> PathBuf {
        self.wal_dir.join(format!("{:06}.log", log_number))
    }

    pub fn sst_path(&self, file_number: u64) -> PathBuf {
        self.sst_dir.join(format!("{:06}.sst", file_number))
    }

    pub fn manifest_path(&self, manifest_number: u64) -> PathBuf {
        self.manifest_dir
            .join(format!("MANIFEST-{:06}", manifest_number))
    }

    pub fn current_path(&self) -> PathBuf {
        self.db_path.join("CURRENT")
    }

    pub fn write_current(&self, manifest_number: u64) -> io::Result<()> {
        let current = self.current_path();
        let tmp = self.db_path.join("CURRENT.tmp");

        let manifest_name = format!("MANIFEST-{:06}", manifest_number);

        {
            let mut f = fs::File::create(&tmp)?;
            f.write_all(manifest_name.as_bytes())?;
            f.write_all(b"\n")?;
            f.sync_all()?;
        }

        fs::rename(tmp, current)?;
        Ok(())
    }

    pub fn read_current_manifest(&self) -> io::Result<PathBuf> {
        let mut s = String::new();
        let mut f = fs::File::open(self.current_path())?;
        f.read_to_string(&mut s)?;

        let name = s.trim();
        Ok(self.manifest_dir.join(name))
    }

    pub fn looks_like_existing_db(&self) -> bool {
        self.current_path().exists()
            && self.manifest_dir.exists()
    }
}



