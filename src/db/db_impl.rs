use std::fs::File;
use std::io::BufWriter;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};
use crate::db::db_iterator::DBIterator;
use crate::db::db_trait::DB;
use crate::engine::background::BackgroundWorker;
use crate::engine::mem::{ColumnFamilyId, MemTable};
use crate::engine::mem::MemTableSet;
use crate::engine::sst::TableCache;
use crate::engine::version::VersionSet;
use crate::engine::wal::WalManager;
use crate::engine::wal::write_batch::WriteBatch;
use crate::engine::sst::block::BlockCache;
use crate::engine::sst::table_builder::TableBuilder;
use crate::error::DBError;
use crate::util::{load_db_config, DbConfig, DbConfigFile, OpenOptions, Options};

pub struct DBImpl {
    name: String,
    options: Arc<Options>,
    db_config: Arc<DbConfig>,

    memtables: Arc<Mutex<MemTableSet>>,
    wal_manager: Arc<WalManager>,
    version_set: Arc<Mutex<VersionSet>>,
    bg_worker: Arc<BackgroundWorker>,
    table_cache: Arc<TableCache>,
}

#[derive(Clone)]
pub struct Snapshot {
    pub seq: u64,
}

impl DB for DBImpl {
    fn put(&self, cf: ColumnFamilyId, key: &[u8], value: &[u8]) -> Result<(),DBError> {
        let mut batch = WriteBatch::new();
        batch.put(cf, key, value);
        self.write(batch)
    }

    fn delete(&self, cf: ColumnFamilyId, key: &[u8]) -> Result<(),DBError> {
        let mut batch = WriteBatch::new();
        batch.delete(cf, key);
        self.write(batch)
    }

    fn write(&self, batch: WriteBatch) -> Result<(),DBError> {
        // 1. 写前限流
        self.make_room_for_write(&batch)?;

        let mut vs = self.version_set.lock().unwrap();
        let base_seq = vs.allocate_sequence(batch.entries.len() as u64);
        drop(vs);

        // 2. 写 WAL
        if self.options.enable_write_ahead_log {
            self.wal_manager.append_sync(base_seq, &batch)?;
        } else {
            self.wal_manager.append_sync(base_seq, &batch)?;
        }

        // 3. 写入 MemTableSet
        let mut mem = self.memtables.lock().unwrap();
        mem.apply(base_seq, batch)?;

        Ok(())
    }

    fn get(&self, cf: ColumnFamilyId, key: &[u8]) -> Result<Option<Vec<u8>>,DBError> {
        let mem =self.memtables.lock().unwrap();
        let seq = self.version_set.lock().unwrap().current_sequence();
        // 现在只查 MemTableSet，它内部会依次查 active → immutables
        if let Some(v) = mem.get(cf, seq, key) {
            return Ok(Some(v));
        }

        self.version_set.lock().unwrap().get(cf, key)
    }

    fn flush(&self, cf: ColumnFamilyId) -> Result<(),DBError> {
        let mut mem = self.memtables.lock().unwrap();
        let seq = self.version_set.lock().unwrap().next_sequence();
        // freeze 返回的是 Arc<MemTable>
        let imm = mem.freeze_active(cf, seq)?;

        // 交给后台 flush
        self.bg_worker.schedule_flush(imm);

        Ok(())
    }

    fn new_iterator(&self, cf: ColumnFamilyId) -> Box<dyn DBIterator> {
        self.version_set.lock().unwrap().new_iterator(cf)
    }

    fn compact_range(
        &self,
        cf: ColumnFamilyId,
        begin: Option<&[u8]>,
        end: Option<&[u8]>,
    ) -> Result<()> {
        self.bg_worker.schedule_compaction(cf, begin, end)
    }

    fn get_snapshot(&self) -> Snapshot {
        Snapshot {
            seq: self.version_set.lock().unwrap().latest_sequence(),
        }
    }

    fn release_snapshot(&self, _snapshot: Snapshot) {
        // Rust 自动 drop，无需人工干预
    }

    fn flush_memtable(&self, mem: Arc<dyn MemTable>) -> Result<(),DBError> {
        // 1️⃣ 创建 SST 文件
        let cf = mem.cf_id();
        let mut vs = self.version_set.lock().unwrap();
        let file_number = vs.new_file_number();
        let file_path = self.db_config.sst_path(file_number);
        let file = File::create(&file_path)?;
        let cfd = vs.column_family_by_id(cf)
            .ok_or_else(|| DBError::InvalidColumnFamily(format!("CF id {} not found", cf)))?;
        let cf_options = cfd.options(&self.options);


        // 2️⃣ TableBuilder
        let mut builder = TableBuilder::from_options(
            file_number,
            BufWriter::new(file),
            &cf_options,
        );

        // 3️⃣ 遍历 memtable
        for (key, value) in mem.iter() {
            builder.add(key, value);
        }

        // 4️⃣ finish -> 写 footer
        builder.finish()?;

        // 5️⃣ 安装到 VersionSet (LSM)
        vs.install_table(
            cf,
            cfd.cf_type,
            file_number,
            &file_path,
            mem.smallest_key(),
            mem.largest_key(),
        )?;

        Ok(())
    }
}

impl DBImpl {
    pub fn open(path: &str) -> Result<Arc<Self>, DBError> {
        let db_path = PathBuf::from(path);

        // =========================================================
        // 0️⃣ Build OpenOptions (Default + config file)
        // =========================================================

        let open_opts = match load_db_config(&db_path) {
            Ok(file_cfg) => file_cfg.to_open_options(),
            Err(_) => OpenOptions::default(),
        };

        // =========================================================
        // 1️⃣ Derive DbConfig (disk layout facts)
        // =========================================================

        let db_config = Arc::new(
            DbConfig::from_open_options(db_path.clone(), &open_opts)
        );

        // Create required directories
        db_config.create_dirs()?;

        // Check whether DB creation is allowed
        if !db_config.looks_like_existing_db()
            && !open_opts.create_if_missing
        {
            return Err(DBError::Io("DB does not exist".into()));
        }

        // =========================================================
        // 2️⃣ Derive runtime Options
        // =========================================================

        let options = Arc::new(open_opts.to_options());

        // =========================================================
        // 3️⃣ Initialize BlockCache (open-only resource)
        // =========================================================

        let cache_capacity = open_opts
            .block_cache_capacity
            .unwrap_or(options.block_cache_size); // or a DEFAULT value

        let cache_shards = open_opts
            .block_cache_shards
            .unwrap_or(16); // a safe recommended default

        let block_cache = Arc::new(
            BlockCache::new(cache_capacity, cache_shards)
        );

        // =========================================================
        // 4️⃣ Initialize filter policy (optional)
        // =========================================================

        let filter_policy = None;
        // let filter_policy = Some(Arc::new(BloomPolicy::new(10)));

        // =========================================================
        // 5️⃣ Initialize TableCache (using DbConfig)
        // =========================================================

        let table_cache = Arc::new(
            TableCache::new(
                &db_config.sst_dir,      // ✅ no longer use db_path directly
                block_cache.clone(),
                filter_policy.clone(),
            )
        );

        // =========================================================
        // 6️⃣ Load VersionSet (replay MANIFEST)
        // =========================================================

        let versions = VersionSet::load(
            &db_config,
            table_cache.clone(),
        )?;

        // =========================================================
        // 7️⃣ Initialize WAL (using DbConfig)
        // =========================================================

        let wal = WalManager::open(
            &db_config.wal_dir,
        )?;

        // =========================================================
        // 8️⃣ Initialize MemTableSet
        // =========================================================

        let memtables = MemTableSet::new(
            versions.current_sequence(),
            versions.column_families().as_slice(),
        );

        // =========================================================
        // 9️⃣ Construct DBImpl
        // =========================================================

        let db = Arc::new(Self {
            name: path.to_string(),

            // Two core components
            options,
            db_config,

            // Existing components
            table_cache,
            version_set: Arc::new(Mutex::new(versions)),
            memtables: Arc::new(Mutex::new(memtables)),
            wal_manager: wal,
            bg_worker: Arc::new(BackgroundWorker::new()),
        });

        // =========================================================
        // 🔟 WAL replay / crash recovery
        // =========================================================

        db.recover()?;

        Ok(db)
    }



    fn recover(&self) -> Result<(),DBError> {
        self.wal_manager.replay_batches(|base_seq, batch| {
            self.memtables.lock().unwrap().apply(base_seq, batch)
        })?;
        Ok(())
    }

    fn make_room_for_write(&self, batch: &WriteBatch) -> Result<(),DBError> {
        const MEMTABLE_MAX_BYTES: usize = 64 * 1024 * 1024;
        const MAX_IMMUTABLES: usize = 4;

        let mut mem = self.memtables.lock().unwrap();

        for cf in batch.involved_cfs() {
            let cf_tables = mem
                .cfs
                .get_mut(&cf)
                .ok_or(DBError::InvalidArgument("unknown CF".into()))?;

            if cf_tables.active_memory_usage() >= MEMTABLE_MAX_BYTES {
                let new_seq = self.version_set.lock().unwrap().next_sequence();
                cf_tables.freeze_active(cf, new_seq);

                if let Some(imm) = cf_tables.pick_flush_candidate() {
                    self.bg_worker.schedule_flush(cf, imm);
                }
            }
        }

        Ok(())
    }
}
