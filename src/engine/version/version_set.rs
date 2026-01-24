use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::sync::atomic::{AtomicU64, Ordering};
use std::thread;
use crate::DBError;
use crate::engine::mem::{ColumnFamilyId, InternalKey};
use crate::engine::mem::memtable_set::CfType;
use crate::engine::sst::iterator::{DBIterator, EmptyIterator};
use crate::engine::sst::{SstReader, TableCache};
use crate::engine::version::{read_current, FileMetaData, ManifestReader, ManifestWriter, Version, VersionEdit};
use crate::engine::version::compaction::{Compactor, SingleLevelCompaction};
use crate::util::{ColumnFamilyOptions, DbConfig, Options, FIRST_MANIFEST, NUM_LEVELS, SYSTEM_COLUMN_FAMILY, USER_COLUMN_FAMILY};
use crate::util::constants::{SYSTEM_COLUMN_FAMILY_ID, USER_COLUMN_FAMILY_ID};

pub struct VersionSet {
    db_config: Arc<DbConfig>,
    /// Current versions of all column families
    cf_map: HashMap<u32, Arc<ColumnFamilyData>>,

    /// Next available SST file number
    next_file_number: AtomicU64,

    /// Global maximum sequence number,
    current_sequence: AtomicU64,

    /// used for MVCC snapshots and WAL replay
    last_sequence: AtomicU64,

    /// MANIFEST log writer
    manifest: Arc<Mutex<ManifestWriter>>,

    /// Table cache for SSTables
    pub table_cache: Arc<TableCache>,
}

pub struct ColumnFamilyData {
    pub cf_id: ColumnFamilyId,
    pub cf_type: CfType,
    pub name: String,
    pub current: Arc<Version>,
    pub builder: VersionBuilder,
}

impl ColumnFamilyData {
    fn options(&self, global_options: &Options) -> &ColumnFamilyOptions {
        match self.cf_type {
            CfType::User => &global_options.user_cf,
            CfType::System => &global_options.system_cf,
        }
    }
}

#[derive(Clone)]
pub struct VersionBuilder {
    pub levels: [Vec<Arc<FileMetaData>>; NUM_LEVELS],
    pub added_files: Vec<FileMetaData>,
    pub deleted_files: Vec<u64>,
    pub table_cache: Arc<TableCache>,
}

impl VersionSet {
    pub fn load(
        db_config: &DbConfig,
        table_cache: Arc<TableCache>,
    ) -> Result<Self, DBError> {
        // Path to the `CURRENT` pointer file
        let manifest_file:Option<String> = read_current(&db_config.db_path)
            .ok()
            .and_then(|s| {
                let t = s.trim();
                if t.is_empty() {None} else {Some(t.to_string())}
            });

        let mut cf_map: HashMap<u32, Arc<ColumnFamilyData>> = HashMap::new();
        let mut last_sequence = 0u64;
        let mut next_file_number = 1u64;

        // If no valid manifest pointer is found, treat this as the first startup
        if manifest_file.is_none() {
            let manifest_name = FIRST_MANIFEST;
            let manifest_path = db_config
                .manifest_dir
                .join(manifest_name);

            // 创建 manifest
            let manifest = ManifestWriter::create_new(&manifest_path)?;

            // build system column family
            let system_cf = Arc::new(ColumnFamilyData {
                cf_id: USER_COLUMN_FAMILY_ID,
                cf_type: CfType::System,
                name: SYSTEM_COLUMN_FAMILY.to_string(),
                current: Arc::new(Version::new_empty(Arc::clone(&table_cache))),
                builder: VersionBuilder::new_from_version(&Version::new_empty(Arc::clone(&table_cache))),
            });
            cf_map.insert(USER_COLUMN_FAMILY_ID, Arc::clone(&system_cf));

            // build system column family
            let user_cf = Arc::new(ColumnFamilyData {
                cf_id: SYSTEM_COLUMN_FAMILY_ID,
                cf_type: CfType::User,
                name: USER_COLUMN_FAMILY.to_string(),
                current: Arc::new(Version::new_empty(Arc::clone(&table_cache))),
                builder: VersionBuilder::new_from_version(&Version::new_empty(Arc::clone(&table_cache))),
            });
            cf_map.insert(SYSTEM_COLUMN_FAMILY_ID, Arc::clone(&user_cf));

            return Ok(Self {
                db_config: Arc::new(db_config.clone()),
                cf_map,
                next_file_number: AtomicU64::new(1),
                current_sequence: AtomicU64::new(0),
                last_sequence: AtomicU64::new(0),
                manifest: Arc::new(Mutex::new(manifest)),
                table_cache,
            });
        }

        // Non-first startup: replay the manifest to rebuild CF versions and sequence/file numbers
        let manifest_name = manifest_file.unwrap();
        let manifest_path = db_config.manifest_dir.join(manifest_name);
        let mut manifest = ManifestReader::open(manifest_path)?;



        manifest.replay(|edit| {

            let cf_id = edit.cf_id;

            if edit.is_cf_add {
                cf_map.entry(cf_id).or_insert_with(|| {
                    Arc::new(ColumnFamilyData {
                        cf_id,
                        cf_type: edit.cf_type,
                        name: edit.cf_name.clone().unwrap_or_else(|| format!("cf_{}", cf_id)),
                        current: Arc::new(Version::new_empty(Arc::clone(&table_cache))),
                        builder: VersionBuilder::new_from_version(&Version::new_empty(Arc::clone(&table_cache))),
                    })
                });
            }

            if edit.is_cf_drop {
                cf_map.remove(&cf_id);
            }

            let cfd = cf_map
                .get_mut(&cf_id)
                .ok_or(DBError::UnknownColumnFamily(cf_id.to_string()))?;
            let mut ver = (*cfd.current).clone();
            ver.apply_edit(&edit, &table_cache);
            Arc::get_mut(cfd).unwrap().current = Arc::new(ver);
            Arc::get_mut(cfd).unwrap().builder = VersionBuilder::new_from_version(&cfd.current);

            last_sequence =
                last_sequence.max(edit.last_sequence.unwrap_or(last_sequence));

            next_file_number =
                next_file_number.max(edit.next_file_number.unwrap_or(next_file_number));

            Ok(())
        })?;

        // Switch to writer phase (write)
        let writer = ManifestWriter::open_existing(manifest_path.to_str().unwrap())?;

        Ok(Self {
            db_config: Arc::new(db_config.clone()),
            cf_map,
            next_file_number: AtomicU64::new(next_file_number),
            current_sequence: AtomicU64::new(0),
            last_sequence: AtomicU64::new(last_sequence),
            manifest: Arc::new(Mutex::new(writer)),
            table_cache,
        })
    }


    /// Allocate a new SST file number.
    /// This method does not clone any data; it simply increments the internal counter.
    pub fn new_file_number(&self) -> u64 {
        self.next_file_number.fetch_add(1, Ordering::Relaxed) + 1
    }

    /// Return the next global monotonically increasing sequence number.
    /// This does NOT persist anything to disk; persistence is handled via `VersionEdit` in MANIFEST.
    #[inline]
    pub fn next_sequence(&self) -> u64 {
        self.current_sequence.fetch_add(1, Ordering::Relaxed) +1
    }

    #[inline]
    pub fn current_sequence(&self) -> u64 {
        self.current_sequence.load(Ordering::Relaxed)
    }

    #[inline]
    pub fn latest_sst_snapshot(&self) -> u64 {
        self.last_sequence.load(Ordering::Acquire)
    }

    /// Allocate a sequence number for a write batch.
    /// Returns the first sequence of the batch, and advances the global sequence counter
    /// by `batch_size` entries.
    pub fn allocate_sequence(&mut self, batch_size: u64) -> u64 {
        self.current_sequence.fetch_add(batch_size, Ordering::Relaxed) + batch_size
    }

    /// Log the version edit to the manifest file and apply it to the in-memory Version.
    /// This is called during runtime when flush, compaction, or other metadata changes occur.
    pub fn log_and_apply(&mut self, edit: VersionEdit) -> Result<(), DBError> {
        // Persist metadata to manifest (append-only)
        {
            let mut mf = self.manifest.lock().unwrap();
            mf.add_record(&edit)?;
        }

        // Apply the edit to the corresponding column family version in memory
        if let Some(cf) = self.cf_map.get(&edit.cf_id) {
            let mut new_version = cf.current.as_ref().clone();
            new_version.apply_edit(&edit, &self.table_cache);

            let cf_data = Arc::new(ColumnFamilyData {
                cf_id: edit.cf_id,
                cf_type: edit.cf_type,
                name: cf.name.clone(),
                current: Arc::new(new_version),
                builder: cf.builder.clone(),
            });

            self.cf_map.insert(edit.cf_id, Arc::clone(&cf_data));
        }

        // Update global sequence and file number trackers
        self.last_sequence.fetch_max(
            edit.last_sequence.unwrap_or(self.last_sequence.load(Ordering::SeqCst)),
            Ordering::SeqCst);
        self.next_file_number.fetch_max(
            edit.next_file_number.unwrap_or(self.next_file_number.load(Ordering::SeqCst)),
            Ordering::SeqCst);

        Ok(())
    }

    /// Get a value by key from the current column family Version.
    /// Errors are converted into `DBError` without crashing the program.
    pub fn get(&self, cf_id: ColumnFamilyId, key: &[u8]) -> Result<Option<Vec<u8>>, DBError> {
        let cf = self.cf_map.get(&cf_id)
            .ok_or(DBError::NotFound(format!("column family {} not found", cf_id)))?;
        match cf.current.get(key) {
            Ok(Some(v)) => Ok(Some(v)),
            Ok(None) => Ok(None),
            Err(e) => Err(DBError::InvalidColumnFamily(format!(
                                               "Get operation failed on CF {}, key {}, error: {}",
                                               cf_id,
                                               String::from_utf8_lossy(key), // convert &[u8] to readable text
                                               e
            ))),
        }
    }

    /// Create a new iterator for a given column family snapshot.
    /// Uses `Arc::clone` to efficiently share ownership without deep copying.
    pub fn new_iterator(&self, cf_id: u32) -> Box<dyn DBIterator> {
        if let Some(cf) = self.cf_map.get(&cf_id) {
            cf.current.new_iterator(self.latest_sst_snapshot())
        } else {
            Box::new(EmptyIterator {})
        }
    }

    /// Return the current Version of a column family.
    /// This is an O(1) pointer clone (reference count increment), no data copy.
    pub fn current_version(&self, cf_id: u32) -> Arc<Version> {
        self.cf_map
            .get(&cf_id)
            .map(|cf| Arc::clone(&cf.current))
            .unwrap_or_else(|| Arc::new(Version::new_empty(Arc::clone(&self.table_cache))))
    }


    pub fn column_families(&self) -> Vec<ColumnFamilyId>  {
        self.cf_map.values().map(|cf| cf.cf_id.clone()).collect()
    }

    pub fn column_family_by_id(&self, cf_id: ColumnFamilyId) -> Result<&ColumnFamilyData, DBError> {
        self.cf_map
            .get(&cf_id)
            .map(|arc| arc.as_ref())
            .ok_or_else(|| DBError::InvalidColumnFamily(format!("CF id {} not found", cf_id)))
    }

    pub fn install_table(
        &mut self,
        cf: ColumnFamilyId,
        cf_type: CfType,
        file_number: u64,
        file_path: &Path,
        smallest: &[u8],
        largest: &[u8],
    ) -> Result<(), DBError> {
        // 1️⃣ 构造 VersionEdit
        let mut edit = VersionEdit::new(cf, cf_type);
        let metadata = std::fs::metadata(file_path)?;
        let file_size = metadata.len();

        edit.add_file(
            0,              // flush → L0
            file_number,
            file_size,
            smallest,
            largest,
        );

        // 2️⃣（可选）预热 table cache
        let table = SstReader::open(file_number,
                        file_path.to_path_buf(),
                        self.table_cache.block_cache(),
                        self.table_cache.filter_policy())?;
        self.table_cache.insert(file_number, Arc::new(table));

        // 3️⃣ 写 MANIFEST + 安装新 Versio n
        self.log_and_apply(edit)?;

        Ok(())
    }

    pub fn auto_compact(self: &Arc<Mutex<Self>>) {
        let vs = self.lock().unwrap();
        let cf_map =vs.cf_map.clone();
        let db_config = vs.db_config.clone();
        for cf in cf_map.values() {
            let cf_clone = Arc::clone(cf);
            let vs_arc_mutex = Arc::new(Mutex::new(Arc::clone(self)));
            thread::spawn(move || {
                let compactor = Compactor::new(
                    db_config,
                    Arc::clone(self),
                    cf_clone,
                    None);
                compactor.auto_compact();
            });
        }
    }

    pub fn compact_level(&self, cf_id: u32, level: usize) -> Result<(), String> {
        let cf = self.cf_map.get(&cf_id).ok_or("Unknown CF")?;
        let compactor = Compactor::new(Arc::clone(cf), None);
        compactor.compact_level(level, None, None)
    }
}

impl VersionBuilder {
    pub fn new_from_version(version: &Version) -> Self {
        let mut levels: [Vec<Arc<FileMetaData>>; NUM_LEVELS] = Default::default();

        for (i, level_files) in version.levels().iter().enumerate() {
            levels[i] = level_files.clone();
        }

        VersionBuilder {
            levels,
            added_files: Vec::new(),
            deleted_files: Vec::new(),
            table_cache: Arc::clone(&version.table_cache()),
        }
    }

    pub fn add_file(&mut self, level: usize, file: FileMetaData) {
        assert!(level < NUM_LEVELS);
        let arc_file = Arc::new(file.clone());
        self.levels[level].push(arc_file);
        self.added_files.push(file);
    }

    pub fn delete_file(&mut self, file_number: u64, level: usize) {
        self.levels[level].retain(|f| f.file_number != file_number);
        self.deleted_files.push(file_number);
    }
}
