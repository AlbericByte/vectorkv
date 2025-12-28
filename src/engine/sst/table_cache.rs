use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use crate::DBError;
use crate::engine::sst::block::{BlockCache, DataBlock, FilterPolicy};
use crate::engine::sst::SstReader;
use crate::engine::version::FileMetaData;

pub struct TableCache {
    cache: Mutex<HashMap<u64, Arc<SstReader>>>, // file_number → reader
    db_path: PathBuf,
    block_cache: Arc<BlockCache<DataBlock>>,
    filter_policy: Option<Arc<dyn FilterPolicy>>,
}

impl TableCache {
    pub fn new<P: AsRef<Path>>(
        db_path: P,
        block_cache: Arc<BlockCache<DataBlock>>,
        filter_policy: Option<Arc<dyn FilterPolicy>>,
    ) -> Self {
        Self {
            cache: Mutex::new(HashMap::new()),
            db_path: db_path.as_ref().to_path_buf(),
            block_cache,
            filter_policy,
        }
    }

    /// 根据 file_number 找 sst reader
    pub fn find_table_by_number(&self, file_number: u64) -> Option<Arc<SstReader>> {
        let mut guard = self.cache.lock().unwrap();

        if let Some(reader) = guard.get(&file_number) {
            return Some(reader.clone());
        }

        let path = self.db_path.join(format!("{file_number}.sst"));

        let reader = Arc::new(
            SstReader::open(
                file_number,
                path,
                self.block_cache.clone(),
                self.filter_policy.clone(),
            ).ok()?
        );

        guard.insert(file_number, reader.clone());
        Some(reader)
    }

    pub fn find_table(&self, file: &Arc<FileMetaData>) -> Option<Arc<SstReader>> {
        let mut cache = self.cache.lock().unwrap();

        if let Some(r) = cache.get(&file.file_number) {
            return Some(r.clone());
        }

        let path = self.db_path.join(format!("{}.sst", file.file_number));
        let reader = Arc::new(SstReader::open(
                                                file.file_number,
                                                path,
                                                Arc::clone(&self.block_cache),
                                                self.filter_policy.clone(),).ok()?);

        cache.insert(file.file_number, reader.clone());
        Some(reader)
    }

    pub fn get(&self, file_number: u64, key: &[u8]) -> Result<Option<Vec<u8>>,DBError> {
        let table = self.find_table_by_number(file_number)
            .ok_or(DBError::NotFound(format!("file {} not found", file_number)))?;
        table.get(key)
    }
}
