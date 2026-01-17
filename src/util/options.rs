use std::path::PathBuf;
use serde::Deserialize;
use crate::util::{ColumnFamilyOptions, WriteOptions};

#[derive(Debug, Clone)]
pub struct Options {
    // MemTable
    pub write_buffer_size: usize,
    pub max_write_buffer_number: usize,
    pub allow_concurrent_memtable_write: bool,

    // Compaction
    pub level0_file_num_compaction_trigger: usize,
    pub max_background_compactions: usize,
    pub max_background_flushes: usize,

    // SST / Compression
    pub compression: CompressionType,

    // Cache / Table
    pub block_cache_size: usize,
    pub optimize_filters_for_hits: bool,

    // WAL
    pub enable_write_ahead_log: bool,

    // Files
    pub max_open_files: i32,

    // Manifest
    pub max_manifest_file_size: u64,

    // Column Families
    pub system_cf: ColumnFamilyOptions,
    pub user_cf: ColumnFamilyOptions,
}

#[derive(Debug, Clone)]
pub struct OpenOptions {
    // open
    pub create_if_missing: bool,

    // Default options
    pub write: WriteOptions,

    // Path override
    pub wal_dir: Option<PathBuf>,
    pub sst_dir: Option<PathBuf>,
    pub manifest_dir: Option<PathBuf>,

    // ===== Block cache（open-only）=====
    pub block_cache_capacity: Option<usize>,
    pub block_cache_shards: Option<usize>,

    // Runtime variable
    pub options: Options,
}

#[derive(Debug, Deserialize)]
pub struct OptionsFile {
    pub write_buffer_size: Option<usize>,
    pub max_write_buffer_number: Option<usize>,
    pub allow_concurrent_memtable_write: Option<bool>,

    pub level0_file_num_compaction_trigger: Option<usize>,
    pub max_background_compactions: Option<usize>,
    pub max_background_flushes: Option<usize>,

    pub compression: Option<CompressionType>,
    pub block_cache_size: Option<usize>,
    pub optimize_filters_for_hits: Option<bool>,

    pub enable_write_ahead_log: Option<bool>,
    pub max_open_files: Option<i32>,
    pub max_manifest_file_size: Option<u64>,
}

/// 压缩类型对应 C++ CompressionType
#[derive(Debug, Clone, Copy)]
pub enum CompressionType {
    NoCompression,
    SnappyCompression,
    ZlibCompression,
    Bz2Compression,
    Lz4Compression,
    ZstdCompression,
}

impl Default for CompressionType {
    fn default() -> Self {
        CompressionType::SnappyCompression
    }
}

impl Default for OpenOptions {
    fn default() -> Self {
        Self {
            create_if_missing: true,
            write: WriteOptions::default(),

            wal_dir: None,
            sst_dir: None,
            manifest_dir: None,

            block_cache_capacity: None,
            block_cache_shards: None,

            options: Options {
                write_buffer_size: 64 << 20,
                max_write_buffer_number: 2,
                allow_concurrent_memtable_write: true,

                level0_file_num_compaction_trigger: 4,
                max_background_compactions: 4,
                max_background_flushes: 2,

                compression: CompressionType::SnappyCompression,

                block_cache_size: 256 << 20,
                optimize_filters_for_hits: true,

                enable_write_ahead_log: true,
                max_open_files: 1024,

                max_manifest_file_size: 64 << 20,

                system_cf: ColumnFamilyOptions::default(),
                user_cf: ColumnFamilyOptions::default(),
            },
        }
    }
}

impl OpenOptions {
    /// Consume open-only information and produce runtime Options
    pub fn to_options(&self) -> Options {
        Options {
            // ===== MemTable =====
            write_buffer_size: self.options.write_buffer_size,
            max_write_buffer_number: self.options.max_write_buffer_number,
            allow_concurrent_memtable_write: self.options.allow_concurrent_memtable_write,

            // ===== Compaction =====
            level0_file_num_compaction_trigger:
            self.options.level0_file_num_compaction_trigger,
            max_background_compactions:
            self.options.max_background_compactions,
            max_background_flushes:
            self.options.max_background_flushes,

            // ===== Compression =====
            compression: self.options.compression,

            // ===== Cache / Table =====
            block_cache_size: self.options.block_cache_size,
            optimize_filters_for_hits: self.options.optimize_filters_for_hits,

            // ===== WAL =====
            enable_write_ahead_log: self.options.enable_write_ahead_log,

            // ===== Files =====
            max_open_files: self.options.max_open_files,

            // ===== Manifest =====
            max_manifest_file_size: self.options.max_manifest_file_size,

            // ===== Column Families =====
            system_cf: self.options.system_cf.clone(),
            user_cf: self.options.user_cf.clone(),
        }
    }
}
