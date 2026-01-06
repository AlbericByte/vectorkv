use std::fs::File;
use std::io::{BufWriter, Write};
use std::path::Path;
use std::sync::atomic::Ordering;
use crate::DBError;
use crate::engine::sst::block::{BlockBuilder, BloomFilterBuilder};
use crate::engine::sst::block::table_properties::TableProperties;
use crate::engine::sst::BlockHandle;
use crate::engine::wal::format::{crc32_ieee, crc32_mask};

const BLOCK_TRAILER_SIZE: usize = 5;
const NO_COMPRESSION: u8 = 0;
const TABLE_MAGIC_NUMBER: u64 = 0xDB47_75A8_4FC2_6C1D;

pub struct BlockBasedTableBuilder {
    w: BufWriter<File>,
    data_block: BlockBuilder,
    index_block: BlockBuilder,
    metaindex_block: BlockBuilder,
    filter_builder: BloomFilterBuilder,

    block_size: usize,
    pending_index_key: Option<Vec<u8>>,
    last_added_key: Vec<u8>,
    last_data_handle: Option<BlockHandle>,

    offset: u64,
    props: TableProperties,
}

impl BlockBasedTableBuilder {
    pub fn create<P: AsRef<Path>>(
        path: P,
        block_size: usize,
        restart_interval: usize,
        bloom_bits_per_key: usize,
    ) -> Result<Self, DBError> {
        let f = File::create(path)?;
        Ok(Self {
            w: BufWriter::new(f),
            data_block: BlockBuilder::new(restart_interval),
            index_block: BlockBuilder::new(restart_interval),
            metaindex_block: BlockBuilder::new(restart_interval),
            filter_builder: BloomFilterBuilder::new(bloom_bits_per_key),

            block_size: block_size.max(1024),
            pending_index_key: None,
            last_added_key: Vec::new(),
            last_data_handle: None,

            offset: 0,
            props: TableProperties::default(),
        })
    }

    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), DBError> {
        if !self.last_added_key.is_empty() && key <= self.last_added_key.as_slice() {
            return Err(DBError::InvalidArgument(
                "SST keys must be strictly increasing".into(),
            ));
        }

        if self.props.num_entries.load(Ordering::Relaxed) == 0 {
            let mut guard = self.props.smallest_key.lock().unwrap();
            *guard = Some(key.to_vec());
        }

        let mut guard = self.props.largest_key.lock().unwrap();
        *guard = Some(key.to_vec());
        self.props.num_entries.fetch_add(1, Ordering::Relaxed);

        self.filter_builder.add_key(key);
        self.data_block.add(key, value);
        self.last_added_key = key.to_vec();

        if self.data_block.current_size_estimate() >= self.block_size {
            self.flush_data_block()?;
        }

        Ok(())
    }

    pub fn finish(&mut self) -> Result<(u64, TableProperties), DBError> {
        if !self.data_block.is_empty() {
            self.flush_data_block()?;
        }

        if let Some(pending) = self.pending_index_key.take() {
            if let Some(handle) = &self.last_data_handle {
                let mut enc = Vec::new();
                handle.encode_to(&mut enc);
                self.index_block.add(&pending, &enc);
            }
        }

        let filter = self.filter_builder.finish();
        let filter_handle = self.write_block(&filter, NO_COMPRESSION)?;

        {
            let mut v = Vec::new();
            filter_handle.encode_to(&mut v);
            self.metaindex_block.add(b"filter.bloom", &v);
        }

        let metaindex_handle = self.write_block(&self.metaindex_block.finish(), NO_COMPRESSION)?;
        let index_handle = self.write_block(&self.index_block.finish(), NO_COMPRESSION)?;
        self.write_footer(metaindex_handle, index_handle).map_err(|e| DBError::Io(
            format!("SST write failed: BlockBasedTableBuilder write footer error {}", e.to_string())))?;

        self.w.flush().map_err(|e| DBError::Io(
            format!("SST write failed: BlockBasedTableBuilder finish error {}", e.to_string())))?;
        Ok((self.offset, self.props.clone()))
    }

    // ------------------- internal helpers --------------------

    fn flush_data_block(&mut self) -> Result<(), DBError> {
        let last_key = self.last_added_key.clone();
        let raw = self.data_block.finish();
        let handle = self.write_block(&raw, NO_COMPRESSION)?;
        self.last_data_handle = Some(handle);
        self.pending_index_key = Some(last_key);
        self.data_block.reset();
        Ok(())
    }

    fn write_block(&mut self, raw: &[u8], block_type: u8) -> Result<BlockHandle, DBError> {
        let offset = self.offset;
        let size = raw.len() as u64;

        self.w.write_all(raw)?;

        let mut trailer = [0u8; BLOCK_TRAILER_SIZE];
        trailer[0] = block_type;
        let mut input = Vec::new();
        input.extend_from_slice(raw);
        input.push(block_type);
        let crc = crc32_mask(crc32_ieee(&input));
        trailer[1..5].copy_from_slice(&crc.to_le_bytes());

        self.w.write_all(&trailer).map_err(|e| DBError::Io(|e| DBError::Io(
            format!("SST write failed: BlockBasedTableBuilder write block error {}", e.to_string()))))?;
        self.offset = offset + size + (BLOCK_TRAILER_SIZE as u64);

        Ok(BlockHandle { offset, size })
    }

    fn write_footer(&mut self, meta: BlockHandle, index: BlockHandle) -> Result<(), DBError> {
        let mut buf = Vec::new();
        meta.encode_to(&mut buf);
        index.encode_to(&mut buf);
        buf.extend_from_slice(&TABLE_MAGIC_NUMBER.to_le_bytes());
        self.w.write_all(&buf).map_err(|e| DBError::Io(|e| DBError::Io(
            format!("SST write failed: BlockBasedTableBuilder write footer error {}", e.to_string()))))?;
        self.offset += buf.len() as u64;
        Ok(())
    }
}
