// src/sst/table_builder.rs
use std::io::{self, Write};
use std::sync::atomic::Ordering;
use crate::DBError;
use crate::engine::mem::InternalKey;
use crate::engine::sst::block::{BlockBuilder, MetaIndexBlockBuilder, TableProperties, FilterBlockBuilder};
use crate::engine::sst::format::{BlockHandle, Footer};
use crate::engine::sst::SstReader;
use crate::engine::version::FileMetaData;
use crate::util::{ColumnFamilyOptions, Options};

pub struct TableBuilder<W: Write> {
    file_number: u64,
    dst: W,
    offset: u64,
    block_size: usize,
    // Blocks
    data_block: BlockBuilder,   // Current data block
    index_block: BlockBuilder,  // Index block
    metaindex_block: MetaIndexBlockBuilder,
    // Optional filter block
    filter_block: Option<FilterBlockBuilder>,

    // Pending index for delayed writing
    pending_index_handle: Option<BlockHandle>,
    pending_index_key:  Option<Vec<u8>>,

    smallest_key: Option<Vec<u8>>,
    last_added_key: Option<Vec<u8>>,
    last_data_handle: Option<BlockHandle>,



    props: TableProperties,
}

impl<W: Write> TableBuilder<W> {

    pub fn from_options(file_number:u64, dst: W, cf_opts: &ColumnFamilyOptions) -> Self {
        let table_opts = &cf_opts.table_options;
        Self::new(
            file_number,
            dst,
            table_opts.block_size,
            table_opts.restart_interval,
            table_opts.filter_policy
                .as_ref()
                .map(|p| FilterBlockBuilder::new(p.clone())),
        )
    }

    pub fn new(
        file_number:u64,
        dst: W,
        block_size: usize,
        restart_interval: usize,
        filter_block: Option<FilterBlockBuilder>,
    ) -> Self {
        Self {
            file_number,
            dst,
            offset: 0,
            block_size,
            data_block: BlockBuilder::new(restart_interval),
            index_block: BlockBuilder::new(1),       // index block restart_interval=1
            metaindex_block: MetaIndexBlockBuilder::new(1),   // metaindex restart_interval=1
            filter_block,
            pending_index_handle: None,
            pending_index_key: None,
            smallest_key: None,
            last_added_key: None,
            last_data_handle: None,
            props: TableProperties::default(),
        }
    }

    /// Add a key-value pair
    pub fn add(&mut self, key: &[u8], value: &[u8]) -> Result<(), DBError> {
        // Check key order
        if let Some(last_key) = &self.last_added_key {
            if key <= last_key.as_slice() {
                return Err(DBError::InvalidKeyOrder("Keys must be added in order".into()));
            }
        }

        // Add key to filter block if present
        if let Some(filter) = &mut self.filter_block {
            filter.add_key(key);
        }

        // Add to data block
        self.data_block.add(key, value);

        // Flush if block size exceeded
        if self.data_block.current_size_estimate() >= self.block_size {
            self.flush_data_block(key)?;
        }

        if let Some(buf) = &mut self.last_added_key {
            buf.clear();
            buf.extend_from_slice(key);
        } else {
            self.last_added_key = Some(key.to_vec());
        }

        if self.smallest_key.is_none() {
            self.smallest_key = Some(key.to_vec());
        }

        Ok(())
    }

    /// Flush current data block to file
    fn flush_data_block(&mut self, next_key: &[u8]) -> Result<(), DBError> {
        if self.data_block.is_empty() {
            return Ok(());
        }

        // Finish block bytes
        let block_bytes = self.data_block.finish();
        let block_len = block_bytes.len() as u64;

        // Write to dst
        self.dst.write_all(&block_bytes)?;
        let handle = BlockHandle {
            offset: self.offset,
            size: block_len,
        };
        self.offset += block_len;

        // Update TableProperties
        self.props.num_entries.fetch_add(self.data_block.counter() as u64, Ordering::Relaxed);

        // If there is a pending index, write it now
        if let Some(pending_key) = self.pending_index_key.take() {
            let mut handle_encoded = Vec::new();
            put_varint64(&mut handle_encoded, handle.offset);
            put_varint64(&mut handle_encoded, handle.size);
            self.index_block.add(&pending_key, &handle_encoded);
        }

        // Set pending_index_key for next flush
        self.pending_index_key = Some(next_key.to_vec());
        self.last_data_handle = Some(handle);

        self.data_block.reset();
        Ok(())
    }

    /// Finish the SSTable
    pub fn finish(mut self) -> Result<FileMetaData, DBError> {
        // 1️⃣ flush data block
        if !self.data_block.is_empty() {
            let data_bytes = self.data_block.finish();
            let offset = self.offset;
            let len = data_bytes.len() as u64;
            self.dst.write_all(&data_bytes)?;
            self.last_data_handle = Some(BlockHandle { offset, size: len });
            self.offset += len;
        }

        // 2️⃣ add the last index entry
        if let Some(pending_key) = self.pending_index_key.take() {
            let handle = self.last_data_handle
                .expect("pending_index_key exists but no last_data_handle");
            let mut handle_encoded = Vec::new();
            put_varint64(&mut handle_encoded, handle.offset);
            put_varint64(&mut handle_encoded, handle.size);
            self.index_block.add(&pending_key, &handle_encoded);
        }

        // 3️⃣ flush filter block (可选)
        let filter_handle = if let Some(filter) = &mut self.filter_block {
            let filter_bytes = filter.finish();
            let offset = self.offset;
            let len = filter_bytes.len() as u64;
            self.dst.write_all(&filter_bytes)?;
            self.offset += len;
            Some(BlockHandle { offset, size: len })
        } else {
            None
        };

        // 4️⃣ flush TableProperties block
        let props_handle = self.props.write_block(&mut self.dst, self.offset)?;
        self.offset += props_handle.size;

        // 5️⃣ 写 metaindex block
        if let Some(fh) = filter_handle {
            self.metaindex_block.add_filter_block("bloomfilter", fh);
        }
        self.metaindex_block.add_properties_block(props_handle);

        // 6️⃣ flush metaindex block
        let meta_bytes = self.metaindex_block.finish();
        let meta_offset = self.offset;
        let meta_len = meta_bytes.len() as u64;
        self.dst.write_all(&meta_bytes)?;
        self.offset += meta_len;
        let meta_handle = BlockHandle {
            offset: meta_offset,
            size: meta_len,
        };

        // 7️⃣ flush index block
        let index_bytes = self.index_block.finish();
        let index_offset = self.offset;
        let index_len = index_bytes.len() as u64;
        self.dst.write_all(&index_bytes)?;
        self.offset += index_len;
        let index_handle = BlockHandle {
            offset: index_offset,
            size: index_len,
        };

        // 8️⃣ write footer
        let footer = Footer {
            metaindex_handle: meta_handle,
            index_handle,
        };
        let footer_bytes = footer.encode();
        self.dst.write_all(&footer_bytes)?;
        self.offset += footer_bytes.len() as u64;

        let file_size = self.offset;
        let smallest = self.smallest_key
            .take()
            .ok_or(DBError::EmptyTable("smallest key is none".into()))?;
        let largest = self.last_added_key
            .take()
            .ok_or(DBError::EmptyTable("last_added_key key is none".into()))?;
        Ok(FileMetaData {
            file_number: self.file_number,
            file_size: file_size,
            smallest_key: smallest,
            largest_key: largest,
            allowed_seeks: 1 << 30,
        })
    }

    pub fn reset(&mut self) {
        self.data_block.reset();
        self.index_block.reset();
        self.metaindex_block.reset();
        if let Some(filter) = &mut self.filter_block {
            filter.reset();
        }
        self.pending_index_handle = None;
        self.pending_index_key = None;
        self.smallest_key = None;
        self.last_added_key = None;
        self.last_data_handle = None;
        self.props = TableProperties::default();
        self.offset = 0;
    }
}

/// Helper: put u64 as varint (simplified)
fn put_varint64(buf: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        buf.push((v as u8) | 0x80);
        v >>= 7;
    }
    buf.push(v as u8);
}
