use std::sync::Arc;
use crate::engine::sst::block::{BloomFilterBuilder, FilterPolicy};

/// FilterBlockBuilder collects bloom filters for each data block
/// and generates the SSTable-level filter block.
pub struct FilterBlockBuilder {
    filter_policy: Arc<dyn FilterPolicy>,         // Bloom filter bits per key
    keys: Vec<Vec<u8>>,           // Keys in current block
    filters: Vec<Vec<u8>>,        // Bloom filter bytes for each block
    block_offsets: Vec<u64>,      // File offsets of each data block
}

impl FilterBlockBuilder {
    /// Create a new FilterBlockBuilder
    pub fn new(filter_policy: Arc<dyn FilterPolicy>) -> Self {
        Self {
            filter_policy,
            keys: Vec::new(),
            filters: Vec::new(),
            block_offsets: Vec::new(),
        }
    }

    /// Add a key to the current data block
    pub fn add_key(&mut self, key: &[u8]) {
        self.keys.push(key.to_vec());
    }

    /// Mark the start of a new data block
    /// `block_offset` is the file offset of the data block
    pub fn start_block(&mut self, block_offset: u64) {
        // If keys exist from previous block, finish its bloom filter
        if !self.keys.is_empty() {
            self.finish_block();
        }
        self.block_offsets.push(block_offset);
    }

    /// Finish the bloom filter for current block
    fn finish_block(&mut self) {
        let key_refs: Vec<&[u8]> = self.keys.iter().map(|k| k.as_slice()).collect();
        let filter_bytes = self.filter_policy.create_filter(&key_refs);
        self.filters.push(filter_bytes);
        self.keys.clear();
    }

    /// Finish the entire filter block (for SSTable)
    /// Returns bytes that can be written to the SSTable file
    pub fn finish(&mut self) -> Vec<u8> {
        // Finish last block if any
        if !self.keys.is_empty() {
            self.finish_block();
        }

        let mut block_bytes = Vec::new();
        let mut filter_offsets = Vec::new();
        let mut offset = 0u32;

        // 1. Append all filter bytes
        for filter in &self.filters {
            filter_offsets.push(offset);
            block_bytes.extend_from_slice(filter);
            offset += filter.len() as u32;
        }

        // 2. Append filter offsets array
        let offset_array_start = block_bytes.len() as u32;
        for &off in &filter_offsets {
            block_bytes.extend_from_slice(&off.to_le_bytes());
        }

        // 3. Append offset of offset array
        block_bytes.extend_from_slice(&offset_array_start.to_le_bytes());

        // 4. Append base_lg (LevelDB default 11 -> 2KB per filter)
        block_bytes.push(11u8);

        block_bytes
    }

    /// Reset the builder to reuse for a new SSTable
    pub fn reset(&mut self) {
        self.keys.clear();
        self.filters.clear();
        self.block_offsets.clear();
    }
}
