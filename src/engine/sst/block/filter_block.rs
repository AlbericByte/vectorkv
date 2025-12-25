use crate::DBError;
use crate::engine::sst::block::{BlockTrait, BlockType};

pub struct FilterBlock {
    data: Vec<u8>,
    offsets: Vec<u32>,
    base_lg: u8,
}

impl BlockTrait for FilterBlock {
    fn size(&self) -> usize {
        self.data.len()
    }

    fn block_type(&self) -> BlockType {
        BlockType::Filter
    }
}

impl FilterBlock {
    pub fn from_bytes(data: Vec<u8>) -> Result<Self, DBError> {
        if data.len() < 5 {
            return Err(DBError::Corruption("filter block too small".into()));
        }

        let base_lg = data[data.len()-1];
        let offset_array_start =
            u32::from_le_bytes(data[data.len()-5..data.len()-1].try_into().unwrap()) as usize;

        let mut offsets = Vec::new();
        let mut p = offset_array_start;
        while p + 4 <= data.len() - 1 {
            offsets.push(
                u32::from_le_bytes(data[p..p+4].try_into().unwrap())
            );
            p += 4;
        }

        Ok(Self { data, offsets, base_lg })
    }

    pub fn filter_for_data_block(&self, data_block_offset: u64) -> Option<&[u8]> {
        let index = (data_block_offset >> self.base_lg) as usize;
        if index + 1 >= self.offsets.len() {
            return None;
        }
        let start = self.offsets[index] as usize;
        let end = self.offsets[index+1] as usize;
        Some(&self.data[start..end])
    }
}

