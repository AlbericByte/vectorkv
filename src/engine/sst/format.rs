use std::io::{Read, Seek, SeekFrom};
use crate::DBError;
use crate::util::TABLE_MAGIC;

#[derive(Clone, Copy, Debug, Default)]
pub struct BlockHandle {
    pub offset: u64,
    pub size: u64,
}

impl BlockHandle {
    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        put_varint64(dst, self.offset);
        put_varint64(dst, self.size);
    }

    pub fn decode_from(src: &[u8], pos: &mut usize) -> Option<Self> {
        let offset = get_varint64(src, pos)?;
        let size = get_varint64(src, pos)?;
        Some(Self { offset, size })
    }

    pub fn decode_from_bytes(bytes: &[u8]) -> Result<Self, DBError> {
        let mut pos = 0usize;
        let offset = get_varint64(bytes, &mut pos)
            .ok_or(DBError::Corruption("bad block handle offset".into()))?;
        let size = get_varint64(bytes, &mut pos)
            .ok_or(DBError::Corruption("bad block handle size".into()))?;
        Ok(Self { offset, size })
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct Footer {
    pub metaindex_handle: BlockHandle, // 可先空
    pub index_handle: BlockHandle,
}

impl Footer {
    // RocksDB/LevelDB footer 固定长度（LevelDB 是 48 bytes）
    pub const ENCODED_LEN: usize = 48;

    pub fn encode(&self) -> [u8; Self::ENCODED_LEN] {
        let mut buf = Vec::with_capacity(Self::ENCODED_LEN);
        self.metaindex_handle.encode_to(&mut buf);
        self.index_handle.encode_to(&mut buf);

        // padding 到 40 bytes，然后写 magic u64 = 8 bytes，共 48
        if buf.len() < 40 {
            buf.resize(40, 0);
        }
        buf.extend_from_slice(&TABLE_MAGIC.to_le_bytes());

        let mut out = [0u8; Self::ENCODED_LEN];
        out.copy_from_slice(&buf[..Self::ENCODED_LEN]);
        out
    }

    pub fn decode(input: &[u8]) -> Option<Self> {
        if input.len() != Self::ENCODED_LEN {
            return None;
        }
        let magic = u64::from_le_bytes(input[40..48].try_into().ok()?);
        if magic != TABLE_MAGIC {
            return None;
        }
        let mut pos = 0usize;
        let metaindex_handle = BlockHandle::decode_from(input, &mut pos)?;
        let index_handle = BlockHandle::decode_from(input, &mut pos)?;
        Some(Self { metaindex_handle, index_handle })
    }


    pub fn read_from_file<R>(
        reader: &mut R,
        file_len: u64,
    ) -> Result<Self, DBError>
    where
        R: Read + Seek,
    {
        if file_len < Self::ENCODED_LEN as u64 {
            return Err(DBError::Corruption("file too short to be an sstable".to_string()));
        }

        // 1️⃣ 定位到 footer 起始位置
        reader.seek(SeekFrom::Start(
            file_len -  Self::ENCODED_LEN  as u64,
        ))?;

        // 2️⃣ 读 footer
        let mut buf = [0u8;  Self::ENCODED_LEN ];
        reader.read_exact(&mut buf)?;

        let mut pos = 0usize;

        // 3️⃣ 解 metaindex block handle
        let metaindex_handle =
            BlockHandle::decode_from(&buf, &mut pos)
                .ok_or_else(|| {
                    DBError::Corruption("bad metaindex handle".to_string())
                })?;

        // 4️⃣ 解 index block handle
        let index_handle =
            BlockHandle::decode_from(&buf, &mut pos)
                .ok_or_else(|| {
                    DBError::Corruption("bad index handle".to_string())
                })?;

        // 5️⃣ 校验 magic number
        let magic = u64::from_le_bytes(
            buf[ Self::ENCODED_LEN  - 8..]
                .try_into()
                .unwrap(),
        );

        if magic != TABLE_MAGIC {
            return Err(DBError::Corruption("bad sstable magic number".to_string()));
        }

        Ok(Footer {
            metaindex_handle,
            index_handle,
        })
    }

}

// ------- coding helpers (varint) -------

pub fn put_varint64(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

pub fn get_varint64(src: &[u8], pos: &mut usize) -> Option<u64> {
    let mut shift = 0u32;
    let mut out = 0u64;
    while *pos < src.len() && shift <= 63 {
        let b = src[*pos];
        *pos += 1;
        out |= ((b & 0x7f) as u64) << shift;
        if (b & 0x80) == 0 {
            return Some(out);
        }
        shift += 7;
    }
    None
}

pub fn decode_fixed32(src: &[u8]) -> u32 {
    let bytes: [u8; 4] = src.try_into().unwrap();
    u32::from_le_bytes(bytes)
}

pub fn hash64(data: &[u8], seed: u64) -> u64 {
    let mut h = seed ^ (data.len() as u64).wrapping_mul(0x9E37_79B9_7F4A_7C15);
    for &b in data {
        h ^= b as u64;
        h = h.wrapping_mul(0xC2B2_AE3D_27D4_EB4F);
        h ^= h >> 33;
    }
    // final avalanche
    h ^= h >> 33;
    h = h.wrapping_mul(0xFF51_AFD7_ED55_8CCD);
    h ^= h >> 33;
    h = h.wrapping_mul(0xC4CE_B9FE_1A85_EC53);
    h ^= h >> 33;
    h
}
