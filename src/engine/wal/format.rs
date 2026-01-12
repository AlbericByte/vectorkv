use crc32fast::Hasher;
use crate::engine::wal::{WriteBatch, WriteBatchEntry};
use crate::engine::mem::{ColumnFamilyId, SequenceNumber};
use crate::error::DBError; // 你已有的 error

pub const BLOCK_SIZE: usize = 32 * 1024;
pub const HEADER_SIZE: usize = 7; // crc32(4) + len(u16) + type(u8)

#[repr(u8)]
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
pub enum RecordType {
    Full = 1,
    First = 2,
    Middle = 3,
    Last = 4,
}

impl RecordType {
    pub fn from_u8(v: u8) -> Option<Self> {
        Some(match v {
            1 => RecordType::Full,
            2 => RecordType::First,
            3 => RecordType::Middle,
            4 => RecordType::Last,
            _ => return None,
        })
    }
}

/// RocksDB/LevelDB: CRC over (type_byte || payload)
pub fn record_crc32c(typ: RecordType, payload: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(&[typ as u8]);
    hasher.update(payload);
    hasher.finalize()
}



pub const RECORD_WRITE_BATCH: u8 = 1;

pub fn encode_write_batch(base_seq: SequenceNumber, batch: &WriteBatch) -> Vec<u8> {
    let mut buf = Vec::new();

    buf.push(RECORD_WRITE_BATCH);
    buf.extend_from_slice(&base_seq.to_le_bytes());

    let count = batch.entries.len() as u32;
    buf.extend_from_slice(&count.to_le_bytes());

    for e in &batch.entries {
        match e {
            WriteBatchEntry::Put { cf, key, value } => {
                buf.push(1u8); // PUT
                buf.extend_from_slice(&cf.to_le_bytes());

                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);

                buf.extend_from_slice(&(value.len() as u32).to_le_bytes());
                buf.extend_from_slice(value);
            }
            WriteBatchEntry::Delete { cf, key } => {
                buf.push(2u8); // DELETE
                buf.extend_from_slice(&cf.to_le_bytes());

                buf.extend_from_slice(&(key.len() as u32).to_le_bytes());
                buf.extend_from_slice(key);
            }
        }
    }

    buf
}

pub fn decode_write_batch(buf: &[u8]) -> Result<(SequenceNumber, WriteBatch), DBError> {
    let mut pos = 0;

    let tag = read_u8(buf, &mut pos)?;
    if tag != RECORD_WRITE_BATCH {
        return Err(DBError::Corruption(format!("unknown record tag: {}", tag)));
    }

    let base_seq = read_u64(buf, &mut pos)?;
    let count = read_u32(buf, &mut pos)? as usize;

    let mut batch = WriteBatch::new();

    for _ in 0..count {
        let entry_tag = read_u8(buf, &mut pos)?;
        let cf: ColumnFamilyId = read_u32(buf, &mut pos)?;

        match entry_tag {
            1 => {
                let klen = read_u32(buf, &mut pos)? as usize;
                let key = read_vec(buf, &mut pos, klen)?;

                let vlen = read_u32(buf, &mut pos)? as usize;
                let value = read_vec(buf, &mut pos, vlen)?;

                batch.entries.push(WriteBatchEntry::Put { cf, key, value });
            }
            2 => {
                let klen = read_u32(buf, &mut pos)? as usize;
                let key = read_vec(buf, &mut pos, klen)?;
                batch.entries.push(WriteBatchEntry::Delete { cf, key });
            }
            other => {
                return Err(DBError::Corruption(format!("unknown entry tag: {}", other)));
            }
        }
    }

    Ok((base_seq, batch))
}

fn need(buf: &[u8], pos: usize, n: usize) -> Result<(), DBError> {
    if pos + n > buf.len() {
        return Err(DBError::Corruption("unexpected eof".into()));
    }
    Ok(())
}

fn read_u8(buf: &[u8], pos: &mut usize) -> Result<u8, DBError> {
    need(buf, *pos, 1)?;
    let v = buf[*pos];
    *pos += 1;
    Ok(v)
}

pub(crate) fn read_u32(buf: &[u8], pos: &mut usize) -> Result<u32, DBError> {
    need(buf, *pos, 4)?;
    let v = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap());
    *pos += 4;
    Ok(v)
}

pub(crate) fn read_u64(buf: &[u8], pos: &mut usize) -> Result<u64, DBError> {
    need(buf, *pos, 8)?;
    let v = u64::from_le_bytes(buf[*pos..*pos + 8].try_into().unwrap());
    *pos += 8;
    Ok(v)
}

fn read_vec(buf: &[u8], pos: &mut usize, n: usize) -> Result<Vec<u8>, DBError> {
    need(buf, *pos, n)?;
    let out = buf[*pos..*pos + n].to_vec();
    *pos += n;
    Ok(out)
}

pub(crate) fn read_bytes(buf: &[u8], pos: &mut usize) -> Result<Vec<u8>, DBError> {
    need(buf, *pos, 4)?;
    let len = u32::from_le_bytes(buf[*pos..*pos + 4].try_into().unwrap()) as usize;
    *pos += 4;
    need(buf, *pos, len)?;
    let v = buf[*pos..*pos + len].to_vec();
    *pos += len;
    Ok(v)
}

pub(crate) fn read_string(buf: &[u8], pos: &mut usize) -> Result<String, DBError> {
    // read length first
    let len = read_u32(buf, pos)? as usize;
    need(buf, *pos, len)?;
    let slice = &buf[*pos..*pos + len];
    *pos += len;
    // convert to String
    let s = String::from_utf8(slice.to_vec())
        .map_err(|_| DBError::Corruption("invalid UTF-8 in string field".into()))?;

    Ok(s)
}

pub fn crc32_ieee(data: &[u8]) -> u32 {
    let mut hasher = Hasher::new();
    hasher.update(data);
    hasher.finalize()
}

/// RocksDB/LevelDB 兼容的 CRC32 mask
pub fn crc32_mask(crc: u32) -> u32 {
    // 右移 15 位 + 左移 17 位，再与原 crc 做 XOR
    ((crc >> 15) | (crc << 17)).wrapping_add(0xA282_EAD8)
}

