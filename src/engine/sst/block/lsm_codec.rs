use std::io::{Read, Write};
use crate::DBError;

impl From<std::io::Error> for DBError {
    fn from(e: std::io::Error) -> Self {
        DBError::Io(e.to_string())
    }
}

/// A collection of encoding/decoding helper functions for LSM storage engine.
pub struct LsmCodec;

impl LsmCodec {
    // ----------------------------- Varint Write (IO) -----------------------------

    /// Encode a u32 value into varint32 format and write it into a writer.
    /// Returns Result to allow `?` to propagate IO errors.
    #[inline]
    pub fn write_varint32<W: Write>(w: &mut W, mut v: u32) -> Result<(), DBError> {
        let mut buf = Vec::new();
        while v >= 0x80 {
            buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        buf.push(v as u8);
        w.write_all(&buf)?; // May fail, so we can propagate the error
        Ok(())
    }

    /// Encode a u64 value into varint64 format and write it into a writer.
    #[inline]
    pub fn write_varint64<W: Write>(w: &mut W, mut v: u64) -> Result<(), DBError> {
        let mut buf = Vec::new();
        while v >= 0x80 {
            buf.push((v as u8) | 0x80);
            v >>= 7;
        }
        buf.push(v as u8);
        w.write_all(&buf)?;
        Ok(())
    }

    // ----------------------------- Varint Read (IO) -----------------------------

    /// Read a varint32-encoded integer from a reader.
    /// Follows RocksDB/LevelDB style to avoid panic on corruption.
    #[inline]
    pub fn read_varint32<R: Read>(r: &mut R) -> Result<u32, DBError> {
        let mut shift = 0;
        let mut out = 0u32;
        let mut buf = [0u8; 1];

        while shift <= 28 {
            r.read_exact(&mut buf)?;
            let b = buf[0];
            out |= ((b & 0x7F) as u32) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
            shift += 7;
        }
        Err(DBError::Corruption("varint32 too long or corrupted".into()))
    }

    /// Read a varint64-encoded integer from a reader.
    #[inline]
    pub fn read_varint64<R: Read>(r: &mut R) -> Result<u64, DBError> {
        let mut shift = 0;
        let mut out = 0u64;
        let mut buf = [0u8; 1];

        while shift <= 63 {
            r.read_exact(&mut buf)?;
            let b = buf[0];
            out |= ((b & 0x7F) as u64) << shift;
            if b & 0x80 == 0 {
                return Ok(out);
            }
            shift += 7;
        }
        Err(DBError::Corruption("varint64 too long or corrupted".into()))
    }

    // ----------------------------- Length-Prefixed Bytes ------------------------

    /// Write bytes in length-prefixed format: `len(varint32) + raw bytes`.
    /// Used by SST flush and WAL batch replay.
    #[inline]
    pub fn put_length_prefixed_bytes<W: Write>(
        w: &mut W,
        bytes: &[u8],
    ) -> Result<(), DBError> {
        Self::write_varint32(w, bytes.len() as u32)?;
        w.write_all(bytes)?;
        Ok(())
    }

    /// Read length-prefixed bytes from a reader.
    #[inline]
    pub fn get_length_prefixed_bytes<R: Read>(
        r: &mut R,
    ) -> Result<Vec<u8>, DBError> {
        let len = Self::read_varint32(r)? as usize;
        let mut buf = vec![0u8; len];
        r.read_exact(&mut buf)?;
        Ok(buf)
    }
}

// ----------------------------- In-Memory Fast Paths -----------------------------

/// Encode a u32 value into varint32 format and store it in a buffer (in-memory only).
/// This version never fails and never writes to IO, so it returns `()`.
#[inline]
pub fn put_varint32(dst: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

/// Encode a u64 value into varint64 format and store it in a buffer (in-memory only).
#[inline]
pub fn put_varint64(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

/// Get a u32 value from a buffer in varint32 format without IO.
/// Returns None on unexpected end or corruption instead of panicking.
#[inline]
pub fn try_get_varint32(src: &[u8], pos: &mut usize) -> Option<u32> {
    let mut shift = 0u32;
    let mut out = 0u32;

    while *pos < src.len() && shift <= 28 {
        let b = src[*pos];
        *pos += 1;
        out |= ((b & 0x7F) as u32) << shift;
        if (b & 0x80) == 0 {
            return Some(out);
        }
        shift += 7;
    }
    None
}

/// Get a u64 value from a buffer in varint64 format.
/// Returns None on unexpected end or corruption.
#[inline]
pub fn try_get_varint64(src: &[u8], pos: &mut usize) -> Option<u64> {
    let mut shift = 0u32;
    let mut out = 0u64;

    while *pos < src.len() && shift <= 63 {
        let b = src[*pos];
        *pos += 1;
        out |= ((b & 0x7F) as u64) << shift;
        if (b & 0x80) == 0 {
            return Some(out);
        }
        shift += 7;
    }
    None
}

/// Fast varint32 decode that panics on corruption. Use only for prototyping.
#[inline]
pub fn get_varint32(src: &[u8], pos: &mut usize) -> u32 {
    try_get_varint32(src, pos).expect("bad varint32")
}

/// Fast varint64 decode that panics on corruption. Use only for prototyping.
#[inline]
pub fn get_varint64(src: &[u8], pos: &mut usize) -> u64 {
    try_get_varint64(src, pos).expect("bad varint64")
}

#[inline]
pub fn encode_fixed32(v: u32) -> [u8; 4] {
    v.to_le_bytes()
}

#[inline]
pub fn encode_fixed64(v: u64) -> [u8; 8] {
    v.to_le_bytes()
}

#[inline]
pub fn decode_fixed32(src: &[u8]) -> u32 {
    let b: [u8; 4] = src
        .get(..4)
        .expect("decode_fixed32: need 4 bytes")
        .try_into()
        .unwrap();
    u32::from_le_bytes(b)
}

#[inline]
pub fn decode_fixed64(src: &[u8]) -> u64 {
    let b: [u8; 8] = src
        .get(..8)
        .expect("decode_fixed64: need 8 bytes")
        .try_into()
        .unwrap();
    u64::from_le_bytes(b)
}

