// src/sst/block/coding.rs

/// Encode fixed-length little-endian integers (RocksDB/LevelDB style).

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

/// Varint encoding/decoding.
/// - put_* appends to Vec<u8>
/// - get_* reads from &[u8] at pos, advances pos
///
/// These are minimal, fast enough, and match RocksDB behavior:
/// - u32 encoded as varint32
/// - u64 encoded as varint64

#[inline]
pub fn put_varint32(dst: &mut Vec<u8>, mut v: u32) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

#[inline]
pub fn put_varint64(dst: &mut Vec<u8>, mut v: u64) {
    while v >= 0x80 {
        dst.push((v as u8) | 0x80);
        v >>= 7;
    }
    dst.push(v as u8);
}

/// Safe version: returns None if buffer ends unexpectedly or varint is too long.
/// 推荐你在 BlockIter / decode 路径用这个，避免 corruption 直接 panic。
#[inline]
pub fn try_get_varint32(src: &[u8], pos: &mut usize) -> Option<u32> {
    let mut shift = 0u32;
    let mut out = 0u32;

    while *pos < src.len() && shift <= 28 {
        let b = src[*pos];
        *pos += 1;

        out |= ((b & 0x7f) as u32) << shift;

        if (b & 0x80) == 0 {
            return Some(out);
        }
        shift += 7;
    }
    None
}

#[inline]
pub fn try_get_varint64(src: &[u8], pos: &mut usize) -> Option<u64> {
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

/// Fast version (panic on invalid/corruption).
/// 如果你现在先追求“跑通”，可以用这个；
/// 等你加 corruption handling，再切到 try_get_*。
#[inline]
pub fn get_varint32(src: &[u8], pos: &mut usize) -> u32 {
    try_get_varint32(src, pos).expect("get_varint32: bad varint or out of range")
}

#[inline]
pub fn get_varint64(src: &[u8], pos: &mut usize) -> u64 {
    try_get_varint64(src, pos).expect("get_varint64: bad varint or out of range")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_fixed32_64() {
        let x32 = 0xdead_beefu32;
        let b32 = encode_fixed32(x32);
        assert_eq!(decode_fixed32(&b32), x32);

        let x64 = 0x0123_4567_89ab_cdefu64;
        let b64 = encode_fixed64(x64);
        assert_eq!(decode_fixed64(&b64), x64);
    }

    #[test]
    fn test_varint32_roundtrip() {
        let vals = [
            0u32,
            1,
            127,
            128,
            129,
            16_383,
            16_384,
            1_000_000,
            u32::MAX,
        ];
        for &v in &vals {
            let mut buf = Vec::new();
            put_varint32(&mut buf, v);
            let mut pos = 0usize;
            let got = get_varint32(&buf, &mut pos);
            assert_eq!(got, v);
            assert_eq!(pos, buf.len());
        }
    }

    #[test]
    fn test_varint64_roundtrip() {
        let vals = [
            0u64,
            1,
            127,
            128,
            129,
            16_383,
            16_384,
            1_000_000,
            u32::MAX as u64,
            u64::MAX,
        ];
        for &v in &vals {
            let mut buf = Vec::new();
            put_varint64(&mut buf, v);
            let mut pos = 0usize;
            let got = get_varint64(&buf, &mut pos);
            assert_eq!(got, v);
            assert_eq!(pos, buf.len());
        }
    }
}
