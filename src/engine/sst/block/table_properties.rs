use std::io::{Read, Seek, Write};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Mutex;
use crate::DBError;
use crate::engine::sst::block::put_varint64;
use crate::engine::sst::block::lsm_codec::LsmCodec;
use crate::engine::sst::BlockHandle;

pub type ColumnFamilyId = u32;
pub type SequenceNumber = u64;

#[derive(Debug, Default)]
pub struct TableProperties {
    pub num_entries: AtomicU64,
    pub data_size: AtomicU64,
    pub index_size: AtomicU64,
    pub filter_size: AtomicU64,
    pub max_sequence: AtomicU64,
    pub column_family_id: ColumnFamilyId,
    pub smallest_key: Mutex<Option<Vec<u8>>>,
    pub largest_key: Mutex<Option<Vec<u8>>>,
}

impl Clone for TableProperties {
    fn clone(&self) -> Self {
        Self {
            num_entries: AtomicU64::new(self.num_entries.load(Ordering::Relaxed)),
            data_size: AtomicU64::new(self.data_size.load(Ordering::Relaxed)),
            index_size: AtomicU64::new(self.index_size.load(Ordering::Relaxed)),
            filter_size: AtomicU64::new(self.filter_size.load(Ordering::Relaxed)),
            max_sequence: AtomicU64::new(self.max_sequence.load(Ordering::Relaxed)),
            column_family_id: self.column_family_id.clone(),
            smallest_key: Mutex::new(self.smallest_key.lock().unwrap().clone()),
            largest_key: Mutex::new(self.largest_key.lock().unwrap().clone()),
        }
    }
}

impl TableProperties {
    pub fn new(cf: ColumnFamilyId) -> Self {
        Self {
            num_entries: AtomicU64::new(0),
            data_size: AtomicU64::new(0),
            index_size: AtomicU64::new(0),
            filter_size: AtomicU64::new(0),
            max_sequence: AtomicU64::new(0),
            column_family_id: cf,
            smallest_key: Mutex::new(None),
            largest_key: Mutex::new(None),
        }
    }

    /// 统计推进（在 memtable flush 里会用）
    pub fn record_entry(&self, seq: SequenceNumber, key: &[u8], value_len: usize) {
        self.num_entries.fetch_add(1, Ordering::SeqCst);
        self.data_size.fetch_add(value_len as u64, Ordering::SeqCst);
        self.max_sequence.fetch_max(seq, Ordering::SeqCst);

        // 维护 key range（只在 first/last 推进一次）
        if self.num_entries.load(Ordering::SeqCst) == 1{
            let mut guard = self.smallest_key.lock().unwrap();
            if guard.is_none() {
                *guard = Some(key.to_vec());
            }
        }
        let mut lg = self.largest_key.lock().unwrap();
        *lg = Some(key.to_vec());
    }

    /// Encode 到 SST 的 properties block 或 footer 附近
    pub fn encode<W: Write>(&self, mut w: W) -> Result<(), DBError> {
        w.write_all(&self.column_family_id.to_le_bytes())?;

        put_varint64(&mut w, self.num_entries.load(Ordering::SeqCst));
        put_varint64(&mut w, self.data_size.load(Ordering::SeqCst));
        put_varint64(&mut w, self.index_size.load(Ordering::SeqCst));
        put_varint64(&mut w, self.filter_size.load(Ordering::SeqCst));
        put_varint64(&mut w, self.max_sequence.load(Ordering::SeqCst));

        let sk_guard = self.smallest_key.lock().unwrap();
        let sk_bytes: &[u8] = match &*sk_guard {
            Some(v) => v.as_slice(),
            None => &[],
        };
        LsmCodec::put_length_prefixed_bytes(&mut w, sk_bytes)?;
        let lk_guard = self.largest_key.lock().unwrap();
        let lk_bytes: &[u8] = match &*lk_guard {
            Some(v) => v.as_slice(),
            None => &[],
        };
        LsmCodec::put_length_prefixed_bytes(&mut w, lk_bytes)?;
        Ok(())
    }

    /// Decode（在 SstReader 解析 footer/properties 时会用）
    pub fn decode<R: Read>(mut r: R) -> Result<Self, DBError> {
        let mut cf_buf = [0u8; 4];
        r.read_exact(&mut cf_buf)?;
        let cf = u32::from_le_bytes(cf_buf);

        let num_entries = LsmCodec::read_varint64(&mut r)?;
        let data_size = LsmCodec::read_varint64(&mut r)?;
        let index_size = LsmCodec::read_varint64(&mut r)?;
        let filter_size = LsmCodec::read_varint64(&mut r)?;
        let max_sequence = LsmCodec::read_varint64(&mut r)?;

        let smallest_key = LsmCodec::get_length_prefixed_bytes(&mut r)?;
        let largest_key = LsmCodec::get_length_prefixed_bytes(&mut r)?;

        Ok(Self {
            num_entries: AtomicU64::new(num_entries),
            data_size: AtomicU64::new(data_size),
            index_size: AtomicU64::new(index_size),
            filter_size: AtomicU64::new(filter_size),
            max_sequence: AtomicU64::new(max_sequence),
            column_family_id: cf,
            smallest_key: Mutex::new(Some(smallest_key)),
            largest_key: Mutex::new(Some(largest_key)),
        })
    }

    /// 读取时判断 snapshot 可见性（你后面 MVCC 读 SST 需要用）
    pub fn seq_visible(&self, snapshot: SequenceNumber) -> bool {
        self.max_sequence.load(Ordering::SeqCst) <= snapshot
    }

    pub fn write_block<W: Write + Seek>(
        &self,
        dst: &mut W,
        offset: u64,
    ) -> Result<BlockHandle, DBError> {
        // 1️⃣ 编码 TableProperties
        let mut buf = Vec::new();
        self.encode_to(&mut buf);  // 现有方法，把最新统计信息编码到字节

        // 2️⃣ 写入 dst
        dst.write_all(&buf)?;

        // 3️⃣ 返回 BlockHandle
        let handle = BlockHandle {
            offset,
            size: buf.len() as u64,
        };

        Ok(handle)
    }
}
