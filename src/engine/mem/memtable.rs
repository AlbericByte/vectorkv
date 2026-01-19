use std::cmp::Ordering;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering as AtomicOrdering};
use crate::DBError;
use crate::engine::mem::{ColumnFamilyId, SequenceNumber};
use super::skiplist::{Node, SkipList};
use super::skiplist::Arena;

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub enum ValueType {
    Put,
    Delete,
}

impl ValueType {
    pub fn from_u8(v: u8) -> Option<Self> {
        match v {
            x if x == ValueType::Put as u8 => Some(ValueType::Put),
            x if x == ValueType::Delete as u8 => Some(ValueType::Delete),
            _ => None,
        }
    }
}

impl Default for ValueType {
    fn default() -> Self {
        ValueType::Put // 或你想要的默认值
    }
}

#[derive(Debug, PartialEq, Eq, PartialOrd, Ord, Clone)]
pub struct InternalKey {
    pub user_key: Vec<u8>,
    pub seq: SequenceNumber,
    pub value_type: ValueType,
}

impl InternalKey {
    pub fn new(
        user_key: Vec<u8>,
        seq: SequenceNumber,
        value_type: ValueType,
    ) -> Self {
        Self {
            user_key,
            seq,
            value_type,
        }
    }

    pub fn from_seq_slice(seq: SequenceNumber, bytes:&[u8],) -> Self{
        Self {
            user_key: bytes.to_vec(),
            seq: seq,
            value_type: ValueType::Put
        }
    }

    pub fn from_slice(bytes: &[u8]) -> Self {
        Self {
            user_key: bytes.to_vec(),
            seq: 0,
            value_type: ValueType::Put,
        }
    }

    pub fn len(&self) -> usize {
        self.user_key.len() + std::mem::size_of::<SequenceNumber>() + std::mem::size_of::<ValueType>()
    }

    pub fn encode_to(&self, dst: &mut Vec<u8>) {
        // user key
        dst.extend_from_slice(&self.user_key);

        let tag = (self.seq << 8) | (self.value_type as u64);
        dst.extend_from_slice(&tag.to_le_bytes());
    }


    pub fn decode(bytes: &[u8]) -> Result<Self, DBError> {
        // 至少要有 8 字节的 tag
        if bytes.len() < 8 {
            return Err(DBError::Corruption(
                "internal key too short".to_string(),
            ));
        }

        let n = bytes.len();

        // 拆 user_key 和 tag
        let user_key = bytes[..n - 8].to_vec();

        let mut tag_bytes = [0u8; 8];
        tag_bytes.copy_from_slice(&bytes[n - 8..]);

        let tag = u64::from_le_bytes(tag_bytes);

        let value_type = ValueType::from_u8((tag & 0xff) as u8)
            .ok_or_else(|| {
                DBError::Corruption("invalid value type".to_string())
            })?;

        let seq = tag >> 8;

        Ok(InternalKey {
            user_key,
            seq,
            value_type,
        })
    }

    /// 构造一个 “最大 internal key”，用于 seek(user_key) 时作为上界
    pub fn max_for_user_key(user_key: &[u8]) -> Vec<u8> {
        let mut buf = Vec::with_capacity(user_key.len() + 9);
        buf.extend_from_slice(user_key);
        buf.extend_from_slice(&u64::MAX.to_be_bytes());
        buf.push(0); // ValueType::Value 假定=0
        buf
    }
}

pub fn mvcc_comparator(
    a: &InternalKey,
    b: &InternalKey,
) -> std::cmp::Ordering {
    use std::cmp::Ordering;

    match a.user_key.cmp(&b.user_key) {
        Ordering::Equal => {
            // seq desc, value_type desc
            match b.seq.cmp(&a.seq) {
                Ordering::Equal => {
                    (b.value_type as u8).cmp(&(a.value_type as u8))
                }
                other => other,
            }
        }
        other => other,
    }
}

pub fn raw_mvcc_compare(a: &[u8], b: &[u8]) -> Ordering {
    let a = InternalKey::decode(a).unwrap();
    let b = InternalKey::decode(b).unwrap();
    mvcc_comparator(&a, &b)
}

impl Default for InternalKey {
    fn default() -> Self {
        InternalKey {
            user_key: Vec::new(),
            seq: 0, // 或 SequenceNumber::default()
            value_type: ValueType::default(),
        }
    }
}

pub struct MemTableIterator<'a> {
    current: Option<&'a Node<InternalKey, Vec<u8>>>,
}

impl<'a> Iterator for MemTableIterator<'a> {
    type Item = (&'a InternalKey, &'a Vec<u8>);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(node) = self.current {
            let res = (&node.key, &node.value);
            self.current = unsafe {
                node.next[0].load(AtomicOrdering::SeqCst).as_ref()
            };
            Some(res)
        } else {
            None
        }
    }
}

pub trait MemTable: Send + Sync {
    fn cf_id(&self) -> ColumnFamilyId;
    fn add(&mut self, seq: SequenceNumber, user_key: &[u8], value: &[u8], value_type: ValueType);
    fn get(&self, seq: SequenceNumber, key: &[u8]) -> Option<Vec<u8>>;
    fn approximate_memory_usage(&self) -> usize;
    fn mark_immutable(&mut self);
    fn is_immutable(&self) -> bool;
    fn iter(&self) -> MemTableIterator;
}

// MemTable 实现
pub struct SkipListMemTable {
    cf: ColumnFamilyId,
    pub(crate) skiplist: SkipList<InternalKey, Vec<u8>,fn(&InternalKey, &InternalKey) -> std::cmp::Ordering,fn(&InternalKey, &InternalKey) -> bool>,
    memory_usage: AtomicUsize,
    immutable: AtomicBool,
    frontier_seq: u64,
}


impl SkipListMemTable {
    pub fn new(cf: ColumnFamilyId, seq: u64) -> Self {
        fn is_visible(a: &InternalKey, b: &InternalKey
        ) -> bool {
            a.user_key == b.user_key && a.seq <= b.seq && a.value_type!=ValueType::Delete
        }
        let arena = Arena::new();
        let skiplist:SkipList<InternalKey, Vec<u8>,
            fn(&InternalKey, &InternalKey) -> std::cmp::Ordering,
            fn(&InternalKey, &InternalKey) -> bool> = SkipList::new(arena, mvcc_comparator, is_visible);
        Self {
            cf,
            skiplist,
            memory_usage:AtomicUsize::new(0),
            immutable:AtomicBool::new(false),
            frontier_seq: seq,
        }
    }
}

impl MemTable for SkipListMemTable
{
    fn cf_id(&self) -> ColumnFamilyId {
        self.cf
    }

    fn add(&mut self, seq: SequenceNumber, user_key: &[u8], value: &[u8], value_type: ValueType) {
        // 外层 DBImpl 应该保证“只有一个写线程”在调用 add
        if self.immutable.load(AtomicOrdering::Acquire) {
            panic!("Cannot modify immutable MemTable");
        }

        let ikey = InternalKey::new(user_key.to_vec(), seq, value_type);
        let v = value.to_vec();

        // 估算内存使用量（这里算的比较粗糙）
        let bytes = ikey.len()
            + v.len()
            + std::mem::size_of::<Node<InternalKey, Vec<u8>>>();

        self.memory_usage
            .fetch_add(bytes, AtomicOrdering::Relaxed);

        // 假设 SkipList::insert 是 &self + 内部原子实现
        self.skiplist.insert(ikey, v);
    }

    fn get(&self, seq:SequenceNumber, key: &[u8]) -> Option<Vec<u8>> {
        if seq < self.frontier_seq {
            return None;
        }
        let temp_key = InternalKey::from_seq_slice(seq, key); // 根据实际 InternalKey 定义
        self.skiplist.search(&temp_key).cloned()
    }

    fn approximate_memory_usage(&self) -> usize {
        self.memory_usage.load(AtomicOrdering::Relaxed)
    }

    fn mark_immutable(&mut self) {
        self.immutable.store(true, AtomicOrdering::Release);
    }

    fn is_immutable(&self) -> bool {
        self.immutable.load(AtomicOrdering::Acquire)
    }

    fn iter(&self) -> MemTableIterator {
        let head_ptr = self
            .skiplist
            .head
            .load(AtomicOrdering::Acquire);
        MemTableIterator {
            current: unsafe {
                head_ptr
                    .as_ref()
                    .and_then(|h| {
                        h.next[0]
                            .load(AtomicOrdering::Acquire)
                            .as_ref()
                    })
            },
        }
    }
}
