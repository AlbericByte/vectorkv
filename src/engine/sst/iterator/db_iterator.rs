use crate::engine::mem::{InternalKey, ValueType};
use crate::engine::sst::iterator::InternalIterator;

pub trait DBIterator {
    fn valid(&self) -> bool;
    fn next(&mut self);
    fn key(&self) -> Option<&[u8]>;
    fn value(&self) -> Option<&[u8]>;
    fn seek(&mut self, user_key: &[u8]);
    fn seek_to_first(&mut self);
}

impl<I: InternalIterator> SnapshotIterator<I> {
    pub fn new(inner: I, snapshot_seq: u64) -> Self {
        let mut s = Self {
            inner,
            snapshot_seq,
            current_key: Vec::new(),
            current_value: Vec::new(),
            valid: false,
        };
        // 不自动 seek_to_first，交给调用方
        s
    }

    fn clear_current(&mut self) {
        self.valid = false;
        self.current_key.clear();
        self.current_value.clear();
    }

    /// 从当前 inner 位置开始，找到下一个对用户可见的 key/value
    /// （同时跳过同一个 user_key 的旧版本和 tombstone）
    fn find_next_user_entry(&mut self, mut skip_user_key: Option<Vec<u8>>) {
        self.clear_current();

        while self.inner.valid() {
            let raw_key = self.inner.key();
            let ikey = match InternalKey::decode(raw_key) {
                Some(k) => k,
                None => {
                    // 损坏条目，跳过
                    self.inner.next();
                    continue;
                }
            };

            // 1. 如果 seq > snapshot ，属于“未来版本”，对当前 snapshot 不可见
            if ikey.seq > self.snapshot_seq {
                self.inner.next();
                continue;
            }

            // 2. 如果正在跳过某个 user_key（刚刚处理过的）
            if let Some(ref skip) = skip_user_key {
                if &ikey.user_key == skip {
                    // 同一个 user_key 的更旧版本，直接 skip
                    self.inner.next();
                    continue;
                } else {
                    // 新 key，结束 skip 模式
                    skip_user_key = None;
                }
            }

            match ikey.value_type {
                ValueType::Delete => {
                    // 这个 key 在当前 snapshot 被删除了：
                    // 需要跳过所有同 key 的旧版本
                    let deleted_key = ikey.user_key.clone();
                    self.inner.next();
                    while self.inner.valid() {
                        let next_raw = self.inner.key();
                        if let Some(next_ikey) = InternalKey::decode(next_raw) {
                            if next_ikey.user_key == deleted_key {
                                self.inner.next();
                                continue;
                            }
                        }
                        break;
                    }
                    // 继续 while，寻找下一个 user key
                    continue;
                }
                ValueType::Put => {
                    // 找到当前 snapshot 下可见的最新版本
                    self.current_key = ikey.user_key.clone();
                    self.current_value = self.inner.value().to_vec();
                    self.valid = true;

                    // 把 inner 移动到下一个 entry（留给下一次 find_next_user_entry 跳过旧版本）
                    self.inner.next();
                    return;
                }
            }
        }
        // inner 已经 invalid，结束
        self.valid = false;
    }
}

pub struct SnapshotIterator<I: InternalIterator> {
    inner: I,
    snapshot_seq: u64,
    // 为了让 key()/value() 返回 &[u8]，这里缓存一份当前 user_key / value
    current_key: Vec<u8>,
    current_value: Vec<u8>,
    valid: bool,
}


impl<I: InternalIterator> DBIterator for SnapshotIterator<I> {
    fn valid(&self) -> bool {
        self.valid
    }

    fn seek_to_first(&mut self) {
        self.inner.seek_to_first();
        self.find_next_user_entry(None);
    }

    fn seek(&mut self, user_key: &[u8]) {
        // 构造 internal seek key = (user_key, max_seq, Value)
        let ikey = InternalKey::max_for_user_key(user_key);
        self.inner.seek(&ikey);
        self.find_next_user_entry(None);
    }

    fn next(&mut self) {
        if !self.valid {
            return;
        }
        // 记录当前 user_key，用于跳过旧版本
        let skip_key = Some(self.current_key.clone());
        self.find_next_user_entry(skip_key);
    }

    fn key(&self) -> Option<&[u8]> {
        if self.valid {
            Some(&self.current_key)
        } else {
            None
        }
    }

    fn value(&self) -> Option<&[u8]> {
        if self.valid {
            Some(&self.current_value)
        } else {
            None
        }
    }
}
