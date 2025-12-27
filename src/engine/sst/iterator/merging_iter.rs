use std::cmp::Ordering;
use crate::engine::sst::iterator::{InternalIterator,DBIterator};

/// 多路归并 iterator：合并多个已排序的 InternalIterator
pub struct MergingIterator<'a> {
    iters: Vec<Box<dyn InternalIterator + 'a>>,
    /// 当前指向“最小 key”的 iterator 下标
    current: Option<usize>,
    /// 比较函数：通常比较 InternalKey（用户传 comparator）
    cmp: fn(&[u8], &[u8]) -> Ordering,
}

impl<'a> MergingIterator<'a> {
    pub fn new(
        mut iters: Vec<Box<dyn InternalIterator + 'a>>,
        cmp: fn(&[u8], &[u8]) -> Ordering,
    ) -> Self {
        // 先全部 seek_to_first
        for it in iters.iter_mut() {
            it.seek_to_first();
        }

        let mut s = Self {
            iters,
            current: None,
            cmp,
        };
        s.find_smallest();
        s
    }

    fn find_smallest(&mut self) {
        let mut best: Option<usize> = None;
        for (i, it) in self.iters.iter().enumerate() {
            if !it.valid() {
                continue;
            }
            if let Some(bi) = best {
                let k_best = self.iters[bi].key();
                let k_cur = it.key();
                if (self.cmp)(k_cur, k_best) == Ordering::Less {
                    best = Some(i);
                }
            } else {
                best = Some(i);
            }
        }
        self.current = best;
    }
}

impl<'a> InternalIterator for MergingIterator<'a> {
    fn valid(&self) -> bool {
        self.current.is_some()
    }

    fn seek_to_first(&mut self) {
        for it in self.iters.iter_mut() {
            it.seek_to_first();
        }
        self.find_smallest();
    }

    fn seek(&mut self, target: &[u8]) {
        for it in self.iters.iter_mut() {
            it.seek(target);
        }
        self.find_smallest();
    }

    fn next(&mut self) {
        if let Some(idx) = self.current {
            self.iters[idx].next();
        }
        self.find_smallest();
    }

    fn key(&self) -> &[u8] {
        let idx = self.current.expect("invalid MergingIterator.key()");
        self.iters[idx].key()
    }

    fn value(&self) -> &[u8] {
        let idx = self.current.expect("invalid MergingIterator.value()");
        self.iters[idx].value()
    }
}

