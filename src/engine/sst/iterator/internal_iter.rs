use std::cmp::Ordering;

/// 所有内部 iterator（datablock / index / two-level / merge）统一实现这个接口
pub trait InternalIterator {
    /// 当前是否指向有效 entry
    fn valid(&self) -> bool;

    /// 定位到第一个 entry
    fn seek_to_first(&mut self);

    /// 定位到 >= target 的第一条记录
    fn seek(&mut self, target: &[u8]);

    /// 前进到下一条
    fn next(&mut self);

    /// 当前 key（仅在 valid() == true 时调用）
    fn key(&self) -> &[u8];

    /// 当前 value（仅在 valid() == true 时调用）
    fn value(&self) -> &[u8];
}
