use crate::{DBError, DB};

pub trait DBIterator {
    /// 移动到第一个元素
    fn seek_to_first(&mut self);

    /// 移动到最后一个元素
    fn seek_to_last(&mut self);

    /// 移动到指定 key
    fn seek(&mut self, key: &[u8]);

    /// 是否有效（当前位置是否有值）
    fn valid(&self) -> bool;

    /// 当前 key
    fn key(&self) -> Option<&[u8]>;

    /// 当前 value
    fn value(&self) -> Option<&[u8]>;

    /// 向前移动
    fn next(&mut self) -> Result<(),DBError>;

    /// 向后移动（可选）
    fn prev(&mut self) -> Result<(),DBError>;
}
