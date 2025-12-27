use std::path::PathBuf;
use crate::engine::sst::iterator::DBIterator;

/// A fallback iterator that holds no data.
/// All operations are no-ops to prevent panics during DB open or when a column family is missing.
pub struct EmptyIterator {
}

impl EmptyIterator {
    /// Create a new empty iterator instance.
    pub fn new() -> Self {
        Self {}
    }
}

impl DBIterator for EmptyIterator {
    /// Always return `false` because the iterator contains no data.
    fn valid(&self) -> bool {
        false
    }

    /// Move to the next entry. No-op for empty iterator.
    fn next(&mut self) {
        // Do nothing
    }

    /// Return the current key. Always `None` for empty iterator.
    fn key(&self) -> Option<&[u8]> {
        None
    }

    /// Return the current value. Always `None` for empty iterator.
    fn value(&self) -> Option<&[u8]> {
        None
    }

    /// Seek to the specified user key. No-op for empty iterator.
    fn seek(&mut self, _user_key: &[u8]) {
        // Do nothing
    }

    /// Seek to the first key in the column family. No-op for empty iterator.
    fn seek_to_first(&mut self) {
        // Do nothing
    }
}
