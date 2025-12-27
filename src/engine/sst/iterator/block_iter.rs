use crate::engine::sst::iterator::InternalIterator;

/// Generic Block Iterator：包装一个 Box<dyn InternalIterator>
pub struct BlockIter<'a> {
    inner: Box<dyn InternalIterator + 'a>,
}

impl<'a> BlockIter<'a> {
    pub fn new(inner: Box<dyn InternalIterator + 'a>) -> Self {
        Self { inner }
    }
}

impl<'a> InternalIterator for BlockIter<'a> {
    fn valid(&self) -> bool {
        self.inner.valid()
    }

    fn seek_to_first(&mut self) {
        self.inner.seek_to_first()
    }

    fn seek(&mut self, target: &[u8]) {
        self.inner.seek(target)
    }

    fn next(&mut self) {
        self.inner.next()
    }

    fn key(&self) -> &[u8] {
        self.inner.key()
    }

    fn value(&self) -> &[u8] {
        self.inner.value()
    }
}
