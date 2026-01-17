use crate::error::DBError;
use crate::db::db_iterator::DBIterator;

pub struct VecDbIterator {
    data: Vec<(Vec<u8>, Vec<u8>)>,
    index: isize,
}

impl VecDbIterator {
    pub fn new(data: Vec<(Vec<u8>, Vec<u8>)>) -> Self {
        Self { data, index: -1 }
    }
}

impl DBIterator for VecDbIterator {
    fn seek_to_first(&mut self) {
        if self.data.is_empty() {
            self.index = -1;
        } else {
            self.index = 0;
        }
    }

    fn seek_to_last(&mut self) {
        if self.data.is_empty() {
            self.index = -1;
        } else {
            self.index = (self.data.len() - 1) as isize;
        }
    }

    fn seek(&mut self, key: &[u8]) {
        self.index = -1;
        for (i, (k, _)) in self.data.iter().enumerate() {
            if k.as_slice() >= key {
                self.index = i as isize;
                break;
            }
        }
    }

    fn valid(&self) -> bool {
        self.index >= 0 && (self.index as usize) < self.data.len()
    }

    fn key(&self) -> Option<&[u8]> {
        if self.valid() {
            Some(self.data[self.index as usize].0.as_slice())
        } else {
            None
        }
    }

    fn value(&self) -> Option<&[u8]> {
        if self.valid() {
            Some(self.data[self.index as usize].1.as_slice())
        } else {
            None
        }
    }

    fn next(&mut self) -> Result<(),DBError> {
        if self.valid() {
            self.index += 1;
        }
        Ok(())
    }

    fn prev(&mut self) -> Result<(),DBError> {
        if self.valid() {
            self.index -= 1;
        }
        Ok(())
    }
}
