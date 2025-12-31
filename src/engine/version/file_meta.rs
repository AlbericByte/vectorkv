pub type FileNumber = u64;

#[derive(Clone)]
pub struct FileMetaData {
    pub file_number: FileNumber,
    pub file_size: u64,

    pub smallest_key: Vec<u8>,
    pub largest_key: Vec<u8>,
}

impl FileMetaData {
    #[inline]
    pub fn contains_key(&self, key: &[u8]) -> bool {
        key >= self.smallest_key.as_slice()
            && key <= self.largest_key.as_slice()
    }
}