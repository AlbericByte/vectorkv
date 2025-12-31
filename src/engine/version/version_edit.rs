use std::sync::Arc;
use crate::DBError;
use crate::engine::mem::{ColumnFamilyId, SequenceNumber};
use crate::engine::version::{FileMetaData, FileNumber};
use crate::engine::wal::{read_bytes, read_string, read_u32, read_u64};

const TAG_CF_ID: u8 = 1;
const TAG_CF_ADD: u8 = 2;
const TAG_CF_DROP: u8 = 3;
const TAG_ADD_FILE: u8 = 4;
const TAG_DELETE_FILE: u8 = 5;
const TAG_NEXT_FILE_NUMBER: u8 = 6;
const TAG_LAST_SEQUENCE: u8 = 7;

pub struct VersionEdit {
    pub cf_id: ColumnFamilyId,
    pub cf_name: Option<String>,   // only CF_ADD writes this
    pub is_cf_add: bool,
    pub is_cf_drop: bool,
    pub add_files: Vec<(usize, FileMetaData)>,
    pub delete_files: Vec<(usize, FileNumber)>,
    pub next_file_number: Option<FileNumber>,
    pub last_sequence: Option<SequenceNumber>,
}

impl Default for VersionEdit {
    fn default() -> Self {
        Self {
            cf_id: 0,
            cf_name: None,
            is_cf_add: false,
            is_cf_drop: false,

            add_files: Vec::new(),
            delete_files: Vec::new(),
            next_file_number: None,
            last_sequence: None,
        }
    }
}

impl VersionEdit {
    pub fn new(cf_id: ColumnFamilyId) -> Self {
        Self {
            cf_id,
            cf_name: None,
            is_cf_add: false,
            is_cf_drop: false,
            add_files: Vec::new(),
            delete_files: Vec::new(),
            next_file_number:None,
            last_sequence: None,
        }
    }

    pub fn encode_version_edit(edit: &VersionEdit) -> Vec<u8> {
        let mut buf = Vec::new();

        if edit.is_cf_add {
            // ---- column family add ----
            buf.push(TAG_CF_ADD);

            // encode cf_id
            buf.extend_from_slice(&edit.cf_id.to_le_bytes());

            // encode cf_name
            let name_bytes = edit.cf_name.as_ref().unwrap().as_bytes();
            buf.extend_from_slice(&(name_bytes.len() as u32).to_le_bytes());
            buf.extend_from_slice(name_bytes);
        } else if edit.is_cf_drop {
            // ---- column family drop ----
            buf.push(TAG_CF_DROP);
            buf.extend_from_slice(&edit.cf_id.to_le_bytes());
        } else {
            buf.push(TAG_CF_ID);
            buf.extend_from_slice(&edit.cf_id.to_le_bytes());
        }

        // tag-based encoding（像 protobuf，但手写）
        for (level, f) in &edit.add_files {
            buf.push(TAG_ADD_FILE); // ADD_FILE
            buf.push(*level as u8);

            buf.extend_from_slice(&f.file_number.to_le_bytes());
            buf.extend_from_slice(&f.file_size.to_le_bytes());

            buf.extend_from_slice(&(f.smallest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&f.smallest_key);

            buf.extend_from_slice(&(f.largest_key.len() as u32).to_le_bytes());
            buf.extend_from_slice(&f.largest_key);
        }

        for (level, file_no) in &edit.delete_files {
            buf.push(TAG_DELETE_FILE); // DELETE_FILE
            buf.push(*level as u8);
            buf.extend_from_slice(&file_no.to_le_bytes());
        }

        if let Some(n) = edit.next_file_number {
            buf.push(TAG_NEXT_FILE_NUMBER); // NEXT_FILE_NUMBER
            buf.extend_from_slice(&n.to_le_bytes());
        }

        if let Some(seq) = edit.last_sequence {
            buf.push(TAG_LAST_SEQUENCE); // LAST_SEQUENCE
            buf.extend_from_slice(&seq.to_le_bytes());
        }

        buf
    }

    pub fn decode_version_edit(buf: &[u8]) -> Result<VersionEdit, DBError> {
        let mut pos = 0;
        let mut edit = VersionEdit::default();

        while pos < buf.len() {
            let tag = buf[pos];
            pos += 1;

            match tag {
                TAG_CF_ADD => {
                    let cf_id = read_u32(buf, &mut pos)?;
                    let name = read_string(buf, &mut pos)?;
                    edit.cf_id = cf_id;
                    edit.cf_name = Some(name);
                    edit.is_cf_add = true;
                }

                TAG_CF_DROP => {
                    let cf_id = read_u32(buf, &mut pos)?;
                    edit.cf_id = cf_id;
                    edit.is_cf_drop = true;
                }

                TAG_CF_ID => {
                    let cf = read_u32(buf, &mut pos)?;
                    edit.cf_id = cf;
                }

                TAG_ADD_FILE => {
                    let level = buf[pos] as usize;
                    pos += 1;

                    let file_number = read_u64(buf, &mut pos)?;
                    let file_size = read_u64(buf, &mut pos)?;

                    let smallest_key = read_bytes(buf, &mut pos)?;
                    let largest_key = read_bytes(buf, &mut pos)?;

                    edit.add_files.push((
                        level,
                        FileMetaData {
                            file_number,
                            file_size,
                            smallest_key,
                            largest_key,
                        },
                    ));
                }

                TAG_DELETE_FILE => {
                    let level = buf[pos] as usize;
                    pos += 1;

                    let file_number = read_u64(buf, &mut pos)?;
                    edit.delete_files.push((level, file_number));
                }

                TAG_NEXT_FILE_NUMBER => {
                    let n = read_u64(buf, &mut pos)?;
                    edit.next_file_number = Some(n);
                }

                TAG_LAST_SEQUENCE => {
                    let seq = read_u64(buf, &mut pos)?;
                    edit.last_sequence = Some(seq);
                }

                _ => {
                    return Err(DBError::Corruption(format!(
                        "unknown VersionEdit tag {}",
                        tag
                    )));
                }
            }
        }

        Ok(edit)
    }
}

