use std::fs::{File, OpenOptions};
use std::io::{BufReader};
use std::path::{Path, PathBuf};

use crate::DBError;
use crate::engine::version::VersionEdit;
use crate::engine::wal::WalReader;

pub struct ManifestReader {
    path: PathBuf,
    reader: WalReader<BufReader<File>>,
}

impl ManifestReader {

    /// 打开MANIFEST（用于重放）
    pub fn open<P: AsRef<Path>>(db_path: P) -> Result<Self, DBError> {
        let manifest_path = db_path.as_ref().join("MANIFEST");

        let f = OpenOptions::new()
            .read(true)
            .open(&manifest_path)
            .map_err(DBError::Io)?;

        Ok(Self {
            path: manifest_path,
            reader: WalReader::new(BufReader::new(f))
        })
    }

    /// 读取下一条 VersionEdit
    pub fn next_edit(&mut self) -> Result<Option<VersionEdit>, DBError> {
        match self.reader.next_record()
            .map_err(|e| DBError::Corruption(e.to_string()))?
        {
            None => Ok(None),

            Some(bytes) => {
                let edit = VersionEdit::decode_version_edit(&bytes)?;
                Ok(Some(edit))
            }
        }
    }

    /// 一次性 replay 所有 edits
    pub fn replay<F>(&mut self, mut apply: F) -> Result<(), DBError>
    where
        F: FnMut(VersionEdit) -> Result<(), DBError>,
    {
        loop {
            match self.next_edit()? {
                None => break,    // EOF
                Some(edit) => apply(edit)?,
            }
        }
        Ok(())
    }
}
