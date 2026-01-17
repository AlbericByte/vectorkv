use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter, Write};
use std::path::{Path, PathBuf};
use crate::DBError;
use crate::engine::version::VersionEdit;
use crate::engine::wal::{WalReader, WalWriter};


const MANIFEST_MAGIC: u32 = 0xF1F2_F3F4;

pub struct ManifestWriter {
    path: PathBuf,
    writer: WalWriter<BufWriter<File>>,
}

impl ManifestWriter {
    /// Create a brand new manifest file on first DB startup.
    pub fn create_new(path: &PathBuf) -> Result<Self, DBError> {
        use std::fs::{File, OpenOptions};
        use std::io::Write;
        use std::path::Path;

        // Ensure the directory exists
        if let Some(dir) = path.as_path().parent() {
            std::fs::create_dir_all(dir).map_err(|e| DBError::Io(e))?;
        }

        // Create or truncate the manifest file
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(path)
            .map_err(|e| DBError::Io(e))?;

        let mut buf = BufWriter::new(file);

        // Write initial header or placeholder if needed
        // (RocksDB manifest starts empty, but we can include a format version header)
        writeln!(file, "manifest_format_version 1")
            .map_err(|e| DBError::Io(e))?;

        buf.flush().map_err(|e| DBError::Io(e))?;

        let wal = WalWriter::new(buf);
        // Return the ManifestWriter instance
        Ok(Self {
            path: PathBuf::from(path),
            writer: wal,
        })
    }

    /// Open an existing manifest file (without truncating history) and wrap it
    /// for future VersionEdit appends.
    pub fn open_existing(path: &str) -> Result<Self, DBError> {

        let path_buf = Path::new(path).to_path_buf();

        // Open the file without truncating existing content
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(false) // must already exist
            .open(&path_buf)
            .map_err(|e| DBError::Io(e.to_string()))?;

        // Wrap the file in a buffered writer
        let buf_writer = BufWriter::new(file);

        // Wrap the buffered writer with WalWriter (assuming you have WalWriter::new)
        let wal_writer = WalWriter::new(buf_writer);

        Ok(Self {
            path: path_buf,
            writer: wal_writer,
        })
    }

    /// 追加一条 VersionEdit 记录到 MANIFEST
    pub fn add_record(&mut self, edit: &VersionEdit) -> Result<(), DBError> {
        let payload = VersionEdit::encode_version_edit(edit);
        self.writer
            .append(&payload)
            .map_err(DBError::Io)?;
        // 是否 fsync 取决于你对元数据持久化的要求
        // self.writer.into_inner().flush()? 之类的可以在 WalWriter 里提供 flush/sync
        self.writer.flush().map_err(DBError::Io)?;
        Ok(())
    }

    /// 回放所有 VersionEdit（用于 DB 启动时重建 VersionSet）
    ///
    /// `apply`：对每一条 edit 调用一次
    pub fn replay<F>(&mut self, mut apply: F) -> Result<(), DBError>
    where
        F: FnMut(VersionEdit) -> Result<(), DBError>,
    {
        let f = OpenOptions::new()
            .read(true)
            .open(&self.path)
            .map_err(DBError::Io)?;

        let mut reader = WalReader::new(BufReader::new(f));

        loop {
            let rec = reader.next_record().map_err(|e| DBError::Corruption(e.to_string()))?;
            match rec {
                None => break,
                Some(bytes) => {
                    let edit = VersionEdit::decode_version_edit(&bytes)?;
                    apply(edit)?;
                }
            }
        }

        Ok(())
    }
}
