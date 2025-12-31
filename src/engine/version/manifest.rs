use std::fs::{File, OpenOptions};
use std::io::{BufReader, BufWriter};
use std::path::{Path, PathBuf};
use crate::DBError;
use crate::engine::wal::{WalReader, WalWriter};
use crate::engine::version::{read_current, write_current};
use crate::engine::version::ManifestReader;
use crate::engine::version::ManifestWriter;
use crate::engine::version::VersionEdit;

pub struct Manifest {
    dir: PathBuf,
    current_manifest: String,
    writer: ManifestWriter,
}

impl Manifest {
    /// 打开 DB 时调用：
    /// 1. 读取 CURRENT
    /// 2. replay MANIFEST
    /// 3. 返回 Manifest + 所有 VersionEdit
    pub fn open(dir: &Path) -> Result<(Self, Vec<VersionEdit>), DBError> {
        let dir = dir.to_path_buf();

        // 1️⃣ 读取 CURRENT
        let manifest_name = read_current(&dir)?;
        let manifest_path = dir.join(&manifest_name);

        // 2️⃣ replay MANIFEST
        let file = File::open(&manifest_path).map_err(|e| DBError::Io(e))?;
        let reader = WalReader::new(BufReader::new(file));
        let mut mr = ManifestReader::new(reader);

        let mut edits = Vec::new();
        mr.replay(|edit| {
            edits.push(edit);
            Ok(())
        })?;

        // 3️⃣ 打开 writer（append 模式）
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&manifest_path).map_err(|e| DBError::Io(e))?;

        let writer = ManifestWriter::new(WalWriter::new(BufWriter::new(file)));

        Ok((
            Self {
                dir,
                current_manifest: manifest_name,
                writer,
            },
            edits,
        ))
    }

    /// 追加一个 VersionEdit（强 durability）
    pub fn append(&mut self, edit: &VersionEdit) -> Result<(), DBError> {
        self.writer.append_edit(edit)
    }

    /// rotate MANIFEST（通常很少触发）
    pub fn rotate(&mut self) -> Result<(), DBError> {
        // 1️⃣ 生成新 MANIFEST 文件名
        let new_name = format!("MANIFEST-{:06}", rand::random::<u32>());
        let new_path = self.dir.join(&new_name);

        // 2️⃣ 创建新 MANIFEST writer
        let file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&new_path).map_err(|e| DBError::Io(e))?;

        let new_writer = ManifestWriter::new(WalWriter::new(BufWriter::new(file)));

        // 3️⃣ 切换 CURRENT（原子）
        write_current(&self.dir, &new_name)?;

        // 4️⃣ 更新内存状态
        self.writer = new_writer;
        self.current_manifest = new_name;

        Ok(())
    }
}
