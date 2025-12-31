use std::fs::{self, File};
use std::io::{Read, Write};
use std::path::Path;

use crate::DBError;

const CURRENT_FILE: &str = "CURRENT";
const CURRENT_TMP_FILE: &str = "CURRENT.tmp";

/// 读取 CURRENT，返回 MANIFEST 文件名
pub fn read_current(db_dir: &Path) -> Result<String, DBError> {
    let path = db_dir.join(CURRENT_FILE);
    let mut file = File::open(&path)
        .map_err(DBError::Io)?;

    let mut buf = String::new();
    file.read_to_string(&mut buf)
        .map_err(DBError::Io)?;

    let name = buf.as_str().trim_end_matches('\n').to_string();

    if name.is_empty() {
        return Err(DBError::Corruption("CURRENT is empty".to_string()));
    }

    Ok(name)
}

/// 原子性写 CURRENT
pub fn write_current(db_dir: &Path, manifest_name: &str) -> Result<(), DBError> {
    let tmp_path = db_dir.join(CURRENT_TMP_FILE);
    let final_path = db_dir.join(CURRENT_FILE);

    {
        let mut file = File::create(&tmp_path)
            .map_err(DBError::Io)?;

        file.write_all(manifest_name.as_bytes())
            .map_err(DBError::Io)?;
        file.write_all(b"\n")
            .map_err(DBError::Io)?;

        file.sync_all()
            .map_err(DBError::Io)?;
    }

    // 原子替换
    fs::rename(&tmp_path, &final_path)
        .map_err(DBError::Io)?;

    Ok(())
}
