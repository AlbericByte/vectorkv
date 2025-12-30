use std::convert::AsRef;
use std::fs::{File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Seek, SeekFrom};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Condvar, Mutex};
use std::time::Duration;
use crate::{DBError, DB};
use crate::engine::wal::WriteBatch;
use crate::engine::mem::SequenceNumber;
use crate::engine::wal::{WalWriter, WalReader, encode_write_batch, decode_write_batch};

pub struct WalManager {
    path: PathBuf,

    // 长期持有 writer（只允许一个线程进入写临界区）
    writer: Mutex<WalWriter<BufWriter<File>>>,

    // 已写但未 fsync 覆盖到的最大 seq（单调递增）
    pending_seq: AtomicU64,

    // 已 fsync 覆盖到的最大 seq（单调递增）
    synced_seq: AtomicU64,

    // 等待 fsync 完成
    sync_mu: Mutex<()>,
    sync_cv: Condvar,
}

impl WalManager {
    pub fn open<P: AsRef<Path>>(path: P) -> Result<Arc<Self>, DBError> {
        let path = path.as_ref().to_path_buf();

        // 追加打开（不存在则创建）
        let f = OpenOptions::new().create(true).append(true).open(&path).map_err(DBError::Io)?;
        let writer = WalWriter::new(BufWriter::new(f));

        let mgr = Arc::new(Self {
            path,
            writer: Mutex::new(writer),
            pending_seq: AtomicU64::new(0),
            synced_seq: AtomicU64::new(0),
            sync_mu: Mutex::new(()),
            sync_cv: Condvar::new(),
        });

        // 启动唯一 sync 线程
        WalManager::start_sync_thread(Arc::clone(&mgr));

        Ok(mgr)
    }

    fn open_reader(&self) -> io::Result<WalReader<BufReader<File>>> {
        let mut f = OpenOptions::new().read(true).open(&self.path)?;
        f.seek(SeekFrom::Start(0))?;
        Ok(WalReader::new(BufReader::new(f)))
    }

    fn start_sync_thread(this: Arc<Self>) {
        std::thread::spawn(move || loop {
            // 你可以改成更精细：Condvar + notify 唤醒；这里先用短 sleep 简化
            std::thread::sleep(Duration::from_millis(1));

            let pending = this.pending_seq.load(Ordering::Acquire);
            let synced = this.synced_seq.load(Ordering::Acquire);

            if pending > synced {
                // 1) 确保 BufWriter 的数据都进内核（这里在写线程里已 flush，但再 flush 一次更稳）
                if let Ok(mut w) = this.writer.lock() {
                    let _ = w.flush(); // 需要 WalWriter::flush()，见下方说明
                }

                // 2) fsync（真正的 durable）
                if let Ok(f) = OpenOptions::new().write(true).open(&this.path) {
                    let _ = f.sync_all();
                }

                // 3) 更新 synced_seq（唤醒等待者）
                this.synced_seq.store(pending, Ordering::Release);
                this.sync_cv.notify_all();
            }
        });
    }

    pub fn append_sync(&self, base_seq: SequenceNumber, batch: &WriteBatch) -> Result<(),DBError> {
        if batch.is_empty() {
            return Ok(());
        }

        let payload = encode_write_batch(base_seq, batch);
        let end_seq = base_seq + (batch.len() as u64) - 1;

        // 1) WAL append + flush（进入内核 page cache）
        {
            let mut w = self.writer.lock().unwrap();
            w.append(&payload).map_err(DBError::Io)?;
            w.flush().map_err(DBError::Io)?;
        }

        // 2) 发布 pending_seq（用 max，保证单调递增）
        self.publish_pending(end_seq);

        // 3) 等待 sync 线程把 synced_seq 推进到 >= end_seq
        let mut g = self.sync_mu.lock().unwrap();
        while self.synced_seq.load(Ordering::Acquire) < end_seq {
            g = self.sync_cv.wait(g).unwrap();
        }

        Ok(())
    }

    /// 非强一致：只写 + flush，不等 fsync（crash 可能丢最后一小段）
    pub fn append_no_sync(&self, base_seq: SequenceNumber, batch: &WriteBatch) -> Result<(), DBError> {
        if batch.is_empty() {
            return Ok(());
        }
        let payload = encode_write_batch(base_seq, batch);
        let end_seq = base_seq + (batch.len() as u64) - 1;

        {
            let mut w = self.writer.lock().unwrap();
            w.append(&payload).map_err(DBError::Io)?;
            w.flush().map_err(DBError::Io)?;
        }

        self.publish_pending(end_seq);
        Ok(())
    }

    #[inline]
    fn publish_pending(&self, end_seq: u64) {
        // pending_seq = max(pending_seq, end_seq)
        let mut cur = self.pending_seq.load(Ordering::Relaxed);
        while cur < end_seq {
            match self.pending_seq.compare_exchange_weak(
                cur,
                end_seq,
                Ordering::Release,
                Ordering::Relaxed,
            ) {
                Ok(_) => break,
                Err(v) => cur = v,
            }
        }
    }

    pub fn replay<F>(&self, mut f: F) -> Result<(),DBError>
    where
        F: FnMut(Vec<u8>) -> Result<(), DBError>,
    {
        let mut r = self.open_reader().map_err(DBError::Io)?;
        while let Some(payload) = r.
            next_record().
            map_err(|e| DBError::Corruption(format!("{:?}", e)))? {
            f(payload)?;
        }
        Ok(())
    }

    pub fn replay_batches<F>(&self, mut apply: F) -> Result<(), DBError>
    where
        F: FnMut(SequenceNumber, WriteBatch) -> Result<(), DBError>,
    {
        self.replay(|payload| {
            let (base_seq, batch) = decode_write_batch(&payload)?;
            apply(base_seq, batch)
        })
    }
}
