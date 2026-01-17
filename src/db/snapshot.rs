use crate::db::db_impl::DBImpl;

#[derive(Clone)]
pub struct Snapshot {
    pub seq: u64,
}

impl DBImpl {
    fn get_snapshot(&self) -> Snapshot {
        Snapshot {
            seq: self.versions.lock().unwrap().latest_sequence(),
        }
    }

    fn release_snapshot(&self, _snapshot: Snapshot) {
        // Rust 自动 drop，通常只做引用计数回收
    }
}
