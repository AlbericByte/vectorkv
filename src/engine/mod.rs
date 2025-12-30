pub mod mem;

pub(crate) mod wal;
pub(crate) mod version;
pub(crate) mod background;
pub(crate) mod sst;

pub fn init_engine() {
    println!("Engine initialized");
}