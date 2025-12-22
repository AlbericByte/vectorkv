use vectorkv::engine;
use vectorkv::network;
use vectorkv::db;
use vectorkv::error;

use vectorkv::engine::mem::{InternalKey, SkipListMemTable, ValueType, MemTable};

fn main() {
    engine::init_engine();

    let mut mem :SkipListMemTable = SkipListMemTable::new();

    mem.add(
        InternalKey { user_key: b"key1".to_vec(), seq: 1, value_type: ValueType::Put },
        b"value1".to_vec(),
    );

    if let Some(v) = mem.get(b"key1") {
        println!("Got value: {:?}", String::from_utf8(v).unwrap());
    }

    mem.mark_immutable();
    println!("Is immutable? {}", mem.is_immutable());
}
