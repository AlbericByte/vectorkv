use super::memtable::*;
use super::skiplist::*;
use std::sync::atomic::Ordering as AtomicOrdering;

fn vec_from_str(s: &str) -> Vec<u8> {
    s.as_bytes().to_vec()
}

#[test]
fn test_add_and_get_basic() {
    let mut table = SkipListMemTable::new();

    table.add(InternalKey::from_slice(&10i32.to_le_bytes()), vec_from_str("a"));
    table.add(InternalKey::from_slice(&20i32.to_le_bytes()), vec_from_str("b"));
    table.add(InternalKey::from_slice(&15i32.to_le_bytes()), vec_from_str("c"));

    assert_eq!(table.get(&10i32.to_le_bytes()), Some(vec_from_str("a")));
    assert_eq!(table.get(&15i32.to_le_bytes()), Some(vec_from_str("c")));
    assert_eq!(table.get(&20i32.to_le_bytes()), Some(vec_from_str("b")));
    assert_eq!(table.get(&999i32.to_le_bytes()), None);
}

#[test]
fn test_add_duplicates() {
    let mut table = SkipListMemTable::new();

    table.add(InternalKey::from_slice(&5i32.to_le_bytes()), vec_from_str("first"));
    table.add(InternalKey::from_slice(&5i32.to_le_bytes()), vec_from_str("second"));

    // 目前 SkipListMemTable 允许重复 key
    // search 应该返回第一个插入的值
    assert_eq!(table.get(&5i32.to_le_bytes()), Some(vec_from_str("second")));
}

#[test]
fn test_empty_table_search() {
    let table = SkipListMemTable::new();

    assert_eq!(table.get(&1i32.to_le_bytes()), None);
}

#[test]
fn test_monotonic_order() {
    let mut table = SkipListMemTable::new();

    for i in 0..100 {
        table.add(InternalKey::from_slice(&(i as i32).to_le_bytes()), ((i as i32)* 10).to_le_bytes().to_vec());
    }

    // 遍历底层 skiplist 链
    let mut x = unsafe {
        table
            .skiplist
            .head
            .as_ref()
            .and_then(|h| h.next[0].load(AtomicOrdering::SeqCst).as_ref())
    };
    let mut expected = 0;

    while let Some(node) = x {
        let key_i32 = i32::from_le_bytes(node.key.user_key[0..4].try_into().unwrap());
        assert_eq!(key_i32, expected);

        let value_i32 = i32::from_le_bytes(node.value[0..4].try_into().unwrap());
        assert_eq!(value_i32, expected * 10);

        expected += 1;
        x = unsafe { node.next[0].load(AtomicOrdering::SeqCst).as_ref() };
    }

    assert_eq!(expected, 100);
}
