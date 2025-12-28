#[cfg(test)]
mod tests {
    use super::*;
    use std::cmp::Ordering;
    use crate::engine::mem::skiplist::{Arena, Node, SkipList, MAX_HEIGHT};

    // ---------- helpers ----------
    fn cmp_u64(a: &u64, b: &u64) -> Ordering {
        a.cmp(b)
    }

    // ---------- Arena / Node ----------
    #[test]
    fn test_arena_alloc_node_stable_address_and_writable() {
        let arena = Arena::new();

        let p1 = arena.alloc_node(Node::<u64, u64>::new_dummy(MAX_HEIGHT));
        let p2 = arena.alloc_node(Node::<u64, u64>::new_dummy(MAX_HEIGHT));
        assert!(!p1.is_null());
        assert!(!p2.is_null());
        assert_ne!(p1, p2, "Arena should return different addresses for different nodes");

        // Mutate the node through the raw pointer to ensure it's writable and stable.
        unsafe {
            (*p1).key = 123;
            (*p1).value = 456;
            assert_eq!((*p1).key, 123);
            assert_eq!((*p1).value, 456);
        }
    }

    // ---------- SkipList: basic insert/search ----------
    #[test]
    fn test_skiplist_insert_and_search_basic() {
        let arena = Arena::new();

        // Always visible: exact key equality
        let is_visible = |a: &u64, b: &u64| a == b;

        let mut sl: SkipList<u64, Vec<u8>, _, _> = SkipList::new(arena, cmp_u64, is_visible);

        sl.insert(10, b"a".to_vec());
        sl.insert(20, b"b".to_vec());
        sl.insert(15, b"c".to_vec());

        assert_eq!(sl.search(&10).map(|v| v.as_slice()), Some(b"a".as_slice()));
        assert_eq!(sl.search(&15).map(|v| v.as_slice()), Some(b"c".as_slice()));
        assert_eq!(sl.search(&20).map(|v| v.as_slice()), Some(b"b".as_slice()));
        assert_eq!(sl.search(&999).is_some(), false);
    }

    // ---------- SkipList: unordered inserts ----------
    #[test]
    fn test_skiplist_unordered_inserts_search_all() {
        let arena = Arena::new();
        let is_visible = |a: &u64, b: &u64| a == b;

        let mut sl: SkipList<u64, u64, _, _> = SkipList::new(arena, cmp_u64, is_visible);

        // Insert in descending order
        for i in (0..100u64).rev() {
            sl.insert(i, i * 10);
        }

        for i in 0..100u64 {
            assert_eq!(sl.search(&i).copied(), Some(i * 10));
        }
        assert_eq!(sl.search(&200), None);
    }

    // ---------- SkipList: duplicates ----------
    #[test]
    fn test_skiplist_duplicate_keys_last_write_wins_with_visibility_rule() {
        let arena = Arena::new();

        // If keys are equal (same u64), visible.
        // With duplicates, search() returns the first node at level0 that satisfies is_visible.
        // Given insert algorithm breaks on Equal, the new node is inserted BEFORE the first equal,
        // so search should see the latest inserted value for the same key.
        let is_visible = |a: &u64, b: &u64| a == b;

        let mut sl: SkipList<u64, Vec<u8>, _, _> = SkipList::new(arena, cmp_u64, is_visible);

        sl.insert(5, b"first".to_vec());
        sl.insert(5, b"second".to_vec());

        // Expect "second" (latest) given current insert() behavior.
        assert_eq!(sl.search(&5).map(|v| v.as_slice()), Some(b"second".as_slice()));
    }

    // ---------- SkipList: MVCC-like key (user_key, seq) ----------
    #[derive(Debug, Clone, PartialEq, Eq)]
    struct IK {
        user: u64,
        seq: u64,
    }

    // Order: by user asc, then seq desc (newer first) – typical MVCC internal key ordering
    fn ik_cmp(a: &IK, b: &IK) -> Ordering {
        match a.user.cmp(&b.user) {
            Ordering::Equal => b.seq.cmp(&a.seq),
            other => other,
        }
    }

    #[test]
    fn test_skiplist_visibility_filter_mvcc_snapshot() {
        let arena = Arena::new();

        // Visible if same user key AND candidate.seq <= snapshot.seq
        // Here "key" passed to search is a synthetic IK(user, snapshot_seq)
        let is_visible = |candidate: &IK, seek: &IK| candidate.user == seek.user && candidate.seq <= seek.seq;

        let mut sl: SkipList<IK, Vec<u8>, _, _> = SkipList::new(arena, ik_cmp, is_visible);

        // Insert multiple versions for the same user key
        sl.insert(IK { user: 7, seq: 105 }, b"v105".to_vec());
        sl.insert(IK { user: 7, seq: 103 }, b"v103".to_vec());
        sl.insert(IK { user: 7, seq: 100 }, b"v100".to_vec());

        // Snapshot at 104 should see 103 (since 105 > 104 is not visible)
        let snap_104 = IK { user: 7, seq: 104 };
        assert_eq!(sl.search(&snap_104).map(|v| v.as_slice()), Some(b"v103".as_slice()));

        // Snapshot at 105 sees 105
        let snap_105 = IK { user: 7, seq: 105 };
        assert_eq!(sl.search(&snap_105).map(|v| v.as_slice()), Some(b"v105".as_slice()));

        // Snapshot at 99 sees none
        let snap_99 = IK { user: 7, seq: 99 };
        assert_eq!(sl.search(&snap_99), None);

        // Different user key should not match
        let other_user = IK { user: 8, seq: 200 };
        assert_eq!(sl.search(&other_user), None);
    }

    // ---------- Empty list ----------
    #[test]
    fn test_skiplist_empty_search_none() {
        let arena = Arena::new();
        let is_visible = |a: &u64, b: &u64| a == b;

        let sl: SkipList<u64, u64, _, _> = SkipList::new(arena, cmp_u64, is_visible);
        assert_eq!(sl.search(&1), None);
    }
}
