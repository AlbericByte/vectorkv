use std::ptr::NonNull;
use std::sync::Arc;
use crate::engine::sst::block::BlockCacheKey;

pub struct Node<V> {
    pub(crate) key: BlockCacheKey,
    pub(crate) value: Arc<V>,
    pub(crate) charge: usize,
    pub(crate) prev: Option<NonNull<Node<V>>>,
    pub(crate) next: Option<NonNull<Node<V>>>,
}

/// 侵入式双向链表：Node 自己存 prev/next
pub struct LruList<V> {
    head: Option<NonNull<Node<V>>>,
    tail: Option<NonNull<Node<V>>>,
}

impl<V> LruList<V> {
    pub fn new() -> Self {
        Self { head: None, tail: None }
    }

    pub fn push_front(&mut self, mut node: NonNull<Node<V>>) {
        unsafe {
            node.as_mut().prev = None;
            node.as_mut().next = self.head;
        }

        if let Some(mut h) = self.head {
            unsafe { h.as_mut().prev = Some(node) };
        } else {
            // 空链表：tail 也指向 node
            self.tail = Some(node);
        }
        self.head = Some(node);
    }

    pub fn remove(&mut self, mut node: NonNull<Node<V>>) {
        let (prev, next) = unsafe {
            let n = node.as_ref();
            (n.prev, n.next)
        };

        // 修 prev.next
        if let Some(mut p) = prev {
            unsafe { p.as_mut().next = next };
        } else {
            // node 是 head
            self.head = next;
        }

        // 修 next.prev
        if let Some(mut n) = next {
            unsafe { n.as_mut().prev = prev };
        } else {
            // node 是 tail
            self.tail = prev;
        }

        // 断开
        unsafe {
            node.as_mut().prev = None;
            node.as_mut().next = None;
        }
    }

    pub fn move_to_front(&mut self, node: NonNull<Node<V>>) {
        // 已经在 front
        if self.head == Some(node) {
            return;
        }
        self.remove(node);
        self.push_front(node);
    }

    pub fn back(&self) -> Option<NonNull<Node<V>>> {
        self.tail
    }
}