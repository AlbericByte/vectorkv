use std::cell::UnsafeCell;
use std::mem::MaybeUninit;
use bumpalo::Bump;
use std::sync::atomic::{AtomicPtr, Ordering as AtomicOrdering};
use rand::prelude::*;

pub(crate) const MAX_HEIGHT: usize = 12;
pub(crate) const BRANCHING: f64 = 0.25;

pub struct Arena {
    bump: UnsafeCell<Bump>,
}

impl Arena {
    pub fn new() -> Self {
        Self {
            bump: UnsafeCell::new(Bump::new()),
        }
    }

    /// 在单写线程中分配 Node，返回稳定地址的裸指针
    pub fn alloc_node<K, V>(&self, node: Node<K, V>) -> *mut Node<K, V> {
        let bump = unsafe { &mut *self.bump.get() };
        let r: &mut Node<K, V> = bump.alloc(node);
        r as *mut Node<K, V>
    }
}

unsafe impl Send for Arena {}
unsafe impl Sync for Arena {}

pub struct Node<K, V> {
    pub(crate) key: K,
    pub(crate) value: V,
    pub(crate) next: [AtomicPtr<Node<K, V>>; MAX_HEIGHT], // lock-free forward pointers
    pub(crate) top_level: usize,
}

impl<K:Default, V:Default> Node<K, V> {

    fn new(key: K, value: V, height: usize) -> Self {
        let mut next: [MaybeUninit<AtomicPtr<Node<K, V>>>; MAX_HEIGHT] = unsafe {
            MaybeUninit::uninit().assume_init()
        };
        for slot in &mut next[..] {
            slot.write(AtomicPtr::new(std::ptr::null_mut()));
        }
        let next = unsafe { std::mem::transmute::<_, [AtomicPtr<Node<K, V>>; MAX_HEIGHT]>(next) };
        Node {
            key,
            value,
            next: Default::default(), // AtomicPtr 默认 null
            top_level: height,
        }
    }

    pub fn new_dummy(level: usize) -> Self {

        let mut next: [MaybeUninit<AtomicPtr<Node<K, V>>>; MAX_HEIGHT] = unsafe {
            MaybeUninit::uninit().assume_init()
        };

        for slot in &mut next[..] {
            slot.write(AtomicPtr::new(std::ptr::null_mut()));
        }

        // 转换为初始化好的数组
        let next = unsafe { std::mem::transmute::<_, [AtomicPtr<Node<K, V>>; MAX_HEIGHT]>(next) };

        Node {
            key: K::default(),
            value: V::default(),
            next,
            top_level: MAX_HEIGHT, // head 节点一般设置为最大高度
        }
    }
}

pub struct SkipList<K, V, C, M> {
    pub(crate) head: AtomicPtr<Node<K, V>>,
    max_height: usize,
    comparator: C,
    is_visible: M,
    arena: Arena,
}

impl<K, V, C, M> SkipList<K, V, C, M> {
    pub(crate) fn random_height(&self) -> usize {
        let mut height = 1;
        let mut rng = rand::rng();
        while height < MAX_HEIGHT && rng.random::<f64>() < BRANCHING {
            height += 1;
        }
        height
    }
}

impl<K:Default, V:Default, C, M> SkipList<K, V, C, M>
where
    C: Fn(&K, &K) -> std::cmp::Ordering,
    M: Fn(&K, &K) -> bool,
{
    pub fn new(arena: Arena, comparator: C, is_visible:M) -> Self {
        // 初始化 head 节点，level = MAX_HEIGHT
        let head_node = Node::new_dummy(MAX_HEIGHT); // key/value 空节点
        let head_ptr = AtomicPtr::new(arena.alloc_node(head_node));

        Self {
            head: head_ptr,
            max_height: 1,
            comparator: comparator,
            is_visible: is_visible,
            arena,
        }
    }

    pub(crate) fn insert(&mut self, key: K, value: V) {
        let mut update: [*mut Node<K, V>; MAX_HEIGHT] = [std::ptr::null_mut(); MAX_HEIGHT];
        let mut x = self.head.load(AtomicOrdering::Acquire);

        // 查找每层前驱节点
        for i in (0..self.max_height).rev() {
            unsafe {
                while let Some(next) = (*x).next[i].load(AtomicOrdering::Acquire).as_ref() {
                    if (self.comparator)(&next.key, &key) == std::cmp::Ordering::Less {
                        x = next as *const Node<K, V> as *mut Node<K, V>;
                    } else {
                        break;
                    }
                }
                update[i] = x;
            }
        }

        let node_height = self.random_height();
        if node_height > self.max_height {
            for i in self.max_height..node_height {
                update[i] = self.head.load(AtomicOrdering::Acquire);
            }
            self.max_height = node_height;
        }
        let new_node = self.arena.alloc_node(Node::new(key, value, node_height));

        for i in 0..node_height {
            unsafe {
                let old = (*update[i]).next[i].load(AtomicOrdering::Acquire);
                (*new_node).next[i].store(old, AtomicOrdering::Release);
                (*update[i]).next[i].store(new_node, AtomicOrdering::Release);
            }
        }
    }

    pub(crate) fn search(&self, key: &K) -> Option<&V> {
        let mut x = self.head.load(AtomicOrdering::Acquire);
        unsafe {
            for i in (0..self.max_height).rev() {
                while let Some(next) = (*x).next[i].load(AtomicOrdering::Acquire).as_ref() {
                    match (self.comparator)(&next.key, key) {
                        std::cmp::Ordering::Less => x = next as *const Node<K, V> as *mut Node<K, V>,
                        std::cmp::Ordering::Equal | std::cmp::Ordering::Greater => break,
                    }
                }
            }
            if let Some(next) = (*x).next[0].load(AtomicOrdering::Acquire).as_ref() {
                if (self.is_visible)(&next.key, key)
                {
                    return Some(&next.value);
                }
            }
        }
        None
    }
}

