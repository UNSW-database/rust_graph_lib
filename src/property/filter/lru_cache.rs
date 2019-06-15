#![cfg_attr(feature = "nightly", feature(alloc))]

extern crate hashbrown;
#[cfg(test)]
extern crate scoped_threadpool;

use std::boxed::Box;
use std::hash::{BuildHasher, Hash, Hasher};
use std::iter::FusedIterator;
use std::marker::PhantomData;
use std::mem;
use std::ptr;
use std::usize;
use hashbrown::hash_map::DefaultHashBuilder;
use hashbrown::HashMap;

// Struct used to hold a reference to a key
struct KeyRef<K> {
    k: *const K,
}

impl<K: Hash> Hash for KeyRef<K> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        unsafe { (*self.k).hash(state) }
    }
}

impl<K: PartialEq> PartialEq for KeyRef<K> {
    fn eq(&self, other: &KeyRef<K>) -> bool {
        unsafe { (*self.k).eq(&*other.k) }
    }
}

impl<K: Eq> Eq for KeyRef<K> {}

// Struct used to hold a key value pair. Also contains references to previous and next entries
// so we can maintain the entries in a linked list ordered by their use.
struct LruEntry<K> {
    key: K,
    prev: *mut LruEntry<K>,
    next: *mut LruEntry<K>,
}

impl<K> LruEntry<K> {
    fn new(key: K) -> Self {
        LruEntry {
            key,
            prev: ptr::null_mut(),
            next: ptr::null_mut(),
        }
    }
}

/// An LRU Cache
pub struct LruCache<K, S = DefaultHashBuilder> {
    map: HashMap<KeyRef<K>, Box<LruEntry<K>>, S>,
    cap: usize,

    // head and tail are sigil nodes to faciliate inserting entries
    head: *mut LruEntry<K>,
    tail: *mut LruEntry<K>,
}

impl<K: Hash + Eq> LruCache<K> {

    pub fn new(cap: usize) -> LruCache<K> {
        LruCache::construct(cap, HashMap::with_capacity(cap))
    }

    pub fn unbounded() -> LruCache<K> {
        LruCache::construct(usize::MAX, HashMap::default())
    }
}

impl<K: Hash + Eq, S: BuildHasher> LruCache<K, S> {
    pub fn with_hasher(cap: usize, hash_builder: S) -> LruCache<K, S> {
        LruCache::construct(cap, HashMap::with_capacity_and_hasher(cap, hash_builder))
    }

    /// Creates a new LRU Cache with the given capacity.
    fn construct(cap: usize, map: HashMap<KeyRef<K>, Box<LruEntry<K>>, S>) -> LruCache<K, S> {
        // NB: The compiler warns that cache does not need to be marked as mutable if we
        // declare it as such since we only mutate it inside the unsafe block.
        let cache = LruCache {
            map,
            cap,
            head: unsafe { Box::into_raw(Box::new(mem::uninitialized::<LruEntry<K>>())) },
            tail: unsafe { Box::into_raw(Box::new(mem::uninitialized::<LruEntry<K>>())) },
        };

        unsafe {
            (*cache.head).next = cache.tail;
            (*cache.tail).prev = cache.head;
        }

        cache
    }

    pub fn put(&mut self, k: K) {
        let node_ptr = self.map.get_mut(&KeyRef { k: &k }).map(|node| {
            let node_ptr: *mut LruEntry<K> = &mut **node;
            node_ptr
        });

        match node_ptr {
            Some(node_ptr) => {
                self.detach(node_ptr);
                self.attach(node_ptr);
            }
            None => {
                let mut node = if self.len() == self.cap() {
                    // if the cache is full, remove the last entry so we can use it for the new key
                    let old_key = KeyRef {
                        k: unsafe { &(*(*self.tail).prev).key },
                    };
                    let mut old_node = self.map.remove(&old_key).unwrap();

                    old_node.key = k;

                    let node_ptr: *mut LruEntry<K> = &mut *old_node;
                    self.detach(node_ptr);

                    old_node
                } else {
                    // if the cache is not full allocate a new LruEntry
                    Box::new(LruEntry::new(k))
                };

                let node_ptr: *mut LruEntry<K> = &mut *node;
                self.attach(node_ptr);

                let keyref = unsafe { &(*node_ptr).key };
                self.map.insert(KeyRef { k: keyref }, node);
            }
        }
    }

    pub fn contains(&self, k: &K) -> bool {
        let key = KeyRef { k };
        self.map.contains_key(&key)
    }

    pub fn pop_lru(&mut self) -> Option<(K)> {
        let node = self.remove_last()?;
        // N.B.: Can't destructure directly because of https://github.com/rust-lang/rust/issues/28536
        let node = *node;
        let LruEntry { key, .. } = node;
        Some((key))
    }

    pub fn len(&self) -> usize {
        self.map.len()
    }

    pub fn is_empty(&self) -> bool {
        self.map.len() == 0
    }

    pub fn cap(&self) -> usize {
        self.cap
    }

    pub fn is_full(&self) -> bool {
        self.cap() == self.len()
    }

    pub fn resize(&mut self, cap: usize) {
        // return early if capacity doesn't change
        if cap == self.cap {
            return;
        }

        while self.map.len() > cap {
            self.remove_last();
        }
        self.map.shrink_to_fit();

        self.cap = cap;
    }

    pub fn clear(&mut self) {
        loop {
            match self.remove_last() {
                Some(_) => (),
                None => break,
            }
        }
    }

    pub fn iter<'a>(&'a self) -> Iter<'a, K> {
        Iter {
            len: self.len(),
            ptr: unsafe { (*self.head).next },
            end: unsafe { (*self.tail).prev },
            phantom: PhantomData,
        }
    }

    fn remove_last(&mut self) -> Option<Box<LruEntry<K>>> {
        let prev;
        unsafe { prev = (*self.tail).prev }
        if prev != self.head {
            let old_key = KeyRef {
                k: unsafe { &(*(*self.tail).prev).key },
            };
            let mut old_node = self.map.remove(&old_key).unwrap();
            let node_ptr: *mut LruEntry<K> = &mut *old_node;
            self.detach(node_ptr);
            Some(old_node)
        } else {
            None
        }
    }

    fn detach(&mut self, node: *mut LruEntry<K>) {
        unsafe {
            (*(*node).prev).next = (*node).next;
            (*(*node).next).prev = (*node).prev;
        }
    }

    fn attach(&mut self, node: *mut LruEntry<K>) {
        unsafe {
            (*node).next = (*self.head).next;
            (*node).prev = self.head;
            (*self.head).next = node;
            (*(*node).next).prev = node;
        }
    }
}

impl<K, S> Drop for LruCache<K, S> {
    fn drop(&mut self) {
        // Prevent compiler from trying to drop the un-initialized fields key and val in head
        // and tail
        unsafe {
            let head = *Box::from_raw(self.head);
            let tail = *Box::from_raw(self.tail);

            let LruEntry {
                key: head_key,
                ..
            } = head;
            let LruEntry {
                key: tail_key,
                ..
            } = tail;

            mem::forget(head_key);
            mem::forget(tail_key);
        }
    }
}

impl<'a, K: Hash + Eq, S: BuildHasher> IntoIterator for &'a LruCache<K, S> {
    type Item = (&'a K);
    type IntoIter = Iter<'a, K>;

    fn into_iter(self) -> Iter<'a, K> {
        self.iter()
    }
}

// The compiler does not automatically derive Send and Sync for LruCache because it contains
// raw pointers. The raw pointers are safely encapsulated by LruCache though so we can
// implement Send and Sync for it below.
unsafe impl<K: Send, V: Send> Send for LruCache<K, V> {}
unsafe impl<K: Sync, V: Sync> Sync for LruCache<K, V> {}

/// An iterator over the entries of a `LruCache`.
///
/// This `struct` is created by the [`iter`] method on [`LruCache`][`LruCache`]. See its
/// documentation for more.
///
/// [`iter`]: struct.LruCache.html#method.iter
/// [`LruCache`]: struct.LruCache.html
pub struct Iter<'a, K: 'a> {
    len: usize,

    ptr: *const LruEntry<K>,
    end: *const LruEntry<K>,

    phantom: PhantomData<&'a K>,
}

impl<'a, K> Iterator for Iter<'a, K> {
    type Item = (&'a K);

    fn next(&mut self) -> Option<(&'a K)> {
        if self.len == 0 {
            return None;
        }

        let key = unsafe { &(*self.ptr).key };

        self.len -= 1;
        self.ptr = unsafe { (*self.ptr).next };

        Some((key))
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        (self.len, Some(self.len))
    }

    fn count(self) -> usize {
        self.len
    }
}

impl<'a, K> DoubleEndedIterator for Iter<'a, K> {
    fn next_back(&mut self) -> Option<(&'a K)> {
        if self.len == 0 {
            return None;
        }

        let key = unsafe { &(*self.end).key };

        self.len -= 1;
        self.end = unsafe { (*self.end).prev };

        Some((key))
    }
}

impl<'a, K> ExactSizeIterator for Iter<'a, K> {}
impl<'a, K> FusedIterator for Iter<'a, K> {}

impl<'a, K> Clone for Iter<'a, K> {
    fn clone(&self) -> Iter<'a, K> {
        Iter {
            len: self.len,
            ptr: self.ptr,
            end: self.end,
            phantom: PhantomData,
        }
    }
}

// The compiler does not automatically derive Send and Sync for Iter because it contains
// raw pointers.
unsafe impl<'a, K: Send> Send for Iter<'a, K> {}
unsafe impl<'a, K: Sync> Sync for Iter<'a, K> {}