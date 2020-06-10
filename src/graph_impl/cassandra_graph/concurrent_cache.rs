use crate::generic::IdType;
use hashbrown::HashMap;
use itertools::Itertools;
use lru::LruCache;
use parking_lot::Mutex;
use std::sync::atomic::{AtomicUsize, Ordering};

pub struct ConcurrentCache<I: IdType> {
    page_num: usize,
    page_size: usize,
    pages: Vec<Mutex<LruCache<I, Vec<I>>>>,
    hits: AtomicUsize,
    misses: AtomicUsize,
}

impl<I: IdType> ConcurrentCache<I> {
    pub fn new(page_num: usize, page_size: usize) -> Self {
        assert!(page_num > 0, "Must have at least one page");
        let mut pages = Vec::with_capacity(page_num);

        for _ in 0..page_num {
            pages.push(Mutex::new(LruCache::new(page_size)));
        }

        Self {
            page_num,
            page_size,
            pages,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    pub fn from_hashmap(
        page_num: usize,
        page_size: usize,
        original_hashmap: HashMap<I, Vec<I>>,
    ) -> Self {
        assert!(page_num > 0, "Must have at least one page");
        let mut caches = Vec::with_capacity(page_num);

        for _ in 0..page_num {
            caches.push(LruCache::new(page_size));
        }

        for (key, val) in original_hashmap {
            let page_id = key.id() % page_num;
            caches[page_id].put(key, val);
        }

        let pages = caches
            .into_iter()
            .map(|cache| Mutex::new(cache))
            .collect_vec();

        Self {
            page_num,
            page_size,
            pages,
            hits: AtomicUsize::new(0),
            misses: AtomicUsize::new(0),
        }
    }

    #[inline(always)]
    pub fn put(&self, key: I, val: Vec<I>) {
        let page_id = key.id() % self.page_num;
        let page = self.pages.get(page_id).expect("Page not found.");
        let mut page = page.lock();
        page.put(key, val);
    }

    #[inline(always)]
    pub fn contains(&self, key: &I) -> bool {
        let page_id = key.id() % self.page_num;
        let page = self.pages.get(page_id).expect("Page not found");
        let page = page.lock();

        page.contains(key)
    }

    #[inline(always)]
    pub fn get(&self, key: &I) -> Option<Vec<I>> {
        let page_id = key.id() % self.page_num;
        let page = self.pages.get(page_id).expect("Page not found");
        let mut page = page.lock();
        if let Some(value) = page.get(key) {
            self.hits.fetch_add(1, Ordering::SeqCst);
            Some(value.clone())
        } else {
            self.misses.fetch_add(1, Ordering::SeqCst);
            None
        }
    }

    #[inline(always)]
    pub fn degree(&self, key: &I) -> Option<usize> {
        let page_id = key.id() % self.page_num;
        let page = self.pages.get(page_id).expect("Page not found");
        let mut page = page.lock();
        if let Some(value) = page.get(key) {
            self.hits.fetch_add(1, Ordering::SeqCst);
            Some(value.len())
        } else {
            self.misses.fetch_add(1, Ordering::SeqCst);
            None
        }
    }

    #[inline(always)]
    pub fn has_edge(&self, src: &I, dst: &I) -> Option<bool> {
        let page_id = src.id() % self.page_num;
        let page = self.pages.get(page_id).expect("Page not found");
        let mut page = page.lock();
        if let Some(value) = page.get(src) {
            self.hits.fetch_add(1, Ordering::SeqCst);
            Some(value.binary_search(dst).is_ok())
        } else {
            self.misses.fetch_add(1, Ordering::SeqCst);
            None
        }
    }

    pub fn get_hits(&self) -> usize {
        self.hits.load(Ordering::SeqCst)
    }

    pub fn get_misses(&self) -> usize {
        self.misses.load(Ordering::SeqCst)
    }

    pub fn get_capacity(&self) -> usize {
        self.page_size * self.page_num
    }

    pub fn get_len(&self) -> usize {
        let mut length = 0usize;
        for page in &self.pages {
            let page = page.lock();
            length += page.len();
        }
        length
    }
}
