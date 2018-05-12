use std::hash::Hash;

use indexmap::IndexSet;

use generic::Iter;
use generic::{MapTrait, MutMapTrait};

/// More efficient but less compact.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct SetMap<L: Hash + Eq> {
    labels: IndexSet<L>,
}

impl<L: Hash + Eq> SetMap<L> {
    pub fn new() -> Self {
        SetMap {
            labels: IndexSet::<L>::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        SetMap {
            labels: IndexSet::<L>::with_capacity(capacity),
        }
    }

    pub fn with_data(data: IndexSet<L>) -> Self {
        SetMap { labels: data }
    }

    pub fn from_vec(vec: Vec<L>) -> Self {
        let indexset: IndexSet<_> = vec.into_iter().collect();
        SetMap::with_data(indexset)
    }

    pub fn clear(&mut self) {
        self.labels.clear();
    }
}

impl<L: Hash + Eq> Default for SetMap<L> {
    fn default() -> Self {
        SetMap::new()
    }
}

impl<L: Hash + Eq> MapTrait<L> for SetMap<L> {
    /// *O(1)*
    fn get_item(&self, id: usize) -> Option<&L> {
        self.labels.get_index(id)
    }

    /// *O(1)*
    fn find_index(&self, item: &L) -> Option<usize> {
        match self.labels.get_full(item) {
            Some((i, _)) => Some(i),
            None => None,
        }
    }

    /// *O(1)*
    fn contains(&self, item: &L) -> bool {
        self.labels.contains(item)
    }

    fn items<'a>(&'a self) -> Iter<'a, &L> {
        Iter::new(Box::new(self.labels.iter()))
    }

    fn items_vec(self) -> Vec<L> {
        self.labels.into_iter().collect::<Vec<_>>()
    }

    /// *O(1)*
    fn len(&self) -> usize {
        self.labels.len()
    }
}

impl<L: Hash + Eq> MutMapTrait<L> for SetMap<L> {
    /// *O(1)*
    fn add_item(&mut self, item: L) -> usize {
        if self.labels.contains(&item) {
            self.labels.get_full(&item).unwrap().0
        } else {
            self.labels.insert(item);
            self.len() - 1
        }
    }
}
