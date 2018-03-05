/// Implementations of id-item mapping table that
/// maps arbitrary data to `usize` integer.

use std::hash::Hash;
//use std::fmt::{Debug, Formatter, Error};

use ordermap::OrderSet;

use generic::{MapTrait, MutMapTrait};
use generic::Iter;

/// More efficient but less compact.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct SetMap<L: Hash + Eq> {
    labels: OrderSet<L>,
}

impl<L: Hash + Eq> SetMap<L> {
    pub fn new() -> Self {
        SetMap {
            labels: OrderSet::<L>::new(),
        }
    }
}

//impl<L: Hash + Eq + Debug> Debug for SetMap<L> {
//    fn fmt(&self, f: &mut Formatter) -> Result<(), Error> {
//        write!(f, "{:?}", self.labels)
//    }
//}
//
//impl<L: Hash + Eq + Clone> Clone for SetMap<L> {
//    fn clone(&self) -> Self {
//        SetMap {
//            labels: self.labels.clone(),
//        }
//    }
//}

impl<L: Hash + Eq> MapTrait<L> for SetMap<L> {
    /// *O(1)*
    fn find_index(&self, item: &L) -> Option<usize> {
        match self.labels.get_full(item) {
            Some((i, _)) => Some(i),
            None => None,
        }
    }

    /// *O(1)*
    fn find_item(&self, id: usize) -> Option<&L> {
        self.labels.get_index(id)
    }

    /// *O(1)*
    fn contains(&self, item: &L) -> bool {
        self.labels.contains(item)
    }

    fn items<'a>(&'a self) -> Iter<'a, &L> {
        Iter::new(Box::new(self.labels.iter()))
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

/// Less efficient but more compact.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct VecMap<L> {
    labels: Vec<L>,
}

impl<L> VecMap<L> {
    pub fn new() -> Self {
        VecMap {
            labels: Vec::<L>::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        VecMap {
            labels: Vec::<L>::with_capacity(capacity),
        }
    }

    pub fn with_data(labels: Vec<L>) -> Self {
        VecMap { labels }
    }

    pub fn shrink_to_fit(&mut self) {
        self.labels.shrink_to_fit();
    }
}

impl<L: Eq> MapTrait<L> for VecMap<L> {
    /// *O(1)*
    fn find_item(&self, id: usize) -> Option<&L> {
        self.labels.get(id)
    }

    /// *O(n)*
    fn find_index(&self, item: &L) -> Option<usize> {
        for (i, elem) in self.labels.iter().enumerate() {
            if elem == item {
                return Some(i);
            }
        }
        None
    }

    /// *O(n)*
    fn contains(&self, item: &L) -> bool {
        self.find_index(item).is_some()
    }

    fn items(&self) -> Iter<&L> {
        Iter::new(Box::new(self.labels.iter()))
    }

    /// *O(1)*
    fn len(&self) -> usize {
        self.labels.len()
    }
}

impl<L: Eq> MutMapTrait<L> for VecMap<L> {
    /// *O(n)*
    fn add_item(&mut self, item: L) -> usize {
        match self.find_index(&item) {
            Some(i) => i,
            None => {
                self.labels.push(item);
                self.len() - 1
            }
        }
    }
}
