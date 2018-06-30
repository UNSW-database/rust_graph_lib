use generic::Iter;
/// Implementations of id-item mapping table that
/// maps arbitrary data to `usize` integer.
use generic::{MapTrait, MutMapTrait};

/// Less efficient but more compact.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
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

    pub fn clear(&mut self) {
        self.labels.clear();
    }
}

impl<L> Default for VecMap<L> {
    fn default() -> Self {
        VecMap::new()
    }
}

impl<L: Eq> MapTrait<L> for VecMap<L> {
    /// *O(1)*
    fn get_item(&self, id: usize) -> Option<&L> {
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

    fn items_vec(self) -> Vec<L> {
        self.labels
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

#[macro_export]
macro_rules! vecmap {
    ( $( $x:expr ),* ) => {
        {
            let mut temp_map = VecMap::new();
            $(
                temp_map.add_item($x);
            )*
            temp_map
        }
    };
}
