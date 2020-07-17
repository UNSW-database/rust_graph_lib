/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

use crate::generic::Iter;

pub trait MapTrait<L> {
    fn get_item(&self, id: usize) -> Option<&L>;
    fn find_index(&self, item: &L) -> Option<usize>;

    fn contains(&self, item: &L) -> bool;

    fn items(&self) -> Iter<'_, &L>;
    fn items_vec(self) -> Vec<L>;

    fn len(&self) -> usize;

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait MutMapTrait<L> {
    /// Add a new item to the map and return its index
    fn add_item(&mut self, item: L) -> usize;

    fn pop_item(&mut self) -> Option<L>;
}
