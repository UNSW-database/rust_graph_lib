use generic::Iter;

pub trait MapTrait<L> {
    fn add_item(&mut self, item: L) -> usize;
    fn find_item(&self, id: usize) -> Option<&L>;
    fn find_index(&self, item: L) -> Option<usize>;

    fn contains(&self, item: L) -> bool;

    fn items(&self) -> Iter<&L>;
    fn len(&self) -> usize;
}