use generic::IndexIter;

pub trait NodeTrait {
    fn get_id(&self) -> usize;
    fn set_label(&mut self, label: usize);
    fn get_label(&self) -> Option<usize>;

//    fn add_edge(&mut self, adj: usize, edge: usize);
//    fn get_edge(&self, adj: usize) -> Option<usize>;
//    fn remove_edge(&mut self, adj: usize) -> Option<usize>;
//
//    fn degree(&self) -> usize;
//
//    fn neighbors<'a>(&'a self) -> IndexIter<'a>;
//    fn edges<'a>(&'a self) -> IndexIter<'a>;
}