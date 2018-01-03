use generic::EdgeTrait;
use generic::NodeTrait;
use generic::{Iter, IndexIter};

pub trait GraphType {
    fn is_directed() -> bool;
}


pub struct Directed();

pub struct Undirected();

impl GraphType for Directed {
    fn is_directed() -> bool {
        true
    }
}

impl GraphType for Undirected {
    fn is_directed() -> bool {
        false
    }
}

pub trait GraphTrait<L>
{
    type N: NodeTrait;
    type E: EdgeTrait;

    fn add_node(&mut self, id: usize, label: Option<L>);
    fn get_node(&self, id: usize) -> Option<&Self::N>;
    fn get_node_mut(&mut self, id: usize) -> Option<&mut Self::N>;
    fn remove_node(&mut self, id: usize) -> Option<Self::N>;

    fn add_edge(&mut self, start: usize, target: usize, label: Option<L>);
    //fn get_edge(&self, id: usize) -> Option<&Self::E>;
    //fn get_edge_mut(&mut self, id: usize) -> Option<&mut Self::E>;
    //fn find_edge_id(&self, start: usize, target: usize) -> Option<usize>;
    fn find_edge(&self, start: usize, target: usize) -> Option<&Self::E>;
    fn find_edge_mut(&mut self, start: usize, target: usize) -> Option<&mut Self::E>;
    fn remove_edge(&mut self, start: usize, target: usize) -> Option<Self::E>;

    fn has_node(&self, id: usize) -> bool;
    fn has_edge(&self, start: usize, target: usize) -> bool;

    fn node_count(&self) -> usize;
    fn edge_count(&self) -> usize;

    fn is_directed(&self) -> bool;

    //fn node_indices<'a>(&'a self) -> IndexIter<'a>;
    //fn edge_indices<'a>(&'a self) -> IndexIter<'a>;
    fn nodes<'a>(&'a self) -> Iter<'a, &Self::N>;
    fn edges<'a>(&'a self) -> Iter<'a, &Self::E>;
    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N>;
    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E>;
}


pub trait UnGraphTrait
{
    fn degree(&self, id: usize) -> usize;

    fn neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a>;
}

pub trait DiGraphTrait
{
    fn in_degree(&self, id: usize) -> usize;
    fn out_degree(&self, id: usize) -> usize;

    fn in_neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a>;
    fn out_neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a>;
}