use std::collections::HashMap;


use generic::GraphTrait;
use generic::EdgeTrait;
use generic::NodeTrait;

use implementation::graph_map::Node;
use implementation::graph_map::Edge;
use implementation::graph_map::LabelMap;

struct GraphMap<L> {
    nodes: HashMap<usize, Node>,
    edges: HashMap<usize, Edge>,
    labels: LabelMap<L>,
    new_edge_id: usize,
    is_directed: bool,
}

impl<L> GraphMap<L> {
    pub fn new(is_directed: bool) -> Self {
        GraphMap {
            nodes: HashMap::<usize, Node>::new(),
            edges: HashMap::<usize, Edge>::new(),
            labels: LabelMap::<L>::new(),
            new_edge_id: 0,
            is_directed,
        }
    }
}


impl<L> GraphTrait<L> for GraphMap<L>
{
    type N = Node;
    type E = Edge;

    fn add_node(&mut self, id: usize, label: Option<L>) -> Option<&Self::N> {
        unimplemented!()
    }

    fn get_node(&self, id: usize) -> Option<&Self::N> {
        self.nodes.get(&id)
    }

    fn get_node_mut(&mut self, id: usize) -> Option<&mut Self::N> {
        self.nodes.get_mut(&id)
    }

    fn remove_node(&mut self, id: usize) -> Option<Self::N> {
        unimplemented!()
    }

    fn add_edge(&self, start: usize, target: usize, label: Option<L>) -> Option<&Self::E> {
        unimplemented!()
    }

    fn get_edge(&self, id: usize) -> Option<&Self::E> {
        unimplemented!()
    }

    fn get_edge_mut(&mut self, id: usize) -> Option<&mut Self::E> {
        unimplemented!()
    }

    fn find_edge(&self, start: usize, target: usize) -> Option<&Self::E> {
        unimplemented!()
    }

    fn find_edge_mut(&mut self, start: usize, target: usize) -> Option<&mut Self::E> {
        unimplemented!()
    }

    fn remove_edge(&mut self, index: usize) -> Option<Self::E> {
        unimplemented!()
    }

    fn node_count(&self) -> usize {
        unimplemented!()
    }

    fn edge_count(&self) -> usize {
        unimplemented!()
    }

    fn degree(&self, node: usize) -> usize {
        unimplemented!()
    }

    fn is_directed(&self) -> bool {
        unimplemented!()
    }

    fn get_label(&self, id: usize) -> Option<&L> {
        unimplemented!()
    }
}