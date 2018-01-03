use std::hash::Hash;

use std::collections::HashMap;
use std::marker::PhantomData;


use generic::{GraphTrait, DiGraphTrait, UnGraphTrait};
use generic::EdgeTrait;
use generic::NodeTrait;
use generic::MapTrait;
use generic::{Iter, IndexIter};
use generic::GraphType;
use generic::{Directed, Undirected};

use implementation::graph_map::Node;
use implementation::graph_map::Edge;
use implementation::graph_map::LabelMap;


pub struct GraphMap<L, Ty: GraphType> {
    nodes: HashMap<usize, Node>,
    edges: HashMap<usize, Edge>,
    node_labels: LabelMap<L>,
    edge_labels: LabelMap<L>,
    new_edge_id: usize,
    graph_type: PhantomData<Ty>,
}

impl<L, Ty: GraphType> GraphMap<L, Ty> {
    pub fn new() -> Self {
        GraphMap {
            nodes: HashMap::<usize, Node>::new(),
            edges: HashMap::<usize, Edge>::new(),
            node_labels: LabelMap::<L>::new(),
            edge_labels: LabelMap::<L>::new(),
            new_edge_id: 0,
            graph_type: PhantomData,
        }
    }
}

pub type DiGraphMap<L> = GraphMap<L, Directed>;
pub type UnGraphMap<L> = GraphMap<L, Undirected>;


impl<L: Hash + Eq, Ty: GraphType> GraphTrait<L> for GraphMap<L, Ty>
{
    type N = Node;
    type E = Edge;

    fn add_node(&mut self, id: usize, label: Option<L>) {
        if self.has_node(id) {
            panic!("Node {} already exist.", id);
        }

        let label_id = label.map(|x| self.node_labels.add_item(x));

        let new_node = Node::new(id, label_id);
        self.nodes.insert(id, new_node);
    }

    fn get_node(&self, id: usize) -> Option<&Self::N> {
        self.nodes.get(&id)
    }

    fn get_node_mut(&mut self, id: usize) -> Option<&mut Self::N> {
        self.nodes.get_mut(&id)
    }

    fn remove_node(&mut self, id: usize) -> Option<Self::N> {
        if !self.has_node(id) {
            return None;
        }

        let node = self.nodes.remove(&id).unwrap();

        for out_edge in node.out_edges() {
            self.edges.remove(&out_edge);
        }

        if self.is_directed() {
            for out_neighbor in node.out_neighbors() {
                self.get_node_mut(out_neighbor).unwrap().remove_in_edge(id);
            }
            for in_edge in node.in_edges() {
                self.edges.remove(&in_edge);
            }
        } else {
            for out_neighbor in node.out_neighbors() {
                self.get_node_mut(out_neighbor).unwrap().remove_out_edge(id);
            }
        }

        Some(node)
    }

    fn add_edge(&mut self, start: usize, target: usize, label: Option<L>) -> usize {
        if !self.has_node(start) {
            panic!("The node with id {} has not been created yet.", start);
        }
        if !self.has_node(target) {
            panic!("The node with id {} has not been created yet.", target);
        }
        if self.find_edge_id(start, target).is_some() {
            panic!("Edge ({},{}) already exist.", start, target)
        }

        let edge_id = self.new_edge_id;

        self.get_node_mut(start).unwrap().add_out_edge(target, edge_id);

        if self.is_directed() {
            self.get_node_mut(target).unwrap().add_in_edge(start, edge_id);
        } else {
            self.get_node_mut(target).unwrap().add_out_edge(start, edge_id);
        }

        let label_id = label.map(|x| self.edge_labels.add_item(x));

        let new_edge = Edge::new(edge_id, start, target, label_id);
        self.edges.insert(edge_id, new_edge);

        self.new_edge_id += 1;
        edge_id
    }

    fn get_edge(&self, id: usize) -> Option<&Self::E> {
        self.edges.get(&id)
    }

    fn get_edge_mut(&mut self, id: usize) -> Option<&mut Self::E> {
        self.edges.get_mut(&id)
    }


    fn find_edge_id(&self, start: usize, target: usize) -> Option<usize> {
        if !self.has_node(start) {
            return None;
        }

        self.get_node(start).unwrap().get_out_edge(target)
    }


    fn find_edge(&self, start: usize, target: usize) -> Option<&Self::E> {
        match self.find_edge_id(start, target) {
            Some(id) => self.get_edge(id),
            None => None
        }
    }

    fn find_edge_mut(&mut self, start: usize, target: usize) -> Option<&mut Self::E> {
        match self.find_edge_id(start, target) {
            Some(id) => self.get_edge_mut(id),
            None => None
        }
    }

    fn remove_edge(&mut self, start: usize, target: usize) -> Option<Self::E> {
        match self.find_edge_id(start, target) {
            Some(edge_id) => {
                self.get_node_mut(start).unwrap().remove_out_edge(target);
                if self.is_directed() {
                    self.get_node_mut(target).unwrap().remove_in_edge(start);
                } else {
                    self.get_node_mut(target).unwrap().remove_out_edge(start);
                }
                self.edges.remove(&edge_id)
            }
            None => None
        }
    }

    fn has_node(&self, id: usize) -> bool {
        self.nodes.contains_key(&id)
    }

    fn has_edge(&self, start: usize, target: usize) -> bool {
        self.find_edge_id(start, target).is_some()
    }

    fn node_count(&self) -> usize {
        self.nodes.len()
    }

    fn edge_count(&self) -> usize {
        self.edges.len()
    }

    fn is_directed(&self) -> bool {
        Ty::is_directed()
    }

    fn node_indices<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.nodes.keys().map(|i| { *i })))
    }

    fn edge_indices<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.edges.keys().map(|i| { *i })))
    }

    fn nodes<'a>(&'a self) -> Iter<'a, &Self::N> {
        Iter::new(Box::new(self.nodes.values()))
    }

    fn edges<'a>(&'a self) -> Iter<'a, &Self::E> {
        Iter::new(Box::new(self.edges.values()))
    }

    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N> {
        Iter::new(Box::new(self.nodes.values_mut()))
    }

    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E> {
        Iter::new(Box::new(self.edges.values_mut()))
    }
}

impl<L: Hash + Eq> UnGraphTrait for UnGraphMap<L> {
    fn degree(&self, id: usize) -> usize {
        match self.get_node(id) {
            Some(ref node) => node.out_degree(),
            None => panic!("Node {} do not exist.", id)
        }
    }

    fn neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a> {
        match self.get_node(id) {
            Some(ref node) => node.out_neighbors(),
            None => panic!("Node {} do not exist.", id)
        }
    }
}


impl<L: Hash + Eq> DiGraphTrait for DiGraphMap<L> {
    fn in_degree(&self, id: usize) -> usize {
        match self.get_node(id) {
            Some(ref node) => node.in_degree(),
            None => panic!("Node {} do not exist.", id)
        }
    }

    fn out_degree(&self, id: usize) -> usize {
        match self.get_node(id) {
            Some(ref node) => node.out_degree(),
            None => panic!("Node {} do not exist.", id)
        }
    }

    fn in_neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a> {
        match self.get_node(id) {
            Some(ref node) => node.in_neighbors(),
            None => panic!("Node {} do not exist.", id)
        }
    }

    fn out_neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a> {
        match self.get_node(id) {
            Some(ref node) => node.out_neighbors(),
            None => panic!("Node {} do not exist.", id)
        }
    }
}

