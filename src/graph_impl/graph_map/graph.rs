use std::hash::Hash;

use std::collections::HashMap;
use std::marker::PhantomData;


use generic::{GraphTrait, DiGraphTrait, UnGraphTrait, MutGraphTrait};
use generic::{NodeTrait, EdgeTrait};
use generic::MapTrait;
use generic::{Iter, IndexIter};
use generic::GraphType;
use generic::{Directed, Undirected};


use graph_impl::graph_map::NodeMap;
use graph_impl::graph_map::Edge;
use graph_impl::graph_map::LabelMap;

use graph_impl::graph_map::node::NodeMapTrait;

/// A graph data structure that nodes and edges are stored in map.
pub struct GraphMap<L, Ty: GraphType> {
    /// A map <node_id:node>.
    nodes: HashMap<usize, NodeMap>,
    /// A map <(start,target):edge>.
    edges: HashMap<(usize, usize), Edge>,
    /// A map of node labels.
    node_labels: LabelMap<L>,
    /// A map of edge labels.
    edge_labels: LabelMap<L>,
    /// A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
}

/// Shortcut of creating a new directed graph where `L` is the data type of labels.
/// # Example
/// ```
/// use rust_graph::DiGraphMap;
/// let  g = DiGraphMap::<&str>::new();
/// ```
pub type DiGraphMap<L> = GraphMap<L, Directed>;

/// Shortcut of creating a new undirected graph where `L` is the data type of labels.
/// # Example
/// ```
/// use rust_graph::UnGraphMap;
/// let  g = UnGraphMap::<&str>::new();
/// ```
pub type UnGraphMap<L> = GraphMap<L, Undirected>;

impl<L, Ty: GraphType> GraphMap<L, Ty> {
    /// Constructs a new graph.
    pub fn new() -> Self {
        GraphMap {
            nodes: HashMap::<usize, NodeMap>::new(),
            edges: HashMap::<(usize, usize), Edge>::new(),
            node_labels: LabelMap::<L>::new(),
            edge_labels: LabelMap::<L>::new(),
            graph_type: PhantomData,
        }
    }
}

impl<L: Hash + Eq + Clone, Ty: GraphType> Clone for GraphMap<L, Ty> {
    fn clone(&self) -> Self {
        GraphMap {
            nodes: self.nodes.clone(),
            edges: self.edges.clone(),
            node_labels: self.node_labels.clone(),
            edge_labels: self.edge_labels.clone(),
            graph_type: PhantomData,
        }
    }
}

impl<L, Ty: GraphType> GraphMap<L, Ty> {
    pub fn get_node_label_map(&self) -> &LabelMap<L> {
        &self.node_labels
    }

    pub fn get_edge_label_map(&self) -> &LabelMap<L> {
        &self.edge_labels
    }
}

impl<L: Hash + Eq, Ty: GraphType> GraphMap<L, Ty> {
    fn swap_edge(&self, start: usize, target: usize) -> (usize, usize) {
        if !self.is_directed() && start > target {
            return (target, start);
        }
        (start, target)
    }
}

impl<L: Hash + Eq, Ty: GraphType> MutGraphTrait<L> for GraphMap<L, Ty> {
    type N = NodeMap;
    type E = Edge;

    fn add_node(&mut self, id: usize, label: Option<L>) {
        if self.has_node(id) {
            panic!("Node {} already exist.", id);
        }

        let label_id = label.map(|x| self.node_labels.add_item(x));

        let new_node = NodeMap::new(id, label_id);
        self.nodes.insert(id, new_node);
    }

    fn get_node_mut(&mut self, id: usize) -> Option<&mut Self::N> {
        self.nodes.get_mut(&id)
    }

    fn remove_node(&mut self, id: usize) -> Option<Self::N> {
        if !self.has_node(id) {
            return None;
        }

        let node = self.nodes.remove(&id).unwrap();

        if self.is_directed() {
            for neighbor in node.neighbors() {
                self.get_node_mut(*neighbor).unwrap().remove_in_edge(id);
                self.edges.remove(&(id, *neighbor));
            }
            for in_neighbor in node.in_neighbors() {
                self.edges.remove(&(*in_neighbor, id));
            }
        } else {
            for neighbor in node.neighbors() {
                let edge_id = self.swap_edge(id, *neighbor);

                self.get_node_mut(*neighbor).unwrap().remove_edge(id);
                self.edges.remove(&edge_id);
            }
        }

        Some(node)
    }

    fn add_edge(&mut self, start: usize, target: usize, label: Option<L>) {
        if !self.has_node(start) {
            panic!("The node with id {} has not been created yet.", start);
        }
        if !self.has_node(target) {
            panic!("The node with id {} has not been created yet.", target);
        }


        let (start, target) = self.swap_edge(start, target);


        if self.has_edge(start, target) {
            panic!("Edge ({},{}) already exist.", start, target)
        }

        self.get_node_mut(start).unwrap().add_edge(target);

        if self.is_directed() {
            self.get_node_mut(target).unwrap().add_in_edge(start);
        } else {
            self.get_node_mut(target).unwrap().add_edge(start);
        }

        let label_id = label.map(|x| self.edge_labels.add_item(x));

        let new_edge = Edge::new(start, target, label_id);
        self.edges.insert((start, target), new_edge);
    }

    fn find_edge_mut(&mut self, start: usize, target: usize) -> Option<&mut Self::E> {
        let edge_id = self.swap_edge(start, target);
        self.edges.get_mut(&edge_id)
    }

    fn remove_edge(&mut self, start: usize, target: usize) -> Option<Self::E> {
        if !self.has_edge(start, target) {
            return None;
        }

        let (start, target) = self.swap_edge(start, target);

        self.get_node_mut(start).unwrap().remove_edge(target);
        if self.is_directed() {
            self.get_node_mut(target).unwrap().remove_in_edge(start);
        } else {
            self.get_node_mut(target).unwrap().remove_edge(start);
        }
        self.edges.remove(&(start, target))
    }

    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N> {
        Iter::new(Box::new(self.nodes.values_mut()))
    }

    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E> {
        Iter::new(Box::new(self.edges.values_mut()))
    }
}

impl<L: Hash + Eq, Ty: GraphType> GraphTrait<L> for GraphMap<L, Ty>
{
    type N = NodeMap;
    type E = Edge;

    fn get_node(&self, id: usize) -> Option<&Self::N> {
        self.nodes.get(&id)
    }

    fn find_edge(&self, start: usize, target: usize) -> Option<&Self::E> {
        let edge_id = self.swap_edge(start, target);
        self.edges.get(&edge_id)
    }

    fn has_node(&self, id: usize) -> bool {
        self.nodes.contains_key(&id)
    }

    fn has_edge(&self, start: usize, target: usize) -> bool {
        let edge_id = self.swap_edge(start, target);
        self.edges.contains_key(&edge_id)
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
        IndexIter::new(Box::new(self.nodes.keys()))
    }

    fn edge_indices<'a>(&'a self) -> Iter<'a, &(usize, usize)> {
        Iter::new(Box::new(self.edges.keys()))
    }

    fn nodes<'a>(&'a self) -> Iter<'a, &Self::N> {
        Iter::new(Box::new(self.nodes.values()))
    }

    fn edges<'a>(&'a self) -> Iter<'a, &Self::E> {
        Iter::new(Box::new(self.edges.values()))
    }

    fn degree(&self, id: usize) -> usize {
        match self.get_node(id) {
            Some(ref node) => node.degree(),
            None => panic!("Node {} do not exist.", id)
        }
    }

    fn neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a> {
        match self.get_node(id) {
            Some(ref node) => node.neighbors(),
            None => panic!("Node {} do not exist.", id)
        }
    }

    fn node_labels<'a>(&'a self) -> Iter<'a, &L> {
        self.node_labels.items()
    }

    fn edge_labels<'a>(&'a self) -> Iter<'a, &L> {
        self.edge_labels.items()
    }

    fn get_node_label(&self, node_id: usize) -> Option<&L> {
        match self.get_node(node_id) {
            Some(ref node) => {
                match node.get_label() {
                    Some(label_id) => self.node_labels.find_item(label_id),
                    None => None,
                }
            }
            None => panic!("Node {} do not exist.", node_id)
        }
    }

    fn get_edge_label(&self, start: usize, target: usize) -> Option<&L> {
        match self.find_edge(start, target) {
            Some(ref edge) => {
                match edge.get_label() {
                    Some(label_id) => self.edge_labels.find_item(label_id),
                    None => None,
                }
            }
            None => panic!("Edge ({},{}) do not exist.", start, target)
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

    fn in_neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a> {
        match self.get_node(id) {
            Some(ref node) => node.in_neighbors(),
            None => panic!("Node {} do not exist.", id)
        }
    }
}

