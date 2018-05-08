use std::hash::Hash;
use std::collections::HashMap;
use std::marker::PhantomData;
use std::borrow::Cow;

use generic::{DefaultId, IdType};
use generic::{DiGraphTrait, GraphLabelTrait, GraphTrait, MutGraphTrait};
use generic::{EdgeTrait, MutEdgeTrait, MutNodeTrait, NodeTrait};
use generic::{MapTrait, MutMapTrait};
use generic::{IndexIter, Iter};
use generic::GraphType;
use generic::{Directed, Undirected};

use graph_impl::graph_map::NodeMap;
use graph_impl::graph_map::Edge;
use graph_impl::graph_map::node::MutNodeMapTrait;

use map::SetMap;

pub type GraphMap<L, Ty> = TypedGraphMap<DefaultId, L, Ty>;
pub type TypedDiGraphMap<Id, L> = TypedGraphMap<Id, L, Directed>;
pub type TypedUnGraphMap<Id, L> = TypedGraphMap<Id, L, Undirected>;

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
/// let g = UnGraphMap::<&str>::new();
/// ```
pub type UnGraphMap<L> = GraphMap<L, Undirected>;

/// A graph data structure that nodes and edges are stored in map.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedGraphMap<Id: IdType, L: Hash + Eq, Ty: GraphType> {
    /// A map <node_id:node>.
    node_map: HashMap<Id, NodeMap<Id>>,
    /// A map <(start,target):edge>.
    edge_map: HashMap<(Id, Id), Edge<Id>>,
    /// A map of node labels.
    node_label_map: SetMap<L>,
    /// A map of edge labels.
    edge_label_map: SetMap<L>,
    /// A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
}

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> TypedGraphMap<Id, L, Ty> {
    /// Constructs a new graph.
    pub fn new() -> Self {
        TypedGraphMap {
            node_map: HashMap::<Id, NodeMap<Id>>::new(),
            edge_map: HashMap::<(Id, Id), Edge<Id>>::new(),
            node_label_map: SetMap::<L>::new(),
            edge_label_map: SetMap::<L>::new(),
            graph_type: PhantomData,
        }
    }

    pub fn with_capacity(
        nodes: usize,
        edge: usize,
        node_labels: usize,
        edge_labels: usize,
    ) -> Self {
        TypedGraphMap {
            node_map: HashMap::<Id, NodeMap<Id>>::with_capacity(nodes),
            edge_map: HashMap::<(Id, Id), Edge<Id>>::with_capacity(edge),
            node_label_map: SetMap::<L>::with_capacity(node_labels),
            edge_label_map: SetMap::<L>::with_capacity(edge_labels),
            graph_type: PhantomData,
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.node_map.shrink_to_fit();
        self.edge_map.shrink_to_fit();
    }

    /// Constructs a new graph using existing label-id mapping.
    /// # Example
    /// ```
    /// use rust_graph::prelude::*;
    /// use rust_graph::UnGraphMap;
    ///
    /// let mut g = UnGraphMap::<&str>::new();
    /// g.add_node(0, Some("a"));
    /// g.add_node(1, Some("b"));
    /// g.add_edge(0, 1, None);
    ///
    /// let mut p = UnGraphMap::with_label_map(g.get_node_label_map().clone(),
    ///                                                g.get_edge_label_map().clone());
    /// p.add_node(1, Some("b"));
    /// p.add_node(0, Some("a"));
    /// p.add_edge(0, 1, None);
    ///
    /// assert_eq!(g.get_node(0).unwrap().get_label_id(), p.get_node(0).unwrap().get_label_id());
    /// assert_eq!(g.get_node(1).unwrap().get_label_id(), p.get_node(1).unwrap().get_label_id());
    ///
    /// ```
    pub fn with_label_map(node_label_map: SetMap<L>, edge_label_map: SetMap<L>) -> Self {
        TypedGraphMap {
            node_map: HashMap::<Id, NodeMap<Id>>::new(),
            edge_map: HashMap::<(Id, Id), Edge<Id>>::new(),
            node_label_map,
            edge_label_map,
            graph_type: PhantomData,
        }
    }

    pub fn from_edges(mut edges: Vec<(usize, usize)>) -> Self {
        let mut g = TypedGraphMap::new();
        for (src, dst) in edges.drain(..) {
            g.add_node(src, None);
            g.add_node(dst, None);
            g.add_edge(src, dst, None);
        }
        g
    }
}

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> Default for TypedGraphMap<Id, L, Ty> {
    fn default() -> Self {
        Self::new()
    }
}

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> TypedGraphMap<Id, L, Ty> {
    pub fn get_node_label_map(&self) -> &SetMap<L> {
        &self.node_label_map
    }

    pub fn get_edge_label_map(&self) -> &SetMap<L> {
        &self.edge_label_map
    }
}

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> TypedGraphMap<Id, L, Ty> {
    fn swap_edge(&self, start: usize, target: usize) -> (usize, usize) {
        if !self.is_directed() && start > target {
            return (target, start);
        }
        (start, target)
    }
}

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> MutGraphTrait<L> for TypedGraphMap<Id, L, Ty> {
    type N = NodeMap<Id>;
    type E = Edge<Id>;

    fn add_node(&mut self, id: usize, label: Option<L>) -> bool {
        if self.has_node(id) {
            return false;
        }
        let label_id = label.map(|x| self.node_label_map.add_item(x));
        let new_node = NodeMap::new(id, label_id);
        self.node_map.insert(Id::new(id), new_node);

        true
    }

    fn get_node_mut(&mut self, id: usize) -> Option<&mut Self::N> {
        self.node_map.get_mut(&Id::new(id))
    }

    fn remove_node(&mut self, id: usize) -> Option<Self::N> {
        if !self.has_node(id) {
            return None;
        }

        let node = self.node_map.remove(&Id::new(id)).unwrap();

        if self.is_directed() {
            for neighbor in node.neighbors_iter() {
                self.get_node_mut(neighbor).unwrap().remove_in_edge(id);
                self.edge_map.remove(&(Id::new(id), Id::new(neighbor)));
            }
            for in_neighbor in node.in_neighbors_iter() {
                self.edge_map.remove(&(Id::new(in_neighbor), Id::new(id)));
            }
        } else {
            for neighbor in node.neighbors_iter() {
                let (s, d) = self.swap_edge(id, neighbor);

                self.get_node_mut(neighbor).unwrap().remove_edge(id);
                self.edge_map.remove(&(Id::new(s), Id::new(d)));
            }
        }

        Some(node)
    }

    fn add_edge(&mut self, start: usize, target: usize, label: Option<L>) -> bool {
        let (start, target) = self.swap_edge(start, target);

        if self.has_edge(start, target) {
            return false;
        }

        if !self.has_node(start) {
            panic!("The node with id {} has not been created yet.", start);
        }
        if !self.has_node(target) {
            panic!("The node with id {} has not been created yet.", target);
        }

        let label_id = label.map(|x| self.edge_label_map.add_item(x));

        self.get_node_mut(start).unwrap().add_edge(target);

        if self.is_directed() || start == target {
            self.get_node_mut(target).unwrap().add_in_edge(start);
        } else {
            self.get_node_mut(target).unwrap().add_edge(start);
        }

        let new_edge = Edge::new(start, target, label_id);
        self.edge_map
            .insert((Id::new(start), Id::new(target)), new_edge);

        true
    }

    fn get_edge_mut(&mut self, start: usize, target: usize) -> Option<&mut Self::E> {
        let (s, d) = self.swap_edge(start, target);
        self.edge_map.get_mut(&(Id::new(s), Id::new(d)))
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
        self.edge_map.remove(&(Id::new(start), Id::new(target)))
    }

    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N> {
        Iter::new(Box::new(self.node_map.values_mut()))
    }

    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E> {
        Iter::new(Box::new(self.edge_map.values_mut()))
    }
}

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> GraphTrait<Id> for TypedGraphMap<Id, L, Ty> {
    type N = NodeMap<Id>;
    type E = Edge<Id>;

    fn get_node(&self, id: usize) -> Option<&Self::N> {
        self.node_map.get(&Id::new(id))
    }

    fn get_edge(&self, start: usize, target: usize) -> Option<&Self::E> {
        let (s, d) = self.swap_edge(start, target);
        self.edge_map.get(&(Id::new(s), Id::new(d)))
    }

    fn has_node(&self, id: usize) -> bool {
        self.node_map.contains_key(&Id::new(id))
    }

    fn has_edge(&self, start: usize, target: usize) -> bool {
        let (s, d) = self.swap_edge(start, target);
        self.edge_map.contains_key(&(Id::new(s), Id::new(d)))
    }

    fn node_count(&self) -> usize {
        self.node_map.len()
    }

    fn edge_count(&self) -> usize {
        self.edge_map.len()
    }

    fn is_directed(&self) -> bool {
        Ty::is_directed()
    }

    fn node_indices(&self) -> IndexIter {
        IndexIter::new(Box::new(self.node_map.keys().map(|i| i.id())))
    }

    fn edge_indices(&self) -> Iter<(usize, usize)> {
        Iter::new(Box::new(
            self.edge_map.keys().map(|&(ref s, ref d)| (s.id(), d.id())),
        ))
    }

    fn nodes<'a>(&'a self) -> Iter<'a, &Self::N> {
        Iter::new(Box::new(self.node_map.values()))
    }

    fn edges<'a>(&'a self) -> Iter<'a, &Self::E> {
        Iter::new(Box::new(self.edge_map.values()))
    }

    fn degree(&self, id: usize) -> usize {
        match self.get_node(id) {
            Some(ref node) => node.degree(),
            None => panic!("Node {} do not exist.", id),
        }
    }

    fn neighbors_iter(&self, id: usize) -> IndexIter {
        match self.get_node(id) {
            Some(ref node) => node.neighbors_iter(),
            None => panic!("Node {} do not exist.", id),
        }
    }

    fn neighbors(&self, id: usize) -> Cow<[Id]> {
        match self.get_node(id) {
            Some(ref node) => Cow::from(node.neighbors()),
            None => panic!("Node {} do not exist.", id),
        }
    }

    fn get_node_label_id(&self, node_id: usize) -> Option<usize> {
        match self.get_node(node_id) {
            Some(ref node) => node.get_label_id(),
            None => panic!("Node {} do not exist.", node_id),
        }
    }

    fn get_edge_label_id(&self, start: usize, target: usize) -> Option<usize> {
        match self.get_edge(start, target) {
            Some(ref edge) => edge.get_label_id(),
            None => panic!("Edge ({},{}) do not exist.", start, target),
        }
    }

    fn max_possible_id(&self) -> usize {
        Id::max_usize()
    }
}

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> GraphLabelTrait<L> for TypedGraphMap<Id, L, Ty> {
    fn node_labels<'a>(&'a self) -> Iter<'a, &L> {
        self.node_label_map.items()
    }

    fn edge_labels<'a>(&'a self) -> Iter<'a, &L> {
        self.edge_label_map.items()
    }

    fn get_node_label(&self, node_id: usize) -> Option<&L> {
        match self.get_node_label_id(node_id) {
            Some(label_id) => self.node_label_map.get_item(label_id),
            None => None,
        }
    }

    fn get_edge_label(&self, start: usize, target: usize) -> Option<&L> {
        match self.get_edge_label_id(start, target) {
            Some(label_id) => self.edge_label_map.get_item(label_id),
            None => None,
        }
    }

    fn update_node_label(&mut self, node_id: usize, label: Option<L>) -> bool {
        if !self.has_node(node_id) {
            return false;
        }

        let label_id = label.map(|x| self.node_label_map.add_item(x));

        self.get_node_mut(node_id).unwrap().set_label_id(label_id);

        true
    }

    fn update_edge_label(&mut self, start: usize, target: usize, label: Option<L>) -> bool {
        if !self.has_edge(start, target) {
            return false;
        }

        let label_id = label.map(|x| self.edge_label_map.add_item(x));

        self.get_edge_mut(start, target)
            .unwrap()
            .set_label_id(label_id);

        true
    }
}

impl<Id: IdType, L: Hash + Eq> DiGraphTrait<Id> for TypedDiGraphMap<Id, L> {
    fn in_degree(&self, id: usize) -> usize {
        match self.get_node(id) {
            Some(ref node) => node.in_degree(),
            None => panic!("Node {} do not exist.", id),
        }
    }

    fn in_neighbors_iter(&self, id: usize) -> IndexIter {
        match self.get_node(id) {
            Some(ref node) => node.in_neighbors_iter(),
            None => panic!("Node {} do not exist.", id),
        }
    }

    fn in_neighbors(&self, id: usize) -> Cow<[Id]> {
        match self.get_node(id) {
            Some(ref node) => Cow::from(node.in_neighbors()),
            None => panic!("Node {} do not exist.", id),
        }
    }
}

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> Drop for TypedGraphMap<Id, L, Ty> {
    fn drop(&mut self) {
        self.edge_map.clear();
        self.edge_label_map.clear();
        self.node_map.clear();
        self.node_label_map.clear();
    }
}
