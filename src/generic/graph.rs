use std::borrow::Cow;

use generic::IdType;
use generic::{IndexIter, Iter};

pub trait MutGraphTrait<L> {
    /// Associated node type
    type N;

    /// Associated edge type
    type E;

    /// Add a new node with specific id and label.
    /// *NOTE*: The label will be converted to an `usize` integer.
    fn add_node(&mut self, id: usize, label: Option<L>) -> bool;

    /// Get a mutable reference to the node.
    fn get_node_mut(&mut self, id: usize) -> Option<&mut Self::N>;

    /// Remove the node and return it.
    fn remove_node(&mut self, id: usize) -> Option<Self::N>;

    /// Add a new edge (`start`,`target)` with a specific label.
    /// *NOTE*: The label will be converted to an `usize` integer.
    fn add_edge(&mut self, start: usize, target: usize, label: Option<L>) -> bool;

    /// Get a mutable reference to the edge.
    fn get_edge_mut(&mut self, start: usize, target: usize) -> Option<&mut Self::E>;

    /// Remove the edge (`start`,`target)` and return it.
    fn remove_edge(&mut self, start: usize, target: usize) -> Option<Self::E>;

    /// Return an iterator of all nodes(mutable) in the graph.
    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N>;

    /// Return an iterator over all edges(mutable) in the graph.
    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E>;
}

pub trait GraphTrait<Id: IdType> {
    /// Associated node type
    type N;

    /// Associated edge type
    type E;

    /// Get an immutable reference to the node.
    fn get_node(&self, id: usize) -> Option<&Self::N>;

    /// Get an immutable reference to the edge.
    fn get_edge(&self, start: usize, target: usize) -> Option<&Self::E>;

    /// Check if the node is in the graph.
    fn has_node(&self, id: usize) -> bool;

    /// Check if the edge is in the graph.
    fn has_edge(&self, start: usize, target: usize) -> bool;

    /// Return the number of nodes in the graph.
    fn node_count(&self) -> usize;

    /// Return the number of edges in the graph.
    fn edge_count(&self) -> usize;

    /// Whether if the graph is directed or not.
    fn is_directed(&self) -> bool;

    /// Return an iterator over the node indices of the graph.
    fn node_indices(&self) -> IndexIter;

    /// Return an iterator over the edge indices of the graph.
    fn edge_indices(&self) -> Iter<(usize, usize)>;

    /// Return an iterator of all nodes in the graph.
    fn nodes<'a>(&'a self) -> Iter<'a, &Self::N>;

    /// Return an iterator over all edges in the graph.
    fn edges<'a>(&'a self) -> Iter<'a, &Self::E>;

    /// Return the degree of a node.
    fn degree(&self, id: usize) -> usize;

    /// Return an iterator over the indices of all nodes adjacent to a given node.
    fn neighbors_iter(&self, id: usize) -> IndexIter;

    /// Return the indices(either owned or borrowed) of all nodes adjacent to a given node.
    fn neighbors(&self, id: usize) -> Cow<[Id]>;

    /// Lookup the node label id by its id.
    fn get_node_label_id(&self, node_id: usize) -> Option<usize>;

    /// Lookup the edge label id by its id.
    fn get_edge_label_id(&self, start: usize, target: usize) -> Option<usize>;

    /// Return the maximum id the graph can represent.
    fn max_possible_id(&self) -> usize;
}

pub trait GraphLabelTrait<L> {
    /// Return an iterator over the set of all node labels.
    fn node_labels<'a>(&'a self) -> Iter<'a, &L>;

    /// Return an iterator over the set of all edge labels.
    fn edge_labels<'a>(&'a self) -> Iter<'a, &L>;

    /// Lookup the node label by its id.
    fn get_node_label(&self, node_id: usize) -> Option<&L>;

    /// Lookup the edge label by its id.
    fn get_edge_label(&self, start: usize, target: usize) -> Option<&L>;

    /// Update the node label.
    fn update_node_label(&mut self, node_id: usize, label: Option<L>) -> bool;

    /// Update the edge label.
    fn update_edge_label(&mut self, start: usize, target: usize, label: Option<L>) -> bool;
}

/// Trait for undirected graphs.
pub trait UnGraphTrait {}

/// Trait for directed graphs.
pub trait DiGraphTrait<Id: IdType> {
    /// Return the in-degree of a node.
    fn in_degree(&self, id: usize) -> usize;

    /// Return an iterator over the indices of all nodes with a edge from a given node.
    fn in_neighbors_iter(&self, id: usize) -> IndexIter;

    /// Return the indices(either owned or borrowed) of all nodes with a edge from a given node.
    fn in_neighbors(&self, id: usize) -> Cow<[Id]>;
}
