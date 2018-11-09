use std::borrow::Cow;
use std::hash::Hash;

use counter::Counter;

use generic::{EdgeTrait, EdgeType, IdType, Iter, MapTrait, NodeTrait, NodeType};
use graph_impl::Graph;
use map::SetMap;

pub trait GeneralGraph<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType>:
    GraphTrait<Id, L> + GraphLabelTrait<Id, NL, EL, L>
{
    fn as_graph(&self) -> &GraphTrait<Id, L>;

    fn as_labeled_graph(&self) -> &GraphLabelTrait<Id, NL, EL, L>;

    #[inline(always)]
    fn as_digraph(&self) -> Option<&DiGraphTrait<Id, L>> {
        None
    }
}

pub trait GraphTrait<Id: IdType, L: IdType> {
    /// Get an immutable reference to the node.
    fn get_node(&self, id: Id) -> NodeType<Id, L>;

    /// Get an immutable reference to the edge.
    fn get_edge(&self, start: Id, target: Id) -> EdgeType<Id, L>;

    /// Check if the node is in the graph.
    fn has_node(&self, id: Id) -> bool;

    /// Check if the edge is in the graph.
    fn has_edge(&self, start: Id, target: Id) -> bool;

    /// Return the number of nodes in the graph.
    fn node_count(&self) -> usize;

    /// Return the number of edges in the graph.
    fn edge_count(&self) -> usize;

    /// Whether if the graph is directed or not.
    fn is_directed(&self) -> bool;

    /// Return an iterator over the node indices of the graph.
    fn node_indices(&self) -> Iter<Id>;

    /// Return an iterator over the edge indices of the graph.
    fn edge_indices(&self) -> Iter<(Id, Id)>;

    /// Return an iterator of all nodes in the graph.
    fn nodes(&self) -> Iter<NodeType<Id, L>>;

    /// Return an iterator over all edges in the graph.
    fn edges(&self) -> Iter<EdgeType<Id, L>>;

    /// Return the degree of a node.
    fn degree(&self, id: Id) -> usize;

    /// Return an iterator over the indices of all nodes adjacent to a given node.
    fn neighbors_iter(&self, id: Id) -> Iter<Id>;

    /// Return the indices(either owned or borrowed) of all nodes adjacent to a given node.
    fn neighbors(&self, id: Id) -> Cow<[Id]>;

    /// Return the maximum id has been seen until now.
    fn max_seen_id(&self) -> Option<Id>;

    /// Return the maximum id the graph can represent.
    #[inline(always)]
    fn max_possible_id(&self) -> Id {
        Id::max_value()
    }

    /// Return how the graph structure is implementated, namely, GraphMap or StaticGraph.
    fn implementation(&self) -> Graph;

    fn get_node_label_id_counter(&self) -> Counter<L> {
        self.nodes().filter_map(|n| n.get_label_id()).collect()
    }

    fn get_edge_label_id_counter(&self) -> Counter<L> {
        self.edges().filter_map(|e| e.get_label_id()).collect()
    }
}

pub trait MutGraphTrait<Id: IdType, NL, EL> {
    /// Associated node type
    type N;

    /// Associated edge type
    type E;

    /// Add a new node with specific id and label.
    /// *NOTE*: The label will be converted to an `usize` integer.
    fn add_node(&mut self, id: Id, label: Option<NL>) -> bool;

    /// Get a mutable reference to the node.
    fn get_node_mut(&mut self, id: Id) -> Option<&mut Self::N>;

    /// Remove the node and return it.
    fn remove_node(&mut self, id: Id) -> Option<Self::N>;

    /// Add a new edge (`start`,`target)` with a specific label.
    /// *NOTE*: The label will be converted to an `usize` integer.
    fn add_edge(&mut self, start: Id, target: Id, label: Option<EL>) -> bool;

    /// Get a mutable reference to the edge.
    fn get_edge_mut(&mut self, start: Id, target: Id) -> Option<&mut Self::E>;

    /// Remove the edge (`start`,`target)` and return it.
    fn remove_edge(&mut self, start: Id, target: Id) -> Option<Self::E>;

    /// Return an iterator of all nodes(mutable) in the graph.
    fn nodes_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::N>;

    /// Return an iterator over all edges(mutable) in the graph.
    fn edges_mut<'a>(&'a mut self) -> Iter<'a, &mut Self::E>;
}

pub trait GraphLabelTrait<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType>:
    GraphTrait<Id, L>
{
    /// Return an iterator over the set of all node labels.
    fn node_labels<'a>(&'a self) -> Iter<'a, &NL> {
        self.get_node_label_map().items()
    }

    /// Return an iterator over the set of all edge labels.
    fn edge_labels<'a>(&'a self) -> Iter<'a, &EL> {
        self.get_edge_label_map().items()
    }

    /// Lookup the node label by its id.
    #[inline(always)]
    fn get_node_label(&self, node_id: Id) -> Option<&NL> {
        match self.get_node(node_id).get_label_id() {
            Some(label_id) => self.get_node_label_map().get_item(label_id.id()),
            None => None,
        }
    }

    /// Lookup the edge label by its id.
    #[inline(always)]
    fn get_edge_label(&self, start: Id, target: Id) -> Option<&EL> {
        match self.get_edge(start, target).get_label_id() {
            Some(label_id) => self.get_edge_label_map().get_item(label_id.id()),
            None => None,
        }
    }

    /// Return the node label - id mapping.
    fn get_node_label_map(&self) -> &SetMap<NL>;

    /// Return the edge label - id mapping.
    fn get_edge_label_map(&self) -> &SetMap<EL>;

    fn get_node_label_counter(&self) -> Counter<&NL> {
        self.node_indices()
            .filter_map(|n| self.get_node_label(n))
            .collect()
    }

    fn get_edge_label_counter(&self) -> Counter<&EL> {
        self.edge_indices()
            .filter_map(|(s, d)| self.get_edge_label(s, d))
            .collect()
    }
}

pub trait MutGraphLabelTrait<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, L: IdType>:
    MutGraphTrait<Id, NL, EL> + GraphLabelTrait<Id, NL, EL, L>
{
    /// Update the node label.
    fn update_node_label(&mut self, node_id: Id, label: Option<NL>) -> bool;

    /// Update the edge label.
    fn update_edge_label(&mut self, start: Id, target: Id, label: Option<EL>) -> bool;
}

/// Trait for undirected graphs.
pub trait UnGraphTrait<Id: IdType, L: IdType>: GraphTrait<Id, L> {}

/// Trait for directed graphs.
pub trait DiGraphTrait<Id: IdType, L: IdType>: GraphTrait<Id, L> {
    /// Return the in-degree of a node.
    fn in_degree(&self, id: Id) -> usize;

    /// Return an iterator over the indices of all nodes with a edge from a given node.
    fn in_neighbors_iter(&self, id: Id) -> Iter<Id>;

    /// Return the indices(either owned or borrowed) of all nodes with a edge from a given node.
    fn in_neighbors(&self, id: Id) -> Cow<[Id]>;
}
