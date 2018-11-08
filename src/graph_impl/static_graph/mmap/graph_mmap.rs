use std::borrow::Cow;
use std::fs::metadata;
use std::hash::Hash;

use serde;

use generic::{
    DiGraphTrait, EdgeType, GeneralGraph, GraphLabelTrait, GraphTrait, IdType, Iter, NodeType,
};
use graph_impl::static_graph::mmap::EdgeVecMmap;
use graph_impl::static_graph::node::StaticNode;
use graph_impl::static_graph::static_edge_iter::StaticEdgeIndexIter;
use graph_impl::static_graph::EdgeVecTrait;
use graph_impl::{Edge, Graph};
use io::mmap::TypedMemoryMap;
use io::serde::{Deserialize, Deserializer};
use map::SetMap;

pub struct StaticGraphMmap<Id: IdType, N: Hash + Eq, E: Hash + Eq = N> {
    /// Outgoing edges, or edges for undirected
    edges: EdgeVecMmap<Id>,
    /// Incoming edges for directed, `None` for undirected
    in_edges: Option<EdgeVecMmap<Id>>,
    /// Maintain the node's labels, whose index is aligned with `offsets`.
    labels: Option<TypedMemoryMap<Id>>,

    num_nodes: usize,
    num_edges: usize,
    node_label_map: SetMap<N>,
    edge_label_map: SetMap<E>,
}

#[derive(Serialize, Deserialize)]
pub struct StaticGraphMmapAux<N: Hash + Eq, E: Hash + Eq = N> {
    num_nodes: usize,
    num_edges: usize,
    node_label_map: SetMap<N>,
    edge_label_map: SetMap<E>,
}

impl<N: Hash + Eq, E: Hash + Eq> StaticGraphMmapAux<N, E> {
    pub fn new(
        num_nodes: usize,
        num_edges: usize,
        node_label_map: SetMap<N>,
        edge_label_map: SetMap<E>,
    ) -> Self {
        StaticGraphMmapAux {
            num_nodes,
            num_edges,
            node_label_map,
            edge_label_map,
        }
    }

    pub fn empty(num_nodes: usize, num_edges: usize) -> Self {
        StaticGraphMmapAux {
            num_nodes,
            num_edges,
            node_label_map: SetMap::new(),
            edge_label_map: SetMap::new(),
        }
    }
}

impl<Id: IdType, N: Hash + Eq, E: Hash + Eq> StaticGraphMmap<Id, N, E>
where
    for<'de> N: serde::Deserialize<'de>,
    for<'de> E: serde::Deserialize<'de>,
{
    pub fn new(prefix: &str) -> Self {
        let edge_prefix = format!("{}_OUT", prefix);
        let in_edge_prefix = format!("{}_IN", prefix);
        let labels_file = format!("{}.labels", prefix);

        let aux_map_file = format!("{}_aux.bin", prefix);

        let edges = EdgeVecMmap::new(&edge_prefix);

        let in_edges = if metadata(&format!("{}.offsets", in_edge_prefix)).is_ok() {
            Some(EdgeVecMmap::new(&in_edge_prefix))
        } else {
            None
        };

        let labels = if metadata(&labels_file).is_ok() {
            Some(TypedMemoryMap::new(&labels_file))
        } else {
            None
        };

        let aux_file = if metadata(&aux_map_file).is_ok() {
            Deserializer::import(&aux_map_file).unwrap()
        } else {
            let num_node = edges.num_nodes();
            let num_edge = if in_edges.is_some() {
                edges.num_edges()
            } else {
                edges.num_edges() >> 1
            };

            StaticGraphMmapAux::empty(num_node, num_edge)
        };

        StaticGraphMmap {
            num_nodes: aux_file.num_nodes,
            num_edges: aux_file.num_edges,
            edges,
            in_edges,
            labels,
            node_label_map: aux_file.node_label_map,
            edge_label_map: aux_file.edge_label_map,
        }
    }
}

impl<Id: IdType, N: Hash + Eq, E: Hash + Eq> StaticGraphMmap<Id, N, E> {
    #[inline]
    pub fn inner_neighbors(&self, id: Id) -> &[Id] {
        self.edges.neighbors(id)
    }

    #[inline]
    pub fn inner_in_neighbors(&self, id: Id) -> &[Id] {
        if let Some(ref in_edges) = self.in_edges {
            in_edges.neighbors(id)
        } else {
            &[]
        }
    }
}

impl<Id: IdType, N: Hash + Eq, E: Hash + Eq> GraphTrait<Id> for StaticGraphMmap<Id, N, E> {
    fn get_node(&self, id: Id) -> NodeType<Id> {
        if !self.has_node(id) {
            return NodeType::None;
        }

        match self.labels {
            Some(ref labels) => {
                NodeType::StaticNode(StaticNode::new_static(id, labels[..][id.id()]))
            }
            None => NodeType::StaticNode(StaticNode::new(id, None)),
        }
    }

    fn get_edge(&self, start: Id, target: Id) -> EdgeType<Id> {
        if !self.has_edge(start, target) {
            return EdgeType::None;
        }

        let _label = self.edges.find_edge_label_id(start, target);
        match _label {
            Some(label) => EdgeType::StaticEdge(Edge::new_static(start, target, *label)),
            None => EdgeType::StaticEdge(Edge::new(start, target, None)),
        }
    }

    fn has_node(&self, id: Id) -> bool {
        id.id() < self.num_nodes
    }

    fn has_edge(&self, start: Id, target: Id) -> bool {
        let neighbors = self.neighbors(start);
        // The neighbors must be sorted anyway
        let pos = neighbors.binary_search(&target);

        pos.is_ok()
    }

    fn node_count(&self) -> usize {
        self.num_nodes
    }

    fn edge_count(&self) -> usize {
        self.num_edges
    }

    fn is_directed(&self) -> bool {
        // A directed graph should have in-coming edges ready
        self.in_edges.is_some()
    }

    fn node_indices(&self) -> Iter<Id> {
        Iter::new(Box::new((0..self.num_nodes).map(|x| Id::new(x))))
    }

    fn edge_indices(&self) -> Iter<(Id, Id)> {
        Iter::new(Box::new(StaticEdgeIndexIter::new(
            Box::new(&self.edges),
            self.is_directed(),
        )))
    }

    fn nodes(&self) -> Iter<NodeType<Id>> {
        match self.labels {
            None => {
                let node_iter = self
                    .node_indices()
                    .map(|i| NodeType::StaticNode(StaticNode::new(i, None)));

                Iter::new(Box::new(node_iter))
            }
            Some(ref labels) => {
                let node_iter = self
                    .node_indices()
                    .zip(labels[..].iter())
                    .map(|n| NodeType::StaticNode(StaticNode::new_static(n.0, *n.1)));

                Iter::new(Box::new(node_iter))
            }
        }
    }

    fn edges(&self) -> Iter<EdgeType<Id>> {
        let labels = self.edges.get_labels();
        if labels.is_empty() {
            let edge_iter = self
                .edge_indices()
                .map(|i| EdgeType::StaticEdge(Edge::new(i.0, i.1, None)));

            Iter::new(Box::new(edge_iter))
        } else {
            let edge_iter = self
                .edge_indices()
                .zip(labels.iter())
                .map(|e| EdgeType::StaticEdge(Edge::new_static((e.0).0, (e.0).1, *e.1)));

            Iter::new(Box::new(edge_iter))
        }
    }

    fn degree(&self, id: Id) -> usize {
        self.neighbors(id).len()
    }

    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        let neighbors = self.edges.neighbors(id);

        Iter::new(Box::new(neighbors.iter().map(|x| *x)))
    }

    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        self.edges.neighbors(id).into()
    }

    fn max_seen_id(&self) -> Option<Id> {
        Some(Id::new(self.node_count() - 1))
    }

    fn max_possible_id(&self) -> Id {
        Id::max_value()
    }

    fn implementation(&self) -> Graph {
        Graph::StaicGraphMmap
    }
}

impl<Id: IdType, N: Hash + Eq, E: Hash + Eq> GraphLabelTrait<Id, N, E>
    for StaticGraphMmap<Id, N, E>
{
    fn get_node_label_map(&self) -> &SetMap<N> {
        &self.node_label_map
    }

    fn get_edge_label_map(&self) -> &SetMap<E> {
        &self.edge_label_map
    }
}

impl<Id: IdType, N: Hash + Eq, E: Hash + Eq> DiGraphTrait<Id> for StaticGraphMmap<Id, N, E> {
    fn in_degree(&self, id: Id) -> usize {
        self.inner_in_neighbors(id).len()
    }

    fn in_neighbors_iter(&self, id: Id) -> Iter<Id> {
        Iter::new(Box::new(self.inner_in_neighbors(id).iter().map(|x| *x)))
    }

    fn in_neighbors(&self, id: Id) -> Cow<[Id]> {
        self.inner_in_neighbors(id).into()
    }
}

impl<Id: IdType, N: Hash + Eq, E: Hash + Eq> GeneralGraph<Id, N, E> for StaticGraphMmap<Id, N, E> {
    #[inline]
    fn as_graph(&self) -> &GraphTrait<Id> {
        self
    }

    #[inline]
    fn as_labeled_graph(&self) -> &GraphLabelTrait<Id, N, E> {
        self
    }

    #[inline]
    fn as_digraph(&self) -> Option<&DiGraphTrait<Id>> {
        if self.is_directed() {
            Some(self)
        } else {
            None
        }
    }
}
