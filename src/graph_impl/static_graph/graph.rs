use std::borrow::Cow;
use std::hash::Hash;
use std::iter;
use std::marker::PhantomData;

use generic::Iter;
use generic::{DefaultId, IdType};
use generic::{DiGraphTrait, GeneralGraph, GeneralLabeledGraph, GraphLabelTrait, GraphTrait,
              UnGraphTrait};
use generic::{Directed, GraphType, Undirected};

use map::SetMap;

use graph_impl::Graph;
use graph_impl::static_graph::edge_vec::EdgeVec;

pub type TypedUnStaticGraph<Id, NL, EL = NL> = TypedStaticGraph<Id, NL, EL, Undirected>;
pub type TypedDiStaticGraph<Id, NL, EL = NL> = TypedStaticGraph<Id, NL, EL, Directed>;
pub type StaticGraph<NL, EL, Ty> = TypedStaticGraph<DefaultId, NL, EL, Ty>;
pub type UnStaticGraph<NL, EL = NL> = StaticGraph<NL, EL, Undirected>;
pub type DiStaticGraph<NL, EL = NL> = StaticGraph<NL, EL, Directed>;

/// `StaticGraph` is a memory-compact graph data structure.
/// The labels of both nodes and edges, if exist, are encoded as `Integer`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedStaticGraph<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> {
    num_nodes: usize,
    num_edges: usize,
    edge_vec: EdgeVec<Id>,
    in_edge_vec: Option<EdgeVec<Id>>,
    // Maintain the node's labels, whose index is aligned with `offsets`.
    labels: Option<Vec<Id>>,
    // A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
    /// A map of node labels.
    node_label_map: SetMap<NL>,
    /// A map of edge labels.
    edge_label_map: SetMap<EL>,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> TypedStaticGraph<Id, NL, EL, Ty> {
    pub fn new(
        num_nodes: usize,
        edges: EdgeVec<Id>,
        in_edges: Option<EdgeVec<Id>>,
        edge_label_map: SetMap<EL>,
    ) -> Self {
        if Ty::is_directed() {
            assert!(in_edges.is_some());
            assert_eq!(in_edges.as_ref().unwrap().len(), edges.len());
        }

        TypedStaticGraph {
            num_nodes,
            num_edges: if Ty::is_directed() {
                edges.len()
            } else {
                edges.len() >> 1
            },
            edge_vec: edges,
            in_edge_vec: in_edges,
            labels: None,
            node_label_map: SetMap::<NL>::new(),
            edge_label_map,
            graph_type: PhantomData,
        }
    }

    pub fn with_labels(
        num_nodes: usize,
        edges: EdgeVec<Id>,
        in_edges: Option<EdgeVec<Id>>,
        labels: Vec<Id>,
        node_label_map: SetMap<NL>,
        edge_label_map: SetMap<EL>,
    ) -> Self {
        if Ty::is_directed() {
            assert!(in_edges.is_some());
            assert_eq!(in_edges.as_ref().unwrap().len(), edges.len());
        }
        assert_eq!(num_nodes, labels.len());

        TypedStaticGraph {
            num_nodes,
            num_edges: if Ty::is_directed() {
                edges.len()
            } else {
                edges.len() >> 1
            },
            edge_vec: edges,
            in_edge_vec: in_edges,
            labels: Some(labels),
            node_label_map,
            edge_label_map,
            graph_type: PhantomData,
        }
    }

    pub fn get_edge_vec(&self) -> &EdgeVec<Id> {
        &self.edge_vec
    }

    pub fn get_in_edge_vec(&self) -> &Option<EdgeVec<Id>> {
        &self.in_edge_vec
    }

    pub fn shrink_to_fit(&mut self) {
        self.edge_vec.shrink_to_fit();
        if let Some(ref mut in_edge_vec) = self.in_edge_vec {
            in_edge_vec.shrink_to_fit();
        }
        if let Some(ref mut labels) = self.labels {
            labels.shrink_to_fit();
        }
    }

    pub fn find_edge_index(&self, start: Id, target: Id) -> Option<usize> {
        self.edge_vec.find_edge_index(start, target)
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> GraphTrait<Id>
    for TypedStaticGraph<Id, NL, EL, Ty>
{
    type N = Id;
    type E = Id;

    /// In `StaticGraph`, a node is simply an `id`. Here we simply get its label.
    fn get_node(&self, id: Id) -> Option<&Self::N> {
        match self.labels {
            None => None,
            Some(ref labels) => labels.get(id.id()),
        }
    }

    /// In `StaticGraph`, an edge is an attribute (as adjacency list) of a node.
    /// Here, we return the edge's label if the label exist.
    fn get_edge(&self, start: Id, target: Id) -> Option<&Self::E> {
        self.edge_vec.find_edge_label(start, target)
    }

    fn has_node(&self, id: Id) -> bool {
        id.id() < self.num_nodes
    }

    fn has_edge(&self, start: Id, target: Id) -> bool {
        self.edge_vec.has_edge(start, target)
    }

    fn node_count(&self) -> usize {
        self.num_nodes
    }

    fn edge_count(&self) -> usize {
        self.num_edges
    }

    fn is_directed(&self) -> bool {
        Ty::is_directed()
    }

    fn node_indices(&self) -> Iter<Id> {
        Iter::new(Box::new((0..self.num_nodes).map(|x| Id::new(x))))
    }

    fn edge_indices(&self) -> Iter<(Id, Id)> {
        Iter::new(Box::new(EdgeIter::new(self)))
    }

    ///In `StaticGraph`, a node is simply an `id`.
    ///Thus, we return an iterator over the labels of all nodes.
    fn nodes<'a>(&'a self) -> Iter<'a, &Self::N> {
        match self.labels {
            Some(ref labels) => Iter::new(Box::new(labels.iter())),
            None => Iter::new(Box::new(iter::empty())),
        }
    }

    /// In `StaticGraph`, an edge is an attribute (as adjacency list) of a node.
    /// Thus, we return an iterator over the labels of all edges.
    fn edges<'a>(&'a self) -> Iter<'a, &Self::E> {
        Iter::new(Box::new(self.edge_vec.get_labels().iter()))
    }

    fn degree(&self, id: Id) -> usize {
        self.edge_vec.degree(id)
    }

    fn neighbors_iter(&self, id: Id) -> Iter<Id> {
        let neighbors = self.edge_vec.neighbors(id);
        Iter::new(Box::new(neighbors.iter().map(|x| *x)))
    }

    fn neighbors(&self, id: Id) -> Cow<[Id]> {
        self.edge_vec.neighbors(id).into()
    }

    fn get_node_label_id(&self, node_id: Id) -> Option<Id> {
        self.get_node(node_id).map(|x| *x)
    }

    fn get_edge_label_id(&self, start: Id, target: Id) -> Option<Id> {
        self.get_edge(start, target).map(|x| *x)
    }

    fn max_seen_id(&self) -> Option<Id> {
        Some(Id::new(self.node_count() - 1))
    }

    fn max_possible_id(&self) -> Id {
        Id::max_value()
    }

    fn implementation(&self) -> Graph {
        Graph::StaticGraph
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> GraphLabelTrait<Id, NL, EL>
    for TypedStaticGraph<Id, NL, EL, Ty>
{
    fn get_node_label_map(&self) -> &SetMap<NL> {
        &self.node_label_map
    }

    fn get_edge_label_map(&self) -> &SetMap<EL> {
        &self.edge_label_map
    }
}

impl<Id, NL, EL> UnGraphTrait<Id> for TypedUnStaticGraph<Id, NL, EL>
where
    Id: IdType,
    NL: Hash + Eq,
    EL: Hash + Eq,
{
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> DiGraphTrait<Id> for TypedDiStaticGraph<Id, NL, EL> {
    fn in_degree(&self, id: Id) -> usize {
        self.in_neighbors(id).len()
    }

    fn in_neighbors_iter(&self, id: Id) -> Iter<Id> {
        let in_neighbors = self.in_edge_vec.as_ref().unwrap().neighbors(id);
        Iter::new(Box::new(in_neighbors.iter().map(|x| *x)))
    }

    fn in_neighbors(&self, id: Id) -> Cow<[Id]> {
        self.in_edge_vec.as_ref().unwrap().neighbors(id).into()
    }
}

pub struct EdgeIter<'a, Id: 'a + IdType, NL: 'a + Hash + Eq, EL: 'a + Hash + Eq, Ty: 'a + GraphType>
{
    g: &'a TypedStaticGraph<Id, NL, EL, Ty>,
    curr_node: usize,
    curr_neighbor_index: usize,
}

impl<'a, Id: 'a + IdType, NL: 'a + Hash + Eq, EL: 'a + Hash + Eq, Ty: 'a + GraphType>
    EdgeIter<'a, Id, NL, EL, Ty>
{
    pub fn new(g: &'a TypedStaticGraph<Id, NL, EL, Ty>) -> Self {
        EdgeIter {
            g,
            curr_node: 0,
            curr_neighbor_index: 0,
        }
    }
}

impl<'a, Id: 'a + IdType, NL: 'a + Hash + Eq, EL: 'a + Hash + Eq, Ty: 'a + GraphType> Iterator
    for EdgeIter<'a, Id, NL, EL, Ty>
{
    type Item = (Id, Id);

    fn next(&mut self) -> Option<Self::Item> {
        let mut node: usize;
        let mut neighbors: &[Id];

        loop {
            while self.g.has_node(Id::new(self.curr_node))
                && self.curr_neighbor_index >= self.g.degree(Id::new(self.curr_node))
            {
                self.curr_node += 1;
                self.curr_neighbor_index = 0;
            }

            node = self.curr_node;
            if !self.g.has_node(Id::new(node)) {
                return None;
            }

            neighbors = self.g.edge_vec.neighbors(Id::new(node));

            if !self.g.is_directed() && neighbors[self.curr_neighbor_index] < Id::new(node) {
                match neighbors.binary_search(&Id::new(node)) {
                    Ok(index) => {
                        self.curr_neighbor_index = index;
                        break;
                    }
                    Err(index) => {
                        if index < neighbors.len() {
                            self.curr_neighbor_index = index;
                            break;
                        } else {
                            self.curr_node += 1;
                            self.curr_neighbor_index = 0;
                        }
                    }
                }
            } else {
                break;
            }
        }

        let neighbor = neighbors[self.curr_neighbor_index];
        let edge = (Id::new(node), neighbor);
        self.curr_neighbor_index += 1;
        Some(edge)
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GeneralGraph<Id> for TypedUnStaticGraph<Id, NL, EL> {
    fn as_graph(
        &self,
    ) -> &GraphTrait<Id, N = <Self as GraphTrait<Id>>::N, E = <Self as GraphTrait<Id>>::E> {
        self
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GeneralGraph<Id> for TypedDiStaticGraph<Id, NL, EL> {
    fn as_graph(
        &self,
    ) -> &GraphTrait<Id, N = <Self as GraphTrait<Id>>::N, E = <Self as GraphTrait<Id>>::E> {
        self
    }

    fn as_digraph(
        &self,
    ) -> Option<&DiGraphTrait<Id, N = <Self as GraphTrait<Id>>::N, E = <Self as GraphTrait<Id>>::E>>
    {
        Some(self)
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GeneralLabeledGraph<Id, NL, EL>
    for TypedUnStaticGraph<Id, NL, EL>
{
    fn as_general_graph(
        &self,
    ) -> &GeneralGraph<Id, N = <Self as GraphTrait<Id>>::N, E = <Self as GraphTrait<Id>>::E> {
        self
    }

    fn as_labeled_graph(
        &self,
    ) -> &GraphLabelTrait<
        Id,
        NL,
        EL,
        N = <Self as GraphTrait<Id>>::N,
        E = <Self as GraphTrait<Id>>::E,
    > {
        self
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GeneralLabeledGraph<Id, NL, EL>
    for TypedDiStaticGraph<Id, NL, EL>
{
    fn as_general_graph(
        &self,
    ) -> &GeneralGraph<Id, N = <Self as GraphTrait<Id>>::N, E = <Self as GraphTrait<Id>>::E> {
        self
    }

    fn as_labeled_graph(
        &self,
    ) -> &GraphLabelTrait<
        Id,
        NL,
        EL,
        N = <Self as GraphTrait<Id>>::N,
        E = <Self as GraphTrait<Id>>::E,
    > {
        self
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> Drop
    for TypedStaticGraph<Id, NL, EL, Ty>
{
    fn drop(&mut self) {
        self.edge_vec.clear();
        if let Some(ref mut in_edges) = self.in_edge_vec {
            in_edges.clear();
        }
        if let Some(ref mut labels) = self.labels {
            labels.clear();
        }
    }
}
