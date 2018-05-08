use std::marker::PhantomData;
use std::iter;
use std::borrow::Cow;

use generic::{DefaultId, IdType};
use generic::{DiGraphTrait, GraphTrait};
use generic::{Directed, GraphType, Undirected};
use generic::{IndexIter, Iter};

use graph_impl::static_graph::edge_vec::EdgeVec;

pub type TypedUnStaticGraph<Id> = TypedStaticGraph<Id, Undirected>;
pub type TypedDiStaticGraph<Id> = TypedStaticGraph<Id, Directed>;
pub type StaticGraph<Ty> = TypedStaticGraph<DefaultId, Ty>;
pub type UnStaticGraph = StaticGraph<Undirected>;
pub type DiStaticGraph = StaticGraph<Directed>;

/// `StaticGraph` is a memory-compact graph data structure.
/// The labels of both nodes and edges, if exist, are encoded as `Integer`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypedStaticGraph<Id: IdType, Ty: GraphType> {
    num_nodes: usize,
    num_edges: usize,
    edge_vec: EdgeVec<Id>,
    in_edge_vec: Option<EdgeVec<Id>>,
    // Maintain the node's labels, whose index is aligned with `offsets`.
    labels: Option<Vec<Id>>,
    // A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
}

impl<Id: IdType, Ty: GraphType> TypedStaticGraph<Id, Ty> {
    pub fn new(num_nodes: usize, edges: EdgeVec<Id>, in_edges: Option<EdgeVec<Id>>) -> Self {
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
            graph_type: PhantomData,
        }
    }

    pub fn with_labels(
        num_nodes: usize,
        edges: EdgeVec<Id>,
        in_edges: Option<EdgeVec<Id>>,
        labels: Vec<Id>,
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

    pub fn find_edge_index(&self, start: usize, target: usize) -> Option<usize> {
        self.edge_vec.find_edge_index(start, target)
    }

    //    pub fn neighbors(&self, node: usize) -> &[Id] {
    //        self.edge_vec.neighbors(node)
    //    }

    //    pub fn in_neighbors(&self, node: usize) -> Option<&[Id]> {
    //        match self.in_edge_vec {
    //            Some(ref edge_vec) => Some(edge_vec.neighbors(node)),
    //            None => None,
    //        }
    //    }
}

impl<Id: IdType, Ty: GraphType> GraphTrait<Id> for TypedStaticGraph<Id, Ty> {
    type N = Id;
    type E = Id;

    /// In `StaticGraph`, a node is simply an `id`. Here we simply get its label.
    fn get_node(&self, id: usize) -> Option<&Self::N> {
        match self.labels {
            None => None,
            Some(ref labels) => labels.get(id),
        }
    }

    /// In `StaticGraph`, an edge is an attribute (as adjacency list) of a node.
    /// Here, we return the edge's label if the label exist.
    fn get_edge(&self, start: usize, target: usize) -> Option<&Self::E> {
        self.edge_vec.find_edge_label(start, target)
    }

    fn has_node(&self, id: usize) -> bool {
        id < self.num_nodes
    }

    fn has_edge(&self, start: usize, target: usize) -> bool {
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

    fn node_indices(&self) -> IndexIter {
        IndexIter::new(Box::new(0..self.num_nodes))
    }

    fn edge_indices(&self) -> Iter<(usize, usize)> {
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

    fn degree(&self, id: usize) -> usize {
        self.edge_vec.degree(id)
    }

    fn neighbors_iter(&self, id: usize) -> IndexIter {
        let neighbors = self.edge_vec.neighbors(id);
        IndexIter::new(Box::new(neighbors.iter().map(|i| i.id())))
    }

    fn neighbors(&self, id: usize) -> Cow<[Id]> {
        self.edge_vec.neighbors(id).into()
    }

    fn get_node_label_id(&self, node_id: usize) -> Option<usize> {
        self.get_node(node_id).map(|i| i.id())
    }

    fn get_edge_label_id(&self, start: usize, target: usize) -> Option<usize> {
        self.get_edge(start, target).map(|i| i.id())
    }

    fn max_possible_id(&self) -> usize {
        Id::max_usize()
    }
}

impl<Id: IdType> DiGraphTrait<Id> for TypedDiStaticGraph<Id> {
    fn in_degree(&self, id: usize) -> usize {
        self.in_neighbors(id).len()
    }

    fn in_neighbors_iter(&self, id: usize) -> IndexIter {
        let in_neighbors = self.in_edge_vec.as_ref().unwrap().neighbors(id);
        IndexIter::new(Box::new(in_neighbors.iter().map(|i| i.id())))
    }

    fn in_neighbors(&self, id: usize) -> Cow<[Id]> {
        self.in_edge_vec.as_ref().unwrap().neighbors(id).into()
    }
}

pub struct EdgeIter<'a, Id: 'a + IdType, Ty: 'a + GraphType> {
    g: &'a TypedStaticGraph<Id, Ty>,
    curr_node: usize,
    curr_neighbor_index: usize,
}

impl<'a, Id: 'a + IdType, Ty: 'a + GraphType> EdgeIter<'a, Id, Ty> {
    pub fn new(g: &'a TypedStaticGraph<Id, Ty>) -> Self {
        EdgeIter {
            g,
            curr_node: 0,
            curr_neighbor_index: 0,
        }
    }
}

impl<'a, Id: 'a + IdType, Ty: 'a + GraphType> Iterator for EdgeIter<'a, Id, Ty> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let mut node: usize;
        let mut neighbors: &[Id];

        loop {
            while self.g.has_node(self.curr_node)
                && self.curr_neighbor_index >= self.g.degree(self.curr_node)
            {
                self.curr_node += 1;
                self.curr_neighbor_index = 0;
            }

            node = self.curr_node;
            if !self.g.has_node(node) {
                return None;
            }

            neighbors = self.g.edge_vec.neighbors(node);

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
        let edge = (node, neighbor.id());
        self.curr_neighbor_index += 1;
        Some(edge)
    }
}

impl<Id: IdType, Ty: GraphType> Drop for TypedStaticGraph<Id, Ty> {
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
