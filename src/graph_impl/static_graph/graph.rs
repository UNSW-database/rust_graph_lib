use std::marker::PhantomData;
use std::iter;

use generic::GraphTrait;
use generic::GraphType;
use generic::{Directed, Undirected};
use generic::{IndexIter, Iter};

use graph_impl::static_graph::edge_vec::EdgeVec;

pub type UnStaticGraph = StaticGraph<Undirected>;
pub type DiStaticGraph = StaticGraph<Directed>;

/// `StaticGraph` is a memory-compact graph data structure.
/// The labels of both nodes and edges, if exist, are encoded as `Integer`.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct StaticGraph<Ty: GraphType> {
    num_nodes: usize,
    num_edges: usize,
    edge_vec: EdgeVec,
    in_edge_vec: Option<EdgeVec>,
    // Maintain the node's labels, whose index is aligned with `offsets`.
    labels: Option<Vec<usize>>,
    // A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
}

// See https://github.com/rust-lang/rust/issues/26925
impl<Ty: GraphType> Clone for StaticGraph<Ty> {
    fn clone(&self) -> Self {
        StaticGraph {
            num_nodes: self.num_nodes.clone(),
            num_edges: self.num_edges.clone(),
            edge_vec: self.edge_vec.clone(),
            in_edge_vec: self.in_edge_vec.clone(),
            labels: self.labels.clone(),
            graph_type: PhantomData,
        }
    }
}

impl<Ty: GraphType> StaticGraph<Ty> {
    pub fn new(num_nodes: usize, edges: EdgeVec, in_edges: Option<EdgeVec>) -> Self {
        if Ty::is_directed() {
            assert!(in_edges.is_some());
            assert_eq!(in_edges.as_ref().unwrap().len(), edges.len());
        }
        StaticGraph {
            num_nodes,
            num_edges: if Ty::is_directed() {
                edges.len()
            } else {
                // Undirected graph's actual `num_edges` shall halve the size of the `edges` vector
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
        edges: EdgeVec,
        in_edges: Option<EdgeVec>,
        labels: Vec<usize>,
    ) -> Self {
        assert_eq!(num_nodes, labels.len());
        if Ty::is_directed() {
            assert!(in_edges.is_some());
            assert_eq!(in_edges.as_ref().unwrap().len(), edges.len());
        }

        StaticGraph {
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

    pub fn neighbors(&self, node: usize) -> &[usize] {
        self.edge_vec.neighbors(node)
    }

    pub fn find_edge_index(&self, start: usize, target: usize) -> Option<usize> {
        self.edge_vec.find_edge_index(start, target)
    }

    pub fn get_num_nodes(&self) -> usize{
        self.num_nodes
    }

    pub fn get_num_edges(&self) -> usize{
        self.num_edges
    }

    pub fn get_edge_vec(&self) -> &EdgeVec{
        &self.edge_vec
    }

    pub fn get_in_edge_vec(&self) -> &Option<EdgeVec>{
        &self.in_edge_vec
    }

    pub fn get_labels(&self) -> &Option<Vec<usize>>{
        &self.labels
    }

    pub fn get_graph_type(&self) -> &PhantomData<Ty>{
        &self.graph_type
    }

}

impl<Ty: GraphType> GraphTrait for StaticGraph<Ty> {
    type N = usize;
    type E = usize;

    /// In `StaticGraph`, a node is simply an `id`. Here we simply get its label.
    fn get_node(&self, id: usize) -> Option<&Self::N> {
        match self.labels {
            None => None,
            Some(ref labels) => labels.get(id),
        }
    }

    /// In `StaticGraph`, an edge is an attribute (as adjacency list) of a node.
    /// Here, we return the edge's label if the label exist.
    fn find_edge(&self, start: usize, target: usize) -> Option<&Self::E> {
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

    fn node_indices<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(0..self.num_nodes))
    }
    fn edge_indices<'a>(&'a self) -> Iter<'a, (usize, usize)> {
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

    fn neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.edge_vec.neighbors(id).iter().map(|i| *i)))
    }
    fn get_node_label_id(&self, node_id: usize) -> Option<usize> {
        self.get_node(node_id).map(|i| *i)
    }

    fn get_edge_label_id(&self, start: usize, target: usize) -> Option<usize> {
        self.find_edge(start, target).map(|i| *i)
    }
}

pub struct EdgeIter<'a, Ty: 'a + GraphType> {
    g: &'a StaticGraph<Ty>,
    curr_node: usize,
    curr_neighbor_index: usize,
}

impl<'a, Ty: 'a + GraphType> EdgeIter<'a, Ty> {
    pub fn new(g: &'a StaticGraph<Ty>) -> Self {
        EdgeIter {
            g,
            curr_node: 0,
            curr_neighbor_index: 0,
        }
    }
}

impl<'a, Ty: 'a + GraphType> Iterator for EdgeIter<'a, Ty> {
    type Item = (usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        let mut node: usize;
        let mut neighbors: &[usize];

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

            if !self.g.is_directed() && neighbors[self.curr_neighbor_index] < node {
                match neighbors.binary_search(&node) {
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
        let edge = (node, neighbor);
        self.curr_neighbor_index += 1;
        Some(edge)
    }
}
