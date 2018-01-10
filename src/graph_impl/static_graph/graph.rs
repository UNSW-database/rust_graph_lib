use std::marker::PhantomData;
use std::collections::HashSet;
use std::iter;
use std::iter::FromIterator;

use generic::GraphTrait;
use generic::{GraphType, Undirected, Directed};
use generic::Iter;
use generic::IndexIter;


pub type UnStaticGraph = StaticGraph<Undirected>;
pub type DiStaticGraph = StaticGraph<Directed>;

/// With the node indexed from 0 .. num_nodes - 1, we can maintain the edges in a compact way,
/// using `offset` and `edges`, in which `offset[node]` maintain the start index of the given
/// node's neighbours in `edges`. Thus, the node's neighbors is maintained in:
/// `edges[offsets[node]]` (included) to `edges[offsets[id+1]` (excluded),
///
/// *Note*: The edges must be sorted according to the starting node, that is,
/// The sub-vector `edges[offset[node]]` (included) - `edges[offsets[node + 1]]` (excluded)
/// for any `node` should be sorted.
#[derive(Debug, PartialEq, Clone)]
pub struct EdgeVec {
    offsets: Vec<usize>,
    edges: Vec<usize>,
    // Maintain the corresponding edge's labels if exist, aligned with `edges`.
    // Note that the label has been encoded as an Integer.
    labels: Option<Vec<usize>>,
}

impl EdgeVec {
    pub fn new(offsets: Vec<usize>, edges: Vec<usize>) -> Self {
        EdgeVec {
            offsets,
            edges,
            labels: None,
        }
    }

    pub fn with_labels(offsets: Vec<usize>, edges: Vec<usize>, labels: Vec<usize>) -> Self {
        assert_eq!(edges.len(), labels.len());
        EdgeVec {
            offsets,
            edges,
            labels: Some(labels),
        }
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    // Get the neighbours of a given `node`.
    pub fn neighbors(&self, node: usize) -> &[usize] {
        assert!(node < self.offsets.len() - 1);
        let start = self.offsets[node];
        let end = self.offsets[node + 1];
        assert!(start < self.edges.len() && end <= self.edges.len());
        &self.edges[start..end]
    }

    pub fn degree(&self, node: usize) -> usize {
        self.neighbors(node).len()
    }

    /// Given a both ends of the edges, `start` and `target`, locate its index
    /// in the edge vector, if the corresponding edge exists.
    pub fn find_edge_index(&self, start: usize, target: usize) -> Option<usize> {
        if !(start < self.offsets.len() && target < self.offsets.len()) {
            None
        } else {
            let neighbours = self.neighbors(start);
            let found = neighbours.binary_search(&target);
            match found {
                Err(_) => None,
                Ok(idx) => Some(self.offsets[start] + idx)
            }
        }
    }

    pub fn has_edge(&self, start: usize, end: usize) -> bool {
        self.find_edge_index(start, end).is_some()
    }

    pub fn find_edge_label(&self, start: usize, target: usize) -> Option<&usize> {
        match self.labels {
            None => None,
            Some(ref labels) => {
                let idx_opt = self.find_edge_index(start, target);
                match idx_opt {
                    None => None,
                    Some(idx) => labels.get(idx)
                }
            }
        }
    }
}

/// `StaticGraph` is a memory-compact graph data structure.
/// The labels of both nodes and edges, if exist, are encoded as `Integer`.
/// While the adjacency list of each
#[derive(Debug, PartialEq, Clone)]
pub struct StaticGraph<Ty: GraphType> {
    num_nodes: usize,
    num_edges: usize,
    edges: EdgeVec,
    in_edges: Option<EdgeVec>,
    // Maintain the node's labels, whose index is aligned with `offsets`.
    labels: Option<Vec<usize>>,
    // A marker of thr graph type, namely, directed or undirected.
    graph_type: PhantomData<Ty>,
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
                edges.len() >> 1
            },
            edges,
            in_edges,
            labels: None,
            graph_type: PhantomData,
        }
    }

    pub fn with_labels(num_nodes: usize, edges: EdgeVec,
                       in_edges: Option<EdgeVec>,
                       labels: Vec<usize>) -> Self {
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
            edges,
            in_edges,
            labels: Some(labels),
            graph_type: PhantomData,
        }
    }

    pub fn neighbors(&self, node: usize) -> &[usize] {
        self.edges.neighbors(node)
    }

    pub fn find_edge_index(&self, start: usize, target: usize) -> Option<usize> {
        self.edges.find_edge_index(start, target)
    }

    pub fn node_indices_of_label<'a>(&'a self, label: usize) -> IndexIter<'a> {
//        IndexIter::new(Box::new((0..self.num_nodes).filter(move |&idx| {
//            let label_opt = self.get_node_label(idx);
//            match label_opt {
//                None => false,
//                Some(&l) => l == label
//            }
//        })))
        unimplemented!()
    }
}

impl<Ty: GraphType> GraphTrait for StaticGraph<Ty> {
    type N = usize;
    type E = usize;

    /// In `StaticGraph`, a node is simply an `id`. Here we simply get its label.
    fn get_node(&self, id: usize) -> Option<&Self::N> {
        match self.labels {
            None => None,
            Some(ref labels) => labels.get(id)
        }
    }

    /// In `StaticGraph`, an edge is an attribute (as adjacency list) of a node.
    /// Here, we return the edge's label if the label exist.
    fn find_edge(&self, start: usize, target: usize) -> Option<&Self::E> {
        self.edges.find_edge_label(start, target)
    }

    fn has_node(&self, id: usize) -> bool {
        id < self.num_nodes
    }

    fn has_edge(&self, start: usize, target: usize) -> bool {
        self.edges.has_edge(start, target)
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

    fn degree(&self, id: usize) -> usize {
        self.edges.len()
    }

    /// In `StaticGraph`, the label has already been encoded as `id`.
    /// Here, we simple query the label using the node's id.
    fn get_node_label_id(&self, node_id: usize) -> Option<usize> {
        self.get_node(node_id).map(|i| { *i })
    }

    fn get_edge_label_id(&self, start: usize, target: usize) -> Option<usize> {
        self.find_edge(start, target).map(|i| { *i })
    }

    // Below are unimplemented `GraphTrait` functions. Considering modify the `GraphTrait`
    // to exclude some unnecessary functions.

    fn node_indices<'a>(&'a self) -> IndexIter<'a> {
//        IndexIter::new(Box::new(0..self.num_nodes))
        unimplemented!()
    }

    fn nodes<'a>(&'a self) -> Iter<'a, &Self::N> {
        unimplemented!()
    }

    fn neighbor_indices<'a>(&'a self, id: usize) -> IndexIter<'a> {
        unimplemented!()
    }

    fn edge_indices<'a>(&'a self) -> Iter<'a, &(usize, usize)> {
        unimplemented!();
    }

    fn edges<'a>(&'a self) -> Iter<'a, &Self::E> {
        unimplemented!();
    }


//    fn node_labels<'a>(&'a self) -> Iter<'a, &usize> {
//        match self.labels {
//            None => Iter::new(Box::new(iter::empty::<&usize>())),
//            Some(ref labels) => {
//                let labels_set: HashSet<&usize> = HashSet::from_iter(labels);
//                Iter::new(Box::new(labels_set.into_iter()))
//            }
//        }
//    }
//
//    fn edge_labels<'a>(&'a self) -> Iter<'a, &usize> {
//        match self.edges.labels {
//            None => Iter::new(Box::new(iter::empty::<&usize>())),
//            Some(ref labels) => {
//                let labels_set: HashSet<&usize> = HashSet::from_iter(labels);
//                Iter::new(Box::new(labels_set.into_iter()))
//            }
//        }
//    }
}

