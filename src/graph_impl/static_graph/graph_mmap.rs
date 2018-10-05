//! This file defines a mmap version of `StaticGraph`, so that when the graph is huge,
//! we can rely on mmap to save physical memory usage.
//!
use generic::{DiGraphTrait, GeneralGraph, GraphTrait, GraphLabelTrait, NodeType, EdgeType, Iter};
use graph_impl::Graph;
use io::mmap::{typed_as_byte_slice, TypedMemoryMap};
use map::SetMap;
use prelude::IdType;
use std::borrow::Cow;
use std::fs::{metadata, File};
use std::hash::Hash;
use std::io::{BufWriter, Result, Write};

/// A mmap version of `EdgeVec`.
pub struct EdgeVecMmap<Id: IdType + Copy + Ord, L: Copy + Ord> {
    offsets: TypedMemoryMap<usize>,
    edges: TypedMemoryMap<Id>,
    labels: Option<TypedMemoryMap<L>>,
}

impl<Id: IdType + Copy + Ord, L: Copy + Ord> EdgeVecMmap<Id, L> {
    pub fn new(prefix: &str) -> Self {
        let offsets_file = format!("{}.offsets", prefix);
        let edges_file = format!("{}.edges", prefix);
        let labels_file = format!("{}.labels", prefix);

        if metadata(&labels_file).is_ok() {
            EdgeVecMmap {
                offsets: TypedMemoryMap::new(&offsets_file),
                edges: TypedMemoryMap::new(&edges_file),
                labels: Some(TypedMemoryMap::new(&labels_file)),
            }
        } else {
            EdgeVecMmap {
                offsets: TypedMemoryMap::new(&offsets_file),
                edges: TypedMemoryMap::new(&edges_file),
                labels: None,
            }
        }
    }

    #[inline(always)]
    pub fn neighbors(&self, node: Id) -> &[Id] {
        let idx = node.id();
        if idx < self.num_nodes() {
            let start = self.offsets[..][idx];
            let limit = self.offsets[..][idx + 1];
            &self.edges[..][start..limit]
        } else {
            &[]
        }
    }

    #[inline(always)]
    pub fn num_nodes(&self) -> usize {
        self.offsets.len - 1
    }

    #[inline(always)]
    pub fn num_edges(&self) -> usize {
        self.edges.len
    }

    #[inline(always)]
    pub fn get_edge_label(&self, src: Id, dst: Id) -> Option<&L> {
        let idx = src.id();
        if let Some(ref labels) = self.labels {
            if idx < self.num_nodes() {
                let start = self.offsets[..][idx];
                let limit = self.offsets[..][idx + 1];
                let edges = &self.edges[..][start..limit];
                let labels = &labels[..][start..limit];

                let _pos = edges.binary_search(&dst);

                match _pos {
                    Ok(pos) => Some(&labels[pos]),
                    Err(_) => None,
                }
            } else {
                None
            }
        } else {
            None
        }
    }
}

pub struct StaticGraphMmap<Id: IdType + Copy + Ord, N: Copy + Ord, E: Copy + Ord = N> {
    /// Outgoing edges, or edges for undirected
    edges: EdgeVecMmap<Id, E>,
    /// Incoming edges for directed, `None` for undirected
    in_edges: Option<EdgeVecMmap<Id, E>>,
    /// Maintain the node's labels, whose index is aligned with `offsets`.
    labels: Option<TypedMemoryMap<N>>,
}

impl<Id: IdType + Copy + Ord, N: Copy + Ord, E: Copy + Ord> StaticGraphMmap<Id, N, E> {
    pub fn new(prefix: &str) -> Self {
        let edge_prefix = format!("{}_OUT", prefix);
        let in_edge_prefix = format!("{}_IN", prefix);
        let labels_file = format!("{}.labels", prefix);

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

        StaticGraphMmap {
            edges,
            in_edges,
            labels,
        }
    }

    #[inline(always)]
    pub fn num_nodes(&self) -> usize {
        self.edges.num_nodes()
    }

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

impl<Id: IdType + Copy + Ord, N: Copy + Ord, E: Copy + Ord> GraphTrait<Id>
for StaticGraphMmap<Id, N, E> {
    // TODO(longbin) To implement it later
    fn get_node(&self, _id: Id) -> NodeType<Id> {
        unimplemented!()
    }

    // TODO(longbin) To implement it later
    fn get_edge(&self, _start: Id, _target: Id) -> EdgeType<Id> {
        unimplemented!()
    }

    fn has_node(&self, id: Id) -> bool {
        id.id() < self.num_nodes()
    }

    fn has_edge(&self, start: Id, target: Id) -> bool {
        let neighbors = self.neighbors(start);
        // The neighbors must be sorted anyway
        let pos = neighbors.binary_search(&target);

        pos.is_ok()
    }

    fn node_count(&self) -> usize {
        self.num_nodes()
    }

    fn edge_count(&self) -> usize {
        if self.is_directed() {
            self.edges.num_edges()
        } else {
            self.edges.num_edges() >> 1
        }
    }

    fn is_directed(&self) -> bool {
        // A directed graph should have in-coming edges ready
        self.in_edges.is_some()
    }

    fn node_indices(&self) -> Iter<Id> {
        Iter::new(Box::new((0..self.num_nodes()).map(|x| Id::new(x))))
    }

    // TODO(longbin) Implement this later
    fn edge_indices(&self) -> Iter<(Id, Id)> {
        unimplemented!()
    }

    // TODO(longbin) Implement this later
    fn nodes(&self) -> Iter<NodeType<Id>> {
        unimplemented!()
    }

    // TODO(longbin) Implement this later
    fn edges(&self) -> Iter<EdgeType<Id>> {
        unimplemented!()
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

    fn num_of_neighbors(&self, id: Id) -> usize {
        self.degree(id)
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

impl<Id: IdType + Copy + Ord, N: Copy + Ord + Hash + Eq, E: Copy + Ord + Hash + Eq> GraphLabelTrait<Id, N, E>
for StaticGraphMmap<Id, N, E> {
    /// Lookup the node label by its id.
    fn get_node_label(&self, node_id: Id) -> Option<&N> {
        match self.labels {
            Some(ref labels) =>
                if self.has_node(node_id) {
                    Some(&labels[..][node_id.id()])
                } else {
                    None
                },
            None => None
        }
    }

    /// Lookup the edge label by its id.
    fn get_edge_label(&self, start: Id, target: Id) -> Option<&E> {
        self.edges.get_edge_label(start, target)
    }

    // TODO(longbin) Implement this later
    fn get_node_label_map(&self) -> &SetMap<N> {
        unimplemented!()
    }

    // TODO(longbin) Implement this later
    fn get_edge_label_map(&self) -> &SetMap<E> {
        unimplemented!()
    }
}

impl<Id: IdType + Copy + Ord, N: Copy + Ord, E: Copy + Ord> DiGraphTrait<Id>
for StaticGraphMmap<Id, N, E> {
    fn in_degree(&self, id: Id) -> usize {
        self.num_of_in_neighbors(id)
    }

    fn in_neighbors_iter(&self, id: Id) -> Iter<Id> {
        Iter::new(Box::new(self.inner_in_neighbors(id).iter().map(|x| *x)))
    }

    fn in_neighbors(&self, id: Id) -> Cow<[Id]> {
        self.inner_in_neighbors(id).into()
    }

    fn num_of_in_neighbors(&self, id: Id) -> usize {
        self.in_neighbors(id).len()
    }
}

impl<Id: IdType + Copy + Ord, N: Copy + Ord + Hash + Eq, E: Copy + Ord + Hash + Eq>  GeneralGraph<Id, N, E>
for StaticGraphMmap<Id, N, E> {
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

#[cfg(test)]
mod test {
    use graph_impl::EdgeVec;
    use super::*;

    fn remove_all(prefix: &str) -> Result<()> {
        let offsets_file = format!("{}.offsets", prefix);
        let edges_file = format!("{}.edges", prefix);
        let labels_file = format!("{}.labels", prefix);

        ::std::fs::remove_file(&offsets_file)?;
        ::std::fs::remove_file(&edges_file)?;

        ::std::fs::remove_file(&labels_file)
    }

    #[test]
    fn test_edge_vec_mmap() {
        let offsets = vec![0, 3, 5, 8, 10];
        let edges = vec![1, 2, 3, 0, 2, 0, 1, 3, 0, 2];

        let edgevec = EdgeVec::new(offsets, edges);
        edgevec.dump_mmap("edgevec").expect("Dump edgevec error");

        let edgevec_mmap = EdgeVecMmap::<u32, u32>::new("edgevec");

        assert_eq!(edgevec.num_nodes(), edgevec_mmap.num_nodes());
        for node in 0..edgevec.num_nodes() as u32 {
            assert_eq!(edgevec.neighbors(node), edgevec_mmap.neighbors(node))
        }
        for node in 0..edgevec.num_nodes() as u32 {
            for &nbr in edgevec_mmap.neighbors(node) {
                assert!(edgevec_mmap.get_edge_label(node, nbr).is_none());
            }
        }

        let _ = remove_all("edgevec");
    }

    #[test]
    fn test_edge_vec_mmap_label() {
        let offsets = vec![0, 3, 5, 8, 10];
        let edges = vec![1, 2, 3, 0, 2, 0, 1, 3, 0, 2];
        let labels = vec![0, 4, 3, 0, 1, 4, 1, 2, 3, 2];

        let edgevec = EdgeVec::with_labels(offsets, edges, labels);
        edgevec.dump_mmap("edgevecl").expect("Dump edgevec error");

        let edgevec_mmap = EdgeVecMmap::<u32, u32>::new("edgevecl");

        assert_eq!(edgevec.num_nodes(), edgevec_mmap.num_nodes());
        for node in 0..edgevec.num_nodes() as u32 {
            assert_eq!(edgevec.neighbors(node), edgevec_mmap.neighbors(node))
        }

        let expected_label = [[0, 0, 4, 3], [0, 0, 1, 0], [4, 1, 0, 2], [3, 0, 2, 0]];
        for node in 0..edgevec_mmap.num_nodes() as u32 {
            for &nbr in edgevec_mmap.neighbors(node) {
                assert_eq!(
                    *edgevec_mmap.get_edge_label(node, nbr).unwrap(),
                    expected_label[node.id()][nbr.id()]
                );
            }
        }

        let _ = remove_all("edgevecl").unwrap();
    }
}
