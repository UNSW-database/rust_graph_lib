//! This file defines a mmap version of `StaticGraph`, so that when the graph is huge,
//! we can rely on mmap to save physical memory usage.
//!
use graph_impl::static_graph::EdgeVec;
use io::mmap::{typed_as_byte_slice, TypedMemoryMap};
use prelude::IdType;
use std::fs::{metadata, File};
use std::io::{BufWriter, Result, Write};
use std::path::Path;

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

    #[inline(always)]
    pub fn neighbors(&self, node: Id) -> &[Id] {
        self.edges.neighbors(node)
    }

    #[inline(always)]
    pub fn in_neighbors(&self, node: Id) -> &[Id] {
        if let Some(ref in_edges) = self.in_edges {
            in_edges.neighbors(node)
        } else {
            &[]
        }
    }

    #[inline(always)]
    pub fn get_node_label(&self, node: Id) -> Option<&N> {
        if let Some(ref labels) = self.labels {
            if node.id() < self.num_nodes() {
                Some(&labels[..][node.id()])
            } else {
                None
            }
        } else {
            None
        }
    }

    #[inline(always)]
    pub fn get_edge_label(&self, src: Id, dst: Id) -> Option<&E> {
        self.edges.get_edge_label(src, dst)
    }
}

#[cfg(test)]
mod test {
    extern crate tempfile;

    use self::tempfile::tempfile;
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
