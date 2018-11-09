use generic::IdType;
use io::mmap::dump;
use std::fs::File;
use std::io::Result;

/// With the node indexed from 0 .. num_nodes - 1, we can maintain the edges in a compact way,
/// using `offset` and `edges`, in which `offset[node]` maintain the start index of the given
/// node's neighbors in `edges`. Thus, the node's neighbors is maintained in:
/// `edges[offsets[node]]` (included) to `edges[offsets[node+1]]` (excluded),
///
/// *Note*: The edges must be sorted according to the starting node, that is,
/// The sub-vector `edges[offsets[node]]` (included) - `edges[offsets[node + 1]]` (excluded)
/// for any `node` should be sorted.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EdgeVec<Id: IdType, L: IdType = Id> {
    offsets: Vec<usize>,
    edges: Vec<Id>,
    labels: Option<Vec<L>>,
}

pub trait EdgeVecTrait<Id: IdType, L: IdType> {
    fn get_offsets(&self) -> &[usize];
    fn get_edges(&self) -> &[Id];
    fn get_labels(&self) -> &[L];

    #[inline]
    fn num_nodes(&self) -> usize {
        self.get_offsets().len() - 1
    }

    #[inline]
    fn num_edges(&self) -> usize {
        self.get_edges().len()
    }

    #[inline]
    fn neighbors(&self, node: Id) -> &[Id] {
        assert!(self.has_node(node));
        let start = self.get_offsets()[node.id()].id();
        let end = self.get_offsets()[node.id() + 1].id();

        &self.get_edges()[start..end]
    }

    #[inline]
    fn degree(&self, node: Id) -> usize {
        assert!(self.has_node(node));
        let start = self.get_offsets()[node.id()].id();
        let end = self.get_offsets()[node.id() + 1].id();

        end - start
    }

    #[inline]
    fn has_node(&self, node: Id) -> bool {
        node.id() < self.num_nodes()
    }

    #[inline]
    fn find_edge_index(&self, start: Id, target: Id) -> Option<usize> {
        if !(self.has_node(start) && self.has_node(target)) {
            None
        } else {
            let neighbors = self.neighbors(start);
            let found = neighbors.binary_search(&target);
            match found {
                Err(_) => None,
                Ok(idx) => Some(self.get_offsets()[start.id()].id() + idx),
            }
        }
    }

    #[inline]
    fn find_edge_label_id(&self, start: Id, target: Id) -> Option<&L> {
        let labels = self.get_labels();

        if labels.is_empty() {
            return None;
        }

        let idx_opt = self.find_edge_index(start, target);
        match idx_opt {
            None => None,
            Some(idx) => labels.get(idx),
        }
    }

    #[inline]
    fn has_edge(&self, start: Id, target: Id) -> bool {
        self.find_edge_index(start, target).is_some()
    }
}

impl<Id: IdType, L: IdType> EdgeVec<Id, L> {
    pub fn new(offsets: Vec<usize>, edges: Vec<Id>) -> Self {
        EdgeVec {
            offsets,
            edges,
            labels: None,
        }
    }

    pub fn with_labels(offsets: Vec<usize>, edges: Vec<Id>, labels: Vec<L>) -> Self {
        if edges.len() != labels.len() {
            panic!(
                "Unequal length: there are {} edges, but {} labels",
                edges.len(),
                labels.len()
            );
        }
        EdgeVec {
            offsets,
            edges,
            labels: Some(labels),
        }
    }

    pub fn from_raw(offsets: Vec<usize>, edges: Vec<Id>, labels: Option<Vec<L>>) -> Self {
        match labels {
            Some(labels) => EdgeVec::with_labels(offsets, edges, labels),
            None => EdgeVec::new(offsets, edges),
        }
    }

    pub fn remove_labels(&mut self) {
        self.labels = None;
    }

    pub fn clear(&mut self) {
        self.offsets.clear();
        self.edges.clear();
        if let Some(ref mut labels) = self.labels {
            labels.clear();
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.edges.shrink_to_fit();
        if let Some(ref mut labels) = self.labels {
            labels.shrink_to_fit();
        }
    }

    /// Dump self to bytearray in order to be deserialised as `EdgeVecMmap`.
    pub fn dump_mmap(&self, prefix: &str) -> Result<()> {
        let offsets_file = format!("{}.offsets", prefix);
        let edges_file = format!("{}.edges", prefix);
        let labels_file = format!("{}.labels", prefix);

        unsafe {
            dump(self.get_offsets(), File::create(offsets_file)?)?;
            dump(self.get_edges(), File::create(edges_file)?)?;

            if self.get_labels().len() != 0 {
                dump(self.get_labels(), File::create(labels_file)?)
            } else {
                Ok(())
            }
        }
    }
}

impl<Id: IdType, L: IdType> EdgeVecTrait<Id, L> for EdgeVec<Id, L> {
    #[inline]
    fn get_offsets(&self) -> &[usize] {
        &self.offsets
    }

    #[inline]
    fn get_edges(&self) -> &[Id] {
        &self.edges
    }

    #[inline]
    fn get_labels(&self) -> &[L] {
        match self.labels {
            Some(ref labels) => &labels[..],
            None => &[],
        }
    }
}

impl<Id: IdType, L: IdType> Default for EdgeVec<Id, L> {
    fn default() -> Self {
        EdgeVec::new(Vec::new(), Vec::new())
    }
}
