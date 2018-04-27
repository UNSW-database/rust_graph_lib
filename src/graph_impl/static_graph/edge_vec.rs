use generic::IdType;

/// With the node indexed from 0 .. num_nodes - 1, we can maintain the edges in a compact way,
/// using `offset` and `edges`, in which `offset[node]` maintain the start index of the given
/// node's neighbors in `edges`. Thus, the node's neighbors is maintained in:
/// `edges[offsets[node]]` (included) to `edges[offsets[node+1]]` (excluded),
///
/// *Note*: The edges must be sorted according to the starting node, that is,
/// The sub-vector `edges[offsets[node]]` (included) - `edges[offsets[node + 1]]` (excluded)
/// for any `node` should be sorted.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EdgeVec<Id: IdType> {
    offsets: Vec<Id>,
    edges: Vec<Id>,
    // Maintain the corresponding edge's labels if exist, aligned with `edges`.
    // Note that the label has been encoded as an Integer.
    labels: Option<Vec<Id>>,
}

impl<Id: IdType> EdgeVec<Id> {
    pub fn new(offsets: Vec<Id>, edges: Vec<Id>) -> Self {
        EdgeVec {
            offsets,
            edges,
            labels: None,
        }
    }

    pub fn with_labels(offsets: Vec<Id>, edges: Vec<Id>, labels: Vec<Id>) -> Self {
        assert_eq!(edges.len(), labels.len());
        EdgeVec {
            offsets,
            edges,
            labels: Some(labels),
        }
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
        match self.labels {
            Some(ref mut labels) => {
                labels.shrink_to_fit();
            }
            None => {}
        }
    }

    pub fn num_nodes(&self) -> usize {
        self.offsets.len() - 1
    }

    pub fn len(&self) -> usize {
        self.edges.len()
    }

    pub fn get_labels(&self) -> &[Id] {
        match self.labels {
            Some(ref labels) => &labels[..],
            None => &[],
        }
    }

    pub fn get_offsets(&self) -> &[Id] {
        &self.offsets[..]
    }

    pub fn get_edges(&self) -> &[Id] {
        &self.edges[..]
    }

    // Get the neighbours of a given `node`.
    pub fn neighbors(&self, node: usize) -> &[Id] {
        assert!(self.valid_node(node));
        let start = self.offsets[node].id();
        let end = self.offsets[node + 1].id();
        //        assert!(start < self.edges.len() && end <= self.edges.len());
        &self.edges[start..end]
    }

    pub fn degree(&self, node: usize) -> usize {
        self.neighbors(node).len()
    }

    /// Given a both ends of the edges, `start` and `target`, locate its index
    /// in the edge vector, if the corresponding edge exists.
    pub fn find_edge_index(&self, start: usize, target: usize) -> Option<usize> {
        if !(self.valid_node(start) && self.valid_node(target)) {
            None
        } else {
            let neighbors = self.neighbors(start);
            let found = neighbors.binary_search(&Id::new(target));
            match found {
                Err(_) => None,
                Ok(idx) => Some(self.offsets[start].id() + idx),
            }
        }
    }

    pub fn has_edge(&self, start: usize, target: usize) -> bool {
        self.find_edge_index(start, target).is_some()
    }

    pub fn find_edge_label(&self, start: usize, target: usize) -> Option<&Id> {
        match self.labels {
            None => None,
            Some(ref labels) => {
                let idx_opt = self.find_edge_index(start, target);
                match idx_opt {
                    None => None,
                    Some(idx) => labels.get(idx),
                }
            }
        }
    }

    // Verify whether a given `node` is a valid node id.
    // Suppose the maximum node id is `m`, then we must have offsets[m+1], therefore
    // given a node, we must have `node <= m < offsets.len - 1`
    fn valid_node(&self, node: usize) -> bool {
        node < self.num_nodes()
    }
}
