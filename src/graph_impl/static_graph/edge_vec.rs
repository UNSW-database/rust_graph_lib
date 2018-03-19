/// With the node indexed from 0 .. num_nodes - 1, we can maintain the edges in a compact way,
/// using `offset` and `edges`, in which `offset[node]` maintain the start index of the given
/// node's neighbors in `edges`. Thus, the node's neighbors is maintained in:
/// `edges[offsets[node]]` (included) to `edges[offsets[node+1]]` (excluded),
///
/// *Note*: The edges must be sorted according to the starting node, that is,
/// The sub-vector `edges[offsets[node]]` (included) - `edges[offsets[node + 1]]` (excluded)
/// for any `node` should be sorted.
#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
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

    pub fn get_labels(&self) -> &[usize] {
        match self.labels {
            Some(ref labels) => &labels[..],
            None => &[],
        }
    }

    // Get the neighbours of a given `node`.
    pub fn neighbors(&self, node: usize) -> &[usize] {
        assert!(self.valid_node(node));
        let start = self.offsets[node];
        let end = self.offsets[node + 1];
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
            let found = neighbors.binary_search(&target);
            match found {
                Err(_) => None,
                Ok(idx) => Some(self.offsets[start] + idx),
            }
        }
    }

    pub fn has_edge(&self, start: usize, target: usize) -> bool {
        self.find_edge_index(start, target).is_some()
    }

    pub fn find_edge_label(&self, start: usize, target: usize) -> Option<&usize> {
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
        node < self.offsets.len() - 1
    }
}
