use std::collections::HashSet;

use generic::{NodeTrait, MutNodeTrait};
use generic::IndexIter;

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct NodeMap {
    id: usize,
    label: Option<usize>,
    edges: HashSet<usize>,
    in_edges: HashSet<usize>,
}

impl NodeMap {
    pub fn new(id: usize, label: Option<usize>) -> Self {
        NodeMap {
            id,
            label,
            edges: HashSet::<usize>::new(),
            in_edges: HashSet::<usize>::new(),
        }
    }
}

impl NodeTrait for NodeMap {
    fn get_id(&self) -> usize {
        self.id
    }

    fn get_label_id(&self) -> Option<usize> {
        self.label
    }
}

impl MutNodeTrait for NodeMap {
    fn set_label_id(&mut self, label: usize) {
        self.label = Some(label);
    }
}

pub trait NodeMapTrait {
    fn has_in_neighbor(&self, id: usize) -> bool;
    fn has_neighbor(&self, id: usize) -> bool;
    fn add_in_edge(&mut self, adj: usize);
    fn add_edge(&mut self, adj: usize);
    fn remove_in_edge(&mut self, adj: usize) -> bool;
    fn remove_edge(&mut self, adj: usize) -> bool;
    fn in_degree(&self) -> usize;
    fn degree(&self) -> usize;
    fn in_neighbors<'a>(&'a self) -> IndexIter<'a>;
    fn neighbors<'a>(&'a self) -> IndexIter<'a>;
}

impl NodeMapTrait for NodeMap {
    fn has_in_neighbor(&self, id: usize) -> bool {
        self.in_edges.contains(&id)
    }

    fn has_neighbor(&self, id: usize) -> bool {
        self.edges.contains(&id)
    }


    fn add_in_edge(&mut self, adj: usize) {
        if self.has_in_neighbor(adj) {
            panic!("Edge ({},{}) already exist.", adj, self.get_id());
        }
        self.in_edges.insert(adj);
    }

    fn add_edge(&mut self, adj: usize) {
        if self.has_neighbor(adj) {
            panic!("Edge ({},{}) already exist.", self.get_id(), adj);
        }
        self.edges.insert(adj);
    }

    fn remove_in_edge(&mut self, adj: usize) -> bool {
        self.edges.remove(&adj)
    }

    fn remove_edge(&mut self, adj: usize) -> bool {
        self.edges.remove(&adj)
    }

    fn in_degree(&self) -> usize {
        self.in_edges.len()
    }

    fn degree(&self) -> usize {
        self.edges.len()
    }

    fn in_neighbors<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.in_edges.iter().map(|i| { *i })))
    }

    fn neighbors<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.edges.iter().map(|i| { *i })))
    }
}