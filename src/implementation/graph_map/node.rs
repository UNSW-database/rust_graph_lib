use std::collections::HashSet;

use generic::NodeTrait;
use generic::IndexIter;

#[derive(Debug, PartialEq, Clone)]
pub struct Node {
    id: usize,
    label: Option<usize>,
    in_edges: HashSet<usize>,
    out_edges: HashSet<usize>,
}

impl Node {
    pub fn new(id: usize, label: Option<usize>) -> Self {
        Node {
            id,
            label,
            in_edges: HashSet::<usize>::new(),
            out_edges: HashSet::<usize>::new(),
        }
    }
}

impl NodeTrait for Node {
    fn get_id(&self) -> usize {
        self.id
    }

    fn set_label(&mut self, label: usize) {
        self.label = Some(label);
    }

    fn get_label(&self) -> Option<usize> {
        self.label
    }
}

pub trait NodeMapTrait {
    fn has_in_neighbor(&self, id: usize) -> bool;
    fn has_out_neighbor(&self, id: usize) -> bool;
    fn add_in_edge(&mut self, adj: usize);
    fn add_out_edge(&mut self, adj: usize);
    fn remove_in_edge(&mut self, adj: usize) -> bool;
    fn remove_out_edge(&mut self, adj: usize) -> bool;
    fn in_degree(&self) -> usize;
    fn out_degree(&self) -> usize;
    fn in_neighbors<'a>(&'a self) -> IndexIter<'a>;
    fn out_neighbors<'a>(&'a self) -> IndexIter<'a>;
}

impl NodeMapTrait for Node {
    fn has_in_neighbor(&self, id: usize) -> bool {
        self.in_edges.contains(&id)
    }

    fn has_out_neighbor(&self, id: usize) -> bool {
        self.out_edges.contains(&id)
    }


    fn add_in_edge(&mut self, adj: usize) {
        if self.has_in_neighbor(adj) {
            panic!("Edge ({},{}) already exist.", adj, self.get_id());
        }
        self.in_edges.insert(adj);
    }

    fn add_out_edge(&mut self, adj: usize) {
        if self.has_out_neighbor(adj) {
            panic!("Edge ({},{}) already exist.", self.get_id(), adj);
        }
        self.out_edges.insert(adj);
    }

    fn remove_in_edge(&mut self, adj: usize) -> bool {
        self.out_edges.remove(&adj)
    }

    fn remove_out_edge(&mut self, adj: usize) -> bool {
        self.out_edges.remove(&adj)
    }

    fn in_degree(&self) -> usize {
        self.in_edges.len()
    }

    fn out_degree(&self) -> usize {
        self.out_edges.len()
    }

    fn in_neighbors<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.in_edges.iter().map(|i| { *i })))
    }

    fn out_neighbors<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.out_edges.iter().map(|i| { *i })))
    }
}