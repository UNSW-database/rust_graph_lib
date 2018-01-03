use std::collections::HashMap;

use generic::NodeTrait;
use generic::IndexIter;

#[derive(Debug, PartialEq, Clone)]
pub struct Node {
    id: usize,
    label: Option<usize>,
    in_edges: HashMap<usize, usize>,
    // <adj node:edge id>
    out_edges: HashMap<usize, usize>,
}

impl Node {
    pub fn new(id: usize, label: Option<usize>) -> Self {
        Node {
            id,
            label,
            in_edges: HashMap::<usize, usize>::new(),
            out_edges: HashMap::<usize, usize>::new(),
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

impl Node {
    pub fn add_in_edge(&mut self, adj: usize, edge: usize) {
        match self.get_in_edge(adj) {
            Some(_) => { panic!("Edge ({},{}) already exist.", adj, self.get_id()); }
            None => { self.in_edges.insert(adj, edge); }
        }
    }

    pub fn add_out_edge(&mut self, adj: usize, edge: usize) {
        match self.get_out_edge(adj) {
            Some(_) => { panic!("Edge ({},{}) already exist.", self.get_id(), adj); }
            None => { self.out_edges.insert(adj, edge); }
        }
    }

    pub fn get_in_edge(&self, adj: usize) -> Option<usize> {
        self.in_edges.get(&adj).map(|x| *x)
    }

    pub fn get_out_edge(&self, adj: usize) -> Option<usize> {
        self.out_edges.get(&adj).map(|x| *x)
    }

    pub fn remove_in_edge(&mut self, adj: usize) -> Option<usize> {
        self.out_edges.remove(&adj)
    }

    pub fn remove_out_edge(&mut self, adj: usize) -> Option<usize> {
        self.out_edges.remove(&adj)
    }

    pub fn in_degree(&self) -> usize {
        self.in_edges.len()
    }

    pub fn out_degree(&self) -> usize {
        self.out_edges.len()
    }

    pub fn in_neighbors<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.in_edges.keys().map(|i| { *i })))
    }

    pub fn out_neighbors<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.out_edges.keys().map(|i| { *i })))
    }

    pub fn in_edges<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.in_edges.values().map(|i| { *i })))
    }

    pub fn out_edges<'a>(&'a self) -> IndexIter<'a> {
        IndexIter::new(Box::new(self.out_edges.values().map(|i| { *i })))
    }
}