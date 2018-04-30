use std::collections::HashSet;

use generic::IdType;
use generic::{MutNodeTrait, NodeTrait};
use generic::IndexIter;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NodeMap<Id: IdType> {
    id: Id,
    label: Option<Id>,
    edges: HashSet<Id>,
    in_edges: HashSet<Id>,
}

impl<Id: IdType> NodeMap<Id> {
    pub fn new(id: usize, label: Option<usize>) -> Self {
        NodeMap {
            id: Id::new(id),
            label: label.map(Id::new),
            edges: HashSet::<Id>::new(),
            in_edges: HashSet::<Id>::new(),
        }
    }
}

impl<Id: IdType> NodeTrait for NodeMap<Id> {
    fn get_id(&self) -> usize {
        self.id.id()
    }

    fn get_label_id(&self) -> Option<usize> {
        match self.label {
            Some(ref x) => Some(x.id()),
            None => None,
        }
    }
}

impl<Id: IdType> MutNodeTrait for NodeMap<Id> {
    fn set_label_id(&mut self, label: Option<usize>) {
        self.label = label.map(Id::new);
    }
}

impl<Id: IdType> NodeMap<Id> {
    pub fn has_in_neighbor(&self, id: usize) -> bool {
        self.in_edges.contains(&Id::new(id))
    }

    pub fn has_neighbor(&self, id: usize) -> bool {
        self.edges.contains(&Id::new(id))
    }

    pub fn in_degree(&self) -> usize {
        self.in_edges.len()
    }

    pub fn degree(&self) -> usize {
        self.edges.len()
    }

    pub fn in_neighbors(&self) -> IndexIter {
        IndexIter::new(Box::new(self.in_edges.iter().map(|i| i.id())))
    }

    pub fn neighbors(&self) -> IndexIter {
        IndexIter::new(Box::new(self.edges.iter().map(|i| i.id())))
    }
}

pub trait MutNodeMapTrait {
    fn add_in_edge(&mut self, adj: usize);
    fn add_edge(&mut self, adj: usize);
    fn remove_in_edge(&mut self, adj: usize) -> bool;
    fn remove_edge(&mut self, adj: usize) -> bool;
}

impl<Id: IdType> MutNodeMapTrait for NodeMap<Id> {
    fn add_in_edge(&mut self, adj: usize) {
        if self.has_in_neighbor(adj) {
            panic!("Edge ({},{}) already exist.", adj, self.get_id());
        }
        self.in_edges.insert(Id::new(adj));
    }

    fn add_edge(&mut self, adj: usize) {
        if self.has_neighbor(adj) {
            panic!("Edge ({},{}) already exist.", self.get_id(), adj);
        }
        self.edges.insert(Id::new(adj));
    }

    fn remove_in_edge(&mut self, adj: usize) -> bool {
        self.edges.remove(&Id::new(adj))
    }

    fn remove_edge(&mut self, adj: usize) -> bool {
        self.edges.remove(&Id::new(adj))
    }
}
