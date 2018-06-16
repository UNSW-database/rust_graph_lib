use std::collections::HashSet;

use generic::IdType;
use generic::Iter;
use generic::{MutNodeTrait, NodeTrait};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NodeMap<Id: IdType> {
    id: Id,
    label: Option<Id>,
    edges: HashSet<Id>,
    in_edges: HashSet<Id>,
}

impl<Id: IdType> NodeMap<Id> {
    pub fn new(id: Id, label: Option<Id>) -> Self {
        NodeMap {
            id,
            label,
            edges: HashSet::<Id>::new(),
            in_edges: HashSet::<Id>::new(),
        }
    }
}

impl<Id: IdType> NodeTrait<Id> for NodeMap<Id> {
    fn get_id(&self) -> Id {
        self.id
    }

    fn get_label_id(&self) -> Option<Id> {
        self.label
    }
}

impl<Id: IdType> MutNodeTrait<Id> for NodeMap<Id> {
    fn set_label_id(&mut self, label: Option<Id>) {
        self.label = label;
    }
}

pub trait NodeMapTrait<Id> {
    fn has_in_neighbor(&self, id: Id) -> bool;
    fn has_neighbor(&self, id: Id) -> bool;
    fn in_degree(&self) -> usize;
    fn degree(&self) -> usize;
    fn neighbors_iter(&self) -> Iter<Id>;
    fn in_neighbors_iter(&self) -> Iter<Id>;
    fn neighbors(&self) -> Vec<Id>;
    fn in_neighbors(&self) -> Vec<Id>;
}

impl<Id: IdType> NodeMapTrait<Id> for NodeMap<Id> {
    fn has_in_neighbor(&self, id: Id) -> bool {
        self.in_edges.contains(&id)
    }

    fn has_neighbor(&self, id: Id) -> bool {
        self.edges.contains(&id)
    }

    fn in_degree(&self) -> usize {
        self.in_edges.len()
    }

    fn degree(&self) -> usize {
        self.edges.len()
    }

    fn neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.edges.iter().map(|x| *x)))
    }

    fn in_neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.in_edges.iter().map(|x| *x)))
    }

    fn neighbors(&self) -> Vec<Id> {
        self.edges.iter().cloned().collect()
    }

    fn in_neighbors(&self) -> Vec<Id> {
        self.in_edges.iter().cloned().collect()
    }
}

pub trait MutNodeMapTrait<Id> {
    fn add_in_edge(&mut self, adj: Id);
    fn add_edge(&mut self, adj: Id);
    fn remove_in_edge(&mut self, adj: Id) -> bool;
    fn remove_edge(&mut self, adj: Id) -> bool;
}

impl<Id: IdType> MutNodeMapTrait<Id> for NodeMap<Id> {
    fn add_in_edge(&mut self, adj: Id) {
        if self.has_in_neighbor(adj) {
            panic!("Edge ({},{}) already exist.", adj, self.get_id());
        }
        self.in_edges.insert(adj);
    }

    fn add_edge(&mut self, adj: Id) {
        if self.has_neighbor(adj) {
            panic!("Edge ({},{}) already exist.", self.get_id(), adj);
        }
        self.edges.insert(adj);
    }

    fn remove_in_edge(&mut self, adj: Id) -> bool {
        self.edges.remove(&adj)
    }

    fn remove_edge(&mut self, adj: Id) -> bool {
        self.edges.remove(&adj)
    }
}
