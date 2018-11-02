use std::collections::BTreeSet;

use generic::node::{MutNodeMapTrait, NodeMapTrait};
use generic::IdType;
use generic::Iter;
use generic::{MutNodeTrait, NodeTrait};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NodeMap<Id: IdType> {
    pub(crate) id: Id,
    pub(crate) label: Option<Id>,
    pub(crate) neighbors: BTreeSet<Id>,
    pub(crate) in_neighbors: BTreeSet<Id>,
}

impl<Id: IdType> NodeMap<Id> {
    pub fn new(id: Id, label: Option<Id>) -> Self {
        NodeMap {
            id,
            label,
            neighbors: BTreeSet::<Id>::new(),
            in_neighbors: BTreeSet::<Id>::new(),
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

impl<Id: IdType> NodeMapTrait<Id> for NodeMap<Id> {
    fn has_in_neighbor(&self, id: Id) -> bool {
        self.in_neighbors.contains(&id)
    }

    fn has_neighbor(&self, id: Id) -> bool {
        self.neighbors.contains(&id)
    }

    fn in_degree(&self) -> usize {
        self.in_neighbors.len()
    }

    fn degree(&self) -> usize {
        self.neighbors.len()
    }

    fn neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.neighbors.iter().map(|x| *x)))
    }

    fn in_neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.in_neighbors.iter().map(|x| *x)))
    }

    fn neighbors(&self) -> Vec<Id> {
        let neighbors: Vec<Id> = self.neighbors.iter().cloned().collect();

        neighbors
    }

    fn in_neighbors(&self) -> Vec<Id> {
        let in_neighbors: Vec<Id> = self.in_neighbors.iter().cloned().collect();

        in_neighbors
    }

    fn num_of_neighbors(&self) -> usize {
        self.neighbors.len()
    }

    fn num_of_in_neighbors(&self) -> usize {
        self.in_neighbors.len()
    }
}

impl<Id: IdType> MutNodeMapTrait<Id> for NodeMap<Id> {
    fn add_in_edge(&mut self, adj: Id) -> bool {
        if self.has_in_neighbor(adj) {
            warn!(
                "NodeMap::add_in_edge - Edge ({},{}) already exist, ignoring.",
                adj,
                self.get_id()
            );

            return false;
        }
        self.in_neighbors.insert(adj);

        true
    }

    fn add_edge(&mut self, adj: Id) -> bool {
        if self.has_neighbor(adj) {
            warn!(
                "NodeMap::add_edge - Edge ({},{}) already exist, ignoring.",
                self.get_id(),
                adj
            );

            return false;
        }
        self.neighbors.insert(adj);

        true
    }

    fn remove_in_edge(&mut self, adj: Id) -> bool {
        self.in_neighbors.remove(&adj)
    }

    fn remove_edge(&mut self, adj: Id) -> bool {
        self.neighbors.remove(&adj)
    }
}
