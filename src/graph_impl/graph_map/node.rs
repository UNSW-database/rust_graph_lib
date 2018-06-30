use std::collections::BTreeSet;
//use std::collections::HashSet;

use generic::IdType;
use generic::Iter;
use generic::node::{MutNodeMapTrait, NodeMapTrait};
use generic::{MutNodeTrait, NodeTrait};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NodeMap<Id: IdType> {
    id: Id,
    label: Option<Id>,
    edges: BTreeSet<Id>,
    in_edges: BTreeSet<Id>,
    //    edges: HashSet<Id>,
    //    in_edges: HashSet<Id>,
}

impl<Id: IdType> NodeMap<Id> {
    pub fn new(id: Id, label: Option<Id>) -> Self {
        NodeMap {
            id,
            label,
            edges: BTreeSet::<Id>::new(),
            in_edges: BTreeSet::<Id>::new(),
            //            edges: HashSet::<Id>::new(),
            //            in_edges: HashSet::<Id>::new(),
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
        let neighbors: Vec<Id> = self.edges.iter().cloned().collect();
        //        neighbors.sort();
        neighbors
    }

    fn in_neighbors(&self) -> Vec<Id> {
        let in_neighbors: Vec<Id> = self.in_edges.iter().cloned().collect();
        //        in_neighbors.sort();
        in_neighbors
    }
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
