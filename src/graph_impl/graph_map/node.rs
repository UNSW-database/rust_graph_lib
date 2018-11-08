use std::collections::{BTreeMap, BTreeSet};

use generic::node::{MutNodeMapTrait, NodeMapTrait};
use generic::{IdType, Iter, MutNodeTrait, NodeTrait};
use graph_impl::Edge;

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NodeMap<Id: IdType, L: IdType = Id> {
    pub(crate) id: Id,
    pub(crate) label: Option<L>,
    pub(crate) neighbors: BTreeMap<Id, Option<L>>,
    pub(crate) in_neighbors: BTreeSet<Id>,
}

impl<Id: IdType, L: IdType> NodeMap<Id, L> {
    pub fn new(id: Id, label: Option<L>) -> Self {
        NodeMap {
            id,
            label,
            neighbors: BTreeMap::new(),
            in_neighbors: BTreeSet::new(),
        }
    }
}

impl<Id: IdType, L: IdType> NodeTrait<Id, L> for NodeMap<Id, L> {
    fn get_id(&self) -> Id {
        self.id
    }

    fn get_label_id(&self) -> Option<L> {
        self.label
    }
}

impl<Id: IdType, L: IdType> MutNodeTrait<Id, L> for NodeMap<Id, L> {
    fn set_label_id(&mut self, label: Option<L>) {
        self.label = label;
    }
}

impl<Id: IdType, L: IdType> NodeMapTrait<Id, L> for NodeMap<Id, L> {
    fn has_in_neighbor(&self, id: Id) -> bool {
        self.in_neighbors.contains(&id)
    }

    fn has_neighbor(&self, id: Id) -> bool {
        self.neighbors.contains_key(&id)
    }

    fn in_degree(&self) -> usize {
        self.in_neighbors.len()
    }

    fn degree(&self) -> usize {
        self.neighbors.len()
    }

    fn neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.neighbors.keys().map(|x| *x)))
    }

    fn in_neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.in_neighbors.iter().map(|x| *x)))
    }

    fn neighbors(&self) -> Vec<Id> {
        self.neighbors.keys().cloned().collect()
    }

    fn in_neighbors(&self) -> Vec<Id> {
        self.in_neighbors.iter().cloned().collect()
    }

    fn get_neighbor(&self, id: Id) -> Option<Option<L>> {
        self.neighbors.get(&id).map(|x| *x)
    }

    fn non_less_neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.neighbors.range(self.id..).map(|(&id, _)| id)))
    }

    fn neighbors_iter_full(&self) -> Iter<Edge<Id, L>> {
        let nid = self.id;

        Iter::new(Box::new(
            self.neighbors
                .iter()
                .map(move |(&n, &l)| Edge::new(nid, n, l)),
        ))
    }

    fn non_less_neighbors_iter_full(&self) -> Iter<Edge<Id, L>> {
        let nid = self.id;

        Iter::new(Box::new(
            self.neighbors
                .range(self.get_id()..)
                .map(move |(&n, &l)| Edge::new(nid, n, l)),
        ))
    }
}

impl<Id: IdType, L: IdType> MutNodeMapTrait<Id, L> for NodeMap<Id, L> {
    fn add_in_edge(&mut self, adj: Id) -> bool {
        if self.has_in_neighbor(adj) {
            return false;
        }
        self.in_neighbors.insert(adj);

        true
    }

    fn add_edge(&mut self, adj: Id, label: Option<L>) -> bool {
        let mut result = false;
        let edge_label = self.neighbors.entry(adj).or_insert_with(|| {
            result = true;

            None
        });
        *edge_label = label;

        result
    }

    fn remove_in_edge(&mut self, adj: Id) -> bool {
        self.in_neighbors.remove(&adj)
    }

    fn remove_edge(&mut self, adj: Id) -> Option<Option<Id>> {
        self.neighbors.remove(&adj)
    }

    fn get_neighbor_mut(&mut self, id: Id) -> Option<&mut Option<L>> {
        self.neighbors.get_mut(&id)
    }

    fn neighbors_iter_mut(&mut self) -> Iter<&mut Option<L>> {
        Iter::new(Box::new(self.neighbors.values_mut()))
    }

    fn non_less_neighbors_iter_mut(&mut self) -> Iter<&mut Option<L>> {
        Iter::new(Box::new(
            self.neighbors.range_mut(self.id..).map(|(_, label)| label),
        ))
    }
}
