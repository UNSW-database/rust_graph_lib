/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

use std::collections::{BTreeMap, BTreeSet};

use crate::generic::{IdType, Iter, MutEdgeType, MutNodeTrait, NodeTrait, OwnedEdgeType};
use crate::graph_impl::graph_map::{Edge, MutEdge};

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NodeMap<Id: IdType, L: IdType = Id> {
    pub(crate) id: Id,
    pub(crate) label: Option<L>,
    pub(crate) neighbors: BTreeMap<Id, Option<L>>,
    pub(crate) in_neighbors: BTreeSet<Id>,
}

impl<Id: IdType, L: IdType> NodeMap<Id, L> {
    #[inline(always)]
    pub fn new(id: Id, label: Option<L>) -> Self {
        NodeMap {
            id,
            label,
            neighbors: BTreeMap::new(),
            in_neighbors: BTreeSet::new(),
        }
    }
}

pub trait NodeMapTrait<Id: IdType, L: IdType> {
    fn has_in_neighbor(&self, id: Id) -> bool;
    fn has_neighbor(&self, id: Id) -> bool;
    fn in_degree(&self) -> usize;
    fn degree(&self) -> usize;
    fn neighbors_iter(&self) -> Iter<'_, Id>;
    fn in_neighbors_iter(&self) -> Iter<'_, Id>;
    fn neighbors(&self) -> Vec<Id>;
    fn in_neighbors(&self) -> Vec<Id>;
    fn get_neighbor(&self, id: Id) -> Option<Option<L>>;
    fn non_less_neighbors_iter(&self) -> Iter<'_, Id>;
    fn neighbors_iter_full(&self) -> Iter<'_, Edge<Id, L>>;
    fn non_less_neighbors_iter_full(&self) -> Iter<'_, Edge<Id, L>>;
}

pub trait MutNodeMapTrait<Id: IdType, L: IdType> {
    fn add_in_edge(&mut self, adj: Id) -> bool;
    fn add_edge(&mut self, adj: Id, label: Option<L>) -> bool;
    fn remove_in_edge(&mut self, adj: Id) -> bool;
    fn remove_edge(&mut self, adj: Id) -> OwnedEdgeType<Id, L>;
    fn get_neighbor_mut(&mut self, id: Id) -> MutEdgeType<'_, Id, L>;
    fn neighbors_iter_mut(&mut self) -> Iter<'_, MutEdgeType<'_, Id, L>>;
    fn non_less_neighbors_iter_mut(&mut self) -> Iter<'_, MutEdgeType<'_, Id, L>>;
}

impl<Id: IdType, L: IdType> NodeTrait<Id, L> for NodeMap<Id, L> {
    #[inline(always)]
    fn get_id(&self) -> Id {
        self.id
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        self.label
    }
}

impl<Id: IdType, L: IdType> MutNodeTrait<Id, L> for NodeMap<Id, L> {
    #[inline(always)]
    fn set_label_id(&mut self, label: Option<L>) {
        self.label = label;
    }
}

impl<Id: IdType, L: IdType> NodeMapTrait<Id, L> for NodeMap<Id, L> {
    #[inline]
    fn has_in_neighbor(&self, id: Id) -> bool {
        self.in_neighbors.contains(&id)
    }

    #[inline]
    fn has_neighbor(&self, id: Id) -> bool {
        self.neighbors.contains_key(&id)
    }

    #[inline]
    fn in_degree(&self) -> usize {
        self.in_neighbors.len()
    }

    #[inline]
    fn degree(&self) -> usize {
        self.neighbors.len()
    }

    #[inline]
    fn neighbors_iter(&self) -> Iter<'_, Id> {
        Iter::new(Box::new(self.neighbors.keys().map(|x| *x)))
    }

    #[inline]
    fn in_neighbors_iter(&self) -> Iter<'_, Id> {
        Iter::new(Box::new(self.in_neighbors.iter().map(|x| *x)))
    }

    #[inline]
    fn neighbors(&self) -> Vec<Id> {
        self.neighbors.keys().cloned().collect()
    }

    #[inline]
    fn in_neighbors(&self) -> Vec<Id> {
        self.in_neighbors.iter().cloned().collect()
    }

    #[inline]
    fn get_neighbor(&self, id: Id) -> Option<Option<L>> {
        self.neighbors.get(&id).map(|x| *x)
    }

    #[inline]
    fn non_less_neighbors_iter(&self) -> Iter<'_, Id> {
        Iter::new(Box::new(self.neighbors.range(self.id..).map(|(&id, _)| id)))
    }

    #[inline]
    fn neighbors_iter_full(&self) -> Iter<'_, Edge<Id, L>> {
        let nid = self.id;

        Iter::new(Box::new(
            self.neighbors
                .iter()
                .map(move |(&n, &l)| Edge::new(nid, n, l)),
        ))
    }

    #[inline]
    fn non_less_neighbors_iter_full(&self) -> Iter<'_, Edge<Id, L>> {
        let nid = self.id;

        Iter::new(Box::new(
            self.neighbors
                .range(self.get_id()..)
                .map(move |(&n, &l)| Edge::new(nid, n, l)),
        ))
    }
}

impl<Id: IdType, L: IdType> MutNodeMapTrait<Id, L> for NodeMap<Id, L> {
    #[inline]
    fn add_in_edge(&mut self, adj: Id) -> bool {
        if self.has_in_neighbor(adj) {
            return false;
        }
        self.in_neighbors.insert(adj);

        true
    }

    #[inline]
    fn add_edge(&mut self, adj: Id, label: Option<L>) -> bool {
        let mut result = false;
        let edge_label = self.neighbors.entry(adj).or_insert_with(|| {
            result = true;

            None
        });
        *edge_label = label;

        result
    }

    #[inline]
    fn remove_in_edge(&mut self, adj: Id) -> bool {
        self.in_neighbors.remove(&adj)
    }

    #[inline]
    fn remove_edge(&mut self, adj: Id) -> OwnedEdgeType<Id, L> {
        match self.neighbors.remove(&adj) {
            Some(edge) => OwnedEdgeType::Edge(Edge::new(self.get_id(), adj, edge)),
            None => OwnedEdgeType::None,
        }
    }

    #[inline]
    fn get_neighbor_mut(&mut self, id: Id) -> MutEdgeType<'_, Id, L> {
        let nid = self.get_id();
        match self.neighbors.get_mut(&id) {
            Some(edge) => MutEdgeType::EdgeRef(MutEdge::new(nid, id, edge)),
            None => MutEdgeType::None,
        }
    }

    #[inline]
    fn neighbors_iter_mut(&mut self) -> Iter<'_, MutEdgeType<'_, Id, L>> {
        let nid = self.get_id();
        Iter::new(Box::new(self.neighbors.iter_mut().map(move |(n, l)| {
            MutEdgeType::EdgeRef(MutEdge::new(nid, *n, l))
        })))
    }

    #[inline]
    fn non_less_neighbors_iter_mut(&mut self) -> Iter<'_, MutEdgeType<'_, Id, L>> {
        let nid = self.get_id();
        Iter::new(Box::new(self.neighbors.range_mut(self.id..).map(
            move |(n, l)| MutEdgeType::EdgeRef(MutEdge::new(nid, *n, l)),
        )))
    }
}
