/*
 * Copyright (c) 2018 UNSW Sydney, Data and Knowledge Group.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use hashbrown::HashMap;

use generic::{IdType, Iter, MutEdgeType, MutNodeTrait, NodeTrait, OwnedEdgeType};
use graph_impl::graph_map::{Edge, MutEdge};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct NodeMap<Id: IdType, L: IdType = Id> {
    pub(crate) id: Id,
    pub(crate) label: Option<L>,
    pub(crate) neighbors: HashMap<Id, Option<L>>,
    pub(crate) neighbors_vec: Vec<Id>,
    pub(crate) in_neighbors: Vec<Id>,
}

impl<Id: IdType, L: IdType> NodeMap<Id, L> {
    #[inline(always)]
    pub fn new(id: Id, label: Option<L>) -> Self {
        NodeMap {
            id,
            label,
            neighbors: HashMap::new(),
            neighbors_vec: Vec::new(),
            in_neighbors: Vec::new(),
        }
    }
}

pub trait NodeMapTrait<Id: IdType, L: IdType> {
    fn has_in_neighbor(&self, id: Id) -> bool;
    fn has_neighbor(&self, id: Id) -> bool;
    fn in_degree(&self) -> usize;
    fn degree(&self) -> usize;
    fn neighbors_iter(&self) -> Iter<Id>;
    fn in_neighbors_iter(&self) -> Iter<Id>;
    fn neighbors(&self) -> Vec<Id>;
    fn in_neighbors(&self) -> Vec<Id>;
    fn get_neighbor(&self, id: Id) -> Option<Option<L>>;
    fn non_less_neighbors_iter(&self) -> Iter<Id>;
    fn neighbors_iter_full(&self) -> Iter<Edge<Id, L>>;
    fn non_less_neighbors_iter_full(&self) -> Iter<Edge<Id, L>>;
}

pub trait MutNodeMapTrait<Id: IdType, L: IdType> {
    fn add_in_edge(&mut self, adj: Id) -> bool;
    fn add_edge(&mut self, adj: Id, label: Option<L>) -> bool;
    fn remove_in_edge(&mut self, adj: Id) -> bool;
    fn remove_edge(&mut self, adj: Id) -> OwnedEdgeType<Id, L>;
    fn get_neighbor_mut(&mut self, id: Id) -> MutEdgeType<Id, L>;
    fn neighbors_iter_mut(&mut self) -> Iter<MutEdgeType<Id, L>>;
    fn non_less_neighbors_iter_mut(&mut self) -> Iter<MutEdgeType<Id, L>>;
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
        self.in_neighbors.binary_search(&id).is_ok()
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
        self.neighbors_vec.len()
    }

    #[inline]
    fn neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.neighbors_vec.iter().map(|x| *x)))
    }

    #[inline]
    fn in_neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.in_neighbors.iter().map(|x| *x)))
    }

    #[inline]
    fn neighbors(&self) -> Vec<Id> {
        self.neighbors_vec.clone()
    }

    #[inline]
    fn in_neighbors(&self) -> Vec<Id> {
        self.in_neighbors.clone()
    }

    #[inline]
    fn get_neighbor(&self, id: Id) -> Option<Option<L>> {
        self.neighbors.get(&id).map(|x| *x)
    }

    #[inline]
    fn non_less_neighbors_iter(&self) -> Iter<Id> {
        let pos = self
            .neighbors_vec
            .binary_search(&self.id)
            .unwrap_or_else(|e| e);

        Iter::new(Box::new((&self.neighbors_vec[pos..]).iter().map(|x| *x)))
    }

    #[inline]
    fn neighbors_iter_full(&self) -> Iter<Edge<Id, L>> {
        let nid = self.id;

        Iter::new(Box::new(self.neighbors_vec.iter().map(move |n| {
            Edge::new(nid, *n, *self.neighbors.get(n).unwrap())
        })))
    }

    #[inline]
    fn non_less_neighbors_iter_full(&self) -> Iter<Edge<Id, L>> {
        let nid = self.id;

        Iter::new(Box::new(self.non_less_neighbors_iter().map(move |n| {
            Edge::new(nid, n, *self.neighbors.get(&n).unwrap())
        })))
    }
}

impl<Id: IdType, L: IdType> MutNodeMapTrait<Id, L> for NodeMap<Id, L> {
    #[inline]
    fn add_in_edge(&mut self, adj: Id) -> bool {
        match self.in_neighbors.binary_search(&adj) {
            Ok(pos) => false,
            Err(pos) => {
                self.in_neighbors.insert(pos, adj);

                true
            }
        }
    }

    #[inline]
    fn add_edge(&mut self, adj: Id, label: Option<L>) -> bool {
        if self.has_neighbor(adj) {
            return false;
        }

        self.neighbors.insert(adj, label);

        let pos = self.neighbors_vec.binary_search(&adj).unwrap_err();

        self.neighbors_vec.insert(pos, adj);

        true
    }

    #[inline]
    fn remove_in_edge(&mut self, adj: Id) -> bool {
        match self.in_neighbors.binary_search(&adj) {
            Ok(pos) => {
                self.in_neighbors.remove(pos);

                true
            }
            Err(_) => false,
        }
    }

    #[inline]
    fn remove_edge(&mut self, adj: Id) -> OwnedEdgeType<Id, L> {
        match self.neighbors.remove(&adj) {
            Some(edge) => {
                let pos = self.neighbors_vec.binary_search(&adj).unwrap();
                self.neighbors_vec.remove(pos);

                OwnedEdgeType::Edge(Edge::new(self.get_id(), adj, edge))
            }
            None => OwnedEdgeType::None,
        }
    }

    #[inline]
    fn get_neighbor_mut(&mut self, id: Id) -> MutEdgeType<Id, L> {
        let nid = self.get_id();
        match self.neighbors.get_mut(&id) {
            Some(edge) => MutEdgeType::EdgeRef(MutEdge::new(nid, id, edge)),
            None => MutEdgeType::None,
        }
    }

    #[inline]
    fn neighbors_iter_mut(&mut self) -> Iter<MutEdgeType<Id, L>> {
        unimplemented!()
        //        let nid = self.get_id();
        //        Iter::new(Box::new(self.neighbors.iter_mut().map(move |(n, l)| {
        //            MutEdgeType::EdgeRef(MutEdge::new(nid, *n, l))
        //        })))
    }

    #[inline]
    fn non_less_neighbors_iter_mut(&mut self) -> Iter<MutEdgeType<Id, L>> {
        unimplemented!()
        //        let nid = self.get_id();
        //        Iter::new(Box::new(self.neighbors.range_mut(self.id..).map(
        //            move |(n, l)| MutEdgeType::EdgeRef(MutEdge::new(nid, *n, l)),
        //        )))
    }
}
