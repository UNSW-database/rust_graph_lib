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
    fn neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.neighbors.keys().map(|x| *x)))
    }

    #[inline]
    fn in_neighbors_iter(&self) -> Iter<Id> {
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
    fn non_less_neighbors_iter(&self) -> Iter<Id> {
        Iter::new(Box::new(self.neighbors.range(self.id..).map(|(&id, _)| id)))
    }

    #[inline]
    fn neighbors_iter_full(&self) -> Iter<Edge<Id, L>> {
        let nid = self.id;

        Iter::new(Box::new(
            self.neighbors
                .iter()
                .map(move |(&n, &l)| Edge::new(nid, n, l)),
        ))
    }

    #[inline]
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
    fn remove_edge(&mut self, adj: Id) -> Option<Option<L>> {
        self.neighbors.remove(&adj)
    }

    #[inline]
    fn get_neighbor_mut(&mut self, id: Id) -> Option<&mut Option<L>> {
        self.neighbors.get_mut(&id)
    }

    #[inline]
    fn neighbors_iter_mut(&mut self) -> Iter<&mut Option<L>> {
        Iter::new(Box::new(self.neighbors.values_mut()))
    }

    #[inline]
    fn non_less_neighbors_iter_mut(&mut self) -> Iter<&mut Option<L>> {
        Iter::new(Box::new(
            self.neighbors.range_mut(self.id..).map(|(_, label)| label),
        ))
    }
}
