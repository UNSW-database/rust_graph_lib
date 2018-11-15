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
use generic::IdType;
use generic::Iter;

use graph_impl::graph_map::NodeMap;
use graph_impl::static_graph::StaticNode;
use graph_impl::Edge;

pub trait NodeTrait<Id: IdType, L: IdType> {
    fn get_id(&self) -> Id;
    fn get_label_id(&self) -> Option<L>;
}

pub trait MutNodeTrait<Id: IdType, L: IdType> {
    fn set_label_id(&mut self, label: Option<L>);
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
    fn remove_edge(&mut self, adj: Id) -> Option<Option<L>>;
    fn get_neighbor_mut(&mut self, id: Id) -> Option<&mut Option<L>>;
    fn neighbors_iter_mut(&mut self) -> Iter<&mut Option<L>>;
    fn non_less_neighbors_iter_mut(&mut self) -> Iter<&mut Option<L>>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeType<'a, Id: 'a + IdType, L: 'a + IdType> {
    NodeMap(&'a NodeMap<Id, L>),
    StaticNode(StaticNode<Id, L>),
    None,
}

impl<'a, Id: IdType, L: IdType> NodeType<'a, Id, L> {
    #[inline(always)]
    pub fn is_none(&self) -> bool {
        match *self {
            NodeType::None => true,
            _ => false,
        }
    }

    #[inline(always)]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    #[inline(always)]
    pub fn unwrap_nodemap(self) -> &'a NodeMap<Id, L> {
        match self {
            NodeType::NodeMap(node) => node,
            NodeType::StaticNode(_) => {
                panic!("called `NodeType::unwrap_nodemap()` on a `StaticNode` value")
            }

            NodeType::None => panic!("called `NodeType::unwrap_nodemap()` on a `None` value"),
        }
    }

    #[inline(always)]
    pub fn unwrap_staticnode(self) -> StaticNode<Id, L> {
        match self {
            NodeType::NodeMap(_) => {
                panic!("called `NodeType::unwrap_staticnode()` on a `NodeMap` value")
            }
            NodeType::StaticNode(node) => node,
            NodeType::None => panic!("called `NodeType::unwrap_staticnode()` on a `None` value"),
        }
    }
}

impl<'a, Id: IdType, L: IdType> NodeTrait<Id, L> for NodeType<'a, Id, L> {
    #[inline(always)]
    fn get_id(&self) -> Id {
        match self {
            NodeType::NodeMap(node) => node.get_id(),
            NodeType::StaticNode(ref node) => node.get_id(),
            NodeType::None => panic!("called `NodeType::get_id()` on a `None` value"),
        }
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        match self {
            NodeType::NodeMap(node) => node.get_label_id(),
            NodeType::StaticNode(ref node) => node.get_label_id(),
            NodeType::None => None,
        }
    }
}
