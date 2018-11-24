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
pub use graph_impl::graph_map::NodeMap;
pub use graph_impl::static_graph::StaticNode;

pub trait NodeTrait<Id: IdType, L: IdType> {
    #[inline(always)]
    fn is_none(&self) -> bool {
        false
    }
    #[inline(always)]
    fn is_some(&self) -> bool {
        !self.is_none()
    }
    fn get_id(&self) -> Id;
    fn get_label_id(&self) -> Option<L>;
}

pub trait MutNodeTrait<Id: IdType, L: IdType>: NodeTrait<Id, L> {
    fn set_label_id(&mut self, label: Option<L>);
}

#[derive(Debug, PartialEq, Eq)]
pub enum MutNodeType<'a, Id: 'a + IdType, L: 'a + IdType = Id> {
    NodeMapRef(&'a mut NodeMap<Id, L>),
    None,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OwnedNodeType<Id: IdType, L: IdType = Id> {
    NodeMap(NodeMap<Id, L>),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeType<'a, Id: 'a + IdType, L: 'a + IdType = Id> {
    NodeMap(&'a NodeMap<Id, L>),
    StaticNode(StaticNode<Id, L>),
    None,
}

impl<'a, Id: IdType, L: IdType> MutNodeType<'a, Id, L> {
    #[inline(always)]
    pub fn unwrap_nodemap_ref(self) -> &'a mut NodeMap<Id, L> {
        match self {
            MutNodeType::NodeMapRef(node) => node,
            MutNodeType::None => panic!("`unwrap_nodemap_ref()` on `None`"),
        }
    }
}

impl<'a, Id: IdType + 'a, L: IdType + 'a> NodeTrait<Id, L> for MutNodeType<'a, Id, L> {
    #[inline(always)]
    fn is_none(&self) -> bool {
        match *self {
            MutNodeType::None => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn get_id(&self) -> Id {
        match self {
            MutNodeType::NodeMapRef(node) => node.get_id(),
            MutNodeType::None => panic!("`get_id()` on `None`"),
        }
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        match self {
            MutNodeType::NodeMapRef(node) => node.get_label_id(),
            MutNodeType::None => panic!("`get_label_id()` on `None`"),
        }
    }
}

impl<'a, Id: IdType + 'a, L: IdType + 'a> MutNodeTrait<Id, L> for MutNodeType<'a, Id, L> {
    #[inline(always)]
    fn set_label_id(&mut self, label: Option<L>) {
        match self {
            MutNodeType::NodeMapRef(node) => node.set_label_id(label),
            MutNodeType::None => panic!("`set_label_id()` on `None`"),
        }
    }
}

impl<Id: IdType, L: IdType> NodeTrait<Id, L> for OwnedNodeType<Id, L> {
    fn is_none(&self) -> bool {
        unimplemented!()
    }

    fn get_id(&self) -> Id {
        unimplemented!()
    }

    fn get_label_id(&self) -> Option<L> {
        unimplemented!()
    }
}

impl<'a, Id: IdType, L: IdType> NodeType<'a, Id, L> {
    #[inline(always)]
    pub fn unwrap_nodemap(self) -> &'a NodeMap<Id, L> {
        match self {
            NodeType::NodeMap(node) => node,
            NodeType::StaticNode(_) => panic!("`unwrap_nodemap()` on `StaticNode`"),
            NodeType::None => panic!("`unwrap_nodemap()` on `None`"),
        }
    }

    #[inline(always)]
    pub fn unwrap_staticnode(self) -> StaticNode<Id, L> {
        match self {
            NodeType::NodeMap(_) => panic!("`unwrap_staticnode()` on `NodeMap`"),
            NodeType::StaticNode(node) => node,
            NodeType::None => panic!("`unwrap_staticnode()` on `None`"),
        }
    }
}

impl<'a, Id: IdType, L: IdType> NodeTrait<Id, L> for NodeType<'a, Id, L> {
    #[inline(always)]
    fn is_none(&self) -> bool {
        match *self {
            NodeType::None => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn get_id(&self) -> Id {
        match self {
            NodeType::NodeMap(node) => node.get_id(),
            NodeType::StaticNode(ref node) => node.get_id(),
            NodeType::None => panic!("`get_id()` on `None`"),
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
