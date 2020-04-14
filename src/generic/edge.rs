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
use crate::generic::IdType;
pub use crate::graph_impl::graph_map::{Edge, MutEdge};

pub trait EdgeTrait<Id: IdType, L: IdType> {
    #[inline(always)]
    fn is_none(&self) -> bool {
        false
    }
    #[inline(always)]
    fn is_some(&self) -> bool {
        !self.is_none()
    }
    fn get_start(&self) -> Id;
    fn get_target(&self) -> Id;
    fn get_label_id(&self) -> Option<L>;
}

pub trait MutEdgeTrait<Id: IdType, L: IdType>: EdgeTrait<Id, L> {
    fn set_label_id(&mut self, label: Option<L>);
}

#[derive(Debug, PartialEq, Eq)]
pub enum MutEdgeType<'a, Id: IdType, L: IdType = Id> {
    EdgeRef(MutEdge<'a, Id, L>),
    None,
}

#[derive(Debug, PartialEq, Eq)]
pub enum OwnedEdgeType<Id: IdType, L: IdType = Id> {
    Edge(Edge<Id, L>),
    None,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeType<Id: IdType, L: IdType = Id> {
    Edge(Edge<Id, L>),
    None,
}

impl<'a, Id: IdType, L: IdType> MutEdgeType<'a, Id, L> {
    #[inline(always)]
    pub fn unwrap_edge(self) -> Edge<Id, L> {
        match self {
            MutEdgeType::EdgeRef(_) => panic!("'unwrap_edge()` on `EdgeRef`"),
            MutEdgeType::None => panic!("`unwrap_edge()` on `None`"),
        }
    }

    #[inline(always)]
    pub fn unwrap_edge_ref(self) -> MutEdge<'a, Id, L> {
        match self {
            MutEdgeType::EdgeRef(edge) => edge,
            MutEdgeType::None => panic!("`unwrap_edge_ref()` on `None`"),
        }
    }
}

impl<'a, Id: IdType, L: IdType> EdgeTrait<Id, L> for MutEdgeType<'a, Id, L> {
    #[inline(always)]
    fn is_none(&self) -> bool {
        match self {
            MutEdgeType::None => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn get_start(&self) -> Id {
        match self {
            MutEdgeType::EdgeRef(edge) => edge.get_start(),
            MutEdgeType::None => panic!("`get_start()` on `None`"),
        }
    }

    #[inline(always)]
    fn get_target(&self) -> Id {
        match self {
            MutEdgeType::EdgeRef(edge) => edge.get_target(),
            MutEdgeType::None => panic!("`get_target()` on `None`"),
        }
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        match self {
            MutEdgeType::EdgeRef(edge) => edge.get_label_id(),
            MutEdgeType::None => panic!("`get_label_id()` on `None`"),
        }
    }
}

impl<'a, Id: IdType, L: IdType> MutEdgeTrait<Id, L> for MutEdgeType<'a, Id, L> {
    #[inline(always)]
    fn set_label_id(&mut self, label: Option<L>) {
        match self {
            MutEdgeType::EdgeRef(edge) => edge.set_label_id(label),
            MutEdgeType::None => panic!("`set_label_id()` on `None`"),
        }
    }
}

impl<Id: IdType, L: IdType> EdgeTrait<Id, L> for OwnedEdgeType<Id, L> {
    fn is_none(&self) -> bool {
        match self {
            OwnedEdgeType::None => true,
            _ => false,
        }
    }

    fn get_start(&self) -> Id {
        match self {
            OwnedEdgeType::Edge(edge) => edge.get_start(),
            OwnedEdgeType::None => panic!("`get_start()` on `None`"),
        }
    }

    fn get_target(&self) -> Id {
        match self {
            OwnedEdgeType::Edge(edge) => edge.get_target(),
            OwnedEdgeType::None => panic!("`get_target()` on `None`"),
        }
    }

    fn get_label_id(&self) -> Option<L> {
        match self {
            OwnedEdgeType::Edge(edge) => edge.get_label_id(),
            OwnedEdgeType::None => panic!("`get_label_id()` on `None`"),
        }
    }
}

impl<Id: IdType, L: IdType> EdgeType<Id, L> {
    #[inline(always)]
    pub fn unwrap(self) -> Edge<Id, L> {
        match self {
            EdgeType::Edge(edge) => edge,
            EdgeType::None => panic!("`unwrap()` on `None`"),
        }
    }
}

impl<Id: IdType, L: IdType> EdgeTrait<Id, L> for EdgeType<Id, L> {
    #[inline(always)]
    fn is_none(&self) -> bool {
        match *self {
            EdgeType::None => true,
            _ => false,
        }
    }

    #[inline(always)]
    fn get_start(&self) -> Id {
        match self {
            EdgeType::Edge(edge) => edge.get_start(),
            EdgeType::None => panic!("`get_start()` on `None`"),
        }
    }

    #[inline(always)]
    fn get_target(&self) -> Id {
        match self {
            EdgeType::Edge(edge) => edge.get_target(),
            EdgeType::None => panic!("`get_target()` on`None`"),
        }
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        match self {
            EdgeType::Edge(edge) => edge.get_label_id(),
            EdgeType::None => None,
        }
    }
}
