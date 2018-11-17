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
use graph_impl::Edge;

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeType<Id: IdType, L: IdType = Id> {
    Edge(Edge<Id, L>),
    None,
}

impl<Id: IdType, L: IdType> EdgeType<Id, L> {
    #[inline(always)]
    pub fn unwrap(self) -> Edge<Id, L> {
        match self {
            EdgeType::Edge(edge) => edge,
            EdgeType::None => panic!("called `EdgeType::unwrap()` on a `None` edge"),
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
            EdgeType::None => panic!("called `EdgeType::unwrap()` on a `None` edge"),
        }
    }

    #[inline(always)]
    fn get_target(&self) -> Id {
        match self {
            EdgeType::Edge(edge) => edge.get_target(),
            EdgeType::None => panic!("called `EdgeType::unwrap()` on a `None` edge"),
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
