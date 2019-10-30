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
pub mod dtype;
pub mod edge;
pub mod graph;
pub mod iter;
pub mod map;
pub mod node;

pub use crate::generic::dtype::{DefaultId, DefaultTy, Directed, GraphType, IdType, Undirected, Void};
pub use crate::generic::edge::{
    Edge, EdgeTrait, EdgeType, MutEdge, MutEdgeTrait, MutEdgeType, OwnedEdgeType,
};
pub use crate::generic::graph::{
    DiGraphTrait, GeneralGraph, GraphLabelTrait, GraphTrait, MutGraphLabelTrait, MutGraphTrait,
    UnGraphTrait,
};
pub use crate::generic::iter::Iter;
pub use crate::generic::map::{MapTrait, MutMapTrait};
pub use crate::generic::node::{MutNodeTrait, MutNodeType, NodeTrait, NodeType, OwnedNodeType};
pub use crate::graph_impl::graph_map::{MutNodeMapTrait, NodeMapTrait};
