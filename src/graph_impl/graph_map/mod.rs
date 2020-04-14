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
//! An implementation of graph data structure that supports directed graph, undirected graph,
//! node label, edge label, self loop, but not multi-edge.
//!
//! A unique id of type `usize` must be given to each node when creating the graph.
//!
//! # Example
//! ```
//! use rust_graph::prelude::*;
//! use rust_graph::UnGraphMap;
//!
//! let mut g = UnGraphMap::<&str>::new();
//! g.add_node(0,None);
//! g.add_node(1,Some("node label"));
//! g.add_edge(0,1,Some("edge label"));
//! ```

pub mod edge;
pub mod graph;
pub mod node;

pub use crate::graph_impl::graph_map::edge::{Edge, MutEdge};
pub use crate::graph_impl::graph_map::graph::{
    new_general_graphmap, DiGraphMap, GraphMap, TypedDiGraphMap, TypedGraphMap, TypedUnGraphMap,
    UnGraphMap,
};
pub use crate::graph_impl::graph_map::node::NodeMap;
pub use crate::graph_impl::graph_map::node::{MutNodeMapTrait, NodeMapTrait};
