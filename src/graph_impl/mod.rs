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
pub mod cassandra_graph;
pub mod graph_map;
pub mod graph_vec;
// pub mod rpc_graph;
pub mod static_graph;

pub use crate::graph_impl::cassandra_graph::CassandraCore;
pub use crate::graph_impl::cassandra_graph::CassandraGraph;
pub use crate::graph_impl::graph_map::{
    DiGraphMap, Edge, GraphMap, MutEdge, TypedDiGraphMap, TypedGraphMap, TypedUnGraphMap,
    UnGraphMap,
};
pub use crate::graph_impl::graph_vec::{GraphVec, TypedGraphVec};
pub use crate::graph_impl::static_graph::{
    DiStaticGraph, EdgeVec, StaticGraph, TypedDiStaticGraph, TypedStaticGraph, TypedUnStaticGraph,
    UnStaticGraph,
};

#[derive(Eq, PartialEq, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum GraphImpl {
    GraphMap,
    StaticGraph,
    CassandraGraph,
}

impl ::std::str::FromStr for GraphImpl {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.to_lowercase();
        match s.as_ref() {
            "graphmap" => Ok(GraphImpl::GraphMap),
            "staticgraph" => Ok(GraphImpl::StaticGraph),
            _other => Err(format!("Unsupported implementation {:?}", _other)),
        }
    }
}
