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
pub mod graph_map;
pub mod static_graph;

pub use graph_impl::graph_map::{
    DiGraphMap, Edge, GraphMap, MutEdge, TypedDiGraphMap, TypedGraphMap, TypedUnGraphMap,
    UnGraphMap,
};
pub use graph_impl::static_graph::mmap::{EdgeVecMmap, StaticGraphMmap};
pub use graph_impl::static_graph::{
    DiStaticGraph, EdgeVec, StaticGraph, TypedDiStaticGraph, TypedStaticGraph, TypedUnStaticGraph,
    UnStaticGraph,
};

#[derive(Eq, PartialEq, Copy, Clone, Debug)]
pub enum GraphImpl {
    GraphMap,
    StaticGraph,
    StaicGraphMmap,
}

impl ::std::str::FromStr for GraphImpl {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.to_lowercase();
        match s.as_ref() {
            "graphmap" => Ok(GraphImpl::GraphMap),
            "staticgraph" => Ok(GraphImpl::StaticGraph),
            "staticgraphmmap" => Ok(GraphImpl::StaicGraphMmap),
            _other => Err(format!("Unsupported implementation {:?}", _other)),
        }
    }
}
