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
extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::graph_gen::{complete_graph_unlabeled, empty_graph_unlabeled};
use rust_graph::graph_gen::{random_gnm_graph_unlabeled, random_gnp_graph_unlabeled};
use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};

#[test]
fn test_random_gnp_graph() {
    let num_of_nodes = 100;

    let empty: DiGraphMap<u8> = random_gnp_graph_unlabeled(num_of_nodes, 0f32);
    assert_eq!(empty, empty_graph_unlabeled(num_of_nodes));

    let clique: UnGraphMap<u8> = random_gnp_graph_unlabeled(num_of_nodes, 1f32);
    assert_eq!(clique, complete_graph_unlabeled(num_of_nodes));
}

#[test]
fn test_random_gnm_graph() {
    let num_of_nodes = 100;
    let num_of_edges = 1000;

    let g1: DiGraphMap<u8> = random_gnm_graph_unlabeled(num_of_nodes, num_of_edges);
    assert_eq!(g1.node_count(), num_of_nodes);
    assert_eq!(g1.edge_count(), num_of_edges);

    let g2: UnGraphMap<u8> = random_gnm_graph_unlabeled(num_of_nodes, num_of_edges);
    assert_eq!(g2.node_count(), num_of_nodes);
    assert_eq!(g2.edge_count(), num_of_edges);
}
