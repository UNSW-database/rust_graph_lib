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

use rust_graph::algorithm::{graph_minus, graph_union, Bfs, ConnComp, Dfs};
use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;

#[test]
fn test_cc_undirected_one_component() {
    let mut graph = UnGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(2, 3, None);

    let cc = ConnComp::new(&graph);

    assert_eq!(cc.get_count(), 1);

    assert_eq!(cc.is_connected(1, 2), true);
    assert_eq!(cc.is_connected(2, 3), true);
    assert_eq!(cc.is_connected(1, 3), true);

    assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 3);
    assert_eq!(cc.get_connected_nodes(2).unwrap().len(), 3);
    assert_eq!(cc.get_connected_nodes(3).unwrap().len(), 3);
}

#[test]
fn test_cc_undirected_seperate_components() {
    let mut graph = UnGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(3, 4, None);

    let cc = ConnComp::new(&graph);

    assert_eq!(cc.get_count(), 2);

    assert_eq!(cc.is_connected(1, 2), true);
    assert_eq!(cc.is_connected(2, 3), false);
    assert_eq!(cc.is_connected(1, 3), false);
    assert_eq!(cc.is_connected(1, 4), false);
    assert_eq!(cc.is_connected(2, 4), false);
    assert_eq!(cc.is_connected(3, 4), true);

    assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 2);
    assert_eq!(cc.get_connected_nodes(2).unwrap().len(), 2);
    assert_eq!(cc.get_connected_nodes(3).unwrap().len(), 2);
    assert_eq!(cc.get_connected_nodes(4).unwrap().len(), 2);
}

#[test]
fn test_cc_directed_one_component() {
    let mut graph = DiGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(2, 3, None);

    let cc = ConnComp::new(&graph);

    assert_eq!(cc.get_count(), 1);

    assert_eq!(cc.is_connected(1, 2), true);
    assert_eq!(cc.is_connected(2, 3), true);
    assert_eq!(cc.is_connected(1, 3), true);

    assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 3);
    assert_eq!(cc.get_connected_nodes(2).unwrap().len(), 3);
    assert_eq!(cc.get_connected_nodes(3).unwrap().len(), 3);
}

#[test]
fn test_cc_directed_seperate_components() {
    let mut graph = DiGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(3, 4, None);

    let cc = ConnComp::new(&graph);

    assert_eq!(cc.get_count(), 2);

    assert_eq!(cc.is_connected(1, 2), true);
    assert_eq!(cc.is_connected(2, 3), false);
    assert_eq!(cc.is_connected(1, 3), false);
    assert_eq!(cc.is_connected(1, 4), false);
    assert_eq!(cc.is_connected(2, 4), false);
    assert_eq!(cc.is_connected(3, 4), true);

    assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 2);
    assert_eq!(cc.get_connected_nodes(2).unwrap().len(), 2);
    assert_eq!(cc.get_connected_nodes(3).unwrap().len(), 2);
    assert_eq!(cc.get_connected_nodes(4).unwrap().len(), 2);
}

#[test]
fn test_bfs_undirected_one_component() {
    let mut graph = UnGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(2, 3, None);

    let mut bfs = Bfs::new(&graph, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(2));
    let x = bfs.next();
    assert_eq!(x, Some(3));
    let x = bfs.next();
    assert_eq!(x, None);
}

#[test]
fn test_bfs_undirected_radomly_chosen_start() {
    let mut graph = UnGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);

    let mut bfs = Bfs::new(&graph, None);
    let x = bfs.next();
    let result = x == Some(1) || x == Some(2);
    assert_eq!(result, true);
}

#[test]
fn test_bfs_undirected_seperate_components() {
    let mut graph = UnGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(3, 4, None);

    let mut bfs = Bfs::new(&graph, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(2));
    let x = bfs.next();
    let result = x == Some(3) || x == Some(4);
    assert_eq!(result, true);
}

#[test]
fn test_bfs_directed_one_component() {
    let mut graph = DiGraphMap::<Void>::new();
    graph.add_edge(2, 1, None);
    graph.add_edge(3, 1, None);

    let mut bfs = Bfs::new(&graph, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(1));
    let x = bfs.next();
    let result = x == Some(3) || x == Some(2);
    assert_eq!(result, true);
}

#[test]
fn test_bfs_directed_radomly_chosen_start() {
    let mut graph = DiGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);

    let mut bfs = Bfs::new(&graph, None);
    let x = bfs.next();
    let result = x == Some(1) || x == Some(2);
    assert_eq!(result, true);
}

#[test]
fn test_bfs_directed_seperate_components() {
    let mut graph = DiGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(3, 4, None);

    let mut bfs = Bfs::new(&graph, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(2));
    let x = bfs.next();
    let result = x == Some(3) || x == Some(4);
    assert_eq!(result, true);
}

#[test]
fn test_dfs_undirected_one_component() {
    let mut graph = UnGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(2, 3, None);

    let mut dfs = Dfs::new(&graph, Some(1));
    let x = dfs.next();
    assert_eq!(x, Some(1));
    let x = dfs.next();
    assert_eq!(x, Some(2));
    let x = dfs.next();
    assert_eq!(x, Some(3));
    let x = dfs.next();
    assert_eq!(x, None);
}

#[test]
fn test_dfs_undirected_radomly_chosen_start() {
    let mut graph = UnGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);

    let mut dfs = Dfs::new(&graph, None);
    let x = dfs.next();
    let result = x == Some(1) || x == Some(2);
    assert_eq!(result, true);
}

#[test]
fn test_dfs_undirected_seperate_components() {
    let mut graph = UnGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(3, 4, None);

    let mut dfs = Dfs::new(&graph, Some(1));
    let x = dfs.next();
    assert_eq!(x, Some(1));
    let x = dfs.next();
    assert_eq!(x, Some(2));
    let x = dfs.next();
    let result = x == Some(3) || x == Some(4);
    assert_eq!(result, true);
}

#[test]
fn test_dfs_directed_one_component() {
    let mut graph = DiGraphMap::<Void>::new();
    graph.add_edge(2, 1, None);
    graph.add_edge(3, 1, None);

    let mut dfs = Dfs::new(&graph, Some(1));
    let x = dfs.next();
    assert_eq!(x, Some(1));
    let x = dfs.next();
    let result = x == Some(3) || x == Some(2);
    assert_eq!(result, true);
}

#[test]
fn test_dfs_directed_radomly_chosen_start() {
    let mut graph = DiGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);

    let mut dfs = Dfs::new(&graph, None);
    let x = dfs.next();
    let result = x == Some(1) || x == Some(2);
    assert_eq!(result, true);
}

#[test]
fn test_dfs_directed_seperate_components() {
    let mut graph = DiGraphMap::<Void>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(3, 4, None);

    let mut dfs = Dfs::new(&graph, Some(1));
    let x = dfs.next();
    assert_eq!(x, Some(1));
    let x = dfs.next();
    assert_eq!(x, Some(2));
    let x = dfs.next();
    let result = x == Some(3) || x == Some(4);
    assert_eq!(result, true);
}

#[test]
fn test_graph_union_directed_graphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_edge(1, 2, Some(10));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph_union(&graph0, &graph1);

    assert_eq!(result_graph.node_count(), 4);
    assert_eq!(result_graph.edge_count(), 2);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), true);
    assert_eq!(result_graph.has_node(4), true);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), true);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), Some(&2));
    assert_eq!(result_graph.get_node_label(4), Some(&3));

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_union_undirected_graphs() {
    let mut graph0 = UnGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_edge(1, 2, Some(10));

    let mut graph1 = UnGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph_union(&graph0, &graph1);

    assert_eq!(result_graph.node_count(), 4);
    assert_eq!(result_graph.edge_count(), 2);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), true);
    assert_eq!(result_graph.has_node(4), true);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), true);
    assert_eq!(result_graph.has_edge(2, 1), true);
    assert_eq!(result_graph.has_edge(4, 3), true);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), Some(&2));
    assert_eq!(result_graph.get_node_label(4), Some(&3));

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
    assert_eq!(result_graph.get_edge_label(4, 3), Some(&20));
}

#[test]
fn test_graph_add_directed_graphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_edge(1, 2, Some(10));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph0.as_general_graph() + graph1.as_general_graph();

    assert_eq!(result_graph.node_count(), 4);
    assert_eq!(result_graph.edge_count(), 2);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), true);
    assert_eq!(result_graph.has_node(4), true);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), true);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), Some(&2));
    assert_eq!(result_graph.get_node_label(4), Some(&3));

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_add_undirected_graphs() {
    let mut graph0 = UnGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_edge(1, 2, Some(10));

    let mut graph1 = UnGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let box0: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph0);
    let box1: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph1);
    let result_graph = box0 + box1;

    assert_eq!(result_graph.node_count(), 4);
    assert_eq!(result_graph.edge_count(), 2);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), true);
    assert_eq!(result_graph.has_node(4), true);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), true);
    assert_eq!(result_graph.has_edge(2, 1), true);
    assert_eq!(result_graph.has_edge(4, 3), true);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), Some(&2));
    assert_eq!(result_graph.get_node_label(4), Some(&3));

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
    assert_eq!(result_graph.get_edge_label(4, 3), Some(&20));
}

#[test]
fn test_graph_add_boxed_directed_generalgraphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_edge(1, 2, Some(10));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let box0: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph0);
    let box1: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph1);
    let result_graph = box0 + box1;

    assert_eq!(result_graph.node_count(), 4);
    assert_eq!(result_graph.edge_count(), 2);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), true);
    assert_eq!(result_graph.has_node(4), true);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), true);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), Some(&2));
    assert_eq!(result_graph.get_node_label(4), Some(&3));

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_add_boxed_undirected_generalgraphs() {
    let mut graph0 = UnGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_edge(1, 2, Some(10));

    let mut graph1 = UnGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let box0: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph0);
    let box1: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph1);
    let result_graph = box0 + box1;

    assert_eq!(result_graph.node_count(), 4);
    assert_eq!(result_graph.edge_count(), 2);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), true);
    assert_eq!(result_graph.has_node(4), true);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), true);
    assert_eq!(result_graph.has_edge(2, 1), true);
    assert_eq!(result_graph.has_edge(4, 3), true);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), Some(&2));
    assert_eq!(result_graph.get_node_label(4), Some(&3));

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
    assert_eq!(result_graph.get_edge_label(4, 3), Some(&20));
}

#[test]
fn test_graph_minus_directed_boxed_typedgraphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = Box::new(graph0) - Box::new(graph1);

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_add_directed_typedgraphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_edge(1, 2, Some(10));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph0 + graph1;

    assert_eq!(result_graph.node_count(), 4);
    assert_eq!(result_graph.edge_count(), 2);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), true);
    assert_eq!(result_graph.has_node(4), true);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), true);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), Some(&2));
    assert_eq!(result_graph.get_node_label(4), Some(&3));

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_add_undirected_typedgraphs() {
    let mut graph0 = UnGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_edge(1, 2, Some(10));

    let mut graph1 = UnGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph0 + graph1;

    assert_eq!(result_graph.node_count(), 4);
    assert_eq!(result_graph.edge_count(), 2);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), true);
    assert_eq!(result_graph.has_node(4), true);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), true);
    assert_eq!(result_graph.has_edge(2, 1), true);
    assert_eq!(result_graph.has_edge(4, 3), true);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), Some(&2));
    assert_eq!(result_graph.get_node_label(4), Some(&3));

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), Some(&20));
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
    assert_eq!(result_graph.get_edge_label(4, 3), Some(&20));
}

#[test]
fn test_graph_minus_directed_graphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph_minus(&graph0, &graph1);

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_minus_undirected_graphs() {
    let mut graph0 = UnGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = UnGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph_minus(&graph0, &graph1);

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), true);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_sub_directed_graphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph0 - graph1;

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_sub_undirected_graphs() {
    let mut graph0 = UnGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = UnGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = graph0 - graph1;

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), true);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_sub_boxed_directed_generalgraphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let box0: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph0);
    let box1: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph1);
    let result_graph = box0 - box1;

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_sub_boxed_undirected_generalgraphs() {
    let mut graph0 = UnGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = UnGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let box0: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph0);
    let box1: Box<GeneralGraph<DefaultId, u32>> = Box::new(graph1);
    let result_graph = box0 - box1;

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), true);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_sub_boxed_directed_typedgraphs() {
    let mut graph0 = DiGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = DiGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = Box::new(graph0) - Box::new(graph1);

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), false);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), None);
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}

#[test]
fn test_graph_sub_boxed_undirected_typedgraphs() {
    let mut graph0 = UnGraphMap::<u32>::new();
    graph0.add_node(1, Some(0));
    graph0.add_node(2, Some(1));
    graph0.add_node(3, Some(2));
    graph0.add_node(4, Some(3));
    graph0.add_edge(1, 2, Some(10));
    graph0.add_edge(3, 4, Some(20));

    let mut graph1 = UnGraphMap::<u32>::new();
    graph1.add_node(3, Some(2));
    graph1.add_node(4, Some(3));
    graph1.add_edge(3, 4, Some(20));

    let result_graph = Box::new(graph0) - Box::new(graph1);

    assert_eq!(result_graph.node_count(), 2);
    assert_eq!(result_graph.edge_count(), 1);

    assert_eq!(result_graph.has_node(1), true);
    assert_eq!(result_graph.has_node(2), true);
    assert_eq!(result_graph.has_node(3), false);
    assert_eq!(result_graph.has_node(4), false);

    assert_eq!(result_graph.has_edge(1, 2), true);
    assert_eq!(result_graph.has_edge(3, 4), false);
    assert_eq!(result_graph.has_edge(2, 1), true);
    assert_eq!(result_graph.has_edge(4, 3), false);
    assert_eq!(result_graph.has_edge(2, 3), false);
    assert_eq!(result_graph.has_edge(1, 4), false);

    assert_eq!(result_graph.get_node_label(1), Some(&0));
    assert_eq!(result_graph.get_node_label(2), Some(&1));
    assert_eq!(result_graph.get_node_label(3), None);
    assert_eq!(result_graph.get_node_label(4), None);

    assert_eq!(result_graph.get_edge_label(1, 2), Some(&10));
    assert_eq!(result_graph.get_edge_label(3, 4), None);
    assert_eq!(result_graph.get_edge_label(1, 4), None);
    assert_eq!(result_graph.get_edge_label(2, 3), None);
    assert_eq!(result_graph.get_edge_label(2, 1), Some(&10));
    assert_eq!(result_graph.get_edge_label(4, 3), None);
}
