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
extern crate tempfile;

use rust_graph::graph_gen::{random_gnm_graph, random_gnm_graph_unlabeled};
use rust_graph::graph_impl::{DiGraphMap, GraphMap, UnGraphMap};
#[cfg(feature = "hdfs")]
use rust_graph::io::hdfs::read_from_hdfs;
use rust_graph::io::{read_from_csv, write_to_csv};
use rust_graph::prelude::*;
use tempfile::TempDir;

#[test]
fn test_cvs_unlabeled() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    let nodes = 10;
    let edges = 20;

    let g: UnGraphMap<Void> = random_gnm_graph_unlabeled(nodes, edges);
    let path_to_nodes = tmp_dir_path.join("nodes_1.csv");
    let path_to_edges = tmp_dir_path.join("edges_1.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    let mut g_ = GraphMap::new();
    read_from_csv(
        &mut g_,
        vec![path_to_nodes],
        vec![path_to_edges],
        None,
        true,
        true,
    );
    assert_eq!(g, g_);

    let g: DiGraphMap<Void> = random_gnm_graph_unlabeled(nodes, edges);
    let path_to_nodes = tmp_dir_path.join("nodes_2.csv");
    let path_to_edges = tmp_dir_path.join("edges_2.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    let mut g_ = GraphMap::new();
    read_from_csv(
        &mut g_,
        vec![path_to_nodes],
        vec![path_to_edges],
        None,
        true,
        true,
    );
    assert_eq!(g, g_);
}

#[test]
fn test_cvs_labeled() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    let nodes = 10;
    let edges = 20;

    let node_labels = &vec!["a".to_owned(), "b".to_owned()];
    let edge_labels = &vec![1, 2, 3];

    let g: UnGraphMap<String, u32> =
        random_gnm_graph(nodes, edges, node_labels.clone(), edge_labels.clone());
    let path_to_nodes = tmp_dir_path.join("nodes_1.csv");
    let path_to_edges = tmp_dir_path.join("edges_1.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    let mut g_ = GraphMap::with_label_map(node_labels.into(), edge_labels.into());
    read_from_csv(
        &mut g_,
        vec![path_to_nodes],
        vec![path_to_edges],
        None,
        true,
        true,
    );
    assert_eq!(g, g_);

    let g: DiGraphMap<String, u32> =
        random_gnm_graph(nodes, edges, node_labels.clone(), edge_labels.clone());
    let path_to_nodes = tmp_dir_path.join("nodes_2.csv");
    let path_to_edges = tmp_dir_path.join("edges_2.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    let mut g_ = GraphMap::with_label_map(node_labels.into(), edge_labels.into());
    read_from_csv(
        &mut g_,
        vec![path_to_nodes],
        vec![path_to_edges],
        None,
        true,
        true,
    );
    assert_eq!(g, g_);
}

/// Because of the requirement of hadoop environment on local, we `ignore` the test here.
/// If you have configure the environment according to `README.md`, use parameter `--ignore` to test it.
#[test]
#[cfg(feature = "hdfs")]
#[ignore]
fn test_csv_hdfs_read() {
    let path_to_nodes = "hdfs://localhost:9000/labelled/nodes/";
    let path_to_edges = "hdfs://localhost:9000/labelled/edges/edges.csv";
    let node_labels = &vec!["a".to_owned(), "b".to_owned()];
    let edge_labels = &vec![1, 2, 3];
    let mut g_: DiGraphMap<String, i32> =
        GraphMap::with_label_map(node_labels.into(), edge_labels.into());

    read_from_hdfs(
        &mut g_,
        vec![path_to_nodes],
        vec![path_to_edges],
        Some(","),
        true,
        false,
    );
}
