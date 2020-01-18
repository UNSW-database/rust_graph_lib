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
#[macro_use]
extern crate rust_graph;
extern crate hashbrown;
extern crate itertools;
extern crate tempfile;

use hashbrown::HashMap;
use itertools::Itertools;
use rust_graph::generic::DefaultId;
use rust_graph::graph_impl::multi_graph::plan::query_plan_worker::QPWorkers;
use rust_graph::graph_impl::multi_graph::planner::catalog::query_edge::QueryEdge;
use rust_graph::graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use rust_graph::graph_impl::multi_graph::runner::{catalog_generator, optimizer_executor};
use rust_graph::graph_impl::static_graph::StaticNode;
use rust_graph::graph_impl::EdgeVec;
use rust_graph::graph_impl::{Edge, TypedDiGraphMap, TypedGraphMap};
use rust_graph::io::read_from_csv;
use rust_graph::map::SetMap;
use rust_graph::prelude::*;
use rust_graph::{DiStaticGraph, UnStaticGraph};
use std::path::Path;

#[test]
fn test_directed() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::<Void>::new(edge_vec, Some(in_edge_vec), None, None);

    assert_eq!(g.get_node_ids(), &vec![0, 1, 2]);
    assert_eq!(g.get_node_types(), &vec![0, 0, 0]);
    assert_eq!(g.get_node_type_offsets(), &vec![0, 4]);

    let fwd_adj_list = g.get_fwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![1, 2]);
    let fwd_adj_list = g.get_fwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 1, 1, 1]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![0]);
    let fwd_adj_list = g.get_fwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 1, 1, 1]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![0]);

    let bwd_adj_list = g.get_bwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![1, 2]);
    let bwd_adj_list = g.get_bwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 1, 1, 1]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![0]);
    let bwd_adj_list = g.get_bwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 1, 1, 1]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![0]);

    assert_eq!(g.node_count(), 3);
    assert_eq!(g.edge_count(), 4);

    assert_eq!(g.neighbors(0)[..], [1, 2]);
    assert_eq!(&g.neighbors(1)[..], &[0]);
    assert_eq!(&g.neighbors(2)[..], &[0]);

    assert_eq!(g.degree(0), 2);
    assert_eq!(g.degree(1), 1);
    assert_eq!(g.degree(2), 1);

    assert_eq!(g.in_neighbors(0).into_owned(), vec![1, 2]);
    assert_eq!(g.in_neighbors(1).into_owned(), vec![0]);
    assert_eq!(g.in_neighbors(2).into_owned(), vec![0]);

    assert_eq!(g.in_degree(0), 2);
    assert_eq!(g.in_degree(1), 1);
    assert_eq!(g.in_degree(2), 1);

    let node_0 = StaticNode::<DefaultId>::new(0, None);
    let node_1 = StaticNode::<DefaultId>::new(1, None);
    let node_2 = StaticNode::<DefaultId>::new(2, None);

    let edge_0_1 = Edge::<DefaultId>::new(0, 1, None);
    let edge_0_2 = Edge::<DefaultId>::new(0, 2, None);
    let edge_1_0 = Edge::<DefaultId>::new(1, 0, None);
    let edge_2_0 = Edge::<DefaultId>::new(2, 0, None);

    assert_eq!(g.get_node(0).unwrap_staticnode(), node_0);
    assert_eq!(g.get_node(1).unwrap_staticnode(), node_1);
    assert_eq!(g.get_node(2).unwrap_staticnode(), node_2);

    assert!(g.get_node(3).is_none());

    assert_eq!(g.get_edge(0, 1).unwrap(), edge_0_1);
    assert_eq!(g.get_edge(0, 2).unwrap(), edge_0_2);
    assert_eq!(g.get_edge(1, 0).unwrap(), edge_1_0);
    assert_eq!(g.get_edge(2, 0).unwrap(), edge_2_0);
    assert!(g.get_edge(2, 3).is_none());

    let nodes: Vec<_> = g.nodes().collect();
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains(&g.get_node(0)));
    assert!(nodes.contains(&g.get_node(1)));
    assert!(nodes.contains(&g.get_node(2)));

    let edges: Vec<_> = g.edges().collect();
    assert_eq!(edges.len(), 4);
    assert!(edges.contains(&g.get_edge(0, 1)));
    assert!(edges.contains(&g.get_edge(0, 2)));
    assert!(edges.contains(&g.get_edge(1, 0)));
    assert!(edges.contains(&g.get_edge(2, 0)));
}

#[test]
fn test_undirected() {
    let edge_vec = EdgeVec::new(vec![0, 2, 4, 6], vec![1, 2, 0, 2, 0, 1]);
    let g = UnStaticGraph::<Void>::new(edge_vec, None, None, None);
    let edges: Vec<_> = g.edge_indices().collect();
    assert_eq!(edges, vec![(0, 1), (0, 2), (1, 2)]);

    assert_eq!(g.get_node_ids(), &vec![0, 1, 2]);
    // Without node labels
    assert_eq!(g.get_node_types(), &vec![0, 0, 0]);
    assert_eq!(g.get_node_type_offsets(), &vec![0, 4]);

    let fwd_adj_list = g.get_fwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![1, 2]);
    let fwd_adj_list = g.get_fwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![0, 2]);
    let fwd_adj_list = g.get_fwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![0, 1]);

    let bwd_adj_list = g.get_bwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![1, 2]);
    let bwd_adj_list = g.get_bwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![0, 2]);
    let bwd_adj_list = g.get_bwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![0, 1]);
}

#[test]
fn test_labeled() {
    let edge_vec = EdgeVec::with_labels(vec![0, 2, 3, 4], vec![1, 2, 0, 0], vec![0, 1, 0, 1]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let labels = vec![1, 0, 1];
    let g = DiStaticGraph::<&str>::with_labels(
        edge_vec,
        Some(in_edge_vec),
        labels,
        setmap!["a", "b"],
        setmap!["a", "b"],
        None,
        None,
    );

    assert_eq!(g.get_node_label(0), Some(&"b"));
    assert_eq!(g.get_node_label(1), Some(&"a"));
    assert_eq!(g.get_node_label(2), Some(&"b"));
    assert_eq!(g.get_node_label(4), None);

    assert_eq!(g.get_edge_label(0, 1), Some(&"a"));
    assert_eq!(g.get_edge_label(0, 2), Some(&"b"));
    assert_eq!(g.get_edge_label(1, 0), Some(&"a"));
    assert_eq!(g.get_edge_label(2, 0), Some(&"b"));
    assert_eq!(g.get_edge_label(2, 3), None);

    let node_0 = StaticNode::new(0 as DefaultId, Some(1));
    let node_1 = StaticNode::new(1 as DefaultId, Some(0));
    let node_2 = StaticNode::new(2 as DefaultId, Some(1));

    let edge_0_1 = Edge::new(0 as DefaultId, 1, Some(0));
    let edge_0_2 = Edge::new(0 as DefaultId, 2, Some(1));
    let edge_1_0 = Edge::new(1 as DefaultId, 0, Some(0));
    let edge_2_0 = Edge::new(2 as DefaultId, 0, Some(1));

    assert_eq!(g.get_node(0).unwrap_staticnode(), node_0);
    assert_eq!(g.get_node(1).unwrap_staticnode(), node_1);
    assert_eq!(g.get_node(2).unwrap_staticnode(), node_2);
    assert!(g.get_node(3).is_none());

    assert_eq!(g.get_edge(0, 1).unwrap(), edge_0_1);
    assert_eq!(g.get_edge(0, 2).unwrap(), edge_0_2);
    assert_eq!(g.get_edge(1, 0).unwrap(), edge_1_0);
    assert_eq!(g.get_edge(2, 0).unwrap(), edge_2_0);
    assert!(g.get_edge(2, 3).is_none());

    let nodes: Vec<_> = g.nodes().collect();
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains(&g.get_node(0)));
    assert!(nodes.contains(&g.get_node(1)));
    assert!(nodes.contains(&g.get_node(2)));

    let edges: Vec<_> = g.edges().collect();
    assert_eq!(edges.len(), 4);
    assert!(edges.contains(&g.get_edge(0, 1)));
    assert!(edges.contains(&g.get_edge(0, 2)));
    assert!(edges.contains(&g.get_edge(1, 0)));
    assert!(edges.contains(&g.get_edge(2, 0)));

    assert_eq!(g.get_node_ids(), &vec![1, 0, 2]);
    assert_eq!(g.get_node_types(), &vec![2, 1, 2]);
    assert_eq!(g.get_node_type_offsets(), &vec![0, 0, 1, 3, 3]);

    let fwd_adj_list = g.get_fwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 0, 1, 2]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![1, 2]);
    let fwd_adj_list = g.get_fwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 0, 1, 1]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![0]);
    let fwd_adj_list = g.get_fwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 0, 0, 1]);
    assert_eq!(fwd_adj_list.get_neighbor_ids(), &vec![0]);

    let bwd_adj_list = g.get_bwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 0, 1, 2]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![1, 2]);
    let bwd_adj_list = g.get_bwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 0, 1, 1]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![0]);
    let bwd_adj_list = g.get_bwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 0, 0, 1]);
    assert_eq!(bwd_adj_list.get_neighbor_ids(), &vec![0]);

    let neighbour_edge_no: Vec<u32> = g.neighbors_of_edge_iter(0, None).collect();
    let neighbour_edge_0_a: Vec<u32> = g.neighbors_of_edge_iter(0, Some("a")).collect();
    let neighbour_edge_0_b: Vec<u32> = g.neighbors_of_edge_iter(0, Some("b")).collect();
    let neighbour_edge_1_a: Vec<u32> = g.neighbors_of_edge_iter(1, Some("a")).collect();
    let neighbour_edge_2_b: Vec<u32> = g.neighbors_of_edge_iter(2, Some("b")).collect();
    assert_eq!(&neighbour_edge_no, &vec![1, 2]);
    assert_eq!(&neighbour_edge_0_a, &vec![1]);
    assert_eq!(&neighbour_edge_0_b, &vec![2]);
    assert_eq!(&neighbour_edge_1_a, &vec![0]);
    assert_eq!(&neighbour_edge_2_b, &vec![0]);

    let nodes_a = g.nodes_with_label(Some("a"));
    let nodes_b = g.nodes_with_label(Some("b"));
    assert_eq!(nodes_a.collect_vec(), vec![1, 0]);
    assert_eq!(nodes_b.collect_vec(), vec![2, 0]);

    let edges_a = g.edges_with_label(Some("a"));
    let edges_b = g.edges_with_label(Some("b"));
    assert_eq!(edges_a.collect_vec(), vec![(0, 1), (1, 0)]);
    assert_eq!(edges_b.collect_vec(), vec![(0, 2), (2, 0)]);
}

#[test]
fn test_get_neighbours_by_label() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let labels = vec![1, 0, 1];
    let g = DiStaticGraph::<&str>::with_labels(
        edge_vec,
        Some(in_edge_vec),
        labels,
        setmap!["a", "b"],
        setmap![],
        None,
        None,
    );
    let neighbour_edge_no_iter: Vec<u32> = g.neighbors_of_node_iter(0, None).collect();
    let neighbour_edge_0_a_iter: Vec<u32> = g.neighbors_of_node_iter(0, Some("a")).collect();
    let neighbour_edge_0_b_iter: Vec<u32> = g.neighbors_of_node_iter(0, Some("b")).collect();
    let neighbour_edge_1_a_iter: Vec<u32> = g.neighbors_of_node_iter(1, Some("a")).collect();
    let neighbour_edge_2_b_iter: Vec<u32> = g.neighbors_of_node_iter(2, Some("b")).collect();
    assert_eq!(&neighbour_edge_no_iter, &vec![1, 2]);
    assert_eq!(&neighbour_edge_0_a_iter, &vec![1]);
    assert_eq!(&neighbour_edge_0_b_iter, &vec![2]);
    assert_eq!(&neighbour_edge_1_a_iter, &(Vec::<u32>::new()));
    assert_eq!(&neighbour_edge_2_b_iter, &vec![0]);
    let neighbour_edge_no = g.neighbors_of_node(0, None);
    let neighbour_edge_0_a = g.neighbors_of_node(0, Some("a"));
    let neighbour_edge_0_b = g.neighbors_of_node(0, Some("b"));
    let neighbour_edge_1_a = g.neighbors_of_node(1, Some("a"));
    let neighbour_edge_2_b = g.neighbors_of_node(2, Some("b"));
    assert_eq!(&neighbour_edge_no.iter().collect_vec(), &vec![&1, &2]);
    assert_eq!(&neighbour_edge_0_a.iter().collect_vec(), &vec![&1]);
    assert_eq!(&neighbour_edge_0_b.iter().collect_vec(), &vec![&2]);
    assert_eq!(
        &neighbour_edge_1_a.iter().collect_vec(),
        &(Vec::<&u32>::new())
    );
    assert_eq!(&neighbour_edge_2_b.iter().collect_vec(), &vec![&0]);

    let nodes_a = g.nodes_with_label(Some("a"));
    let nodes_b = g.nodes_with_label(Some("b"));
    assert_eq!(nodes_a.collect_vec(), vec![1]);
    assert_eq!(nodes_b.collect_vec(), vec![2, 0]);

    let edges_a = g.edges_with_label(Some("a"));
    let edges_b = g.edges_with_label(Some("b"));
    assert_eq!(edges_a.collect_vec(), Vec::<(u32, u32)>::new());
    assert_eq!(edges_b.collect_vec(), Vec::<(u32, u32)>::new());
}

#[test]
fn test_clone() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::<Void>::new(edge_vec, Some(in_edge_vec), None, None);
    assert_eq!(g, g.clone());
}

#[test]
fn test_graphflow_planner() {
    let mut g_: TypedGraphMap<u32, u32, u32, Directed, u32> = TypedDiGraphMap::new();
    let path_to_nodes =
        Path::new("C:\\Users\\cheny\\OneDrive\\桌面\\rust_graphflow\\human-vertices.csv");
    let path_to_edges =
        Path::new("C:\\Users\\cheny\\OneDrive\\桌面\\rust_graphflow\\human-edges.csv");
    read_from_csv(
        &mut g_,
        vec![path_to_nodes],
        vec![path_to_edges],
        None,
        false,
        false,
    );
    let g = g_.into_static();
    println!("node_count={}", g.node_count());
    println!("edge_count={}", g.edge_count());
    println!("num_of_node_labels={}", g.num_of_node_labels());
    println!("num_of_edge_labels={}", g.num_of_edge_labels());
    println!("load finished.");
    let catalog = catalog_generator::default(&g);
    let mut qvertex_to_qedges_map = HashMap::new();
    let mut qvertex_to_type_map = HashMap::new();
    let mut qvertex_to_deg_map = HashMap::new();

    let q_edges = vec![
        QueryEdge::default("a".to_owned(), "b".to_owned()),
        QueryEdge::default("a".to_owned(), "c".to_owned()),
        QueryEdge::default("b".to_owned(), "c".to_owned()),
        QueryEdge::default("c".to_owned(), "e".to_owned()),
        QueryEdge::default("c".to_owned(), "f".to_owned()),
        QueryEdge::default("e".to_owned(), "f".to_owned()),
    ];
    let mut qedges_map = HashMap::new();
    qedges_map.insert(
        "b".to_owned(),
        vec![QueryEdge::default("a".to_owned(), "b".to_owned())],
    );
    qedges_map.insert(
        "c".to_owned(),
        vec![QueryEdge::default("a".to_owned(), "c".to_owned())],
    );
    qvertex_to_qedges_map.insert("a".to_owned(), qedges_map);
    qedges_map = HashMap::new();
    qedges_map.insert(
        "a".to_owned(),
        vec![QueryEdge::default("a".to_owned(), "b".to_owned())],
    );
    qedges_map.insert(
        "c".to_owned(),
        vec![QueryEdge::default("b".to_owned(), "c".to_owned())],
    );
    qvertex_to_qedges_map.insert("b".to_owned(), qedges_map);
    qedges_map = HashMap::new();
    qedges_map.insert(
        "a".to_owned(),
        vec![QueryEdge::default("a".to_owned(), "c".to_owned())],
    );
    qedges_map.insert(
        "b".to_owned(),
        vec![QueryEdge::default("b".to_owned(), "c".to_owned())],
    );
    qedges_map.insert(
        "e".to_owned(),
        vec![QueryEdge::default("c".to_owned(), "e".to_owned())],
    );
    qedges_map.insert(
        "f".to_owned(),
        vec![QueryEdge::default("c".to_owned(), "f".to_owned())],
    );
    qvertex_to_qedges_map.insert("c".to_owned(), qedges_map);
    qedges_map = HashMap::new();
    qedges_map.insert(
        "c".to_owned(),
        vec![QueryEdge::default("c".to_owned(), "e".to_owned())],
    );
    qedges_map.insert(
        "f".to_owned(),
        vec![QueryEdge::default("e".to_owned(), "f".to_owned())],
    );
    qvertex_to_qedges_map.insert("e".to_owned(), qedges_map);
    qedges_map = HashMap::new();
    qedges_map.insert(
        "c".to_owned(),
        vec![QueryEdge::default("c".to_owned(), "f".to_owned())],
    );
    qedges_map.insert(
        "e".to_owned(),
        vec![QueryEdge::default("e".to_owned(), "f".to_owned())],
    );
    qvertex_to_qedges_map.insert("f".to_owned(), qedges_map);

    qvertex_to_type_map.insert("a".to_owned(), 0);
    qvertex_to_type_map.insert("b".to_owned(), 0);
    qvertex_to_type_map.insert("c".to_owned(), 0);
    qvertex_to_type_map.insert("e".to_owned(), 0);
    qvertex_to_type_map.insert("f".to_owned(), 0);

    qvertex_to_deg_map.insert("a".to_owned(), vec![2, 0]);
    qvertex_to_deg_map.insert("b".to_owned(), vec![1, 1]);
    qvertex_to_deg_map.insert("c".to_owned(), vec![2, 2]);
    qvertex_to_deg_map.insert("e".to_owned(), vec![1, 1]);
    qvertex_to_deg_map.insert("f".to_owned(), vec![0, 2]);

    let query_graph = QueryGraph {
        qvertex_to_qedges_map,
        qvertex_to_type_map,
        qvertex_to_deg_map,
        q_edges,
        it: None,
        encoding: None,
        limit: 0,
    };
    let mut query_plan = optimizer_executor::generate_plan(query_graph, catalog, g.clone());
    println!("QueryPlan output:{}", query_plan.get_output_log());
    let mut workers = QPWorkers::new(query_plan, 1);
    workers.init(&g);
    workers.execute();
    println!("QueryPlan output:{}", workers.get_output_log());
    assert!(false);
}
