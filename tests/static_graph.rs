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
extern crate tempfile;

use rust_graph::generic::DefaultId;
use rust_graph::graph_impl::static_graph::StaticNode;
use rust_graph::graph_impl::Edge;
use rust_graph::graph_impl::EdgeVec;
use rust_graph::map::SetMap;
use rust_graph::prelude::*;
use rust_graph::{DiStaticGraph, UnStaticGraph};

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
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![1, 2]);
    let fwd_adj_list = g.get_fwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 1, 1, 1]);
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![0]);
    let fwd_adj_list = g.get_fwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 1, 1, 1]);
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![0]);

    let bwd_adj_list = g.get_bwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![1, 2]);
    let bwd_adj_list = g.get_bwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 1, 1, 1]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![0]);
    let bwd_adj_list = g.get_bwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 1, 1, 1]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![0]);

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
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![1, 2]);
    let fwd_adj_list = g.get_fwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![0, 2]);
    let fwd_adj_list = g.get_fwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![0, 1]);

    let bwd_adj_list = g.get_bwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![1, 2]);
    let bwd_adj_list = g.get_bwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![0, 2]);
    let bwd_adj_list = g.get_bwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 2, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![0, 1]);
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
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 0, 1, 2, 2]);
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![1, 2]);
    let fwd_adj_list = g.get_fwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 0, 1, 1, 1]);
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![0]);
    let fwd_adj_list = g.get_fwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(fwd_adj_list.get_offsets(), &vec![0, 0, 0, 1, 1]);
    assert_eq!(fwd_adj_list.get_neighbour_ids(), &vec![0]);

    let bwd_adj_list = g.get_bwd_adj_list()[0].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 0, 1, 2, 2]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![1, 2]);
    let bwd_adj_list = g.get_bwd_adj_list()[1].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 0, 1, 1, 1]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![0]);
    let bwd_adj_list = g.get_bwd_adj_list()[2].as_ref().unwrap();
    assert_eq!(bwd_adj_list.get_offsets(), &vec![0, 0, 0, 1, 1]);
    assert_eq!(bwd_adj_list.get_neighbour_ids(), &vec![0]);
}

#[test]
fn test_clone() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::<Void>::new(edge_vec, Some(in_edge_vec), None, None);
    assert_eq!(g, g.clone());
}
