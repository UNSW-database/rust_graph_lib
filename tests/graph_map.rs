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

use std::collections::HashMap;

use rust_graph::generic::{MutEdgeType, MutNodeType};
use rust_graph::graph_impl::graph_map::{Edge, MutEdge, NodeMap, NodeMapTrait};
use rust_graph::prelude::*;
use rust_graph::{DiGraphMap, UnGraphMap};

#[test]
fn test_add_get_node() {
    let mut g = UnGraphMap::<&str>::new();

    g.add_node(0, Some("a"));
    assert_eq!(g.node_count(), 1);
    g.add_node(1, Some("b"));
    assert_eq!(g.node_count(), 2);
    g.add_node(2, Some("a"));
    assert_eq!(g.node_count(), 3);
    g.add_node(3, None);
    assert_eq!(g.node_count(), 4);

    let n0_expected = NodeMap::new(0, Some(0));
    let n1_expected = NodeMap::new(1, Some(1));
    let mut n2_expected = NodeMap::new(2, Some(0));
    let mut n3_expected = NodeMap::new(3, None);

    assert_eq!(g.get_node(0).unwrap_nodemap(), &n0_expected);
    assert_eq!(g.get_node(1).unwrap_nodemap(), &n1_expected);
    assert_eq!(g.get_node_mut(2), MutNodeType::NodeMapRef(&mut n2_expected));
    assert_eq!(g.get_node_mut(3), MutNodeType::NodeMapRef(&mut n3_expected));
    assert!(g.get_node(4).is_none());
}

#[test]
fn test_duplicate_node() {
    let mut g = UnGraphMap::<&str>::new();

    g.add_node(0, Some("a"));
    assert!(!g.add_node(0, None));
}

#[test]
fn test_remove_node_directed() {
    let mut g = DiGraphMap::<&str>::new();

    g.add_node(0, None);
    g.add_node(1, None);
    g.add_node(2, None);

    g.add_edge(0, 1, None);
    g.add_edge(0, 2, None);
    g.add_edge(1, 0, None);

    g.remove_node(0);

    assert_eq!(g.node_count(), 2);
    assert_eq!(g.edge_count(), 0);

    assert!(!g.has_node(0));

    assert!(!g.has_edge(0, 1));
    assert!(!g.has_edge(0, 2));
    assert!(!g.has_edge(1, 0));
}

#[test]
fn test_remove_node_undirected() {
    let mut g = UnGraphMap::<&str>::new();

    g.add_node(0, None);
    g.add_node(1, None);
    g.add_node(2, None);

    g.add_edge(0, 1, None);
    g.add_edge(0, 2, None);
    g.add_edge(1, 2, None);

    g.remove_node(0);

    assert_eq!(g.node_count(), 2);
    assert_eq!(g.edge_count(), 1);

    assert!(!g.has_node(0));

    assert!(!g.has_edge(0, 1));
    assert!(!g.has_edge(1, 0));
    assert!(!g.has_edge(0, 2));
    assert!(!g.has_edge(2, 0));
}

#[test]
fn test_add_get_edge_directed() {
    let mut g = DiGraphMap::<&str>::new();

    g.add_node(0, None);
    g.add_node(1, None);
    g.add_node(2, None);

    g.add_edge(0, 1, Some("a"));
    assert_eq!(g.edge_count(), 1);
    g.add_edge(1, 2, Some("b"));
    assert_eq!(g.edge_count(), 2);
    g.add_edge(2, 0, Some("a"));
    assert_eq!(g.edge_count(), 3);
    g.add_edge(1, 0, None);
    assert_eq!(g.edge_count(), 4);
    g.add_edge(0, 0, None);
    assert_eq!(g.edge_count(), 5);

    let e0_expected = Edge::new(0, 1, Some(0));
    let e1_expected = Edge::new(1, 2, Some(1));
    let mut e2_expected = Some(DefaultId::new(0));

    assert_eq!(g.get_edge(0, 1).unwrap(), e0_expected);
    assert_eq!(g.get_edge(1, 2).unwrap(), e1_expected);
    assert_eq!(
        g.get_edge_mut(2, 0),
        MutEdgeType::EdgeRef(MutEdge::new(2, 0, &mut e2_expected))
    );
    assert_eq!(
        g.get_edge_mut(1, 0),
        MutEdgeType::EdgeRef(MutEdge::new(1, 0, &mut None))
    );

    assert!(g.get_edge(1, 3).is_none());
    assert!(g.get_edge_mut(2, 1).is_none());

    assert!(g.has_edge(0, 0));
}

#[test]
fn test_add_get_edge_undirected() {
    let mut g = UnGraphMap::<&str>::new();

    g.add_node(0, None);
    g.add_node(1, None);
    g.add_node(2, None);
    g.add_node(3, None);

    g.add_edge(0, 1, Some("a"));
    assert_eq!(g.edge_count(), 1);
    g.add_edge(1, 2, Some("b"));
    assert_eq!(g.edge_count(), 2);
    g.add_edge(2, 0, Some("a"));
    assert_eq!(g.edge_count(), 3);
    g.add_edge(0, 0, None);
    assert_eq!(g.edge_count(), 4);

    let e0_expected = Edge::new(0, 1, Some(0));
    let e1_expected = Edge::new(1, 2, Some(1));
    let e1_expected_1 = Edge::new(2, 1, Some(1));
    let mut e2_expected = Some(DefaultId::new(0));

    assert_eq!(g.get_edge(0, 1).unwrap(), e0_expected);
    assert_eq!(g.get_edge(1, 2).unwrap(), e1_expected);
    assert_eq!(g.get_edge(2, 1).unwrap(), e1_expected_1);
    assert_eq!(
        g.get_edge_mut(2, 0),
        MutEdgeType::EdgeRef(MutEdge::new(2, 0, &mut e2_expected))
    );
    assert_eq!(
        g.get_edge_mut(0, 2),
        MutEdgeType::EdgeRef(MutEdge::new(0, 2, &mut e2_expected))
    );

    assert!(g.get_edge(1, 3).is_none());
    assert!(g.get_edge_mut(0, 3).is_none());

    assert!(g.has_edge(0, 0));
}

#[test]
fn test_multi_adding() {
    let mut g = DiGraphMap::<&str>::new();
    g.add_node(0, None);
    g.add_node(1, None);
    g.add_edge(0, 1, None);

    assert!(!g.add_node(0, Some("label")));
    assert_eq!(g.get_node_label(0), Some(&"label"));

    assert!(!g.add_edge(0, 1, Some("label")));
    assert_eq!(g.get_edge_label(0, 1), Some(&"label"));

    let mut g = UnGraphMap::<&str>::new();
    g.add_node(0, None);
    g.add_node(1, None);
    g.add_edge(0, 1, None);

    assert!(!g.add_node(0, Some("label")));
    assert_eq!(g.get_node_label(0), Some(&"label"));

    assert!(!g.add_edge(1, 0, Some("label")));
    assert_eq!(g.get_edge_label(0, 1), Some(&"label"));
}

#[test]
fn test_remove_edge_directed() {
    let mut g = DiGraphMap::<&str>::new();
    g.add_node(0, None);
    g.add_node(1, None);
    g.add_node(2, None);
    g.add_edge(0, 1, None);
    g.add_edge(1, 0, None);
    g.add_edge(1, 2, None);

    g.remove_edge(0, 1);

    assert_eq!(g.edge_count(), 2);
    assert!(!g.has_edge(0, 1));
    assert!(!g.get_node(0).unwrap_nodemap().neighbors().contains(&1));
    assert!(!g.get_node(1).unwrap_nodemap().in_neighbors().contains(&0));

    g.remove_edge(1, 2);
    assert_eq!(g.edge_count(), 1);
    assert!(!g.has_edge(1, 2));
    assert!(!g.get_node(1).unwrap_nodemap().neighbors().contains(&2));
    assert!(!g.get_node(2).unwrap_nodemap().in_neighbors().contains(&1));

    assert!(g.has_edge(1, 0));
}

#[test]
fn test_remove_edge_undirected() {
    let mut g = UnGraphMap::<&str>::new();
    g.add_node(0, None);
    g.add_node(1, None);
    g.add_node(2, None);
    g.add_edge(0, 1, None);
    g.add_edge(2, 1, None);

    g.remove_edge(1, 0);
    assert_eq!(g.edge_count(), 1);
    assert!(!g.has_edge(0, 1));
    assert!(!g.get_node(0).unwrap_nodemap().neighbors().contains(&1));
    assert!(!g.get_node(1).unwrap_nodemap().neighbors().contains(&0));

    g.remove_edge(1, 2);
    assert_eq!(g.edge_count(), 0);
    assert!(!g.get_node(1).unwrap_nodemap().neighbors().contains(&2));
    assert!(!g.get_node(2).unwrap_nodemap().neighbors().contains(&1));
}

#[test]
fn test_get_node_edge_label() {
    let mut g = DiGraphMap::<&str>::new();
    g.add_node(0, Some("0"));
    g.add_node(1, None);
    g.add_edge(0, 1, Some("(0,1)"));
    g.add_edge(1, 0, None);

    assert_eq!(g.get_node_label(0), Some(&"0"));
    assert_eq!(g.get_node_label(1), None);

    assert_eq!(g.get_edge_label(0, 1), Some(&"(0,1)"));
    assert_eq!(g.get_edge_label(1, 0), None);
}

#[test]
fn test_iter() {
    let mut g = DiGraphMap::<&str>::new();
    g.add_node(0, Some("0"));
    g.add_node(1, Some("1"));
    g.add_node(2, None);
    g.add_edge(0, 1, Some("(0,1)"));
    g.add_edge(1, 0, Some("(1,0)"));
    g.add_edge(1, 2, None);

    let node_ids: Vec<_> = g.node_indices().collect();
    assert_eq!(node_ids.len(), 3);
    assert!(node_ids.contains(&&0));
    assert!(node_ids.contains(&&1));
    assert!(node_ids.contains(&&2));

    let edge_ids: Vec<_> = g.edge_indices().collect();
    assert_eq!(edge_ids.len(), 3);
    assert!(edge_ids.contains(&&(0, 1)));
    assert!(edge_ids.contains(&&(1, 0)));
    assert!(edge_ids.contains(&&(1, 2)));

    let nodes: Vec<_> = g.nodes().collect();
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains(&g.get_node(0)));
    assert!(nodes.contains(&g.get_node(1)));
    assert!(nodes.contains(&g.get_node(2)));

    let edges: Vec<_> = g.edges().collect();
    assert_eq!(edges.len(), 3);
    assert!(edges.contains(&g.get_edge(0, 1)));
    assert!(edges.contains(&g.get_edge(1, 0)));
    assert!(edges.contains(&g.get_edge(1, 2)));

    let node_labels: Vec<_> = g.node_labels().collect();
    assert_eq!(node_labels.len(), 2);
    assert!(node_labels.contains(&&"0"));
    assert!(node_labels.contains(&&"1"));

    let edge_labels: Vec<_> = g.edge_labels().collect();
    assert_eq!(edge_labels.len(), 2);
    assert!(edge_labels.contains(&&"(0,1)"));
    assert!(edge_labels.contains(&&"(1,0)"));

    let neighbors_of_1: Vec<_> = g.neighbors_iter(1).collect();
    assert_eq!(neighbors_of_1.len(), 2);
    assert!(neighbors_of_1.contains(&&0));
    assert!(neighbors_of_1.contains(&&2));
}

#[test]
fn test_iter_mut() {
    let mut g = DiGraphMap::<&str>::new();
    g.add_node(0, None);
    g.add_node(1, None);
    g.add_edge(0, 1, None);
    g.add_edge(1, 0, None);

    let mut n0 = g.get_node(0).unwrap_nodemap().clone();
    let mut n1 = g.get_node(1).unwrap_nodemap().clone();

    {
        let nodes: Vec<_> = g.nodes_mut().map(|n| n.unwrap_nodemap_ref()).collect();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&&mut n0));
        assert!(nodes.contains(&&mut n1));
    }

    let mut e0 = g.get_edge(0, 1).unwrap().clone().get_label_id();
    let mut e1 = g.get_edge(1, 0).unwrap().clone().get_label_id();

    {
        let edges: Vec<_> = g.edges_mut().map(|e| e.unwrap_edge_ref()).collect();
        assert_eq!(edges.len(), 2);
        assert!(edges.contains(&MutEdge::new(0, 1, &mut e0)));
        assert!(edges.contains(&MutEdge::new(1, 0, &mut e1)));
    }
}

#[test]
fn test_neighbors() {
    let mut g = DiGraphMap::<u8>::new();
    g.add_node(0, Some(0));
    g.add_node(1, Some(0));
    g.add_node(2, None);

    g.add_edge(0, 1, Some(0));
    g.add_edge(0, 2, Some(0));
    g.add_edge(2, 1, None);

    assert_eq!(g.neighbors(0)[..], [1, 2]);
    assert_eq!(&g.in_neighbors(1)[..], [0, 2]);

    assert_eq!(g.degree(0), 2);
    assert_eq!(g.in_degree(1), 2);

    assert!(g.neighbors(1).is_empty());
    assert!(g.in_neighbors(0).is_empty());

    assert_eq!(g.degree(1), 0);
    assert_eq!(g.in_degree(0), 0);
}

#[test]
fn test_max_id() {
    let mut g = DiGraphMap::<u8>::new();

    assert_eq!(g.max_seen_id(), None);

    g.add_node(1, Some(0));
    assert_eq!(g.max_seen_id(), Some(1));

    g.add_node(0, Some(0));
    assert_eq!(g.max_seen_id(), Some(1));

    g.add_node(2, None);
    assert_eq!(g.max_seen_id(), Some(2));

    g.add_edge(3, 4, Some(0));
    assert_eq!(g.max_seen_id(), Some(4));

    g.add_edge(6, 5, Some(0));
    assert_eq!(g.max_seen_id(), Some(6));
}

#[test]
fn test_clone() {
    let mut g = DiGraphMap::<u8>::new();
    g.add_node(0, Some(0));
    g.add_node(1, Some(0));
    g.add_node(2, Some(1));
    g.add_node(3, None);

    g.add_edge(0, 1, Some(0));
    g.add_edge(1, 2, Some(0));
    g.add_edge(2, 3, Some(1));
    g.add_edge(3, 1, None);

    assert_eq!(g, g.clone());
}

#[test]
fn test_stats() {
    let mut g = DiGraphMap::<u8>::new();
    g.add_node(0, Some(0));
    g.add_node(1, Some(0));
    g.add_node(2, Some(1));
    g.add_node(3, None);

    g.add_edge(0, 1, Some(0));
    g.add_edge(1, 2, Some(0));
    g.add_edge(2, 3, Some(1));
    g.add_edge(3, 1, None);

    let mut expected_counter = HashMap::new();
    expected_counter.insert(0, 2);
    expected_counter.insert(1, 1);

    assert_eq!(g.get_node_label_id_counter().into_map(), expected_counter);
    assert_eq!(g.get_edge_label_id_counter().into_map(), expected_counter)
}
