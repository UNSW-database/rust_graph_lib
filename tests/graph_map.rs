extern crate rust_graph;

use std::collections::HashMap;

use rust_graph::prelude::*;

use rust_graph::{DiGraphMap, UnGraphMap};
use rust_graph::graph_impl::graph_map::{Edge, NodeMap};

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

    assert_eq!(g.get_node(0), Some(&n0_expected));
    assert_eq!(g.get_node(1), Some(&n1_expected));
    assert_eq!(g.get_node_mut(2), Some(&mut n2_expected));
    assert_eq!(g.get_node_mut(3), Some(&mut n3_expected));
    assert_eq!(g.get_node(4), None);
}

#[test]
#[should_panic]
fn test_duplicate_node() {
    let mut g = UnGraphMap::<&str>::new();

    g.add_node(0, Some("a"));
    g.add_node(0, None);
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

    let e0_expected = Edge::new(0, 1, Some(0));
    let e1_expected = Edge::new(1, 2, Some(1));
    let mut e2_expected = Edge::new(2, 0, Some(0));
    let mut e3_expected = Edge::new(1, 0, None);

    assert_eq!(g.find_edge(0, 1), Some(&e0_expected));
    assert_eq!(g.find_edge(1, 2), Some(&e1_expected));
    assert_eq!(g.find_edge_mut(2, 0), Some(&mut e2_expected));
    assert_eq!(g.find_edge_mut(1, 0), Some(&mut e3_expected));

    assert_eq!(g.find_edge(1, 3), None);
    assert_eq!(g.find_edge_mut(2, 1), None);
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

    let e0_expected = Edge::new(0, 1, Some(0));
    let e1_expected = Edge::new(1, 2, Some(1));
    let mut e2_expected = Edge::new(0, 2, Some(0));

    assert_eq!(g.find_edge(0, 1), Some(&e0_expected));
    assert_eq!(g.find_edge(1, 2), Some(&e1_expected));
    assert_eq!(g.find_edge(2, 1), Some(&e1_expected));
    assert_eq!(g.find_edge_mut(2, 0), Some(&mut e2_expected));
    assert_eq!(g.find_edge_mut(0, 2), Some(&mut e2_expected));

    assert_eq!(g.find_edge(1, 3), None);
    assert_eq!(g.find_edge_mut(0, 3), None);
}

#[test]
#[should_panic]
fn test_multi_edge_directed() {
    let mut g = DiGraphMap::<&str>::new();
    g.add_node(0, None);
    g.add_node(1, None);
    g.add_edge(0, 1, None);
    g.add_edge(0, 1, None);
}

#[test]
#[should_panic]
fn test_multi_edge_undirected() {
    let mut g = UnGraphMap::<&str>::new();
    g.add_node(0, None);
    g.add_node(1, None);
    g.add_edge(0, 1, None);
    g.add_edge(1, 0, None);
}

#[test]
#[should_panic]
fn test_invalid_edge() {
    let mut g = DiGraphMap::<&str>::new();
    g.add_edge(0, 1, None);
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

    g.remove_edge(1, 2);
    assert_eq!(g.edge_count(), 1);
    assert!(!g.has_edge(1, 2));

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

    g.remove_edge(1, 2);
    assert_eq!(g.edge_count(), 0);
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
    assert!(nodes.contains(&g.get_node(0).unwrap()));
    assert!(nodes.contains(&g.get_node(1).unwrap()));
    assert!(nodes.contains(&g.get_node(2).unwrap()));

    let edges: Vec<_> = g.edges().collect();
    assert_eq!(edges.len(), 3);
    assert!(edges.contains(&g.find_edge(0, 1).unwrap()));
    assert!(edges.contains(&g.find_edge(1, 0).unwrap()));
    assert!(edges.contains(&g.find_edge(1, 2).unwrap()));

    let node_labels: Vec<_> = g.node_labels().collect();
    assert_eq!(node_labels.len(), 2);
    assert!(node_labels.contains(&&"0"));
    assert!(node_labels.contains(&&"1"));

    let edge_labels: Vec<_> = g.edge_labels().collect();
    assert_eq!(edge_labels.len(), 2);
    assert!(edge_labels.contains(&&"(0,1)"));
    assert!(edge_labels.contains(&&"(1,0)"));

    let neighbors_of_1: Vec<_> = g.neighbor_indices(1).collect();
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

    let mut n0 = g.get_node(0).unwrap().clone();
    let mut n1 = g.get_node(1).unwrap().clone();

    {
        let nodes: Vec<_> = g.nodes_mut().collect();
        assert_eq!(nodes.len(), 2);
        assert!(nodes.contains(&&mut n0));
        assert!(nodes.contains(&&mut n1));
    }

    let mut e0 = g.find_edge(0, 1).unwrap().clone();
    let mut e1 = g.find_edge(1, 0).unwrap().clone();

    {
        let edges: Vec<_> = g.edges_mut().collect();
        assert_eq!(edges.len(), 2);
        assert!(edges.contains(&&mut e0));
        assert!(edges.contains(&&mut e1));
    }
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

    let mut expected_counter = HashMap::<usize, usize>::new();
    expected_counter.insert(0, 2);
    expected_counter.insert(1, 1);

    assert_eq!(g.get_node_label_id_counter(), expected_counter);
    assert_eq!(g.get_edge_label_id_counter(), expected_counter)
}
