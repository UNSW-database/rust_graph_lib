extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::{DiGraphMap, UnGraphMap};
use rust_graph::graph_impl::graph_map::{Node, Edge};


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

    let n0_expected = Node::new(0, Some(0));
    let n1_expected = Node::new(1, Some(1));
    let mut n2_expected = Node::new(2, Some(0));
    let mut n3_expected = Node::new(3, None);

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
