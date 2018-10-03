extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::{DiStaticGraphConverter, UnStaticGraphConverter};

#[test]
fn test_undirected() {
    let mut g = rust_graph::UnGraphMap::<&str>::new();

    g.add_node(0, Some("n"));
    g.add_node(1, Some("n"));
    g.add_node(2, Some("m"));
    g.add_node(3, None);

    g.add_edge(0, 1, Some("a"));
    g.add_edge(1, 2, Some("b"));
    g.add_edge(2, 0, None);

    let converter = UnStaticGraphConverter::new(g, false, false);
    let g = converter.convert();

    let edges: Vec<_> = g.edge_indices().collect();

    assert_eq!(edges, vec![(0, 1), (0, 2), (1, 2)]);

    assert_eq!(g.get_node_label(0), Some(&"n"));
    assert_eq!(g.get_node_label(1), Some(&"n"));
    assert_eq!(g.get_node_label(2), Some(&"m"));
    assert_eq!(g.get_node_label(3), None);

    assert_eq!(g.get_edge_label(0, 1), Some(&"a"));
    assert_eq!(g.get_edge_label(1, 2), Some(&"b"));
    assert_eq!(g.get_edge_label(2, 0), None);
}

#[test]
fn test_undirected_reorder() {
    let mut g = rust_graph::UnGraphMap::<&str>::new();

    g.add_node(0, Some("n"));
    g.add_node(1, Some("n"));
    g.add_node(2, Some("m"));
    g.add_node(3, None);

    g.add_edge(0, 1, Some("a"));
    g.add_edge(1, 2, Some("b"));
    g.add_edge(2, 0, None);

    let converter = UnStaticGraphConverter::new(g, true, true);

    println!("node id map:{:?}", converter.get_node_id_map());
    let g = converter.convert();

    let edges: Vec<_> = g.edge_indices().collect();

    assert_eq!(edges, vec![(1, 2), (1, 3), (2, 3)]);

    println!("{:?}", &g);

    let node0 = converter.find_new_node_id(0);
    println!("0->{}", node0);

    let node1 = converter.find_new_node_id(1);
    let node2 = converter.find_new_node_id(2);
    let node3 = converter.find_new_node_id(3);

    assert_eq!(g.get_node_label(node0), Some(&"n"));
    assert_eq!(g.get_node_label(node1), Some(&"n"));
    assert_eq!(g.get_node_label(node2), Some(&"m"));
    assert_eq!(g.get_node_label(node3), None);

    assert_eq!(g.get_edge_label(node0, node1), Some(&"a"));
    assert_eq!(g.get_edge_label(node1, node2), Some(&"b"));
    assert_eq!(g.get_edge_label(node0, node2), None);
}

#[test]
fn test_directed() {
    let mut g = rust_graph::DiGraphMap::<&str>::new();

    g.add_node(0, None);
    g.add_node(1, None);
    g.add_node(2, None);
    g.add_node(3, None);

    g.add_edge(1, 0, Some("a"));
    g.add_edge(2, 0, Some("a"));
    g.add_edge(2, 3, Some("b"));
    g.add_edge(3, 0, Some("a"));
    g.add_edge(3, 1, Some("a"));
    g.add_edge(3, 2, Some("a"));

    let converter = DiStaticGraphConverter::new(g, true, true);
    let g = converter.convert();

    let edges: Vec<_> = g.edge_indices().collect();

    assert_eq!(edges, vec![(1, 0), (2, 0), (2, 3), (3, 0), (3, 1), (3, 2)]);
}
