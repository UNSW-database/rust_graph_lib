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

    let converter = UnStaticGraphConverter::new(&g);
    let g = converter.get_graph();

    let edges: Vec<_> = g.edge_indices().collect();

    assert_eq!(edges, vec![(1, 2), (1, 3), (2, 3)]);

    let node0 = converter.find_new_node_id(0).unwrap();
    let node1 = converter.find_new_node_id(1).unwrap();
    let node2 = converter.find_new_node_id(2).unwrap();
    let node3 = converter.find_new_node_id(3).unwrap();

    assert_eq!(g.get_node_label(node0),Some(&"n"));
    assert_eq!(g.get_node_label(node1),Some(&"n"));
    assert_eq!(g.get_node_label(node2),Some(&"m"));
    assert_eq!(g.get_node_label(node3),None);

    assert_eq!(g.get_edge_label(node0,node1),Some(&"a"));
    assert_eq!(g.get_edge_label(node1,node2),Some(&"b"));
    assert_eq!(g.get_edge_label(node0,node2),None);
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

    let converter = DiStaticGraphConverter::new(&g);
    let g = converter.get_graph();

    let edges: Vec<_> = g.edge_indices().collect();

    assert_eq!(edges, vec![(1, 0), (2, 0), (2, 3), (3, 0), (3, 1), (3, 2)]);
}
