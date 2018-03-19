extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::{DiStaticGraph, UnStaticGraph};

#[test]
fn test_undirected() {
    let mut g = rust_graph::UnGraphMap::<&str>::new();

    g.add_node(0, None);
    g.add_node(1, None);
    g.add_node(2, None);
    g.add_node(3, None);

    g.add_edge(0, 1, Some("a"));
    g.add_edge(1, 2, Some("b"));
    g.add_edge(2, 0, Some("a"));

    let g = UnStaticGraph::from(g);

    println!("g: {:?}", g);

    let edges: Vec<_> = g.edge_indices().collect();

    assert_eq!(edges, vec![(1, 2), (1, 3), (2, 3)]);
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

    let g = DiStaticGraph::from(g);

    println!("g: {:?}", g);

    let edges: Vec<_> = g.edge_indices().collect();

    assert_eq!(edges, vec![(1, 0), (2, 0), (2, 3), (3, 0), (3, 1), (3, 2)]);
}
