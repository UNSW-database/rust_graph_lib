#[macro_use]
extern crate rust_graph;

use rust_graph::graph_impl::static_graph::EdgeVec;
use rust_graph::map::SetMap;
use rust_graph::prelude::*;
use rust_graph::{DiStaticGraph, UnStaticGraph};

#[test]
fn test_directed() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::<Void>::new(3, edge_vec, Some(in_edge_vec), SetMap::new());

    assert_eq!(g.neighbors(0)[..], [1, 2]);
    assert_eq!(&g.neighbors(1)[..], &[0]);
    assert_eq!(&g.neighbors(2)[..], &[0]);

    assert_eq!(g.in_neighbors(0).into_owned(), vec![1, 2]);
    assert_eq!(g.in_neighbors(1).into_owned(), vec![0]);
    assert_eq!(g.in_neighbors(2).into_owned(), vec![0]);
}

#[test]
fn test_label() {
    let edge_vec = EdgeVec::with_labels(vec![0, 2, 3, 4], vec![1, 2, 0, 0], vec![0, 1, 0, 1]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::<&str>::new(3, edge_vec, Some(in_edge_vec), setmap!["a", "b"]);

    assert_eq!(g.get_edge_label(0, 1), Some(&"a"));
    assert_eq!(g.get_edge_label(0, 2), Some(&"b"));
    assert_eq!(g.get_edge_label(1, 0), Some(&"a"));
    assert_eq!(g.get_edge_label(2, 0), Some(&"b"));

    assert_eq!(g.get_edge_label(2, 3), None);
}

#[test]
fn test_clone() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::<Void>::new(3, edge_vec, Some(in_edge_vec), SetMap::new());
    assert_eq!(g, g.clone());
}
