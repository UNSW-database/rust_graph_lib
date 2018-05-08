extern crate rust_graph;

use rust_graph::prelude::*;
use rust_graph::{DiStaticGraph, UnStaticGraph};
use rust_graph::graph_impl::static_graph::EdgeVec;

#[test]
fn test_directed() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::new(3, edge_vec, Some(in_edge_vec));

    assert_eq!(g.neighbors(0).into_owned(), vec![1, 2]);
    assert_eq!(g.neighbors(1).into_owned(), vec![0]);
    assert_eq!(g.neighbors(2).into_owned(), vec![0]);

    assert_eq!(g.in_neighbors(0).into_owned(), vec![1, 2]);
    assert_eq!(g.in_neighbors(1).into_owned(), vec![0]);
    assert_eq!(g.in_neighbors(2).into_owned(), vec![0]);
}

#[test]
fn test_clone() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::new(3, edge_vec, Some(in_edge_vec));
    assert_eq!(g, g.clone());
}
