extern crate rust_graph;

use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;

fn main() {
    let directed_graph = DiGraphMap::<Void, Void>::new();
    let undirected_graph = UnGraphMap::<Void, Void>::new();
    assert!(directed_graph.is_directed());
    assert!(!undirected_graph.is_directed());
}
