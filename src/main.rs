extern crate rust_graph;

use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;

fn main() {
    let directed_graph = DiGraphMap::<Void>::new();
    let undirected_graph = UnGraphMap::<Void>::new();
    assert!(directed_graph.is_directed());
    assert!(!undirected_graph.is_directed());

    /// `cargo run` -> The default ID type can hold 4294967295 nodes at maximum.
    /// `cargo run --features=usize_id` -> The default ID type can hold 18446744073709551615 nodes at maximum.
    println!(
        "The default ID type can hold {} nodes at maximum.",
        directed_graph.max_possible_id()
    )
}

fn num_of_in_neighbors<Id: IdType>(g: impl GeneralGraph<Id>, node: Id) -> Option<usize> {
    if let Some(dg) = g.as_digraph() {
        Some(dg.in_neighbors(node).len())
    } else {
        None
    }
}
