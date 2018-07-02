extern crate rust_graph;

use std::hash::Hash;

use rust_graph::graph_impl::{DiGraphMap, UnGraphMap,DiStaticGraph};
use rust_graph::prelude::*;
use rust_graph::io::serde::*;

fn main() {
    let mut directed_graph = DiGraphMap::<Void>::new();
    let mut undirected_graph = UnGraphMap::<Void>::new();
    assert!(directed_graph.is_directed());
    assert!(!undirected_graph.is_directed());

    directed_graph.add_edge(0, 1, None);
    undirected_graph.add_edge(0, 1, None);

    assert_eq!(num_of_in_neighbors(&directed_graph, 0), 0);
    assert_eq!(num_of_in_neighbors(&undirected_graph, 0), 1);

    /// `cargo run` -> The default ID type can hold 4294967295 nodes at maximum.
    /// `cargo run --features=usize_id` -> The default ID type can hold 18446744073709551615 nodes at maximum.
    println!(
        "The default ID type can hold {} nodes at maximum.",
        directed_graph.max_possible_id()
    );

    let old_graph = Deserializer::import::<DiStaticGraph<Void>>("old_graph.bin").unwrap();

}

fn num_of_in_neighbors<Id: IdType, NL: Hash + Eq, EL: Hash + Eq>(
    g: &impl GeneralGraph<Id, NL, EL>,
    node: Id,
) -> usize {
    if let Some(dg) = g.as_digraph() {
        dg.in_neighbors(node).len()
    } else {
        g.as_graph().neighbors(node).len()
    }
}
