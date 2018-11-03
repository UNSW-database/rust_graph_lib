extern crate rust_graph;

use rust_graph::graph_gen::random_gnp_graph;
use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;

fn main() {
    /// `cargo run` -> The default ID type can hold 4294967295 nodes at maximum.
    /// `cargo run --features=usize_id` -> The default ID type can hold 18446744073709551615 nodes at maximum.
    println!(
        "The default ID type can hold {} nodes at maximum.",
        UnGraphMap::<Void>::new().max_possible_id()
    );

    println!("Generating random graph...");
    let graph: UnGraphMap<usize> =
        random_gnp_graph(10000, 0.5, (0..10).collect(), (0..10).collect());

    println!("Reordering id...");
    let new_graph = graph.reorder_id(true, true, true);
}
