extern crate rust_graph;

use std::path::Path;

use rust_graph::graph_impl::UnGraphMap;
use rust_graph::prelude::*;

fn main() {
    let g = UnGraphMap::<Void>::new();

    /// `cargo run` -> The default ID type can hold 4294967295 nodes at maximum.
    /// `cargo run --features=usize_id` -> The default ID type can hold 18446744073709551615 nodes at maximum.
    println!(
        "The graph can hold {} nodes and {} labels at maximum.",
        g.max_possible_id(),
        g.max_possible_label_id()
    );
}
