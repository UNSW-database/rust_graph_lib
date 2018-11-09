extern crate rust_graph;

use std::path::Path;

use rust_graph::graph_impl::UnGraphMap;
use rust_graph::io::read_from_csv;
use rust_graph::io::serde::{Serialize, Serializer};
use rust_graph::prelude::*;

fn main() {
    /// `cargo run` -> The default ID type can hold 4294967295 nodes at maximum.
    /// `cargo run --features=usize_id` -> The default ID type can hold 18446744073709551615 nodes at maximum.
    println!(
        "The default ID type can hold {} nodes at maximum.",
        UnGraphMap::<Void>::new().max_possible_id()
    );

    let args: Vec<_> = std::env::args().collect();

    let out_file = Path::new(&args[1]);

    let mut g = UnGraphMap::<Void, Void, Void>::new();
    println!("Reading graph");
    read_from_csv(&mut g, None, out_file, None, true, false).expect("Error when loading csv");

    println!("Exporting graph");

    Serializer::export(&g, "out.graphmap").unwrap();
}
