extern crate rust_graph;

use std::path::Path;

use rust_graph::graph_impl::{DiStaticGraph, UnStaticGraph};
use rust_graph::io::serde::{Deserialize, Deserializer, Serialize, Serializer};
use rust_graph::prelude::*;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let in_graph = Path::new(&args[1]);
    let out_file = Path::new(&args[2]);

    let mut graph: UnStaticGraph<DefaultId> = Deserializer::import(in_graph).unwrap();

    graph.remove_labels();

    Serializer::export(&graph, out_file).unwrap();
}
