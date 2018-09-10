extern crate rust_graph;
extern crate time;

use std::path::Path;

use time::PreciseTime;

use rust_graph::converter::{DiStaticGraphConverter, UnStaticGraphConverter};
use rust_graph::io::read_from_csv;
use rust_graph::io::serde::{Serialize, Serializer};
use rust_graph::prelude::*;
use rust_graph::{DiGraphMap, UnGraphMap};

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let node_file = Path::new(&args[1]);
    let edge_file = Path::new(&args[2]);
    let out_file = Path::new(&args[3]);
    let is_directed: bool = (&args[4]).parse().unwrap();

    let start = PreciseTime::now();

    if is_directed {
        let mut g = DiGraphMap::<String>::new();
        read_from_csv(&mut g, &node_file, &edge_file).expect("Error when loading csv");

        let static_graph = DiStaticGraphConverter::new(&g).to_graph();
        Serializer::export(&static_graph, out_file).unwrap();
    } else {
        let mut g = UnGraphMap::<String>::new();
        read_from_csv(&mut g, &node_file, &edge_file).expect("Error when loading csv");

        let static_graph = UnStaticGraphConverter::new(&g).to_graph();
        Serializer::export(&static_graph, out_file).unwrap();
    }

    let end = PreciseTime::now();
    println!("Finished in {} seconds.", start.to(end));
}
