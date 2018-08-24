extern crate rust_graph;
extern crate time;

use std::fs::create_dir_all;
use std::path::Path;

use time::PreciseTime;

use rust_graph::converter::UnStaticGraphConverter;
use rust_graph::io::serde::{Deserialize, Deserializer};
use rust_graph::io::write_to_csv;
use rust_graph::prelude::*;
use rust_graph::{UnGraphMap, UnStaticGraph};

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let in_file = Path::new(&args[1]);
    let out_file = Path::new(&args[2]);

    let start = PreciseTime::now();

    println!("Loading {:?}", &in_file);
    //    let g: UnGraphMap<String> = Deserializer::import(in_file).unwrap();
    let g: UnStaticGraph<String> = Deserializer::import(in_file).unwrap();

    println!("{:?}", g.get_node_label_map());
    println!("{:?}", g.get_edge_label_map());

    if !out_file.exists() {
        create_dir_all(out_file).unwrap();
    }

    println!("Exporting to {:?}...", &out_file);

    write_to_csv(&g, out_file.join("nodes.csv"), out_file.join("edges.csv")).unwrap();

    let end = PreciseTime::now();

    println!("Finished in {} seconds.", start.to(end));
}
