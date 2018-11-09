extern crate rust_graph;
extern crate time;

use std::fs::create_dir_all;
use std::path::Path;

use time::PreciseTime;

use rust_graph::io::serde::{Deserialize, Deserializer};
use rust_graph::io::write_to_csv;
use rust_graph::prelude::*;
use rust_graph::UnStaticGraph;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let in_file = Path::new(&args[1]);
    let out_dir = Path::new(&args[2]);

    let start = PreciseTime::now();

    println!("Loading {:?}", &in_file);
    let g =
        Deserializer::import::<UnStaticGraph<DefaultId>, _>(in_file).expect("Deserializer error");

    println!("{:?}", g.get_node_label_map());
    println!("{:?}", g.get_edge_label_map());

    if !out_dir.exists() {
        create_dir_all(out_dir).unwrap();
    }

    println!("Exporting to {:?}...", &out_dir);

    write_to_csv(&g, out_dir.join("nodes.csv"), out_dir.join("edges.csv")).unwrap();

    let end = PreciseTime::now();

    println!("Finished in {} seconds.", start.to(end));
}
