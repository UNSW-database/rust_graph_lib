extern crate rust_graph;
extern crate time;

use std::path::Path;

use time::PreciseTime;

use rust_graph::io::serde::{Serialize, Serializer};
use rust_graph::io::*;
use rust_graph::prelude::*;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let ldbc_dir = Path::new(&args[1]);
    let output_dir = Path::new(&args[2]);

    let start = PreciseTime::now();

    println!("Loading {:?}", &ldbc_dir);
    let g = read_ldbc_from_path::<u32, Undirected, _>(ldbc_dir);
    let num_of_nodes = g.node_count();
    let num_of_edges = g.edge_count();

    println!("{} nodes, {} edges.", num_of_nodes, num_of_edges);

    println!("Node labels: {:?}", g.get_node_label_map());
    println!("Edge labels: {:?}", g.get_edge_label_map());

    let dir_name = ldbc_dir
        .components()
        .last()
        .unwrap()
        .as_os_str()
        .to_str()
        .unwrap();
    let export_filename = format!("{}_{}_{}.bin", dir_name, num_of_nodes, num_of_edges);
    let export_path = output_dir.join(export_filename);

    println!("Exporting to {:?}...", export_path);

    Serializer::export(&g, export_path).unwrap();

    let end = PreciseTime::now();

    println!("Finished in {} seconds.", start.to(end));
}
