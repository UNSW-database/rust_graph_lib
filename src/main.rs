extern crate rust_graph;

use std::path::Path;

use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
use rust_graph::io::csv::reader::UnGraphReader;
use rust_graph::io::csv::writer::GraphWriter;
use rust_graph::io::ldbc;
use rust_graph::io::serde::{Serialize, Serializer};
use rust_graph::prelude::*;

fn main() {
    let mut directed_graph = DiGraphMap::<Void>::new();
    let mut undirected_graph = UnGraphMap::<Void>::new();
    assert!(directed_graph.is_directed());
    assert!(!undirected_graph.is_directed());

    directed_graph.add_edge(0, 1, None);
    undirected_graph.add_edge(0, 1, None);

    //    assert_eq!(num_of_in_neighbors(&directed_graph, 0), 0);
    //    assert_eq!(num_of_in_neighbors(&undirected_graph, 0), 1);

    /// `cargo run` -> The default ID type can hold 4294967295 nodes at maximum.
    /// `cargo run --features=usize_id` -> The default ID type can hold 18446744073709551615 nodes at maximum.
    println!(
        "The default ID type can hold {} nodes at maximum.",
        directed_graph.max_possible_id()
    );

    let args: Vec<_> = std::env::args().collect();

    if args.len() > 1 {
        let ldbc_dir = Path::new(&args[1]);
        let output_dir = Path::new(&args[2]);

        println!("Loading {:?}", &ldbc_dir);
        let g = ldbc::read_from_path::<u32, Undirected, _>(ldbc_dir);
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

        //        Serializer::export(&g, export_path).unwrap();

        GraphWriter::new(&g, "./nodes.csv", "./edges.csv")
            .write()
            .unwrap();

        let gg = UnGraphReader::<String, String>::new("./nodes.csv", "./edges.csv")
            .read()
            .unwrap();
    }
}

//fn num_of_in_neighbors<Id: IdType, NL: Hash + Eq, EL: Hash + Eq>(
//    g: &impl GeneralGraph<Id, NL, EL>,
//    node: Id,
//) -> usize {
//    if let Some(dg) = g.as_digraph() {
//        dg.in_neighbors(node).len()
//    } else {
//        g.as_graph().neighbors(node).len()
//    }
//}
