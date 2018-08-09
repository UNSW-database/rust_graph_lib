extern crate rust_graph;
extern crate time;

use time::PreciseTime;

use rust_graph::graph_impl::UnGraphMap;
use rust_graph::io::serde::{Deserialize, Deserializer};
use rust_graph::prelude::*;

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();

    let start = PreciseTime::now();

    for arg in args {
        println!("------------------------------");
        println!("Loading {}", &arg);

        let g: UnGraphMap<String> = Deserializer::import(arg).unwrap();

        let node_labels = g.get_node_label_map().clone();
        let edge_labels = g.get_edge_label_map().clone();

        let node_label_ids = g.get_node_label_id_counter();
        let edge_label_ids = g.get_edge_label_id_counter();

        println!("Node labels:");

        for (id, count) in node_label_ids {
            println!("- {} : {}", node_labels.get_item(id.id()).unwrap(), count);
        }

        println!();
        println!("Edge labels:");

        for (id, count) in edge_label_ids {
            println!("- {} : {}", edge_labels.get_item(id.id()).unwrap(), count);
        }

        println!("------------------------------");
    }

    let end = PreciseTime::now();

    println!("Finished in {} seconds.", start.to(end));
}
