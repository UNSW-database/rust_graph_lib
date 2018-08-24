extern crate itertools;
extern crate rust_graph;
extern crate time;

use itertools::Itertools;
use time::PreciseTime;

use rust_graph::io::serde::{Deserialize, Deserializer};
use rust_graph::prelude::*;
use rust_graph::{UnGraphMap, UnStaticGraph};

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();

    let start = PreciseTime::now();

    for arg in args {
        println!("------------------------------");
        println!("Loading {}", &arg);

        //        let g: UnGraphMap<String> = Deserializer::import(arg).unwrap();
        let g: UnStaticGraph<u32> = Deserializer::import(arg).unwrap();

        let max_degree = g.node_indices().map(|i| g.degree(i)).max().unwrap();

        println!("Max degree: {}", max_degree);

        let node_labels = g.get_node_label_counter();
        let edge_labels = g.get_edge_label_counter();

        println!("Node labels:");

        for (label, count) in node_labels.into_iter().sorted_by_key(|&(_, v)| v) {
            println!("- {} : {}", label, count);
        }

        println!();
        println!("Edge labels:");

        for (label, count) in edge_labels.into_iter().sorted_by_key(|&(_, v)| v) {
            println!("- {} : {}", label, count);
        }

        println!("------------------------------");
    }

    let end = PreciseTime::now();

    println!("Finished in {} seconds.", start.to(end));
}
