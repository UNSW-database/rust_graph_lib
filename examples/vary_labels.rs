extern crate rand;
extern crate rust_graph;

use std::path::Path;

use rand::{thread_rng, Rng};

use rust_graph::graph_impl::UnStaticGraph;
use rust_graph::io::serde::{Deserialize, Deserializer, Serialize, Serializer};
use rust_graph::prelude::*;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let in_graph = Path::new(&args[1]);
    let out_file = Path::new(&args[2]);

    let mut rng = thread_rng();

    let mut graph: UnStaticGraph<DefaultId> = Deserializer::import(in_graph).unwrap();

    graph.remove_edge_labels();

    {
        let node_label_map = graph.get_node_label_map_mut();
        for i in 11..15 {
            node_label_map.add_item(i);
        }
    }

    {
        let labels = graph.get_labels_mut().as_mut().unwrap();
        for label in labels.iter_mut() {
            let r = rng.gen_range(0, 15);
            if r > 10 {
                *label = r;
            }
        }
    }

    Serializer::export(&graph, out_file).unwrap();
}
