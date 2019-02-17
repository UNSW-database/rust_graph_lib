extern crate rust_graph;

use rust_graph::generic::MutNodeMapTrait;
use rust_graph::prelude::*;
use rust_graph::*;

fn main() {
    let mut g = UnGraphMap::<&str>::new();

    g.add_node(1, Some("node_1"));
    g.add_node(3, Some("node_3"));

    g.get_node_mut(1).unwrap().add_edge(4, None);
    g.get_node_mut(3).unwrap().add_edge(6, None);

    g.refine_edge_count();

    let sg = g.into_static();

    println!("{:?}", sg);
}
