extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
use rust_graph::graph_gen::{random_graph, random_graph_unlabeled};

#[test]
fn test_random_graph() {
    let num_of_nodes = 100;

    let empty: DiGraphMap<u8> = random_graph_unlabeled(num_of_nodes, 0f32);
    assert_eq!(empty.node_count(), num_of_nodes);
    assert_eq!(empty.edge_count(), 0);

    let clique: UnGraphMap<u8> = random_graph_unlabeled(num_of_nodes, 1f32);
    assert_eq!(clique.node_count(), num_of_nodes);
    assert_eq!(clique.edge_count(), (0..num_of_nodes + 1).sum());
}
