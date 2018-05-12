extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::graph_gen::{complete_graph_unlabeled, empty_graph_unlabeled};
use rust_graph::graph_gen::{random_gnm_graph_unlabeled, random_gnp_graph_unlabeled};
use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};

#[test]
fn test_random_gnp_graph() {
    let num_of_nodes = 100;

    let empty: DiGraphMap<u8> = random_gnp_graph_unlabeled(num_of_nodes, 0f32);
    assert_eq!(empty, empty_graph_unlabeled(num_of_nodes));

    let clique: UnGraphMap<u8> = random_gnp_graph_unlabeled(num_of_nodes, 1f32);
    assert_eq!(clique, complete_graph_unlabeled(num_of_nodes));
}

#[test]
fn test_random_gnm_graph() {
    let num_of_nodes = 100;
    let num_of_edges = 1000;

    let g1: DiGraphMap<u8> = random_gnm_graph_unlabeled(num_of_nodes, num_of_edges);
    assert_eq!(g1.node_count(), num_of_nodes);
    assert_eq!(g1.edge_count(), num_of_edges);

    let g2: UnGraphMap<u8> = random_gnm_graph_unlabeled(num_of_nodes, num_of_edges);
    assert_eq!(g2.node_count(), num_of_nodes);
    assert_eq!(g2.edge_count(), num_of_edges);
}
