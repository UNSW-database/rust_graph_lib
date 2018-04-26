extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
use rust_graph::graph_gen::{random_graph, random_graph_unlabeled};
use rust_graph::graph_gen::{random_er_graph, random_er_graph_unlabeled};

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

#[test]
fn test_random_er_graph() {
    let num_of_nodes = 100;
    let num_of_edges = 1000;

    let g1: DiGraphMap<u8> = random_er_graph_unlabeled(num_of_nodes, num_of_edges);
    assert_eq!(g1.node_count(), num_of_nodes);
    assert_eq!(g1.edge_count(), num_of_edges);

    let g2: UnGraphMap<u8> = random_er_graph_unlabeled(num_of_nodes, num_of_edges);
    assert_eq!(g2.node_count(), num_of_nodes);
    assert_eq!(g2.edge_count(), num_of_edges);
}
