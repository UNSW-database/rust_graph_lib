extern crate rust_graph;
extern crate tempfile;

use rust_graph::graph_gen::{random_gnm_graph, random_gnm_graph_unlabeled};
use rust_graph::graph_impl::{DiGraphMap, GraphMap, UnGraphMap};
use rust_graph::io::{read_from_csv, write_to_csv};
use rust_graph::prelude::*;

use tempfile::TempDir;

#[test]
fn test_cvs_unlabeled() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    let nodes = 10;
    let edges = 20;

    let g: UnGraphMap<Void> = random_gnm_graph_unlabeled(nodes, edges);
    let path_to_nodes = tmp_dir_path.join("nodes_1.csv");
    let path_to_edges = tmp_dir_path.join("edges_1.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    let mut g_ = GraphMap::new();
    assert!(read_from_csv(&mut g_, Some(path_to_nodes), path_to_edges, None).is_ok());
    assert_eq!(g, g_);

    let g: DiGraphMap<Void> = random_gnm_graph_unlabeled(nodes, edges);
    let path_to_nodes = tmp_dir_path.join("nodes_2.csv");
    let path_to_edges = tmp_dir_path.join("edges_2.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    let mut g_ = GraphMap::new();
    assert!(read_from_csv(&mut g_, Some(path_to_nodes), path_to_edges, None).is_ok());
    assert_eq!(g, g_);
}

#[test]
fn test_cvs_labeled() {
    let tmp_dir = TempDir::new().unwrap();
    let tmp_dir_path = tmp_dir.path();

    let nodes = 10;
    let edges = 20;

    let node_labels = &vec!["a".to_owned(), "b".to_owned()];
    let edge_labels = &vec![1, 2, 3];

    let g: UnGraphMap<String, u32> =
        random_gnm_graph(nodes, edges, node_labels.clone(), edge_labels.clone());
    let path_to_nodes = tmp_dir_path.join("nodes_1.csv");
    let path_to_edges = tmp_dir_path.join("edges_1.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    let mut g_ = GraphMap::with_label_map(node_labels.into(), edge_labels.into());
    assert!(read_from_csv(&mut g_, Some(path_to_nodes), path_to_edges, None).is_ok());
    assert_eq!(g, g_);

    let g: DiGraphMap<String, u32> =
        random_gnm_graph(nodes, edges, node_labels.clone(), edge_labels.clone());
    let path_to_nodes = tmp_dir_path.join("nodes_2.csv");
    let path_to_edges = tmp_dir_path.join("edges_2.csv");
    assert!(write_to_csv(&g, &path_to_nodes, &path_to_edges).is_ok());

    let mut g_ = GraphMap::with_label_map(node_labels.into(), edge_labels.into());
    assert!(read_from_csv(&mut g_, Some(path_to_nodes), path_to_edges, None).is_ok());
    assert_eq!(g, g_);
}
