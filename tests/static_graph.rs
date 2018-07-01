#[macro_use]
extern crate rust_graph;

use rust_graph::prelude::*;

use rust_graph::generic::DefaultId;
use rust_graph::graph_impl::Edge;
use rust_graph::graph_impl::static_graph::EdgeVec;
use rust_graph::graph_impl::static_graph::StaticNode;
use rust_graph::map::SetMap;
use rust_graph::{DiStaticGraph, UnStaticGraph};

#[test]
fn test_directed() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::<Void>::new(3, edge_vec, Some(in_edge_vec), SetMap::new());

    assert_eq!(g.neighbors(0)[..], [1, 2]);
    assert_eq!(&g.neighbors(1)[..], &[0]);
    assert_eq!(&g.neighbors(2)[..], &[0]);

    assert_eq!(g.in_neighbors(0).into_owned(), vec![1, 2]);
    assert_eq!(g.in_neighbors(1).into_owned(), vec![0]);
    assert_eq!(g.in_neighbors(2).into_owned(), vec![0]);

    let node_0 = StaticNode::new(0 as DefaultId, None);
    let node_1 = StaticNode::new(1 as DefaultId, None);
    let node_2 = StaticNode::new(2 as DefaultId, None);

    let edge_0_1 = Edge::new(0 as DefaultId, 1, None);
    let edge_0_2 = Edge::new(0 as DefaultId, 2, None);
    let edge_1_0 = Edge::new(1 as DefaultId, 0, None);
    let edge_2_0 = Edge::new(2 as DefaultId, 0, None);

    assert_eq!(g.get_node(0).unwrap_staticnode(), node_0);
    assert_eq!(g.get_node(1).unwrap_staticnode(), node_1);
    assert_eq!(g.get_node(2).unwrap_staticnode(), node_2);
    assert!(g.get_node(3).is_none());

    assert_eq!(g.get_edge(0, 1).unwrap_staticedge(), edge_0_1);
    assert_eq!(g.get_edge(0, 2).unwrap_staticedge(), edge_0_2);
    assert_eq!(g.get_edge(1, 0).unwrap_staticedge(), edge_1_0);
    assert_eq!(g.get_edge(2, 0).unwrap_staticedge(), edge_2_0);
    assert!(g.get_edge(2, 3).is_none());

    let nodes: Vec<_> = g.nodes().collect();
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains(&g.get_node(0)));
    assert!(nodes.contains(&g.get_node(1)));
    assert!(nodes.contains(&g.get_node(2)));

    let edges: Vec<_> = g.edges().collect();
    assert_eq!(edges.len(), 4);
    assert!(edges.contains(&g.get_edge(0, 1)));
    assert!(edges.contains(&g.get_edge(0, 2)));
    assert!(edges.contains(&g.get_edge(1, 0)));
    assert!(edges.contains(&g.get_edge(2, 0)));
}

#[test]
fn test_labeled() {
    let edge_vec = EdgeVec::with_labels(vec![0, 2, 3, 4], vec![1, 2, 0, 0], vec![0, 1, 0, 1]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let labels = vec![1, 0, 1];
    let g = DiStaticGraph::<&str>::with_labels(
        3,
        edge_vec,
        Some(in_edge_vec),
        labels,
        setmap!["a", "b"],
        setmap!["a", "b"],
    );

    assert_eq!(g.get_node_label(0), Some(&"b"));
    assert_eq!(g.get_node_label(1), Some(&"a"));
    assert_eq!(g.get_node_label(2), Some(&"b"));
    assert_eq!(g.get_node_label(4), None);

    assert_eq!(g.get_edge_label(0, 1), Some(&"a"));
    assert_eq!(g.get_edge_label(0, 2), Some(&"b"));
    assert_eq!(g.get_edge_label(1, 0), Some(&"a"));
    assert_eq!(g.get_edge_label(2, 0), Some(&"b"));
    assert_eq!(g.get_edge_label(2, 3), None);

    let node_0 = StaticNode::new(0 as DefaultId, Some(1));
    let node_1 = StaticNode::new(1 as DefaultId, Some(0));
    let node_2 = StaticNode::new(2 as DefaultId, Some(1));

    let edge_0_1 = Edge::new(0 as DefaultId, 1, Some(0));
    let edge_0_2 = Edge::new(0 as DefaultId, 2, Some(1));
    let edge_1_0 = Edge::new(1 as DefaultId, 0, Some(0));
    let edge_2_0 = Edge::new(2 as DefaultId, 0, Some(1));

    assert_eq!(g.get_node(0).unwrap_staticnode(), node_0);
    assert_eq!(g.get_node(1).unwrap_staticnode(), node_1);
    assert_eq!(g.get_node(2).unwrap_staticnode(), node_2);
    assert!(g.get_node(3).is_none());

    assert_eq!(g.get_edge(0, 1).unwrap_staticedge(), edge_0_1);
    assert_eq!(g.get_edge(0, 2).unwrap_staticedge(), edge_0_2);
    assert_eq!(g.get_edge(1, 0).unwrap_staticedge(), edge_1_0);
    assert_eq!(g.get_edge(2, 0).unwrap_staticedge(), edge_2_0);
    assert!(g.get_edge(2, 3).is_none());

    let nodes: Vec<_> = g.nodes().collect();
    assert_eq!(nodes.len(), 3);
    assert!(nodes.contains(&g.get_node(0)));
    assert!(nodes.contains(&g.get_node(1)));
    assert!(nodes.contains(&g.get_node(2)));

    let edges: Vec<_> = g.edges().collect();
    assert_eq!(edges.len(), 4);
    assert!(edges.contains(&g.get_edge(0, 1)));
    assert!(edges.contains(&g.get_edge(0, 2)));
    assert!(edges.contains(&g.get_edge(1, 0)));
    assert!(edges.contains(&g.get_edge(2, 0)));
}

#[test]
fn test_clone() {
    let edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let in_edge_vec = EdgeVec::new(vec![0, 2, 3, 4], vec![1, 2, 0, 0]);
    let g = DiStaticGraph::<Void>::new(3, edge_vec, Some(in_edge_vec), SetMap::new());
    assert_eq!(g, g.clone());
}
