extern crate rust_graph;

//extern crate serde_json;

use std::hash::Hash;

use rust_graph::prelude::*;
use rust_graph::*;

fn clone<L: Hash + Eq + Clone, Ty: GraphType>(g: GraphMap<L, Ty>) -> GraphMap<L, Ty> {
    g.clone()
}


fn main() {
    let mut g = DiGraphMap::<&str>::new();

    g.add_node(0, None);
    g.add_node(1, Some("1"));
    g.add_node(2, Some("1"));
    g.add_node(3, Some("2"));


    g.add_edge(0, 1, Some("2"));
    g.add_edge(1, 0, Some("2"));
    g.add_edge(0, 2, Some("1"));
    g.add_edge(0, 3, Some("1"));
    g.add_edge(1, 2, Some("1"));

//    let g = DiStaticGraph::from(g);

//    println!("degree:{:?}", g.degree(3));

    let g_clone = g.clone();

    println!("{:?}", g);

    println!("{:?}", g_clone);

    assert_eq!(g, g_clone);

//    let edges: Vec<_> = g.edge_indices().collect();
//    println!("{:?}", edges);

//    let serialized = serde_json::to_string(&g).unwrap();
//    println!("{}", serialized);
//
//    let deserialized: DiStaticGraph = serde_json::from_str(&serialized).unwrap();
//
//    println!("{:?}", deserialized);
}