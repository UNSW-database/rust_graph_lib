extern crate rust_graph;

use rust_graph::graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;
use std::hash::Hash;
use std::collections::HashMap;


pub fn start_detecting<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType>(
    g: &TypedGraphMap<Id, NL, EL, Ty>
) -> HashMap<Id, i32> {
    let mut node_to_group: HashMap<Id, i32> = HashMap::new();
    let mut current_group: i32 = 0;

    for node in g.nodes() {
        let node_id: Id = node.get_id();

        if !node_to_group.contains_key(&node_id) {
            dfs_grouping(&g, &mut node_to_group, node_id, current_group);
            current_group += 1;
        }
    }

    println!("There's a total of {} connected components", &node_to_group.keys().len());

    for (key, value) in &node_to_group {
        println!("node: {} -> component: {}", key, value);
    }
    println!();

    return node_to_group;
}

fn dfs_grouping<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType>(
    g: &TypedGraphMap<Id, NL, EL, Ty>,
    node_to_group: &mut HashMap<Id, i32>,
    node: Id,
    group: i32
) {
    if !node_to_group.contains_key(&node) {
        node_to_group.insert(node, group);

        for neighbour in g.neighbors_iter(node) {
            dfs_grouping(&g, node_to_group, neighbour, group)
        }
    }
}