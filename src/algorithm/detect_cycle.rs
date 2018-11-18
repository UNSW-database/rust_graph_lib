extern crate rust_graph;

use rust_graph::graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;
use std::hash::Hash;
use std::collections::HashSet;


pub fn start_detecting<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType>(
    g: &TypedGraphMap<Id, NL, EL, Ty>
) -> bool {
    let mut visited: HashSet<Id> = HashSet::new();
    let mut has_cycle: bool = false;

    for node in g.nodes() {
        let node_id: Id = node.get_id();

        if !visited.contains(&node_id) {
            if dfs_detection(&g, &mut visited, node_id) {
                has_cycle = true;
                break;
            }
        }
    }

    if has_cycle {
        println!("Cycle detected\n");
    } else {
        println!("No cycle detected\n");
    }

    return has_cycle;
}

fn dfs_detection<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType>(
    g: &TypedGraphMap<Id, NL, EL, Ty>,
    visited: &mut HashSet<Id>,
    node: Id,
) -> bool {
    visited.insert(node);
    for neighbour in g.neighbors_iter(node) {
        if visited.contains(&neighbour) {
            return true;
        }
        dfs_detection(&g, visited, neighbour);
    }
    return false;
}