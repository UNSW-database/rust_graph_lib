extern crate rust_graph;

use rust_graph::graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;
use std::hash::Hash;
use std::collections::HashMap;
use std::collections::VecDeque;


pub fn start_detecting<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType>(
    g: &TypedGraphMap<Id, NL, EL, Ty>,
    start: Id,
    destination: Id
) -> i32 {
    let mut prev: HashMap<Id, Id> = HashMap::new();
    let mut queue: VecDeque<Id> = VecDeque::new();
    let mut is_found: bool = false;

    queue.push_back(start);

    while queue.len() != 0 && !is_found {
        let current_node = queue.pop_front().unwrap();

        for neighbour in g.neighbors_iter(current_node) {
            if !prev.contains_key(&neighbour) {
                prev.insert(neighbour, current_node);
                queue.push_back(neighbour);

                if neighbour == destination {
                    is_found = true;
                    break;
                }
            }
        }

    }

    if is_found {
        println!("Path found");

        for (key, value) in &prev {
            println!("node: {} -> prev: {}", key, value);
        }
        return 1;
    } else {
        println!("No path found");
        return 0
    }

}

