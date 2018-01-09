use std::hash::Hash;

use std::collections::HashMap;

use generic::GraphTrait;
use generic::{NodeTrait, EdgeTrait};
use generic::MapTrait;
use generic::GraphType;

use graph_impl::GraphMap;


impl<L: Hash + Eq, Ty: GraphType> GraphMap<L, Ty> {
    pub fn get_node_label_id_counter<'a>(&self) -> HashMap<usize, usize> {
        let mut counter: HashMap<usize, usize> = HashMap::with_capacity(self.get_node_label_map().len());

        for node in self.nodes() {
            if let Some(label) = node.get_label() {
                let count = counter.entry(label).or_insert(0);
                *count += 1;
            }
        }
        counter
    }

    pub fn get_edge_label_id_counter(&self) -> HashMap<usize, usize> {
        let mut counter: HashMap<usize, usize> = HashMap::with_capacity(self.get_edge_label_map().len());

        for edge in self.edges() {
            if let Some(label) = edge.get_label() {
                let count = counter.entry(label).or_insert(0);
                *count += 1;
            }
        }
        counter
    }
}