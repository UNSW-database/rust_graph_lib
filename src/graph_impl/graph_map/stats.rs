use std::hash::Hash;

use std::collections::HashMap;

use generic::IdType;
use generic::GraphTrait;
use generic::{EdgeTrait, NodeTrait};
use generic::MapTrait;
use generic::GraphType;

use graph_impl::TypedGraphMap;

impl<Id: IdType, L: Hash + Eq, Ty: GraphType> TypedGraphMap<Id, L, Ty> {
    pub fn get_node_label_id_counter(&self) -> HashMap<Id, usize> {
        let mut counter = HashMap::with_capacity(self.get_node_label_map().len());

        for node in self.nodes() {
            if let Some(label) = node.get_label_id() {
                let count = counter.entry(Id::new(label)).or_insert(0);
                *count += 1;
            }
        }
        counter
    }

    pub fn get_edge_label_id_counter(&self) -> HashMap<Id, usize> {
        let mut counter = HashMap::with_capacity(self.get_edge_label_map().len());

        for edge in self.edges() {
            if let Some(label) = edge.get_label_id() {
                let count = counter.entry(Id::new(label)).or_insert(0);
                *count += 1;
            }
        }
        counter
    }
}
