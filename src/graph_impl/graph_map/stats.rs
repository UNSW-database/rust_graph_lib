use std::hash::Hash;

use std::collections::HashMap;

use generic::GraphType;
use generic::IdType;
use generic::MapTrait;
use generic::{EdgeTrait, NodeTrait};
use generic::{GraphLabelTrait, GraphTrait};

use graph_impl::TypedGraphMap;

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> TypedGraphMap<Id, NL, EL, Ty> {
    pub fn get_node_label_id_counter(&self) -> HashMap<Id, usize> {
        let mut counter = HashMap::with_capacity(self.get_node_label_map().len());

        for node in self.nodes() {
            if let Some(label) = node.unwrap_nodemap().get_label_id() {
                let count = counter.entry(label).or_insert(0);
                *count += 1;
            }
        }
        counter
    }

    pub fn get_edge_label_id_counter(&self) -> HashMap<Id, usize> {
        let mut counter = HashMap::with_capacity(self.get_edge_label_map().len());

        for edge in self.edges() {
            if let Some(label) = edge.get_label_id() {
                let count = counter.entry(label).or_insert(0);
                *count += 1;
            }
        }
        counter
    }
}
