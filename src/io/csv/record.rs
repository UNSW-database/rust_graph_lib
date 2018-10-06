use std::hash::Hash;

use csv;

use generic::IdType;
use generic::MutGraphTrait;

#[derive(Debug, Serialize, Deserialize)]
pub struct NodeRecord<Id: IdType, NL: Hash + Eq> {
    #[serde(rename = "nodeId:ID")]
    id: Id,
    #[serde(rename = ":LABEL")]
    label: Option<NL>,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct EdgeRecord<Id: IdType, EL: Hash + Eq> {
    #[serde(rename = ":START_ID")]
    start: Id,
    #[serde(rename = ":END_ID")]
    target: Id,
    #[serde(rename = ":TYPE")]
    label: Option<EL>,
}

impl<Id: IdType, NL: Hash + Eq> NodeRecord<Id, NL> {
    pub fn new(id: Id, label: Option<NL>) -> Self {
        NodeRecord { id, label }
    }

    pub fn add_to_graph<EL: Hash + Eq, G: MutGraphTrait<Id, NL, EL>>(self, g: &mut G) {
        g.add_node(self.id, self.label);
    }
}

impl<Id: IdType, EL: Hash + Eq> EdgeRecord<Id, EL> {
    pub fn new(start: Id, target: Id, label: Option<EL>) -> Self {
        EdgeRecord {
            start,
            target,
            label,
        }
    }

    pub fn add_to_graph<NL: Hash + Eq, G: MutGraphTrait<Id, NL, EL>>(self, g: &mut G) {
        g.add_edge(self.start, self.target, self.label);
    }
}
