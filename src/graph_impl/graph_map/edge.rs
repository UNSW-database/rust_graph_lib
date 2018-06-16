use generic::IdType;
use generic::{EdgeTrait, MutEdgeTrait};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Edge<Id: IdType> {
    start: Id,
    target: Id,
    label: Option<Id>,
}

impl<Id: IdType> Edge<Id> {
    pub fn new(start: Id, target: Id, label: Option<Id>) -> Self {
        Edge {
            start,
            target,
            label,
        }
    }
}

impl<Id: IdType> EdgeTrait<Id> for Edge<Id> {
    fn get_start(&self) -> Id {
        self.start
    }

    fn get_target(&self) -> Id {
        self.target
    }

    fn get_label_id(&self) -> Option<Id> {
        self.label
    }
}

impl<Id: IdType> MutEdgeTrait<Id> for Edge<Id> {
    fn set_label_id(&mut self, label: Option<Id>) {
        self.label = label
    }
}
