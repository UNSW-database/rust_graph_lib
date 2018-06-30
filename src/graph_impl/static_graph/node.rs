use generic::IdType;
use generic::NodeTrait;

pub struct StaticNode<Id: IdType> {
    id: Id,
    label: Option<Id>,
}

impl<Id: IdType> StaticNode<Id> {
    pub fn new(id: Id, label: Id) -> Self {
        StaticNode {
            id,
            label: if label == Id::max_value() {
                None
            } else {
                Some(label)
            },
        }
    }
}

impl<Id: IdType> NodeTrait<Id> for StaticNode<Id> {
    fn get_id(&self) -> Id {
        self.id
    }

    fn get_label_id(&self) -> Option<Id> {
        self.label
    }
}
