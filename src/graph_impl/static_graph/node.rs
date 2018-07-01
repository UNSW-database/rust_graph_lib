use generic::IdType;
use generic::NodeTrait;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticNode<Id: IdType> {
    id: Id,
    label: Option<Id>,
}

impl<Id: IdType> StaticNode<Id> {
    pub fn new(id: Id, label: Option<Id>) -> Self {
        StaticNode { id, label }
    }

    pub fn new_static(id: Id, label: Id) -> Self {
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
