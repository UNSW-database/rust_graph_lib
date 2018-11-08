use generic::{EdgeTrait, IdType};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Edge<Id: IdType> {
    pub(crate) src: Id,
    pub(crate) dst: Id,
    pub(crate) label: Option<Id>,
}

impl<Id: IdType> Edge<Id> {
    pub fn new(start: Id, target: Id, label: Option<Id>) -> Self {
        Edge {
            src: start,
            dst: target,
            label,
        }
    }

    pub fn new_static(start: Id, target: Id, label: Id) -> Self {
        Edge {
            src: start,
            dst: target,
            label: if label == Id::max_value() {
                None
            } else {
                Some(label)
            },
        }
    }
}

impl<Id: IdType> EdgeTrait<Id> for Edge<Id> {
    fn get_start(&self) -> Id {
        self.src
    }

    fn get_target(&self) -> Id {
        self.dst
    }

    fn get_label_id(&self) -> Option<Id> {
        self.label
    }
}
