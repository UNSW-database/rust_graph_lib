use generic::{EdgeTrait, IdType};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Edge<Id: IdType, L: IdType> {
    pub(crate) src: Id,
    pub(crate) dst: Id,
    pub(crate) label: Option<L>,
}

impl<Id: IdType, L: IdType> Edge<Id, L> {
    pub fn new(start: Id, target: Id, label: Option<L>) -> Self {
        Edge {
            src: start,
            dst: target,
            label,
        }
    }

    pub fn new_static(start: Id, target: Id, label: L) -> Self {
        Edge {
            src: start,
            dst: target,
            label: if label == L::max_value() {
                None
            } else {
                Some(label)
            },
        }
    }
}

impl<Id: IdType, L: IdType> EdgeTrait<Id, L> for Edge<Id, L> {
    fn get_start(&self) -> Id {
        self.src
    }

    fn get_target(&self) -> Id {
        self.dst
    }

    fn get_label_id(&self) -> Option<L> {
        self.label
    }
}
