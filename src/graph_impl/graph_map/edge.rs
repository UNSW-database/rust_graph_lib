use generic::{EdgeTrait, IdType};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Edge<Id: IdType, L: IdType> {
    pub(crate) src: Id,
    pub(crate) dst: Id,
    pub(crate) label: Option<L>,
}

impl<Id: IdType, L: IdType> Edge<Id, L> {
    #[inline(always)]
    pub fn new(start: Id, target: Id, label: Option<L>) -> Self {
        Edge {
            src: start,
            dst: target,
            label,
        }
    }

    #[inline(always)]
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
    #[inline(always)]
    fn get_start(&self) -> Id {
        self.src
    }

    #[inline(always)]
    fn get_target(&self) -> Id {
        self.dst
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        self.label
    }
}
