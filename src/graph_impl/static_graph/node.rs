/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

use crate::generic::{IdType, NodeTrait};

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StaticNode<Id: IdType, L: IdType = Id> {
    id: Id,
    label: Option<L>,
}

impl<Id: IdType, L: IdType> StaticNode<Id, L> {
    #[inline(always)]
    pub fn new(id: Id, label: Option<L>) -> Self {
        StaticNode { id, label }
    }

    #[inline(always)]
    pub fn new_static(id: Id, label: L) -> Self {
        StaticNode {
            id,
            label: if label == L::max_value() {
                None
            } else {
                Some(label)
            },
        }
    }
}

impl<Id: IdType, L: IdType> NodeTrait<Id, L> for StaticNode<Id, L> {
    #[inline(always)]
    fn get_id(&self) -> Id {
        self.id
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        self.label
    }
}
