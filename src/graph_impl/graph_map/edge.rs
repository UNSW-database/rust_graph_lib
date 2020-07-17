/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

use crate::generic::{EdgeTrait, IdType, MutEdgeTrait};

#[derive(Debug, PartialEq, Eq)]
pub struct MutEdge<'a, Id: IdType, L: IdType = Id> {
    src: Id,
    dst: Id,
    label: &'a mut Option<L>,
}

impl<'a, Id: IdType, L: IdType> MutEdge<'a, Id, L> {
    #[inline(always)]
    pub fn new(start: Id, target: Id, label: &'a mut Option<L>) -> Self {
        MutEdge {
            src: start,
            dst: target,
            label,
        }
    }
}

impl<'a, Id: IdType, L: IdType> EdgeTrait<Id, L> for MutEdge<'a, Id, L> {
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
        *self.label
    }
}

impl<'a, Id: IdType, L: IdType> MutEdgeTrait<Id, L> for MutEdge<'a, Id, L> {
    #[inline(always)]
    fn set_label_id(&mut self, label: Option<L>) {
        *self.label = label;
    }
}

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct Edge<Id: IdType, L: IdType = Id> {
    src: Id,
    dst: Id,
    label: Option<L>,
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
