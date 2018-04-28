use generic::IdType;
use generic::{EdgeTrait, MutEdgeTrait};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct Edge<Id: IdType> {
    start: Id,
    target: Id,
    label: Option<Id>,
}

impl<Id: IdType> Edge<Id> {
    pub fn new(start: usize, target: usize, label: Option<usize>) -> Self {
        Edge {
            start: Id::new(start),
            target: Id::new(target),
            label: label.map(Id::new),
        }
    }
}

impl<Id: IdType> EdgeTrait for Edge<Id> {
    fn get_start(&self) -> usize {
        self.start.id()
    }

    fn get_target(&self) -> usize {
        self.target.id()
    }

    fn get_label_id(&self) -> Option<usize> {
        match self.label {
            Some(ref x) => Some(x.id()),
            None => None,
        }
    }
}

impl<Id: IdType> MutEdgeTrait for Edge<Id> {
    fn set_label_id(&mut self, label: Option<usize>) {
        self.label = label.map(Id::new);
    }
}
