use std::collections::HashMap;

use generic::NodeTrait;

pub struct Node {
    id: usize,
    label: Option<usize>,
    adj: HashMap<usize, usize>, // <adj node:edge id>
}

impl Node {
    pub fn new(id: usize, label: Option<usize>) -> Self {
        Node {
            id,
            label,
            adj: HashMap::<usize, usize>::new(),
        }
    }
}

impl NodeTrait for Node {
    fn get_id(&self) -> usize {
        self.id
    }

    fn set_label(&mut self, label: usize) {
        self.label = Some(label);
    }

    fn get_label(&self) -> Option<usize> {
        self.label
    }
}