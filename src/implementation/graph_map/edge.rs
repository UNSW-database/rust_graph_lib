use generic::EdgeTrait;

pub struct Edge {
    id: usize,
    start: usize,
    target: usize,
    label: Option<usize>,
}


impl Edge {
    pub fn new(id: usize, start: usize, target: usize, label: Option<usize>) -> Self {
        Edge {
            id,
            start,
            target,
            label,
        }
    }
}

impl EdgeTrait for Edge {
    fn get_id(&self) -> usize {
        self.id
    }

    fn get_start(&self) -> usize {
        self.start
    }

    fn get_target(&self) -> usize {
        self.target
    }

    fn set_label(&mut self, label: usize) {
        self.label = Some(label);
    }

    fn get_label(&self) -> Option<usize> {
        self.label
    }
}
