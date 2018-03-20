pub trait NodeTrait {
    fn get_id(&self) -> usize;
    fn get_label_id(&self) -> Option<usize>;
}

pub trait MutNodeTrait {
    fn set_label_id(&mut self, label: Option<usize>);
}
