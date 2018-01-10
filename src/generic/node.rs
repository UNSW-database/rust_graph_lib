pub trait NodeTrait {
    fn get_id(&self) -> usize;
    fn get_label(&self) -> Option<usize>;
}

pub trait MutNodeTrait {
    fn set_label(&mut self, label: usize);
}