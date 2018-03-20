pub trait EdgeTrait {
    fn get_start(&self) -> usize;
    fn get_target(&self) -> usize;
    fn get_label_id(&self) -> Option<usize>;
}

pub trait MutEdgeTrait {
    fn set_label_id(&mut self, label: Option<usize>);
}
