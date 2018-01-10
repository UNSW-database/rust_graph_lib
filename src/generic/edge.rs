pub trait EdgeTrait {
    fn get_start(&self) -> usize;
    fn get_target(&self) -> usize;
    fn get_label(&self) -> Option<usize>;
}

pub trait MutEdgeTrait {
    fn set_label(&mut self, label: usize);
}