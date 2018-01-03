pub trait EdgeTrait {
//    fn get_id(&self) -> usize;

    fn get_start(&self) -> usize;
    fn get_target(&self) -> usize;

    fn set_label(&mut self, label: usize);
    fn get_label(&self) -> Option<usize>;
}