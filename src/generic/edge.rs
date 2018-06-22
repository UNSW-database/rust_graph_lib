use generic::IdType;

pub trait EdgeTrait<Id: IdType> {
    fn get_start(&self) -> Id;
    fn get_target(&self) -> Id;
    fn get_label_id(&self) -> Option<Id>;
}

pub trait MutEdgeTrait<Id: IdType> {
    fn set_label_id(&mut self, label: Option<Id>);
}
