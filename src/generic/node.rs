use generic::IdType;

pub trait NodeTrait<Id: IdType> {
    fn get_id(&self) -> Id;
    fn get_label_id(&self) -> Option<Id>;
}

pub trait MutNodeTrait<Id: IdType> {
    fn set_label_id(&mut self, label: Option<Id>);
}
