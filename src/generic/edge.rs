use generic::IdType;
use graph_impl::Edge;

pub trait EdgeTrait<Id: IdType, L: IdType> {
    fn get_start(&self) -> Id;
    fn get_target(&self) -> Id;
    fn get_label_id(&self) -> Option<L>;
}

pub type EdgeType<Id: IdType, L: IdType = Id> = Option<Edge<Id, L>>;

impl<Id: IdType, L: IdType> EdgeTrait<Id, L> for EdgeType<Id, L> {
    #[inline(always)]
    fn get_start(&self) -> Id {
        self.unwrap().get_start()
    }

    #[inline(always)]
    fn get_target(&self) -> Id {
        self.unwrap().get_target()
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<L> {
        self.unwrap().get_label_id()
    }
}
