use generic::IdType;
use graph_impl::Edge;

pub trait EdgeTrait<Id: IdType> {
    fn get_start(&self) -> Id;
    fn get_target(&self) -> Id;
    fn get_label_id(&self) -> Option<Id>;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeType<Id: IdType> {
    EdgeMap(Edge<Id>),
    StaticEdge(Edge<Id>),
    None,
}

impl<Id: IdType> EdgeType<Id> {
    #[inline(always)]
    pub fn is_none(&self) -> bool {
        match *self {
            EdgeType::None => true,
            _ => false,
        }
    }

    #[inline(always)]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    #[inline(always)]
    pub fn unwrap_edgemap(self) -> Edge<Id> {
        match self {
            EdgeType::EdgeMap(edge) => edge,
            EdgeType::StaticEdge(_) => {
                panic!("called `EdgeType::unwrap_edgemap()` on a `StaticEdge` value")
            }
            EdgeType::None => panic!("called `EdgeType::unwrap_edgemap()` on a `None` value"),
        }
    }

    #[inline(always)]
    pub fn unwrap_staticedge(self) -> Edge<Id> {
        match self {
            EdgeType::EdgeMap(_) => {
                panic!("called `EdgeType::unwrap_staticedge()` on a `EdgeMap` value")
            }
            EdgeType::StaticEdge(edge) => edge,
            EdgeType::None => panic!("called `EdgeType::unwrap_staticedge()` on a `None` value"),
        }
    }
}

impl<Id: IdType> EdgeTrait<Id> for EdgeType<Id> {
    #[inline(always)]
    fn get_start(&self) -> Id {
        match self {
            &EdgeType::EdgeMap(ref edge) => edge.get_start(),
            &EdgeType::StaticEdge(ref edge) => edge.get_start(),
            &EdgeType::None => panic!("called `EdgeType::get_start()` on a `None` value"),
        }
    }

    #[inline(always)]
    fn get_target(&self) -> Id {
        match self {
            &EdgeType::EdgeMap(ref edge) => edge.get_target(),
            &EdgeType::StaticEdge(ref edge) => edge.get_target(),
            &EdgeType::None => panic!("called `EdgeType::get_target()` on a `None` value"),
        }
    }

    #[inline(always)]
    fn get_label_id(&self) -> Option<Id> {
        match self {
            &EdgeType::EdgeMap(ref edge) => edge.get_label_id(),
            &EdgeType::StaticEdge(ref edge) => edge.get_label_id(),
            &EdgeType::None => None, // panic!("called `EdgeType::get_label_id()` on a `None` value"),
        }
    }
}
