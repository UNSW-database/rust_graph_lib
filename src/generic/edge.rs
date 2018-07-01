use generic::IdType;
use graph_impl::Edge;

pub trait EdgeTrait<Id: IdType> {
    fn get_start(&self) -> Id;
    fn get_target(&self) -> Id;
    fn get_label_id(&self) -> Option<Id>;
}

pub trait MutEdgeTrait<Id: IdType> {
    fn set_label_id(&mut self, label: Option<Id>);
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum EdgeType<'a, Id: 'a + IdType> {
    EdgeMap(&'a Edge<Id>),
    StaticEdge(Edge<Id>),
    None,
}

impl<'a, Id: 'a + IdType> EdgeType<'a, Id> {
    #[inline]
    pub fn is_none(&self) -> bool {
        match *self {
            EdgeType::None => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    #[inline]
    pub fn unwrap_edgemap(self) -> &'a Edge<Id> {
        match self {
            EdgeType::EdgeMap(edge) => edge,
            EdgeType::StaticEdge(_) => {
                panic!("called `EdgeType::unwrap_edgemap()` on a `StaticEdge` value")
            }
            EdgeType::None => panic!("called `EdgeType::unwrap_edgemap()` on a `None` value"),
        }
    }

    #[inline]
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
