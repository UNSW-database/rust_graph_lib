use generic::IdType;
use generic::Iter;

use graph_impl::graph_map::NodeMap;
use graph_impl::static_graph::StaticNode;

pub trait NodeTrait<Id: IdType> {
    fn get_id(&self) -> Id;
    fn get_label_id(&self) -> Option<Id>;
}

pub trait MutNodeTrait<Id: IdType> {
    fn set_label_id(&mut self, label: Option<Id>);
}

pub trait NodeMapTrait<Id> {
    fn has_in_neighbor(&self, id: Id) -> bool;
    fn has_neighbor(&self, id: Id) -> bool;
    fn in_degree(&self) -> usize;
    fn degree(&self) -> usize;
    fn neighbors_iter(&self) -> Iter<Id>;
    fn in_neighbors_iter(&self) -> Iter<Id>;
    fn neighbors(&self) -> Vec<Id>;
    fn in_neighbors(&self) -> Vec<Id>;
    fn num_of_neighbors(&self) -> usize;
    fn num_of_in_neighbors(&self) -> usize;
}

pub trait MutNodeMapTrait<Id> {
    fn add_in_edge(&mut self, adj: Id) -> bool;
    fn add_edge(&mut self, adj: Id) -> bool;
    fn remove_in_edge(&mut self, adj: Id) -> bool;
    fn remove_edge(&mut self, adj: Id) -> bool;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeType<'a, Id: 'a + IdType> {
    NodeMap(&'a NodeMap<Id>),
    StaticNode(StaticNode<Id>),
    None,
}

impl<'a, Id: 'a + IdType> NodeType<'a, Id> {
    #[inline]
    pub fn is_none(&self) -> bool {
        match *self {
            NodeType::None => true,
            _ => false,
        }
    }

    #[inline]
    pub fn is_some(&self) -> bool {
        !self.is_none()
    }

    #[inline]
    pub fn unwrap_nodemap(self) -> &'a NodeMap<Id> {
        match self {
            NodeType::NodeMap(node) => node,
            NodeType::StaticNode(_) => {
                panic!("called `NodeType::unwrap_nodemap()` on a `StaticNode` value")
            }

            NodeType::None => panic!("called `NodeType::unwrap_nodemap()` on a `None` value"),
        }
    }

    #[inline]
    pub fn unwrap_staticnode(self) -> StaticNode<Id> {
        match self {
            NodeType::NodeMap(_) => {
                panic!("called `NodeType::unwrap_staticnode()` on a `NodeMap` value")
            }
            NodeType::StaticNode(node) => node,
            NodeType::None => panic!("called `NodeType::unwrap_staticnode()` on a `None` value"),
        }
    }
}

impl<'a, Id: IdType> NodeTrait<Id> for NodeType<'a, Id> {
    fn get_id(&self) -> Id {
        match self {
            &NodeType::NodeMap(node) => node.get_id(),
            &NodeType::StaticNode(ref node) => node.get_id(),
            &NodeType::None => panic!("called `NodeType::get_id()` on a `None` value"),
        }
    }

    fn get_label_id(&self) -> Option<Id> {
        match self {
            &NodeType::NodeMap(node) => node.get_label_id(),
            &NodeType::StaticNode(ref node) => node.get_label_id(),
            &NodeType::None => panic!("called `NodeType::get_label_id()` on a `None` value"),
        }
    }
}
