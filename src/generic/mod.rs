pub mod dtype;
pub mod edge;
pub mod graph;
pub mod iter;
pub mod map;
pub mod node;

pub use generic::dtype::{DefaultId, DefaultTy, Directed, GraphType, IdType, Undirected, Void};

pub use generic::edge::{EdgeTrait, EdgeType};
pub use generic::node::{MutNodeMapTrait, MutNodeTrait, NodeMapTrait, NodeTrait, NodeType};

pub use generic::graph::{
    DiGraphTrait, GeneralGraph, GraphLabelTrait, GraphTrait, MutGraphLabelTrait, MutGraphTrait,
    UnGraphTrait,
};

pub use generic::map::{MapTrait, MutMapTrait};

pub use generic::iter::Iter;
