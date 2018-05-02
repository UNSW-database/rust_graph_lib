pub mod dtype;
pub mod edge;
pub mod node;
pub mod graph;

pub mod map;
pub mod iter;

pub use generic::dtype::Void;
pub use generic::dtype::{DefaultId, IdType};
pub use generic::dtype::{Directed, GraphType, Undirected};

pub use generic::edge::{EdgeTrait, MutEdgeTrait};
pub use generic::node::{MutNodeTrait, NodeTrait};

pub use generic::graph::{DiGraphTrait, GraphLabelTrait, GraphTrait, MutGraphTrait, UnGraphTrait};

pub use generic::map::{MapTrait, MutMapTrait};

pub use generic::iter::{IndexIter, Iter};
