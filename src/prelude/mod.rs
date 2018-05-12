//! Commonly used items and traits.
//! # Example
//! ```
//! use rust_graph::prelude::*;
//! ```

pub use generic::GraphType;
pub use generic::Void;
pub use generic::{DefaultId, IdType};
pub use generic::{Directed, Undirected};

pub use generic::{IndexIter, Iter};

pub use generic::{DiGraphTrait, GraphLabelTrait, GraphTrait, MutGraphTrait, UnGraphTrait};
pub use generic::{EdgeTrait, MutEdgeTrait, MutNodeTrait, NodeTrait};
pub use generic::{MapTrait, MutMapTrait};
