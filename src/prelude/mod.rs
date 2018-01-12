//! Commonly used items and traits.
//! # Example
//! ```
//! use rust_graph::prelude::*;
//! ```


pub use generic::{EdgeTrait, MutEdgeTrait, NodeTrait, MutNodeTrait};

pub use generic::{GraphTrait, DiGraphTrait, UnGraphTrait, MutGraphTrait, GraphLabelTrait};
pub use generic::GraphType;
pub use generic::{Directed, Undirected};

pub use generic::{MapTrait, MutMapTrait};

pub use generic::Iter;
pub use generic::IndexIter;