//! An implementation of graph data structure that supports directed graph, undirected graph,
//! node label, edge label, self loop, but not multi-edge.
//!
//! A unique id of type `usize` must be given to each node when creating the graph.
//!
//! # Example
//! ```
//! use rust_graph::prelude::*;
//! use rust_graph::UnGraphMap;
//!
//! let mut g = UnGraphMap::<&str>::new();
//! g.add_node(0,None);
//! g.add_node(1,Some("node label"));
//! g.add_edge(0,1,Some("edge label"));
//! ```

pub mod node;
pub mod edge;
pub mod graph;
pub mod stats;

pub use graph_impl::graph_map::node::NodeMap;
pub use graph_impl::graph_map::edge::Edge;

pub use graph_impl::graph_map::graph::{DiGraphMap, GraphMap, UnGraphMap};
