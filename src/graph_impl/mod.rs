pub mod graph_map;
pub mod static_graph;

pub use graph_impl::graph_map::{DiGraphMap, GraphMap, UnGraphMap};
pub use graph_impl::static_graph::{DiStaticGraph, EdgeVec, StaticGraph, UnStaticGraph};
