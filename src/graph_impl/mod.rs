pub mod graph_map;
pub mod static_graph;

pub use graph_impl::graph_map::Edge;
pub use graph_impl::graph_map::{DiGraphMap, GraphMap, UnGraphMap};
pub use graph_impl::static_graph::EdgeVec;
pub use graph_impl::static_graph::{DiStaticGraph, StaticGraph, UnStaticGraph};

pub use graph_impl::graph_map::{TypedDiGraphMap, TypedGraphMap, TypedUnGraphMap};
pub use graph_impl::static_graph::{TypedDiStaticGraph, TypedStaticGraph, TypedUnStaticGraph};

pub enum Graph {
    GraphMap,
    StaticGraph,
    StaicGraphMmap,
}
