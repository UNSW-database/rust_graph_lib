pub mod graph_map;
pub mod static_graph;

pub use graph_impl::graph_map::{
    DiGraphMap, Edge, GraphMap, TypedDiGraphMap, TypedGraphMap, TypedUnGraphMap, UnGraphMap,
};
pub use graph_impl::static_graph::mmap::{EdgeVecMmap, StaticGraphMmap};
pub use graph_impl::static_graph::{
    DiStaticGraph, EdgeVec, StaticGraph, TypedDiStaticGraph, TypedStaticGraph, TypedUnStaticGraph,
    UnStaticGraph,
};

pub enum Graph {
    GraphMap,
    StaticGraph,
    StaicGraphMmap,
}
