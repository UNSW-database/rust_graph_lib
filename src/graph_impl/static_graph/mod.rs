pub mod edge_vec;
pub mod graph;

pub use graph_impl::static_graph::edge_vec::EdgeVec;
pub use graph_impl::static_graph::graph::{DiStaticGraph, StaticGraph, UnStaticGraph};
pub use graph_impl::static_graph::graph::{TypedDiStaticGraph, TypedStaticGraph, TypedUnStaticGraph};
