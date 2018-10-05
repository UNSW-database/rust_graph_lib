//! This file defines a mmap version of `StaticGraph`, so that when the graph is huge,
//! we can rely on mmap to save physical memory usage.
//!
//!

pub mod edge_vec_mmap;
pub mod graph_mmap;

pub use graph_impl::static_graph::mmap::edge_vec_mmap::EdgeVecMmap;
pub use graph_impl::static_graph::mmap::graph_mmap::StaticGraphMmap;
