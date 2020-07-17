/*
 * FOR REVIEWERS ONLY. DO NOT DISTRIBUTE.
 */

pub mod graph_map;
pub mod graph_vec;
pub mod static_graph;

pub use crate::graph_impl::graph_map::{
    DiGraphMap, Edge, GraphMap, MutEdge, TypedDiGraphMap, TypedGraphMap, TypedUnGraphMap,
    UnGraphMap,
};
pub use crate::graph_impl::graph_vec::{GraphVec, TypedGraphVec};
pub use crate::graph_impl::static_graph::{
    DiStaticGraph, EdgeVec, StaticGraph, TypedDiStaticGraph, TypedStaticGraph, TypedUnStaticGraph,
    UnStaticGraph,
};

#[derive(Eq, PartialEq, Copy, Clone, Debug, Serialize, Deserialize)]
pub enum GraphImpl {
    GraphMap,
    StaticGraph,
}

impl ::std::str::FromStr for GraphImpl {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, String> {
        let s = s.to_lowercase();
        match s.as_ref() {
            "graphmap" => Ok(GraphImpl::GraphMap),
            "staticgraph" => Ok(GraphImpl::StaticGraph),
            _other => Err(format!("Unsupported implementation {:?}", _other)),
        }
    }
}
