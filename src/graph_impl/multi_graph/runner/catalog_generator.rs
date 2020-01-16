use generic::{GraphType, IdType};
use graph_impl::multi_graph::planner::catalog::catalog::Catalog;
use graph_impl::multi_graph::planner::catalog::catalog_plans::{
    DEF_MAX_INPUT_NUM_VERTICES, DEF_NUM_EDGES_TO_SAMPLE,
};
use graph_impl::TypedStaticGraph;
use std::hash::Hash;

pub fn default<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
    graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
) -> Catalog {
    get(
        graph,
        DEF_MAX_INPUT_NUM_VERTICES,
        DEF_NUM_EDGES_TO_SAMPLE,
        1,
    )
}

pub fn get<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
    graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    max_input_num_vertices: usize,
    num_sampled_edges: usize,
    num_threads: usize,
) -> Catalog {
    let mut catalog = Catalog::new(num_sampled_edges, max_input_num_vertices);
    catalog.populate(graph, num_threads);
    catalog
}
