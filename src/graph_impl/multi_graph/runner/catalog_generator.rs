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
    let mut catalog = Catalog::new(DEF_NUM_EDGES_TO_SAMPLE, DEF_MAX_INPUT_NUM_VERTICES);
    catalog.populate(graph, 1);
    println!(
        "Catalog generation finished in {} (ms)",
        catalog.elapsed_time
    );
    catalog
}
