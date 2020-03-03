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
    let mut max_input_num_vertex = DEF_MAX_INPUT_NUM_VERTICES;
    if graph.is_sorted_by_node() {
        max_input_num_vertex = 2;
    }
    let mut catalog = Catalog::new(DEF_NUM_EDGES_TO_SAMPLE, max_input_num_vertex);
    catalog.populate(graph, 1);
    catalog.in_subgraphs.iter_mut().for_each(|graph| {
        graph.it = None;
    });
    println!(
        "Catalog generation finished in {} (ms)",
        catalog.elapsed_time
    );
    catalog
}
