use generic::{GraphType, IdType};
use graph_impl::multi_graph::planner::catalog::catalog::Catalog;
use graph_impl::multi_graph::planner::catalog::query_edge::QueryEdge;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::planner::query_planner::QueryPlanner;
use graph_impl::multi_graph::planner::query_planner_big::QueryPlannerBig;
use graph_impl::multi_graph::runner::catalog_generator;
use graph_impl::multi_graph::utils::time_utils;
use graph_impl::{EdgeVec, TypedStaticGraph};
use hashbrown::HashMap;
use std::hash::Hash;
use DiStaticGraph;

pub fn generate_plan<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
    query_graph: QueryGraph,
    catalog: Catalog,
    g: TypedStaticGraph<Id, NL, EL, Ty, L>,
) {
    let num_qvertices = query_graph.get_num_qvertices();
    let start_time = time_utils::current_time();
    let elapsed_time;
    if query_graph.get_num_qvertices() <= 8 {
        let mut planner = QueryPlanner::new(query_graph, catalog, g);
        let query_plan = planner.plan();
        elapsed_time = time_utils::get_elapsed_time_in_millis(start_time);
    } else {
        let mut planner = QueryPlannerBig::new(query_graph, catalog, g);
        let query_plan = planner.plan();
        elapsed_time = time_utils::get_elapsed_time_in_millis(start_time);
    }
    println!("Optimizer runtime: {} (ms)", elapsed_time);
}
