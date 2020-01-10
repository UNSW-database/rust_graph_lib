use generic::{GraphType, IdType};
use graph_impl::multi_graph::planner::catalog::catalog::Catalog;
use graph_impl::multi_graph::planner::catalog::query_edge::QueryEdge;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::planner::query_planner::QueryPlanner;
use graph_impl::multi_graph::planner::query_planner_big::QueryPlannerBig;
use graph_impl::multi_graph::runner::catalog_generator;
use graph_impl::{EdgeVec, TypedStaticGraph};
use hashbrown::HashMap;
use std::hash::Hash;
use std::time::SystemTime;
use DiStaticGraph;

pub fn generate_plan<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
    query_graph: QueryGraph,
    catalog: Catalog,
    g: TypedStaticGraph<Id, NL, EL, Ty, L>,
) {
    let num_qvertices = query_graph.get_num_qvertices();
    let start_time = SystemTime::now();
    let elapsed_time;
    let mut query_plan = if query_graph.get_num_qvertices() <= 8 {
        let mut planner = QueryPlanner::new(query_graph, catalog, g);
        let query_plan = planner.plan();
        elapsed_time = SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_millis();
        query_plan
    } else {
        let mut planner = QueryPlannerBig::new(query_graph, catalog, g);
        let query_plan = planner.plan();
        elapsed_time = SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_millis();
        query_plan
    };
    println!("Optimizer runtime: {} (ms)", elapsed_time);
    println!("QueryPlan output:{}", query_plan.get_output_log());
}
