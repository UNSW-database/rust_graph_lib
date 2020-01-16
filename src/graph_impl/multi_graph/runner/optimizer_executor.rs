use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::multi_graph::planner::catalog::catalog::Catalog;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::planner::query_planner::QueryPlanner;
use graph_impl::multi_graph::planner::query_planner_big::QueryPlannerBig;
use graph_impl::TypedStaticGraph;
use std::hash::Hash;
use std::time::SystemTime;

pub fn generate_plan<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
    query_graph: QueryGraph,
    catalog: Catalog,
    g: TypedStaticGraph<Id, NL, EL, Ty, L>,
) -> QueryPlan<Id> {
    let start_time = SystemTime::now();
    let elapsed_time;
    let query_plan = if query_graph.get_num_qvertices() <= 8 {
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
    query_plan
}
