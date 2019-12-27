use generic::{GraphLabelTrait, GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::multi_graph::planner::catalog::adj_list_descriptor::AdjListDescriptor;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::query::query_graph_set::QueryGraphSet;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::Hash;

pub static DEF_NUM_EDGES_TO_SAMPLE: usize = 1000;
pub static DEF_MAX_INPUT_NUM_VERTICES: usize = 3;
static QUERY_VERTICES: [&str; 7] = ["a", "b", "c", "d", "e", "f", "g"];

pub struct CatalogPlans<Id: IdType> {
    num_sampled_edges: usize,
    max_input_num_vertices: usize,
    num_types: usize,
    num_labels: usize,
    sorted_by_node: bool,
    query_graphs_to_extend: QueryGraphSet,
    query_plans_arrs: Vec<Vec<QueryPlan<Id>>>,
    is_directed: bool,
    selectivity_zero: Vec<(QueryGraph, Vec<AdjListDescriptor>, usize)>,
    query_vertices: Vec<String>,
    query_vertex_to_idx_map: HashMap<String, usize>,
}

impl<Id: IdType> CatalogPlans<Id> {
    pub fn new<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        num_thread: usize,
        num_sampled_edges: usize,
        max_input_num_vertices: usize,
    ) -> Self {
        let mut plans = CatalogPlans {
            num_sampled_edges,
            max_input_num_vertices,
            num_types: graph.num_of_edge_labels(),
            num_labels: graph.num_of_node_labels(),
            sorted_by_node: graph.is_sorted_by_node(),
            query_graphs_to_extend: QueryGraphSet::new(),
            query_plans_arrs: vec![],
            is_directed: graph.is_directed(),
            selectivity_zero: vec![],
            query_vertices: vec![
                "a".to_string(),
                "b".to_string(),
                "c".to_string(),
                "d".to_string(),
                "e".to_string(),
                "f".to_string(),
                "g".to_string(),
            ],
            query_vertex_to_idx_map: HashMap::new(),
        };
        for (i, v) in plans.query_vertices.iter().enumerate() {
            plans.query_vertex_to_idx_map.insert(v.clone(), i);
        }
        //        let scans = Vec::new();
        // TODO: Implement scan operator

        plans
    }

    pub fn set_next_operators<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        graph: TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
    }

    pub fn query_graphs_to_extend(&self) -> &QueryGraphSet {
        &self.query_graphs_to_extend
    }

    pub fn get_selectivity_zero(
        &mut self,
    ) -> &mut Vec<(QueryGraph, Vec<AdjListDescriptor>, usize)> {
        &mut self.selectivity_zero
    }

    pub fn get_query_plan_arrs(&mut self) -> &mut Vec<Vec<QueryPlan<Id>>> {
        self.query_plans_arrs.as_mut()
    }
}

pub struct Descriptor {
    out_subgraph: QueryGraph,
    alds: Vec<AdjListDescriptor>,
}
