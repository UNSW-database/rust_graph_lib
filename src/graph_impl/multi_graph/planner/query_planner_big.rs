use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::EI::{BaseEI, CachingType, EI};
use graph_impl::multi_graph::plan::operator::hashjoin::probe::{BaseProbe, Probe};
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::{
    ProbeMultiVertices, PMV,
};
use graph_impl::multi_graph::plan::operator::operator::Operator;
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::plan::operator::sink::sink::SinkType;
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::multi_graph::planner::catalog::catalog::Catalog;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::planner::query_planner::QueryPlanner;
use graph_impl::TypedStaticGraph;
use hashbrown::{HashMap, HashSet};
use std::cmp::max;
use std::hash::Hash;

pub struct QueryPlannerBig<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> {
    base_planner: QueryPlanner<Id, NL, EL, Ty, L>,
    subgraph_plans: HashMap<usize, Vec<QueryPlan<Id>>>,
    num_top_plans_kept: usize,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    QueryPlannerBig<Id, NL, EL, Ty, L>
{
    pub fn new(
        query_graph: QueryGraph,
        catalog: Catalog,
        graph: TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) -> Self {
        let mut planner = QueryPlannerBig {
            base_planner: QueryPlanner::new(query_graph, catalog, graph),
            subgraph_plans: HashMap::new(),
            num_top_plans_kept: 5,
        };
        let num_vertices = planner.base_planner.num_qvertices;
        if num_vertices >= 15 {
            planner.num_top_plans_kept = 3;
        } else if num_vertices >= 20 && num_vertices <= 25 {
            planner.num_top_plans_kept = 5;
        } else if num_vertices > 25 {
            planner.num_top_plans_kept = 1;
        }
        planner
    }

    pub fn plan(&mut self) -> QueryPlan<Id> {
        self.consider_least_selective_scans();
        while self.base_planner.next_num_qvertices <= self.base_planner.num_qvertices {
            self.consider_next_query_extensions();
            self.base_planner.next_num_qvertices += 1;
        }
        let plans = self
            .subgraph_plans
            .get(&self.base_planner.num_qvertices)
            .unwrap();
        let mut best_plan = plans.get(0).unwrap();
        for plan in plans {
            if best_plan.estimated_icost > plan.estimated_icost {
                best_plan = plan;
            }
        }
        let mut best_plan = best_plan.clone();
        // each operator added only sets its prev pointer (to reuse operator objects).
        // the picked plan needs to set the next pointer for each operator in the linear subplans.
        self.base_planner.set_next_pointers(&mut best_plan);
        if self.base_planner.query_graph.limit > 0 {
            best_plan.sink_type = SinkType::Limit;
            best_plan.out_tuples_limit = self.base_planner.query_graph.limit;
        }
        best_plan
    }

    fn consider_least_selective_scans(&mut self) {
        self.base_planner.next_num_qvertices = 2; /* level = 2 for edge scan */
        self.subgraph_plans.entry(2).or_insert(vec![]);
        let mut edges_to_scan = vec![];
        let mut num_edges_to_scan = vec![];
        let q_edges = self.base_planner.query_graph.get_query_edges();
        for i in 0..self.num_top_plans_kept {
            let edge = q_edges.get(i).unwrap().clone();
            num_edges_to_scan.push(self.base_planner.get_num_edges(&edge));
            edges_to_scan.push(edge);
        }

        for i in self.num_top_plans_kept..q_edges.len() {
            let num_edges = self.base_planner.get_num_edges(q_edges.get(i).unwrap());
            for j in 0..self.num_top_plans_kept {
                if num_edges < num_edges_to_scan[j] {
                    edges_to_scan[j] = q_edges.get(i).unwrap().clone();
                    num_edges_to_scan[j] = num_edges;
                    break;
                }
            }
        }
        for i in 0..self.num_top_plans_kept {
            let mut output_subgraph = QueryGraph::empty();
            output_subgraph.add_qedge(edges_to_scan[i].clone());
            let scan = Scan::Base(BaseScan::new(Box::new(output_subgraph)));
            let query_plan = QueryPlan::new_from_last_op(scan, num_edges_to_scan[i] as f64);
            self.subgraph_plans
                .get_mut(&self.base_planner.next_num_qvertices)
                .unwrap()
                .push(query_plan);
        }
        self.base_planner.next_num_qvertices = 3;
    }

    fn consider_next_query_extensions(&mut self) {
        let mut new_query_plans = vec![];
        let plans = self
            .subgraph_plans
            .get_mut(&(self.base_planner.next_num_qvertices - 1))
            .unwrap();
        for prev_query_plan in plans {
            let last_base_op = prev_query_plan.last_operator.as_ref().unwrap().as_ref();
            let last_op = get_base_op_as_ref!(last_base_op);
            let prev_qvertices = last_op.out_subgraph.get_query_vertices();
            let to_qvertices = self.base_planner.query_graph.get_neighbors(prev_qvertices);
            let in_subgraph = last_op.out_subgraph.as_ref();
            let next_to_qvertices = Self::filter_to_qvertices_by_max_num_alds(
                &self.base_planner.query_graph,
                to_qvertices,
                &in_subgraph,
            );
            for to_qvertex in next_to_qvertices {
                let (_key, plan) = self
                    .base_planner
                    .get_plan_with_next_extend(prev_query_plan.clone(), &to_qvertex);
                let icost = plan.estimated_icost;
                if new_query_plans.len() < self.num_top_plans_kept {
                    new_query_plans.push(plan);
                } else {
                    for i in 0..self.num_top_plans_kept {
                        if new_query_plans.get(i).unwrap().estimated_icost > icost {
                            new_query_plans.insert(i, plan.clone());
                        }
                    }
                }
            }
        }
        self.subgraph_plans
            .insert(self.base_planner.next_num_qvertices, new_query_plans);
    }
    fn filter_to_qvertices_by_max_num_alds(
        query_graph: &QueryGraph,
        to_qvertices: HashSet<String>,
        in_subgraph: &QueryGraph,
    ) -> Vec<String> {
        let mut max_num_alds = 0;
        let mut to_qvertex_to_num_alds_map = HashMap::new();
        to_qvertices.iter().for_each(|to_qvertex| {
            let num_alds = in_subgraph
                .get_query_vertices()
                .iter()
                .filter(|&from_qvertex| query_graph.contains_query_edge(from_qvertex, to_qvertex))
                .count();
            max_num_alds = max(max_num_alds, num_alds);
            to_qvertex_to_num_alds_map.insert(to_qvertex, num_alds);
        });
        let final_max_num_alds = max_num_alds;
        to_qvertices
            .iter()
            .filter(|&to_qvertex| {
                to_qvertex_to_num_alds_map
                    .get(to_qvertex)
                    .map_or(false, |&to| to == final_max_num_alds)
            })
            .map(|x| x.clone())
            .collect()
    }
}
