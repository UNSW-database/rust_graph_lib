use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::BaseIntersect;
use graph_impl::multi_graph::plan::operator::extend::intersect::Intersect;
use graph_impl::multi_graph::plan::operator::extend::EI::{BaseEI, CachingType, EI};
use graph_impl::multi_graph::plan::operator::hashjoin::hash_join::HashJoin;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::{BaseProbe, Probe};
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::{
    ProbeMultiVertices, PMV,
};
use graph_impl::multi_graph::plan::operator::operator::Operator;
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::plan::operator::sink::sink::SinkType;
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::multi_graph::planner::catalog::adj_list_descriptor::{
    AdjListDescriptor, Direction,
};
use graph_impl::multi_graph::planner::catalog::catalog::{
    Catalog, MULTI_VERTEX_WEIGHT_BUILD_COEF, MULTI_VERTEX_WEIGHT_PROBE_COEF,
    SINGLE_VERTEX_WEIGHT_BUILD_COEF, SINGLE_VERTEX_WEIGHT_PROBE_COEF,
};
use graph_impl::multi_graph::planner::catalog::query_edge::QueryEdge;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::{HashMap, HashSet};
use itertools::min;
use std::cell::RefCell;
use std::hash::Hash;
use std::ptr::null;

pub struct QueryPlanner<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType> {
    subgraph_plans: HashMap<usize, HashMap<String, Vec<QueryPlan<Id>>>>,
    pub query_graph: QueryGraph,
    pub num_qvertices: usize,
    pub next_num_qvertices: usize,
    graph: TypedStaticGraph<Id, NL, EL, Ty, L>,
    catalog: Catalog,
    has_limit: bool,
    computed_selectivities: HashMap<String, Vec<(QueryGraph, f64)>>,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>
    QueryPlanner<Id, NL, EL, Ty, L>
{
    pub fn new(
        query_graph: QueryGraph,
        catalog: Catalog,
        graph: TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) -> Self {
        QueryPlanner {
            subgraph_plans: HashMap::new(),
            has_limit: query_graph.limit > 0,
            num_qvertices: query_graph.get_num_qvertices(),
            query_graph,
            next_num_qvertices: 0,
            graph,
            catalog,
            computed_selectivities: HashMap::new(),
        }
    }

    pub fn plan(&mut self) -> QueryPlan<Id> {
        if self.num_qvertices == 2 {
            return QueryPlan::new_from_operator(Box::new(Operator::Scan(Scan::Base(
                BaseScan::new(Box::new(self.query_graph.clone())),
            ))));
        }
        self.consider_all_scan_operators();
        while self.next_num_qvertices <= self.num_qvertices {
            self.consider_all_next_query_extensions();
            self.next_num_qvertices += 1;
        }
        let key = self.subgraph_plans[&self.num_qvertices]
            .keys()
            .next()
            .unwrap();
        let mut best_plan = self.get_best_plan(self.num_qvertices, key);
        // each operator added only sets its prev pointer (to reuse operator objects).
        // the picked plan needs to set the next pointer for each operator in the linear subplans.
        self.set_next_pointers(&mut best_plan);
        if self.has_limit {
            best_plan.sink_type = SinkType::Limit;
            best_plan.out_tuples_limit = self.query_graph.limit;
        }
        best_plan
    }

    pub fn set_next_pointers(&self, best_plan: &mut QueryPlan<Id>) {
        best_plan
            .subplans
            .iter_mut()
            .map(|op| op.as_mut())
            .for_each(|last_op| {
                let mut cur_op = last_op;
                while get_op_attr_as_ref!(cur_op, prev).is_some() {
                    let cur_copy = cur_op.clone();
                    let prev = get_op_attr_as_mut!(cur_op, prev).as_mut().unwrap().as_mut();
                    *get_op_attr_as_mut!(prev, next) = vec![cur_copy];
                    cur_op = get_op_attr_as_mut!(cur_op, prev).as_mut().unwrap().as_mut();
                }
            });
    }

    fn consider_all_scan_operators(&mut self) {
        self.next_num_qvertices = 2;
        self.subgraph_plans
            .entry(self.next_num_qvertices)
            .or_insert(HashMap::new());
        for query_edge in &self.query_graph.q_edges {
            let mut out_subgraph = QueryGraph::empty();
            out_subgraph.add_qedge(query_edge.clone());
            let scan = Scan::Base(BaseScan::new(Box::new(out_subgraph)));
            let num_edges = self.get_num_edges(&query_edge);
            let query_plan = QueryPlan::new_from_last_op(scan, num_edges as f64);
            let mut query_plans = vec![];
            query_plans.push(query_plan);
            let key = self.get_key(&mut vec![
                query_edge.from_query_vertex.clone(),
                query_edge.to_query_vertex.clone(),
            ]);
            let plan = self
                .subgraph_plans
                .get_mut(&self.next_num_qvertices)
                .unwrap();
            plan.insert(key, query_plans);
        }
        self.next_num_qvertices = 3;
    }

    fn consider_all_next_query_extensions(&mut self) {
        self.subgraph_plans
            .entry(self.next_num_qvertices)
            .or_insert(HashMap::new());
        let plan_map = &self.subgraph_plans[&(self.next_num_qvertices - 1)];
        let plan_map_keys: Vec<String> = plan_map.keys().map(|v| v.clone()).collect();
        for key in plan_map_keys {
            self.consider_all_next_extend_operators(&key);
        }
        if !self.has_limit && self.next_num_qvertices >= 4 {
            let plan_map_keys: Vec<String> = self.subgraph_plans[&self.next_num_qvertices]
                .keys()
                .map(|v| v.clone())
                .collect();
            for plan_map_key in plan_map_keys {
                self.consider_all_next_hash_join_operators(&plan_map_key);
            }
        }
    }

    fn consider_all_next_extend_operators(&mut self, key: &String) {
        let prev_plan_map = &self.subgraph_plans[&(self.next_num_qvertices - 1)];
        let prev_query_plans = &prev_plan_map[key];
        let op = prev_query_plans[0].last_operator.as_ref().unwrap().as_ref();
        let prev_qvertices = get_op_attr_as_ref!(op, out_subgraph).get_query_vertices();
        let to_qvertices = self.query_graph.get_neighbors(prev_qvertices);
        let prev_query_plans_len = prev_plan_map[key].len();
        let mut plans = vec![];
        for to_qvertex in to_qvertices {
            for i in 0..prev_query_plans_len {
                plans.push(self.get_plan_with_next_extend_by_index(i, key, &to_qvertex));
            }
        }
        let plan_map = self
            .subgraph_plans
            .get_mut(&self.next_num_qvertices)
            .unwrap();
        for (key, plan) in plans {
            plan_map.entry(key.clone()).or_insert(vec![]);
            plan_map.get_mut(&key).unwrap().push(plan);
        }
    }

    pub fn get_plan_with_next_extend_by_index(
        &mut self,
        prev_query_plan_index: usize,
        key: &String,
        to_qvertex: &String,
    ) -> (String, QueryPlan<Id>) {
        let prev_plan_map = &self.subgraph_plans[&(self.next_num_qvertices - 1)];
        let prev_query_plans = &prev_plan_map[key];
        let prev_query_plan = &prev_query_plans[prev_query_plan_index];
        self.get_plan_with_next_extend(prev_query_plan.clone(), to_qvertex)
    }

    pub fn get_plan_with_next_extend(
        &mut self,
        mut prev_query_plan: QueryPlan<Id>,
        to_qvertex: &String,
    ) -> (String, QueryPlan<Id>) {
        let last_operator = prev_query_plan.last_operator.as_ref().unwrap().as_ref();
        let base_last_op = get_base_op_as_ref!(last_operator);
        let in_subgraph = base_last_op.out_subgraph.as_ref();
        let last_previous_repeated_index = base_last_op.last_repeated_vertex_idx;
        let mut alds = vec![];
        let mut next_extend = self.get_next_ei(in_subgraph, to_qvertex, &mut alds, last_operator);
        let next_copy = next_extend.clone();
        let base_next_extend = get_ei_as_mut!(&mut next_extend);
        base_next_extend.init_caching(last_previous_repeated_index);
        let prev_estimated_num_out_tuples = prev_query_plan.estimated_num_out_tuples;
        let to_type = base_next_extend
            .base_op
            .out_subgraph
            .get_query_vertex_type(to_qvertex);
        let last_operator = prev_query_plan.last_operator.as_mut().unwrap().as_mut();
        let mut base_last_op = get_base_op_as_mut!(last_operator);
        let in_subgraph = base_last_op.out_subgraph.as_mut();
        let estimated_selectivity = self.get_selectivity(
            in_subgraph,
            base_next_extend.base_op.out_subgraph.as_mut(),
            &alds,
            to_type,
        );
        let icost;
        if let CachingType::None = base_next_extend.caching_type {
            icost = prev_estimated_num_out_tuples
                * self.catalog.get_icost(
                    in_subgraph,
                    alds.iter().collect(),
                    base_next_extend.to_type,
                );
        } else {
            let mut out_tuples_to_process = prev_estimated_num_out_tuples;
            if base_last_op.prev.is_some() {
                let index = 0;
                let mut last_estimated_num_out_tuples_for_extension_qvertex = -1.0;
                for ald in alds.iter().filter(|ald| ald.vertex_idx > index) {
                    last_estimated_num_out_tuples_for_extension_qvertex =
                        prev_query_plan.q_vertex_to_num_out_tuples[&ald.from_query_vertex].clone();
                }
                out_tuples_to_process /= last_estimated_num_out_tuples_for_extension_qvertex;
            }
            if let CachingType::FullCaching = base_next_extend.caching_type {
                icost = out_tuples_to_process * self.catalog.get_icost(in_subgraph, alds.iter().collect(), to_type) +
                    // added to make caching effect on cost more robust.
                    (prev_estimated_num_out_tuples - out_tuples_to_process) * estimated_selectivity;
            } else {
                // cachingType == CachingType.PARTIAL_CACHING
                let alds_to_cache = alds
                    .iter()
                    .filter(|ald| ald.vertex_idx <= last_previous_repeated_index)
                    .collect();
                let alds_to_always_intersect = alds
                    .iter()
                    .filter(|ald| ald.vertex_idx > last_previous_repeated_index)
                    .collect();
                let always_intersect_icost = prev_estimated_num_out_tuples
                    * self
                        .catalog
                        .get_icost(in_subgraph, alds_to_always_intersect, to_type);
                let cached_intersect_icost = out_tuples_to_process
                    * self.catalog.get_icost(in_subgraph, alds_to_cache, to_type);
                icost = prev_estimated_num_out_tuples * always_intersect_icost +
                    out_tuples_to_process * cached_intersect_icost +
                    // added to make caching effect on cost more robust.
                    (prev_estimated_num_out_tuples - out_tuples_to_process) * estimated_selectivity;
            }
        }

        let estimated_icost = prev_query_plan.estimated_icost + icost;
        let estimated_num_out_tuples = prev_estimated_num_out_tuples * estimated_selectivity;

        let mut q_vertex_to_num_out_tuples = HashMap::new();
        prev_query_plan
            .q_vertex_to_num_out_tuples
            .iter()
            .for_each(|(k, v)| {
                q_vertex_to_num_out_tuples.insert(k.clone(), v.clone());
            });
        q_vertex_to_num_out_tuples.insert(
            base_next_extend.to_query_vertex.clone(),
            estimated_num_out_tuples,
        );

        let mut new_query_plan = prev_query_plan.shallow_copy();
        new_query_plan.estimated_icost = estimated_icost;
        new_query_plan.estimated_num_out_tuples = estimated_num_out_tuples;
        new_query_plan.append(Operator::EI(next_copy));
        new_query_plan.q_vertex_to_num_out_tuples = q_vertex_to_num_out_tuples;
        (
            self.get_key(
                &mut (base_next_extend
                    .base_op
                    .out_qvertex_to_idx_map
                    .keys()
                    .map(|k| k.clone())
                    .collect()),
            ),
            new_query_plan,
        )
    }

    fn get_next_ei(
        &self,
        in_subgraph: &QueryGraph,
        to_qvertex: &String,
        alds: &mut Vec<AdjListDescriptor>,
        last_operator: &Operator<Id>,
    ) -> EI<Id> {
        let mut out_subgraph = in_subgraph.copy();
        in_subgraph
            .get_query_vertices()
            .iter()
            .for_each(|from_qvertex| {
                if self
                    .query_graph
                    .contains_query_edge(from_qvertex, to_qvertex)
                {
                    // simple query graph so there is only 1 query_edge, so get query_edge at index '0'.
                    let query_edge =
                        self.query_graph.get_qedges(from_qvertex, to_qvertex)[0].clone();
                    let index = get_op_attr_as_ref!(last_operator, out_qvertex_to_idx_map)
                        [from_qvertex]
                        .clone();
                    let direction = if from_qvertex == &query_edge.from_query_vertex {
                        Direction::Fwd
                    } else {
                        Direction::Bwd
                    };
                    let label = query_edge.label;
                    alds.push(AdjListDescriptor::new(
                        from_qvertex.clone(),
                        index,
                        direction,
                        label,
                    ));
                    out_subgraph.add_qedge(query_edge);
                }
            });
        let mut output_variable_idx_map = HashMap::new();
        get_op_attr_as_ref!(last_operator, out_qvertex_to_idx_map)
            .iter()
            .for_each(|(k, v)| {
                output_variable_idx_map.insert(k.clone(), v.clone());
            });
        output_variable_idx_map.insert(to_qvertex.clone(), output_variable_idx_map.len());
        EI::make(
            to_qvertex.clone(),
            self.query_graph.get_query_vertex_type(to_qvertex),
            alds.clone(),
            out_subgraph,
            in_subgraph.clone(),
            output_variable_idx_map,
        )
    }

    fn get_selectivity(
        &mut self,
        in_subgraph: &mut QueryGraph,
        out_subgraph: &mut QueryGraph,
        alds: &Vec<AdjListDescriptor>,
        to_type: usize,
    ) -> f64 {
        let selectivity;
        let computed_selectivity_op = self
            .computed_selectivities
            .get_mut(&out_subgraph.get_encoding());
        if computed_selectivity_op.is_some() {
            for (graph, selectivity) in computed_selectivity_op.unwrap() {
                if graph.is_isomorphic_to(out_subgraph) {
                    return selectivity.clone();
                }
            }
        } else {
            self.computed_selectivities
                .insert(out_subgraph.get_encoding(), vec![]);
        }
        selectivity = self.catalog.get_selectivity(in_subgraph, alds, to_type);
        self.computed_selectivities
            .get_mut(&out_subgraph.get_encoding())
            .map(|selectivities| {
                selectivities.push((out_subgraph.clone(), selectivity));
            });
        selectivity
    }

    fn consider_all_next_hash_join_operators(&mut self, map_key: &String) {
        let plan_map = &self.subgraph_plans[&self.next_num_qvertices];
        let plans = &plan_map[map_key];
        let op = plans[0].last_operator.as_ref().unwrap().as_ref();
        let out_subgraph = get_op_attr_as_ref!(op, out_subgraph).as_ref().clone();

        let query_vertices = out_subgraph.get_query_vertices();
        let min_size = 3;
        let mut max_size = out_subgraph.get_query_vertices().len() - min_size;
        if max_size < min_size {
            max_size = min_size;
        }
        for set_size in min_size..=max_size {
            let plans = self.subgraph_plans[&set_size].clone();
            for key in plans.keys() {
                let prev_query_plan = self.get_best_plan(set_size, key);
                let last_op = prev_query_plan.last_operator.as_ref().unwrap().as_ref();
                let base_last_op = get_base_op_as_ref!(last_op);
                let prev_qvertices = base_last_op.out_subgraph.get_query_vertices_as_set();
                let is_subset = query_vertices
                    .iter()
                    .map(|v| prev_qvertices.contains(v))
                    .filter(|&x| !x)
                    .count()
                    == 0;
                if !is_subset {
                    return;
                }
                let mut other_set: Vec<String> = query_vertices
                    .iter()
                    .filter(|&x| !prev_qvertices.contains(x))
                    .map(|x| x.clone())
                    .collect();
                if other_set.len() == 1 {
                    return;
                }
                let join_qvertices =
                    Self::get_join_qvertices(&out_subgraph, &prev_qvertices, &other_set);
                if join_qvertices.len() < 1
                    || join_qvertices.len() > 2
                    || other_set.len() + join_qvertices.len() > self.next_num_qvertices - 1
                {
                    return;
                }
                join_qvertices.iter().for_each(|v| {
                    other_set.push(v.clone());
                });
                let rest_size = other_set.len();
                let rest_key = self.get_key(&mut other_set);
                if !self.subgraph_plans[&rest_size].contains_key(&rest_key) {
                    return;
                }
                let other_prev_operator = self.get_best_plan(rest_size, &rest_key);
                self.consider_hash_join_operator(
                    &out_subgraph,
                    query_vertices.clone(),
                    &prev_query_plan,
                    &other_prev_operator,
                    join_qvertices.len(),
                );
            }
        }
    }

    fn get_join_qvertices(
        query_graph: &QueryGraph,
        vertices: &HashSet<String>,
        other_vertices: &Vec<String>,
    ) -> Vec<String> {
        let mut join_qvertices = HashSet::new();
        vertices.iter().for_each(|cur| {
            other_vertices
                .iter()
                .filter(|&other| query_graph.contains_query_edge(cur, other))
                .for_each(|_other| {
                    join_qvertices.insert(cur.clone());
                })
        });
        join_qvertices.into_iter().collect()
    }

    fn consider_hash_join_operator(
        &mut self,
        out_subgraph: &QueryGraph,
        mut query_vertices: Vec<String>,
        subplan: &QueryPlan<Id>,
        other_subplan: &QueryPlan<Id>,
        num_join_qvertices: usize,
    ) {
        let is_plan_build_subplan =
            subplan.estimated_num_out_tuples < other_subplan.estimated_num_out_tuples;
        let build_subplan = if is_plan_build_subplan {
            subplan
        } else {
            other_subplan
        };
        let probe_subplan = if is_plan_build_subplan {
            other_subplan
        } else {
            subplan
        };
        let build_coef = if num_join_qvertices == 1 {
            SINGLE_VERTEX_WEIGHT_BUILD_COEF
        } else {
            MULTI_VERTEX_WEIGHT_BUILD_COEF
        };
        let probe_coef = if num_join_qvertices == 1 {
            SINGLE_VERTEX_WEIGHT_PROBE_COEF
        } else {
            MULTI_VERTEX_WEIGHT_PROBE_COEF
        };
        let icost = build_subplan.estimated_icost
            + probe_subplan.estimated_icost
            + build_coef * build_subplan.estimated_num_out_tuples
            + probe_coef * probe_subplan.estimated_num_out_tuples;

        let key = self.get_key(&mut query_vertices);
        let curr_best_query_plan = self.get_best_plan(query_vertices.len(), &key);
        if curr_best_query_plan.estimated_icost > icost {
            let mut query_plan = HashJoin::make(
                out_subgraph.clone(),
                build_subplan.clone(),
                probe_subplan.clone(),
            );
            query_plan.estimated_icost = icost;
            query_plan.estimated_num_out_tuples = curr_best_query_plan.estimated_num_out_tuples;

            let mut q_vertex_to_num_out_tuples = HashMap::new();
            probe_subplan
                .q_vertex_to_num_out_tuples
                .iter()
                .for_each(|(k, v)| {
                    q_vertex_to_num_out_tuples.insert(k.clone(), v.clone());
                });

            let last_op = build_subplan.last_operator.as_ref().unwrap().as_ref();
            let base_last_op = get_base_op_as_ref!(last_op);
            base_last_op
                .out_subgraph
                .get_query_vertices()
                .iter()
                .for_each(|v| {
                    q_vertex_to_num_out_tuples
                        .entry(v.clone())
                        .or_insert(curr_best_query_plan.estimated_num_out_tuples);
                });
            query_plan.q_vertex_to_num_out_tuples = q_vertex_to_num_out_tuples;

            let query_plans = self
                .subgraph_plans
                .get_mut(&query_vertices.len())
                .unwrap()
                .get_mut(&key)
                .unwrap();
            query_plans.clear();
            query_plans.push(query_plan);
        }
    }

    fn get_best_plan(&self, num_qvertices: usize, key: &String) -> QueryPlan<Id> {
        let possible_query_plans = &self.subgraph_plans[&num_qvertices][key];
        let mut best_plan = &possible_query_plans[0];
        possible_query_plans.iter().for_each(|possible_query_plan| {
            if possible_query_plan.estimated_icost < best_plan.estimated_icost {
                best_plan = possible_query_plan;
            }
        });
        best_plan.clone()
    }

    pub fn get_num_edges(&self, query_edge: &QueryEdge) -> usize {
        let from_type = self
            .query_graph
            .get_query_vertex_type((&query_edge.from_query_vertex));
        let to_type = self
            .query_graph
            .get_query_vertex_type(&query_edge.to_query_vertex);
        let label = query_edge.label;
        self.graph.get_num_edges(from_type, to_type, label)
    }

    fn get_key(&self, query_vertices: &mut Vec<String>) -> String {
        query_vertices.sort();
        serde_json::to_string(&query_vertices).unwrap()
    }
}
