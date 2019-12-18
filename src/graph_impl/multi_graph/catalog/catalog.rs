use generic::{GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::catalog::adj_list_descriptor::{AdjListDescriptor, Direction};
use graph_impl::multi_graph::catalog::catalog_plans::{
    CatalogPlans, DEF_MAX_INPUT_NUM_VERTICES, DEF_NUM_EDGES_TO_SAMPLE,
};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::utils::io_utils;
use graph_impl::TypedStaticGraph;
use hashbrown::{HashMap, HashSet};
use indexmap::Equivalent;
use itertools::Itertools;
use std::cell::RefCell;
use std::hash::Hash;
use std::iter::FromIterator;
use std::mem::replace;
use std::ops::Deref;
use std::panic::catch_unwind;
use std::ptr::null;
use std::thread;

pub struct Catalog {
    in_subgraphs: Vec<QueryGraph>,
    sampled_icost: HashMap<usize, HashMap<String, f64>>,
    sampled_selectivity: HashMap<usize, HashMap<String, f64>>,
    is_sorted_by_node: bool,
    num_sampled_edge: usize,
    max_input_num_vertices: usize,
    elapsed_time: f32,
}

impl Catalog {
    pub fn new(num_sampled_edge: usize, max_input_num_vertices: usize) -> Self {
        Self {
            in_subgraphs: vec![],
            sampled_icost: Default::default(),
            sampled_selectivity: Default::default(),
            is_sorted_by_node: false,
            num_sampled_edge,
            max_input_num_vertices,
            elapsed_time: 0.0,
        }
    }

    // Icost is the intersection costs sampled.
    // Cardinality is the sampledSelectivity sampled.
    // InSubgraphs are the set of input subgraphs sampled.
    pub fn with_cost(
        i_cost: HashMap<usize, HashMap<String, f64>>,
        cardinality: HashMap<usize, HashMap<String, f64>>,
        in_subgraphs: Vec<QueryGraph>,
    ) -> Self {
        Self {
            in_subgraphs,
            sampled_icost: i_cost,
            sampled_selectivity: cardinality,
            is_sorted_by_node: false,
            num_sampled_edge: 0,
            max_input_num_vertices: 0,
            elapsed_time: 0.0,
        }
    }

    /// Returns the i-cost of a particular extension from an input.
    pub fn get_icost(
        &self,
        query_graph: &mut QueryGraph,
        alds: Vec<AdjListDescriptor>,
        to_type: usize,
    ) -> f64 {
        let mut approx_icost = 0.0;
        let mut min_icost = std::f64::MAX;
        alds.iter().for_each(|ald| {
            // Get each ALD icost by finding the largest subgraph (num vertices then num edges)
            // of queryGraph used in stats collection and also minimizing sampledIcost.
            for num_vertices in (DEF_NUM_EDGES_TO_SAMPLE - 1)..=2 {
                min_icost = std::f64::MAX;
                let mut num_edges_matched = 0;
                for (i, sub_graph) in self.in_subgraphs.iter().enumerate() {
                    if sub_graph.get_num_qvertices() != num_vertices {
                        continue;
                    }
                    let new_num_edges_matched = query_graph.get_query_edges().len();
                    let it = query_graph
                        .get_subgraph_mapping_iterator(self.in_subgraphs.get(i).unwrap());
                    if new_num_edges_matched < num_edges_matched {
                        continue;
                    }
                    while it.has_next() {
                        let new_vertex_mapping = it.next().unwrap();
                        if new_vertex_mapping.get(&ald.from_query_vertex).is_none() {
                            continue;
                        }
                        let sampled_icost;
                        let aldas_str = "(".to_string()
                            + new_vertex_mapping.get(&ald.from_query_vertex).unwrap()
                            + ") "
                            + &ald.direction.to_string()
                            + "["
                            + &ald.label.to_string()
                            + "]";
                        if self.is_sorted_by_node {
                            sampled_icost = self
                                .sampled_selectivity
                                .get(&i)
                                .unwrap()
                                .get(&(aldas_str + "~" + &to_type.to_string()))
                                .unwrap()
                                .clone();
                        } else {
                            sampled_icost = self
                                .sampled_icost
                                .get(&i)
                                .unwrap()
                                .get(&aldas_str)
                                .unwrap()
                                .clone();
                        }
                        if new_num_edges_matched > num_edges_matched || min_icost > sampled_icost {
                            min_icost = sampled_icost;
                            num_edges_matched = new_num_edges_matched;
                        }
                    }
                }
                if min_icost < std::f64::MAX {
                    break;
                }
            }
            approx_icost += min_icost;
        });
        return approx_icost;
    }

    /// Returns the sampledSelectivity of a particular extension from an input.
    pub fn get_selectivity(
        &self,
        in_subgraph: &mut QueryGraph,
        alds: &Vec<AdjListDescriptor>,
        to_type: usize,
    ) -> f64 {
        let mut approx_selectivity = std::f64::MAX;
        let mut num_vertices = DEF_MAX_INPUT_NUM_VERTICES - 1;
        while num_vertices >= 2 {
            let mut num_alds_matched = 0;
            for (i, sub_graph) in self.in_subgraphs.iter().enumerate() {
                if sub_graph.get_num_qvertices() != num_vertices {
                    continue;
                }
                let it = in_subgraph.get_subgraph_mapping_iterator(sub_graph);
                while it.has_next() {
                    let vertex_mapping = it.next().unwrap();
                    let new_num_alds_matched = self.get_num_alds_matched(&alds, vertex_mapping);
                    if new_num_alds_matched == 0 || new_num_alds_matched < num_alds_matched {
                        continue;
                    }
                    let selectivity_map = self.sampled_selectivity.get(&i).unwrap();
                    let sampled_selectivity = selectivity_map
                        .get(&self.get_alds_as_str(&alds, Some(vertex_mapping), Some(to_type)))
                        .unwrap()
                        .clone();
                    if new_num_alds_matched > num_alds_matched
                        || sampled_selectivity < approx_selectivity
                    {
                        num_alds_matched = new_num_alds_matched;
                        approx_selectivity = sampled_selectivity;
                    }
                }
            }
            num_vertices -= 1;
        }
        approx_selectivity
    }

    fn get_alds_as_str(
        &self,
        alds: &Vec<AdjListDescriptor>,
        vertex_mapping: Option<&HashMap<String, String>>,
        to_type: Option<usize>,
    ) -> String {
        let mut from_qvertices_and_dirs = alds
            .iter()
            .map(|ald| {
                let tail = (") ".to_owned()
                    + &ald.direction.to_string()
                    + "["
                    + &ald.label.to_string()
                    + "]");
                if vertex_mapping.is_none() {
                    return Some("(".to_owned() + &ald.from_query_vertex + &tail);
                } else {
                    let vertex_mapping = vertex_mapping.unwrap();
                    if let Some(from) = vertex_mapping.get(&ald.from_query_vertex) {
                        return Some("(".to_owned() + from + &tail);
                    }
                }
                None
            })
            .skip_while(|x| x.is_none())
            .map(|x| x.unwrap())
            .sorted()
            .join(",");
        if to_type.is_some() {
            from_qvertices_and_dirs += &("~".to_owned() + &to_type.unwrap().to_string());
        }
        from_qvertices_and_dirs
    }

    fn get_num_alds_matched(
        &self,
        alds: &Vec<AdjListDescriptor>,
        vertex_mapping: &HashMap<String, String>,
    ) -> usize {
        let mut from_vertices_in_alds = HashSet::new();
        for ald in alds.iter() {
            from_vertices_in_alds.insert(ald.from_query_vertex.clone());
        }
        let num_alds_matched = 0;
        vertex_mapping
            .keys()
            .filter(|&vertex| {
                from_vertices_in_alds.contains(vertex) && vertex_mapping.get(vertex).unwrap() != ""
            })
            .count()
    }

    pub fn populate<Id:IdType,NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: TypedStaticGraph<Id, NL, EL, Ty, L>,
        num_threads: usize,
        filename: String,
    ) {
        let start_time = io_utils::current_time();
        self.is_sorted_by_node = graph.is_sorted_by_node();
        self.sampled_icost = HashMap::new();
        self.sampled_selectivity = HashMap::new();
        let mut plans = CatalogPlans::new(
            &graph,
            num_threads,
            self.num_sampled_edge,
            self.max_input_num_vertices,
        );
        self.set_input_subgraphs(plans.query_graphs_to_extend().get_query_graph_set());
        self.add_zero_selectivities(&graph, &mut plans);

        for query_plan_arr in plans.get_query_plan_arrs() {
            self.init(&graph, query_plan_arr);
            self.execute(query_plan_arr);
            self.log_output(&graph, query_plan_arr);
            query_plan_arr.clear();
        }
        self.elapsed_time = io_utils::get_elapsed_time_in_millis(start_time);
        //log here
    }

    fn init<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        query_plan_arr: &mut Vec<QueryPlan<Id>>,
    ) {
        for query_plan in query_plan_arr {
            let probe_tuple = vec![Id::new(0); self.max_input_num_vertices + 1];
            if let Some(scan) = query_plan.get_scan_sampling() {
                scan.init(probe_tuple, graph);
            }
        }
    }

    fn execute<Id:IdType>(&self, query_plan_arr: &mut Vec<QueryPlan<Id>>) {
        if query_plan_arr.len() > 1 {
            let mut handlers = vec![];
            for i in 0..query_plan_arr.len() {
                let mut sink = query_plan_arr[i].get_sink().clone();
                handlers.push(thread::spawn(move || {
                    sink.execute();
                }));
            }
            for handler in handlers {
                handler.join();
            }
        } else {
            let sink = query_plan_arr[0].get_sink();
            sink.execute();
        }
    }

    fn log_output<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        query_plan_arr: &mut Vec<QueryPlan<Id>>,
    ) {
        let mut other: Vec<&mut Operator<Id>> = query_plan_arr
            .iter_mut()
            .map(|query_plan| {
                let base_sink = get_sink_as_mut!(query_plan.get_sink());
                let mut op = &mut base_sink.previous.as_mut().unwrap()[0];
                while if let Operator::Scan(Scan::ScanSampling(sp)) = op.deref() { false } else { true } {
                    let prev_op = get_op_attr_as_mut!(op, prev).as_mut().unwrap();
                    op = prev_op.as_mut();
                }
                &mut get_op_attr_as_mut!(op, next).as_mut().unwrap()[0]
            })
            .collect();

        let op = other.remove(0);
        if self.is_sorted_by_node {
            self.add_icost_and_selectivity_sorted_by_node(op, other, graph.is_directed());
        } else {
            self.add_icost_and_selectivity(op, other, graph.is_directed());
        }
    }

    fn add_icost_and_selectivity_sorted_by_node<Id: IdType>(
        &self,
        operator: &mut Operator<Id>,
        other: Vec<&mut Operator<Id>>,
        is_directed: bool,
    ) {}

    fn add_icost_and_selectivity<Id: IdType>(
        &mut self,
        operator: &mut Operator<Id>,
        other: Vec<&mut Operator<Id>>,
        is_directed: bool,
    ) {
        if let Operator::Sink(sink) = &get_op_attr_as_ref!(operator, next).as_ref().unwrap()[0] {
            return;
        }
        let mut num_input_tuples = get_op_attr!(operator, num_out_tuples);
        for other_op in other {
            num_input_tuples += get_op_attr!(other_op, num_out_tuples);
        }
        let in_subgraph = get_op_attr_as_mut!(operator, out_subgraph);
        let subgraph_idx = self.get_subgraph_idx(in_subgraph);
        let next_list = get_op_attr_as_ref!(operator, next).as_ref().unwrap();
        for (i, next) in next_list.iter().enumerate() {
            let intersect = next;
            //TODO: Implement following code.
        }
    }

    fn set_input_subgraphs(&mut self, in_subgraphs: Vec<QueryGraph>) {
        self.in_subgraphs = vec![];
        for mut in_subgraph in in_subgraphs {
            let mut is_unique = true;
            for subgraph in self.in_subgraphs.iter_mut() {
                if subgraph.is_isomorphic_to(&mut in_subgraph) {
                    is_unique = false;
                    break;
                }
            }
            if is_unique {
                self.in_subgraphs.push(in_subgraph);
            }
        }
    }

    fn get_subgraph_idx(&mut self, in_subgraph: &mut QueryGraph) -> usize {
        for (idx, sub_graph) in self.in_subgraphs.iter_mut().enumerate() {
            if in_subgraph.is_isomorphic_to(sub_graph) {
                return idx;
            }
        }
        //TODO:Fix the case when the given subgraph not found
        0
    }

    fn generate_direction_patterns(&self, size: usize, is_directed: bool) -> Vec<Vec<Direction>> {
        let mut direction_patterns = vec![];
        let mut directions = vec![Direction::Bwd; size];
        self.sub_generate_direction_patterns(
            &mut directions,
            size,
            &mut direction_patterns,
            is_directed,
        );
        direction_patterns
    }

    fn sub_generate_direction_patterns(
        &self,
        directions: &mut Vec<Direction>,
        size: usize,
        direction_pattern: &mut Vec<Vec<Direction>>,
        is_directed: bool,
    ) {
        if size <= 0 {
            direction_pattern.push(directions.to_vec());
        } else {
            directions[size - 1] = Direction::Bwd;
            self.sub_generate_direction_patterns(
                directions,
                size - 1,
                direction_pattern,
                is_directed,
            );
            if is_directed {
                directions[size - 1] = Direction::Fwd;
                self.sub_generate_direction_patterns(
                    directions,
                    size - 1,
                    direction_pattern,
                    is_directed,
                );
            }
        }
    }

    fn add_zero_selectivities<
        Id: IdType,
        NL: Hash + Eq,
        EL: Hash + Eq,
        Ty: GraphType,
        L: IdType,
    >(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        plans: &mut CatalogPlans<Id>,
    ) {
        let selectivity_zero = plans.get_selectivity_zero();
        for select in selectivity_zero {
            let subgraph_idx = self.get_subgraph_idx(&mut select.0);
            if self.sampled_selectivity.get(&subgraph_idx).is_none() {
                self.sampled_selectivity
                    .insert(subgraph_idx, HashMap::new());
            }
            let mut alds_as_str_list = vec![];
            let alds_str = self.get_alds_as_str(&select.1, None, None);
            if !graph.is_directed() {
                let splits: Vec<&str> = alds_str.split(',').collect();
                let direction_patterns =
                    self.generate_direction_patterns(splits.len(), !graph.is_directed());
                for pattern in direction_patterns {
                    let mut alds_str_with_pattern = "".to_string();
                    for i in 0..pattern.len() {
                        let ok: Vec<&str> = splits[i].split("Bwd").collect();
                        alds_str_with_pattern =
                            alds_str_with_pattern + ok[0] + &pattern[i].to_string() + &ok[1];
                        if i != pattern.len() - 1 {
                            alds_str_with_pattern.push_str(",");
                        }
                    }
                    alds_as_str_list.push(alds_str_with_pattern);
                }
            } else {
                alds_as_str_list.push(alds_str);
            }
            for alds_as_str in alds_as_str_list {
                if let Some(selectivity) = self.sampled_selectivity.get_mut(&subgraph_idx) {
                    selectivity.insert(alds_as_str + "~" + &select.2.to_string(), 0.00);
                }
            }
        }
    }
}
