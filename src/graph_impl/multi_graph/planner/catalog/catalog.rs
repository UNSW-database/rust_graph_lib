use generic::{GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::Intersect;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::multi_graph::planner::catalog::adj_list_descriptor::{
    AdjListDescriptor, Direction,
};
use graph_impl::multi_graph::planner::catalog::catalog_plans::{
    CatalogPlans, DEF_MAX_INPUT_NUM_VERTICES, DEF_NUM_EDGES_TO_SAMPLE,
};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::{HashMap, HashSet};
use itertools::Itertools;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::time::SystemTime;

pub static SINGLE_VERTEX_WEIGHT_PROBE_COEF: f64 = 3.0;
pub static SINGLE_VERTEX_WEIGHT_BUILD_COEF: f64 = 12.0;
pub static MULTI_VERTEX_WEIGHT_PROBE_COEF: f64 = 12.0;
pub static MULTI_VERTEX_WEIGHT_BUILD_COEF: f64 = 720.0;

pub static mut LOGGER_FLAG: bool = false;

pub struct Catalog {
    pub in_subgraphs: Vec<QueryGraph>,
    pub sampled_icost: HashMap<usize, HashMap<String, f64>>,
    pub sampled_selectivity: HashMap<usize, HashMap<String, f64>>,
    pub is_sorted_by_node: bool,
    pub num_sampled_edge: usize,
    pub max_input_num_vertices: usize,
    pub elapsed_time: u128,
}

impl Catalog {
    pub fn new(num_sampled_edge: usize, max_input_num_vertices: usize) -> Self {
        Self {
            in_subgraphs: vec![],
            sampled_icost: HashMap::new(),
            sampled_selectivity: HashMap::new(),
            is_sorted_by_node: false,
            num_sampled_edge,
            max_input_num_vertices,
            elapsed_time: 0,
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
            elapsed_time: 0,
        }
    }

    /// Returns the i-cost of a particular extension from an input.
    pub fn get_icost(
        &self,
        query_graph: &mut QueryGraph,
        alds: Vec<&AdjListDescriptor>,
        to_type: i32,
    ) -> f64 {
        let mut approx_icost = 0.0;
        let mut min_icost = std::f64::MAX;
        alds.iter().for_each(|ald| {
            for num_vertices in (2..=(DEF_NUM_EDGES_TO_SAMPLE - 1)).rev() {
                min_icost = std::f64::MAX;
                let mut num_edges_matched = 0;
                for (i, sub_graph) in self.in_subgraphs.iter().enumerate() {
                    if sub_graph.get_num_qvertices() != num_vertices {
                        continue;
                    }
                    let new_num_edges_matched = query_graph.q_edges.len();
                    let it = query_graph.get_subgraph_mapping_iterator(&self.in_subgraphs[i]);
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
                            + &new_vertex_mapping[&ald.from_query_vertex]
                            + ") "
                            + &ald.direction.to_string()
                            + "["
                            + &ald.label.to_string()
                            + "]";
                        if self.is_sorted_by_node {
                            sampled_icost = self.sampled_selectivity[&i]
                                [&(aldas_str + "~" + &to_type.to_string())]
                                .clone();
                        } else {
                            sampled_icost = self.sampled_icost[&i][&aldas_str].clone();
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
        to_type: i32,
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
                    let new_num_alds_matched = self.get_num_alds_matched(&alds, &vertex_mapping);
                    if new_num_alds_matched == 0 || new_num_alds_matched < num_alds_matched {
                        continue;
                    }
                    let sampled_selectivity = self.sampled_selectivity[&i]
                        [&self.get_alds_as_str(&alds, Some(&vertex_mapping), Some(to_type))]
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
        to_type: Option<i32>,
    ) -> String {
        let mut from_qvertices_and_dirs = alds
            .iter()
            .filter(|ald| {
                vertex_mapping.is_none()
                    || vertex_mapping
                        .unwrap()
                        .get(&ald.from_query_vertex)
                        .is_some()
            })
            .map(|ald| {
                "(".to_owned()
                    + if vertex_mapping.is_none() {
                        &ald.from_query_vertex
                    } else {
                        let vertex_mapping = vertex_mapping.unwrap();
                        vertex_mapping.get(&ald.from_query_vertex).unwrap()
                    }
                    + ") "
                    + &ald.direction.to_string()
                    + "["
                    + &ald.label.to_string()
                    + "]"
            })
            .sorted()
            .join(", ");
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
        for ald in alds {
            from_vertices_in_alds.insert(ald.from_query_vertex.clone());
        }
        let num_alds_matched = 0;
        vertex_mapping
            .keys()
            .filter(|&vertex| {
                from_vertices_in_alds.contains(vertex) && vertex_mapping[vertex] != ""
            })
            .count()
    }

    ///TODO: Multi thread catalog building
    pub fn populate<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        num_threads: usize,
    ) {
        let start_time = SystemTime::now();
        self.is_sorted_by_node = graph.is_sorted_by_node();
        self.sampled_icost = HashMap::new();
        self.sampled_selectivity = HashMap::new();
        let mut plans = CatalogPlans::new(
            &graph,
            num_threads,
            self.num_sampled_edge,
            self.max_input_num_vertices,
        );
        self.set_input_subgraphs(plans.query_graphs_to_extend.get_query_graph_set());
        self.add_zero_selectivities(&graph, &mut plans);

        for query_plan_arr in &mut plans.query_plans_arrs {
            self.init(&graph, query_plan_arr);
            self.execute(query_plan_arr);
            self.log_output(&graph, query_plan_arr);
            query_plan_arr.clear();
        }
        self.elapsed_time = SystemTime::now()
            .duration_since(start_time)
            .unwrap()
            .as_millis();
    }

    fn init<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        query_plan_arr: &mut Vec<QueryPlan<Id>>,
    ) {
        for query_plan in query_plan_arr {
            let probe_tuple = Rc::new(RefCell::new(vec![
                Id::new(0);
                self.max_input_num_vertices + 1
            ]));
            if let Some(scan) = &mut query_plan.scan_sampling {
                scan.borrow_mut().init(probe_tuple, graph);
            }
        }
    }

    fn execute<Id: IdType>(&self, query_plan_arr: &mut Vec<QueryPlan<Id>>) {
        if query_plan_arr.len() > 1 {
            //            let mut handlers = vec![];
            for i in 0..query_plan_arr.len() {
                let mut sink = query_plan_arr[i].sink.as_mut().unwrap().borrow_mut();
                sink.execute();
                //                handlers.push(thread::spawn(move || {
                //                    sink.execute();
                //                }));
            }
        //            for handler in handlers {
        //                handler.join();
        //            }
        } else {
            let mut sink = query_plan_arr[0].sink.as_mut().unwrap().as_ptr();
            unsafe {
                (&mut *sink).execute();
            }
        }
    }

    fn retrieve_op<Id: IdType>(op: &Rc<RefCell<Operator<Id>>>) {
        unsafe {
            print!("{:?}->", op.as_ptr());
        }
        let op_ref = op.borrow();
        let base = get_base_op_as_ref!(op_ref.deref());
        if let Some(op) = &base.prev {
            Self::retrieve_op(op);
        }
    }

    fn log_output<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        query_plan_arr: &mut Vec<QueryPlan<Id>>,
    ) {
        let mut other: Vec<Rc<RefCell<Operator<Id>>>> = query_plan_arr
            .iter_mut()
            .map(|plan| plan.sink.as_ref().unwrap().borrow())
            .map(|query_plan| {
                if let Operator::Sink(sink) = query_plan.deref() {
                    let base_sink = get_sink_as_ref!(sink);
                    let mut op = base_sink.previous[0].clone();
                    loop {
                        if let Operator::Scan(Scan::ScanSampling(sp)) = op.borrow().deref() {
                            break;
                        }
                        op = {
                            let op_ref = op.borrow();
                            get_op_attr_as_ref!(op_ref.deref(), prev)
                                .as_ref()
                                .unwrap()
                                .clone()
                        };
                    }
                    let op_ref = op.borrow();
                    get_op_attr_as_ref!(op_ref.deref(), next)[0].clone()
                } else {
                    panic!("Sink has not been set.")
                }
            })
            .collect();

        let op = other.remove(0);
        if self.is_sorted_by_node {
            self.add_icost_and_selectivity_sorted_by_node(op, other, !graph.is_directed());
        } else {
            self.add_icost_and_selectivity(op, other, !graph.is_directed());
        }
    }

    fn add_icost_and_selectivity_sorted_by_node<Id: IdType>(
        &mut self,
        operator: Rc<RefCell<Operator<Id>>>,
        other: Vec<Rc<RefCell<Operator<Id>>>>,
        is_undirected: bool,
    ) {
        if let Operator::Sink(sink) = get_op_attr_as_ref!(operator.borrow().deref(), next)[0]
            .borrow()
            .deref()
        {
            return;
        }
        let mut num_input_tuples = get_op_attr!(operator.borrow().deref(), num_out_tuples);

        for other_op in &other {
            num_input_tuples += get_op_attr!(other_op.borrow().deref(), num_out_tuples);
        }
        let mut in_subgraph = {
            let op_ref = operator.borrow();
            get_op_attr_as_ref!(op_ref.deref(), out_subgraph).clone()
        };
        let subgraph_idx = self.get_subgraph_idx(&mut in_subgraph);
        let next = {
            let op_ref = operator.borrow();
            get_op_attr_as_ref!(op_ref.deref(), next).clone()
        };

        for i in 0..next.len() {
            let next_i = next[i].borrow();
            if let Operator::EI(EI::Intersect(Intersect::IntersectCatalog(intersect))) =
                next_i.deref()
            {
                let to_type = intersect.base_intersect.base_ei.to_type;
                let mut alds_as_str_list = vec![];
                let alds_str = self.get_alds_as_str(
                    &intersect.base_intersect.base_ei.alds,
                    None,
                    Some(to_type),
                );
                if is_undirected {
                    let splits: Vec<&str> = alds_str.split(", ").collect();
                    let direction_patterns = CatalogPlans::<Id>::generate_direction_patterns(
                        splits.len(),
                        is_undirected,
                    );
                    for pattern in direction_patterns {
                        let mut alds_str_with_pattern = "".to_owned();
                        for j in 0..pattern.len() {
                            let ok: Vec<&str> = splits[j].split("Bwd").collect();
                            alds_str_with_pattern =
                                alds_str_with_pattern + ok[0] + &pattern[j].to_string() + ok[1];
                            if j != pattern.len() - 1 {
                                alds_str_with_pattern += ", ";
                            }
                        }
                        alds_as_str_list.push(alds_str_with_pattern);
                    }
                } else {
                    alds_as_str_list.push(alds_str);
                }
                let mut selectivity = intersect.base_intersect.base_ei.base_op.num_out_tuples;
                for other_op in &other {
                    let next = {
                        let other_op_ref = other_op.borrow();
                        get_op_attr_as_ref!(other_op_ref.deref(), next)[i].clone()
                    };
                    let next_ref = next.borrow();
                    selectivity += get_op_attr!(next_ref.deref(), num_out_tuples);
                }
                self.sampled_selectivity
                    .entry(subgraph_idx)
                    .or_insert(HashMap::new());
                for alds_as_str in alds_as_str_list {
                    self.sampled_selectivity
                        .get_mut(&subgraph_idx)
                        .unwrap()
                        .insert(
                            alds_as_str,
                            if num_input_tuples > 0 {
                                (selectivity as f64) / (num_input_tuples as f64)
                            } else {
                                0.0
                            },
                        );
                }
                let noop = {
                    let next_ref = next[i].borrow();
                    get_op_attr_as_ref!(next_ref.deref(), next)[0].clone()
                };
                let mut other_noops = vec![];
                for (j, other) in other.iter().enumerate() {
                    other_noops.push({
                        let other_ref = other.borrow();
                        let next_i = get_op_attr_as_ref!(other_ref.deref(), next)[i].clone();
                        let next_ref = next_i.borrow();
                        get_op_attr_as_ref!(next_ref.deref(), next)[j].clone()
                    });
                }
                self.add_icost_and_selectivity(noop, other_noops, is_undirected);
            }
        }
    }

    fn add_icost_and_selectivity<Id: IdType>(
        &mut self,
        operator: Rc<RefCell<Operator<Id>>>,
        other: Vec<Rc<RefCell<Operator<Id>>>>,
        is_undirected: bool,
    ) {
        if let Operator::Sink(sink) = get_op_attr_as_ref!(operator.borrow().deref(), next)[0]
            .borrow()
            .deref()
        {
            return;
        }
        let mut num_input_tuples = get_op_attr!(operator.borrow().deref(), num_out_tuples);
        for other_op in &other {
            num_input_tuples += get_op_attr!(other_op.borrow().deref(), num_out_tuples);
        }
        let mut in_subgraph = {
            let op_ref = operator.borrow();
            get_op_attr_as_ref!(op_ref.deref(), out_subgraph).clone()
        };
        let subgraph_idx = self.get_subgraph_idx(&mut in_subgraph);
        let next_vec = {
            let op_ref = operator.borrow();
            get_op_attr_as_ref!(op_ref.deref(), next).clone()
        };
        for (i, next) in next_vec.iter().enumerate() {
            let next_ref = next.borrow();
            if let Operator::EI(EI::Intersect(Intersect::IntersectCatalog(intersect))) =
                next_ref.deref()
            {
                let alds = &intersect.base_intersect.base_ei.alds;
                let mut alds_as_str_list = vec![];
                let alds_str =
                    self.get_alds_as_str(&intersect.base_intersect.base_ei.alds, None, None);
                if is_undirected {
                    let splits: Vec<&str> = alds_str.split(", ").collect();
                    let direction_patterns = CatalogPlans::<Id>::generate_direction_patterns(
                        splits.len(),
                        is_undirected,
                    );
                    for pattern in direction_patterns {
                        let mut alds_str_with_pattern = "".to_owned();
                        for j in 0..pattern.len() {
                            let ok: Vec<&str> = splits[j].split("Bwd").collect();
                            alds_str_with_pattern =
                                alds_str_with_pattern + ok[0] + &pattern[j].to_string() + ok[1];
                            if j != pattern.len() - 1 {
                                alds_str_with_pattern += ", ";
                            }
                        }
                        alds_as_str_list.push(alds_str_with_pattern);
                    }
                } else {
                    alds_as_str_list.push(alds_str);
                }
                if 1 == alds.len() {
                    let mut icost = get_op_attr!(next.borrow().deref(), icost);
                    for other_op in &other {
                        let next = {
                            let other_ref = other_op.borrow();
                            get_op_attr_as_ref!(other_ref.deref(), next)[i].clone()
                        };
                        icost += get_op_attr!(next.borrow().deref(), icost);
                    }
                    self.sampled_icost
                        .entry(subgraph_idx)
                        .or_insert(HashMap::new());
                    for alds_as_str in &alds_as_str_list {
                        if num_input_tuples > 0 {
                            self.sampled_icost
                                .get_mut(&subgraph_idx)
                                .unwrap()
                                .entry(alds_as_str.clone())
                                .or_insert((icost as f64) / (num_input_tuples as f64));
                        } else {
                            self.sampled_icost
                                .get_mut(&subgraph_idx)
                                .unwrap()
                                .entry(alds_as_str.clone())
                                .or_insert(0.0);
                        }
                    }
                }
                let noops = {
                    let next_ref = next.borrow();
                    get_op_attr_as_ref!(next_ref.deref(), next).clone()
                };
                for to_type in 0..noops.len() {
                    let noop = noops[to_type].clone();
                    let mut selectivity = {
                        let noop_ref = noop.borrow();
                        get_op_attr!(noop_ref.deref(), num_out_tuples)
                    };
                    for other_op in &other {
                        let next = {
                            let other_op_ref = other_op.borrow();
                            get_op_attr_as_ref!(other_op_ref.deref(), next)[i].clone()
                        };
                        let o_next = {
                            let next_ref = next.borrow();
                            get_op_attr_as_ref!(next_ref.deref(), next)[to_type].clone()
                        };
                        selectivity += {
                            let o_next_ref = next.borrow();
                            get_op_attr!(o_next_ref.deref(), num_out_tuples)
                        };
                    }
                    self.sampled_selectivity
                        .entry(subgraph_idx)
                        .or_insert(HashMap::new());
                    for alds_as_str in &alds_as_str_list {
                        self.sampled_selectivity
                            .get_mut(&subgraph_idx)
                            .unwrap()
                            .insert(
                                alds_as_str.to_owned() + "~" + &to_type.to_string(),
                                if num_input_tuples > 0 {
                                    (selectivity as f64) / (num_input_tuples as f64)
                                } else {
                                    0.0
                                },
                            );
                    }
                    let mut other_noops = vec![];
                    for other_op in &other {
                        let next = {
                            let other_op_ref = other_op.borrow();
                            get_op_attr_as_ref!(other_op_ref.deref(), next)[i].clone()
                        };
                        let next_op = {
                            let next_ref = next.borrow();
                            get_op_attr_as_ref!(next_ref.deref(), next)[to_type].clone()
                        };
                        other_noops.push(next_op);
                    }
                    self.add_icost_and_selectivity(noop, other_noops, is_undirected);
                }
            }
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
        panic!("Illegal argument exception.")
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
        let selectivity_zero = &mut plans.selectivity_zero;
        for (q_graph, alds, to_type) in selectivity_zero {
            let subgraph_idx = self.get_subgraph_idx(q_graph);
            if self.sampled_selectivity.get(&subgraph_idx).is_none() {
                self.sampled_selectivity
                    .insert(subgraph_idx, HashMap::new());
            }
            let mut alds_as_str_list = vec![];
            let alds_str = self.get_alds_as_str(alds, None, None);
            if !graph.is_directed() {
                let splits: Vec<&str> = alds_str.split(", ").collect();
                let direction_patterns =
                    self.generate_direction_patterns(splits.len(), !graph.is_directed());
                for pattern in direction_patterns {
                    let mut alds_str_with_pattern = "".to_string();
                    for i in 0..pattern.len() {
                        let ok: Vec<&str> = splits[i].split("Bwd").collect();
                        alds_str_with_pattern =
                            alds_str_with_pattern + ok[0] + &pattern[i].to_string() + &ok[1];
                        if i != pattern.len() - 1 {
                            alds_str_with_pattern.push_str(", ");
                        }
                    }
                    alds_as_str_list.push(alds_str_with_pattern);
                }
            } else {
                alds_as_str_list.push(alds_str);
            }
            for alds_as_str in alds_as_str_list {
                let selectivity = self.sampled_selectivity.get_mut(&subgraph_idx).unwrap();
                selectivity.insert(alds_as_str + "~" + &to_type.to_string(), 0.00);
            }
        }
    }
}