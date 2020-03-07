use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::BaseIntersect;
use graph_impl::multi_graph::plan::operator::extend::intersect::Intersect;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::hash_table::HashTable;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::multi_graph::plan::operator::scan::scan_sampling::ScanSampling;
use graph_impl::multi_graph::plan::operator::sink::sink::{BaseSink, Sink, SinkType};
use graph_impl::multi_graph::plan::operator::sink::sink_copy::SinkCopy;
use graph_impl::multi_graph::plan::operator::sink::sink_limit::SinkLimit;
use graph_impl::multi_graph::plan::operator::sink::sink_print::SinkPrint;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use std::time::SystemTime;

#[derive(Clone)]
pub struct QueryPlan<Id: IdType> {
    pub sink: Option<Rc<RefCell<Operator<Id>>>>,
    pub sink_type: SinkType,
    pub scan_sampling: Option<Rc<RefCell<Operator<Id>>>>,
    pub last_operator: Option<Rc<RefCell<Operator<Id>>>>,
    pub out_tuples_limit: usize,
    pub elapsed_time: u128,
    pub icost: usize,
    pub num_intermediate_tuples: usize,
    pub num_out_tuples: usize,
    pub operator_metrics: Vec<(String, usize, usize)>,
    executed: bool,
    adaptive_enabled: bool,
    pub subplans: Vec<Rc<RefCell<Operator<Id>>>>,
    pub estimated_icost: f64,
    pub estimated_num_out_tuples: f64,
    pub q_vertex_to_num_out_tuples: HashMap<String, f64>,
}

impl<Id: IdType> QueryPlan<Id> {
    pub fn new(scan_sampling: Rc<RefCell<Operator<Id>>>) -> Self {
        let mut last_operators = {
            let op_ref = scan_sampling.borrow();
            op_ref.deref().get_last_operators()
        };

        let out_subgraph = {
            let op = last_operators[0].borrow();
            get_op_attr_as_ref!(op.deref(), out_subgraph).clone()
        };
        let mut sink = Rc::new(RefCell::new(Operator::Sink(Sink::BaseSink(BaseSink::new(
            out_subgraph,
        )))));
        for op in last_operators.iter_mut() {
            *get_op_attr_as_mut!(op.borrow_mut().deref_mut(), next) = vec![sink.clone()];
        }
        if let Operator::Sink(Sink::BaseSink(sink)) = sink.borrow_mut().deref_mut() {
            sink.previous = last_operators.clone();
        }
        Self {
            sink: Some(sink),
            sink_type: SinkType::Counter,
            scan_sampling: Some(scan_sampling),
            last_operator: None,
            out_tuples_limit: 0,
            elapsed_time: 0,
            icost: 0,
            num_intermediate_tuples: 0,
            num_out_tuples: 0,
            operator_metrics: vec![],
            executed: false,
            adaptive_enabled: false,
            subplans: vec![],
            estimated_icost: 0.0,
            estimated_num_out_tuples: 0.0,
            q_vertex_to_num_out_tuples: HashMap::new(),
        }
    }
    pub fn new_from_operator(last_operator: Rc<RefCell<Operator<Id>>>) -> Self {
        Self {
            sink: None,
            sink_type: SinkType::Counter,
            scan_sampling: None,
            last_operator: Some(last_operator.clone()),
            out_tuples_limit: 0,
            elapsed_time: 0,
            icost: 0,
            num_intermediate_tuples: 0,
            num_out_tuples: 0,
            operator_metrics: vec![],
            executed: false,
            adaptive_enabled: false,
            subplans: vec![last_operator],
            estimated_icost: 0.0,
            estimated_num_out_tuples: 0.0,
            q_vertex_to_num_out_tuples: HashMap::new(),
        }
    }
    pub fn new_from_subplans(subplans: Vec<Rc<RefCell<Operator<Id>>>>) -> Self {
        Self {
            sink: None,
            sink_type: SinkType::Counter,
            scan_sampling: None,
            last_operator: subplans.get(subplans.len() - 1).map(|x| x.clone()),
            out_tuples_limit: 0,
            elapsed_time: 0,
            icost: 0,
            num_intermediate_tuples: 0,
            num_out_tuples: 0,
            operator_metrics: vec![],
            executed: false,
            adaptive_enabled: false,
            subplans,
            estimated_icost: 0.0,
            estimated_num_out_tuples: 0.0,
            q_vertex_to_num_out_tuples: HashMap::new(),
        }
    }
    pub fn new_from_last_op(last_operator: Scan<Id>, estimated_num_out_tuples: f64) -> Self {
        let mut map = HashMap::new();
        let op = get_scan_as_ref!(&last_operator);
        map.insert(op.from_query_vertex.clone(), estimated_num_out_tuples);
        map.insert(op.to_query_vertex.clone(), estimated_num_out_tuples);
        let mut plan =
            QueryPlan::new_from_operator(Rc::new(RefCell::new(Operator::Scan(last_operator))));
        plan.estimated_num_out_tuples = estimated_num_out_tuples;
        map.into_iter().for_each(|(k, v)| {
            plan.q_vertex_to_num_out_tuples.insert(k, v);
        });
        plan
    }

    pub fn shallow_copy(&self) -> QueryPlan<Id> {
        QueryPlan::new_from_subplans(self.subplans.clone())
    }

    pub fn append(&mut self, mut new_operator: Rc<RefCell<Operator<Id>>>) {
        {
            let mut last_operator = self.last_operator.as_mut().unwrap().borrow_mut();
            *get_op_attr_as_mut!(last_operator.deref_mut(), next) = vec![new_operator.clone()];
        }
        *get_op_attr_as_mut!(new_operator.borrow_mut().deref_mut(), prev) =
            self.last_operator.as_ref().map(|op| op.clone());
        self.subplans.pop();
        self.subplans.push(new_operator.clone());
        self.last_operator = Some(new_operator);
    }

    pub fn get_output_log(&mut self) -> String {
        self.set_stats();
        let mut str_joiner = vec![];
        if self.executed {
            str_joiner.push(format!("{}", self.elapsed_time));
            str_joiner.push(format!("{}", self.num_out_tuples));
            str_joiner.push(format!("{}", self.num_intermediate_tuples));
            str_joiner.push(format!("{}", self.icost));
        }
        for operator_metric in &self.operator_metrics {
            str_joiner.push(format!("{}", operator_metric.0)); /* operator name */
            if self.executed {
                if !operator_metric.0.contains("PROBE")
                    && !operator_metric.0.contains("HASH")
                    && !operator_metric.0.contains("SCAN")
                {
                    str_joiner.push(format!("{}", operator_metric.1)); /* i-cost */
                }
                if !operator_metric.0.contains("HASH") {
                    str_joiner.push(format!("{}", operator_metric.2)); /* num out tuples */
                }
            }
        }
        str_joiner.join(",")
    }

    pub fn set_stats(&mut self) {
        for subplan in &self.subplans {
            let mut first_op = subplan.clone();
            loop {
                let prev = {
                    let first_op_ref = first_op.borrow();
                    get_op_attr_as_ref!(first_op_ref.deref(), prev)
                        .as_ref()
                        .map(|op| op.clone())
                };
                if prev.is_none() {
                    break;
                }
                first_op = prev.as_ref().unwrap().clone();
            }
            first_op
                .borrow()
                .get_operator_metrics_next_operators(&mut self.operator_metrics);
        }
        for i in 0..self.operator_metrics.len() - 1 {
            self.icost += self.operator_metrics[i].1;
            self.num_intermediate_tuples += self.operator_metrics[i].2;
        }
        self.icost += self.operator_metrics[self.operator_metrics.len() - 1].1;
    }

    pub fn copy(&self, is_thread_safe: bool) -> QueryPlan<Id> {
        let mut subplans = vec![];
        for subplan in &self.subplans {
            subplans.push(Rc::new(RefCell::new(
                subplan.borrow().deref().copy(is_thread_safe),
            )));
        }
        QueryPlan::new_from_subplans(subplans)
    }

    pub fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        let plan_size = self.subplans.len();
        let last_operator = self.subplans[plan_size - 1].clone();
        let query_graph = {
            let last_operator = last_operator.borrow();
            get_op_attr_as_ref!(last_operator.deref(), out_subgraph).clone()
        };
        let mut sink = Rc::new(RefCell::new(Operator::Sink(match self.sink_type {
            SinkType::Copy => Sink::SinkCopy(SinkCopy::new(
                query_graph.clone(),
                get_op_attr!(last_operator.borrow().deref(), out_tuple_len),
            )),
            SinkType::Print => Sink::SinkPrint(SinkPrint::new(query_graph.clone())),
            SinkType::Limit => {
                Sink::SinkLimit(SinkLimit::new(query_graph.clone(), self.out_tuples_limit))
            }
            SinkType::Counter => Sink::BaseSink(BaseSink::new(query_graph.clone())),
        })));
        get_op_attr_as_mut!(sink.borrow_mut().deref_mut(), prev).replace(last_operator.clone());
        *get_op_attr_as_mut!(last_operator.borrow_mut().deref_mut(), next) = vec![sink.clone()];
        self.sink = Some(sink);
        let mut probes = vec![];
        for i in 1..self.subplans.len() {
            let mut operator = self.subplans[i].clone();
            loop {
                {
                    let mut op_ref = operator.borrow();
                    if let Operator::Probe(pb) = op_ref.deref() {
                        probes.push(operator.clone());
                    }
                }
                let prev = {
                    let mut op_ref = operator.borrow();
                    get_op_attr_as_ref!(op_ref.deref(), prev)
                        .as_ref()
                        .map(|op| op.clone())
                };
                if prev.is_none() {
                    break;
                }
                operator = prev.unwrap();
            }
        }
        for i in 0..self.subplans.len() - 1 {
            let subplan = self.subplans.get_mut(i).unwrap();
            if let Operator::Build(build) = subplan.borrow_mut().deref_mut() {
                let hash_table = Rc::new(RefCell::new(HashTable::new(
                    build.build_hash_idx,
                    build.hashed_tuple_len,
                )));
                build.hash_table = Some(hash_table.clone());
                for probe in &probes {
                    let mut probe_mut = probe.borrow_mut();
                    if get_op_attr_as_ref!(probe_mut.deref(), in_subgraph)
                        .as_ref()
                        .unwrap()
                        == build.probing_subgraph.as_ref().unwrap()
                    {
                        if let Operator::Probe(pb) = probe_mut.deref_mut() {
                            let mut base_probe = get_probe_as_mut!(pb);
                            base_probe.hash_tables = vec![hash_table.clone()];
                            break;
                        }
                    }
                }
            }
        }

        for subplan in &mut self.subplans {
            let probe_tuple = Rc::new(RefCell::new(vec![
                Id::new(0);
                get_op_attr!(
                    subplan.borrow().deref(),
                    out_tuple_len
                )
            ]));
            let mut first_op = subplan.clone();
            loop {
                let prev = get_op_attr_as_ref!(first_op.borrow().deref(), prev)
                    .as_ref()
                    .map(|op| op.clone());
                if prev.is_none() {
                    break;
                }
                first_op = prev.as_ref().unwrap().clone();
            }
            first_op.borrow_mut().init(probe_tuple, graph);
        }
    }

    fn init_hashtable(
        &mut self,
        build_insubgrpah: &QueryGraph,
        hash_table: Rc<RefCell<HashTable<Id>>>,
    ) {
        for operator in &mut self.subplans {
            let mut op_mut = operator.borrow_mut();
            if let Operator::Probe(_p) = op_mut.deref_mut() {
                if Self::check_and_init(build_insubgrpah, op_mut.deref_mut(), hash_table.clone()) {
                    break;
                }
            }
            let mut op = operator.clone();
            loop {
                let prev = {
                    let op_ref = op.borrow();
                    get_op_attr_as_ref!(op_ref.deref(), prev)
                        .as_ref()
                        .map(|op| op.clone())
                };
                if prev.is_none() {
                    break;
                }
                op = prev.as_ref().unwrap().clone();
                let mut op_mut = op.borrow_mut();
                if let Operator::Probe(p) = op_mut.deref_mut() {
                    if Self::check_and_init(
                        build_insubgrpah,
                        op_mut.deref_mut(),
                        hash_table.clone(),
                    ) {
                        return;
                    }
                }
            }
        }
    }

    fn check_and_init(
        build_insubgrpah: &QueryGraph,
        probe: &mut Operator<Id>,
        hash_table: Rc<RefCell<HashTable<Id>>>,
    ) -> bool {
        let prob_insubgraph = get_op_attr_as_ref!(probe, in_subgraph).as_ref().unwrap();
        if prob_insubgraph == build_insubgrpah {
            if let Operator::Probe(probe_op) = probe {
                let mut base_probe = get_probe_as_mut!(probe_op);
                base_probe.hash_tables = vec![hash_table.clone()];
                return true;
            }
        }
        false
    }

    pub fn execute(&mut self) {
        if let SinkType::Limit = self.sink_type {
            if let Operator::Sink(Sink::SinkLimit(sink)) =
                self.sink.as_mut().unwrap().borrow_mut().deref_mut()
            {
                sink.start_time = SystemTime::now();
                self.subplans
                    .iter_mut()
                    .map(|plan| plan.borrow_mut())
                    .for_each(|mut plan| plan.execute());
                self.elapsed_time = sink.elapsed_time;
            }
        } else {
            let start_time = SystemTime::now();
            self.subplans.iter_mut().for_each(|mut plan| {
                let mut op = plan.as_ptr();
                unsafe {
                    (&mut *op).execute();
                }
            });
            self.elapsed_time = SystemTime::now()
                .duration_since(start_time)
                .unwrap()
                .as_millis();
        }
        self.executed = true;
        self.num_out_tuples = self
            .sink
            .as_ref()
            .unwrap()
            .borrow()
            .deref()
            .get_num_out_tuples();
    }
}
