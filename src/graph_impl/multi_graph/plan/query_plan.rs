use generic::IdType;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::{BaseOperator, Operator};
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::multi_graph::plan::operator::scan::scan_sampling::ScanSampling;
use graph_impl::multi_graph::plan::operator::sink::sink::{BaseSink, Sink, SinkType};
use hashbrown::HashMap;
use std::ops::Deref;
use std::rc::Rc;

#[derive(Clone)]
pub struct QueryPlan<Id: IdType> {
    sink: Option<Sink<Id>>,
    pub sink_type: SinkType,
    scan_sampling: Option<ScanSampling<Id>>,
    pub last_operator: Option<Box<Operator<Id>>>,
    pub out_tuples_limit: usize,
    elapsed_time: f64,
    icost: usize,
    num_intermediate_tuples: usize,
    num_out_tuples: usize,
    operator_metrics: Vec<(String, usize, usize)>,
    executed: bool,
    adaptive_enabled: bool,
    pub subplans: Vec<Box<Operator<Id>>>,
    pub estimated_icost: f64,
    pub estimated_num_out_tuples: f64,
    pub q_vertex_to_num_out_tuples: HashMap<String, f64>,
}

impl<Id: IdType> QueryPlan<Id> {
    pub fn new(scan_sampling: ScanSampling<Id>) -> Self {
        let mut last_operators = Vec::new();
        let scan_sampling_op = Operator::Scan(Scan::ScanSampling(scan_sampling.clone()));
        scan_sampling_op.get_last_operators(&mut last_operators);
        let op = &last_operators[0];
        let out_subgraph = Box::new(get_op_attr_as_ref!(op, out_subgraph).as_ref().clone());
        let mut sink = BaseSink::new(out_subgraph);
        for op in last_operators.iter_mut() {
            let next = get_op_attr_as_mut!(op, next);
            next.replace(vec![Operator::Sink(Sink::BaseSink(sink.clone()))]);
        }
        sink.previous = Some(last_operators);
        Self {
            sink: Some(Sink::BaseSink(sink)),
            sink_type: SinkType::Counter,
            scan_sampling: Some(scan_sampling),
            last_operator: None,
            out_tuples_limit: 0,
            elapsed_time: 0.0,
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
    pub fn new_from_operator(last_operator: Box<Operator<Id>>) -> Self {
        Self {
            sink: None,
            sink_type: SinkType::Counter,
            scan_sampling: None,
            last_operator: Some(last_operator.clone()),
            out_tuples_limit: 0,
            elapsed_time: 0.0,
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
    pub fn new_from_subplans(subplans: Vec<Box<Operator<Id>>>) -> Self {
        Self {
            sink: None,
            sink_type: SinkType::Copy,
            scan_sampling: None,
            last_operator: subplans.get(subplans.len() - 1).map(|x| x.clone()),
            out_tuples_limit: 0,
            elapsed_time: 0.0,
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
        let mut plan = QueryPlan::new_from_operator(Box::new(Operator::Scan(last_operator)));
        plan.estimated_num_out_tuples = estimated_num_out_tuples;
        map.into_iter().for_each(|(k, v)| {
            plan.q_vertex_to_num_out_tuples.insert(k, v);
        });
        plan
    }

    pub fn get_scan_sampling(&mut self) -> Option<&mut ScanSampling<Id>> {
        self.scan_sampling.as_mut()
    }

    pub fn get_sink(&mut self) -> &mut Sink<Id> {
        self.sink.as_mut().unwrap()
    }

    pub fn get_sink_as_ref(&self) -> &Sink<Id> {
        self.sink.as_ref().unwrap()
    }

    pub fn shallow_copy(&self) -> QueryPlan<Id> {
        QueryPlan::new_from_subplans(self.subplans.clone())
    }

    pub fn append(&mut self, mut new_operator: Operator<Id>) {
        let mut last_operator = self.last_operator.as_mut().unwrap().as_mut();
        let last_op = get_base_op_as_mut!(&mut last_operator);
        last_op.next = Some(vec![new_operator.clone()]);
        let new_op = get_base_op_as_mut!(&mut new_operator);
        new_op.prev = self.last_operator.clone();
        self.subplans.push(Box::new(new_operator.clone()));
        self.last_operator = Some(Box::new(new_operator));
    }
}
