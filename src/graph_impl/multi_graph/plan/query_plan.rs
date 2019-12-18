use graph_impl::multi_graph::plan::operator::operator::{BaseOperator, Operator};
use graph_impl::multi_graph::plan::operator::scan::scan_sampling::ScanSampling;
use graph_impl::multi_graph::plan::operator::sink::sink::{Sink, SinkType, BaseSink};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use hashbrown::HashMap;
use std::ops::Deref;
use std::rc::Rc;
use generic::IdType;

pub struct QueryPlan<Id: IdType> {
    sink: Option<Sink<Id>>,
    sink_type: SinkType,
    scan_sampling: Option<ScanSampling<Id>>,
    last_operator: Option<Rc<Operator<Id>>>,
    out_tuples_limit: usize,
    elapsed_time: f64,
    icost: usize,
    num_intermediate_tuples: usize,
    num_out_tuples: usize,
    operator_metrics: Vec<(String, usize, usize)>,
    executed: bool,
    adaptive_enabled: bool,
    subplans: Vec<Rc<Operator<Id>>>,
    estimated_icost: f64,
    estimated_num_out_tuples: f64,
    q_vertex_to_num_out_tuples: HashMap<String, f64>,
}

impl <Id: IdType>QueryPlan<Id> {
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
    pub fn new_from_operator(last_operator: Rc<Operator<Id>>) -> Self {
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
    pub fn get_scan_sampling(&mut self) -> Option<&mut ScanSampling<Id>> {
        self.scan_sampling.as_mut()
    }

    pub fn get_sink(&mut self) -> &mut Sink<Id> {
        self.sink.as_mut().unwrap()
    }

    pub fn get_sink_as_ref(&self) -> &Sink<Id> {
        self.sink.as_ref().unwrap()
    }
}
