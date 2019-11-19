use graph_impl::multi_graph::plan::operator::operator::{BaseOperator, Operator};
use graph_impl::multi_graph::plan::operator::scan::scan_sampling::ScanSampling;
use graph_impl::multi_graph::plan::operator::sink::sink::{Sink, SinkType};
use hashbrown::HashMap;
use std::rc::Rc;

pub struct QueryPlan {
    sink: Option<Sink>,
    sink_type: SinkType,
    scan_sampling: Option<ScanSampling>,
    last_operator: Option<Rc<BaseOperator>>,
    out_tuples_limit: usize,
    elapsed_time: f64,
    icost: usize,
    num_intermediate_tuples: usize,
    num_out_tuples: usize,
    operator_metrics: Vec<(String, usize, usize)>,
    executed: bool,
    adaptive_enabled: bool,
    subplans: Vec<Rc<BaseOperator>>,
    estimated_icost: f64,
    estimated_num_out_tuples: f64,
    q_vertex_to_num_out_tuples: HashMap<String, f64>,
}

impl QueryPlan {
    pub fn new(scan_sampling: ScanSampling) -> Self {
        let mut last_operators = Vec::new();
        scan_sampling
            .scan
            .base_op
            .get_last_operators(&mut last_operators);
        let out_subgraph = last_operators.get(0).unwrap().out_subgraph.clone();
        let mut sink = Sink::new(out_subgraph);
        for op in last_operators.iter_mut() {
            op.set_next(sink.base_op.clone())
        }
        sink.previous = Some(last_operators);
        Self {
            sink: Some(sink),
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
    pub fn new_from_operator(last_operator: Rc<BaseOperator>) -> Self {
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
    pub fn get_scan_sampling(&mut self) -> Option<&mut ScanSampling> {
        self.scan_sampling.as_mut()
    }
}
