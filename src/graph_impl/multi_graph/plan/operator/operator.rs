use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::BaseIntersect;
use graph_impl::multi_graph::plan::operator::extend::intersect::Intersect;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::build::Build;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::multi_graph::plan::operator::scan::scan_sampling::ScanSampling;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::planner::catalog::operator::noop::Noop;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::{HashMap, HashSet};
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::Deref;
use std::rc::Rc;

/// Operator types
#[derive(Clone)]
pub enum Operator<Id: IdType> {
    Base(BaseOperator<Id>),
    Sink(Sink<Id>),
    Scan(Scan<Id>),
    EI(EI<Id>),
    Build(Build<Id>),
    Probe(Probe<Id>),
    Noop(Noop<Id>),
}

/// Basic operator
#[derive(Clone)]
pub struct BaseOperator<Id: IdType> {
    pub name: String,
    pub next: Vec<Rc<RefCell<Operator<Id>>>>,
    pub prev: Option<Rc<RefCell<Operator<Id>>>>,
    pub probe_tuple: Vec<Id>,
    pub out_tuple_len: usize,
    pub in_subgraph: Option<QueryGraph>,
    pub out_subgraph: QueryGraph,
    pub out_qvertex_to_idx_map: HashMap<String, usize>,
    pub last_repeated_vertex_idx: usize,
    pub num_out_tuples: usize,
    pub icost: usize,
}

impl<Id: IdType> BaseOperator<Id> {
    pub fn new(out_subgraph: QueryGraph, in_subgraph: Option<QueryGraph>) -> BaseOperator<Id> {
        BaseOperator {
            name: "".to_string(),
            next: vec![],
            prev: None,
            probe_tuple: vec![],
            out_tuple_len: out_subgraph.get_num_qvertices(),
            in_subgraph,
            out_subgraph,
            out_qvertex_to_idx_map: HashMap::new(),
            last_repeated_vertex_idx: 0,
            num_out_tuples: 0,
            icost: 0,
        }
    }

    pub fn empty() -> BaseOperator<Id> {
        BaseOperator {
            name: "".to_string(),
            next: vec![],
            prev: None,
            probe_tuple: vec![],
            out_tuple_len: 0,
            in_subgraph: None,
            out_subgraph: QueryGraph::empty(),
            out_qvertex_to_idx_map: HashMap::new(),
            last_repeated_vertex_idx: 0,
            num_out_tuples: 0,
            icost: 0,
        }
    }
}

/// Common operations for every kind of operator
pub trait CommonOperatorTrait<Id: IdType> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    );
    fn process_new_tuple(&mut self);
    fn execute(&mut self);
    fn get_alds_as_string(&self) -> String;
    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>);
    fn copy(&self, is_thread_safe: bool) -> Operator<Id>;
    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool;
    fn get_num_out_tuples(&self) -> usize;
}

impl<Id: IdType> Operator<Id> {
    pub fn get_last_operators(&self, last_operators: &mut Vec<Rc<RefCell<Operator<Id>>>>) {
        let next = get_op_attr_as_ref!(self, next);
        if next.is_empty() {
            return;
        }
        for op in next {
            let next = op.borrow();
            if get_op_attr_as_ref!(next.deref(), next).is_empty() {
                last_operators.push(op.clone());
                continue;
            }
            op.borrow().get_last_operators(last_operators);
        }
    }

    pub fn get_operator_metrics_next_operators(
        &self,
        operator_metrics: &mut Vec<(String, usize, usize)>,
    ) {
        let name: &String = get_op_attr_as_ref!(self, name);
        let icost = get_op_attr!(self, icost);
        let num_out_tuples = get_op_attr!(self, num_out_tuples);
        operator_metrics.push((name.clone(), icost, num_out_tuples));
        get_op_attr_as_ref!(self, next)
            .iter()
            .map(|op| op.borrow())
            .for_each(|op| match op.deref() {
                Operator::Sink(_) => {}
                _ => op.get_operator_metrics_next_operators(operator_metrics),
            });
    }

    pub fn has_multi_edge_extends(&self) -> bool {
        match self {
            Operator::EI(ei) => ei.has_multi_edge_extends(),
            _ => {
                if let Some(prev) = get_op_attr_as_ref!(self, prev) {
                    return prev.borrow().deref().has_multi_edge_extends();
                }
                false
            }
        }
    }

    pub fn get_out_query_vertices(&self) -> HashSet<String> {
        let idx_map = get_op_attr_as_ref!(self, out_qvertex_to_idx_map);
        idx_map.iter().map(|(key, _val)| key.clone()).collect()
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for BaseOperator<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        panic!("unsupported operation exception")
    }

    fn process_new_tuple(&mut self) {
        panic!("unsupported operation exception")
    }

    fn execute(&mut self) {
        if let Some(prev) = self.prev.as_mut() {
            prev.borrow_mut().execute();
        }
    }

    fn get_alds_as_string(&self) -> String {
        String::from("")
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        panic!("`update_operator_name()` on neither `EI` or `Scan`")
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        panic!("unsupported operation exception")
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        panic!("unsupported operation exception")
    }

    fn get_num_out_tuples(&self) -> usize {
        self.num_out_tuples
    }
}

/// Abstract methods
impl<Id: IdType> CommonOperatorTrait<Id> for Operator<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self {
            Operator::Base(base) => base.init(probe_tuple, graph),
            Operator::Sink(sink) => sink.init(probe_tuple, graph),
            Operator::Scan(scan) => scan.init(probe_tuple, graph),
            Operator::EI(ei) => ei.init(probe_tuple, graph),
            Operator::Build(build) => build.init(probe_tuple, graph),
            Operator::Probe(probe) => probe.init(probe_tuple, graph),
            Operator::Noop(noop) => noop.init(probe_tuple, graph),
        }
    }

    fn process_new_tuple(&mut self) {
        match self {
            Operator::Base(base) => base.process_new_tuple(),
            Operator::Sink(sink) => sink.process_new_tuple(),
            Operator::Scan(scan) => scan.process_new_tuple(),
            Operator::EI(ei) => ei.process_new_tuple(),
            Operator::Build(build) => build.process_new_tuple(),
            Operator::Probe(probe) => probe.process_new_tuple(),
            Operator::Noop(noop) => noop.process_new_tuple(),
        }
    }

    fn execute(&mut self) {
        match self {
            Operator::Base(base) => base.execute(),
            Operator::Sink(sink) => sink.execute(),
            Operator::Scan(scan) => scan.execute(),
            Operator::EI(ei) => ei.execute(),
            Operator::Build(build) => build.execute(),
            Operator::Probe(probe) => probe.execute(),
            Operator::Noop(noop) => noop.execute(),
        }
    }

    fn get_alds_as_string(&self) -> String {
        match self {
            Operator::Base(base) => base.get_alds_as_string(),
            Operator::Sink(sink) => sink.get_alds_as_string(),
            Operator::Scan(scan) => scan.get_alds_as_string(),
            Operator::EI(ei) => ei.get_alds_as_string(),
            Operator::Build(build) => build.get_alds_as_string(),
            Operator::Probe(probe) => probe.get_alds_as_string(),
            Operator::Noop(noop) => noop.get_alds_as_string(),
        }
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        match self {
            Operator::Base(base) => base.update_operator_name(query_vertex_to_index_map),
            Operator::Sink(sink) => sink.update_operator_name(query_vertex_to_index_map),
            Operator::Scan(scan) => scan.update_operator_name(query_vertex_to_index_map),
            Operator::EI(ei) => ei.update_operator_name(query_vertex_to_index_map),
            Operator::Build(build) => build.update_operator_name(query_vertex_to_index_map),
            Operator::Probe(probe) => probe.update_operator_name(query_vertex_to_index_map),
            Operator::Noop(noop) => noop.update_operator_name(query_vertex_to_index_map),
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        match self {
            Operator::Base(base) => base.copy(is_thread_safe),
            Operator::Sink(sink) => sink.copy(is_thread_safe),
            Operator::Scan(scan) => scan.copy(is_thread_safe),
            Operator::EI(ei) => ei.copy(is_thread_safe),
            Operator::Build(build) => build.copy(is_thread_safe),
            Operator::Probe(probe) => probe.copy(is_thread_safe),
            Operator::Noop(noop) => noop.copy(is_thread_safe),
        }
    }
    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        match self {
            Operator::Base(base) => base.is_same_as(op),
            Operator::Sink(sink) => sink.is_same_as(op),
            Operator::Scan(scan) => scan.is_same_as(op),
            Operator::EI(ei) => ei.is_same_as(op),
            Operator::Build(build) => build.is_same_as(op),
            Operator::Probe(probe) => probe.is_same_as(op),
            Operator::Noop(noop) => noop.is_same_as(op),
        }
    }

    fn get_num_out_tuples(&self) -> usize {
        match self {
            Operator::Base(base) => base.get_num_out_tuples(),
            Operator::Sink(sink) => sink.get_num_out_tuples(),
            Operator::Scan(scan) => scan.get_num_out_tuples(),
            Operator::EI(ei) => ei.get_num_out_tuples(),
            Operator::Build(build) => build.get_num_out_tuples(),
            Operator::Probe(probe) => probe.get_num_out_tuples(),
            Operator::Noop(noop) => noop.get_num_out_tuples(),
        }
    }
}
