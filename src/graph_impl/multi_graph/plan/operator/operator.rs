use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::scan::scan_sampling::ScanSampling;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::TypedStaticGraph;
use hashbrown::{HashMap, HashSet};
use std::hash::{BuildHasherDefault, Hash};
use std::iter::FromIterator;
use std::rc::Rc;

/// Operator types
#[derive(Clone)]
pub enum Operator<Id: IdType> {
    Base(BaseOperator<Id>),
    Sink(Sink<Id>),
    Scan(Scan<Id>),
    ScanSampling(ScanSampling<Id>),
    EI(EI<Id>),
}

/// Basic operator
#[derive(Clone)]
pub struct BaseOperator<Id: IdType> {
    pub name: String,
    pub next: Option<Vec<Operator<Id>>>,
    pub prev: Option<Box<Operator<Id>>>,
    pub probe_tuple: Vec<Id>,
    pub out_tuple_len: usize,
    pub in_subgraph: Option<Box<QueryGraph>>,
    pub out_subgraph: Box<QueryGraph>,
    pub out_qvertex_to_idx_map: HashMap<String, usize>,
    pub last_repeated_vertex_idx: usize,
    pub num_out_tuples: usize,
    pub icost: usize,
}

impl<Id: IdType> BaseOperator<Id> {
    pub fn new(
        out_subgraph: Box<QueryGraph>,
        in_subgraph: Option<Box<QueryGraph>>,
    ) -> BaseOperator<Id> {
        BaseOperator {
            name: "".to_string(),
            next: None,
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

    fn get_out_query_vertices(&self) -> Vec<&String> {
        let idx_map = &self.out_qvertex_to_idx_map;
        idx_map.iter().map(|(key, _val)| key).collect()
    }

    fn get_next(&self, index: usize) -> &Operator<Id> {
        let next = self.next.as_ref().unwrap();
        &next[index]
    }
    fn set_next_vec(&mut self, op: Vec<Operator<Id>>) {
        self.next.replace(op);
    }

    fn execute(&mut self) {
        if let Some(prev) = self.prev.as_mut() {
            prev.execute();
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
    fn get_alds_as_string(&self) -> String {
        "".to_string()
    }
    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        panic!("`update_operator_name()` on `BaseOperator`")
    }
    fn copy(&self, is_thread_safe: bool) -> Option<Operator<Id>>;
    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool;
}

/// Abstract methods
impl<Id: IdType> CommonOperatorTrait<Id> for Operator<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self {
            Operator::Base(base) => {}
            Operator::Sink(sink) => sink.init(probe_tuple, graph),
            Operator::Scan(scan) => scan.init(probe_tuple, graph),
            Operator::ScanSampling(sp) => sp.init(probe_tuple, graph),
            _ => {}
        }
    }

    fn process_new_tuple(&mut self) {
        match self {
            Operator::Base(base) => {}
            Operator::Sink(sink) => sink.process_new_tuple(),
            Operator::Scan(scan) => scan.process_new_tuple(),
            Operator::ScanSampling(sp) => sp.process_new_tuple(),
            _ => {}
        }
    }

    fn execute(&mut self) {
        match self {
            Operator::Base(base) => base.execute(),
            Operator::Sink(sink) => sink.execute(),
            Operator::Scan(scan) => scan.execute(),
            Operator::ScanSampling(sp) => sp.execute(),
            _ => {}
        }
    }
    fn copy(&self, is_thread_safe: bool) -> Option<Operator<Id>> {
        match self {
            Operator::Base(base) => None,
            Operator::Sink(sink) => sink.copy(is_thread_safe),
            Operator::Scan(scan) => scan.copy(is_thread_safe),
            Operator::ScanSampling(sp) => sp.copy(is_thread_safe),
            _ => None
        }
    }
    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        match self {
            Operator::Base(base) => false,
            Operator::Sink(sink) => sink.is_same_as(op),
            Operator::Scan(scan) => scan.is_same_as(op),
            Operator::ScanSampling(sp) => sp.is_same_as(op),
            _ => false
        }
    }
}

impl<Id: IdType> Operator<Id> {
    pub fn get_last_operators(&self, last_operators: &mut Vec<Operator<Id>>) {
        if let Some(next) = get_op_attr_as_ref!(self, next) {
            for op in next {
                op.get_last_operators(last_operators);
            }
        } else {
            last_operators.push(self.clone());
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
        if let Some(next) = get_op_attr_as_ref!(self, next) {
            next.iter().for_each(|op| match op {
                Operator::Sink(_) => {}
                _ => op.get_operator_metrics_next_operators(operator_metrics),
            });
        }
    }

    pub fn has_multi_edge_extends(&self) -> bool {
        match self {
            Operator::EI(ei) => ei.has_multi_edge_extends(),
            _ => {
                if let Some(prev) = get_op_attr_as_ref!(self, prev) {
                    return prev.has_multi_edge_extends();
                }
                false
            }
        }
    }
}
