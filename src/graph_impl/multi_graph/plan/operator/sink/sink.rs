use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::BaseIntersect;
use graph_impl::multi_graph::plan::operator::extend::intersect::Intersect;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
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

#[derive(Clone)]
pub enum SinkType {
    Copy,
    Print,
    Limit,
    Counter,
}

#[derive(Clone)]
pub enum Sink<Id: IdType> {
    BaseSink(BaseSink<Id>),
    SinkCopy(SinkCopy<Id>),
    SinkPrint(SinkPrint<Id>),
    SinkLimit(SinkLimit<Id>),
}

#[derive(Clone)]
pub struct BaseSink<Id: IdType> {
    pub base_op: BaseOperator<Id>,
    pub previous: Vec<Rc<RefCell<Operator<Id>>>>,
}

impl<Id: IdType> BaseSink<Id> {
    pub fn new(query_graph: QueryGraph) -> Self {
        Self {
            base_op: BaseOperator::new(query_graph.clone(), Some(query_graph)),
            previous: vec![],
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for BaseSink<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_op.probe_tuple = probe_tuple;
    }

    fn process_new_tuple(&mut self) {}

    fn execute(&mut self) {
        if !self.previous.is_empty() {
            let mut prev = self.previous[0].as_ptr();
            unsafe {
                (&mut *prev).execute();
            }
        } else {
            self.base_op.prev.as_mut().unwrap().borrow_mut().execute();
        }
    }

    fn get_alds_as_string(&self) -> String {
        self.base_op.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_op.update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let mut sink = BaseSink::new(self.base_op.out_subgraph.clone());
        if let Some(prev) = &self.base_op.prev {
            sink.base_op.prev = Some(Rc::new(RefCell::new(
                prev.borrow().deref().copy(is_thread_safe),
            )));
        } else {
            sink.base_op.prev = None;
        }
        Operator::Sink(Sink::BaseSink(sink))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        if let Operator::Sink(sink) = op.borrow().deref() {
            if let Some(prev) = &mut self.base_op.prev {
                let mut op = op.borrow_mut();
                let op_prev = get_op_attr_as_mut!(op.deref_mut(), prev).as_mut().unwrap();
                return prev.borrow_mut().is_same_as(op_prev);
            }
        }
        false
    }

    fn get_num_out_tuples(&self) -> usize {
        if !self.previous.is_empty() {
            self.previous
                .iter()
                .map(|op| op.borrow())
                .map(|op| get_op_attr!(op.deref(), num_out_tuples))
                .sum()
        } else {
            self.base_op.num_out_tuples
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Sink<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self {
            Sink::BaseSink(base) => base.init(probe_tuple, graph),
            Sink::SinkCopy(sc) => sc.init(probe_tuple, graph),
            Sink::SinkPrint(sp) => sp.init(probe_tuple, graph),
            Sink::SinkLimit(sl) => sl.init(probe_tuple, graph),
        }
    }

    fn process_new_tuple(&mut self) {
        match self {
            Sink::BaseSink(base) => base.process_new_tuple(),
            Sink::SinkCopy(sc) => sc.process_new_tuple(),
            Sink::SinkPrint(sp) => sp.process_new_tuple(),
            Sink::SinkLimit(sl) => sl.process_new_tuple(),
        }
    }

    fn execute(&mut self) {
        match self {
            Sink::BaseSink(base) => base.execute(),
            Sink::SinkCopy(sc) => sc.execute(),
            Sink::SinkPrint(sp) => sp.execute(),
            Sink::SinkLimit(sl) => sl.execute(),
        }
    }

    fn get_alds_as_string(&self) -> String {
        match self {
            Sink::BaseSink(base) => base.get_alds_as_string(),
            Sink::SinkCopy(sc) => sc.get_alds_as_string(),
            Sink::SinkPrint(sp) => sp.get_alds_as_string(),
            Sink::SinkLimit(sl) => sl.get_alds_as_string(),
        }
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        match self {
            Sink::BaseSink(base) => base.update_operator_name(query_vertex_to_index_map),
            Sink::SinkCopy(sc) => sc.update_operator_name(query_vertex_to_index_map),
            Sink::SinkPrint(sp) => sp.update_operator_name(query_vertex_to_index_map),
            Sink::SinkLimit(sl) => sl.update_operator_name(query_vertex_to_index_map),
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        match self {
            Sink::BaseSink(base) => base.copy(is_thread_safe),
            Sink::SinkCopy(sc) => sc.copy(is_thread_safe),
            Sink::SinkPrint(sp) => sp.copy(is_thread_safe),
            Sink::SinkLimit(sl) => sl.copy(is_thread_safe),
        }
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        match self {
            Sink::BaseSink(base) => base.is_same_as(op),
            Sink::SinkCopy(sc) => sc.is_same_as(op),
            Sink::SinkPrint(sp) => sp.is_same_as(op),
            Sink::SinkLimit(sl) => sl.is_same_as(op),
        }
    }

    fn get_num_out_tuples(&self) -> usize {
        match self {
            Sink::BaseSink(base) => base.get_num_out_tuples(),
            Sink::SinkCopy(sc) => sc.get_num_out_tuples(),
            Sink::SinkPrint(sp) => sp.get_num_out_tuples(),
            Sink::SinkLimit(sl) => sl.get_num_out_tuples(),
        }
    }
}
