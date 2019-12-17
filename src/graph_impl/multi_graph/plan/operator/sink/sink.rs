use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use::graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use std::hash::{BuildHasherDefault, Hash};
use std::rc::Rc;

pub enum SinkType {
    Copy,
    Print,
    Limit,
    Counter,
}

#[derive(Clone)]
pub struct Sink<Id: IdType> {
    pub base_op: BaseOperator<Id>,
    pub previous: Option<Vec<Operator<Id>>>,
}

impl<Id: IdType> Sink<Id> {
    pub fn new(query_graph: Box<QueryGraph>) -> Self {
        Self {
            base_op: BaseOperator::new(query_graph.clone(), Some(query_graph)),
            previous: None,
        }
    }

    fn get_num_out_tuples(&self) -> usize {
        if let Some(prev) = &self.previous {
            prev.iter()
                .map(|op| get_op_attr_as_ref!(op, num_out_tuples))
                .sum()
        } else {
            self.base_op.num_out_tuples
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Sink<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_op.probe_tuple = probe_tuple;
    }

    fn process_new_tuple(&mut self) {}

    fn execute(&mut self) {
        if let Some(prev) = self.previous.as_mut() {
            prev[0].execute();
        } else {
            self.base_op.prev.as_mut().unwrap().execute();
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Option<Operator<Id>> {
        let mut sink = Sink::new(self.base_op.out_subgraph.clone());
        if let Some(prev) = &self.base_op.prev {
            sink.base_op.prev = Some(Box::new(prev.copy(is_thread_safe).unwrap()));
        } else {
            sink.base_op.prev = None;
        }
        Some(Operator::Sink(sink))
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        if let Operator::Sink(sink) = op {
            if let Some(prev) = &mut self.base_op.prev {
                let op_prev = get_op_attr_as_mut!(op, prev).as_mut().unwrap();
                return prev.is_same_as(op_prev.as_mut());
            }
        }
        false
    }
}
