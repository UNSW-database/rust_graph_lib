use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::operator::{BaseOperator, OpType, Operator};
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};
use std::ops::Deref;
use std::rc::Rc;

pub enum SinkType {
    Copy,
    Print,
    Limit,
    Counter,
}

pub struct Sink {
    pub base_op: BaseOperator,
    pub previous: Option<Vec<BaseOperator>>,
}

impl Sink {
    pub fn new(query_graph: Rc<QueryGraph>) -> Self {
        Self {
            base_op: BaseOperator::new(query_graph.clone(), Some(query_graph)),
            previous: None,
        }
    }
    pub fn copy(&self, is_thread_safe: bool) -> Sink {
        let mut sink = Sink::new(self.base_op.out_subgraph.clone());
        if let Some(prev) = &self.base_op.prev {
            sink.base_op.prev = prev.copy(is_thread_safe);
        } else {
            sink.base_op.prev = None;
        }
        sink
    }
}

impl Operator for Sink {
    fn get_op(&self) -> BaseOperator {
        self.base_op.clone()
    }

    fn get_name(&self) -> String {
        unimplemented!()
    }

    fn get_icost(&self) -> usize {
        unimplemented!()
    }

    fn get_type(&self) -> OpType {
        unimplemented!()
    }

    fn get_num_out_tuples(&self) -> usize {
        if let Some(prev) = &self.previous {
            prev.iter().map(|op| op.get_num_out_tuples()).sum()
        } else {
            self.base_op.get_num_out_tuples()
        }
    }

    fn get_query_vertex_id_map(&self) -> &HashMap<String, usize> {
        unimplemented!()
    }

    fn init<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<usize>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_op.probe_tuple = probe_tuple;
    }

    fn is_same_as(&self, op: &BaseOperator) -> bool {
        if let OpType::Sink = op.get_type() {
            if let Some(prev) = self.get_prev() {
                return prev.is_same_as(op.get_prev().unwrap());
            }
        }
        false
    }

    fn get_next_vec(&self) -> &Option<Vec<BaseOperator>> {
        unimplemented!()
    }

    fn get_next_vec_as_mut(&mut self) -> &mut Option<Vec<BaseOperator>> {
        unimplemented!()
    }

    fn get_prev(&self) -> Option<&Rc<BaseOperator>> {
        unimplemented!()
    }

    fn set_next_batch(&mut self, ops: Vec<BaseOperator>) {
        unimplemented!()
    }

    fn process_new_tuple() {}

    fn execute(&self) {
        if let Some(prev) = &self.previous {
            prev[0].execute();
        } else {
            if let Some(prev) = self.base_op.get_prev() {
                prev.execute();
            }
        }
    }

    fn update_operator_name(query_vertex_to_index_map: HashMap<String, usize>) {
        unimplemented!()
    }
}
