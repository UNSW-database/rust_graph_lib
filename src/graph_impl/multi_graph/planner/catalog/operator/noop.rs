use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;

#[derive(Clone)]
pub struct Noop<Id: IdType> {
    pub base_op: BaseOperator<Id>,
}

impl<Id: IdType> Noop<Id> {
    pub fn new(query_graph: QueryGraph) -> Noop<Id> {
        Noop {
            base_op: BaseOperator::new(query_graph.clone(), Some(query_graph)),
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Noop<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_op.probe_tuple = probe_tuple.clone();
        for next_op in &self.base_op.next {
            next_op.borrow_mut().init(probe_tuple.clone(), graph);
        }
    }

    fn process_new_tuple(&mut self) {
        self.base_op.num_out_tuples += 1;
        self.base_op.next.iter().for_each(|next_op| {
            next_op.borrow_mut().process_new_tuple();
        });
    }

    fn execute(&mut self) {
        self.base_op.execute()
    }

    fn get_alds_as_string(&self) -> String {
        self.base_op.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_op.update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        self.base_op.copy(is_thread_safe)
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        self.base_op.is_same_as(op)
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_op.get_num_out_tuples()
    }
}
