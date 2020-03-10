use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::multi_graph::plan::operator::sink::sink::{BaseSink, Sink};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::Deref;
use std::rc::Rc;

#[derive(Clone)]
pub struct SinkPrint<Id: IdType> {
    pub base_sink: BaseSink<Id>,
}

impl<Id: IdType> SinkPrint<Id> {
    pub fn new(query_graph: QueryGraph) -> SinkPrint<Id> {
        SinkPrint {
            base_sink: BaseSink::new(query_graph),
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for SinkPrint<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_sink.init(probe_tuple, graph)
    }

    fn process_new_tuple(&mut self) {
        println!("{:?}", self.base_sink.base_op.probe_tuple);
    }

    fn execute(&mut self) {
        self.base_sink.execute()
    }

    fn get_alds_as_string(&self) -> String {
        self.base_sink.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_sink
            .update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let base_op = &self.base_sink.base_op;
        let mut sink = SinkPrint::new(base_op.out_subgraph.clone());
        let origin_prev = base_op.prev.as_ref().unwrap();
        sink.base_sink.base_op.prev = Some(Rc::new(RefCell::new(
            origin_prev.borrow().deref().copy(is_thread_safe),
        )));
        Operator::Sink(Sink::SinkPrint(sink))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        self.base_sink.is_same_as(op)
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_sink.get_num_out_tuples()
    }
}
