use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::sink::sink::{BaseSink, Sink};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};
use std::time::SystemTime;

#[derive(Clone)]
pub struct SinkLimit<Id: IdType> {
    pub base_sink: BaseSink<Id>,
    pub start_time: SystemTime,
    pub elapsed_time: u128,
    out_tuples_limit: usize,
}

impl<Id: IdType> SinkLimit<Id> {
    pub fn new(query_graph: Box<QueryGraph>, out_tuple_limit: usize) -> SinkLimit<Id> {
        SinkLimit {
            base_sink: BaseSink::new(query_graph),
            start_time: SystemTime::now(),
            elapsed_time: 0,
            out_tuples_limit: out_tuple_limit,
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for SinkLimit<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_sink.init(probe_tuple, graph)
    }

    fn process_new_tuple(&mut self) {
        let prev = self.base_sink.base_op.prev.as_ref().unwrap().as_ref();
        if get_op_attr!(prev, num_out_tuples) >= self.out_tuples_limit {
            self.elapsed_time = SystemTime::now()
                .duration_since(self.start_time.clone())
                .unwrap()
                .as_millis();
        }
    }

    fn execute(&mut self) {
        self.base_sink.execute();
    }

    fn get_alds_as_string(&self) -> String {
        self.base_sink.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_sink
            .update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        self.base_sink.copy(is_thread_safe)
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        self.base_sink.is_same_as(op)
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_sink.get_num_out_tuples()
    }
}
