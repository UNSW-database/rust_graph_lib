use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::static_graph::sorted_adj_vec::SortedAdjVec;
use graph_impl::TypedStaticGraph;
use std::hash::Hash;
use std::rc::Rc;

//TODO: Fixing Scan Operator
#[derive(Clone)]
pub struct Scan<Id: IdType> {
    pub base_op: BaseOperator<Id>,
    from_query_vertex: String,
    to_query_vertex: String,
    from_type: usize,
    to_type: usize,
    label_or_to_type: usize,
    fwd_adj_list: Vec<SortedAdjVec<usize>>,
    vertex_ids: Vec<usize>,
    vertex_types: Vec<usize>,
    from_vertex_start_idx: usize,
    from_vertex_end_idx: usize,
}

impl<Id: IdType> Scan<Id> {
    //TODO: fixing functions
    pub fn new(out_subgraph: Box<QueryGraph>) -> Scan<Id> {
        Scan {
            base_op: BaseOperator::new(out_subgraph, None),
            from_query_vertex: "".to_string(),
            to_query_vertex: "".to_string(),
            from_type: 0,
            to_type: 0,
            label_or_to_type: 0,
            fwd_adj_list: vec![],
            vertex_ids: vec![],
            vertex_types: vec![],
            from_vertex_start_idx: 0,
            from_vertex_end_idx: 0,
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Scan<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        unimplemented!()
    }

    fn process_new_tuple(&mut self) {
        unimplemented!()
    }

    fn execute(&mut self) {
        unimplemented!()
    }

    fn copy(&self, is_thread_safe: bool) -> Option<Operator<Id>> {
        if is_thread_safe {
            //TODO: Fixing Scanblocking
            //            return ScanBlocking::new(outSubgraph);
        }
        Some(Operator::Scan(Scan::new(self.base_op.out_subgraph.clone())))
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        unimplemented!()
    }
}
