use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};
use std::rc::Rc;

//TODO: Fixing ScanSampling
#[derive(Clone)]
pub struct ScanSampling<Id: IdType> {
    pub scan: Scan<Id>,
    pub edges_queue: Vec<Vec<Id>>,
}

impl<Id: IdType> ScanSampling<Id> {
    pub fn new(out_subgraph: Box<QueryGraph>) -> Self {
        Self {
            scan: Scan::new(out_subgraph),
            edges_queue: vec![],
        }
    }

    pub fn get_last_operator() {}
}

impl<Id: IdType> CommonOperatorTrait<Id> for ScanSampling<Id> {
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
        while !self.edges_queue.is_empty() {
            let edge = self.edges_queue.pop().unwrap();
            self.scan.base_op.probe_tuple[0] = edge[0];
            self.scan.base_op.probe_tuple[1] = edge[0];
            self.scan.base_op.num_out_tuples += 1;
            for next_op in self.scan.base_op.next.as_mut().unwrap() {
                next_op.process_new_tuple();
            }
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Option<Operator<Id>> {
        let mut scan_sampling = ScanSampling::new(self.scan.base_op.out_subgraph.clone());
        scan_sampling.edges_queue = self.edges_queue.clone();
        Some(Operator::ScanSampling(scan_sampling))
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        unimplemented!()
    }
}
