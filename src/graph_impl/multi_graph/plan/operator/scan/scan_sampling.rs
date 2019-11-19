use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::operator::{BaseOperator, OpType, Operator};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};
use std::rc::Rc;

//TODO: Fixing ScanSampling
pub struct ScanSampling {
    pub scan: Scan,
    pub edges_queue: Vec<Vec<usize>>,
}

impl ScanSampling {
    pub fn new(out_subgraph: Rc<QueryGraph>) -> Self {
        Self {
            scan: Scan::new(out_subgraph),
            edges_queue: vec![],
        }
    }

    pub fn get_last_operator() {}
}

impl Operator for ScanSampling {
    fn get_op(&self) -> BaseOperator {
        unimplemented!()
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
        unimplemented!()
    }

    fn get_query_vertex_id_map(&self) -> &HashMap<String, usize> {
        unimplemented!()
    }

    fn init<Id: IdType, NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<usize>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        unimplemented!()
    }

    fn is_same_as(&self, op: &BaseOperator) -> bool {
        unimplemented!()
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

    fn process_new_tuple() {
        unimplemented!()
    }

    fn update_operator_name(query_vertex_to_index_map: HashMap<String, usize>) {
        unimplemented!()
    }
}
