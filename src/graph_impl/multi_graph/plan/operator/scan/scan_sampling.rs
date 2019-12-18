use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};
use std::rc::Rc;
use rand::{thread_rng, Rng};

#[derive(Clone)]
pub struct ScanSampling<Id: IdType> {
    pub base_scan: BaseScan<Id>,
    pub edges_queue: Vec<Vec<Id>>,
}

impl<Id: IdType> ScanSampling<Id> {
    pub fn new(out_subgraph: Box<QueryGraph>) -> Self {
        Self {
            base_scan: BaseScan::new(out_subgraph),
            edges_queue: vec![],
        }
    }

    pub fn set_edge_indices_to_sample(&mut self, edges: Vec<Id>, num_edges_to_sample: usize) {
        let mut rng = thread_rng();
        let num_edges = edges.len() / 2;
        while self.edges_queue.len() < num_edges_to_sample {
            let edge_idx = rng.gen_range(0, num_edges);
            self.edges_queue.push(vec![edges[edge_idx], edges[edge_idx + 1]]);
        }
    }

    pub fn set_edge_indices_to_sample_by_edges(&mut self, edges: Vec<Vec<Id>>, num_edges_to_sample: usize) {
        let mut rng = thread_rng();
        self.edges_queue = vec![vec![]; num_edges_to_sample];
        while self.edges_queue.len() < num_edges_to_sample {
            let edge_idx = rng.gen_range(0, edges.len());
            self.edges_queue.push(edges[edge_idx].clone());
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for ScanSampling<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        if self.base_scan.base_op.probe_tuple.is_empty() {
            self.base_scan.base_op.probe_tuple = probe_tuple.clone();
            self.base_scan.base_op.next.as_mut().map(|next| {
                next.iter_mut().for_each(|next_op| {
                    next_op.init(probe_tuple.clone(), graph);
                })
            });
        }
    }

    fn process_new_tuple(&mut self) {
        self.base_scan.process_new_tuple()
    }

    fn execute(&mut self) {
        while !self.edges_queue.is_empty() {
            let edge = self.edges_queue.pop().unwrap();
            self.base_scan.base_op.probe_tuple[0] = edge[0];
            self.base_scan.base_op.probe_tuple[1] = edge[0];
            self.base_scan.base_op.num_out_tuples += 1;
            for next_op in self.base_scan.base_op.next.as_mut().unwrap() {
                next_op.process_new_tuple();
            }
        }
    }

    fn get_alds_as_string(&self) -> String {
        self.base_scan.get_alds_as_string()
}

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_scan.update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Option<Operator<Id>> {
        let mut scan_sampling = ScanSampling::new(self.base_scan.base_op.out_subgraph.clone());
        scan_sampling.edges_queue = self.edges_queue.clone();
        Some(Operator::Scan(Scan::ScanSampling(scan_sampling)))
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        self.base_scan.is_same_as(op)
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_scan.get_num_out_tuples()
    }
}
