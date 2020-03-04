use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use rand::{thread_rng, Rng};
use std::cell::RefCell;
use std::hash::Hash;
use std::rc::Rc;
use itertools::Itertools;

#[derive(Clone)]
pub struct ScanSampling<Id: IdType> {
    pub base_scan: BaseScan<Id>,
    pub edges_queue: Vec<Vec<Id>>,
}

static mut FLAG: bool = false;

impl<Id: IdType> ScanSampling<Id> {
    pub fn new(out_subgraph: QueryGraph) -> ScanSampling<Id> {
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
            self.edges_queue
                .push(vec![edges[edge_idx], edges[edge_idx + 1]]);
        }
    }

    pub fn set_edge_indices_to_sample_list(
        &mut self,
        edges: Vec<Vec<Id>>,
        num_edges_to_sample: usize,
    ) {
        let mut rng = thread_rng();
        while self.edges_queue.len() < num_edges_to_sample {
            let edge_idx = rng.gen_range(0, edges.len());
            self.edges_queue.push(edges[edge_idx].clone());
        }
    }

    pub fn set_edge_indices_to_sample_by_edges(
        &mut self,
        edges: Vec<Vec<Id>>,
        num_edges_to_sample: usize,
    ) {
        let mut rng = thread_rng();
        self.edges_queue = vec![vec![]; num_edges_to_sample];
        while self.edges_queue.len() < num_edges_to_sample {
            let edge_idx = rng.gen_range(0, edges.len());
            self.edges_queue.push(edges[edge_idx].clone());
        }
    }

    pub fn copy_default(&self) -> Operator<Id> {
        let mut scan_sampling = ScanSampling::new(self.base_scan.base_op.out_subgraph.clone());
        scan_sampling.edges_queue = self.edges_queue.clone();
        Operator::Scan(Scan::ScanSampling(scan_sampling))
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for ScanSampling<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        if self.base_scan.base_op.probe_tuple.borrow().is_empty() {
            self.base_scan.base_op.probe_tuple = probe_tuple.clone();
            self.base_scan.base_op.next.iter().for_each(|next_op| {
                next_op.borrow_mut().init(probe_tuple.clone(), graph);
            });
        }
    }

    fn process_new_tuple(&mut self) {
        self.base_scan.process_new_tuple()
    }

    fn execute(&mut self) {
        while !self.edges_queue.is_empty() {
            let edge = self.edges_queue.pop().unwrap();
            self.base_scan.base_op.probe_tuple.borrow_mut()[0] = edge[0];
            self.base_scan.base_op.probe_tuple.borrow_mut()[1] = edge[1];
            self.base_scan.base_op.num_out_tuples += 1;
            for next_op in &mut self.base_scan.base_op.next {
                next_op.borrow_mut().process_new_tuple();
            }
        }
    }

    fn get_alds_as_string(&self) -> String {
        self.base_scan.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_scan
            .update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let mut scan_sampling = ScanSampling::new(self.base_scan.base_op.out_subgraph.clone());
        scan_sampling.edges_queue = self.edges_queue.clone();
        Operator::Scan(Scan::ScanSampling(scan_sampling))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        self.base_scan.is_same_as(op)
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_scan.get_num_out_tuples()
    }
}
