use generic::{GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan_blocking::ScanBlocking;
use graph_impl::multi_graph::plan::operator::scan::scan_sampling::ScanSampling;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::static_graph::sorted_adj_vec::SortedAdjVec;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};
use std::rc::Rc;

#[derive(Clone)]
pub enum Scan<Id: IdType> {
    Base(BaseScan<Id>),
    ScanSampling(ScanSampling<Id>),
    ScanBlocking(ScanBlocking<Id>),
}

#[derive(Clone)]
pub struct BaseScan<Id: IdType> {
    pub base_op: BaseOperator<Id>,
    from_query_vertex: String,
    to_query_vertex: String,
    pub from_type: usize,
    pub to_type: usize,
    pub label_or_to_type: usize,
    pub fwd_adj_list: Vec<Option<SortedAdjVec<Id>>>,
    pub vertex_ids: Vec<Id>,
    pub vertex_types: Vec<usize>,
    from_vertex_start_idx: usize,
    from_vertex_end_idx: usize,
}

impl<Id: IdType> BaseScan<Id> {
    pub fn new(out_subgraph: Box<QueryGraph>) -> BaseScan<Id> {
        let mut scan = BaseScan {
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
        };
        let out_subgraph = scan.base_op.out_subgraph.as_ref();
        if out_subgraph.get_query_edges().len() > 1 {
            panic!("IllegalArgumentException");
        }
        let query_edge = &out_subgraph.get_query_edges()[0];
        scan.from_type = query_edge.from_type;
        scan.to_type = query_edge.to_type;
        scan.label_or_to_type = query_edge.label;
        scan.base_op.last_repeated_vertex_idx = 0;
        scan.from_query_vertex = query_edge.from_query_vertex.clone();
        scan.to_query_vertex = query_edge.to_query_vertex.clone();
        scan.base_op
            .out_qvertex_to_idx_map
            .insert(scan.from_query_vertex.clone(), 0);
        scan.base_op
            .out_qvertex_to_idx_map
            .insert(scan.to_query_vertex.clone(), 1);
        scan.base_op.name =
            "SCAN (".to_owned() + &scan.from_query_vertex + ")->(" + &scan.to_query_vertex + ")";
        scan
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for BaseScan<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_op.probe_tuple = probe_tuple.clone();
        self.vertex_ids = graph.get_node_ids().clone();
        self.vertex_types = graph.get_node_types().clone();
        if 0 != self.from_type {
            self.from_vertex_start_idx = graph.get_node_type_offsets()[self.from_type];
            self.from_vertex_end_idx = graph.get_node_type_offsets()[self.from_type + 1];
        } else {
            self.from_vertex_start_idx = 0;
            self.from_vertex_end_idx = graph.node_count();
        }
        self.fwd_adj_list = graph.get_fwd_adj_list().clone();
        if graph.is_sorted_by_node() {
            self.label_or_to_type = self.to_type;
            self.to_type = 0;
        }
        for next_op in self.base_op.next.as_mut().unwrap() {
            next_op.init(probe_tuple.clone(), graph);
        }
    }

    fn process_new_tuple(&mut self) {
        panic!("Operator `scan` does not support execute().")
    }

    fn execute(&mut self) {
        for from_idx in self.from_vertex_start_idx..self.from_vertex_end_idx {
            let from_vertex = self.vertex_ids[from_idx];
            self.base_op.probe_tuple[0] = from_vertex;
            let to_vertex_start_idx =
                self.fwd_adj_list[from_idx].as_ref().unwrap().get_offsets()[self.label_or_to_type];
            let to_vertex_end_idx = self.fwd_adj_list[from_idx].as_ref().unwrap().get_offsets()
                [self.label_or_to_type + 1];
            for to_idx in to_vertex_start_idx..to_vertex_end_idx {
                self.base_op.probe_tuple[1] = self.fwd_adj_list[from_idx]
                    .as_ref()
                    .unwrap()
                    .get_neighbor_id(Id::new(to_idx));
                if self.to_type == 0
                    || self.vertex_types[self.base_op.probe_tuple[1].id()] == self.to_type
                {
                    self.base_op.num_out_tuples += 1;
                    self.base_op
                        .next
                        .as_mut()
                        .map(|next| (&mut next[0]).process_new_tuple());
                }
            }
        }
    }

    fn get_alds_as_string(&self) -> String {
        self.base_op.get_alds_as_string()
    }

    fn update_operator_name(&mut self, mut query_vertex_to_index_map: HashMap<String, usize>) {
        query_vertex_to_index_map = HashMap::new();
        query_vertex_to_index_map.insert(self.from_query_vertex.clone(), 0);
        query_vertex_to_index_map.insert(self.to_query_vertex.clone(), 1);
        self.base_op.next.as_mut().map(|next| {
            for next_op in next {
                next_op.update_operator_name(query_vertex_to_index_map.clone());
            }
        });
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        if is_thread_safe {
            return Operator::Scan(Scan::ScanBlocking(ScanBlocking::new(
                self.base_op.out_subgraph.clone(),
            )));
        }
        Operator::Scan(Scan::Base(BaseScan::new(self.base_op.out_subgraph.clone())))
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        if let Operator::Scan(scan) = op {
            return self.from_type == get_scan_as_ref!(scan).from_type
                && self.to_type == get_scan_as_ref!(scan).to_type
                && self.label_or_to_type == get_scan_as_ref!(scan).label_or_to_type;
        }
        false
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_op.get_num_out_tuples()
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Scan<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self {
            Scan::Base(base) => base.init(probe_tuple, graph),
            Scan::ScanSampling(ss) => ss.init(probe_tuple, graph),
            Scan::ScanBlocking(sb) => sb.init(probe_tuple, graph),
        }
    }

    fn process_new_tuple(&mut self) {
        match self {
            Scan::Base(base) => base.process_new_tuple(),
            Scan::ScanSampling(ss) => ss.process_new_tuple(),
            Scan::ScanBlocking(sb) => sb.process_new_tuple(),
        }
    }

    fn execute(&mut self) {
        match self {
            Scan::Base(base) => base.execute(),
            Scan::ScanSampling(ss) => ss.execute(),
            Scan::ScanBlocking(sb) => sb.execute(),
        }
    }

    fn get_alds_as_string(&self) -> String {
        match self {
            Scan::Base(base) => base.get_alds_as_string(),
            Scan::ScanSampling(ss) => ss.get_alds_as_string(),
            Scan::ScanBlocking(sb) => sb.get_alds_as_string(),
        }
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        match self {
            Scan::Base(base) => base.update_operator_name(query_vertex_to_index_map),
            Scan::ScanSampling(ss) => ss.update_operator_name(query_vertex_to_index_map),
            Scan::ScanBlocking(sb) => sb.update_operator_name(query_vertex_to_index_map),
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        match self {
            Scan::Base(base) => base.copy(is_thread_safe),
            Scan::ScanSampling(ss) => ss.copy(is_thread_safe),
            Scan::ScanBlocking(sb) => sb.copy(is_thread_safe),
        }
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        match self {
            Scan::Base(base) => base.is_same_as(op),
            Scan::ScanSampling(ss) => ss.is_same_as(op),
            Scan::ScanBlocking(sb) => sb.is_same_as(op),
        }
    }

    fn get_num_out_tuples(&self) -> usize {
        match self {
            Scan::Base(base) => base.get_num_out_tuples(),
            Scan::ScanSampling(ss) => ss.get_num_out_tuples(),
            Scan::ScanBlocking(sb) => sb.get_num_out_tuples(),
        }
    }
}
