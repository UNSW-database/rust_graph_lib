use generic::{GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::multi_graph::plan::operator::scan::scan::BaseScan;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};

static PARTITION_SIZE: usize = 100;

#[derive(Clone)]
pub struct VertexIdxLimits {
    pub from_variable_index_limit: usize,
    pub to_variable_index_limit: usize,
}

///TODO:ReentrantLock
#[derive(Clone)]
pub struct ScanBlocking<Id: IdType> {
    pub base_scan: BaseScan<Id>,
    curr_from_idx: usize,
    curr_to_idx: usize,
    from_idx_limit: usize,
    to_idx_limit: usize,
    highest_from_idx: usize,
    highest_to_idx: usize,
    pub global_vertices_idx_limits: VertexIdxLimits,
}

impl<Id: IdType> ScanBlocking<Id> {
    pub fn new(out_subgraph: Box<QueryGraph>) -> ScanBlocking<Id> {
        ScanBlocking {
            base_scan: BaseScan::new(out_subgraph),
            curr_from_idx: 0,
            curr_to_idx: 0,
            from_idx_limit: 0,
            to_idx_limit: 0,
            highest_from_idx: 0,
            highest_to_idx: 0,
            global_vertices_idx_limits: VertexIdxLimits {
                from_variable_index_limit: 0,
                to_variable_index_limit: 0,
            },
        }
    }

    fn update_indices_limits(&mut self) {
        //ReentrantLock lock here.
        self.curr_from_idx = self.global_vertices_idx_limits.from_variable_index_limit;
        self.curr_to_idx = self.global_vertices_idx_limits.to_variable_index_limit;
        self.from_idx_limit = self.curr_from_idx;
        self.to_idx_limit = self.curr_to_idx;
        let mut num_edges_left = PARTITION_SIZE;
        while num_edges_left > 0 {
            let flag = self.from_idx_limit == self.highest_from_idx - 1
                && self.to_idx_limit < self.highest_to_idx - 1
                || self.from_idx_limit < self.highest_from_idx - 1;
            if !flag {
                break;
            }
            let mut label = self.base_scan.label_or_to_type;
            let to_limit = self.base_scan.fwd_adj_list[self.from_idx_limit]
                .as_mut()
                .map_or(0, |adj| adj.get_offsets()[label + 1]);
            if self.to_idx_limit + num_edges_left <= to_limit - 1 {
                self.to_idx_limit += (num_edges_left - 1);
                num_edges_left = 0;
            } else {
                num_edges_left -= (to_limit - 1 - self.to_idx_limit);
                self.to_idx_limit = to_limit;
                if self.from_idx_limit == self.highest_from_idx - 1 {
                    break;
                }
                self.from_idx_limit += 1;
                label = self.base_scan.label_or_to_type;
                self.to_idx_limit = self.base_scan.fwd_adj_list[self.from_idx_limit]
                    .as_mut()
                    .map_or(0, |adj| adj.get_offsets()[label]);
            }
        }
        self.global_vertices_idx_limits.from_variable_index_limit = self.from_idx_limit;
        self.global_vertices_idx_limits.to_variable_index_limit = self.to_idx_limit;
    }

    fn produce_new_edges(&mut self, from_idx: usize, start_to_idx: usize, end_to_idx: usize) {
        let base_op = &mut self.base_scan.base_op;
        for to_idx in start_to_idx..end_to_idx {
            base_op.probe_tuple[0] = self.base_scan.vertex_ids[from_idx];
            base_op.probe_tuple[1] = self.base_scan.fwd_adj_list[from_idx]
                .as_mut()
                .unwrap()
                .get_neighbor_id(Id::new(to_idx));
            base_op.num_out_tuples += 1;
            base_op.next[0].process_new_tuple();
        }
    }

    fn produce_new_edges_default(&mut self) {
        for from_idx in self.curr_from_idx + 1..self.from_idx_limit {
            let label = self.base_scan.label_or_to_type;
            self.base_scan.base_op.probe_tuple[0] = self.base_scan.vertex_ids[from_idx];
            let to_vertex_idx_start = self.base_scan.fwd_adj_list[from_idx]
                .as_mut()
                .map_or(0, |adj| adj.get_offsets()[label]);
            let to_vertex_idx_limit = self.base_scan.fwd_adj_list[from_idx]
                .as_mut()
                .map_or(0, |adj| adj.get_offsets()[label + 1]);
            for to_idx in to_vertex_idx_start..to_vertex_idx_limit {
                self.base_scan.base_op.probe_tuple[1] = self.base_scan.fwd_adj_list[from_idx]
                    .as_mut()
                    .unwrap()
                    .get_neighbor_id(Id::new(to_idx));
                if self.base_scan.to_type == 0
                    || self.base_scan.vertex_types[self.base_scan.base_op.probe_tuple[1].id()]
                        == self.base_scan.to_type
                {
                    self.base_scan.base_op.num_out_tuples += 1;
                    self.base_scan.base_op.next[0].process_new_tuple();
                }
            }
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for ScanBlocking<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_scan.init(probe_tuple.clone(), graph);
        if self.base_scan.from_type != 0 {
            self.curr_from_idx = graph.get_node_type_offsets()[self.base_scan.from_type];
            self.highest_from_idx = graph.get_node_type_offsets()[self.base_scan.from_type + 1];
        } else {
            self.curr_from_idx = 0;
            self.highest_from_idx = graph.node_count() + 1;
        }
        let label = self.base_scan.label_or_to_type;
        self.curr_to_idx = self.base_scan.fwd_adj_list
            [self.base_scan.vertex_ids[self.curr_from_idx].id()]
        .as_mut()
        .map_or(0, |adj| adj.get_offsets()[label]);
        self.highest_to_idx = self.base_scan.fwd_adj_list
            [self.base_scan.vertex_ids[self.highest_from_idx - 1].id()]
        .as_mut()
        .map_or(0, |adj| adj.get_offsets()[label + 1]);
        self.from_idx_limit = self.curr_from_idx;
        self.to_idx_limit = self.curr_to_idx;
        self.base_scan
            .base_op
            .next
            .iter_mut()
            .for_each(|next_op| next_op.init(probe_tuple.clone(), graph));
    }

    fn process_new_tuple(&mut self) {
        self.base_scan.process_new_tuple()
    }

    fn execute(&mut self) {
        self.update_indices_limits();
        while self.curr_from_idx == self.highest_from_idx - 1
            && self.curr_to_idx < self.highest_to_idx - 1
            || self.curr_from_idx < self.highest_from_idx - 1
        {
            if self.curr_from_idx == self.from_idx_limit {
                self.produce_new_edges(self.curr_from_idx, self.curr_to_idx, self.to_idx_limit);
            } else if self.curr_from_idx < self.from_idx_limit {
                let label = self.base_scan.label_or_to_type;
                let to_vertex_idx_limit = self.base_scan.fwd_adj_list
                    [self.base_scan.vertex_ids[self.curr_from_idx].id()]
                .as_mut()
                .map_or(0, |adj| adj.get_offsets()[label + 1]);
                self.produce_new_edges(self.curr_from_idx, self.curr_to_idx, to_vertex_idx_limit);
                self.produce_new_edges_default(/* startFromIdx: currFromIdx + 1, endFromIdx: fromIdxLimit */);
                let start_idx = self.base_scan.fwd_adj_list
                    [self.base_scan.vertex_ids[self.from_idx_limit].id()]
                .as_mut()
                .map_or(0, |adj| adj.get_offsets()[label]);
                self.produce_new_edges(self.from_idx_limit, start_idx, self.to_idx_limit);
            }
            self.update_indices_limits();
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
        self.base_scan.copy(is_thread_safe)
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        self.base_scan.is_same_as(op)
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_scan.get_num_out_tuples()
    }
}
