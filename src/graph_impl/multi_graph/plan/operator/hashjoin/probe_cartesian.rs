use generic::{GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::plan::operator::hashjoin::hash_table::BlockInfo;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::{BaseProbe, Probe};
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::DerefMut;
use std::rc::Rc;

#[derive(Clone)]
pub struct ProbeCartesian<Id: IdType> {
    pub base_probe: BaseProbe<Id>,
    other_block_info: BlockInfo<Id>,
    highest_vertex_id: usize,
}

impl<Id: IdType> ProbeCartesian<Id> {
    pub fn new(
        out_subgraph: QueryGraph,
        in_subgraph: QueryGraph,
        join_qvertices: Vec<String>,
        probe_hash_idx: usize,
        hashed_tuple_len: usize,
        probe_tuple_len: usize,
        out_qvertex_to_idx_map: HashMap<String, usize>,
    ) -> ProbeCartesian<Id> {
        let mut pc = ProbeCartesian {
            base_probe: BaseProbe::new(
                out_subgraph,
                in_subgraph,
                join_qvertices,
                probe_hash_idx,
                hashed_tuple_len,
                probe_tuple_len,
                out_qvertex_to_idx_map,
            ),
            other_block_info: BlockInfo::empty(),
            highest_vertex_id: 0,
        };
        pc.base_probe.base_op.name = "CARTESIAN ".to_owned() + &pc.base_probe.base_op.name;
        pc
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for ProbeCartesian<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        if self.base_probe.base_op.probe_tuple.borrow().len() == 0 {
            self.highest_vertex_id = graph.node_count() + 1;
            self.other_block_info = BlockInfo::empty();
        }
        self.base_probe.init(probe_tuple, graph);
    }

    fn process_new_tuple(&mut self) {
        for a_hash_vertex in 0..self.highest_vertex_id {
            let base_probe = &mut self.base_probe;
            base_probe.base_op.probe_tuple.borrow_mut()[base_probe.hashed_tuple_len] = Id::new(a_hash_vertex);
            for hash_table in base_probe.hash_tables.clone() {
                let a_last_chunk_idx = hash_table.num_chunks[a_hash_vertex];
                let mut a_prev_first_vertex = -1i32;
                for a_chunk_idx in 0..a_last_chunk_idx {
                    hash_table.get_block_and_offsets(
                        a_hash_vertex,
                        a_chunk_idx,
                        &mut self.other_block_info,
                    );
                    let mut an_offset = self.other_block_info.start_offset;
                    while an_offset < self.other_block_info.end_offset {
                        if base_probe.hashed_tuple_len == 2 {
                            let first_vertex = self.other_block_info.block[an_offset];
                            an_offset += 1;
                            if a_prev_first_vertex != first_vertex.id() as i32 {
                                base_probe.base_op.probe_tuple.borrow_mut()[0] = first_vertex;
                                a_prev_first_vertex = first_vertex.id() as i32;
                            }
                            base_probe.base_op.probe_tuple.borrow_mut()[1] =
                                self.other_block_info.block[an_offset];
                            an_offset += 1;
                        } else {
                            for k in 0..base_probe.hashed_tuple_len {
                                base_probe.base_op.probe_tuple.borrow_mut()[k] =
                                    self.other_block_info.block[an_offset];
                                an_offset += 1;
                            }
                        }
                        base_probe.process_new_tuple();
                    }
                }
            }
        }
    }

    fn execute(&mut self) {
        self.base_probe.execute()
    }

    fn get_alds_as_string(&self) -> String {
        self.base_probe.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_probe
            .update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let op = &self.base_probe.base_op;
        Operator::Probe(Probe::PC(ProbeCartesian::new(
            op.out_subgraph.clone(),
            op.in_subgraph.as_ref().unwrap().clone(),
            self.base_probe.join_qvertices.clone(),
            self.base_probe.probe_hash_idx,
            self.base_probe.hashed_tuple_len,
            self.base_probe.probe_tuple_len,
            op.out_qvertex_to_idx_map.clone(),
        )))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        if let Operator::Probe(Probe::PC(pc)) = op.borrow_mut().deref_mut() {
            let self_op = &mut self.base_probe.base_op;
            let other_op = &mut pc.base_probe.base_op;
            let in_subgraph = self_op.in_subgraph.as_mut().map_or(false, |in_subgraph| {
                in_subgraph.is_isomorphic_to(other_op.in_subgraph.as_mut().unwrap())
            });
            let out_subgraph = self_op
                .out_subgraph
                .is_isomorphic_to(&mut other_op.out_subgraph);
            return in_subgraph && out_subgraph;
        }
        false
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_probe.get_num_out_tuples()
    }
}
