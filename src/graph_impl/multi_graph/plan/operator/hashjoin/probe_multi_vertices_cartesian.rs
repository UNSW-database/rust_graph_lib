use generic::{GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::hashjoin::hash_table::BlockInfo;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::{
    ProbeMultiVertices, PMV,
};
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};

#[derive(Clone)]
pub struct ProbeMultiVerticesCartesian<Id: IdType> {
    pub base_pmv: ProbeMultiVertices<Id>,
    other_block_info: BlockInfo<Id>,
    highest_vertex_id: usize,
}

impl<Id: IdType> ProbeMultiVerticesCartesian<Id> {
    pub fn new(
        out_subgraph: QueryGraph,
        in_subgraph: QueryGraph,
        join_qvertices: Vec<String>,
        probe_hash_idx: usize,
        probe_indices: Vec<usize>,
        build_indices: Vec<usize>,
        hashed_tuple_len: usize,
        probe_tuple_len: usize,
        out_qvertex_to_idx_map: HashMap<String, usize>,
    ) -> ProbeMultiVerticesCartesian<Id> {
        let mut pmvc = ProbeMultiVerticesCartesian {
            base_pmv: ProbeMultiVertices::new(
                out_subgraph,
                in_subgraph,
                join_qvertices,
                probe_hash_idx,
                probe_indices,
                build_indices,
                hashed_tuple_len,
                probe_tuple_len,
                out_qvertex_to_idx_map,
            ),
            other_block_info: BlockInfo::empty(),
            highest_vertex_id: 0,
        };
        pmvc.base_pmv.base_probe.base_op.name =
            "CARTESIAN ".to_owned() + &pmvc.base_pmv.base_probe.base_op.name;
        pmvc
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for ProbeMultiVerticesCartesian<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        if self.base_pmv.base_probe.base_op.probe_tuple.len() == 0 {
            self.highest_vertex_id = graph.node_count() + 1;
            self.other_block_info = BlockInfo::empty();
        }
        self.base_pmv.init(probe_tuple, graph);
    }

    fn process_new_tuple(&mut self) {
        for a_hash_vertex in 0..self.highest_vertex_id {
            self.base_pmv.base_probe.base_op.probe_tuple
                [self.base_pmv.base_probe.hashed_tuple_len] = Id::new(a_hash_vertex);
            for hash_table in self.base_pmv.base_probe.hash_tables.clone() {
                let a_last_chunk_idx = hash_table.num_chunks[a_hash_vertex];
                let mut a_prev_first_vertex = -1;
                for a_chunk_idx in 0..a_last_chunk_idx {
                    hash_table.get_block_and_offsets(
                        a_hash_vertex,
                        a_chunk_idx,
                        &mut self.other_block_info,
                    );
                    let mut an_offset = self.other_block_info.start_offset;
                    while an_offset < self.other_block_info.end_offset {
                        if self.base_pmv.base_probe.hashed_tuple_len == 2 {
                            let first_vertex = self.other_block_info.block[an_offset];
                            an_offset += 1;
                            if a_prev_first_vertex != first_vertex.id() as i32 {
                                self.base_pmv.base_probe.base_op.probe_tuple[0] = first_vertex;
                                a_prev_first_vertex = first_vertex.id() as i32;
                            }
                            self.base_pmv.base_probe.base_op.probe_tuple[1] =
                                self.other_block_info.block[an_offset];
                            an_offset += 1;
                        } else {
                            for k in 0..self.base_pmv.base_probe.hashed_tuple_len {
                                self.base_pmv.base_probe.base_op.probe_tuple[k] =
                                    self.other_block_info.block[an_offset];
                                an_offset += 1;
                            }
                        }
                        self.base_pmv.process_new_tuple();
                    }
                }
            }
        }
    }

    fn execute(&mut self) {
        self.base_pmv.execute()
    }

    fn get_alds_as_string(&self) -> String {
        self.base_pmv.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_pmv
            .update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let op = &self.base_pmv.base_probe.base_op;
        Operator::Probe(Probe::PMV(PMV::PMVC(ProbeMultiVerticesCartesian::new(
            op.out_subgraph.as_ref().clone(),
            op.in_subgraph.as_ref().unwrap().as_ref().clone(),
            self.base_pmv.base_probe.join_qvertices.clone(),
            self.base_pmv.base_probe.probe_hash_idx,
            self.base_pmv.probe_indices.clone(),
            self.base_pmv.build_indices.clone(),
            self.base_pmv.base_probe.hashed_tuple_len,
            self.base_pmv.base_probe.probe_tuple_len,
            op.out_qvertex_to_idx_map.clone(),
        ))))
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        if let Operator::Probe(Probe::PMV(PMV::PMVC(pc))) = op {
            let self_op = &mut self.base_pmv.base_probe.base_op;
            let other_op = &mut pc.base_pmv.base_probe.base_op;
            let in_subgraph = self_op.in_subgraph.as_mut().map_or(false, |in_subgraph| {
                in_subgraph.is_isomorphic_to(other_op.in_subgraph.as_mut().unwrap())
            });
            let out_subgraph = self_op
                .out_subgraph
                .as_mut()
                .is_isomorphic_to(other_op.out_subgraph.as_mut());
            return in_subgraph && out_subgraph;
        }
        false
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_pmv.get_num_out_tuples()
    }
}
