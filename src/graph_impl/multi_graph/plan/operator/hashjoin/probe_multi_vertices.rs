use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::hashjoin::probe::{BaseProbe, Probe};
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices_cartesian::ProbeMultiVerticesCartesian;
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use itertools::Itertools;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

#[derive(Clone)]
pub enum PMV<Id: IdType> {
    BasePMV(ProbeMultiVertices<Id>),
    PMVC(ProbeMultiVerticesCartesian<Id>),
}

#[derive(Clone)]
pub struct ProbeMultiVertices<Id: IdType> {
    pub base_probe: BaseProbe<Id>,
    pub probe_indices: Vec<usize>,
    pub build_indices: Vec<usize>,
}

impl<Id: IdType> ProbeMultiVertices<Id> {
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
    ) -> ProbeMultiVertices<Id> {
        let mut name = "PROBE ON ".to_owned();
        if 1 == join_qvertices.len() {
            name = name + "(" + &join_qvertices[0] + ")";
        } else {
            for i in 0..join_qvertices.len() {
                if i > 0 && i < join_qvertices.len() - 1 {
                    name += ", ";
                }
                if i == join_qvertices.len() - 1 {
                    name += " & "
                }
                name = name + "(" + &join_qvertices[i] + ")";
            }
        }
        let mut pmv = ProbeMultiVertices {
            base_probe: BaseProbe::new(
                out_subgraph,
                in_subgraph,
                join_qvertices,
                probe_hash_idx,
                hashed_tuple_len,
                probe_tuple_len,
                out_qvertex_to_idx_map,
            ),
            probe_indices,
            build_indices,
        };
        pmv.base_probe.base_op.name = name;
        pmv
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for ProbeMultiVertices<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_probe.init(probe_tuple, graph);
    }

    fn process_new_tuple(&mut self) {
        let hash_vertex = self.base_probe.base_op.probe_tuple[self.base_probe.probe_hash_idx].id();
        for hash_table in self.base_probe.hash_tables.clone() {
            let last_chunk_idx = hash_table.num_chunks[hash_vertex];
            for chunk_idx in 0..last_chunk_idx {
                hash_table.get_block_and_offsets(
                    hash_vertex,
                    chunk_idx,
                    &mut self.base_probe.block_info,
                );
                let offset = self.base_probe.block_info.start_offset;
                (self.base_probe.block_info.start_offset..self.base_probe.block_info.end_offset)
                    .step(self.base_probe.hashed_tuple_len)
                    .for_each(|offset| {
                        let mut flag = true;
                        for i in 0..self.probe_indices.len() {
                            if self.base_probe.base_op.probe_tuple[self.probe_indices[i]]
                                != self.base_probe.block_info.block[offset + self.build_indices[i]]
                            {
                                flag = false;
                                break;
                            }
                        }
                        if flag {
                            self.base_probe.base_op.num_out_tuples += 1;
                            let mut out = 0;
                            for k in 0..self.base_probe.hashed_tuple_len {
                                let mut copy = true;
                                for build_idx in &self.build_indices {
                                    if k == build_idx.clone() {
                                        copy = false;
                                        break;
                                    }
                                }
                                if copy {
                                    self.base_probe.base_op.probe_tuple
                                        [self.base_probe.probe_tuple_len + out] =
                                        self.base_probe.block_info.block[offset + k];
                                    out += 1;
                                }
                            }
                            self.base_probe.base_op.next[0]
                                .borrow_mut()
                                .process_new_tuple();
                        }
                    });
            }
        }
    }

    fn execute(&mut self) {
        self.base_probe.execute();
    }

    fn get_alds_as_string(&self) -> String {
        self.base_probe.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_probe
            .update_operator_name(query_vertex_to_index_map);
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let mut probe = ProbeMultiVertices::new(
            self.base_probe.base_op.out_subgraph.clone(),
            self.base_probe
                .base_op
                .in_subgraph
                .as_ref()
                .unwrap()
                .clone(),
            self.base_probe.join_qvertices.clone(),
            self.base_probe.probe_hash_idx,
            self.probe_indices.clone(),
            self.build_indices.clone(),
            self.base_probe.hashed_tuple_len,
            self.base_probe.probe_tuple_len,
            self.base_probe.base_op.out_qvertex_to_idx_map.clone(),
        );
        probe.base_probe.base_op.prev = self
            .base_probe
            .base_op
            .prev
            .as_ref()
            .map(|prev| Rc::new(RefCell::new(prev.borrow().deref().copy(is_thread_safe))));
        probe.base_probe.base_op.next = vec![Rc::new(RefCell::new(Operator::Probe(Probe::PMV(
            PMV::BasePMV(probe.clone()),
        ))))];
        Operator::Probe(Probe::PMV(PMV::BasePMV(probe)))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        if let Operator::Probe(Probe::PMV(PMV::BasePMV(probe))) = op.borrow_mut().deref_mut() {
            let self_op = &mut self.base_probe.base_op;
            let other_op = &mut probe.base_probe.base_op;
            let in_subgraph = self_op.in_subgraph.as_mut().map_or(false, |in_subgraph| {
                in_subgraph.is_isomorphic_to(other_op.in_subgraph.as_mut().unwrap())
            });
            let out_subgraph = self_op
                .out_subgraph
                .is_isomorphic_to(&mut other_op.out_subgraph);
            let prev = self_op.prev.as_mut().map_or(false, |prev| {
                prev.borrow_mut()
                    .is_same_as(other_op.prev.as_mut().unwrap())
            });
            return in_subgraph && out_subgraph && prev;
        }
        false
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_probe.get_num_out_tuples()
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for PMV<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self {
            PMV::BasePMV(base) => base.init(probe_tuple, graph),
            PMV::PMVC(pmvc) => pmvc.init(probe_tuple, graph),
        }
    }

    fn process_new_tuple(&mut self) {
        match self {
            PMV::BasePMV(base) => base.process_new_tuple(),
            PMV::PMVC(pmvc) => pmvc.process_new_tuple(),
        }
    }

    fn execute(&mut self) {
        match self {
            PMV::BasePMV(base) => base.execute(),
            PMV::PMVC(pmvc) => pmvc.execute(),
        }
    }

    fn get_alds_as_string(&self) -> String {
        match self {
            PMV::BasePMV(base) => base.get_alds_as_string(),
            PMV::PMVC(pmvc) => pmvc.get_alds_as_string(),
        }
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        match self {
            PMV::BasePMV(base) => base.update_operator_name(query_vertex_to_index_map),
            PMV::PMVC(pmvc) => pmvc.update_operator_name(query_vertex_to_index_map),
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        match self {
            PMV::BasePMV(base) => base.copy(is_thread_safe),
            PMV::PMVC(pmvc) => pmvc.copy(is_thread_safe),
        }
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        match self {
            PMV::BasePMV(base) => base.is_same_as(op),
            PMV::PMVC(pmvc) => pmvc.is_same_as(op),
        }
    }

    fn get_num_out_tuples(&self) -> usize {
        match self {
            PMV::BasePMV(base) => base.get_num_out_tuples(),
            PMV::PMVC(pmvc) => pmvc.get_num_out_tuples(),
        }
    }
}
