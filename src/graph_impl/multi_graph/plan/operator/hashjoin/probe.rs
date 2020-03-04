use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::hashjoin::hash_table::{BlockInfo, HashTable};
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;

use graph_impl::multi_graph::plan::operator::hashjoin::probe_cartesian::ProbeCartesian;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;

#[derive(Clone)]
pub enum Probe<Id: IdType> {
    BaseProbe(BaseProbe<Id>),
    PC(ProbeCartesian<Id>),
    PMV(PMV<Id>),
}

#[derive(Clone)]
pub struct BaseProbe<Id: IdType> {
    pub base_op: BaseOperator<Id>,
    pub hash_tables: Vec<HashTable<Id>>,
    pub join_qvertices: Vec<String>,
    pub probe_hash_idx: usize,
    pub hashed_tuple_len: usize,
    pub probe_tuple_len: usize,
    pub block_info: BlockInfo<Id>,
}

impl<Id: IdType> BaseProbe<Id> {
    pub fn new(
        out_subgraph: QueryGraph,
        in_subgraph: QueryGraph,
        join_qvertices: Vec<String>,
        probe_hash_idx: usize,
        hashed_tuple_len: usize,
        probe_tuple_len: usize,
        out_qvertex_to_idx_map: HashMap<String, usize>,
    ) -> BaseProbe<Id> {
        let mut probe = BaseProbe {
            base_op: BaseOperator::new(out_subgraph, Some(in_subgraph)),
            hash_tables: vec![],
            join_qvertices,
            probe_hash_idx,
            hashed_tuple_len,
            probe_tuple_len,
            block_info: BlockInfo::empty(),
        };
        probe.base_op.out_tuple_len = out_qvertex_to_idx_map.len();
        probe.base_op.out_qvertex_to_idx_map = out_qvertex_to_idx_map;
        probe.base_op.name = "PROBE ON (".to_owned() + &probe.join_qvertices[0] + ")";
        probe
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for BaseProbe<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        if self.base_op.probe_tuple.borrow().len() == 0 {
            self.base_op.probe_tuple = probe_tuple.clone();
            self.block_info = BlockInfo::empty();
            self.base_op
                .next
                .iter_mut()
                .map(|next_op| next_op.borrow_mut())
                .for_each(|mut next_op| next_op.deref_mut().init(probe_tuple.clone(), graph));
        }
    }

    fn process_new_tuple(&mut self) {
        let hash_vertex = self.base_op.probe_tuple.borrow()[self.probe_hash_idx].id();
        for hash_table in &mut self.hash_tables {
            let last_chunk_idx = hash_table.num_chunks[hash_vertex];
            let mut prev_first_item = -1i32;
            for chunk_idx in 0..last_chunk_idx {
                hash_table.get_block_and_offsets(hash_vertex, chunk_idx, &mut self.block_info);
                let mut offset = self.block_info.start_offset;
                while offset < self.block_info.end_offset {
                    self.base_op.num_out_tuples += 1;
                    if self.hashed_tuple_len == 2 {
                        let first_item = self.block_info.block[offset];
                        offset += 1;
                        if prev_first_item != first_item.id() as i32 {
                            self.base_op.probe_tuple.borrow_mut()[self.probe_tuple_len] = first_item;
                            prev_first_item = first_item.id() as i32;
                        }
                        self.base_op.probe_tuple.borrow_mut()[self.probe_tuple_len + 1] =
                            self.block_info.block[offset];
                        offset += 1;
                    } else {
                        for k in 0..self.hashed_tuple_len {
                            self.base_op.probe_tuple.borrow_mut()[self.probe_tuple_len + k] =
                                self.block_info.block[offset];
                            offset += 1;
                        }
                    }
                    self.base_op.next[0]
                        .borrow_mut()
                        .deref_mut()
                        .process_new_tuple();
                }
            }
        }
    }

    fn execute(&mut self) {
        self.base_op.execute()
    }

    fn get_alds_as_string(&self) -> String {
        self.base_op.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_op.update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let op = &self.base_op;
        let mut probe = BaseProbe::new(
            op.out_subgraph.clone(),
            op.in_subgraph.as_ref().unwrap().clone(),
            self.join_qvertices.clone(),
            self.probe_hash_idx,
            self.hashed_tuple_len,
            self.probe_tuple_len,
            op.out_qvertex_to_idx_map.clone(),
        );
        probe.base_op.prev = op
            .prev
            .as_ref()
            .map(|prev| Rc::new(RefCell::new(prev.borrow().deref().copy(is_thread_safe))));
        probe.base_op.next = vec![Rc::new(RefCell::new(Operator::Probe(Probe::BaseProbe(
            probe.clone(),
        ))))];
        Operator::Probe(Probe::BaseProbe(probe))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        if let Operator::Probe(Probe::BaseProbe(probe)) = op.borrow_mut().deref_mut() {
            let self_op = &mut self.base_op;
            let other_op = &mut probe.base_op;
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
        self.base_op.get_num_out_tuples()
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Probe<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self {
            Probe::BaseProbe(base) => base.init(probe_tuple, graph),
            Probe::PC(pc) => pc.init(probe_tuple, graph),
            Probe::PMV(pmv) => pmv.init(probe_tuple, graph),
        }
    }

    fn process_new_tuple(&mut self) {
        match self {
            Probe::BaseProbe(base) => base.process_new_tuple(),
            Probe::PC(pc) => pc.process_new_tuple(),
            Probe::PMV(pmv) => pmv.process_new_tuple(),
        }
    }

    fn execute(&mut self) {
        match self {
            Probe::BaseProbe(base) => base.execute(),
            Probe::PC(pc) => pc.execute(),
            Probe::PMV(pmv) => pmv.execute(),
        }
    }

    fn get_alds_as_string(&self) -> String {
        match self {
            Probe::BaseProbe(base) => base.get_alds_as_string(),
            Probe::PC(pc) => pc.get_alds_as_string(),
            Probe::PMV(pmv) => pmv.get_alds_as_string(),
        }
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        match self {
            Probe::BaseProbe(base) => base.update_operator_name(query_vertex_to_index_map),
            Probe::PC(pc) => pc.update_operator_name(query_vertex_to_index_map),
            Probe::PMV(pmv) => pmv.update_operator_name(query_vertex_to_index_map),
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        match self {
            Probe::BaseProbe(base) => base.copy(is_thread_safe),
            Probe::PC(pc) => pc.copy(is_thread_safe),
            Probe::PMV(pmv) => pmv.copy(is_thread_safe),
        }
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        match self {
            Probe::BaseProbe(base) => base.is_same_as(op),
            Probe::PC(pc) => pc.is_same_as(op),
            Probe::PMV(pmv) => pmv.is_same_as(op),
        }
    }

    fn get_num_out_tuples(&self) -> usize {
        match self {
            Probe::BaseProbe(base) => base.get_num_out_tuples(),
            Probe::PC(pc) => pc.get_num_out_tuples(),
            Probe::PMV(pmv) => pmv.get_num_out_tuples(),
        }
    }
}
