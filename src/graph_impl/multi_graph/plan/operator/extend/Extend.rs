use graph_impl::multi_graph::plan::operator::extend::EI::{BaseEI, Neighbours, EI, DIFFERENTIATE_FWD_BWD_SINGLE_ALD};
use graph_impl::multi_graph::catalog::adj_list_descriptor::{Direction, AdjListDescriptor};
use graph_impl::static_graph::sorted_adj_vec::SortedAdjVec;
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use hashbrown::HashMap;
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::TypedStaticGraph;
use generic::{IdType, GraphType};
use std::hash::Hash;

#[derive(Clone)]
pub struct Extend<Id: IdType> {
    pub base_ei: BaseEI<Id>,
    vertex_index: usize,
    label_or_to_type: usize,
    pub dir: Direction,
    adj_list: Vec<Option<SortedAdjVec<Id>>>,
}

impl<Id: IdType> Extend<Id> {
    pub fn new(to_qvertex: String, to_type: usize, alds: Vec<AdjListDescriptor>,
               out_subgraph: Box<QueryGraph>, in_subgraph: Option<Box<QueryGraph>>,
               out_qvertex_to_idx_map: HashMap<String, usize>) -> Extend<Id> {
        let ald = alds.get(0).unwrap().clone();
        let mut extend = Extend {
            base_ei: BaseEI::new(to_qvertex.clone(), to_type, alds, out_subgraph, in_subgraph),
            vertex_index: ald.vertex_idx,
            label_or_to_type: ald.label,
            dir: ald.direction.clone(),
            adj_list: vec![],
        };
        extend.base_ei.base_op.last_repeated_vertex_idx = extend.base_ei.base_op.out_tuple_len - 2;
        extend.base_ei.out_idx = out_qvertex_to_idx_map.get(&to_qvertex).unwrap().clone();
        extend.base_ei.base_op.out_qvertex_to_idx_map = out_qvertex_to_idx_map;
        extend
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Extend<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(&mut self, probe_tuple: Vec<Id>, graph: &TypedStaticGraph<Id, NL, EL, Ty, L>) {
        self.base_ei.out_neighbours = Neighbours::new();
        self.base_ei.base_op.probe_tuple = probe_tuple.clone();
        self.base_ei.vertex_types = graph.get_node_types().clone();
        self.adj_list = if let Direction::Fwd = self.dir { graph.get_fwd_adj_list() } else { graph.get_bwd_adj_list() }.clone();
        if graph.is_sorted_by_node() {
            self.label_or_to_type = self.base_ei.to_type;
            self.base_ei.to_type = 0;
        }
        for next_operator in self.base_ei.base_op.next.as_mut().unwrap() {
            next_operator.init(probe_tuple.clone(), graph);
        }
    }

    fn process_new_tuple(&mut self) {
        let adj_vec = self.adj_list[self.base_ei.base_op.probe_tuple[self.vertex_index].id()].as_mut().unwrap();
        let out_neighbour = &mut self.base_ei.out_neighbours;
        adj_vec.set_neighbor_ids(self.label_or_to_type, out_neighbour);
        self.base_ei.base_op.icost += out_neighbour.end_idx - out_neighbour.start_idx;
        for idx in out_neighbour.start_idx..out_neighbour.end_idx {
            if self.base_ei.to_type == 0 || self.base_ei.to_type == self.base_ei.vertex_types[out_neighbour.ids[idx].id()] {
                self.base_ei.base_op.num_out_tuples += 1;
                self.base_ei.base_op.probe_tuple[self.base_ei.out_idx] = out_neighbour.ids[idx];
                self.base_ei.base_op.next.as_mut().unwrap()[0].process_new_tuple();
            }
        }
    }

    fn execute(&mut self) {
        unimplemented!()
    }

    fn copy(&self, is_thread_safe: bool) -> Option<Operator<Id>> {
        let base_ei = &self.base_ei;
        let base_op = &base_ei.base_op;
        let mut extend = Extend::new(
            base_ei.to_query_vertex.clone(), base_ei.to_type,
            base_ei.alds.clone(),
            base_op.out_subgraph.clone(), base_op.in_subgraph.clone(),
            base_op.out_qvertex_to_idx_map.clone(),
        );
        let extend_copy = extend.clone();
        extend.base_ei.base_op.prev = base_op.prev.as_ref().unwrap().copy(is_thread_safe).map(|op| Box::new(op));
        let prev = extend.base_ei.base_op.prev.as_mut().unwrap().as_mut();
        *get_op_attr_as_mut!(prev,next) = Some(vec![Operator::EI(EI::Extend(extend_copy))]);
        let last_repeated_vertex_idx = get_op_attr!(prev,last_repeated_vertex_idx);
        extend.base_ei.init_caching(last_repeated_vertex_idx);
        Some(Operator::EI(EI::Extend(extend)))
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        if let Operator::EI(EI::Extend(extend)) = op {
            return (!DIFFERENTIATE_FWD_BWD_SINGLE_ALD || self.dir == extend.dir) &&
                self.label_or_to_type == extend.label_or_to_type &&
                self.base_ei.to_type == extend.base_ei.to_type &&
                self.base_ei.base_op.in_subgraph.as_mut().unwrap().is_isomorphic_to(get_op_attr_as_mut!(op,in_subgraph).as_mut().unwrap().as_mut()) &&
                self.base_ei.base_op.out_subgraph.is_isomorphic_to(get_op_attr_as_mut!(op,out_subgraph).as_mut()) &&
                self.base_ei.base_op.prev.as_mut().unwrap().is_same_as(get_op_attr_as_mut!(op,prev).as_mut().unwrap());
        }
        false
    }
}