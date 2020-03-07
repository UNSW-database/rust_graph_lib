use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::BaseIntersect;
use graph_impl::multi_graph::plan::operator::extend::intersect::Intersect;
use graph_impl::multi_graph::plan::operator::extend::EI::{
    BaseEI, Neighbours, DIFFERENTIATE_FWD_BWD_SINGLE_ALD, EI,
};
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::planner::catalog::adj_list_descriptor::{
    AdjListDescriptor, Direction,
};
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::static_graph::graph::KEY_ANY;
use graph_impl::static_graph::sorted_adj_vec::SortedAdjVec;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::DerefMut;
use std::rc::Rc;

#[derive(Clone)]
pub struct Extend<Id: IdType> {
    pub base_ei: BaseEI<Id>,
    vertex_index: usize,
    label_or_to_type: i32,
    pub dir: Direction,
    adj_list: Vec<Option<SortedAdjVec<Id>>>,
}

impl<Id: IdType> Extend<Id> {
    pub fn new(
        to_qvertex: String,
        to_type: i32,
        alds: Vec<AdjListDescriptor>,
        out_subgraph: QueryGraph,
        in_subgraph: Option<QueryGraph>,
        out_qvertex_to_idx_map: HashMap<String, usize>,
    ) -> Extend<Id> {
        let ald = alds[0].clone();
        let mut extend = Extend {
            base_ei: BaseEI::new(to_qvertex.clone(), to_type, alds, out_subgraph, in_subgraph),
            vertex_index: ald.vertex_idx,
            label_or_to_type: ald.label,
            dir: ald.direction.clone(),
            adj_list: vec![],
        };
        extend.base_ei.base_op.last_repeated_vertex_idx = extend.base_ei.base_op.out_tuple_len - 2;
        extend.base_ei.out_idx = out_qvertex_to_idx_map[&to_qvertex].clone();
        extend.base_ei.base_op.out_qvertex_to_idx_map = out_qvertex_to_idx_map;
        extend
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Extend<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_ei.out_neighbours = Neighbours::new();
        self.base_ei.base_op.probe_tuple = probe_tuple.clone();
        self.base_ei.vertex_types = graph.get_node_types().clone();
        self.adj_list = if let Direction::Fwd = self.dir {
            graph.get_fwd_adj_list()
        } else {
            graph.get_bwd_adj_list()
        }
        .clone();
        if graph.is_sorted_by_node() {
            self.label_or_to_type = self.base_ei.to_type;
            self.base_ei.to_type = KEY_ANY;
        }
        for next_operator in &mut self.base_ei.base_op.next {
            next_operator.borrow_mut().init(probe_tuple.clone(), graph);
        }
    }

    fn process_new_tuple(&mut self) {
        let adj_vec = self.adj_list
            [self.base_ei.base_op.probe_tuple.borrow()[self.vertex_index].id()]
        .as_mut()
        .unwrap();
        let out_neighbour = &mut self.base_ei.out_neighbours;
        adj_vec.set_neighbor_ids(self.label_or_to_type, out_neighbour);
        self.base_ei.base_op.icost += out_neighbour.end_idx - out_neighbour.start_idx;
        for idx in out_neighbour.start_idx..out_neighbour.end_idx {
            if self.base_ei.to_type == KEY_ANY
                || self.base_ei.to_type == self.base_ei.vertex_types[out_neighbour.ids[idx].id()]
            {
                self.base_ei.base_op.num_out_tuples += 1;
                self.base_ei.base_op.probe_tuple.borrow_mut()[self.base_ei.out_idx] =
                    out_neighbour.ids[idx];
                self.base_ei.base_op.next[0]
                    .borrow_mut()
                    .process_new_tuple();
            }
        }
    }

    fn execute(&mut self) {
        self.base_ei.execute()
    }

    fn get_alds_as_string(&self) -> String {
        self.base_ei.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_ei.update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        let base_ei = &self.base_ei;
        let base_op = &base_ei.base_op;
        let mut extend = Extend::new(
            base_ei.to_query_vertex.clone(),
            base_ei.to_type,
            base_ei.alds.clone(),
            base_op.out_subgraph.clone(),
            base_op.in_subgraph.clone(),
            base_op.out_qvertex_to_idx_map.clone(),
        );
        let extend_copy = extend.clone();
        extend.base_ei.base_op.prev = Some(Rc::new(RefCell::new(
            base_op.prev.as_ref().unwrap().borrow().copy(is_thread_safe),
        )));

        let last_repeated_vertex_idx = {
            let mut prev = extend.base_ei.base_op.prev.as_mut().unwrap().borrow_mut();
            *get_op_attr_as_mut!(prev.deref_mut(), next) =
                vec![Rc::new(RefCell::new(Operator::EI(EI::Extend(extend_copy))))];
            get_op_attr!(prev.deref_mut(), last_repeated_vertex_idx)
        };
        extend.base_ei.init_caching(last_repeated_vertex_idx);
        Operator::EI(EI::Extend(extend))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        if let Operator::EI(EI::Extend(extend)) = op.borrow_mut().deref_mut() {
            let base_self = &mut self.base_ei.base_op;
            let other_op = &mut extend.base_ei.base_op;
            return (!DIFFERENTIATE_FWD_BWD_SINGLE_ALD || self.dir == extend.dir)
                && self.label_or_to_type == extend.label_or_to_type
                && self.base_ei.to_type == extend.base_ei.to_type
                && base_self
                    .in_subgraph
                    .as_mut()
                    .unwrap()
                    .is_isomorphic_to(other_op.in_subgraph.as_mut().unwrap())
                && base_self
                    .out_subgraph
                    .is_isomorphic_to(&mut other_op.out_subgraph)
                && base_self
                    .prev
                    .as_mut()
                    .unwrap()
                    .borrow_mut()
                    .is_same_as(other_op.prev.as_mut().unwrap());
        }
        false
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_ei.get_num_out_tuples()
    }
}