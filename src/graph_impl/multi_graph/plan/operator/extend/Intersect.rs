use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::EI::EI::Base;
use graph_impl::multi_graph::plan::operator::extend::EI::{BaseEI, CachingType, Neighbours, EI};
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::planner::catalog::adj_list_descriptor::AdjListDescriptor;
use graph_impl::multi_graph::planner::catalog::operator::intersect_catalog::IntersectCatalog;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::static_graph::graph::KEY_ANY;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::DerefMut;
use std::rc::Rc;

pub enum IntersectType {
    InitCached,
    TempCached,
    CachedOut,
    TempOut,
}

#[derive(Clone)]
pub enum Intersect<Id: IdType> {
    BaseIntersect(BaseIntersect<Id>),
    IntersectCatalog(IntersectCatalog<Id>),
}

#[derive(Clone)]
pub struct BaseIntersect<Id: IdType> {
    pub base_ei: BaseEI<Id>,
}

impl<Id: IdType> BaseIntersect<Id> {
    pub fn new(
        to_qvertex: String,
        to_type: i32,
        alds: Vec<AdjListDescriptor>,
        out_subgraph: QueryGraph,
        in_subgraph: Option<QueryGraph>,
        out_qvertex_to_idx_map: HashMap<String, usize>,
    ) -> BaseIntersect<Id> {
        let mut intersect = BaseIntersect {
            base_ei: BaseEI::new(to_qvertex.clone(), to_type, alds, out_subgraph, in_subgraph),
        };
        let base_op = &mut intersect.base_ei.base_op;
        base_op.last_repeated_vertex_idx = base_op.out_tuple_len - 2;
        base_op.out_qvertex_to_idx_map = out_qvertex_to_idx_map;
        intersect.base_ei.out_idx = base_op.out_qvertex_to_idx_map[&to_qvertex].clone();
        intersect
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for BaseIntersect<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_ei.init(probe_tuple, graph)
    }

    fn process_new_tuple(&mut self) {
        let mut temp = Neighbours::new();
        if CachingType::None == self.base_ei.caching_type || !self.base_ei.is_intersection_cached()
        {
            let base_ei = &mut self.base_ei;
            let cache_id = base_ei.vertex_idx_to_cache[0];
            let to_id = base_ei.base_op.probe_tuple.borrow()[cache_id].id();
            let adj_vec = base_ei.adj_lists_to_cache[0][to_id].as_ref();
            let cache_id = base_ei.labels_or_to_types_to_cache[0];
            let neighbours = &mut base_ei.init_neighbours;
            adj_vec.map(|adj| adj.set_neighbor_ids(cache_id, neighbours));
            base_ei.base_op.icost +=
                base_ei.init_neighbours.end_idx - base_ei.init_neighbours.start_idx;
            base_ei.base_op.icost += base_ei.execute_intersect(1, IntersectType::InitCached);

            if base_ei.to_type != KEY_ANY {
                let mut curr_end_idx = 0;
                let cached_neighbours = &mut base_ei.cached_neighbours;
                for i in cached_neighbours.start_idx..cached_neighbours.end_idx {
                    if base_ei.vertex_types[cached_neighbours.ids[i].id()] == base_ei.to_type {
                        cached_neighbours.ids[curr_end_idx] = cached_neighbours.ids[i];
                        curr_end_idx += 1;
                    }
                }
                cached_neighbours.end_idx = curr_end_idx;
            }
            for i in 2..base_ei.adj_lists_to_cache.len() {
                temp = base_ei.cached_neighbours.clone();
                base_ei.cached_neighbours = base_ei.temp_neighbours.clone();
                base_ei.temp_neighbours = temp.clone();
                base_ei.base_op.icost += base_ei.execute_intersect(i, IntersectType::TempCached);
            }
        }

        let base_ei = &mut self.base_ei;
        match base_ei.caching_type {
            CachingType::None | CachingType::FullCaching => {
                base_ei.out_neighbours = base_ei.cached_neighbours.clone()
            }
            CachingType::PartialCaching => {
                let cost = base_ei.execute_intersect(0, IntersectType::CachedOut);
                base_ei.base_op.icost += cost;
                for i in 1..base_ei.adj_lists.len() {
                    temp = base_ei.out_neighbours.clone();
                    base_ei.out_neighbours = base_ei.temp_neighbours.clone();
                    base_ei.temp_neighbours = temp.clone();
                    base_ei.base_op.icost += base_ei.execute_intersect(i, IntersectType::TempOut);
                }
            }
        }

        let base_op = &mut base_ei.base_op;
        // setAdjListSortOrder the initNeighbours ids in the output tuple.
        let out_neighbours = &mut base_ei.out_neighbours;
        base_op.num_out_tuples += out_neighbours.end_idx - out_neighbours.start_idx;
        for idx in out_neighbours.start_idx..out_neighbours.end_idx {
            base_op.probe_tuple.borrow_mut()[base_ei.out_idx] = out_neighbours.ids[idx];
            base_op.next[0].borrow_mut().process_new_tuple();
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
        let mut intersect = BaseIntersect::new(
            base_ei.to_query_vertex.clone(),
            base_ei.to_type,
            base_ei.alds.clone(),
            base_op.out_subgraph.clone(),
            base_op.in_subgraph.clone(),
            base_op.out_qvertex_to_idx_map.clone(),
        );
        let intersect_copy = intersect.clone();
        intersect.base_ei.base_op.prev = Some(Rc::new(RefCell::new(
            base_op.prev.as_ref().unwrap().borrow().copy(is_thread_safe),
        )));
        let last_repeated_vertex_idx = {
            let mut prev = intersect
                .base_ei
                .base_op
                .prev
                .as_mut()
                .unwrap()
                .borrow_mut();
            *get_op_attr_as_mut!(prev.deref_mut(), next) = vec![Rc::new(RefCell::new(
                Operator::EI(EI::Intersect(Intersect::BaseIntersect(intersect_copy))),
            ))];
            get_op_attr!(prev.deref_mut(), last_repeated_vertex_idx)
        };
        intersect.base_ei.init_caching(last_repeated_vertex_idx);
        Operator::EI(EI::Intersect(Intersect::BaseIntersect(intersect)))
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        if let Operator::EI(EI::Intersect(Intersect::BaseIntersect(intersect))) =
            op.borrow_mut().deref_mut()
        {
            return self.base_ei.caching_type == intersect.base_ei.caching_type
                && self.get_alds_as_string() == intersect.base_ei.base_op.get_alds_as_string()
                && self
                    .base_ei
                    .base_op
                    .in_subgraph
                    .as_mut()
                    .unwrap()
                    .is_isomorphic_to(intersect.base_ei.base_op.in_subgraph.as_mut().unwrap())
                && self
                    .base_ei
                    .base_op
                    .out_subgraph
                    .is_isomorphic_to(&mut intersect.base_ei.base_op.out_subgraph)
                && self
                    .base_ei
                    .base_op
                    .prev
                    .as_mut()
                    .unwrap()
                    .borrow_mut()
                    .is_same_as(intersect.base_ei.base_op.prev.as_mut().unwrap());
        }
        false
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_ei.get_num_out_tuples()
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Intersect<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Rc<RefCell<Vec<Id>>>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self {
            Intersect::BaseIntersect(base) => base.init(probe_tuple, graph),
            Intersect::IntersectCatalog(ic) => ic.init(probe_tuple, graph),
        }
    }

    fn process_new_tuple(&mut self) {
        match self {
            Intersect::BaseIntersect(base) => base.process_new_tuple(),
            Intersect::IntersectCatalog(ic) => ic.process_new_tuple(),
        }
    }

    fn execute(&mut self) {
        match self {
            Intersect::BaseIntersect(base) => base.execute(),
            Intersect::IntersectCatalog(ic) => ic.execute(),
        }
    }

    fn get_alds_as_string(&self) -> String {
        match self {
            Intersect::BaseIntersect(base) => base.get_alds_as_string(),
            Intersect::IntersectCatalog(ic) => ic.get_alds_as_string(),
        }
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        match self {
            Intersect::BaseIntersect(base) => base.update_operator_name(query_vertex_to_index_map),
            Intersect::IntersectCatalog(ic) => ic.update_operator_name(query_vertex_to_index_map),
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        match self {
            Intersect::BaseIntersect(base) => base.copy(is_thread_safe),
            Intersect::IntersectCatalog(ic) => ic.copy(is_thread_safe),
        }
    }

    fn is_same_as(&mut self, op: &mut Rc<RefCell<Operator<Id>>>) -> bool {
        match self {
            Intersect::BaseIntersect(base) => base.is_same_as(op),
            Intersect::IntersectCatalog(ic) => ic.is_same_as(op),
        }
    }

    fn get_num_out_tuples(&self) -> usize {
        match self {
            Intersect::BaseIntersect(base) => base.get_num_out_tuples(),
            Intersect::IntersectCatalog(ic) => ic.get_num_out_tuples(),
        }
    }
}
