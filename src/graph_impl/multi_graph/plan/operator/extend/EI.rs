use generic::{GraphType, IdType};
use graph_impl::multi_graph::catalog::adj_list_descriptor::{AdjListDescriptor, Direction};
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::plan::operator::extend::extend::Extend;
use graph_impl::multi_graph::plan::operator::extend::intersect::{Intersect, IntersectType};
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::{
    BaseOperator, CommonOperatorTrait, Operator,
};
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::static_graph::sorted_adj_vec::SortedAdjVec;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use indexmap::Equivalent;
use itertools::Itertools;
use std::hash::{BuildHasherDefault, Hash};

pub static DIFFERENTIATE_FWD_BWD_SINGLE_ALD: bool = false;

#[derive(Clone)]
pub enum CachingType {
    None,
    FullCaching,
    PartialCaching,
}

impl PartialEq for CachingType {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (CachingType::None, CachingType::None)
            | (CachingType::FullCaching, CachingType::FullCaching)
            | (CachingType::PartialCaching, CachingType::PartialCaching) => true,
            _ => false,
        }
    }
}

#[derive(Clone)]
pub enum EI<Id: IdType> {
    Base(BaseEI<Id>),
    Extend(Extend<Id>),
    Intersect(Intersect<Id>),
}

#[derive(Clone)]
pub struct BaseEI<Id: IdType> {
    pub base_op: BaseOperator<Id>,
    pub vertex_types: Vec<usize>,
    pub to_type: usize,
    pub to_query_vertex: String,
    pub alds: Vec<AdjListDescriptor>,
    pub out_idx: usize,
    pub vertex_idx: Vec<usize>,
    pub vertex_idx_to_cache: Vec<usize>,
    pub labels_or_to_types: Vec<usize>,
    pub labels_or_to_types_to_cache: Vec<usize>,
    pub adj_lists: Vec<Vec<Option<SortedAdjVec<Id>>>>,
    pub adj_lists_to_cache: Vec<Vec<Option<SortedAdjVec<Id>>>>,
    pub caching_type: CachingType,
    is_intersection_cached: bool,
    last_vertex_ids_intersected: Option<Vec<Id>>,
    pub out_neighbours: Neighbours<Id>,
    pub init_neighbours: Neighbours<Id>,
    pub temp_neighbours: Neighbours<Id>,
    pub cached_neighbours: Neighbours<Id>,
}

impl<Id: IdType> BaseEI<Id> {
    pub fn new(
        to_query_vertex: String,
        to_type: usize,
        alds: Vec<AdjListDescriptor>,
        out_subgraph: Box<QueryGraph>,
        in_subgraph: Option<Box<QueryGraph>>,
    ) -> BaseEI<Id> {
        let mut ei = BaseEI {
            base_op: BaseOperator::new(out_subgraph, in_subgraph),
            vertex_types: vec![],
            to_type,
            to_query_vertex,
            alds,
            out_idx: 0,
            vertex_idx: vec![],
            vertex_idx_to_cache: vec![],
            labels_or_to_types: vec![],
            labels_or_to_types_to_cache: vec![],
            adj_lists: vec![],
            adj_lists_to_cache: vec![],
            caching_type: CachingType::None,
            is_intersection_cached: false,
            last_vertex_ids_intersected: None,
            out_neighbours: Neighbours::new(),
            init_neighbours: Neighbours::new(),
            temp_neighbours: Neighbours::new(),
            cached_neighbours: Neighbours::new(),
        };
        ei.set_operator_name();
        ei
    }

    fn set_operator_name(&mut self) {
        let variables = self
            .alds
            .iter()
            .map(|ald| {
                ald.from_query_vertex.clone()
                    + "["
                    + if let Direction::Fwd = ald.direction {
                        "Fwd"
                    } else {
                        "Bwd"
                    }
                    + "]"
            })
            .sorted()
            .join("-");
        if 1 == self.alds.len() {
            self.base_op.name = "Single-Edge-extend".to_string();
        } else {
            self.base_op.name = "Multi-Edge-extend".to_string();
        }
        self.base_op.name +=
            &(" TO (".to_owned() + &self.to_query_vertex + ") From (" + &variables + ")");
    }

    pub fn make(
        to_qvertex: String,
        to_type: usize,
        alds: Vec<AdjListDescriptor>,
        out_subgraph: QueryGraph,
        in_subgraph: QueryGraph,
        out_qvertex_to_idx_map: HashMap<String, usize>,
    ) -> EI<Id> {
        if 1 == alds.len() {
            return EI::Extend(Extend::new(
                to_qvertex,
                to_type,
                alds,
                Box::new(out_subgraph),
                Some(Box::new(in_subgraph)),
                out_qvertex_to_idx_map,
            ));
        }
        EI::Intersect(Intersect::new(
            to_qvertex,
            to_type,
            alds,
            Box::new(out_subgraph),
            Some(Box::new(in_subgraph)),
            out_qvertex_to_idx_map,
        ))
    }

    pub fn is_intersection_cached(&mut self) -> bool {
        self.is_intersection_cached = true;
        for i in 0..self.last_vertex_ids_intersected.as_ref().unwrap().len() {
            if self.last_vertex_ids_intersected.as_ref().unwrap()[i]
                != self.base_op.probe_tuple[self.vertex_idx_to_cache[i]]
            {
                self.is_intersection_cached = false;
                self.last_vertex_ids_intersected.as_mut().unwrap()[i] =
                    self.base_op.probe_tuple[self.vertex_idx_to_cache[i]];
            }
        }
        self.is_intersection_cached
    }
    pub fn init_caching(&mut self, last_repeated_vertex_idx: usize) {
        if self.alds.len() == 1 {
            return;
        }
        let num_cached_alds = self
            .alds
            .iter()
            .filter(|&ald| ald.vertex_idx <= last_repeated_vertex_idx)
            .count();
        if num_cached_alds <= 1 {
            return;
        }
        if num_cached_alds == self.alds.len() {
            self.caching_type = CachingType::FullCaching;
        } else {
            self.caching_type = CachingType::PartialCaching;
        }
        self.last_vertex_ids_intersected = Some(vec![Id::new(0); num_cached_alds]);
    }

    fn init_extensions<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self.caching_type {
            CachingType::None | CachingType::FullCaching => {
                *&mut self.out_neighbours = Neighbours::new();
                if 1 == self.alds.len() {
                    return;
                }
            }
            _ => {}
        }
        let mut largest_adj_list_size = 0;
        for ald in &self.alds {
            let label = if graph.is_sorted_by_node() {
                self.to_type
            } else {
                ald.label
            };
            let adj_list_size = graph.get_largest_adj_list_size(label, ald.direction.clone());
            if adj_list_size > largest_adj_list_size {
                largest_adj_list_size = adj_list_size;
            }
        }
        if let CachingType::PartialCaching = self.caching_type {
            self.out_neighbours = Neighbours::with_capacity(largest_adj_list_size);
        }
        self.init_neighbours = Neighbours::new();
        self.cached_neighbours = Neighbours::with_capacity(largest_adj_list_size);
        if self.alds.len() > 2 {
            self.temp_neighbours = Neighbours::with_capacity(largest_adj_list_size);
        }
    }

    fn set_alds_and_adj_lists<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        last_repeated_vertex_idx: usize,
    ) {
        let num_cached_alds = if let Some(ids) = &mut self.last_vertex_ids_intersected {
            ids.len()
        } else {
            self.alds.len()
        };
        self.vertex_idx_to_cache = vec![0; num_cached_alds];
        self.labels_or_to_types_to_cache = vec![0; num_cached_alds];
        self.adj_lists_to_cache = vec![vec![]; num_cached_alds];
        if let CachingType::PartialCaching = self.caching_type {
            self.vertex_idx = vec![0; self.alds.len() - num_cached_alds];
            self.labels_or_to_types = vec![0; self.alds.len() - num_cached_alds];
            self.adj_lists = vec![vec![]; self.alds.len() - num_cached_alds];
        }
        let mut idx = 0;
        let mut idx_to_cache = 0;
        for ald in &self.alds {
            if let CachingType::PartialCaching = self.caching_type {
                if ald.vertex_idx > last_repeated_vertex_idx {
                    self.vertex_idx[idx] = ald.vertex_idx;
                    self.labels_or_to_types[idx] = if graph.is_sorted_by_node() {
                        self.to_type
                    } else {
                        ald.label
                    };
                    self.adj_lists[idx] = if let Direction::Fwd = ald.direction {
                        graph.get_fwd_adj_list().clone()
                    } else {
                        graph.get_bwd_adj_list().clone()
                    };
                    idx += 1;
                    continue;
                }
            }
            self.vertex_idx_to_cache[idx_to_cache] = ald.vertex_idx;
            self.labels_or_to_types_to_cache[idx_to_cache] = if graph.is_sorted_by_node() {
                self.to_type
            } else {
                ald.label
            };
            self.adj_lists_to_cache[idx_to_cache] = if let Direction::Fwd = ald.direction {
                graph.get_fwd_adj_list().clone()
            } else {
                graph.get_bwd_adj_list().clone()
            };
            idx_to_cache += 1;
        }
    }

    pub fn execute_intersect(&mut self, idx: usize, intersect_type: IntersectType) -> usize {
        let (adj_vec, label_or_type) = match intersect_type {
            IntersectType::CachedOut | IntersectType::TempOut => (
                self.adj_lists_to_cache[idx][self.base_op.probe_tuple[self.vertex_idx[idx]].id()]
                    .as_ref(),
                self.labels_or_to_types[idx],
            ),
            _ => (
                self.adj_lists_to_cache[idx]
                    [self.base_op.probe_tuple[self.vertex_idx_to_cache[idx]].id()]
                .as_ref(),
                self.labels_or_to_types_to_cache[idx],
            ),
        };
        let init = &mut self.init_neighbours;
        let cached = &mut self.cached_neighbours;
        let temp = &mut self.temp_neighbours;
        let out = &mut self.out_neighbours;
        adj_vec.map_or(0, |adj| match intersect_type {
            IntersectType::InitCached => adj.intersect(label_or_type, init, cached),
            IntersectType::TempCached => adj.intersect(label_or_type, temp, cached),
            IntersectType::CachedOut => adj.intersect(label_or_type, cached, out),
            IntersectType::TempOut => adj.intersect(label_or_type, temp, out),
        })
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for BaseEI<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_op.probe_tuple = probe_tuple.clone();
        self.caching_type = CachingType::None;
        self.vertex_types = graph.get_node_types().clone();
        let prev = self.base_op.prev.as_mut().unwrap().as_mut();
        let last_repeated_vertex_idx = get_op_attr_as_mut!(prev, last_repeated_vertex_idx).clone();
        self.init_caching(last_repeated_vertex_idx);
        self.init_extensions(graph);
        self.set_alds_and_adj_lists(graph, last_repeated_vertex_idx);
        self.base_op
            .next
            .as_mut()
            .unwrap()
            .iter_mut()
            .foreach(|next_op| {
                next_op.init(probe_tuple.clone(), graph);
            });
    }

    fn process_new_tuple(&mut self) {
        panic!("unsupported operation exception")
    }

    fn execute(&mut self) {
        self.base_op.execute()
    }

    fn get_alds_as_string(&self) -> String {
        if !DIFFERENTIATE_FWD_BWD_SINGLE_ALD && 1 == self.alds.len() {
            return "E".to_owned() + &self.alds[0].label.to_string();
        }
        let mut directions = vec!["".to_owned(); self.alds.len()];
        for ald in &self.alds {
            let dir = if let Direction::Fwd = ald.direction {
                "F".to_owned()
            } else {
                "B".to_owned()
            };
            directions.push(dir + &ald.label.to_string());
        }
        directions.sort();
        directions.join("-")
    }

    fn update_operator_name(&mut self, mut query_vertex_to_index_map: HashMap<String, usize>) {
        let mut prev_to_query_vertices = vec!["".to_owned(); query_vertex_to_index_map.len()];
        for (query_vertex, &index) in &query_vertex_to_index_map {
            prev_to_query_vertices[index] = query_vertex.clone();
        }
        self.base_op.name =
            serde_json::to_string(&prev_to_query_vertices).unwrap() + " - " + &self.base_op.name;
        query_vertex_to_index_map.insert(
            self.to_query_vertex.clone(),
            query_vertex_to_index_map.len(),
        );
        if let Some(next) = &mut self.base_op.next {
            next.iter_mut()
                .foreach(|op| op.update_operator_name(query_vertex_to_index_map.clone()))
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        panic!("unsupported operation exception")
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        panic!("unsupported operation exception")
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_op.num_out_tuples
    }
}

impl<Id: IdType> EI<Id> {
    pub fn has_multi_edge_extends(&self) -> bool {
        let base_ei = get_ei_as_ref!(self);
        if base_ei.alds.len() > 1 {
            return true;
        }
        if let Some(prev) = &base_ei.base_op.prev {
            return prev.has_multi_edge_extends();
        }
        false
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for EI<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        match self {
            EI::Base(base) => base.init(probe_tuple, graph),
            EI::Intersect(intersect) => intersect.init(probe_tuple, graph),
            EI::Extend(extend) => extend.init(probe_tuple, graph),
        }
    }

    fn process_new_tuple(&mut self) {
        match self {
            EI::Base(base) => base.process_new_tuple(),
            EI::Intersect(intersect) => intersect.process_new_tuple(),
            EI::Extend(extend) => extend.process_new_tuple(),
        }
    }

    fn execute(&mut self) {
        match self {
            EI::Base(base) => base.execute(),
            EI::Intersect(intersect) => intersect.execute(),
            EI::Extend(extend) => extend.execute(),
        }
    }

    fn get_alds_as_string(&self) -> String {
        match self {
            EI::Base(base) => base.get_alds_as_string(),
            EI::Intersect(intersect) => intersect.get_alds_as_string(),
            EI::Extend(extend) => extend.get_alds_as_string(),
        }
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        match self {
            EI::Base(base) => base.update_operator_name(query_vertex_to_index_map),
            EI::Intersect(intersect) => intersect.update_operator_name(query_vertex_to_index_map),
            EI::Extend(extend) => extend.update_operator_name(query_vertex_to_index_map),
        }
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        match self {
            EI::Base(base) => base.copy(is_thread_safe),
            EI::Intersect(intersect) => intersect.copy(is_thread_safe),
            EI::Extend(extend) => extend.copy(is_thread_safe),
        }
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        match self {
            EI::Base(base) => base.is_same_as(op),
            EI::Intersect(intersect) => intersect.is_same_as(op),
            EI::Extend(extend) => extend.is_same_as(op),
        }
    }

    fn get_num_out_tuples(&self) -> usize {
        match self {
            EI::Base(base) => base.get_num_out_tuples(),
            EI::Intersect(intersect) => intersect.get_num_out_tuples(),
            EI::Extend(extend) => extend.get_num_out_tuples(),
        }
    }
}

#[derive(Clone)]
pub struct Neighbours<Id: IdType> {
    pub ids: Vec<Id>,
    pub start_idx: usize,
    pub end_idx: usize,
}

impl<Id: IdType> Neighbours<Id> {
    pub fn new() -> Neighbours<Id> {
        Neighbours {
            ids: vec![],
            start_idx: 0,
            end_idx: 0,
        }
    }

    pub fn with_capacity(capacity: usize) -> Neighbours<Id> {
        Neighbours {
            ids: vec![Id::new(0); capacity],
            start_idx: 0,
            end_idx: 0,
        }
    }

    pub fn reset(&mut self) {
        self.start_idx = 0;
        self.end_idx = 0;
    }
}
