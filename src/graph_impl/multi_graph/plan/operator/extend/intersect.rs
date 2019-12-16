use graph_impl::multi_graph::plan::operator::extend::EI::{BaseEI, Neighbours, CachingType, EI};
use graph_impl::multi_graph::plan::operator::extend::EI::EI::Base;
use graph_impl::multi_graph::catalog::query_graph::QueryGraph;
use hashbrown::HashMap;
use graph_impl::multi_graph::catalog::adj_list_descriptor::AdjListDescriptor;
use generic::{IdType, GraphType};
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::TypedStaticGraph;
use std::hash::Hash;

pub enum IntersectType {
    InitCached,
    TempCached,
    CachedOut,
    TempOut,
}

#[derive(Clone)]
pub struct Intersect<Id: IdType> {
    pub base_ei: BaseEI<Id>,
}

impl<Id: IdType> Intersect<Id> {
    pub fn new(to_qvertex: String, to_type: usize, alds: Vec<AdjListDescriptor>,
               out_subgraph: Box<QueryGraph>, in_subgraph: Option<Box<QueryGraph>>,
               out_qvertex_to_idx_map: HashMap<String, usize>) -> Intersect<Id> {
        let mut intersect = Intersect {
            base_ei: BaseEI::new(to_qvertex.clone(), to_type, alds, out_subgraph, in_subgraph),
        };
        let base_op = &mut intersect.base_ei.base_op;
        base_op.last_repeated_vertex_idx = base_op.out_tuple_len - 2;
        base_op.out_qvertex_to_idx_map = out_qvertex_to_idx_map;
        intersect.base_ei.out_idx = base_op.out_qvertex_to_idx_map.get(&to_qvertex).unwrap().clone();
        intersect
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for Intersect<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(&mut self, probe_tuple: Vec<Id>, graph: &TypedStaticGraph<Id, NL, EL, Ty, L>) {
        unimplemented!()
    }

    fn process_new_tuple(&mut self) {
        let mut temp = Neighbours::new();
        if CachingType::None == self.base_ei.caching_type || !self.base_ei.is_intersection_cached() {
            let base_ei = &mut self.base_ei;
            let mut cache_id = base_ei.vertex_idx_to_cache[0];
            let to_id = base_ei.base_op.probe_tuple[cache_id].id();
            let mut adj_vec = base_ei.adj_lists_to_cache[0][to_id].as_ref();
            cache_id = base_ei.labels_or_to_types_to_cache[0];
            let neighbours = &mut base_ei.init_neighbours;
            adj_vec.map(|adj| adj.set_neighbor_ids(cache_id, neighbours));
            base_ei.base_op.icost += (base_ei.init_neighbours.end_idx - base_ei.init_neighbours.start_idx);
            base_ei.base_op.icost += base_ei.execute_intersect(1, IntersectType::InitCached);

            if base_ei.to_type != 0 {
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
            CachingType::None | CachingType::FullCaching => base_ei.out_neighbours = base_ei.cached_neighbours.clone(),
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
        base_op.num_out_tuples += (out_neighbours.end_idx - out_neighbours.start_idx);
        for idx in out_neighbours.start_idx..out_neighbours.end_idx {
            base_op.probe_tuple[base_ei.out_idx] = out_neighbours.ids[idx];
            base_op.next.as_mut().map(|op_vec| {
                op_vec.get_mut(0).map(|op| op.process_new_tuple());
            });
        }
    }

    fn execute(&mut self) {
        unimplemented!()
    }

    fn copy(&self, is_thread_safe: bool) -> Option<Operator<Id>> {
        let base_ei = &self.base_ei;
        let base_op = &base_ei.base_op;
        let mut intersect = Intersect::new(
            base_ei.to_query_vertex.clone(), base_ei.to_type,
            base_ei.alds.clone(),
            base_op.out_subgraph.clone(), base_op.in_subgraph.clone(),
            base_op.out_qvertex_to_idx_map.clone(),
        );
        let intersect_copy = intersect.clone();
        intersect.base_ei.base_op.prev = base_op.prev.as_ref().unwrap().copy(is_thread_safe).map(|op| Box::new(op));
        let prev = intersect.base_ei.base_op.prev.as_mut().unwrap().as_mut();
        *get_op_attr_as_mut!(prev,next) = Some(vec![Operator::EI(EI::Intersect(intersect_copy))]);
        let last_repeated_vertex_idx = get_op_attr!(prev,last_repeated_vertex_idx);
        intersect.base_ei.init_caching(last_repeated_vertex_idx);
        Some(Operator::EI(EI::Intersect(intersect)))
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        if let Operator::EI(EI::Intersect(intersect)) = op {
            return
                self.base_ei.caching_type == intersect.base_ei.caching_type &&
                    self.get_alds_as_string() == intersect.get_alds_as_string() &&
                    self.base_ei.base_op.in_subgraph.as_mut().unwrap().is_isomorphic_to(get_op_attr_as_mut!(op,in_subgraph).as_mut().unwrap().as_mut()) &&
                    self.base_ei.base_op.out_subgraph.is_isomorphic_to(get_op_attr_as_mut!(op,out_subgraph).as_mut()) &&
                    self.base_ei.base_op.prev.as_mut().unwrap().is_same_as(get_op_attr_as_mut!(op,prev).as_mut().unwrap());
        }
        false
    }
}