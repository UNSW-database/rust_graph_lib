use generic::{GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::BaseIntersect;
use graph_impl::multi_graph::plan::operator::extend::EI::CachingType;
use graph_impl::multi_graph::plan::operator::operator::{CommonOperatorTrait, Operator};
use graph_impl::multi_graph::planner::catalog::adj_list_descriptor::AdjListDescriptor;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::hash::{BuildHasherDefault, Hash};

#[derive(Clone)]
pub struct IntersectCatalog<Id: IdType> {
    pub base_intersect: BaseIntersect<Id>,
    is_adj_list_sorted_by_type: bool,
    last_icost: usize,
    caching_enable: bool,
}

impl<Id: IdType> IntersectCatalog<Id> {
    pub fn new(
        to_qvertex: String,
        to_type: usize,
        alds: Vec<AdjListDescriptor>,
        out_subgraph: QueryGraph,
        in_subgraph: QueryGraph,
        out_qvertex_to_idx_map: HashMap<String, usize>,
        is_adj_list_sorted_by_type: bool,
    ) -> IntersectCatalog<Id> {
        IntersectCatalog {
            base_intersect: BaseIntersect::new(
                to_qvertex,
                to_type,
                alds,
                Box::new(out_subgraph),
                Some(Box::new(in_subgraph)),
                out_qvertex_to_idx_map,
            ),
            is_adj_list_sorted_by_type,
            last_icost: 0,
            caching_enable: true,
        }
    }
}

impl<Id: IdType> CommonOperatorTrait<Id> for IntersectCatalog<Id> {
    fn init<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        probe_tuple: Vec<Id>,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) {
        self.base_intersect.init(probe_tuple, graph)
    }

    fn process_new_tuple(&mut self) {
        let base_ei = &mut self.base_intersect.base_ei;
        if 1 == base_ei.alds.len() {
            // intersect the adjacency lists and setAdjListSortOrder the output vertex values.
            let adj = base_ei.adj_lists_to_cache[0]
                [base_ei.base_op.probe_tuple[base_ei.vertex_idx_to_cache[0]].id()]
            .as_ref()
            .unwrap();
            adj.set_neighbor_ids(
                base_ei.labels_or_to_types_to_cache[0],
                &mut base_ei.out_neighbours,
            );
            base_ei.base_op.icost +=
                base_ei.out_neighbours.end_idx - base_ei.out_neighbours.start_idx;
        } else {
            // intersect the adjacency lists and setAdjListSortOrder the output vertex values.
            let mut temp;
            if base_ei.caching_type == CachingType::None || !base_ei.is_intersection_cached() {
                let adj = base_ei.adj_lists_to_cache[0]
                    [base_ei.base_op.probe_tuple[base_ei.vertex_idx_to_cache[0]].id()]
                .as_ref()
                .unwrap();
                adj.set_neighbor_ids(
                    base_ei.labels_or_to_types_to_cache[0],
                    &mut base_ei.init_neighbours,
                );
                self.last_icost =
                    base_ei.init_neighbours.end_idx - base_ei.init_neighbours.start_idx;
                let adj = base_ei.adj_lists_to_cache[1]
                    [base_ei.base_op.probe_tuple[base_ei.vertex_idx_to_cache[1]].id()]
                .as_ref()
                .unwrap();
                self.last_icost += adj.intersect(
                    base_ei.labels_or_to_types_to_cache[1],
                    &mut base_ei.init_neighbours,
                    &mut base_ei.cached_neighbours,
                );
                if base_ei.to_type != 0 {
                    let mut curr_end_idx = 0;
                    for i in base_ei.cached_neighbours.start_idx..base_ei.cached_neighbours.end_idx
                    {
                        if base_ei.vertex_types[base_ei.cached_neighbours.ids[i].id()]
                            == base_ei.to_type
                        {
                            base_ei.cached_neighbours.ids[curr_end_idx] =
                                base_ei.cached_neighbours.ids[i];
                            curr_end_idx += 1;
                        }
                    }
                    base_ei.cached_neighbours.end_idx = curr_end_idx;
                }
                for i in 2..base_ei.adj_lists_to_cache.len() {
                    temp = base_ei.cached_neighbours.clone();
                    base_ei.cached_neighbours = base_ei.temp_neighbours.clone();
                    base_ei.temp_neighbours = temp;
                    let adj = base_ei.adj_lists_to_cache[i]
                        [base_ei.base_op.probe_tuple[base_ei.vertex_idx_to_cache[i]].id()]
                    .as_ref()
                    .unwrap();
                    self.last_icost += adj.intersect(
                        base_ei.labels_or_to_types_to_cache[i],
                        &mut base_ei.temp_neighbours,
                        &mut base_ei.cached_neighbours,
                    );
                }
            }
            match base_ei.caching_type {
                CachingType::None | CachingType::FullCaching => {
                    base_ei.base_op.icost += self.last_icost;
                    base_ei.out_neighbours = base_ei.cached_neighbours.clone();
                }
                CachingType::PartialCaching => {
                    let adj = base_ei.adj_lists[0]
                        [base_ei.base_op.probe_tuple[base_ei.vertex_idx[0]].id()]
                    .as_ref()
                    .unwrap();
                    base_ei.base_op.icost += adj.intersect(
                        base_ei.labels_or_to_types[0],
                        &mut base_ei.cached_neighbours,
                        &mut base_ei.out_neighbours,
                    );
                    for i in 1..base_ei.adj_lists.len() {
                        temp = base_ei.out_neighbours.clone();
                        base_ei.out_neighbours = base_ei.temp_neighbours.clone();
                        base_ei.temp_neighbours = temp;
                        let adj = base_ei.adj_lists[i]
                            [base_ei.base_op.probe_tuple[base_ei.vertex_idx[i]].id()]
                        .as_ref()
                        .unwrap();
                        base_ei.base_op.icost += adj.intersect(
                            base_ei.labels_or_to_types[i],
                            &mut base_ei.temp_neighbours,
                            &mut base_ei.out_neighbours,
                        );
                    }
                }
            }
        }

        for idx in base_ei.out_neighbours.start_idx..base_ei.out_neighbours.end_idx {
            base_ei.base_op.probe_tuple[base_ei.out_idx] = base_ei.out_neighbours.ids[idx];
            base_ei.base_op.num_out_tuples += 1;
            if self.is_adj_list_sorted_by_type {
                base_ei.base_op.next[0].process_new_tuple();
            } else {
                base_ei
                    .base_op
                    .next
                    .get_mut(
                        base_ei.vertex_types[base_ei.base_op.probe_tuple[base_ei.out_idx].id()],
                    )
                    .map(|next_op| next_op.process_new_tuple());
            }
        }
    }

    fn execute(&mut self) {
        self.base_intersect.execute()
    }

    fn get_alds_as_string(&self) -> String {
        self.base_intersect.get_alds_as_string()
    }

    fn update_operator_name(&mut self, query_vertex_to_index_map: HashMap<String, usize>) {
        self.base_intersect
            .update_operator_name(query_vertex_to_index_map)
    }

    fn copy(&self, is_thread_safe: bool) -> Operator<Id> {
        self.base_intersect.copy(is_thread_safe)
    }

    fn is_same_as(&mut self, op: &mut Operator<Id>) -> bool {
        self.base_intersect.is_same_as(op)
    }

    fn get_num_out_tuples(&self) -> usize {
        self.base_intersect.get_num_out_tuples()
    }
}
