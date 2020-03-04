use generic::{GraphLabelTrait, GraphTrait, GraphType, IdType};
use graph_impl::multi_graph::plan::operator::extend::intersect::Intersect;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::Probe;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::PMV;
use graph_impl::multi_graph::plan::operator::operator::Operator;
use graph_impl::multi_graph::plan::operator::scan::scan::{BaseScan, Scan};
use graph_impl::multi_graph::plan::operator::scan::scan_sampling::ScanSampling;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::multi_graph::planner::catalog::adj_list_descriptor::{
    AdjListDescriptor, Direction,
};
use graph_impl::multi_graph::planner::catalog::operator::intersect_catalog::IntersectCatalog;
use graph_impl::multi_graph::planner::catalog::operator::noop::Noop;
use graph_impl::multi_graph::planner::catalog::query_edge::QueryEdge;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use graph_impl::multi_graph::query::query_graph_set::QueryGraphSet;
use graph_impl::multi_graph::utils::set_utils;
use graph_impl::static_graph::graph::KEY_ANY;
use graph_impl::TypedStaticGraph;
use hashbrown::HashMap;
use std::cell::RefCell;
use std::hash::Hash;
use std::ops::{Deref, DerefMut};
use std::rc::Rc;
use itertools::Itertools;

pub static DEF_NUM_EDGES_TO_SAMPLE: usize = 1000;
pub static DEF_MAX_INPUT_NUM_VERTICES: usize = 3;
static QUERY_VERTICES: [&str; 7] = ["a", "b", "c", "d", "e", "f", "g"];

pub struct CatalogPlans<Id: IdType> {
    num_sampled_edges: usize,
    max_input_num_vertices: usize,
    num_node_labels: usize,
    num_edge_labels: usize,
    sorted_by_node: bool,
    pub query_graphs_to_extend: QueryGraphSet,
    pub query_plans_arrs: Vec<Vec<QueryPlan<Id>>>,
    is_directed: bool,
    pub selectivity_zero: Vec<(QueryGraph, Vec<AdjListDescriptor>, i32)>,
    query_vertex_to_idx_map: HashMap<String, usize>,
}

impl<Id: IdType> CatalogPlans<Id> {
    pub fn new<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        num_thread: usize,
        num_sampled_edges: usize,
        max_input_num_vertices: usize,
    ) -> Self {
        let mut plans = CatalogPlans {
            num_sampled_edges,
            max_input_num_vertices,
            num_node_labels: graph.num_of_node_labels(),
            num_edge_labels: graph.num_of_edge_labels(),
            sorted_by_node: graph.is_sorted_by_node(),
            query_graphs_to_extend: QueryGraphSet::new(),
            query_plans_arrs: vec![],
            is_directed: graph.is_directed(),
            selectivity_zero: vec![],
            query_vertex_to_idx_map: HashMap::new(),
        };
        for i in 0..QUERY_VERTICES.len() {
            plans
                .query_vertex_to_idx_map
                .insert(QUERY_VERTICES[i].to_owned(), i);
        }
        let scans = if graph.edge_count() > 1073741823 {
            plans.generate_all_scans_for_large_graph(graph)
        } else {
            plans.generate_all_scans(graph)
        };
        for scan in scans {
            let mut noop = Noop::new(scan.base_scan.base_op.out_subgraph.clone());
            let scan_pointer = Rc::new(RefCell::new(Operator::Scan(Scan::ScanSampling(scan))));
            noop.base_op.prev = Some(scan_pointer.clone());
            noop.base_op.out_qvertex_to_idx_map = get_op_attr_as_ref!(scan_pointer.borrow().deref(), out_qvertex_to_idx_map).clone();
            let mut noop_pointer = Rc::new(RefCell::new(Operator::Noop(noop)));
            *get_op_attr_as_mut!(scan_pointer.borrow_mut().deref_mut(), next) = vec![noop_pointer.clone()];
            plans.set_next_operators(graph, noop_pointer, false);
            let mut query_plans_arr = vec![QueryPlan::new(scan_pointer.clone())];
            for i in 1..num_thread {
                let mut scan_ref = scan_pointer.borrow();
                let scan_copy = if let Operator::Scan(Scan::ScanSampling(scan)) = scan_ref.deref() {
                    scan.copy_default()
                } else {
                    panic!("Scan initial failed!");
                };
                let mut another_noop =
                    Noop::new(get_op_attr_as_ref!(&scan_copy, out_subgraph).clone());
                another_noop.base_op.out_qvertex_to_idx_map =
                    get_op_attr_as_ref!(&scan_copy, out_qvertex_to_idx_map).clone();
                let mut scan_copy = Rc::new(RefCell::new(scan_copy));
                another_noop.base_op.prev = Some(scan_copy.clone());
                let mut another_noop_op = Rc::new(RefCell::new(Operator::Noop(another_noop)));
                *get_op_attr_as_mut!(scan_copy.borrow_mut().deref_mut(), next) =
                    vec![another_noop_op.clone()];
                plans.set_next_operators(graph, another_noop_op, true);
                let scan_copy_ref = scan_copy.borrow();
                if let Operator::Scan(Scan::ScanSampling(sc)) = scan_copy_ref.deref() {
                    query_plans_arr.push(QueryPlan::new(scan_copy.clone()));
                }
            }
            plans.query_plans_arrs.push(query_plans_arr);
        }
        plans
    }

    pub fn set_next_operators<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
        operator: Rc<RefCell<Operator<Id>>>,
        is_none: bool,
    ) {
        let mut in_subgraph = {
            let op_ref = operator.borrow();
            get_op_attr_as_ref!(op_ref.deref(), out_subgraph).clone()
        };
        if !is_none && !self.query_graphs_to_extend.contains(&mut in_subgraph) {
            self.query_graphs_to_extend.add(in_subgraph.clone());
        } else if !is_none {
            return;
        }

        let query_vertices = in_subgraph.get_query_vertices().clone();
        let mut descriptors = vec![];
        for query_vertex_to_extend in set_utils::get_power_set_excluding_empty_set(query_vertices) {
            for alds in self.generate_alds(&query_vertex_to_extend, self.is_directed) {
                descriptors.push(Descriptor {
                    out_subgraph: self.get_out_subgraph(in_subgraph.copy(), alds.clone()),
                    alds,
                });
            }
        }
        let to_qvertex = QUERY_VERTICES[in_subgraph.get_num_qvertices()];
        let mut next = vec![];
        let last_repeated_vertex_idx =
            get_op_attr_as_ref!(operator.borrow().deref(), last_repeated_vertex_idx).clone();

        if self.sorted_by_node {
            for mut descriptor in descriptors {
                let mut types = vec![];
                let node_label_cnt = std::cmp::max(self.num_node_labels, 1);
                for to_type in 0..node_label_cnt {
                    let mut produces_output = true;
                    for ald in &descriptor.alds {
                        let from_type = in_subgraph.get_query_vertex_type(&ald.from_query_vertex);
                        if (ald.direction == Direction::Fwd
                            && 0 == graph.get_num_edges(from_type, to_type as i32, ald.label))
                            || (ald.direction == Direction::Bwd
                                && 0 == graph.get_num_edges(to_type as i32, from_type, ald.label))
                        {
                            produces_output = false;
                            break;
                        }
                    }
                    if produces_output {
                        types.push(to_type as i32);
                    } else {
                        self.selectivity_zero.push((
                            in_subgraph.clone(),
                            descriptor.alds.clone(),
                            to_type as i32,
                        ));
                    }
                }
                let mut out_qvertex_to_idx_map =
                    get_op_attr_as_ref!(operator.borrow().deref(), out_qvertex_to_idx_map).clone();
                out_qvertex_to_idx_map.insert(to_qvertex.to_owned(), out_qvertex_to_idx_map.len());
                for to_type in types {
                    descriptor
                        .out_subgraph
                        .set_query_vertex_type(to_qvertex.to_owned(), to_type);
                    let mut intersect = IntersectCatalog::new(
                        to_qvertex.to_owned(),
                        to_type,
                        descriptor.alds.clone(),
                        descriptor.out_subgraph.clone(),
                        in_subgraph.clone(),
                        out_qvertex_to_idx_map.clone(),
                        self.sorted_by_node,
                    );
                    intersect
                        .base_intersect
                        .base_ei
                        .init_caching(last_repeated_vertex_idx);
                    next.push(Rc::new(RefCell::new(Operator::EI(EI::Intersect(
                        Intersect::IntersectCatalog(intersect),
                    )))));
                }
            }
        } else {
            for i in 0..descriptors.len() {
                let descriptor = &descriptors[i];
                let mut out_qvertex_to_idx_map = {
                    let op_ref = operator.borrow();
                    let prev = get_op_attr_as_ref!(op_ref.deref(), prev)
                        .as_ref()
                        .unwrap()
                        .clone();
                    let prev_ref = prev.borrow();
                    get_op_attr_as_ref!(prev_ref.deref(), out_qvertex_to_idx_map).clone()
                };

                out_qvertex_to_idx_map.insert(to_qvertex.to_owned(), out_qvertex_to_idx_map.len());
                let mut ic = IntersectCatalog::new(
                    to_qvertex.to_owned(),
                    KEY_ANY,
                    descriptor.alds.clone(),
                    descriptor.out_subgraph.clone(),
                    in_subgraph.clone(),
                    out_qvertex_to_idx_map,
                    self.sorted_by_node,
                );
                ic.base_intersect
                    .base_ei
                    .init_caching(last_repeated_vertex_idx);
                next.push(Rc::new(RefCell::new(Operator::EI(EI::Intersect(
                    Intersect::IntersectCatalog(ic),
                )))));
            }
        }
        Self::set_next_pointer(operator.clone(), next.clone());
        for next_op in next {
            let mut next_noops = if self.sorted_by_node {
                vec![Noop::new(QueryGraph::empty()); 1]
            } else {
                vec![Noop::new(QueryGraph::empty()); self.num_node_labels + 1]
            };
            self.set_noops(
                get_op_attr_as_ref!(next_op.borrow().deref(), out_subgraph),
                to_qvertex.to_owned(),
                &mut next_noops,
                get_op_attr_as_ref!(next_op.borrow().deref(), out_qvertex_to_idx_map),
            );
            let next_noops: Vec<Rc<RefCell<Operator<Id>>>> = next_noops
                .into_iter()
                .map(|noop| Rc::new(RefCell::new(Operator::Noop(noop))))
                .collect();
            Self::set_next_pointer(next_op.clone(), next_noops.clone());
            if get_op_attr_as_ref!(next_op.borrow().deref(), out_subgraph).get_num_qvertices()
                <= self.max_input_num_vertices
            {
                println!(
                    "next_noops_cnt={},sort_by_node={},max_input_num_vertices={}",
                    next_noops.len(),
                    self.sorted_by_node,
                    self.max_input_num_vertices
                );
                for next_noop in next_noops {
                    *get_op_attr_as_mut!(
                        next_noop.borrow_mut().deref_mut(),
                        last_repeated_vertex_idx
                    ) = last_repeated_vertex_idx;
                    self.set_next_operators(graph, next_noop, is_none)
                }
            }
        }
    }

    fn set_next_pointer(operator: Rc<RefCell<Operator<Id>>>, next: Vec<Rc<RefCell<Operator<Id>>>>) {
        *get_op_attr_as_mut!(operator.borrow_mut().deref_mut(), next) = next.clone();
        for next_op in next {
            *get_op_attr_as_mut!(next_op.borrow_mut().deref_mut(), prev) = Some(operator.clone());
        }
    }

    fn set_noops(
        &self,
        query_graph: &QueryGraph,
        to_qvertex: String,
        noops: &mut Vec<Noop<Id>>,
        out_qvertex_to_idx_map: &HashMap<String, usize>,
    ) {
        if self.sorted_by_node {
            noops[0] = Noop::new(query_graph.clone());
            noops[0].base_op.out_qvertex_to_idx_map = out_qvertex_to_idx_map.clone();
        } else {
            let node_label_cnt = std::cmp::max(self.num_node_labels, 1);
            for to_type in 0..node_label_cnt {
                let mut query_graph_copy = query_graph.copy();
                query_graph_copy.set_query_vertex_type(to_qvertex.clone(), to_type as i32);
                noops[to_type] = Noop::new(query_graph_copy);
            }
        }
    }

    fn generate_alds(
        &self,
        qvertices: &Vec<String>,
        is_direccted: bool,
    ) -> Vec<Vec<AdjListDescriptor>> {
        let direction_patterns = Self::generate_direction_patterns(qvertices.len(), is_direccted);
        let label_patterns = self.generate_labels_patterns(qvertices.len());
        let mut alds_list = vec![];
        for directions in direction_patterns {
            for labels in &label_patterns {
                let mut alds = vec![];
                for i in 0..directions.len() {
                    let vertex_idx = self.query_vertex_to_idx_map[&qvertices[i]];
                    let to_qvertex = QUERY_VERTICES[vertex_idx];
                    alds.push(AdjListDescriptor::new(
                        to_qvertex.to_owned(),
                        vertex_idx,
                        directions[i].clone(),
                        labels[i].clone(),
                    ));
                }
                alds_list.push(alds);
            }
        }
        alds_list
    }

    fn generate_labels_patterns(&self, size: usize) -> Vec<Vec<i32>> {
        let mut labels = vec![];
        let edge_label_cnt = std::cmp::max(self.num_edge_labels, 1);
        for label in 0..edge_label_cnt {
            labels.push(label as i32);
        }
        set_utils::generate_permutations(labels, size)
    }

    pub fn generate_direction_patterns(size: usize, is_directed: bool) -> Vec<Vec<Direction>> {
        let mut direction_patterns = vec![];
        Self::generate_direction_patterns_inner(
            &mut vec![Direction::Fwd; size],
            size,
            &mut direction_patterns,
            is_directed,
        );
        direction_patterns
    }

    fn generate_direction_patterns_inner(
        direction_arr: &mut Vec<Direction>,
        size: usize,
        direction_patterns: &mut Vec<Vec<Direction>>,
        is_directed: bool,
    ) {
        if size <= 0 {
            direction_patterns.push(direction_arr.clone());
        } else {
            direction_arr[size - 1] = Direction::Bwd;
            Self::generate_direction_patterns_inner(
                direction_arr,
                size - 1,
                direction_patterns,
                is_directed,
            );
            if is_directed {
                direction_arr[size - 1] = Direction::Fwd;
                Self::generate_direction_patterns_inner(
                    direction_arr,
                    size - 1,
                    direction_patterns,
                    is_directed,
                );
            }
        }
    }

    fn get_out_subgraph(
        &self,
        mut query_graph: QueryGraph,
        alds: Vec<AdjListDescriptor>,
    ) -> QueryGraph {
        let num_qvertices = query_graph.get_num_qvertices();
        for ald in alds {
            let mut query_edge = if let Direction::Fwd = ald.direction {
                let mut query_edge = QueryEdge::default(
                    ald.from_query_vertex.clone(),
                    QUERY_VERTICES[num_qvertices].to_owned(),
                );
                query_edge.from_type = query_graph.get_query_vertex_type(&ald.from_query_vertex);
                query_edge
            } else {
                let mut query_edge = QueryEdge::default(
                    QUERY_VERTICES[num_qvertices].to_owned(),
                    ald.from_query_vertex.clone(),
                );
                query_edge.to_type = query_graph.get_query_vertex_type(&ald.from_query_vertex);
                query_edge
            };
            query_edge.label = ald.label;
            query_graph.add_qedge(query_edge);
        }
        query_graph
    }

    pub fn generate_all_scans_for_large_graph<
        NL: Hash + Eq,
        EL: Hash + Eq,
        Ty: GraphType,
        L: IdType,
    >(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) -> Vec<ScanSampling<Id>> {
        let fwd_adj_lists = graph.get_fwd_adj_list();
        let num_vertices = graph.node_count();
        let mut edges = vec![];
        for from_vertex in 0..num_vertices {
            for to_vertex in fwd_adj_lists[from_vertex]
                .as_ref()
                .unwrap()
                .get_neighbor_ids()
            {
                edges.push(vec![Id::new(from_vertex), to_vertex.clone()]);
            }
        }
        let mut out_subgraph = QueryGraph::empty();
        out_subgraph.add_qedge(QueryEdge::new("a".to_owned(), "b".to_owned(), 0, 0, 0));
        let mut scan = ScanSampling::new(out_subgraph);
        scan.set_edge_indices_to_sample_list(edges, self.num_sampled_edges);
        vec![scan]
    }

    pub fn generate_all_scans<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType, L: IdType>(
        &mut self,
        graph: &TypedStaticGraph<Id, NL, EL, Ty, L>,
    ) -> Vec<ScanSampling<Id>> {
        let fwd_adj_lists = graph.get_fwd_adj_list();
        let vertex_types = graph.get_node_types();
        let num_vertices = graph.node_count();
        let mut key_to_edges_map = HashMap::new();
        let mut key_to_curr_idx = HashMap::new();
        let node_label_cnt = std::cmp::max(self.num_node_labels, 1);
        let edge_label_cnt = std::cmp::max(self.num_edge_labels, 1);
        for from_type in 0..node_label_cnt {
            for label in 0..edge_label_cnt {
                for to_type in 0..node_label_cnt {
                    let edge_key = TypedStaticGraph::<Id, NL, EL, Ty, L>::get_edge_key(
                        from_type, to_type, label,
                    );
                    let num_edges =
                        graph.get_num_edges(from_type as i32, to_type as i32, label as i32);
                    key_to_edges_map.insert(edge_key, vec![0; num_edges * 2]);
                    key_to_curr_idx.insert(edge_key, 0);
                }
            }
        }
        for from_vertex in 0..num_vertices {
            let from_type = vertex_types[from_vertex];
            let offsets = fwd_adj_lists[from_vertex].as_ref().unwrap().get_offsets();
            let neighbours = fwd_adj_lists[from_vertex]
                .as_ref()
                .unwrap()
                .get_neighbor_ids();
            for label_type in 0..offsets.len() - 1 {
                for to_idx in offsets[label_type]..offsets[label_type + 1] {
                    let (to_type, label) = if self.sorted_by_node {
                        (label_type, 0)
                    } else {
                        (vertex_types[neighbours[to_idx].id()] as usize, label_type)
                    };
                    let edge_key = TypedStaticGraph::<Id, NL, EL, Ty, L>::get_edge_key(
                        from_type as usize,
                        to_type,
                        label,
                    );
                    let curr_idx = key_to_curr_idx[&edge_key];
                    key_to_edges_map.get_mut(&edge_key).unwrap()[curr_idx] = from_vertex;
                    key_to_edges_map.get_mut(&edge_key).unwrap()[curr_idx + 1] =
                        neighbours[to_idx].id();
                    key_to_curr_idx.insert(edge_key, curr_idx + 2);
                }
            }
        }
        let mut scans = vec![];
        for from_type in 0..node_label_cnt {
            for edge_label in 0..edge_label_cnt {
                for to_type in 0..node_label_cnt {
                    let mut out_subgraph = QueryGraph::empty();
                    out_subgraph.add_qedge(QueryEdge::new(
                        "a".to_owned(),
                        "b".to_owned(),
                        from_type as i32,
                        to_type as i32,
                        edge_label as i32,
                    ));
                    let edge_key = TypedStaticGraph::<Id, NL, EL, Ty, L>::get_edge_key(
                        from_type, to_type, edge_label,
                    );
                    let actual_num_edges =
                        graph.get_num_edges(from_type as i32, to_type as i32, edge_label as i32);
                    if actual_num_edges <= 0 {
                        continue;
                    }
                    let mut num_edges_to_sample = (self.num_sampled_edges as f64
                        * (graph.get_num_edges(from_type as i32, to_type as i32, edge_label as i32) as f64
                            / graph.edge_count() as f64)) as usize;
                    let mut scan = ScanSampling::new(out_subgraph);
                    if self.sorted_by_node && num_edges_to_sample < 1000 {
                        num_edges_to_sample = actual_num_edges;
                    }
                    scan.set_edge_indices_to_sample(
                        key_to_edges_map[&edge_key]
                            .iter()
                            .map(|edge| Id::new(edge.clone()))
                            .collect(),
                        num_edges_to_sample,
                    );
                    scans.push(scan);
                }
            }
        }
        scans
    }
}

pub struct Descriptor {
    out_subgraph: QueryGraph,
    alds: Vec<AdjListDescriptor>,
}
