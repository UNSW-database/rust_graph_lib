use generic::IdType;
use graph_impl::multi_graph::plan::operator::extend::EI::EI;
use graph_impl::multi_graph::plan::operator::hashjoin::build::Build;
use graph_impl::multi_graph::plan::operator::hashjoin::probe::{BaseProbe, Probe};
use graph_impl::multi_graph::plan::operator::hashjoin::probe_cartesian::ProbeCartesian;
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices::{
    ProbeMultiVertices, PMV,
};
use graph_impl::multi_graph::plan::operator::hashjoin::probe_multi_vertices_cartesian::ProbeMultiVerticesCartesian;
use graph_impl::multi_graph::plan::operator::operator::Operator;
use graph_impl::multi_graph::plan::operator::scan::scan::Scan;
use graph_impl::multi_graph::plan::operator::sink::sink::Sink;
use graph_impl::multi_graph::plan::query_plan::QueryPlan;
use graph_impl::multi_graph::planner::catalog::query_graph::QueryGraph;
use hashbrown::HashMap;

pub struct HashJoin {}

impl HashJoin {
    pub fn make<Id: IdType>(
        out_subgraph: QueryGraph,
        build_plan: QueryPlan<Id>,
        probe_plan: QueryPlan<Id>,
    ) -> QueryPlan<Id> {
        QueryPlan::new_from_subplans(HashJoin::inner_make(
            out_subgraph,
            build_plan.shallow_copy().subplans,
            probe_plan.shallow_copy().subplans,
        ))
    }

    fn inner_make<Id: IdType>(
        out_subgraph: QueryGraph,
        mut build_subplans: Vec<Box<Operator<Id>>>,
        mut probe_subplans: Vec<Box<Operator<Id>>>,
    ) -> Vec<Box<Operator<Id>>> {
        let mut pre_build = *build_subplans.pop().unwrap();
        let mut pre_probe = *probe_subplans.pop().unwrap();
        let join_qvertices: Vec<String> = pre_build
            .get_out_query_vertices()
            .intersection(&pre_probe.get_out_query_vertices())
            .map(|x| x.clone())
            .collect();

        let build_qvertex_to_idx_map =
            get_op_attr_as_ref!(&pre_build, out_qvertex_to_idx_map).clone();
        let query_vertex_to_hash = &join_qvertices[0];
        let build_hash_idx = build_qvertex_to_idx_map
            .get(query_vertex_to_hash)
            .unwrap()
            .clone();
        let mut build = Build::new(
            get_op_attr_as_ref!(&pre_build, out_subgraph).clone(),
            query_vertex_to_hash.clone(),
            build_hash_idx.clone(),
        );
        build.base_op.prev.replace(Box::new(pre_build.clone()));
        get_op_attr_as_mut!(&mut pre_build, next).replace(vec![Operator::Build(build.clone())]);
        build_subplans.push(Box::new(Operator::Build(build.clone())));

        let out_subgraph_probe = get_op_attr_as_mut!(&mut pre_probe, out_subgraph).as_mut();
        let out_subgraph_build = get_op_attr_as_mut!(&mut pre_build, out_subgraph).as_mut();
        let mapping = out_subgraph_build.get_isomorphic_mapping_if_any(out_subgraph_probe);
        let mut probe_qvertex_to_idx_map;
        if let Some(map) = mapping {
            probe_qvertex_to_idx_map = HashMap::new();
            build_qvertex_to_idx_map
                .iter()
                .for_each(|(query_vertex, &idx)| {
                    probe_qvertex_to_idx_map.insert(
                        query_vertex.clone(),
                        if idx < build_hash_idx { idx } else { idx - 1 },
                    );
                });
            probe_qvertex_to_idx_map.insert(
                map.get(&join_qvertices[0]).unwrap().clone(),
                build_qvertex_to_idx_map.len() - 1,
            );
        } else {
            probe_qvertex_to_idx_map =
                get_op_attr_as_ref!(&pre_probe, out_qvertex_to_idx_map).clone();
        }
        let &probe_hash_idx = probe_qvertex_to_idx_map.get(query_vertex_to_hash).unwrap();
        let out_qvertex_to_idx_map = HashJoin::compute_out_qvertex_to_idx_map(
            &join_qvertices,
            &build_qvertex_to_idx_map,
            &probe_qvertex_to_idx_map,
        );
        let hashed_tuple_len = build_qvertex_to_idx_map.len() - 1;
        let mut probe_indices = vec![0; join_qvertices.len() - 1];
        let mut build_indices = vec![0; join_qvertices.len() - 1];
        for (i, join_qvertex) in join_qvertices.iter().enumerate() {
            probe_indices[i - 1] = probe_qvertex_to_idx_map.get(join_qvertex).unwrap().clone();
            let mut other_build_idx = build_qvertex_to_idx_map.get(join_qvertex).unwrap().clone();
            if build_hash_idx < other_build_idx {
                other_build_idx -= 1;
            }
            build_indices[i - 1] = other_build_idx;
        }
        let mut probe;
        let in_subgraph = get_op_attr_as_ref!(&pre_probe, out_subgraph)
            .as_ref()
            .clone();
        let out_tuple_len = get_op_attr!(&pre_probe, out_tuple_len);
        if let Some(map) = mapping {
            if probe_indices.len() == 0 {
                probe = Operator::Probe(Probe::PC(ProbeCartesian::new(
                    out_subgraph,
                    in_subgraph,
                    join_qvertices,
                    probe_hash_idx,
                    hashed_tuple_len,
                    out_tuple_len,
                    out_qvertex_to_idx_map,
                )));
            } else {
                probe = Operator::Probe(Probe::PMV(PMV::PMVC(ProbeMultiVerticesCartesian::new(
                    out_subgraph,
                    in_subgraph,
                    join_qvertices,
                    probe_hash_idx,
                    probe_indices,
                    build_indices,
                    hashed_tuple_len,
                    out_tuple_len,
                    out_qvertex_to_idx_map,
                ))));
            }
        } else {
            if probe_indices.len() == 0 {
                probe = Operator::Probe(Probe::BaseProbe(BaseProbe::new(
                    out_subgraph,
                    in_subgraph,
                    join_qvertices,
                    probe_hash_idx,
                    hashed_tuple_len,
                    out_tuple_len,
                    out_qvertex_to_idx_map,
                )));
            } else {
                probe = Operator::Probe(Probe::PMV(PMV::BasePMV(ProbeMultiVertices::new(
                    out_subgraph,
                    in_subgraph,
                    join_qvertices,
                    probe_hash_idx,
                    probe_indices,
                    build_indices,
                    hashed_tuple_len,
                    out_tuple_len,
                    out_qvertex_to_idx_map,
                ))));
            }
            get_op_attr_as_mut!(&mut probe, prev).replace(Box::new(pre_probe.clone()));
            let probe_clone = probe.clone();
            get_op_attr_as_mut!(&mut probe, next).replace(vec![probe_clone]);
            let last_index = probe_subplans.len() - 1;
            probe_subplans[last_index] = Box::new(probe.clone());
        }
        build.probing_subgraph = get_op_attr_as_ref!(&probe, in_subgraph).clone();

        let mut subplans = build_subplans.clone();
        if let None = mapping {
            subplans.append(&mut probe_subplans);
        } else {
            subplans.push(Box::new(probe));
        }
        subplans
    }

    pub fn compute_out_qvertex_to_idx_map(
        join_qvertices: &Vec<String>,
        build_qvertex_to_idx_map: &HashMap<String, usize>,
        probe_qvertex_to_idx_map: &HashMap<String, usize>,
    ) -> HashMap<String, usize> {
        let mut out_qvertices_to_idx_map = probe_qvertex_to_idx_map.clone();
        let mut build_qvertices = vec![String::from(""); build_qvertex_to_idx_map.len()];
        for (build_qvertex, idx) in build_qvertex_to_idx_map {
            build_qvertices[idx.clone()] = build_qvertex.clone();
        }
        for build_qvertex in build_qvertices {
            if join_qvertices.contains(&build_qvertex) {
                continue;
            }
            out_qvertices_to_idx_map.insert(build_qvertex, out_qvertices_to_idx_map.len());
        }
        out_qvertices_to_idx_map
    }
}
