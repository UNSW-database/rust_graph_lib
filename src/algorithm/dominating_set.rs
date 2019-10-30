use std::hash::Hash;

use hashbrown::HashSet;
use itertools::Itertools;

use crate::graph_impl::TypedUnGraphMap;
use crate::prelude::*;
use algorithm::ConnComp;

pub fn min_connected_dominating_set<
    Id: IdType,
    NL: Eq + Hash + Clone,
    EL: Eq + Hash + Clone,
    L: IdType,
>(
    graph: &dyn GeneralGraph<Id, NL, EL, L>,
) -> Vec<Vec<Id>> {
    assert!(!graph.is_directed());

    let node_count = graph.node_count();

    for i in 1..node_count {
        let mut mcds = graph
            .node_indices()
            .combinations(i)
            .filter(|nodes| is_connected_dominating_set(nodes, graph))
            .map(|mut x| {
                x.sort();
                x
            })
            .collect_vec();

        mcds.sort();

        if !mcds.is_empty() {
            return mcds;
        }
    }

    Vec::new()
}

fn is_connected_dominating_set<
    Id: IdType,
    NL: Eq + Hash + Clone,
    EL: Eq + Hash + Clone,
    L: IdType,
>(
    nodes: &Vec<Id>,
    graph: &dyn GeneralGraph<Id, NL, EL, L>,
) -> bool {
    let mut nodes_set = HashSet::new();
    nodes_set.extend(nodes.clone());

    is_connected(&nodes_set, graph) && is_dominating_set(&nodes_set, graph)
}

fn is_connected<Id: IdType, NL: Eq + Hash + Clone, EL: Eq + Hash + Clone, L: IdType>(
    nodes: &HashSet<Id>,
    graph: &dyn GeneralGraph<Id, NL, EL, L>,
) -> bool {
    let mut induced_subgraph = TypedUnGraphMap::<Id, Void, Void>::new();

    for n in nodes{
        induced_subgraph.add_node(*n,None);
    }

    for (src, dst) in graph.edge_indices() {
        if nodes.contains(&src) && nodes.contains(&dst) {
            induced_subgraph.add_edge(src, dst, None);
        }
    }

    ConnComp::new(&induced_subgraph).get_count() == 1
}

fn is_dominating_set<Id: IdType, NL: Eq + Hash + Clone, EL: Eq + Hash + Clone, L: IdType>(
    nodes: &HashSet<Id>,
    graph: &dyn GeneralGraph<Id, NL, EL, L>,
) -> bool {
    let mut total_nodes = nodes.clone();

    for n in nodes {
        total_nodes.extend(graph.neighbors_iter(*n));
    }

    total_nodes.len() == graph.node_count()
}
