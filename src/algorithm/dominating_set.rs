use std::hash::Hash;
use std::iter::FromIterator;

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
) -> Vec<HashSet<Id>> {
    assert!(!graph.is_directed());

    let node_count = graph.node_count();

    for i in 1..node_count {
        let mcds = graph
            .node_indices()
            .combinations(i)
            .map(HashSet::from_iter)
            .filter(|nodes| is_connected_dominating_set(nodes, graph))
            .collect_vec();

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
    nodes: &HashSet<Id>,
    graph: &dyn GeneralGraph<Id, NL, EL, L>,
) -> bool {

    is_connected(&nodes, graph) && is_dominating_set(&nodes, graph)
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
