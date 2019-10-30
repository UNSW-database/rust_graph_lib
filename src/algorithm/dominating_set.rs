use std::hash::Hash;
use std::iter::FromIterator;
use std::mem::swap;

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

    for n in nodes {
        induced_subgraph.add_node(*n, None);
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

#[derive(Debug, Clone)]
pub struct SpanningTree<Id: IdType> {
    pub pivot: Id,
    pub span: usize,
    pub non_leaves: HashSet<Id>,
    pub edges: Vec<(Id, Id)>,
}

pub fn min_span_max_leaf_spanning_trees<
    Id: IdType,
    NL: Eq + Hash + Clone,
    EL: Eq + Hash + Clone,
    L: IdType,
>(
    graph: &dyn GeneralGraph<Id, NL, EL, L>,
) -> Vec<SpanningTree<Id>> {
    let mcds = min_connected_dominating_set(graph);
    let mut trees = Vec::new();

    for set in mcds {
        for pivot in set.iter() {
            trees.push(get_max_leaf_spanning_tree(graph, &set, *pivot));
        }
    }

    trees.sort_by_key(|tree| tree.span);

    let min_span = trees[0].span;

    trees
        .into_iter()
        .take_while(|tree| tree.span == min_span)
        .collect()
}

fn get_max_leaf_spanning_tree<
    Id: IdType,
    NL: Eq + Hash + Clone,
    EL: Eq + Hash + Clone,
    L: IdType,
>(
    graph: &dyn GeneralGraph<Id, NL, EL, L>,
    mcds: &HashSet<Id>,
    pivot: Id,
) -> SpanningTree<Id> {
    let mut edges = Vec::new();

    let mut current_level = vec![pivot];
    let mut next_level = Vec::new();

    let mut visited = HashSet::new();

    let mut span = 0usize;

    while !current_level.is_empty() {
        for n in current_level.drain(..) {
            visited.insert(n);

            for neighbor in graph.neighbors_iter(n) {
                if visited.contains(&neighbor) {
                    continue;
                }

                edges.push((n, neighbor));
                visited.insert(neighbor);

                if mcds.contains(&neighbor) {
                    next_level.push(neighbor)
                }
            }
        }

        swap(&mut current_level, &mut next_level);

        span += 1;
    }

    SpanningTree {
        pivot,
        span,
        non_leaves: mcds.clone(),
        edges,
    }
}
