use generic::GeneralGraph;
use std::sync::Arc;
use std::hash::Hash;
use itertools::Itertools;
use generic::dtype::DefaultId;

fn dfs_helper<G, NL, EL>(start: DefaultId, graph: &Arc<G>, dfs_order: &mut Vec<DefaultId>, visited: &mut Vec<bool>)
    where G: GeneralGraph<DefaultId, NL, EL>,
          NL: Eq + Hash,
          EL: Eq + Hash,{
    dfs_order.push(start);
    let mut neighbors = graph.neighbors_iter(start).collect_vec();
    neighbors.sort();
    for i in neighbors{
        if !visited[i as usize] {
            visited[i as usize] = true;
            dfs_helper(i, graph, dfs_order, visited);
        }
    }
}

fn dfs<G, NL, EL>(start: DefaultId, graph: Arc<G>) -> Vec<DefaultId>
    where G: GeneralGraph<DefaultId, NL, EL>,
          NL: Eq + Hash,
          EL: Eq + Hash,{
    let mut dfs_order = vec![];
    let mut visited = vec![false; graph.node_count()];

    visited[start as usize] = true;
    dfs_helper(start, &graph, &mut dfs_order, &mut visited);

    dfs_order
}