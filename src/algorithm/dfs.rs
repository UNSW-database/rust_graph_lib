use generic::GeneralGraph;
use std::sync::Arc;
use std::hash::Hash;
use itertools::Itertools;
use generic::dtype::DefaultId;

fn dfs_helper<G, NL, EL>(start: DefaultId, graph: &Arc<G>, dfs_order: &mut Vec<DefaultId>, visited: &mut Vec<bool>)
    where G: GeneralGraph<DefaultId, NL, EL>,
          NL: Eq + Hash,
          EL: Eq + Hash,{
    visited[start as usize] = true;
    dfs_order.push(start);
    let mut neighbors = graph.neighbors_iter(start).collect_vec();
    neighbors.sort();
    for i in neighbors{
        if !visited[i as usize] {
            dfs_helper(i, graph, dfs_order, visited);
        }
    }
}

fn dfs<G, NL, EL>(start: DefaultId, graph: Arc<G>) -> Vec<DefaultId>
    where G: GeneralGraph<DefaultId, NL, EL>,
          NL: Eq + Hash,
          EL: Eq + Hash,{
    let x = graph.node_indices().max().unwrap();
    let mut dfs_order = vec![];
    let mut visited = vec![false; x as usize + 1];

    dfs_helper(start, &graph, &mut dfs_order, &mut visited);

    dfs_order
}

fn dfs_stack<G, NL, EL>(start: DefaultId, graph: Arc<G>) -> Vec<DefaultId>
    where G: GeneralGraph<DefaultId, NL, EL>,
          NL: Eq + Hash,
          EL: Eq + Hash,{
    let x = graph.node_indices().max().unwrap();
    let mut dfs_order = vec![];
    let mut stack = vec![];
    let mut visited = vec![false; x as usize + 1];

    dfs_order.push(start);
    visited[start as usize] = true;
    stack.push((start, 0));
    while !stack.is_empty() {
        let (point, pos) = stack.pop().unwrap();
        let len = graph.neighbors(point).len();
        if pos < len{
            stack.push((point, pos + 1));
            let next = graph.neighbors(point)[pos];
            if !visited[next as usize] {
                dfs_order.push(next);
                visited[next as usize] = true;
                stack.push((next,0));
            }
        }
    }

    dfs_order
}

#[cfg(test)]
mod test{
    use super::*;
    use UnGraphMap;
    use generic::MutGraphTrait;
    #[test]
    fn dfs_test(){
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1,2, None);
        graph.add_edge(2,3, None);
        graph.add_edge(3,1, None);
        graph.add_edge(3,4, None);
        let graph_arc = Arc::new(graph);
        let res = dfs(1, graph_arc);
        assert_eq!(vec![1,2,3,4], res);
    }
    #[test]
    fn dfs_stack_test(){
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1,2, None);
        graph.add_edge(2,3, None);
        graph.add_edge(3,1, None);
        graph.add_edge(3,4, None);
        let graph_arc = Arc::new(graph);
        let res = dfs_stack(1, graph_arc);
        assert_eq!(vec![1,2,3,4], res);
    }
}