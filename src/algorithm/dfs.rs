use generic::GeneralGraph;
use std::sync::Arc;
use std::hash::Hash;
use itertools::Itertools;
use generic::IdType;
use std::collections::HashSet;
use graph_impl::UnGraphMap;
use generic::MutGraphTrait;

fn dfs_helper<Id, G, NL, EL, L>(start: Id, graph: &Arc<G>, dfs_order: &mut Vec<Id>, visited: &mut HashSet<Id>)
    where Id: IdType,
          L: IdType,
          G: GeneralGraph<Id, NL, EL, L>,
          NL: Eq + Hash,
          EL: Eq + Hash,{
    visited.insert(start);
    dfs_order.push(start);
    for i in graph.neighbors_iter(start){
        if !visited.contains(i) {
            dfs_helper(i, graph, dfs_order, visited);
        }
    }
}

fn dfs<Id, G, NL, EL, L>(start: Id, graph: Arc<G>) -> Vec<Id>
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,{
    let x = graph.node_indices().max().unwrap();
    let mut dfs_order = vec![];
    let mut visited = HashSet::new();
    dfs_helper(start, &graph, &mut dfs_order, &mut visited);

    dfs_order
}

fn dfs_stack<Id, G, NL, EL, L>(start: Id, graph: Arc<G>) -> Vec<Id>
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,{
    let x = graph.node_indices().max().unwrap();
    let mut dfs_order = vec![];
    let mut stack = vec![];
    let mut visited = HashSet::new();

    dfs_order.push(start);
    visited.insert(start);
    stack.push((start, 0));
    while !stack.is_empty() {
        let (point, pos) = stack.pop().unwrap();
        let len = graph.neighbors(point).len();
        if pos < len{
            stack.push((point, pos + 1));
            let next = graph.neighbors(point)[pos];
            if !visited.contains(next) {
                dfs_order.push(next);
                visited.insert(next);
                stack.push((next,0));
            }
        }
    }

    dfs_order
}

fn components<Id, G, NL, EL, L>(start: Id, graph: &Arc<G>, gmap: &mut UnGraphMap<Id, NL, EL>, visited: &mut HashSet<Id>)
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,
{
    visited.insert(start);

    for i in graph.neighbors_iter(start) {
        if !visited.contains(i) {
            gmap.add_edge(start, i, graph.get_edge_label(start, i));
            components(i, graph, gmap, visited)
        }
    }
}

fn connnected_components<Id, G, NL, EL, L>(graph: Arc<G>) -> Vec<UnGraphMap<NL, EL>>
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,
{
    let mut visited = HashSet::new();
    let mut ans = vec![];
    for i in graph.node_indices() {
        if !visited.contains(i) {
            let mut gmap = UnGraphMap::new();
            components(i, &graph, &mut gmap, &visited);
            ans.push(gmap);
        }
    }

    ans
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