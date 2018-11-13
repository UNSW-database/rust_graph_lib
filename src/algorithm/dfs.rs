use generic::GeneralGraph;
use std::sync::Arc;
use std::hash::Hash;
use itertools::Itertools;
use generic::IdType;
use std::collections::HashSet;
use std::marker::PhantomData;
use graph_impl::TypedUnGraphMap;
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
        if !visited.contains(&i) {
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
            if !visited.contains(&next) {
                dfs_order.push(next);
                visited.insert(next);
                stack.push((next,0));
            }
        }
    }

    dfs_order
}
struct DFS<Id, G, NL, EL, L>
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,
{
    stack: Vec<(Id, usize)>,
    visited: HashSet<Id>,
    graph: Arc<G>,
    inited: bool,
    start_point: Id,
    ph1: PhantomData<L>,
    ph2: PhantomData<NL>,
    ph3: PhantomData<EL>,
}

impl<Id, G, NL, EL, L> DFS<Id, G, NL, EL, L>
where
    Id: IdType,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L>,
    NL: Eq + Hash,
    EL: Eq + Hash,
{
    fn new(start_point: Id, graph: Arc<G>) -> Self {
        let stack = vec![];
        let visited = HashSet::new();
        DFS{
            stack,
            visited,
            graph,
            inited: false,
            start_point,
            ph1: PhantomData,
            ph2: PhantomData,
            ph3: PhantomData,
        }
    }
}
impl<Id, G, NL, EL, L> Iterator for DFS<Id, G, NL, EL, L>
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,
{
    type Item = Id;
    fn next(&mut self) -> Option<Id> {
        if !self.inited {
            self.inited = true;
            self.stack.push((self.start_point, 0));
            self.visited.insert(self.start_point);

            Some(self.start_point)
        } else if !self.stack.is_empty() {
            let (cur_pt, pos) = self.stack.pop().unwrap();
            let len = self.graph.neighbors(cur_pt).len();
            if pos < len {
                self.stack.push((cur_pt, pos + 1));
                let n = self.graph.neighbors(cur_pt)[pos];
                if !self.visited.contains(&n) {
                    self.visited.insert(n);
                    self.stack.push((n, 0));

                    Some(n)
                } else{
                    self.next()
                }
            } else {
                self.stack.pop();

                self.next()
            }
        } else{
            None
        }
    }
}

fn components<Id, G, NL, EL, L>(start: Id, graph: &Arc<G>, gmap: &mut TypedUnGraphMap<Id, NL, EL, L>, visited: &mut HashSet<Id>)
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,
{
    visited.insert(start);

    for i in graph.neighbors_iter(start) {
        if !visited.contains(&i) {
            gmap.add_edge(start, i, None);
            components(i, graph, gmap, visited)
        }
    }
}

fn connnected_components<Id, G, NL, EL, L>(graph: Arc<G>) -> Vec<TypedUnGraphMap<Id, NL, EL, L>>
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,
{
    let mut visited = HashSet::new();
    let mut ans = vec![];
    for i in graph.node_indices().sorted() {
        if !visited.contains(&i) {
            let mut gmap = TypedUnGraphMap::new();
            components(i, &graph, &mut gmap, &mut visited);
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
    #[test]
    fn dfs_iter_test(){
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1,2, None);
        graph.add_edge(2,3, None);
        graph.add_edge(3,1, None);
        graph.add_edge(3,4, None);
        let graph_arc = Arc::new(graph);
        let res = DFS::new(1, graph_arc);
        assert_eq!(vec![1,2,3,4], res.collect_vec());
    }
    use generic::GraphTrait;
    #[test]
    fn dfs_connected_component_test(){
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1,2, None);
        graph.add_edge(2,3, None);
        graph.add_edge(3,1, None);
        graph.add_edge(4,5, None);
        let graph_arc = Arc::new(graph);
        let res = connnected_components(graph_arc);
        let mut one = res[0].node_indices().collect_vec();
        one.sort();
        let mut two = res[1].node_indices().collect_vec();
        two.sort();
        assert_eq!(one, vec![1,2,3]);
        assert_eq!(two, vec![4,5]);
    }
}