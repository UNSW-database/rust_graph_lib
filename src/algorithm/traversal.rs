use std::hash::Hash;
use std::marker::PhantomData;

use fnv::{FnvBuildHasher, FnvHashSet};
use indexmap::IndexSet;
use itertools::Itertools;

use graph_impl::TypedUnGraphMap;
use generic::MutGraphTrait;
use generic::GraphTrait;
use generic::{GeneralGraph, IdType};

type FnvIndexSet<T> = IndexSet<T, FnvBuildHasher>;

pub fn dfs<Id, G, NL, EL, L>(start: Option<Id>, graph: &G) -> Vec<Id>
where
    Id: IdType,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L>,
    NL: Eq + Hash,
    EL: Eq + Hash,
{
    let start = match start {
        Some(_start) => if graph.has_node(_start) {
            _start
        } else {
            panic!("Node {:?} is not in the graph.", _start)
        },
        None => match graph.node_indices().next() {
            Some(_start) => _start,
            None => return Vec::new(),
        },
    };

    let mut dfs_order = FnvIndexSet::default();
    dfs_helper(start, graph, &mut dfs_order);

    dfs_order.into_iter().collect()
}

fn dfs_helper<Id, G, NL, EL, L>(start: Id, graph: &G, dfs_order: &mut FnvIndexSet<Id>)
where
    Id: IdType,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L>,
    NL: Eq + Hash,
    EL: Eq + Hash,
{
    dfs_order.insert(start);
    for i in graph.neighbors_iter(start) {
        if !dfs_order.contains(&i) {
            dfs_helper(i, graph, dfs_order);
        }
    }
}

pub fn dfs_stack<Id, G, NL, EL, L>(start: Option<Id>, graph: &G) -> Vec<Id>
where
    Id: IdType,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L>,
    NL: Eq + Hash,
    EL: Eq + Hash,
{
    let start = match start {
        Some(_start) => if graph.has_node(_start) {
            _start
        } else {
            panic!("Node {:?} is not in the graph.", _start)
        },
        None => match graph.node_indices().next() {
            Some(_start) => _start,
            None => return Vec::new(),
        },
    };

    let mut dfs_order = FnvIndexSet::default();
    let mut stack = Vec::new();

    dfs_order.insert(start);
    stack.push((start, 0));

    while !stack.is_empty() {
        let (point, pos) = stack.pop().unwrap();
        let len = graph.neighbors(point).len();
        if pos < len {
            stack.push((point, pos + 1));
            let next = graph.neighbors(point)[pos];
            if !dfs_order.contains(&next) {
                dfs_order.insert(next);
                stack.push((next, 0));
            }
        }
    }

    dfs_order.into_iter().collect()
}

struct DFS<'a, Id, G, NL, EL, L>
where
    Id: IdType,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L> + 'a,
    NL: Eq + Hash,
    EL: Eq + Hash,
{
    stack: Vec<(Id, usize)>,
    visited: FnvHashSet<Id>,
    graph: &'a G,
    inited: bool,
    start_point: Id,
    ph1: PhantomData<L>,
    ph2: PhantomData<NL>,
    ph3: PhantomData<EL>,
}

impl<'a, Id, G, NL, EL, L> DFS<'a, Id, G, NL, EL, L>
where
    Id: IdType,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L>,
    NL: Eq + Hash,
    EL: Eq + Hash,
{
    fn new(start: Option<Id>, graph: &'a G) -> Option<Self> {
        let start = match start {
            Some(_start) => if graph.has_node(_start) {
                Some(_start)
            } else {
                panic!("Node {:?} is not in the graph.", _start)
            },
            None => match graph.node_indices().next() {
                Some(_start) => Some(_start),
                None => None,
            },
        };
        let stack = vec![];
        let visited= FnvHashSet::default();

        match start {
            Some(s) => {
                Some(DFS {
                    stack,
                    visited,
                    graph,
                    inited: false,
                    start_point: s,
                    ph1: PhantomData,
                    ph2: PhantomData,
                    ph3: PhantomData,
                })
            },
            None => None
        }

    }
}
impl<'a, Id, G, NL, EL, L> Iterator for DFS<'a, Id, G, NL, EL, L>
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
                } else {
                    self.next()
                }
            } else {
                self.stack.pop();

                self.next()
            }
        } else {
            None
        }
    }
}

fn components<Id, G, NL, EL, L>(
    start: Id,
    graph: &G,
    vertex_set: &mut FnvIndexSet<Id>,
    visited: &mut FnvHashSet<Id>,
) where
    Id: IdType,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L>,
    NL: Eq + Hash,
    EL: Eq + Hash,
{
    visited.insert(start);
    vertex_set.insert(start);
    for i in graph.neighbors_iter(start) {
        if !visited.contains(&i) {
            components(i, graph, vertex_set, visited)
        }
    }
}

fn induced_graph<Id, G, NL, EL, L>(vertex: &mut FnvIndexSet<Id>, graph: &G) -> TypedUnGraphMap<Id, NL, EL, L>
    where
        Id: IdType,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L>,
        NL: Eq + Hash,
        EL: Eq + Hash,
{
    let mut ng = TypedUnGraphMap::new();
    for (s,e) in graph.edge_indices() {
        if vertex.contains(&s) && vertex.contains(&e) {
            ng.add_edge(s,e, None);
        }
    }
    return ng;
}

fn connnected_components<Id, G, NL, EL, L>(graph: &G) -> Vec<FnvIndexSet<Id>>
where
    Id: IdType,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L>,
    NL: Eq + Hash,
    EL: Eq + Hash,
{
    let mut visited = FnvHashSet::default();
    let mut ans = vec![];
    for i in graph.node_indices().sorted() {
        if !visited.contains(&i) {
            let mut node_set = FnvIndexSet::default();
            components(i, graph, &mut node_set, &mut visited);
            ans.push(node_set);
        }
    }

    ans
}

#[cfg(test)]
mod test {
    use super::*;
    use generic::MutGraphTrait;
    use UnGraphMap;
    #[test]
    fn dfs_test() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);
        graph.add_edge(3, 1, None);
        graph.add_edge(3, 4, None);
        let res = dfs(Some(1), &graph);
        assert_eq!(vec![1, 2, 3, 4], res);
    }
    #[test]
    fn dfs_stack_test() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);
        graph.add_edge(3, 1, None);
        graph.add_edge(3, 4, None);
        let res = dfs_stack(Some(1), &graph);
        assert_eq!(vec![1, 2, 3, 4], res);
    }
    #[test]
    fn dfs_iter_test() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);
        graph.add_edge(3, 1, None);
        graph.add_edge(3, 4, None);
        let res = DFS::new(Some(1), &graph);
        assert_eq!(vec![1, 2, 3, 4], res.unwrap().collect_vec());
    }
    use generic::GraphTrait;
    #[test]
    fn dfs_connected_component_test() {
        let mut graph = UnGraphMap::<u32>::new();
        graph.add_edge(1, 2, None);
        graph.add_edge(2, 3, None);
        graph.add_edge(3, 1, None);
        graph.add_edge(4, 5, None);

        let res = connnected_components(&graph);
        let mut one = res[0].clone().into_iter().collect_vec();
        let mut two = res[1].clone().into_iter().collect_vec();
        assert_eq!(one, vec![1, 2, 3]);
        assert_eq!(two, vec![4, 5]);
    }
}
