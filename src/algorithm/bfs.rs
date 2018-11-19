use prelude::*;
use std::hash::Hash;
use std::collections::HashSet;
use std::collections::VecDeque;
use graph_impl::{DiGraphMap, UnGraphMap};


/// A breadth first search (BFS) of a graph.
///
/// The traversal starts at a given node and only traverses nodes reachable
/// from it.
///
/// `Bfs` is not recursive.
///
/// `Bfs` does not itself borrow the graph, and because of this you can run
/// a traversal over a graph while still retaining mutable access to it
/// example:
///
/// ```
/// use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
/// mod algorithm;
///
/// let mut graph = UnGraphMap::<Void>::new();
///
/// graph.add_edge(0, 1, None);
/// graph.add_edge(1, 2, None);
/// graph.add_edge(2, 3, None);
///
/// let mut bfs = algorithm_practice::Bfs::Bfs::new(&graph, 0);
/// let mut i = 0;
///
/// while let Some(nx) = bfs.next(&graph) {
///     assert_eq!(nx, i);
///     i = i + 1;
/// }
///
/// ```
///
/// **Note:** The algorithm may not behave correctly if nodes are removed
/// during iteration. It may not necessarily visit added nodes or edges.
#[derive(Clone)]
pub struct Bfs<'a, Id:IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a> {
    /// The queue of nodes to visit
    queue: VecDeque<Id>,
    /// The map of discovered nodes
    pub discovered: HashSet<Id>,
    /// The reference to the graph that algorithm is running on
    graph: &'a GeneralGraph<Id, NL, EL>,

}

impl<'a, Id:IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a> Bfs<'a, Id, NL, EL>
{
    /// Create a new **Bfs** by initialising empty prev_discovered map, and put **start**
    /// in the queue of nodes to visit.
    pub fn new<G: GeneralGraph<Id, NL, EL>> (
        graph: &'a G,
        start: Option<Id>
    ) -> Self
    {
        let mut discovered: HashSet<Id> = HashSet::new();
        let mut queue: VecDeque<Id> = VecDeque::new();

        if let Some(start) = start {
            if !graph.has_node(start) {
                panic!("Starting node doesn't exist on graph")
            } else {
                queue.push_back(start);
                discovered.insert(start);
            }
        } else {
            if graph.node_count() == 0 {
                panic!("Graph is empty")
            } else {
                let id = graph.nodes().next().unwrap().get_id();
                queue.push_back(id);
                discovered.insert(id);
            }
        }

        Bfs {
            queue: queue,
            discovered: discovered,
            graph: graph
        }

    }


    /// Return the next node in the bfs, or **None** if the traversal is done.
    pub fn next(&mut self) -> Option<Id>
    {
        if self.queue.len() == 0 {
            if let Some(id) = self.pick_unvisited_node() {
                self.queue.push_back(id);
                self.discovered.insert(id);
            }
        }

        if let Some(current_node) = self.queue.pop_front() {
            for neighbour in self.graph.neighbors_iter(current_node) {
                if !self.discovered.contains(&neighbour) {
                    self.discovered.insert(neighbour);
                    self.queue.push_back(neighbour);
                }
            }
            return Some(current_node);
        } else {
            None
        }
    }


    /// Randomly pick a unvisited node from the map.
    fn pick_unvisited_node(&mut self) -> Option<Id> {
        for node in self.graph.nodes() {
            let id = node.get_id();
            if !self.discovered.contains(&id) {
                return Some(id);
            }
        }
        return None;
    }

}


pub fn test_bfs() {
    test_bfs_one_component();
    test_bfs_radomly_chosen_start();
    test_bfs_seperate_components();
}


pub fn test_bfs_one_component() {
    let mut graph = UnGraphMap::<u32>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(2, 3, None);

    let mut bfs = Bfs::new(&graph, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(2));
    let x = bfs.next();
    assert_eq!(x, Some(3));
    let x = bfs.next();
    assert_eq!(x, None);
    println!("test_bfs_one_component successful!")
}

pub fn test_bfs_radomly_chosen_start() {
    let mut graph = UnGraphMap::<u32>::new();
    graph.add_edge(1, 2, None);

    let mut bfs = Bfs::new(&graph, None);
    let x = bfs.next();
    let result = x == Some(1) || x == Some(2);
    assert_eq!(result, true);
    println!("test_bfs_radomly_chosen_start successful!")
}

pub fn test_bfs_seperate_components() {
    let mut graph = UnGraphMap::<u32>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(3, 4, None);


    let mut bfs = Bfs::new(&graph, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(1));
    let x = bfs.next();
    assert_eq!(x, Some(2));
    let x = bfs.next();
    let result = x == Some(3) || x == Some(4);
    assert_eq!(result, true);
    println!("test_bfs_seperate_components successful!")
}