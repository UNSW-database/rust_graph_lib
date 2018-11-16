extern crate rust_graph;

use rust_graph::graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;
use std::hash::Hash;
use std::collections::HashSet;
use std::collections::VecDeque;

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
pub struct Bfs<Id> {
    /// The queue of nodes to visit
    pub queue: VecDeque<Id>,
    /// The map of discovered nodes
    pub discovered: HashSet<Id>,
}


impl<Id:IdType> Bfs<Id>
    where Id: IdType,
{
    /// Create a new **Bfs** by initialising empty prev_discovered map, and put **start**
    /// in the queue of nodes to visit.
    pub fn new<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> (
        graph: &TypedGraphMap<Id, NL, EL, Ty>,
        start: Id
    ) -> Self
    {
        let mut discovered: HashSet<Id> = HashSet::new();
        let mut queue: VecDeque<Id> = VecDeque::new();

        queue.push_back(start);
        discovered.insert(start);

        Bfs {
            queue: queue,
            discovered: discovered,
        }
    }

    /// Return the next node in the bfs, or **None** if the traversal is done.
    pub fn next<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType>(
        &mut self,
        graph: &TypedGraphMap<Id, NL, EL, Ty>
    ) -> Option<Id>
    {
        if let Some(current_node) = self.queue.pop_front() {
            for neighbour in graph.neighbors_iter(current_node) {
                if !self.discovered.contains(&neighbour) {
                    self.discovered.insert(neighbour);
                    self.queue.push_back(neighbour);
                }
            }
            return Some(current_node);
        }
        None
    }

}