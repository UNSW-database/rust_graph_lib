extern crate rust_graph;

use rust_graph::graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;
use std::hash::Hash;
use std::collections::HashSet;

/// A depth first search (Dfs) of a graph.
///
/// The traversal starts at a given node and only traverses nodes reachable
/// from it.
///
/// `Dfs` is not recursive.
///
/// `Dfs` does not itself borrow the graph, and because of this you can run
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
/// let mut Dfs = algorithm_practice::Dfs::Dfs::new(&graph, 0);
/// let mut i = 0;
///
/// while let Some(nx) = Dfs.next(&graph) {
///     assert_eq!(nx, i);
///     i = i + 1;
/// }
///
/// ```
///
/// **Note:** The algorithm may not behave correctly if nodes are removed
/// during iteration. It may not necessarily visit added nodes or edges.
#[derive(Clone)]
pub struct Dfs<Id> {
    /// The stack of nodes to visit
    pub stack: Vec<Id>,
    /// The map of discovered nodes
    pub discovered: HashSet<Id>,
}


impl<Id:IdType> Dfs<Id>
    where Id: IdType,
{
    /// Create a new **Dfs** by initialising empty prev_discovered map, and put **start**
    /// in the queue of nodes to visit.
    pub fn new<G: GeneralGraph<Id, NL, EL>, NL: Eq + Hash, EL: Eq + Hash> (
        graph: &G,
        start: Option<Id>
    ) -> Self
    {
        let mut dfs = Dfs::empty();
        dfs.move_to(start, graph);
        dfs
    }

    /// Create a `Dfs` from a vector and a map
    pub fn from_parts(stack: Vec<Id>, discovered: HashSet<Id>) -> Self {
        Dfs {
            stack: stack,
            discovered: discovered,
        }
    }

    /// Create a new **Dfs**.
    pub fn empty() -> Self
    {
        Dfs {
            stack: Vec::new(),
            discovered: HashSet::new(),
        }
    }

    /// Clear the stack and restart the dfs from a particular node.
    pub fn move_to<G: GeneralGraph<Id, NL, EL>, NL: Eq + Hash, EL: Eq + Hash> (
        &mut self,
        start: Option<Id>,
        graph: &G
    )
    {
        let start = match start {
            Some(_start) => if graph.has_node(_start) {
                _start
            } else {
                panic!("Node {:?} is not in the graph.", _start)
            },
            None => panic!("No starting node given")
        };

        self.discovered.insert(start);
        self.stack.clear();
        self.stack.push(start);
    }

    /// Clear the visit state
    pub fn reset(&mut self)
    {
        self.discovered.clear();
        self.stack.clear();
    }

    /// Return the next node in the Dfs, or **None** if the traversal is done.
    pub fn next<G: GeneralGraph<Id, NL, EL>, NL: Eq + Hash, EL: Eq + Hash> (
        &mut self,
        graph: &G
    ) -> Option<Id>
    {
        if let Some(current_node) = self.stack.pop() {
            for neighbour in graph.neighbors_iter(current_node) {
                if !self.discovered.contains(&neighbour) {
                    self.discovered.insert(neighbour);
                    self.stack.push(neighbour);
                }
            }
            return Some(current_node);
        }
        None
    }

}