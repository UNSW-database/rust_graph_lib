extern crate rust_graph;

use rust_graph::graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;
use std::hash::Hash;
use std::collections::HashSet;
use std::collections::VecDeque;

/// A depth first search (Dfs) of a graph.
///
/// The traversal starts at a given node and only traverses nodes reachable
/// from it.
///
/// `Dfs` is not recursive.
///
/// `Dfs` does not itself borrow the graph, and because of this you can run
/// a traversal over a graph while still retaining mutable access to it, if you
/// use it like the following example:
///
/// ```
/// use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
/// mod algorithm_practice;
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
    pub fn new<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> (
        graph: &TypedGraphMap<Id, NL, EL, Ty>,
        start: Id
    ) -> Self
    {
        let mut dfs = Dfs::empty();
        dfs.move_to(start);
        dfs
    }

    /// Create a `Dfs` from a vector and a visit map
    pub fn from_parts(stack: Vec<Id>, discovered: HashSet<Id>) -> Self {
        Dfs {
            stack: stack,
            discovered: discovered,
        }
    }

    /// Create a new **Dfs** using the graph's visitor map, and no stack.
    pub fn empty() -> Self
    {
        Dfs {
            stack: Vec::new(),
            discovered: HashSet::new(),
        }
    }

    /// Keep the discovered map, but clear the visit stack and restart
    /// the dfs from a particular node.
    pub fn move_to(&mut self, start: Id)
    {
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
    pub fn next<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType>(
        &mut self,
        graph: &TypedGraphMap<Id, NL, EL, Ty>
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