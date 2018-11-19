use prelude::*;
use std::hash::Hash;
use bit_set::BitSet;


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
pub struct Dfs<'a, Id:IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a> {
    /// The stack of nodes to visit
    stack: Vec<Id>,
    /// The map of discovered nodes
    discovered: BitSet<u32>,
    /// The reference to the graph that algorithm is running on
    graph: &'a GeneralGraph<Id, NL, EL>
}


impl<'a, Id:IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a> Dfs<'a, Id, NL, EL>
{
    /// Create a new **Dfs** by initialising empty prev_discovered map, and put **start**
    /// in the queue of nodes to visit.
    pub fn new<G: GeneralGraph<Id, NL, EL>> (
        graph: &'a G,
        start: Option<Id>
    ) -> Self
    {
        let mut dfs = Dfs::empty(graph);
        dfs.move_to(start);
        dfs
    }

    /// Create a `Dfs` from a vector and a map
    pub fn from_parts<G: GeneralGraph<Id, NL, EL>> (
        stack: Vec<Id>,
        discovered: BitSet<u32>,
        graph: &'a G
    ) -> Self {
        Dfs {
            stack: stack,
            discovered: discovered,
            graph: graph
        }
    }

    /// Create a new **Dfs**.
    pub fn empty<G: GeneralGraph<Id, NL, EL>>(graph: &'a G) -> Self
    {
        Dfs {
            stack: Vec::new(),
            discovered: BitSet::new(),
            graph: graph
        }
    }

    /// Clear the stack and restart the dfs from a particular node.
    pub fn move_to(
        &mut self,
        start: Option<Id>,
    )
    {
        if let Some(start) = start {
            if !self.graph.has_node(start) {
                panic!("Node {:?} is not in the graph.", start);
            } else {
                self.discovered.insert(start.id());
                self.stack.clear();
                self.stack.push(start);
            }
        } else {
            if self.graph.node_count() == 0 {
                panic!("Graph is empty")
            } else {
                let id = self.graph.nodes().next().unwrap().get_id();
                self.discovered.insert(id.id());
                self.stack.clear();
                self.stack.push(id);
            }
        }
    }

    /// Clear the visit state
    pub fn reset(&mut self)
    {
        self.discovered.clear();
        self.stack.clear();
    }

    /// Return the next node in the Dfs, or **None** if the traversal is done.
    pub fn next(&mut self) -> Option<Id>
    {
        if self.stack.len() == 0 {
            if let Some(id) = self.pick_unvisited_node() {
                self.stack.push(id);
                self.discovered.insert(id.id());
            }
        }

        if let Some(current_node) = self.stack.pop() {
            for neighbour in self.graph.neighbors_iter(current_node) {
                if !self.discovered.contains(neighbour.id()) {
                    self.discovered.insert(neighbour.id());
                    self.stack.push(neighbour);
                }
            }
            Some(current_node)
        } else {
            None
        }
    }


    /// Randomly pick a unvisited node from the map.
    fn pick_unvisited_node(&self) -> Option<Id> {
        for node in self.graph.nodes() {
            let id = node.get_id();
            if !self.discovered.contains(id.id()) {
                return Some(id);
            }
        }
        None
    }

}
