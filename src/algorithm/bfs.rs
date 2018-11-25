use fixedbitset::FixedBitSet;
use prelude::*;
use std::collections::VecDeque;
use std::hash::Hash;

/// A breadth first search (BFS) of a graph.
///
/// The traversal starts at a given node and only traverses nodes reachable
/// from it.
///
/// `Bfs` is not recursive.

/// Example:
///
/// ```
/// use rust_graph::prelude::*;
/// use rust_graph::graph_impl::UnGraphMap;
/// use rust_graph::algorithm::Bfs;
///
/// let mut graph = UnGraphMap::<Void>::new();
///
/// graph.add_edge(0, 1, None);
/// graph.add_edge(1, 2, None);
/// graph.add_edge(2, 3, None);
///
/// let mut bfs =Bfs::new(&graph, Some(0));
/// let mut i = 0;
///
/// for n in bfs {
///     assert_eq!(n, i);
///     i = i + 1;
/// }
///
/// ```
///
#[derive(Clone)]
pub struct Bfs<'a, Id: IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a, L: IdType + 'a = Id> {
    /// The queue of nodes to visit
    queue: VecDeque<Id>,
    /// The set of discovered nodes
    discovered: FixedBitSet,
    /// The reference to the graph that algorithm is running on
    graph: &'a GeneralGraph<Id, NL, EL, L>,
}

impl<'a, Id: IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a, L: IdType + 'a>
    Bfs<'a, Id, NL, EL, L>
{
    /// Create a new **Bfs** by initialising empty discovered set, and put **start**
    /// in the queue of nodes to visit.
    pub fn new<G: GeneralGraph<Id, NL, EL, L>>(graph: &'a G, start: Option<Id>) -> Self {
        let mut discovered: FixedBitSet =
            FixedBitSet::with_capacity(graph.max_seen_id().unwrap().id() + 1);
        let mut queue: VecDeque<Id> = VecDeque::new();

        discovered.insert_range(..);

        if let Some(start) = start {
            if !graph.has_node(start) {
                panic!("Starting node doesn't exist on graph")
            } else {
                queue.push_back(start);
                discovered.set(start.id(), false);
            }
        } else {
            if graph.node_count() == 0 {
                panic!("Graph is empty")
            } else {
                let id = graph.node_indices().next().unwrap();
                queue.push_back(id);
                discovered.set(id.id(), false);
            }
        }

        Bfs {
            queue,
            discovered,
            graph,
        }
    }

    /// Return the next node in the bfs, or **None** if the traversal is done.
    pub fn next(&mut self) -> Option<Id> {
        if self.queue.len() == 0 {
            if let Some(id) = self.next_unvisited_node() {
                self.queue.push_back(id);
                self.discovered.set(id.id(), false);
            }
        }

        if let Some(current_node) = self.queue.pop_front() {
            for neighbour in self.graph.neighbors_iter(current_node) {
                if self.discovered.contains(neighbour.id()) {
                    self.discovered.set(neighbour.id(), false);
                    self.queue.push_back(neighbour);
                }
            }
            Some(current_node)
        } else {
            None
        }
    }

    /// Randomly pick a unvisited node from the set.
    fn next_unvisited_node(&self) -> Option<Id> {
        for node in self.discovered.ones() {
            if self.graph.has_node(Id::new(node)) {
                return Some(Id::new(node));
            }
        }
        None
    }
}

impl<'a, Id: IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a, L: IdType + 'a> Iterator
    for Bfs<'a, Id, NL, EL, L>
{
    type Item = Id;

    fn next(&mut self) -> Option<Id> {
        self.next()
    }
}
