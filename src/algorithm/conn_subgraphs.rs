use prelude::*;
use std::hash::Hash;
use algorithm::conn_comp::ConnComp;
use generic::dtype::IdType;
use graph_impl::graph_map::new_general_graphmap;

/// Enumeration of Connected subgraphs of a graph.
///
/// `ConnSubgraph` is not recursive.
/// The algorithm first gets all connected components using ConnComp algorithm.
/// Then generates a vector of subgraphs according to nodes and edges
/// corresponding to each component.
///
///
/// `GraphMinus` generates the result graph as soon as it is newed.
///
/// Example:
///
/// ```
/// use rust_graph::algorithm::conn_subgraphs::ConnSubgraph;
///
/// let mut graph = UnGraphMap::<u32, u32, u32>::new();
/// graph.add_node(1, Some(0));
/// graph.add_node(2, Some(1));
/// graph.add_node(3, Some(2));
/// graph.add_node(4, Some(3));
///
///
/// graph.add_edge(1, 2, Some(10));
/// graph.add_edge(3, 4, Some(20));
///
///
/// let cs = ConnSubgraph::new(&graph);
/// let subgraphs = cs.into_result();
///
/// ```
///
/// **Note:** The algorithm may not behave correctly if nodes are removed
/// during iteration.

/// Macro for processing edges
macro_rules! process_edge {
    ($self:ident, $edge:expr, $graph:expr, $subgraphs:expr) => {{
        let start = $edge.get_start();
        let target = $edge.get_target();
        let root = $self.cc.get_root(start).unwrap();
        let label = $graph.get_edge_label(start, target).cloned();
        let index = $self.root_to_subgraph(root);

        if let Some(index) = index {
            $subgraphs[index].as_mut_graph().unwrap().add_edge(start, target, label);
        } else {
            if $graph.is_directed() {
                $subgraphs.push(new_general_graphmap(true));
            } else {
                $subgraphs.push(new_general_graphmap(false));
            }
            $self.roots.push(root);
            let length = $subgraphs.len();
            $subgraphs[length - 1].as_mut_graph().unwrap().add_edge(start, target, label);
        }
    }}
}

/// Macro for processing nodes
macro_rules! process_node {
    ($self:ident, $node:expr, $graph:expr, $subgraphs:expr) => {{
        let id = $node.get_id();
        let root = $self.cc.get_root(id).unwrap();
        let label = $graph.get_node_label(id).cloned();
        let index = $self.root_to_subgraph(root);

        if let Some(index) = index {
            $subgraphs[index].as_mut_graph().unwrap().add_node(id, label);
        } else {
            if $graph.is_directed() {
                $subgraphs.push(new_general_graphmap(true));
            } else {
                $subgraphs.push(new_general_graphmap(false));
            }
            $self.roots.push(root);
            let length = $subgraphs.len();
            $subgraphs[length - 1].as_mut_graph().unwrap().add_node(id, label);
        }
    }}
}


pub struct ConnSubgraph<Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static = Id> {
    /// The result vector of subgraphs
    pub subgraphs: Vec<Box<GeneralGraph<Id, NL, EL, L> + 'static>>,
    /// The vector of roots. e.g roots[subgraph_index] = subgraph_root_id.
    roots: Vec<Id>,
    /// The Connected Components of given graph
    cc: ConnComp<Id>
}

impl<Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static> ConnSubgraph<Id, NL, EL, L>
{
    /// Create a new **ConnSubgraph** by initialising empty result subgraph vector, and create a ConnComp
    /// instance with given graph. Then run the enumeration.
    pub fn new(graph: &GeneralGraph<Id, NL, EL, L>) -> Self {
        let mut cs = ConnSubgraph::empty(graph);

        cs.run_subgraph_enumeration(graph);
        cs
    }

    /// Create a new **ConnSubgraph** by initialising empty result subgraph vector, and create a ConnComp
    /// instance with given graph.
    pub fn empty(graph: &GeneralGraph<Id, NL, EL, L>) -> Self {
        let subgraphs: Vec<Box<GeneralGraph<Id, NL, EL, L> + 'static>> = Vec::new();
        let cc = ConnComp::new(graph);

        ConnSubgraph {
            subgraphs: subgraphs,
            roots: Vec::new(),
            cc: cc
        }
    }

    /// Run the graph enumeration by adding each node and edge to the subgraph that it
    /// corresponds to.
    pub fn run_subgraph_enumeration(&mut self, graph: &GeneralGraph<Id, NL, EL, L>) {
        for node in graph.nodes() {
            process_node!(self, node, graph, self.subgraphs);
        }

        for edge in graph.edges() {
            process_edge!(self, edge, graph, self.subgraphs);
        }
    }

    /// Get the subgraph from a given root node id.
    pub fn root_to_subgraph(&self, root: Id) -> Option<usize> {
        for index in 0 .. self.roots.len() {
            if self.roots[index] == root {
                return Some(index);
            }
        }
        None
    }

    /// Return the result vector of subgraphs.
    pub fn into_result(self) -> Vec<Box<GeneralGraph<Id, NL, EL, L>>> {
        self.subgraphs
    }
}

