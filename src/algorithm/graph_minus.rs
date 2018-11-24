use prelude::*;
use std::hash::Hash;
use generic::dtype::IdType;
use graph_impl::graph_map::new_general_graphmap;

/// Graph Subtraction of two graphs, g0 and g1.
///
/// `GraphMinus` is not recursive.
/// Firstly, nodes and edges from g0 are added to the result graph.
/// Then nodes and edges from g1 are removed from the result graph.
///
///
/// `GraphMinus` generates the result graph as soon as it is newed.
///
/// Example:
///
/// ```
/// use rust_graph::algorithm::graph_minus::GraphMinus;
///
/// let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
/// graph0.add_node(1, Some(0));
/// graph0.add_node(2, Some(1));
/// graph0.add_node(3, Some(2));
/// graph0.add_node(4, Some(3));
/// graph0.add_edge(1, 2, Some(10));
/// graph0.add_edge(3, 4, Some(20));
///
/// let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
/// graph1.add_node(3, Some(2));
/// graph1.add_node(4, Some(3));
/// graph1.add_edge(3, 4, Some(20));
///
/// let gm = GraphMinus::new(&graph0, &graph1);
/// let result_graph = gm.into_result();
///
/// ```
///
/// **Note:** The algorithm may not behave correctly if nodes are removed
/// during iteration.
pub struct GraphMinus<Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static = Id> {
    /// The result of graphs minus
    pub result_graph: Box<GeneralGraph<Id, NL, EL, L> + 'static>,
}

impl<Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static> GraphMinus<Id, NL, EL, L>
{
    /// Create a new **GraphMinus** by initialising empty result graph, and compute the result graph.
    pub fn new(graph0: &GeneralGraph<Id, NL, EL, L>, graph1: &GeneralGraph<Id, NL, EL, L>) -> Self {
        let mut gm = GraphMinus::empty(graph0.is_directed());
        gm.run_minus(graph0, graph1);
        gm
    }

    /// Create an empty **GraphMinus** by initialising empty result graphs and input graph direction.
    pub fn empty(is_directed: bool) -> Self {
        GraphMinus {
            result_graph: new_general_graphmap(is_directed),
        }
    }

    /// Run the graph minus by adding nodes and edges of graph0
    /// and removing nodes and edges of graph1, on either directed
    /// graph or undirected graph.
    pub fn run_minus(
        &mut self,
        graph0: &GeneralGraph<Id, NL, EL, L>,
        graph1: &GeneralGraph<Id, NL, EL, L>
    ) {
        let result = self.result_graph.as_mut_graph().unwrap();
        for node in graph0.nodes() {
            let id = node.get_id();
            result.add_node(id, graph0.get_node_label(id).cloned());
        }

        for edge in graph0.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            result.add_edge(src, dst, graph0.get_edge_label(src, dst).cloned());
        }

        for node in graph1.nodes() {
            let id = node.get_id();
            result.remove_node(id);
        }

        for edge in graph1.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            result.remove_edge(src, dst);
        }
    }

    /// Return the result graph of subtraction.
    pub fn into_result(self) -> Box<GeneralGraph<Id, NL, EL, L>> {
        self.result_graph
    }
}