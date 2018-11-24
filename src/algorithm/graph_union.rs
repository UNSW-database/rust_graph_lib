use prelude::*;
use std::hash::Hash;
use generic::dtype::IdType;
use graph_impl::graph_map::new_general_graphmap;

/// Graph Union of two graphs, g0 and g1.
///
/// `GraphUnion` is not recursive.
/// Firstly, nodes and edges from g0 are added to the result graph.
/// Then nodes and edges from g1 are added to the result graph.
///
///
/// `GraphUnion` generates the result graph as soon as it is newed.
///
/// Example:
///
/// ```
/// use rust_graph::algorithm::graph_union::GraphUnion;
///
/// let mut graph0 = DiGraphMap::<u32, u32, u32>::new();
/// graph0.add_node(1, Some(0));
/// graph0.add_node(2, Some(1));
/// graph0.add_edge(1, 2, Some(10));
///
/// let mut graph1 = DiGraphMap::<u32, u32, u32>::new();
/// graph1.add_node(3, Some(2));
/// graph1.add_node(4, Some(3));
/// graph1.add_edge(3, 4, Some(20));
///
/// let gu = GraphUnion::new(&graph0, &graph1);
/// let result_graph = gu.into_result();
///
/// ```
///
/// **Note:** The algorithm may not behave correctly if nodes are removed
/// during iteration.

/// Macro for running graph union
macro_rules! run_union {
    ($result:expr, $graph0:expr, $graph1:expr) => {{
        for node in $graph0.nodes() {
            let id = node.get_id();
            $result.add_node(id, $graph0.get_node_label(id).cloned());
        }
        for node in $graph1.nodes() {
            let id = node.get_id();
            $result.add_node(id, $graph1.get_node_label(id).cloned());
        }
        for edge in $graph0.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            $result.add_edge(src, dst, $graph0.get_edge_label(src, dst).cloned());
        }
        for edge in $graph1.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            $result.add_edge(src, dst, $graph1.get_edge_label(src, dst).cloned());
        }
    }}
}

pub struct GraphUnion<Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static = Id> {
    /// The result of graphs union
    pub result_graph: Box<GeneralGraph<Id, NL, EL, L> + 'static>,
}

impl<Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static> GraphUnion<Id, NL, EL, L>
{
    /// Create a new **GraphUnion** by initialising empty result graphs, and compute the result graph.
    pub fn new<'a>(graph0: &'a GeneralGraph<Id, NL, EL, L>, graph1: &'a GeneralGraph<Id, NL, EL, L>) -> Self {
        let mut gu = GraphUnion::empty(graph0.is_directed());
        gu.run_union(graph0, graph1);
        gu
    }

    /// Create an empty **GraphUnion** by initialising empty result graphs and input graph direction.
    pub fn empty(is_directed: bool) -> Self {
        GraphUnion {
            result_graph: new_general_graphmap(is_directed),
        }
    }

    /// Run the graph union by adding nodes and edges of graph0
    /// and adding nodes and edges of graph1, on either directed
    /// graph or undirected graph.
    pub fn run_union(
        &mut self,
        graph0: &GeneralGraph<Id, NL, EL, L>,
        graph1: &GeneralGraph<Id, NL, EL, L>
    ) {
        run_union!(self.result_graph.as_mut_graph().unwrap(), graph0, graph1);
    }

    /// Return the result graph of union.
    pub fn into_result(self) -> Box<GeneralGraph<Id, NL, EL, L>> {
        self.result_graph
    }
}