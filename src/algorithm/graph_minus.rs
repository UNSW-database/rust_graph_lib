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
pub fn graph_minus<'a, Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static>(
    graph0: &'a GeneralGraph<Id, NL, EL, L>,
    graph1: &'a GeneralGraph<Id, NL, EL, L>
) -> Box<GeneralGraph<Id, NL, EL, L>> {
    let mut result_graph: Box<GeneralGraph<Id, NL, EL, L> + 'static> = new_general_graphmap(graph0.is_directed());
    {
        let graph = result_graph.as_mut_graph().unwrap();
        for node in graph0.nodes() {
            let id = node.get_id();
            graph.add_node(id, graph0.get_node_label(id).cloned());
        }
        for edge in graph0.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            graph.add_edge(src, dst, graph0.get_edge_label(src, dst).cloned());
        }
        for node in graph1.nodes() {
            let id = node.get_id();
            graph.remove_node(id);
        }
        for edge in graph1.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            graph.remove_edge(src, dst);
        }
    }
    result_graph
}