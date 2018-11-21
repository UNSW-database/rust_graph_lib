use prelude::*;
use std::hash::Hash;
use graph_impl::{TypedGraphMap};
use generic::dtype::IdType;



/// Detection of Connected Component (ConnComp) of a graph.
///
/// `ConnComp` is not recursive.
/// The detection iterates through every edge.
/// Nodes that are involved in each edge is merged together.
///
/// There are k loops and each loop processing the root array costs log(n).
/// Therefore, time complexity is O(k*log(n)).
///
/// `ConnComp` does not itself borrow the graph, and because of this you can run
/// a detection over a graph while still retaining mutable access to it
/// Example:
///
/// ```
/// use rust_graph::graph_impl::{DiGraphMap, UnGraphMap};
/// mod algorithm;
///
/// let mut graph = UnGraphMap::<Void>::new();
///
/// graph.add_edge(0, 1, None);
/// graph.add_edge(1, 2, None);
/// graph.add_edge(3, 4, None);
///
/// let mut cc = algorithm::cc::ConnComp::new(&graph);
/// cc.get_nodes_in_component_of_given_node(0);
/// cc.check_nodes_in_same_component(0, 1);
/// cc.process_new_edge(edge);
///
/// ```
///
/// **Note:** The algorithm may not behave correctly if nodes are removed
/// during iteration. It may not necessarily visit added nodes or edges.
pub struct GraphMinus<Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static = Id> {
    /// The vector of undirected subgraphs
    pub un_result_graph: TypedGraphMap<Id, NL, EL, Undirected, L>,
    /// The vector of directed subgraphs
    pub di_result_graph: TypedGraphMap<Id, NL, EL, Directed, L>,
    /// Check whether graph is directed
    pub is_directed: bool
}

impl<Id: IdType + 'static, NL: Eq + Hash + Clone + 'static, EL: Eq + Hash + Clone + 'static, L: IdType + 'static> GraphMinus<Id, NL, EL, L>
{
    /// Create a new **GraphUnion** by initialising empty result graphs, and compute the minused graph
    /// of input graphs.
    pub fn new(graph0: &GeneralGraph<Id, NL, EL, L>, graph1: &GeneralGraph<Id, NL, EL, L>) -> Self {
        let mut gm = GraphMinus::empty(graph0.is_directed());
        gm.run_minus(graph0, graph1);
        gm
    }

    pub fn empty(is_directed: bool) -> Self
    {
        GraphMinus {
            un_result_graph: GraphMinus::generate_empty_ungraph(),
            di_result_graph: GraphMinus::generate_empty_digraph(),
            is_directed: is_directed
        }
    }

    pub fn generate_empty_ungraph() -> TypedGraphMap<Id, NL, EL, Undirected, L> {
        TypedGraphMap::<Id, NL, EL, Undirected, L>::new()
    }

    pub fn generate_empty_digraph() -> TypedGraphMap<Id, NL, EL, Directed, L> {
        TypedGraphMap::<Id, NL, EL, Directed, L>::new()
    }

    pub fn run_minus(
        &mut self,
        graph0: &GeneralGraph<Id, NL, EL, L>,
        graph1: &GeneralGraph<Id, NL, EL, L>
    )
    {
        if graph0.is_directed() {
            self.di_run_minus(graph0, graph1);
        } else {
            self.un_run_minus(graph0, graph1);
        }
    }

    pub fn un_run_minus(
        &mut self,
        graph0: &GeneralGraph<Id, NL, EL, L>,
        graph1: &GeneralGraph<Id, NL, EL, L>
    )
    {
        for node in graph0.nodes() {
            let id = node.get_id();
            self.un_result_graph.add_node(id, graph0.get_node_label(id).cloned());
        }

        for edge in graph0.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            self.un_result_graph.add_edge(src, dst, graph0.get_edge_label(src, dst).cloned());
        }

        for node in graph1.nodes() {
            let id = node.get_id();
            self.un_result_graph.remove_node(id);
        }

        for edge in graph1.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            self.un_result_graph.remove_edge(src, dst);
        }
    }

    pub fn di_run_minus(
        &mut self,
        graph0: &GeneralGraph<Id, NL, EL, L>,
        graph1: &GeneralGraph<Id, NL, EL, L>
    )
    {
        for node in graph0.nodes() {
            let id = node.get_id();
            self.di_result_graph.add_node(id, graph0.get_node_label(id).cloned());
        }

        for edge in graph0.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            self.di_result_graph.add_edge(src, dst, graph0.get_edge_label(src, dst).cloned());
        }

        for node in graph1.nodes() {
            let id = node.get_id();
            self.di_result_graph.remove_node(id);
        }

        for edge in graph1.edges() {
            let src = edge.get_start();
            let dst = edge.get_target();
            self.di_result_graph.remove_edge(src, dst);
        }
    }

    pub fn get_result_graph(&self) -> Box<GeneralGraph<Id, NL, EL, L>> {
        if self.is_directed {
            Box::new(self.di_result_graph.clone())
        } else {
            Box::new(self.un_result_graph.clone())
        }
    }
}