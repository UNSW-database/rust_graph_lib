use prelude::*;
use std::hash::Hash;
use std::cell::RefCell;
use std::cell::Ref;
use algorithm::conn_comp::ConnComp;
use graph_impl::{UnGraphMap, DiGraphMap, TypedGraphMap};
use generic::dtype::IdType;
use generic::node::NodeType;
use generic::edge::EdgeType;



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
pub struct ConnSubgraph<Id: IdType, NL: Eq + Hash + Clone, EL: Eq + Hash + Clone, L: IdType = Id> {
    /// The vector of undirected subgraphs
    pub un_subgraphs: Vec<TypedGraphMap<Id, NL, EL, Undirected, L>>,
    /// The vector of directed subgraphs
    pub di_subgraphs: Vec<TypedGraphMap<Id, NL, EL, Directed, L>>,
    /// The vector of roots
    un_roots: Vec<Id>,
    /// The Connected Components of given graph
    cc: ConnComp<Id>
}

impl<Id: IdType, NL: Eq + Hash + Clone, EL: Eq + Hash + Clone, L: IdType> ConnSubgraph<Id, NL, EL, L>
{
    /// Create a new **ConnSubgraph** by initialising empty subgraphs_ref, and save the reference
    /// of input graph.
    pub fn new(graph: &GeneralGraph<Id, NL, EL, L>) -> Self {
        let mut empty_graph:TypedGraphMap<Id, NL, EL, Undirected, L> = TypedGraphMap::<Id, NL, EL, Undirected, L>::new();

        let mut cs = ConnSubgraph::empty(graph);

        cs.run_subgraph_enumeration(graph);

        cs
    }

    pub fn empty(graph: &GeneralGraph<Id, NL, EL, L>) -> Self
    {
        let mut un_subgraphs: Vec<TypedGraphMap<Id, NL, EL, Undirected, L>> = Vec::new();
        let mut di_subgraphs: Vec<TypedGraphMap<Id, NL, EL, Directed, L>> = Vec::new();

        let mut cc = ConnComp::new(graph);

        ConnSubgraph {
            un_subgraphs: un_subgraphs,
            di_subgraphs: di_subgraphs,
            un_roots: Vec::new(),
            cc: cc
        }
    }

    pub fn generate_empty_ungraph() -> TypedGraphMap<Id, NL, EL, Undirected, L> {
        TypedGraphMap::<Id, NL, EL, Undirected, L>::new()
    }

    pub fn generate_empty_digraph() -> TypedGraphMap<Id, NL, EL, Directed, L> {
        TypedGraphMap::<Id, NL, EL, Directed, L>::new()
    }

    pub fn run_subgraph_enumeration(&mut self, graph: &GeneralGraph<Id, NL, EL, L>) {
        for node in graph.nodes() {
            if graph.is_directed() {
                self.di_process_node(node, graph);
            } else {
                self.un_process_node(node, graph);
            }
        }

        for edge in graph.edges() {
            if graph.is_directed() {
                self.di_process_edge(edge, graph);
            } else {
                self.un_process_edge(edge, graph);
            }
        }
    }

    pub fn un_process_node(&mut self, node: NodeType<Id, L>, graph: &GeneralGraph<Id, NL, EL, L>) {
        let root = self.cc.get_root(node.get_id()).unwrap();
        let id = node.get_id();
        let label = graph.get_node_label(id).cloned();

        if let Some(index) = self.root_to_subgraph(root) {
            self.un_subgraphs[index].add_node(id, label);
        } else {
            self.un_subgraphs.push(ConnSubgraph::generate_empty_ungraph());
            self.un_roots.push(root);
            let length = (self.un_subgraphs.len());
            self.un_subgraphs[length - 1].add_node(id, label);
        }
    }

    pub fn un_process_edge(&mut self, edge:EdgeType<Id, L>, graph: &GeneralGraph<Id, NL, EL, L>) {
        let root = self.cc.get_root(edge.get_start()).unwrap();
        let start = edge.get_start();
        let target = edge.get_target();
        let label = graph.get_edge_label(start, target).cloned();

        if let Some(index) = self.root_to_subgraph(root) {
            self.un_subgraphs[index].add_edge(start, target, label);
        } else {
            self.un_subgraphs.push(ConnSubgraph::generate_empty_ungraph());
            self.un_roots.push(root);
            let length = (self.un_subgraphs.len());
            self.un_subgraphs[length - 1].add_edge(start, target, label);
        }
    }

    pub fn di_process_node(&mut self, node: NodeType<Id, L>, graph: &GeneralGraph<Id, NL, EL, L>) {
        let root = self.cc.get_root(node.get_id()).unwrap();
        let id = node.get_id();
        let label = graph.get_node_label(id).cloned();

        if let Some(index) = self.root_to_subgraph(root) {
            self.di_subgraphs[index].add_node(id, label);
        } else {
            self.di_subgraphs.push(ConnSubgraph::generate_empty_digraph());
            self.un_roots.push(root);
            let length = (self.di_subgraphs.len());
            self.di_subgraphs[length - 1].add_node(id, label);
        }
    }

    pub fn di_process_edge(&mut self, edge:EdgeType<Id, L>, graph: &GeneralGraph<Id, NL, EL, L>) {
        let root = self.cc.get_root(edge.get_start()).unwrap();
        let start = edge.get_start();
        let target = edge.get_target();
        let label = graph.get_edge_label(start, target).cloned();

        if let Some(index) = self.root_to_subgraph(root) {
            self.di_subgraphs[index].add_edge(start, target, label);
        } else {
            self.di_subgraphs.push(ConnSubgraph::generate_empty_digraph());
            self.un_roots.push(root);
            let length = (self.di_subgraphs.len());
            self.di_subgraphs[length - 1].add_edge(start, target, label);
        }
    }

    pub fn root_to_subgraph(&mut self, root: Id) -> Option<usize> {
        for index in 0 .. self.un_roots.len() {
            if self.un_roots[index] == root {
                return Some(index);
            }
        }
        None
    }
}