use algorithm::conn_comp::ConnComp;
use generic::dtype::IdType;
use graph_impl::graph_map::new_general_graphmap;
use prelude::*;
use std::hash::Hash;
use itertools::Itertools;

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
/// use rust_graph::prelude::*;
/// use rust_graph::graph_impl::UnGraphMap;
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
pub struct ConnSubgraph<
    'a,
    Id: IdType + 'a,
    NL: Eq + Hash + Clone + 'a,
    EL: Eq + Hash + Clone + 'a,
    L: IdType + 'a,
> {
    /// The result vector of subgraphs
    subgraphs: Vec<Box<GeneralGraph<Id, NL, EL, L> + 'a>>,
    /// The vector of roots. e.g roots[subgraph_index] = subgraph_root_id.
    roots: Vec<Id>,
    /// The Connected Components of given graph
    cc: ConnComp<Id>,
}

impl<
        'a,
        Id: IdType + 'a,
        NL: Eq + Hash + Clone + 'a,
        EL: Eq + Hash + Clone + 'a,
        L: IdType + 'a,
    > ConnSubgraph<'a, Id, NL, EL, L>
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
            cc: cc,
        }
    }

    /// Run the graph enumeration by adding each node and edge to the subgraph that it
    /// corresponds to.
    pub fn run_subgraph_enumeration(&mut self, graph: &GeneralGraph<Id, NL, EL, L>) {
        if graph.edge_count() != 0 {
            let mut num_edges:usize = 1;
            while num_edges <= graph.edge_count() {
                for edge_vec in graph.edges().combinations(num_edges) {
                    let mut g_tmp = new_general_graphmap(graph.is_directed());
                    for edge in edge_vec {
                        let mut_g = g_tmp.as_mut_graph().unwrap();
                        let (start, target) = (edge.get_start(), edge.get_target());

                        let node_label_one = graph.get_node_label(start);
                        let node_label_two = graph.get_node_label(target);

                        mut_g.add_node(start, node_label_one.cloned());
                        mut_g.add_node(target, node_label_two.cloned());

                        let edge_label = graph.get_edge_label(start, target);

                        mut_g.add_edge(start, target, edge_label.cloned());
                    }

                    if g_tmp.node_count() > 0 && self.cc.get_count() == 1 {
                        self.subgraphs.push(g_tmp);
                    }
                }
                num_edges += 1;
            }
        }
    }

    /// Get the subgraph from a given root node id.
    pub fn root_to_subgraph(&self, root: Id) -> Option<usize> {
        for index in 0..self.roots.len() {
            if self.roots[index] == root {
                return Some(index);
            }
        }
        None
    }

    /// Return the result vector of subgraphs.
    pub fn into_result(self) -> Vec<Box<GeneralGraph<Id, NL, EL, L> + 'a>> {
        self.subgraphs
    }
}
