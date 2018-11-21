use prelude::*;
use std::hash::Hash;
use generic::edge::EdgeType;
use std::cmp::Ordering;

/// Detection of Connected Component (ConnComp) of a graph.
///
/// `CanoLabel` is not recursive.
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
pub struct CanoLabel {
    /// The string of canonical label
    pub label: String
}


impl CanoLabel
{
    /// Create a new **ConnComp** by initialising empty root map, and set count to be number
    /// of nodes in graph.
    pub fn new<Id:IdType, NL: Eq + Hash, EL: Eq + Hash, L: IdType> (
        graph: &GeneralGraph<Id, NL, EL, L>
    ) -> Self
    {
        let mut cl = CanoLabel::empty();
        cl.run_labelling(graph);
        cl
    }

    /// Create a new **ConnComp**.
    pub fn empty() -> Self
    {
        CanoLabel {
            label: String::new(),
        }
    }


    /// Run the labelling upon sorted edges. Update the label string based on every edge.
    pub fn run_labelling<Id:IdType, NL: Eq + Hash, EL: Eq + Hash, L: IdType> (
        &mut self,
        graph: &GeneralGraph<Id, NL, EL, L>
    )
    {
        let edge_iter = graph.edges();
        let mut edges:Vec<EdgeType<Id, L>> = Vec::with_capacity(graph.edge_count());
        for edge in edge_iter {
            edges.push(edge);
        }
        edges.sort_by(|a, b| CanoLabel::compare(a, b));

        for edge in edges {
            let src = edge.get_start();
            let dst = edge.get_target();
            self.label.push_str(&format!("({},{})", src.id(), dst.id()));
        }
    }

    pub fn get_label(&self) -> String {
        return self.label.clone();
    }

    pub fn compare<Id:IdType, L: IdType>(a: &EdgeType<Id, L>, b: &EdgeType<Id, L>) -> Ordering {
        if a.get_start().id() < b.get_start().id() {
            Ordering::Less
        } else if a.get_start().id() > b.get_start().id() {
            Ordering::Greater
        } else {
            if a.get_target().id() < b.get_target().id() {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
    }
}
