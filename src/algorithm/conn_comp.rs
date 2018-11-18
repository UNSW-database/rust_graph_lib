use rust_graph::graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;
use std::hash::Hash;
use std::collections::HashMap;
use std::cell::RefCell;


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
#[derive(Clone)]
pub struct ConnComp<Id> {
    /// The map of each node to its root
    pub parent_ref: RefCell<HashMap<Id, Id>>,
    /// The number of connected components found
    pub count: usize,
    /// The subgraphs of corresponding connected components

}


impl<Id:IdType> ConnComp<Id>
{
    /// Create a new **ConnComp** by initialising empty root map, and set count to be number
    /// of nodes in graph.
    pub fn new<G: GeneralGraph<Id, NL, EL>, NL: Eq + Hash, EL: Eq + Hash> (
        graph: &G
    ) -> Self
    {
        let mut cc = ConnComp::empty(graph.node_count());
        cc.run_detection(graph);
        cc
    }

    /// Create a new **ConnComp**.
    pub fn empty(node_count: usize) -> Self
    {
        ConnComp {
            parent_ref: RefCell::new(HashMap::with_capacity(node_count)),
            count: 0,
        }
    }

    /// Get mutable reference of parent map
    pub fn parent(&mut self) -> &mut HashMap<Id, Id> {
        return self.parent_ref.get_mut();
    }

    /// Run the detection upon every edge. Update the root map based on every edge
    pub fn run_detection<G: GeneralGraph<Id, NL, EL>, NL: Eq + Hash, EL: Eq + Hash> (
        &mut self,
        graph: &G
    )
    {
        for edge in graph.edges() {
            self.process_new_edge(&edge);
        }
    }

    /// Update the root map based on a newly given edge
    /// Can be called at anytime after instantiating a ConnComp instance
    pub fn process_new_edge (
        &mut self,
        edge: &EdgeTrait<Id>
    )
    {
        let x = edge.get_start();
        let y = edge.get_target();

        if !self.parent().contains_key(&x) {
            self.parent().insert(x, x);
            self.count += 1;
        }

        if !self.parent().contains_key(&y) {
            self.parent().insert(y, y);
            self.count += 1;
        }

        let x_root = self.get_root(x).unwrap();
        let y_root = self.get_root(y).unwrap();

        if x_root != y_root {
            self.count -= 1;
            self.parent().insert(x_root, y_root);
        }
    }

    /// Get the parent of a node.
    pub fn get_parent(
        &mut self,
        node: Id
    ) -> Option<&Id>
    {
        return self.parent().get(&node);
    }

    /// Get the root of a node.
    pub fn get_root(
        &mut self,
        mut node: Id
    ) -> Option<Id>
    {
        while self.parent().get(&node)!= Some(&node) {
            let p = self.parent()[&node];
            let pp = self.parent()[&p];

            self.parent().insert(node, pp);
            node = pp;
        }

        if self.parent().get(&node) != None {
            return Some(node);
        } else {
            None
        }
    }

    /// Check if two nodes are belong to the same component.
    pub fn is_connected(&mut self, node0: Id, node1: Id) ->bool {
        if !self.parent().contains_key(&node0) || !self.parent().contains_key(&node1) {
            return false;
        } else {
            return self.get_root(node0) == self.get_root(node1);

        }
    }


    /// Clear the state.
    pub fn reset(&mut self)
    {
        self.parent().clear();
        self.count = 0;
    }

    /// Get all nodes in the component of the given node.
    pub fn get_connected_nodes(&mut self, node: Id) -> Option<Vec<Id>> {
        if self.parent().contains_key(&node) {
            let mut result:Vec<Id> = Vec::new();
            let root_id = self.get_root(node);
            let mut keys: Vec<Id> = Vec::new();

            for key in self.parent().keys() {
                keys.push(*key);
            }

            for n in keys {
                if self.get_root(n) == root_id {
                    result.push(n);
                }
            }

            return Some(result);
        } else {
            None
        }
    }

}