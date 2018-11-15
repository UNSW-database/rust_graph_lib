extern crate rust_graph;

use rust_graph::graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use rust_graph::prelude::*;
use std::hash::Hash;
use std::collections::HashMap;


/// Detection of Connected Component (CC) of a graph.
///
/// `CC` is not recursive.
/// The detection iterates through every edge.
/// Nodes that are involved in each edge is merged together.
///
/// There are k loops and each loop processing the root array costs log(n).
/// Therefore, time complexity is O(k*log(n)).
///
/// `CC` does not itself borrow the graph, and because of this you can run
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
/// let mut cc = algorithm::cc::CC::new(&graph);
/// cc.get_nodes_in_component_of_given_node(0);
/// cc.check_nodes_in_same_component(0, 1);
/// cc.process_new_edge(edge);
///
/// ```
///
/// **Note:** The algorithm may not behave correctly if nodes are removed
/// during iteration. It may not necessarily visit added nodes or edges.
#[derive(Clone)]
pub struct CC<Id> {
    /// The map of each node to its root
    pub root: HashMap<Id, Id>,
    /// The number of connected components found
    pub count: usize,
}


impl<Id:IdType> CC<Id>
    where Id: IdType,
{
    /// Create a new **CC** by initialising empty root map, and set count to be number
    /// of nodes in graph.
    pub fn new<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> (
        graph: &TypedGraphMap<Id, NL, EL, Ty>
    ) -> Self
    {
        let mut cc = CC::empty(&graph);
        cc.run_detection(graph);
        cc
    }

    /// Create a `CC` from a root map and a count
    pub fn from_parts(root: HashMap<Id, Id>, count: usize) -> Self {
        CC {
            root: root,
            count: count,
        }
    }

    /// Create a new **CC**.
    pub fn empty<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType> (
        graph: &TypedGraphMap<Id, NL, EL, Ty>
    ) -> Self
    {
        CC {
            root: HashMap::new(),
            count: 0,
        }
    }

    /// Run the detection upon every edge. Update the root map based on every edge
    pub fn run_detection<NL: Hash + Eq, EL: Hash + Eq, Ty: GraphType>(
        &mut self,
        graph: &TypedGraphMap<Id, NL, EL, Ty>
    )
    {
        for edge in graph.edges() {
            self.process_new_edge(&edge);
        }
    }

    /// Update the root map based on a newly given edge
    /// Can be called at anytime after instantiating a CC instance
    pub fn process_new_edge (
        &mut self,
        edge: &EdgeTrait<Id>
    )
    {
        let x = edge.get_start();
        let y = edge.get_target();

        if !self.root.contains_key(&x) {
            self.root.insert(x, x);
            self.count += 1;
        }

        if !self.root.contains_key(&y) {
            self.root.insert(y, y);
            self.count += 1;
        }

        let x_root = self.get_root(x);
        let y_root = self.get_root(y);

        if x_root != y_root {
            self.count -= 1;
            self.root.insert(x_root, y_root);
        }
    }

    /// Get the root of a node.
    pub fn get_root(
        &mut self,
        node: Id
    ) -> Id
    {
        let mut i = node;

        while self.root.get(&i)!= Some(&i) {
            let temp = *self.root.get_mut(&i).unwrap();
            let target = *self.root.get_mut(&temp).unwrap();

            self.root.insert(i, target);
            i = target;
        }

        return i;
    }

    /// Check if two nodes are belong to the same component.
    pub fn check_nodes_in_same_component(&mut self, node0: Id, node1: Id)->bool {
        return self.get_root(node0) == self.get_root(node1);
    }


    /// Clear the state.
    pub fn reset(&mut self)
    {
        self.root.clear();
        self.count = 0;
    }

    /// Get all nodes in the component of the given node.
    pub fn get_nodes_in_component_of_given_node(&mut self, node:Id) -> Vec<Id> {
        let mut result:Vec<Id> = Vec::new();
        let root_id = self.get_root(node);
        let mut keys: Vec<Id> = Vec::new();

        for key in self.root.keys() {
            keys.push(*key);
        }

        for n in keys {
            if self.get_root(n) == root_id {
                result.push(n);
            }
        }

        return result;
    }

}