use prelude::*;
use std::hash::Hash;
use std::collections::HashMap;
use std::cell::RefCell;
use std::cell::Ref;
use std::cell::RefMut;



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
pub struct ConnComp<Id> {
    /// The map of each node to its root
    pub parent_ref: RefCell<HashMap<Id, Id>>,
    /// The number of connected components found
    pub count: usize,
}


impl<Id:IdType> ConnComp<Id>
{
    /// Create a new **ConnComp** by initialising empty root map, and set count to be number
    /// of nodes in graph.
    pub fn new<NL: Eq + Hash, EL: Eq + Hash, L: IdType> (
        graph: &GeneralGraph<Id, NL, EL, L>
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
    pub fn mut_parent(&self) -> RefMut<HashMap<Id, Id>> {
        self.parent_ref.borrow_mut()
    }


    /// Get immutable reference of parent map
    pub fn parent(&self) -> Ref<HashMap<Id, Id>> {
        self.parent_ref.borrow()
    }

    /// Run the detection upon every edge. Update the root map based on every edge
    pub fn run_detection<NL: Eq + Hash, EL: Eq + Hash, L: IdType> (
        &mut self,
        graph: &GeneralGraph<Id, NL, EL, L>
    )
    {
        for edge in graph.edges() {
            self.process_new_edge(&edge);
        }
    }

    /// Update the root map based on a newly given edge
    /// Can be called at anytime after instantiating a ConnComp instance
    pub fn process_new_edge<L: IdType> (
        &mut self,
        edge: &EdgeTrait<Id, L>
    )
    {
        let x = edge.get_start();
        let y = edge.get_target();

        if !self.parent().contains_key(&x) {
            self.mut_parent().insert(x, x);
            self.count += 1;
        }

        if !self.parent().contains_key(&y) {
            self.mut_parent().insert(y, y);
            self.count += 1;
        }

        let x_root = self.get_root(x).unwrap();
        let y_root = self.get_root(y).unwrap();

        if x_root != y_root {
            self.count -= 1;
            self.mut_parent().insert(x_root, y_root);
        }
    }

    /// Get the parent of a node.
    pub fn get_parent(
        &self,
        node: Id
    ) -> Option<Id>
    {
        if let Some(id) = self.parent().get(&node) {
            Some(*id)
        } else {
            None
        }
    }

    /// Get the root of a node.
    pub fn get_root(
        &self,
        mut node: Id
    ) -> Option<Id>
    {
        while self.parent().get(&node)!= Some(&node) {
            let p = self.parent()[&node];
            let pp = self.parent()[&p];

            self.mut_parent().insert(node, pp);
            node = pp;
        }

        if self.parent().get(&node) != None {
            Some(node)
        } else {
            None
        }
    }

    /// Check if two nodes are belong to the same component.
    pub fn is_connected(&self, node0: Id, node1: Id) ->bool {
        if !self.parent().contains_key(&node0) || !self.parent().contains_key(&node1) {
            false
        } else {
            self.get_root(node0) == self.get_root(node1)
        }
    }


    /// Clear the state.
    pub fn reset(&mut self)
    {
        self.mut_parent().clear();
        self.count = 0;
    }

    /// Get all nodes in the component of the given node.
    pub fn get_connected_nodes(&self, node: Id) -> Option<Vec<Id>> {
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
            Some(result)
        } else {
            None
        }
    }
}
