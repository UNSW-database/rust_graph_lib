use graph_impl::{TypedGraphMap, DiGraphMap, UnGraphMap};
use prelude::*;
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
    pub fn mut_parent(&mut self) -> &mut HashMap<Id, Id> {
        return self.parent_ref.get_mut();
    }


//    /// Get immutable reference of parent map
//    pub fn parent(&self) -> &HashMap<Id, Id> {
//        return self.parent_ref.borrow_mut();
//    }


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
        edge: &EdgeTrait<Id, Id>
    )
    {
        let x = edge.get_start();
        let y = edge.get_target();

        if !self.mut_parent().contains_key(&x) {
            self.mut_parent().insert(x, x);
            self.count += 1;
        }

        if !self.mut_parent().contains_key(&y) {
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
        &mut self,
        node: Id
    ) -> Option<&Id>
    {
        return self.mut_parent().get(&node);
    }

    /// Get the root of a node.
    pub fn get_root(
        &mut self,
        mut node: Id
    ) -> Option<Id>
    {
        while self.mut_parent().get(&node)!= Some(&node) {
            let p = self.mut_parent()[&node];
            let pp = self.mut_parent()[&p];

            self.mut_parent().insert(node, pp);
            node = pp;
        }

        if self.mut_parent().get(&node) != None {
            return Some(node);
        } else {
            None
        }
    }

    /// Check if two nodes are belong to the same component.
    pub fn is_connected(&mut self, node0: Id, node1: Id) ->bool {
        if !self.mut_parent().contains_key(&node0) || !self.mut_parent().contains_key(&node1) {
            return false;
        } else {
            return self.get_root(node0) == self.get_root(node1);

        }
    }


    /// Clear the state.
    pub fn reset(&mut self)
    {
        self.mut_parent().clear();
        self.count = 0;
    }

    /// Get all nodes in the component of the given node.
    pub fn get_connected_nodes(&mut self, node: Id) -> Option<Vec<Id>> {
        if self.mut_parent().contains_key(&node) {
            let mut result:Vec<Id> = Vec::new();
            let root_id = self.get_root(node);
            let mut keys: Vec<Id> = Vec::new();

            for key in self.mut_parent().keys() {
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

//
//#[derive(Clone)]
//pub struct ConnSubgraph<'a, Id:IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a> {
//    /// The map of each root to its subgraph
//    pub subgraphs_ref: RefCell<HashMap<Id, &'a GeneralGraph<Id, NL, EL>>>,
//    /// The reference to the graph that algorithm is running on
//    pub graph: &'a GeneralGraph<Id, NL, EL>
//
//}
//
//
//impl<'a, Id:IdType + 'a, NL: Eq + Hash + 'a, EL: Eq + Hash + 'a> ConnSubgraph<'a, Id, NL, EL>
//{
//    /// Create a new **ConnSubgraph** by initialising empty subgraphs_ref, and save the reference
//    /// of input graph.
//    pub fn new<G: GeneralGraph<Id, NL, EL>> (
//        graph: &'a G
//    ) -> Self
//    {
//        let mut subgraphs = HashMap::with_capacity(node_count);
//
//        let mut cc = ConnComp::new(graph);
//
//        let mut empty_graph = *graph;
//
//        ConnSubgraph {
//
//        }
//
//    }
//
//
//}



pub fn test_conn_comp() {
    test_cc_one_component();
    test_cc_seperate_components();
}


pub fn test_cc_one_component() {
    let mut graph = UnGraphMap::<u32>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(2, 3, None);

    let mut cc = ConnComp::new(&graph);

    assert_eq!(cc.count, 1);

    assert_eq!(cc.is_connected(1, 2), true);
    assert_eq!(cc.is_connected(1, 3), true);

    assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 3);

    println!("test_cc_one_component successful!")
}


pub fn test_cc_seperate_components() {
    let mut graph = UnGraphMap::<u32>::new();
    graph.add_edge(1, 2, None);
    graph.add_edge(3, 4, None);

    let mut cc = ConnComp::new(&graph);

    assert_eq!(cc.count, 2);

    assert_eq!(cc.is_connected(1, 2), true);
    assert_eq!(cc.is_connected(2, 3), false);

    assert_eq!(cc.get_connected_nodes(1).unwrap().len(), 2);

    println!("test_cc_seperate_components successful!")
}

