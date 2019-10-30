/*
 * Copyright (c) 2018 UNSW Sydney, Data and Knowledge Group.
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
use hashbrown::HashMap;
use std::cell::RefMut;
use std::cell::{Ref, RefCell};
use std::hash::Hash;

use crate::prelude::*;

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
/// use rust_graph::prelude::*;
/// use rust_graph::graph_impl::UnGraphMap;
/// use rust_graph::algorithm::ConnComp;
///
/// let mut graph = UnGraphMap::<Void>::new();
///
/// graph.add_edge(0, 1, None);
/// graph.add_edge(1, 2, None);
/// graph.add_edge(3, 4, None);
///
/// let mut cc = ConnComp::new(&graph);
/// cc.get_connected_nodes(0);
/// cc.is_connected(0, 1);
///
/// ```
///
#[derive(Debug, Clone)]
pub struct ConnComp<Id: IdType> {
    /// The map of each node to its root
    parent_ref: RefCell<HashMap<Id, Id>>,
    /// The number of connected components found
    count: usize,
}

impl<Id: IdType> ConnComp<Id> {
    /// Create a new **ConnComp** by initialising empty root map, and set count to be number
    /// of nodes in graph.
    pub fn new<NL: Eq + Hash, EL: Eq + Hash, L: IdType>(
        graph: &dyn GeneralGraph<Id, NL, EL, L>,
    ) -> Self {
        let mut cc = ConnComp::with_capacity(graph.node_count());
        cc.run_detection(graph);
        cc
    }

    /// Create a empty **ConnComp**.
    pub fn empty() -> Self {
        Self::with_capacity(0)
    }

    /// Create a new **ConnComp**.
    pub fn with_capacity(node_count: usize) -> Self {
        ConnComp {
            parent_ref: RefCell::new(HashMap::with_capacity(node_count)),
            count: 0,
        }
    }

    /// Get immutable reference of parent map
    pub fn parent(&self) -> Ref<HashMap<Id, Id>> {
        self.parent_ref.borrow()
    }

    /// Run the detection upon every edge. Update the root map based on every edge
    pub fn run_detection<NL: Eq + Hash, EL: Eq + Hash, L: IdType>(
        &mut self,
        graph: &dyn GeneralGraph<Id, NL, EL, L>,
    ) {
        for edge in graph.edge_indices() {
            self.process_new_edge(&edge);
        }

        for node in graph.node_indices() {
            if graph.total_degree(node) == 0 {
                self.mut_parent().insert(node, node);
                self.count += 1;
            }
        }
    }

    /// Update the root map based on a newly given edge
    /// Can be called at anytime after instantiating a ConnComp instance
    pub fn process_new_edge(&mut self, edge: &(Id, Id)) {
        let (x, y) = *edge;

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
    pub fn get_parent(&self, node: Id) -> Option<Id> {
        if let Some(id) = self.parent().get(&node) {
            Some(*id)
        } else {
            None
        }
    }

    /// Get the root of a node.
    pub fn get_root(&self, mut node: Id) -> Option<Id> {
        while self.parent().get(&node) != Some(&node) {
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
    pub fn is_connected(&self, node0: Id, node1: Id) -> bool {
        if !self.parent().contains_key(&node0) || !self.parent().contains_key(&node1) {
            false
        } else {
            self.get_root(node0) == self.get_root(node1)
        }
    }

    /// Clear the state.
    pub fn reset(&mut self) {
        self.mut_parent().clear();
        self.count = 0;
    }

    /// Get the number of components.
    pub fn get_count(&self) -> usize {
        return self.count;
    }

    /// Get all nodes in the component of the given node.
    pub fn get_connected_nodes(&self, node: Id) -> Option<Vec<Id>> {
        if self.parent().contains_key(&node) {
            let mut result: Vec<Id> = Vec::new();
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

    /// Get mutable reference of parent map
    fn mut_parent(&self) -> RefMut<HashMap<Id, Id>> {
        self.parent_ref.borrow_mut()
    }
}
