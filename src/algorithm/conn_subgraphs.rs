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
use std::hash::Hash;

use algorithm::conn_comp::ConnComp;
use graph_impl::graph_map::new_general_graphmap;
use prelude::*;

/// Enumeration of Connected subgraphs of a graph.
///
/// `ConnSubgraph` is not recursive.
/// The algorithm first gets all connected components using ConnComp algorithm.
/// Then generates a vector of subgraphs according to nodes and edges
/// corresponding to each component.
///
/// Example:
///
/// ```
/// use rust_graph::algorithm::ConnSubgraph;
/// use rust_graph::prelude::*;
/// use rust_graph::graph_impl::UnGraphMap;
///
/// let mut graph = UnGraphMap::<u32, u32, u32>::new();
/// graph.add_node(1, Some(0));
/// graph.add_node(2, Some(1));
/// graph.add_node(3, Some(2));
/// graph.add_node(4, Some(3));
///
/// graph.add_edge(1, 2, Some(10));
/// graph.add_edge(3, 4, Some(20));
///
/// let cs = ConnSubgraph::new(&graph);
/// let subgraphs = cs.into_result();
/// ```
///
pub struct ConnSubgraph<
    'a,
    Id: IdType,
    NL: Eq + Hash + Clone + 'a,
    EL: Eq + Hash + Clone + 'a,
    L: IdType,
> {
    /// The result vector of subgraphs
    subgraphs: Vec<Box<GeneralGraph<Id, NL, EL, L> + 'a>>,
    /// The vector of roots. e.g roots[subgraph_index] = subgraph_root_id.
    roots: Vec<Id>,
    /// The Connected Components of given graph
    cc: ConnComp<Id>,
}

impl<'a, Id: IdType, NL: Eq + Hash + Clone + 'a, EL: Eq + Hash + Clone + 'a, L: IdType>
    ConnSubgraph<'a, Id, NL, EL, L>
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
        ConnSubgraph {
            subgraphs: Vec::new(),
            roots: Vec::new(),
            cc: ConnComp::new(graph),
        }
    }

    /// Run the graph enumeration by adding each node and edge to the subgraph that it
    /// corresponds to.
    pub fn run_subgraph_enumeration(&mut self, graph: &GeneralGraph<Id, NL, EL, L>) {
        self.process_nodes(graph);
        self.process_edges(graph);
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

    /// Add nodes to their corresponding subgraphs
    fn process_nodes(&mut self, graph: &GeneralGraph<Id, NL, EL, L>) {
        for id in graph.node_indices() {
            let root = self.cc.get_root(id).unwrap();
            let label = graph.get_node_label(id).cloned();
            let index = self.root_to_subgraph(root);

            if let Some(index) = index {
                self.subgraphs[index]
                    .as_mut_graph()
                    .unwrap()
                    .add_node(id, label);
            } else {
                if graph.is_directed() {
                    self.subgraphs.push(new_general_graphmap(true));
                } else {
                    self.subgraphs.push(new_general_graphmap(false));
                }
                self.roots.push(root);
                let length = self.subgraphs.len();
                self.subgraphs[length - 1]
                    .as_mut_graph()
                    .unwrap()
                    .add_node(id, label);
            }
        }
    }

    /// Add edges to their corresponding subgraphs
    fn process_edges(&mut self, graph: &GeneralGraph<Id, NL, EL, L>) {
        for edge in graph.edges() {
            let start = edge.get_start();
            let target = edge.get_target();
            let root = self.cc.get_root(start).unwrap();
            let label = graph.get_edge_label(start, target).cloned();
            let index = self.root_to_subgraph(root);

            if let Some(index) = index {
                self.subgraphs[index]
                    .as_mut_graph()
                    .unwrap()
                    .add_edge(start, target, label);
            } else {
                if graph.is_directed() {
                    self.subgraphs.push(new_general_graphmap(true));
                } else {
                    self.subgraphs.push(new_general_graphmap(false));
                }
                self.roots.push(root);
                let length = self.subgraphs.len();
                self.subgraphs[length - 1]
                    .as_mut_graph()
                    .unwrap()
                    .add_edge(start, target, label);
            }
        }
    }
}
