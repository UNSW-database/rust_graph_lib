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
use itertools::Itertools;
use std::hash::Hash;

use algorithm::conn_comp::ConnComp;
use graph_impl::graph_map::new_general_graphmap;
use prelude::*;

/// Enumeration of Connected subgraphs of a graph.
///
/// `ConnSubgraph` is not recursive.
/// The algorithm first gets all the possible combination of edges which can form subgraphs.
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
}

impl<'a, Id: IdType, NL: Eq + Hash + Clone + 'a, EL: Eq + Hash + Clone + 'a, L: IdType>
    ConnSubgraph<'a, Id, NL, EL, L>
{
    /// Create a new **ConnSubgraph** by initialising empty result subgraph vector, and create a ConnComp
    /// instance with given graph. Then run the enumeration.
    pub fn new(graph: &GeneralGraph<Id, NL, EL, L>) -> Self {
        let mut cs = ConnSubgraph::empty();

        cs.run_subgraph_enumeration(graph);
        cs
    }

    /// Create a new **ConnSubgraph** by initialising empty result subgraph vector, and create a ConnComp
    /// instance with given graph.
    pub fn empty() -> Self {
        ConnSubgraph {
            subgraphs: Vec::new(),
        }
    }

    /// Run the graph enumeration by adding each node and edge to the subgraph that it
    /// corresponds to.
    pub fn run_subgraph_enumeration(&mut self, graph: &GeneralGraph<Id, NL, EL, L>) {
        if graph.edge_count() != 0 {
            let mut num_edges: usize = 1;
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

                    if g_tmp.node_count() > 0 && ConnComp::new(g_tmp.as_ref()).get_count() == 1 {
                        self.subgraphs.push(g_tmp);
                    }
                }
                num_edges += 1;
            }
        }
    }

    /// Return the result vector of subgraphs.
    pub fn into_result(self) -> Vec<Box<GeneralGraph<Id, NL, EL, L> + 'a>> {
        self.subgraphs
    }
}
