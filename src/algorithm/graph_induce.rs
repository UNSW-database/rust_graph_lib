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

use generic::dtype::IdType;
use graph_impl::graph_map::{new_general_graphmap};
use prelude::*;

macro_rules! induce {
    ($graph0:ident,$graph1:ident,$graph:ident) => {
        for id in $graph1.node_indices() {
            let mut_graph = $graph.as_mut_graph().unwrap();
            mut_graph.add_node(id, $graph1.get_node_label(id).cloned());
        }

        for (src, dst) in $graph1.edge_indices() {
            let mut_graph = $graph.as_mut_graph().unwrap();
            mut_graph.add_edge(src, dst, $graph1.get_edge_label(src, dst).cloned());
        }

        for (src, dst) in $graph0.edge_indices() {
            if $graph.has_node(src) && $graph.has_node(dst) {
                if $graph.has_edge(src, dst) &&
                    $graph0.get_edge_label(src, dst) == $graph.get_edge_label(src, dst) {
                    continue;
                }
                let mut_graph = $graph.as_mut_graph().unwrap();
                mut_graph.add_edge(src, dst, $graph0.get_edge_label(src, dst).cloned());
            }
        }
    };
}

/// Graph Interset of two graphs, g0 and g1.
///
/// Firstly, nodes that are in both g0 and g1 are added to the result graph.
/// The edges are added which have both of their ends in the result graph.
///
/// Example:
///
/// ```
/// use rust_graph::algorithm::intersect;
/// use rust_graph::prelude::*;
/// use rust_graph::graph_impl::DiGraphMap;
///
/// let mut graph0 = DiGraphMap::<u32>::new();
/// graph0.add_node(1, Some(1));
/// graph0.add_node(2, Some(2));
/// graph0.add_node(3, Some(3));
/// graph0.add_edge(1, 2, Some(12));
/// graph0.add_edge(2, 3, Some(23));
///
/// let mut graph1 = DiGraphMap::<u32>::new();
/// graph1.add_node(3, Some(3));
/// graph1.add_node(4, Some(4));
/// graph1.add_node(1, Some(1));
/// graph1.add_edge(1, 4, Some(14));
/// graph1.add_edge(3, 4, Some(34));
/// graph1.add_edge(1, 3, Some(13));
///
/// let result_graph = graph_intersect(&graph0, &graph1);
///
/// ```
///
pub fn graph_induce<
    'a,
    'b,
    'c,
    Id: IdType + 'c,
    NL: Eq + Hash + Clone + 'c,
    EL: Eq + Hash + Clone + 'c,
    L: IdType + 'c,
>(
    graph0: &'a GeneralGraph<Id, NL, EL, L>,
    graph1: &'b GeneralGraph<Id, NL, EL, L>,
) -> Box<GeneralGraph<Id, NL, EL, L> + 'c> {
    let mut result_graph = new_general_graphmap(graph0.is_directed());
    induce!(graph0, graph1, result_graph);
    result_graph
}
