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
use std::marker::PhantomData;

use fixedbitset::FixedBitSet;

use prelude::*;

/// A depth first search (Dfs) of a graph.
///
/// The traversal starts at a given node and only traverses nodes reachable
/// from it.
///
/// `Dfs` is not recursive.
///
/// Example:
///
/// ```
/// use rust_graph::prelude::*;
/// use rust_graph::graph_impl::UnGraphMap;
/// use rust_graph::algorithm::Dfs;
///
/// let mut graph = UnGraphMap::<Void>::new();
///
/// graph.add_edge(0, 1, None);
/// graph.add_edge(1, 2, None);
/// graph.add_edge(2, 3, None);
///
/// let mut dfs = Dfs::new(&graph, Some(0));
/// let mut i = 0;
///
/// for n in dfs {
///     assert_eq!(n, i);
///     i = i + 1;
/// }
///
/// ```
///
#[derive(Clone)]
pub struct Dfs<
    'a,
    Id: IdType,
    NL: Eq + Hash + 'a,
    EL: Eq + Hash + 'a,
    L: IdType,
    G: GeneralGraph<Id, NL, EL, L> + ?Sized,
> {
    /// The stack of nodes to visit
    stack: Vec<Id>,
    /// The map of discovered nodes
    discovered: FixedBitSet,
    /// The reference to the graph that algorithm is running on
    graph: &'a G,

    _ph: PhantomData<(NL, EL, L)>,
}

impl<
        'a,
        Id: IdType,
        NL: Eq + Hash + 'a,
        EL: Eq + Hash + 'a,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L> + ?Sized,
    > Dfs<'a, Id, NL, EL, L, G>
{
    /// Create a new **Dfs** by initialising empty prev_discovered map, and put **start**
    /// in the queue of nodes to visit.
    pub fn new(graph: &'a G, start: Option<Id>) -> Self {
        let mut dfs = Dfs::with_capacity(graph);
        dfs.move_to(start);
        dfs
    }

    /// Create a `Dfs` from a vector and a map
    pub fn from_parts(stack: Vec<Id>, discovered: FixedBitSet, graph: &'a G) -> Self {
        Dfs {
            stack,
            discovered,
            graph,
            _ph: PhantomData,
        }
    }

    /// Create a new **Dfs**.
    pub fn with_capacity(graph: &'a G) -> Self {
        let mut discovered: FixedBitSet =
            FixedBitSet::with_capacity(graph.max_seen_id().unwrap().id() + 1);
        discovered.insert_range(..);

        Dfs {
            stack: Vec::new(),
            discovered,
            graph,
            _ph: PhantomData,
        }
    }

    /// Clear the visit state
    pub fn reset(&mut self) {
        self.discovered.clear();
        self.stack.clear();
        self.discovered.insert_range(..);
    }

    /// Randomly pick a unvisited node from the map.
    fn next_unvisited_node(&self) -> Option<Id> {
        for node in self.discovered.ones() {
            if self.graph.has_node(Id::new(node)) {
                return Some(Id::new(node));
            }
        }
        None
    }

    /// Clear the stack and restart the dfs from a particular node.
    fn move_to(&mut self, start: Option<Id>) {
        if let Some(start) = start {
            if !self.graph.has_node(start) {
                panic!("Node {:?} is not in the graph.", start);
            } else {
                self.discovered.set(start.id(), false);
                self.stack.clear();
                self.stack.push(start);
            }
        } else if self.graph.node_count() == 0 {
            panic!("Graph is empty")
        } else {
            let id = self.graph.node_indices().next().unwrap();
            self.discovered.set(id.id(), false);
            self.stack.clear();
            self.stack.push(id);
        }
    }
}

impl<
        'a,
        Id: IdType,
        NL: Eq + Hash + 'a,
        EL: Eq + Hash + 'a,
        L: IdType,
        G: GeneralGraph<Id, NL, EL, L> + ?Sized,
    > Iterator for Dfs<'a, Id, NL, EL, L, G>
{
    type Item = Id;

    /// Return the next node in the Dfs, or **None** if the traversal is done.
    fn next(&mut self) -> Option<Id> {
        if self.stack.is_empty() {
            if let Some(id) = self.next_unvisited_node() {
                self.stack.push(id);
                self.discovered.set(id.id(), false);
            }
        }

        if let Some(current_node) = self.stack.pop() {
            for neighbour in self.graph.neighbors_iter(current_node) {
                if self.discovered.contains(neighbour.id()) {
                    self.discovered.set(neighbour.id(), false);
                    self.stack.push(neighbour);
                }
            }
            Some(current_node)
        } else {
            None
        }
    }
}
