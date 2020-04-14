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

use rand::thread_rng;

use crate::generic::{GraphType, IdType, MutGraphTrait};
use crate::graph_impl::TypedGraphMap;
use crate::map::SetMap;

use crate::graph_gen::helper::{complete_edge_pairs, random_edge_label, random_node_label};

pub fn empty_graph<Id, NL, EL, Ty>(
    n: usize,
    node_label: Vec<NL>,
    edge_label: Vec<EL>,
) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    let mut rng = thread_rng();

    let node_label_map = SetMap::from_vec(node_label);
    let edge_label_map = SetMap::from_vec(edge_label);

    let mut g = TypedGraphMap::with_label_map(node_label_map, edge_label_map);

    for i in 0..n {
        let label = random_node_label(&mut rng, &g);
        g.add_node(Id::new(i), label);
    }

    g
}

pub fn complete_graph<Id, NL, EL, Ty>(
    n: usize,
    node_label: Vec<NL>,
    edge_label: Vec<EL>,
) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    let mut rng = thread_rng();

    let mut g = empty_graph::<Id, NL, EL, Ty>(n, node_label, edge_label);
    for (s, d) in complete_edge_pairs::<Ty>(n) {
        let label = random_edge_label(&mut rng, &g);
        g.add_edge(Id::new(s), Id::new(d), label);
    }

    g
}

pub fn empty_graph_unlabeled<Id, NL, EL, Ty>(n: usize) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    empty_graph(n, Vec::new(), Vec::new())
}

pub fn complete_graph_unlabeled<Id, NL, EL, Ty>(n: usize) -> TypedGraphMap<Id, NL, EL, Ty>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    complete_graph(n, Vec::new(), Vec::new())
}
