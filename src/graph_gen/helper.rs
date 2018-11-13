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

use itertools::Itertools;
use rand::{Rng, ThreadRng};

use generic::{GraphLabelTrait, GraphType, IdType, Iter, MapTrait};
use graph_impl::TypedGraphMap;

pub fn complete_edge_pairs<'a, Ty>(n: usize) -> Iter<'a, (usize, usize)>
where
    Ty: 'a + GraphType,
{
    if Ty::is_directed() {
        Iter::new(Box::new(
            (0..n)
                .tuple_combinations()
                .flat_map(|(s, d)| vec![(s, d), (d, s)]),
        ))
    } else {
        Iter::new(Box::new((0..n).tuple_combinations()))
    }
}

pub fn random_node_label<Id, NL, EL, Ty>(
    rng: &mut ThreadRng,
    g: &TypedGraphMap<Id, NL, EL, Ty>,
) -> Option<NL>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    let labels = g.get_node_label_map();

    if labels.is_empty() {
        return None;
    }

    let random_index = rng.gen_range(0, labels.len());

    labels.get_item(random_index).cloned()
}

pub fn random_edge_label<Id, NL, EL, Ty>(
    rng: &mut ThreadRng,
    g: &TypedGraphMap<Id, NL, EL, Ty>,
) -> Option<EL>
where
    Id: IdType,
    NL: Hash + Eq + Clone,
    EL: Hash + Eq + Clone,
    Ty: GraphType,
{
    let labels = g.get_edge_label_map();

    if labels.is_empty() {
        return None;
    }

    let random_index = rng.gen_range(0, labels.len());

    labels.get_item(random_index).cloned()
}
