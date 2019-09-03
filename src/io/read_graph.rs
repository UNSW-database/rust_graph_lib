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
use crate::generic::{IdType, Iter, MutGraphTrait};
use crate::io::csv::CborValue;
use itertools::Itertools;
use serde::Deserialize;
use std::hash::Hash;

pub trait ReadGraph<Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    fn get_node_iter(&self, idx: usize) -> Option<Iter<(Id, Option<NL>)>>;
    fn get_edge_iter(&self, idx: usize) -> Option<Iter<(Id, Id, Option<EL>)>>;
    fn get_prop_node_iter(&self, idx: usize) -> Option<Iter<(Id, Option<NL>, CborValue)>>;
    fn get_prop_edge_iter(&self, idx: usize) -> Option<Iter<(Id, Id, Option<EL>, CborValue)>>;
    fn num_of_node_files(&self) -> usize;
    fn num_of_edge_files(&self) -> usize;

    fn node_iter(&self) -> Iter<(Id, Option<NL>)> {
        let iter_vec = (0..self.num_of_node_files())
            .map(|i| self.get_node_iter(i).unwrap())
            .collect_vec();
        let iter = iter_vec.into_iter().flat_map(|x| x);

        Iter::new(Box::new(iter))
    }

    fn edge_iter(&self) -> Iter<(Id, Id, Option<EL>)> {
        let iter_vec = (0..self.num_of_edge_files())
            .map(|i| self.get_edge_iter(i).unwrap())
            .collect_vec();
        let iter = iter_vec.into_iter().flat_map(|x| x);

        Iter::new(Box::new(iter))
    }

    fn prop_node_iter(&self) -> Iter<(Id, Option<NL>, CborValue)> {
        let iter_vec = (0..self.num_of_node_files())
            .map(|i| self.get_prop_node_iter(i).unwrap())
            .collect_vec();
        let iter = iter_vec.into_iter().flat_map(|x| x);

        Iter::new(Box::new(iter))
    }

    fn prop_edge_iter(&self) -> Iter<(Id, Id, Option<EL>, CborValue)> {
        let iter_vec = (0..self.num_of_edge_files())
            .map(|i| self.get_prop_edge_iter(i).unwrap())
            .collect_vec();
        let iter = iter_vec.into_iter().flat_map(|x| x);

        Iter::new(Box::new(iter))
    }
}

pub trait ReadGraphTo<Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static>:
    ReadGraph<Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    fn read<G: MutGraphTrait<Id, NL, EL, L>, L: IdType>(&self, g: &mut G) {
        for (n, label) in self.node_iter() {
            g.add_node(n, label);
        }

        for (s, d, label) in self.edge_iter() {
            g.add_edge(s, d, label);
        }
    }
}
