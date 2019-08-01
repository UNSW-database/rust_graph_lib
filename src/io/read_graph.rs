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
use generic::{IdType, Iter, MutGraphTrait};
use io::csv::JsonValue;
use serde::Deserialize;
use std::hash::Hash;

pub trait ReadGraph<Id: IdType, NL: Hash + Eq, EL: Hash + Eq>
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

    fn node_iter(&self) -> Iter<(Id, Option<NL>)>;
    fn edge_iter(&self) -> Iter<(Id, Id, Option<EL>)>;
    fn prop_node_iter(&self) -> Iter<(Id, Option<NL>, JsonValue)>;
    fn prop_edge_iter(&self) -> Iter<(Id, Id, Option<EL>, JsonValue)>;
}
