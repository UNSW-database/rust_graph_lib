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
extern crate rust_graph;
#[macro_use]
extern crate json;

use std::collections::HashMap;

use rust_graph::graph_impl::UnGraphMap;
use rust_graph::io::serde::{Deserialize, Serialize};
use rust_graph::prelude::*;
use rust_graph::property::NaiveProperty;

fn main() {
    let g = UnGraphMap::<Void>::new();

    /// `cargo run` -> The default ID type can hold 4294967295 nodes at maximum.
    /// `cargo run --features=usize_id` -> The default ID type can hold 18446744073709551615 nodes at maximum.
    println!(
        "The graph can hold {} nodes and {} labels at maximum.",
        g.max_possible_id(),
        g.max_possible_label_id()
    );

    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();

    node_property.insert(
        0u32,
        object!(
            "name"=>"John",
            "age"=>12,
            "is_member"=>true,
            "scores"=>array![9,8,10],
            ),
    );

    node_property.insert(
        1,
        object!(
            "name"=>"Marry",
            "age"=>13,
            "is_member"=>false,
            "scores"=>array![10,10,9],
            ),
    );

    edge_property.insert(
        (0, 1),
        object!(
            "friend_since"=>"2018-11-15",
            ),
    );

    let graph = NaiveProperty::with_data(node_property, edge_property, false);

    println!("{:#?}", &graph);

    graph.export("NaivePropertyGraph.bin").unwrap();

    let graph1 = NaiveProperty::import("NaivePropertyGraph.bin").unwrap();

    assert_eq!(graph, graph1);
}
