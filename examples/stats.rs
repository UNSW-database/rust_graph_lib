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
extern crate time;

use time::PreciseTime;

use rust_graph::io::serde::{Deserialize, Deserializer};
use rust_graph::prelude::*;
use rust_graph::{UnGraphMap, UnStaticGraph};

fn main() {
    let args: Vec<_> = std::env::args().skip(1).collect();

    let start = PreciseTime::now();

    for arg in args {
        println!("------------------------------");
        println!("Loading {}", &arg);

        let g: UnStaticGraph<DefaultId> = Deserializer::import(arg).unwrap();

        let max_degree = g.node_indices().map(|i| g.degree(i)).max().unwrap();

        println!("Max degree: {}", max_degree);

        let node_labels_counter = g.get_node_label_counter();
        let edge_labels_counter = g.get_edge_label_counter();

        println!("Node labels:");

        for (label, count) in node_labels_counter.most_common() {
            println!("- {} : {}", label, count);
        }

        println!();
        println!("Edge labels:");

        for (label, count) in edge_labels_counter.most_common() {
            println!("- {} : {}", label, count);
        }

        println!("------------------------------");
    }

    let end = PreciseTime::now();

    println!("Finished in {} seconds.", start.to(end));
}
