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

use std::path::Path;

use time::PreciseTime;

use rust_graph::io::serde::{Serialize, Serializer};
use rust_graph::io::*;
use rust_graph::prelude::*;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let ldbc_dir = Path::new(&args[1]);
    let output_dir = Path::new(&args[2]);

    let start = PreciseTime::now();

    println!("Loading {:?}", &ldbc_dir);
    let g = read_ldbc_from_path::<DefaultId, Undirected, _>(ldbc_dir);
    let num_of_nodes = g.node_count();
    let num_of_edges = g.edge_count();

    println!("{} nodes, {} edges.", num_of_nodes, num_of_edges);

    println!("Node labels: {:?}", g.get_node_label_map());
    println!("Edge labels: {:?}", g.get_edge_label_map());

    let dir_name = ldbc_dir
        .components()
        .last()
        .unwrap()
        .as_os_str()
        .to_str()
        .unwrap();
    let export_filename = format!("{}_{}_{}.graphmap", dir_name, num_of_nodes, num_of_edges);
    let export_path = output_dir.join(export_filename);

    println!("Exporting to {:?}...", export_path);

    Serializer::export(&g, export_path).unwrap();

    let end = PreciseTime::now();

    println!("Finished in {} seconds.", start.to(end));
}
