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

use std::fs::create_dir_all;
use std::path::Path;
use std::time::Instant;

use rust_graph::io::serde::Deserialize;
use rust_graph::io::write_to_csv;
use rust_graph::prelude::*;
use rust_graph::UnStaticGraph;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let in_file = Path::new(&args[1]);
    let out_dir = Path::new(&args[2]);

    let start = Instant::now();

    println!("Loading {:?}", &in_file);
    let g = UnStaticGraph::<DefaultId>::import(in_file).expect("Deserializer error");

    println!("{:?}", g.get_node_label_map());
    println!("{:?}", g.get_edge_label_map());

    if !out_dir.exists() {
        create_dir_all(out_dir).unwrap();
    }

    println!("Exporting to {:?}...", &out_dir);

    write_to_csv(&g, out_dir.join("nodes.csv"), out_dir.join("edges.csv")).unwrap();

    let duration = start.elapsed();
    println!(
        "Finished in {} seconds.",
        duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9
    );
}
