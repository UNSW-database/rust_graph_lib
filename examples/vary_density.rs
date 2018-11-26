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
extern crate pbr;
extern crate rand;
extern crate rust_graph;

use std::path::Path;
use std::time::Instant;

use pbr::ProgressBar;
use rand::{thread_rng, Rng};

use rust_graph::graph_impl::UnGraphMap;
use rust_graph::io::serde::{Deserialize, Serialize};
use rust_graph::prelude::*;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let in_graph = Path::new(&args[1]);
    let out_dir = Path::new(&args[2]);

    let average_degrees: Vec<usize> = args.iter().skip(3).map(|n| n.parse().unwrap()).collect();

    println!("average_degrees:{:?}", average_degrees);

    let start = Instant::now();

    let mut rng = thread_rng();

    println!("Loading {:?}", &in_graph);
    let mut g = UnGraphMap::<String>::import(in_graph).unwrap();

    let num_of_nodes = g.node_count();
    let num_of_edges = g.edge_count();

    println!("Average degree: {}", 2 * num_of_edges / num_of_nodes);
    assert_eq!(g.max_seen_id().unwrap().id(), num_of_nodes - 1);

    for d in average_degrees {
        println!("Targeting average degree {}: ", d);

        let target_num_of_edges = d * num_of_nodes / 2;

        assert!(target_num_of_edges > num_of_edges);

        let i = target_num_of_edges - num_of_edges;
        let nodes = DefaultId::new(num_of_nodes);

        let mut pb = ProgressBar::new(i as u64);

        for _ in 0..i {
            pb.inc();
            loop {
                let s = rng.gen_range(0, nodes);
                let t = rng.gen_range(0, nodes);
                if s != t && !g.has_edge(s, t) {
                    g.add_edge(s, t, None);
                    break;
                }
            }
        }

        let file_name = in_graph
            .components()
            .last()
            .unwrap()
            .as_os_str()
            .to_str()
            .unwrap();
        let export_filename = format!(
            "{}_{}_{}_{}.graphmap",
            file_name,
            g.node_count(),
            g.edge_count(),
            d
        );
        let export_path = out_dir.join(export_filename);

        pb.finish_print("done");

        println!("Exporting to {:?}...", export_path);

        &g.export(export_path).unwrap();
    }

    let duration = start.elapsed();
    println!(
        "Finished in {} seconds.",
        duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9
    );
}
