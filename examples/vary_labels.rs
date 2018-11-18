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
extern crate rand;
extern crate rust_graph;

use std::path::Path;

use rand::{thread_rng, Rng};

use rust_graph::graph_impl::UnStaticGraph;
use rust_graph::io::serde::{Deserialize, Serialize};
use rust_graph::prelude::*;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    let in_graph = Path::new(&args[1]);
    let out_file = Path::new(&args[2]);

    let mut rng = thread_rng();

    let mut graph = UnStaticGraph::<DefaultId>::import(in_graph).unwrap();

    graph.remove_edge_labels();

    {
        let node_label_map = graph.get_node_label_map_mut();
        for i in 11..15 {
            node_label_map.add_item(i);
        }
    }

    {
        let labels = graph.get_labels_mut().as_mut().unwrap();
        for label in labels.iter_mut() {
            let r = rng.gen_range(0, 15);
            if r > 10 {
                *label = r;
            }
        }
    }

    graph.export(out_file).unwrap();
}
