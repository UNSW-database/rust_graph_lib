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
extern crate clap;
extern crate rust_graph;

use std::path::PathBuf;
use std::time::Instant;

use clap::{App, Arg};

use rust_graph::io::read_from_csv;
use rust_graph::io::serde::Serialize;
use rust_graph::{DiGraphMap, UnGraphMap};

fn main() {
    let matches = App::new("CSV to StaticGraph Converter")
        .arg(
            Arg::with_name("node_file")
                .short("n")
                .long("node")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("edge_file")
                .short("e")
                .long("edge")
                .required(true)
                .takes_value(true),
        )
        .arg(
            Arg::with_name("out_file")
                .short("o")
                .long("out")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("separator")
                .short("s")
                .long("separator")
                .long_help("allowed separator: [comma|space|tab]")
                .takes_value(true),
        )
        .arg(
            Arg::with_name("has_headers")
                .short("h")
                .long("headers")
                .multiple(true),
        )
        .arg(
            Arg::with_name("is_flexible")
                .short("f")
                .long("flexible")
                .multiple(true),
        )
        .arg(
            Arg::with_name("is_directed")
                .short("d")
                .long("directed")
                .multiple(true),
        )
        .arg(
            Arg::with_name("reorder_node_id")
                .short("i")
                .long("reorder_nodes")
                .multiple(true),
        )
        .arg(
            Arg::with_name("reorded_label_id")
                .short("l")
                .long("reorder_labels")
                .multiple(true),
        )
        .get_matches();

    let node_file = matches.value_of("node_file").map(PathBuf::from);
    let edge_file = PathBuf::from(matches.value_of("edge_file").unwrap());

    let node_path = match node_file {
        Some(p) => {
            if p.is_dir() {
                let mut vec = ::std::fs::read_dir(p)
                    .unwrap()
                    .map(|x| x.unwrap().path())
                    .collect::<Vec<_>>();
                vec.sort();

                vec
            } else {
                vec![p]
            }
        }
        None => Vec::new(),
    };

    let edge_path = if edge_file.is_dir() {
        let mut vec = ::std::fs::read_dir(edge_file)
            .unwrap()
            .map(|x| x.unwrap().path())
            .collect::<Vec<_>>();
        vec.sort();

        vec
    } else {
        vec![edge_file]
    };

    let out_file = PathBuf::from(matches.value_of("out_file").unwrap_or("graph.static"));
    let separator = matches.value_of("separator");
    let is_directed = matches.is_present("is_directed");
    let has_headers = matches.is_present("has_headers");
    let is_flexible = matches.is_present("is_flexible");
    let reorder_node_id = matches.is_present("reorder_node_id");
    let reorder_label_id = matches.is_present("reorder_label_id");

    let start = Instant::now();

    if is_directed {
        let mut g = DiGraphMap::<String, String>::new();
        println!("Reading graph");
        read_from_csv(
            &mut g,
            node_path,
            edge_path,
            separator,
            has_headers,
            is_flexible,
        );

        println!("Converting graph");
        let static_graph = g
            .reorder_id(reorder_node_id, reorder_label_id, reorder_label_id)
            .take_graph()
            .unwrap()
            .into_static();

        static_graph.export(out_file).unwrap()
    } else {
        let mut g = UnGraphMap::<String, String>::new();
        println!("Reading graph");
        read_from_csv(
            &mut g,
            node_path,
            edge_path,
            separator,
            has_headers,
            is_flexible,
        );

        println!("Converting graph");
        let static_graph = g
            .reorder_id(reorder_node_id, reorder_label_id, reorder_label_id)
            .take_graph()
            .unwrap()
            .into_static();

        static_graph.export(out_file).unwrap()
    }

    let duration = start.elapsed();
    println!(
        "Finished in {} seconds.",
        duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9
    );
}
