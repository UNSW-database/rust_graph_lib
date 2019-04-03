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

use clap::{App, Arg};
use rust_graph::graph_impl::{DiStaticGraph, UnStaticGraph};
use rust_graph::io::serde::Deserialize;

fn main() {
//    let matches = App::new("StaticGraph to MMap")
//        .arg(
//            Arg::with_name("graph")
//                .short("g")
//                .long("graph")
//                .required(true)
//                .takes_value(true),
//        )
//        .arg(Arg::with_name("directed").short("d").long("directed"))
//        .arg(
//            Arg::with_name("output")
//                .short("o")
//                .long("output")
//                .required(true)
//                .takes_value(true),
//        )
//        .get_matches();
//
//    let graph = matches.value_of("graph").unwrap();
//    let output = matches.value_of("output").unwrap();
//    let is_directed = matches.is_present("directed");
//
//    if !is_directed {
//        let graph = UnStaticGraph::<u32>::import(graph).unwrap();
//        graph.dump_mmap(output).expect("Dump graph error");
//    } else {
//        let graph = DiStaticGraph::<u32>::import(graph).unwrap();
//        graph.dump_mmap(output).expect("Dump graph error");
//    }
}
