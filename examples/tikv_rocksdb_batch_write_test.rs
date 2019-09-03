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
#![feature(async_await)]
extern crate rust_graph;
extern crate tempdir;
extern crate tikv_client;

use rust_graph::io::rocksdb::rocksdb_loader::RocksDBLoader;
use rust_graph::io::tikv::tikv_loader::TikvLoader;
use rust_graph::io::{CSVReader, GraphLoader};
use std::str::FromStr;
use std::time::Instant;
use tikv_client::Config;

fn main() {
    let args: Vec<_> = std::env::args().collect();

    if args.len() < 3 {
        println!("tikv_rocksdb_batch_write_test [node_path] [edge_path] [batch_size]");
        return;
    }
    let node_path = &args[1];
    let edge_path = &args[2];
    let thread_cnt = usize::from_str(&args[3]).expect("Thread_cnt format error.");
    let batch_size = usize::from_str(&args[4]).expect("Batch_size format error.");

    println!("Testing tikv time ...");

    println!("Test time_tikv_batch_insert time...");
    time_tikv_batch_insert(node_path, edge_path, thread_cnt, batch_size);

    println!("\nTesting rocksdb time ...");

    println!("Test time_rocksdb_batch_insert time...");
    time_rocksdb_batch_insert(node_path, edge_path, batch_size);
}

fn time_tikv_batch_insert(nodes: &str, edges: &str, thread_cnt: usize, batch_size: usize) {
    let node_pd_server_addr: Vec<&str> =
        vec!["192.168.2.3:2379", "192.168.2.4:2379", "192.168.2.5:2379"];
    let edge_pd_server_addr: Vec<&str> =
        vec!["192.168.2.3:2379", "192.168.2.4:2379", "192.168.2.5:2379"];

    let reader = CSVReader::<u32, String, String>::new(vec![nodes], vec![edges])
        .headers(true)
        .flexible(true)
        .with_separator("bar");

    let tike_loader = TikvLoader::new(
        Config::new(node_pd_server_addr.to_owned()),
        Config::new(edge_pd_server_addr.to_owned()),
        false,
    );

    let start = Instant::now();
    tike_loader.load(&reader, thread_cnt, batch_size);
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;
    println!(
        "Finished tikv graph insertion in {} seconds, and it takes {}ms per insertion.",
        total_time,
        total_time * 10f64
    );
}

fn time_rocksdb_batch_insert(nodes: &str, edges: &str, batch_size: usize) {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();

    let node_path = node.path();
    let edge_path = edge.path();
    let rocks_db_loader = RocksDBLoader::new(node_path, edge_path, false);

    let reader = CSVReader::<u32, String, String>::new(vec![nodes], vec![edges])
        .headers(true)
        .flexible(true)
        .with_separator("bar");

    let start = Instant::now();
    rocks_db_loader.load(&reader, 1, batch_size);
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;
    println!(
        "Finished rocksdb graph insertion in {} seconds, and it takes {}ms per insertion.",
        total_time,
        total_time * 10f64
    );
}
