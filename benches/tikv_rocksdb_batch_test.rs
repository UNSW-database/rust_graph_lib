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
use serde_json::{json, to_vec};

use rust_graph::io::rocksdb::rocksdb_loader::RocksDBLoader;
use rust_graph::io::tikv::tikv_loader::TikvLoader;
use rust_graph::io::{CSVReader, GraphLoader};
use rust_graph::property::tikv_property::TikvProperty;
use rust_graph::property::{PropertyGraph, RocksProperty};
use std::str::FromStr;
use std::time::Instant;
use test::Bencher;
use tikv_client::Config;

#[bench]
fn bench_tikv_batch_get_node_property_all(b: &mut Bencher) {
    let batch_size = 500;
    let mut graph = TikvProperty::new(
        Config::new(vec![
            "192.168.2.3::2379",
            "192.168.2.4:2379",
            "192.168.2.5:2379",
        ]),
        false,
    )
    .unwrap();

    let mut raw_props = Vec::new();
    for i in 0..batch_size {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.extend(vec![(i, raw_prop)]);
    }
    graph.extend_node_raw(raw_props.into_iter()).unwrap();

    let keys = (0..batch_size).collect();

    b.iter(|| {
        let pairs = graph.batch_get_node_property_all(keys).unwrap();
    });

    assert_eq!(pairs.unwrap().len(), batch_size);
    let mut i = 0u32;
    while i < batch_size {
        let node_property = graph.get_node_property_all(i).unwrap();
        assert_eq!(
            Some(json!({i.to_string(): (i + 1).to_string()})),
            node_property
        );
        i += 1;
    }
}

#[bench]
fn bench_tikv_batch_get_edge_property_all(b: &mut Bencher) {
    let batch_size = 500;
    let mut graph = TikvProperty::new(
        Config::new(vec![
            "192.168.2.3::2379",
            "192.168.2.4:2379",
            "192.168.2.5:2379",
        ]),
        false,
    )
    .unwrap();

    let mut raw_props = Vec::new();
    for i in 0..batch_size {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.extend(vec![((i, i + 1), raw_prop)]);
    }
    graph.extend_edge_raw(raw_props.into_iter()).unwrap();

    let keys = (0..batch_size).map(|x| (x, x + 1)).collect();

    b.iter(|| {
        let pairs = graph.batch_get_edge_property_all(keys).unwrap();
    });

    assert_eq!(pairs.unwrap().len(), batch_size);
    let mut i = 0u32;
    while i < batch_size {
        let edge_property = graph.get_edge_property_all(i, i + 1).unwrap();
        assert_eq!(
            Some(json!({i.to_string(): (i + 1).to_string()})),
            edge_property
        );
        i += 1;
    }
}

#[bench]
fn bench_rocksdb_get_node_property_all(b: &mut Bencher) {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();
    let batch_size = 500;

    let node_path = node.path();
    let edge_path = edge.path();
    {
        let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();
        for i in 0..batch_size {
            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();
        }
    }
    let graph1 = RocksProperty::open(node_path, edge_path, false, true).unwrap();

    b.iter(|| {
        for i in 0..batch_size {
            graph1.get_node_property_all(0u32).unwrap();
        }
    });
}

#[bench]
fn bench_rocksdb_get_edge_property_all(b: &mut Bencher) {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();
    let batch_size = 500;

    let node_path = node.path();
    let edge_path = edge.path();
    {
        let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();
        for i in 0..batch_size {
            graph0
                .insert_edge_property(i, 1u32, json!({"name": "jack"}))
                .unwrap();
        }
    }
    let graph1 = RocksProperty::open(node_path, edge_path, false, true).unwrap();
    b.iter(|| {
        for i in 0..batch_size {
            graph1.get_edge_property_all(0u32, 1u32).unwrap();
        }
    });
}

fn load_graph_to_tikv(nodes: &str, edges: &str, thread_cnt: usize, batch_size: usize) {
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
    println!("Finished tikv graph insertion in {} seconds.", total_time);
}

fn load_graph_to_rocksdb(nodes: &str, edges: &str, batch_size: usize) {
    let node = tempdir::TempDir::new_in(".", "node").unwrap();
    let edge = tempdir::TempDir::new_in(".", "edge").unwrap();

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
        "Finished rocksdb graph insertion in {} seconds.",
        total_time
    );
}
