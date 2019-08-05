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
extern crate serde_json;
extern crate tempdir;
extern crate tikv_client;

use rust_graph::property::rocks_property::*;
use rust_graph::property::tikv_property::*;
use rust_graph::property::PropertyGraph;
use serde_json::{json, to_vec};
use std::time::Instant;
use tikv_client::Config;

const NODE_PD_SERVER_ADDR: &str = "192.168.2.2:2379";
const EDGE_PD_SERVER_ADDR: &str = "192.168.2.7:2379";

fn main() {
    println!("Testing tikv...");
    test_insert_raw_node();
    println!("test_insert_raw_node success...");
    test_insert_raw_edge();
    println!("test_insert_raw_edge success...");
    test_insert_property_node();
    println!("test_insert_property_node success...");
    test_insert_property_edge();
    println!("test_insert_property_edge success...");
    test_extend_raw_node();
    println!("test_extend_raw_node success...");
    test_extend_raw_edge();
    println!("test_extend_raw_edge success...");
    test_extend_property_node();
    println!("test_extend_property_node success...");
    test_extend_property_edge();
    println!("test_extend_property_edge success...");
    test_open_existing_db();
    println!("test_open_existing_db success...");
    test_open_readonly_db();
    println!("test_open_readonly_db success...");
    test_open_writable_db();
    println!("test_open_writable_db success...");
    test_scan_node_property();
    println!("test_scan_node_property success...");
    test_scan_edge_property();
    println!("test_scan_edge_property success...");

    println!("Testing tikv time ...");

    println!("Test tikv_insert_raw_node time...");
    time_tikv_insert_raw_node();
    println!("Test tikv_insert_raw_edge time...");
    time_tikv_insert_raw_edge();
    println!("Test tikv_extend_raw_node time...");
    time_tikv_extend_raw_node();
    println!("Test tikv_extend_edge_node time...");
    time_tikv_extend_raw_edge();
    println!("Test tikv_get_node_property_all time...");
    time_tikv_get_node_property_all();
    println!("Test tikv_get_edge_property_all time...");
    time_tikv_get_edge_property_all();

    println!("Testing rocksdb time ...");

    println!("Test rocksdb_insert_raw_node time...");
    time_rocksdb_insert_raw_node();
    println!("Test rocksdb_insert_raw_edge time...");
    time_rocksdb_insert_raw_edge();
    println!("Test rocksdb_extend_raw_node time...");
    time_rocksdb_extend_raw_node();
    println!("Test rocksdb_extend_edge_edge time...");
    time_rocksdb_extend_raw_edge();
    println!("Test rocksdb_get_node_property_all time...");
    time_rocksdb_get_node_property_all();
    println!("Test rocksdb_get_edge_property_all time...");
    time_rocksdb_get_edge_property_all();
}

fn time_tikv_insert_raw_node() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let mut raw_props = Vec::new();
    for i in 0..100u32 {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.push(raw_prop);
    }

    let start = Instant::now();
    for i in 0..100 {
        graph.insert_node_raw(i, raw_props[i].clone()).unwrap();
    }
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished 100 tikv node property insertion in {} seconds, and it takes {}s per insertion.",
        total_time,
        total_time / 100f64
    );
}

fn time_tikv_insert_raw_edge() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let mut raw_props = Vec::new();
    for i in 0..100u32 {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.push(raw_prop);
    }

    let start = Instant::now();
    for i in 0..100 {
        graph
            .insert_edge_raw(i, i + 1, raw_props[i].clone())
            .unwrap();
    }
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished 100 tikv edge property insertion in {} seconds, and it takes {}s per insertion.",
        total_time,
        total_time / 100f64
    );
}

fn time_tikv_extend_raw_node() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let mut raw_props = Vec::new();
    for i in 0..100u32 {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.extend(vec![(i, raw_prop)]);
    }

    let start = Instant::now();
    graph.extend_node_raw(raw_props.into_iter()).unwrap();
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished 100 tikv node property extension in {} seconds, and it takes {}s per extension.",
        total_time,
        total_time / 100f64
    );
}

fn time_tikv_extend_raw_edge() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let mut raw_props = Vec::new();
    for i in 0..100u32 {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.extend(vec![((i, i + 1), raw_prop)]);
    }

    let start = Instant::now();
    graph.extend_edge_raw(raw_props.into_iter()).unwrap();
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished 100 tikv edge property extension in {} seconds, and it takes {}s per extension.",
        total_time,
        total_time / 100f64
    );
}

fn time_tikv_get_node_property_all() {
    {
        let mut graph0 = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        graph0
            .insert_node_property(0u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    let graph1 = TikvProperty::open(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
        true,
    )
    .unwrap();

    let start = Instant::now();
    graph1.get_node_property_all(0u32).unwrap();

    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished tikv_get_node_property_all() in {} seconds",
        total_time
    );
}

fn time_tikv_get_edge_property_all() {
    {
        let mut graph0 = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        graph0
            .insert_edge_property(0u32, 1u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_edge_property_all(0u32, 1u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    let graph1 = TikvProperty::open(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
        true,
    )
    .unwrap();

    let start = Instant::now();
    graph1.get_edge_property_all(0u32, 1u32).unwrap();
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished tikv_get_edge_property_all() in {} seconds",
        total_time
    );
}

fn time_rocksdb_insert_raw_node() {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();

    let node_path = node.path();
    let edge_path = edge.path();

    let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

    let mut raw_props = Vec::new();
    for i in 0..100u32 {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.push(raw_prop);
    }

    let start = Instant::now();
    for i in 0..100 {
        graph.insert_node_raw(i, raw_props[i].clone()).unwrap();
    }
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished 100 rocksdb node property insertion in {} seconds, and it takes {}s per insertion.",
        total_time,
        total_time / 100f64
    );
}

fn time_rocksdb_insert_raw_edge() {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();

    let node_path = node.path();
    let edge_path = edge.path();

    let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

    let mut raw_props = Vec::new();
    for i in 0..100u32 {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.push(raw_prop);
    }

    let start = Instant::now();
    for i in 0..100 {
        graph
            .insert_edge_raw(i, i + 1, raw_props[i].clone())
            .unwrap();
    }
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished 100 rocksdb edge property insertion in {} seconds, and it takes {}s per insertion.",
        total_time,
        total_time / 100f64
    );
}

fn time_rocksdb_extend_raw_node() {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();

    let node_path = node.path();
    let edge_path = edge.path();

    let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

    let mut raw_props = Vec::new();
    for i in 0..100u32 {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.extend(vec![(i, raw_prop)]);
    }

    let start = Instant::now();
    graph.extend_node_raw(raw_props.into_iter()).unwrap();
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished 100 rocksdb node property extension in {} seconds, and it takes {}s per extension.",
        total_time,
        total_time / 100f64
    );
}

fn time_rocksdb_extend_raw_edge() {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();

    let node_path = node.path();
    let edge_path = edge.path();

    let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

    let mut raw_props = Vec::new();
    for i in 0..100u32 {
        let key = i.to_string();
        let value = (i + 1).to_string();
        let new_prop = json!({ key: value });
        let raw_prop = to_vec(&new_prop).unwrap();
        raw_props.extend(vec![((i, i + 1), raw_prop)]);
    }

    let start = Instant::now();
    graph.extend_edge_raw(raw_props.into_iter()).unwrap();
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished 100 rocksdb edge property extension in {} seconds, and it takes {}s per extension.",
        total_time,
        total_time / 100f64
    );
}

fn time_rocksdb_get_node_property_all() {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();

    let node_path = node.path();
    let edge_path = edge.path();

    {
        let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();
        graph0
            .insert_node_property(0u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    let graph1 = RocksProperty::open(node_path, edge_path, false, true).unwrap();

    let start = Instant::now();
    graph1.get_node_property_all(0u32).unwrap();
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished rocksdb_get_node_property_all() in {} seconds.",
        total_time
    );
}

fn time_rocksdb_get_edge_property_all() {
    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();

    let node_path = node.path();
    let edge_path = edge.path();
    {
        let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();

        graph0
            .insert_edge_property(0u32, 1u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_edge_property_all(0u32, 1u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    let graph1 = RocksProperty::open(node_path, edge_path, false, true).unwrap();

    let start = Instant::now();
    graph1.get_edge_property_all(0u32, 1u32).unwrap();
    let duration = start.elapsed();
    let total_time = duration.as_secs() as f64 + duration.subsec_nanos() as f64 * 1e-9;

    println!(
        "Finished rocksdb_get_edge_property_all() in {} seconds.",
        total_time
    );
}

fn test_insert_raw_node() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let new_prop = json!({"name":"jack"});
    let raw_prop = to_vec(&new_prop).unwrap();

    graph.insert_node_raw(0u32, raw_prop).unwrap();
    let node_property = graph.get_node_property_all(0u32).unwrap();

    assert_eq!(Some(json!({"name":"jack"})), node_property);
}

fn test_insert_raw_edge() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let new_prop = json!({"length":"15"});
    let raw_prop = to_vec(&new_prop).unwrap();

    graph.insert_edge_raw(0u32, 1u32, raw_prop).unwrap();
    let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

    assert_eq!(Some(json!({"length":"15"})), edge_property);
}

fn test_insert_property_node() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let new_prop = json!({"name":"jack"});

    graph.insert_node_property(0u32, new_prop).unwrap();
    let node_property = graph.get_node_property_all(0u32).unwrap();

    assert_eq!(Some(json!({"name":"jack"})), node_property);
}

fn test_insert_property_edge() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let new_prop = json!({"length":"15"});

    graph.insert_edge_property(0u32, 1u32, new_prop).unwrap();
    let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

    assert_eq!(Some(json!({"length":"15"})), edge_property);
}

fn test_extend_raw_node() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let new_prop = json!({"name":"jack"});
    let raw_prop = to_vec(&new_prop).unwrap();
    let raw_properties = vec![(0u32, raw_prop)].into_iter();
    graph.extend_node_raw(raw_properties).unwrap();

    let node_property = graph.get_node_property_all(0u32).unwrap();

    assert_eq!(Some(json!({"name":"jack"})), node_property);
}

fn test_extend_raw_edge() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let new_prop = json!({"length":"15"});
    let raw_prop = to_vec(&new_prop).unwrap();
    let raw_properties = vec![((0u32, 1u32), raw_prop)].into_iter();
    graph.extend_edge_raw(raw_properties).unwrap();
    let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

    assert_eq!(Some(json!({"length":"15"})), edge_property);
}

fn test_extend_property_node() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let new_prop = json!({"name":"jack"});

    let properties = vec![(0u32, new_prop)].into_iter();
    graph.extend_node_property(properties).unwrap();

    let node_property = graph.get_node_property_all(0u32).unwrap();

    assert_eq!(Some(json!({"name":"jack"})), node_property);
}

fn test_extend_property_edge() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    let new_prop = json!({"length":"15"});

    let properties = vec![((0u32, 1u32), new_prop)].into_iter();
    graph.extend_edge_property(properties).unwrap();
    let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

    assert_eq!(Some(json!({"length":"15"})), edge_property);
}

fn test_open_existing_db() {
    {
        let mut graph0 = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        graph0
            .insert_node_property(0u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    let graph1 = TikvProperty::open(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
        true,
    )
    .unwrap();

    assert_eq!(
        graph1.get_node_property_all(0u32).unwrap(),
        Some(json!({"name": "jack"}))
    );
}

fn test_open_writable_db() {
    {
        let mut graph0 = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        graph0
            .insert_node_property(0u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }
    let mut graph1 = TikvProperty::open(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
        false,
    )
    .unwrap();
    graph1
        .insert_node_property(1u32, json!({"name": "tom"}))
        .unwrap();
    assert_eq!(
        graph1.get_node_property_all(1u32).unwrap(),
        Some(json!({"name": "tom"}))
    );
}

fn test_open_readonly_db() {
    {
        let mut graph0 = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        graph0
            .insert_node_property(0u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    let mut graph1 = TikvProperty::open(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
        true,
    )
    .unwrap();
    assert_eq!(
        graph1.get_node_property_all(0u32).unwrap(),
        Some(json!({"name": "jack"}))
    );

    let err = graph1
        .insert_node_property(1u32, json!({"name": "tom"}))
        .is_err();
    assert_eq!(err, true);
}

fn test_scan_node_property() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    graph
        .insert_node_property(0u32, json!({"name": "jack"}))
        .unwrap();

    graph
        .insert_node_property(1u32, json!({"name": "tom"}))
        .unwrap();

    let mut iter = graph.scan_node_property_all();
    assert_eq!(
        (0u32, json!({"name": "jack"})),
        iter.next().unwrap().unwrap()
    );
    assert_eq!(
        (1u32, json!({"name": "tom"})),
        iter.next().unwrap().unwrap()
    );
}

fn test_scan_edge_property() {
    let mut graph = TikvProperty::new(
        Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
        Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
        false,
    )
    .unwrap();

    graph
        .insert_edge_property(0u32, 1u32, json!({"length": "5"}))
        .unwrap();

    graph
        .insert_edge_property(1u32, 2u32, json!({"length": "10"}))
        .unwrap();

    let mut iter = graph.scan_edge_property_all();
    assert_eq!(
        ((0u32, 1u32), json!({"length": "5"})),
        iter.next().unwrap().unwrap()
    );
    assert_eq!(
        ((1u32, 2u32), json!({"length": "10"})),
        iter.next().unwrap().unwrap()
    );
}
