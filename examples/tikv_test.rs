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
extern crate tikv_client;

use rust_graph::property::tikv_property::*;
use rust_graph::property::PropertyGraph;
use serde_json::{json, to_vec};
use tikv_client::Config;

const NODE_PD_SERVER_ADDR: &str = "192.168.2.2:2379";
const EDGE_PD_SERVER_ADDR: &str = "192.168.2.3:2379";

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
    println!("test_open_writable_dbe success...");
    test_scan_node_property();
    println!("test_scan_node_property success...");
    test_scan_edge_property();
    println!("test_scan_edge_property success...");
    println!("Finish all tests!");
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
    let node_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

    assert_eq!(Some(json!({"length":"15"})), node_property);
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
    let node_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

    assert_eq!(Some(json!({"length":"15"})), node_property);
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
