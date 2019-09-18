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
#![feature(test)]
extern crate rust_graph;
extern crate serde_json;
extern crate test;
extern crate tikv_client;

use std::time::Instant;
use test::Bencher;

use rust_graph::property::tikv_property::*;
use rust_graph::property::PropertyGraph;
use serde_json::{json, to_vec};

use tikv_client::Config;

const PD_SERVER_ADDR: &str = "192.168.2.2:2379";

#[bench]
fn bench_tikv_insert_raw_node(b: &mut Bencher) {
    let mut graph = TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

    let new_prop = json!({"name":"jack"});
    let raw_prop = to_vec(&new_prop).unwrap();

    b.iter(|| graph.insert_node_raw(0u32, raw_prop.clone()));
}

#[bench]
fn bench_tikv_insert_raw_edge(b: &mut Bencher) {
    let mut graph = TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

    let new_prop = json!({"length":"15"});
    let raw_prop = to_vec(&new_prop).unwrap();

    b.iter(|| graph.insert_edge_raw(0u32, 1u32, raw_prop.clone()));
}

#[bench]
fn bench_tikv_extend_raw_node(b: &mut Bencher) {
    let mut graph = TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

    let new_prop = json!({"name":"jack"});
    let raw_prop = to_vec(&new_prop).unwrap();
    let raw_properties = vec![(0u32, raw_prop)].into_iter();
    b.iter(|| graph.extend_node_raw(raw_properties.clone()).unwrap());
}

#[bench]
fn bench_tikv_extend_raw_edge(b: &mut Bencher) {
    let mut graph = TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

    let new_prop = json!({"length":"15"});
    let raw_prop = to_vec(&new_prop).unwrap();
    let raw_properties = vec![((0u32, 1u32), raw_prop)].into_iter();
    b.iter(|| graph.extend_edge_raw(raw_properties.clone()).unwrap());
}

#[bench]
fn bench_tikv_get_node_property_all(b: &mut Bencher) {
    {
        let mut graph0 =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        graph0
            .insert_node_property(0u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    let graph1 =
        TikvProperty::open(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false, true).unwrap();

    b.iter(|| graph1.get_node_property_all(0u32).unwrap());
}

#[bench]
fn bench_tikv_get_edge_property_all(b: &mut Bencher) {
    {
        let mut graph0 =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        graph0
            .insert_edge_property(0u32, 1u32, json!({"name": "jack"}))
            .unwrap();

        assert_eq!(
            graph0.get_edge_property_all(0u32, 1u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    let graph1 =
        TikvProperty::open(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false, true).unwrap();

    b.iter(|| graph1.get_node_property_all(0u32, 1u32).unwrap());
}
