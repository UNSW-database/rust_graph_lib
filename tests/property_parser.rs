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
extern crate sled;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use rust_graph::property::filter::*;
use rust_graph::property::parse_property;
use rust_graph::property::parse_property_tree;
use rust_graph::property::*;

use serde_json::json;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

#[test]
fn test_cypher_two_vars() {
    // match (a)-[b]-(c) where a.age > 10 and b.age < 5;
    // match (0)-[3]-(1) where 0.age > 10 and 3.age < 5;

    let result = lines_from_file("tests/cypher_tree/4.txt");

    let (node_property, edge_property) = parse_property_tree(result.clone());
    println!("{:?}", node_property.keys());
    println!("{:?}", edge_property.keys());

    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_cache.pre_fetch(vec.clone().into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp["0"].box_clone()))
        .collect();

    assert_eq!(vec![0, 4], result);
}

#[test]
fn test_cypher_two_vars2() {
    // match (a)-[b]-(c) where a.age > 10 and b.age + 5 < a.age;

    let result = lines_from_file("tests/cypher_tree/5.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_cached_property();
    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_cache.pre_fetch(vec.clone().into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp["a"].box_clone()))
        .collect();

    assert_eq!(vec![0, 4], result);
}

#[test]
fn test_cypher_two_vars3() {
    // match (a)-[b]-(c) where a.age + b.age > 10 or b.age + 5 < a.age;

    let result = lines_from_file("tests/cypher_tree/6.txt");

    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_cached_property();
    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_cache.pre_fetch(vec.clone().into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp["a"].box_clone()))
        .collect();

    assert_eq!(vec![0, 1, 2, 3, 4, 5], result);
}

#[test]
fn test_cypher_two_vars4() {
    // match (a)-[b]-(c) ;

    let result = lines_from_file("tests/cypher_tree/7.txt");

    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_cached_property();
    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_cache.pre_fetch(vec.clone().into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp["a"].box_clone()))
        .collect();

    assert_eq!(vec![0, 1, 2, 3, 4, 5], result);
}
#[test]
fn test_cypher_larger_than() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.age > 10 RETURN a

    let result = lines_from_file("tests/cypher_tree/0.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_cached_property();
    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_cache.pre_fetch(vec.clone().into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp["a"].box_clone()))
        .collect();

    assert_eq!(vec![0, 4], result);
}

#[test]
fn test_cypher_number_addition() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.age + 5.5 > 10 RETURN a

    let result = lines_from_file("tests/cypher_tree/1.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_cached_property();
    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_cache.pre_fetch(vec.clone().into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp["a"].box_clone()))
        .collect();

    assert_eq!(vec![0, 1, 3, 4], result);
}

#[test]
fn test_cypher_string_contains() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.name CONTAINS "hello" RETURN a

    let result = lines_from_file("tests/cypher_tree/2.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_cached_property();
    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_cache.pre_fetch(vec.clone().into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp["a"].box_clone()))
        .collect();

    assert_eq!(vec![0, 2, 3, 4, 5], result);
}

#[test]
fn test_cypher_and_operator() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.name CONTAINS "hello" AND a.age + 5.5 > 10 RETURN a

    let result = lines_from_file("tests/cypher_tree/3.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_cached_property();
    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_cache.pre_fetch(vec.clone().into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp["a"].box_clone()))
        .collect();

    assert_eq!(vec![0, 3, 4], result);
}

#[test]
fn test_compelx_cypher_query() {
    // WHERE a.is_member AND ((a.age % 5 = 0) AND (18 <= a.age <= 35 AND((a.name CONTAINS "a") OR (a.name CONTAINS "o"))))

    let result = lines_from_file("tests/cypher_tree/9.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let _exp = parse_property(cypher_tree);
}

fn lines_from_file(filename: impl AsRef<Path>) -> Vec<String> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    buf.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect()
}

fn create_cached_property() -> CachedProperty<u32> {
    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();

    node_property.insert(
        0u32,
        json!({
        "name":"Bhello",
        "age":15,
        }),
    );

    node_property.insert(
        1,
        json!({
        "name":"Jack",
        "age":6,
        }),
    );

    node_property.insert(
        2,
        json!({
        "name":"Thello",
        "age":3,
        }),
    );

    node_property.insert(
        3,
        json!({
        "name":"hello",
        "age":5,
        }),
    );

    node_property.insert(
        4,
        json!({
        "name":"Chello",
        "age":13,
        }),
    );

    node_property.insert(
        5,
        json!({
        "name":"Shello",
        "age":1,
        }),
    );

    edge_property.insert(
        (0u32, 1),
        json!({
        "friend_since":"2018-11-15",
        }),
    );

    CachedProperty::with_data(node_property, edge_property, false)
}
