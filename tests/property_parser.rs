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
extern crate json;
extern crate sled;

use std::collections::HashMap;
use std::path::Path;

use rust_graph::property::*;
use rust_graph::property::filter::*;
use rust_graph::property::parse_property;

use json::JsonValue;
use json::number::Number;
use json::{array, object};

use sled::Db;
use std::mem::transmute;
use std::time::Instant;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;

#[test]
fn test_cypher_larger_than() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.age > 10 RETURN a

    let result = lines_from_file("/Users/hao/RustProject/rust_graph_lib/tests/cypher_tree/0.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_sled_property();
    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(exp.as_ref(), &mut node_cache);
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_filter.pre_fetch(&vec, &property_graph);

    let result: Vec<u32> = vec.into_iter().filter(|x| property_filter.filter(*x)).collect();

    assert_eq!(vec![0, 4], result);
}

#[test]
fn test_cypher_number_addition() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.age + 5.5 > 10 RETURN a

    let result = lines_from_file("/Users/hao/RustProject/rust_graph_lib/tests/cypher_tree/1.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_sled_property();
    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(exp.as_ref(), &mut node_cache);
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_filter.pre_fetch(&vec, &property_graph);

    let result: Vec<u32> = vec.into_iter().filter(|x| property_filter.filter(*x)).collect();

    assert_eq!(vec![0, 1, 3, 4], result);
}

#[test]
fn test_cypher_string_contains() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.name CONTAINS "hello" RETURN a

    let result = lines_from_file("/Users/hao/RustProject/rust_graph_lib/tests/cypher_tree/2.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_sled_property();
    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(exp.as_ref(), &mut node_cache);
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_filter.pre_fetch(&vec, &property_graph);

    let result: Vec<u32> = vec.into_iter().filter(|x| property_filter.filter(*x)).collect();

    assert_eq!(vec![0, 2, 3, 4, 5], result);
}

#[test]
fn test_cypher_and_operator() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.name CONTAINS "hello" AND a.age + 5.5 > 10 RETURN a

    let result = lines_from_file("/Users/hao/RustProject/rust_graph_lib/tests/cypher_tree/3.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);

    let property_graph = create_sled_property();
    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(exp.as_ref(), &mut node_cache);
    let vec: Vec<u32> = vec![0, 1, 2, 3, 4, 5];
    property_filter.pre_fetch(&vec, &property_graph);

    let result: Vec<u32> = vec.into_iter().filter(|x| property_filter.filter(*x)).collect();

    assert_eq!(vec![0, 3, 4], result);
}



fn lines_from_file(filename: impl AsRef<Path>) -> Vec<String> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    buf.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect()
}


fn create_sled_property() -> SledProperty {
    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();

    node_property.insert(
        0u32,
        object!(
            "name"=>"Bhello",
            "age"=>15,
            ),
    );

    node_property.insert(
        1,
        object!(
            "name"=>"Jack",
            "age"=>6,
            ),
    );

    node_property.insert(
        2,
        object!(
            "name"=>"Thello",
            "age"=>3,
            ),
    );

    node_property.insert(
        3,
        object!(
            "name"=>"hello",
            "age"=>5,
            ),
    );

    node_property.insert(
        4,
        object!(
            "name"=>"Chello",
            "age"=>13,
            ),
    );

    node_property.insert(
        5,
        object!(
            "name"=>"Shello",
            "age"=>1,
            ),
    );

    edge_property.insert(
        (0u32, 1),
        object!(
            "friend_since"=>"2018-11-15",
            ),
    );

    let path = Path::new("../undirected");
    let db = SledProperty::with_data(path, node_property.into_iter(),
                                     edge_property.into_iter(), false).unwrap();
    db.flush();
    db
}
