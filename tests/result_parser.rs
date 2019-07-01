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
//extern crate sled;

use std::collections::HashMap;
use std::path::Path;
use std::sync::Arc;

use rust_graph::property::filter::*;
use rust_graph::property::{parse_result_blueprint, ResultBlueprint, NodeElement};
use rust_graph::property::*;

use serde_json::json;

use std::fs::File;
use std::io::prelude::*;
use std::io::BufReader;


#[test]
fn test_cypher_three_attributes() {
    // MATCH (p:Person) WHERE p.age > 10 RETURN p, p.name, p.age, count(p)

    let cypher_tree = lines_from_file("tests/cypher_tree/10.txt");
    let exp = parse_result_blueprint(cypher_tree);
    assert_eq!(exp.get_node_elements().len(), 4);
}

#[test]
fn test_cypher_movie_query() {
    let cypher_tree = lines_from_file("tests/cypher_tree/11.txt");
    let exp = parse_result_blueprint(cypher_tree);
}

fn lines_from_file(filename: impl AsRef<Path>) -> Vec<String> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    buf.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect()
}