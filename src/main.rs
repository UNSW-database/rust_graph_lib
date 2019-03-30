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
extern crate sled;
extern crate json;
extern crate rust_graph;

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


fn main() {
    let result = lines_from_file("/Users/mengmeng/RustProject/rust_graph_lib/tests/cypher_tree/8.txt");
    let cypher_tree: Vec<&str> = result.iter().map(AsRef::as_ref).collect();
    let exp = parse_property(cypher_tree);
}

fn lines_from_file(filename: impl AsRef<Path>) -> Vec<String> {
    let file = File::open(filename).expect("no such file");
    let buf = BufReader::new(file);
    buf.lines()
        .map(|l| l.expect("Could not parse line"))
        .collect()
}


fn sled_num_compare_expression() {
    // Match (a:A)-[b:B]-(c:C) WHERE a.name CONTAINS "hello" AND a.age + 5.5 > 10 RETURN a

    let exp0 = Var::new("is_member".to_owned());
    let exp1 = Var::new("age".to_owned());
    let exp2 = Const::new(JsonValue::Number(Number::from(5)));
    let exp3 = Const::new(JsonValue::Number(Number::from(0)));
    let exp4 = Const::new(array![18, 35]);
    let exp5 = Var::new("age".to_owned());
    let exp6 = Var::new("name".to_owned());
    let exp7 = Const::new(JsonValue::String("a".to_owned()));
    let exp8 = Var::new("name".to_owned());
    let exp9 = Const::new(JsonValue::String("o".to_owned()));
    let exp12 = ArithmeticExpression::new(Box::new(exp1), Box::new(exp2), ArithmeticOperator::Modulo);
    let exp123 = PredicateExpression::new(Box::new(exp12), Box::new(exp3), PredicateOperator::Equal);
    let exp45 = PredicateExpression::new(Box::new(exp4), Box::new(exp5), PredicateOperator::Range);
    let exp67 = PredicateExpression::new(Box::new(exp6), Box::new(exp7), PredicateOperator::Contains);
    let exp89 = PredicateExpression::new(Box::new(exp8), Box::new(exp9), PredicateOperator::Contains);
    let exp6789 = PredicateExpression::new(Box::new(exp67), Box::new(exp89), PredicateOperator::OR);
    let exp456789 = PredicateExpression::new(Box::new(exp45), Box::new(exp6789), PredicateOperator::AND);
    let exp123456789 = PredicateExpression::new(Box::new(exp123), Box::new(exp456789), PredicateOperator::AND);
    let final_exp = PredicateExpression::new(Box::new(exp0), Box::new(exp123456789), PredicateOperator::AND);

    let t0 = Instant::now();
    let property_graph = create_sled_property();
    println!("create: {:?}", t0.elapsed());

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&final_exp, &mut node_cache);
    let vec = (0..50u32).collect::<Vec<u32>>();
    let t1 = Instant::now();
    property_filter.pre_fetch(&vec, &property_graph);
    println!("fetch: {:?}", t1.elapsed());

    let t2 = Instant::now();
    let result: Vec<u32> = vec.into_iter().filter(|x| property_filter.filter(*x)).collect();
    println!("exp_filter: {:?}", t2.elapsed());

    let vec0 = (0..50u32).collect::<Vec<u32>>();

    let t3 = Instant::now();
    let result: Vec<u32> = vec0.into_iter().filter(|x| property_filter.hard_coded_filter(*x)).collect();
    println!("coded_filter: {:?}", t3.elapsed());

    //    let result0 = property_filter.get_result(0);
    //    let result1 = property_filter.get_result(1);
    //
    //    assert_eq!(result0.unwrap(), false);
    //    assert_eq!(result1.unwrap(), true);
}


fn create_sled_property() -> SledProperty {
    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();
    //    for i in 0u32..50 {
    //        node_property.insert(
    //            i,
    //            object!(
    //            "name"=>"Mike",
    //            "age"=>30,
    //            "is_member"=>false,
    //            "scores"=>array![10,10,9],
    //            ),
    //        );
    //    }
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
            "name"=>"Thello",
            "age"=>5,
            ),
    );

    node_property.insert(
        4,
        object!(
            "name"=>"Thello",
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
