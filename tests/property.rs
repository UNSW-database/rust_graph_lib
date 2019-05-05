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

use std::collections::HashMap;

use rust_graph::property::filter::*;
use rust_graph::property::*;
use serde_json::json;
use std::sync::Arc;


#[test]
fn test_cached_boolean_expression() {
    // WHERE a.is_member;
    let exp = Box::new(Var::new("is_member".to_owned()));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(vec![0], result);
}

#[test]
fn test_cached_num_compare_expression() {
    // WHERE a.age > 25;

    let exp0 = Box::new(Var::new("age".to_owned()));
    let exp1 = Box::new(Const::new(json!(25)));
    let exp = Box::new(PredicateExpression::new(
        exp0,
        exp1,
        PredicateOperator::GreaterThan,
    ));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(vec![1], result);
}

#[test]
fn test_cached_arithmetic_expression() {
    // WHERE a.age + 10 > 35;

    let exp0 = Box::new(Var::new("age".to_owned()));
    let exp1 = Box::new(Const::new(json!(10)));
    let exp2 = Box::new(ArithmeticExpression::new(
        exp0,
        exp1,
        ArithmeticOperator::Add,
    ));
    let exp3 = Box::new(Const::new(json!(35)));

    let exp = Box::new(PredicateExpression::new(
        exp2,
        exp3,
        PredicateOperator::GreaterThan,
    ));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(result, vec![1]);
}

#[test]
fn test_cached_logical_expression() {
    // WHERE a.age + 10 > 35 AND a.is_member;

    let exp0 = Box::new(Var::new("age".to_owned()));
    let exp1 = Box::new(Const::new(json!(10)));
    let exp2 = Box::new(ArithmeticExpression::new(
        exp0,
        exp1,
        ArithmeticOperator::Add,
    ));
    let exp3 = Box::new(Const::new(json!(35)));
    let exp4 = Box::new(PredicateExpression::new(
        exp2,
        exp3,
        PredicateOperator::LessEqual,
    ));
    let exp5 = Box::new(Var::new("is_member".to_owned()));

    let exp = Box::new(PredicateExpression::new(exp4, exp5, PredicateOperator::AND));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(vec![0], result);
}

#[test]
fn test_cached_string_compare_expression() {
    // WHERE a.name CONTAINS "arr";

    let exp0 = Box::new(Var::new("name".to_owned()));
    let exp1 = Box::new(Const::new(json!("arr".to_owned())));
    let exp = Box::new(PredicateExpression::new(
        exp0,
        exp1,
        PredicateOperator::Contains,
    ));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(vec![1], result);
}

#[test]
fn test_cached_string_concat_expression() {
    // WHERE a.name + "hello" CONTAINS "arr";

    let exp0 = Box::new(Var::new("name".to_owned()));
    let exp1 = Box::new(Const::new(json!("hello".to_owned())));
    let exp2 = Box::new(ArithmeticExpression::new(
        exp0,
        exp1,
        ArithmeticOperator::Concat,
    ));
    let exp3 = Box::new(Const::new(json!("yhello".to_owned())));

    let exp = Box::new(PredicateExpression::new(
        exp2,
        exp3,
        PredicateOperator::Contains,
    ));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(vec![1], result);
}

#[test]
fn test_cached_range_predicate_expression() {
    // WHERE 18 <= a.age <= 22;

    let exp0 = Box::new(Var::new("age".to_owned()));
    let exp1 = Box::new(Const::new(json!([18, 22])));
    let exp = Box::new(PredicateExpression::new(
        exp0,
        exp1,
        PredicateOperator::Range,
    ));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(vec![0], result);
}

#[test]
fn test_cached_error_boolean_expression() {
    // WHERE a.is_member;
    let exp = Box::new(Var::new("age".to_owned()));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(Vec::<u32>::new(), result);
}

#[test]
fn test_cached_complex_expression() {
    // WHERE a.is_member AND ((a.age MODULE 5 = 0) AND (18 <= a.age <= 35 AND ((a.name CONTAINS "a") OR (a.name CONTAINS "o"))));

    let exp0 = Box::new(Var::new("is_member".to_owned()));
    let exp1 = Box::new(Var::new("age".to_owned()));
    let exp2 = Box::new(Const::new(json!(5)));
    let exp3 = Box::new(Const::new(json!(0)));
    let exp4 = Box::new(Const::new(json!([18, 35])));
    let exp5 = Box::new(Var::new("age".to_owned()));
    let exp6 = Box::new(Var::new("name".to_owned()));
    let exp7 = Box::new(Const::new(json!("a".to_owned())));
    let exp8 = Box::new(Var::new("name".to_owned()));
    let exp9 = Box::new(Const::new(json!("o".to_owned())));
    let exp12 = Box::new(ArithmeticExpression::new(
        exp1,
        exp2,
        ArithmeticOperator::Modulo,
    ));
    let exp123 = Box::new(PredicateExpression::new(
        exp12,
        exp3,
        PredicateOperator::Equal,
    ));
    let exp45 = Box::new(PredicateExpression::new(
        exp4,
        exp5,
        PredicateOperator::Range,
    ));
    let exp67 = Box::new(PredicateExpression::new(
        exp6,
        exp7,
        PredicateOperator::Contains,
    ));
    let exp89 = Box::new(PredicateExpression::new(
        exp8,
        exp9,
        PredicateOperator::Contains,
    ));
    let exp6789 = Box::new(PredicateExpression::new(
        exp67,
        exp89,
        PredicateOperator::OR,
    ));
    let exp456789 = Box::new(PredicateExpression::new(
        exp45,
        exp6789,
        PredicateOperator::AND,
    ));
    let exp123456789 = Box::new(PredicateExpression::new(
        exp123,
        exp456789,
        PredicateOperator::AND,
    ));
    let final_exp = Box::new(PredicateExpression::new(
        exp0,
        exp123456789,
        PredicateOperator::AND,
    ));

    let property_graph = create_cached_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let _result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, final_exp.box_clone()))
        .collect();
    //    assert_eq!(vec![0], result);
}


#[test]
fn test_sled_boolean_expression() {
    // WHERE a.is_member;
    let exp = Box::new(Var::new("is_member".to_owned()));

    let property_graph = create_sled_property();
    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(Vec::<u32>::new(), result);
}

#[test]
fn test_sled_num_compare_expression() {
    // WHERE a.age > 25;

    let exp0 = Box::new(Var::new("age".to_owned()));
    let exp1 = Box::new(Const::new(json!(25)));
    let exp = Box::new(PredicateExpression::new(
        exp0,
        exp1,
        PredicateOperator::GreaterThan,
    ));

    let property_graph = create_sled_property();

    let mut property_cache = PropertyCache::new_default(Arc::new(property_graph));
    property_cache.pre_fetch(vec![0, 1].into_iter(), vec![].into_iter()).unwrap();

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| filter_node(*x, &property_cache, exp.box_clone()))
        .collect();

    assert_eq!(vec![0, 1], result);
}

fn create_sled_property() -> SledProperty {
    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();
    for i in 0u32..10 {
        node_property.insert(
            i,
            json!({
            "name":"Mike",
            "age":30,
            "is_member":false,
            "scores":json!([10,10,9]),
            }),
        );
    }

    edge_property.insert(
        (0, 1),
        json!({
        "friend_since":"2018-11-15",
        }),
    );

    let node = tempdir::TempDir::new("node").unwrap();
    let edge = tempdir::TempDir::new("edge").unwrap();

    let node_path = node.path();
    let edge_path = edge.path();

    let db = SledProperty::with_data(
        node_path,
        edge_path,
        node_property.into_iter(),
        edge_property.into_iter(),
        false,
    )
    .unwrap();
    db.flush().unwrap();
    db
}

fn create_cached_property() -> CachedProperty<u32> {
    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();

    node_property.insert(
        0u32,
        json!({
        "name":"John",
        "age":20,
        "is_member":true,
        "scores":json!([9,8,10]),
        }),
    );

    node_property.insert(
        1,
        json!({
        "name":"Marry",
        "age":30,
        "is_member":false,
        "scores":json!([10,10,9]),
        }),
    );

    edge_property.insert(
        (0, 1),
        json!({
        "friend_since":"2018-11-15",
        }),
    );

    CachedProperty::with_data(node_property, edge_property, false)
}