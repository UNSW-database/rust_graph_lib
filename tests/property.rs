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
extern crate json;
extern crate rust_graph;

use std::collections::HashMap;
use std::path::Path;

use json::number::Number;
use json::JsonValue;
use json::{array, object};
use rust_graph::property::filter::*;
use rust_graph::property::*;

use std::time::Instant;

#[test]
fn test_cached_boolean_expression() {
    // WHERE a.is_member;
    let exp = Var::new("is_member".to_owned());

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();

    assert_eq!(vec![0], result);
}

#[test]
fn test_cached_num_compare_expression() {
    // WHERE a.age > 25;

    let exp0 = Var::new("age".to_owned());
    let exp1 = Const::new(JsonValue::Number(Number::from(25)));
    let exp = PredicateExpression::new(&exp0, &exp1, PredicateOperator::GreaterThan);

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();

    assert_eq!(vec![1], result);
}

#[test]
fn test_cached_arithmetic_expression() {
    // WHERE a.age + 10 > 35;

    let exp0 = Var::new("age".to_owned());
    let exp1 = Const::new(JsonValue::Number(Number::from(10)));
    let exp2 = ArithmeticExpression::new(&exp0, &exp1, ArithmeticOperator::Add);
    let exp3 = Const::new(JsonValue::Number(Number::from(35)));

    let exp = PredicateExpression::new(&exp2, &exp3, PredicateOperator::GreaterThan);

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();

    assert_eq!(result, vec![1]);
}

#[test]
fn test_cached_logical_expression() {
    // WHERE a.age + 10 > 35 AND a.is_member;

    let exp0 = Var::new("age".to_owned());
    let exp1 = Const::new(JsonValue::Number(Number::from(10)));
    let exp2 = ArithmeticExpression::new(&exp0, &exp1, ArithmeticOperator::Add);
    let exp3 = Const::new(JsonValue::Number(Number::from(35)));
    let exp4 = PredicateExpression::new(&exp2, &exp3, PredicateOperator::LessEqual);
    let exp5 = Var::new("is_member".to_owned());

    let exp = PredicateExpression::new(&exp4, &exp5, PredicateOperator::AND);

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();

    assert_eq!(vec![0], result);
}

#[test]
fn test_cached_string_compare_expression() {
    // WHERE a.name CONTAINS "arr";

    let exp0 = Var::new("name".to_owned());
    let exp1 = Const::new(JsonValue::String("arr".to_owned()));
    let exp = PredicateExpression::new(&exp0, &exp1, PredicateOperator::Contains);

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();

    assert_eq!(vec![1], result);
}

#[test]
fn test_cached_string_concat_expression() {
    // WHERE a.name + "hello" CONTAINS "arr";

    let exp0 = Var::new("name".to_owned());
    let exp1 = Const::new(JsonValue::String("hello".to_owned()));
    let exp2 = ArithmeticExpression::new(&exp0, &exp1, ArithmeticOperator::Concat);
    let exp3 = Const::new(JsonValue::String("yhello".to_owned()));

    let exp = PredicateExpression::new(&exp2, &exp3, PredicateOperator::Contains);

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();

    assert_eq!(vec![1], result);
}

#[test]
fn test_cached_range_predicate_expression() {
    // WHERE 18 <= a.age <= 22;

    let exp0 = Var::new("age".to_owned());
    let exp1 = Const::new(array![
        JsonValue::Number(Number::from(18)),
        JsonValue::Number(Number::from(22))
    ]);
    let exp = PredicateExpression::new(&exp0, &exp1, PredicateOperator::Range);

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();

    assert_eq!(vec![0], result);
}

#[test]
fn test_cached_error_boolean_expression() {
    // WHERE a.is_member;
    let exp = Var::new("age".to_owned());

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();

    assert_eq!(Vec::<u32>::new(), result);
}

#[test]
fn test_cached_complex_expression() {
    // WHERE a.is_member AND ((a.age MODULE 5 = 0) AND (18 <= a.age <= 35 AND ((a.name CONTAINS "a") OR (a.name CONTAINS "o"))));

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
    let exp12 = ArithmeticExpression::new(&exp1, &exp2, ArithmeticOperator::Modulo);
    let exp123 = PredicateExpression::new(&exp12, &exp3, PredicateOperator::Equal);
    let exp45 = PredicateExpression::new(&exp4, &exp5, PredicateOperator::Range);
    let exp67 = PredicateExpression::new(&exp6, &exp7, PredicateOperator::Contains);
    let exp89 = PredicateExpression::new(&exp8, &exp9, PredicateOperator::Contains);
    let exp6789 = PredicateExpression::new(&exp67, &exp89, PredicateOperator::OR);
    let exp456789 = PredicateExpression::new(&exp45, &exp6789, PredicateOperator::AND);
    let exp123456789 = PredicateExpression::new(&exp123, &exp456789, PredicateOperator::AND);
    let final_exp = PredicateExpression::new(&exp0, &exp123456789, PredicateOperator::AND);

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&final_exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result: Vec<u32> = vec![0, 1]
        .into_iter()
        .filter(|x| property_filter.filter(*x))
        .collect();
    println!("{:?}", result);
    //    assert_eq!(vec![0], result);
}

fn create_cached_property() -> CachedProperty<u32> {
    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();

    node_property.insert(
        0u32,
        object!(
        "name"=>"John",
        "age"=>20,
        "is_member"=>true,
        "scores"=>array![9,8,10],
        ),
    );

    node_property.insert(
        1,
        object!(
        "name"=>"Marry",
        "age"=>30,
        "is_member"=>false,
        "scores"=>array![10,10,9],
        ),
    );

    edge_property.insert(
        (0, 1),
        object!(
        "friend_since"=>"2018-11-15",
        ),
    );

    CachedProperty::with_data(node_property, edge_property, false)
}

//#[test]
//fn test_sled_boolean_expression() {
//    // WHERE a.is_member;
//    let exp = Var::new("is_member".to_owned());
//
//    let property_graph = create_sled_property();
//
//    let mut node_cache = HashNodeCache::new();
//    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);
//
//    property_filter.pre_fetch(&[0u32, 1], &property_graph);
//
//    let result0 = property_filter.get_result(0);
//    let result1 = property_filter.get_result(1);
//
//    assert_eq!(result0.unwrap(), true);
//    assert_eq!(result1.unwrap(), false);
//}

#[test]
fn test_sled_num_compare_expression() {
    // WHERE a.age > 25;

    let exp0 = Var::new("age".to_owned());
    let exp1 = Const::new(JsonValue::Number(Number::from(25)));
    let exp = PredicateExpression::new(&exp0, &exp1, PredicateOperator::GreaterThan);

    let t0 = Instant::now();
    let property_graph = create_sled_property();
    println!("{:?}", t0.elapsed());

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);
    let t1 = Instant::now();

    property_filter.pre_fetch(&[0u32, 1], &property_graph);
    println!("{:?}", t1.elapsed());
    //    let result0 = property_filter.get_result(0);
    //    let result1 = property_filter.get_result(1);
    //
    //    assert_eq!(result0.unwrap(), false);
    //    assert_eq!(result1.unwrap(), true);
}

fn create_sled_property() -> SledProperty {
    let mut node_property = HashMap::new();
    let mut edge_property = HashMap::new();
    for i in 0u32..100000 {
        node_property.insert(
            i,
            object!(
            "name"=>"Mike",
            "age"=>30,
            "is_member"=>false,
            "scores"=>array![10,10,9],
            ),
        );
    }

    edge_property.insert(
        (0, 1),
        object!(
        "friend_since"=>"2018-11-15",
        ),
    );

    let path = Path::new("../undirected");
    let db = SledProperty::with_data(
        path,
        node_property.into_iter(),
        edge_property.into_iter(),
        false,
    )
    .unwrap();
    db.flush();
    db
}
