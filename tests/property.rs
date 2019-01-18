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

use std::collections::HashMap;

use rust_graph::property::*;
use rust_graph::property::filter::*;
use json::JsonValue;
use json::number::Number;
use json::{array, object};


#[test]
fn test_boolean_expression() {
    // WHERE a.is_member;
    let exp = Var::new("is_member".to_owned());

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);

    let result0 = property_filter.get_result(0);
    let result1 = property_filter.get_result(1);

    assert_eq!(result0.unwrap(), true);
    assert_eq!(result1.unwrap(), false);
}


#[test]
fn test_num_compare_expression() {
    // WHERE a.age > 25;

    let exp0 = Var::new("age".to_owned());
    let exp1 = Const::new(JsonValue::Number(Number::from(25)));
    let exp = PredicateExpression::new(&exp0, &exp1, PredicateOperator::GreaterThan);


    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);
    let result0 = property_filter.get_result(0);
    let result1 = property_filter.get_result(1);

    assert_eq!(result0.unwrap(), false);
    assert_eq!(result1.unwrap(), true);
}


#[test]
fn test_arithmetic_expression() {
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
    let result0 = property_filter.get_result(0);
    let result1 = property_filter.get_result(1);

    let result: Vec<u32> = vec![0, 1].into_iter().filter(|x| property_filter.filter(*x)).collect();

    assert_eq!(result0.unwrap(), false);
    assert_eq!(result1.unwrap(), true);
    assert_eq!(result, vec![1]);
}


#[test]
fn test_logical_expression() {
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
    let result0 = property_filter.get_result(0);
    let result1 = property_filter.get_result(1);

    assert_eq!(result0.unwrap(), true);
    assert_eq!(result1.unwrap(), false);
}


#[test]
fn test_string_compare_expression() {
    // WHERE a.name CONTAINS "arr";

    let exp0 = Var::new("name".to_owned());
    let exp1 = Const::new(JsonValue::String("arr".to_owned()));
    let exp = PredicateExpression::new(&exp0, &exp1, PredicateOperator::Contains);

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);
    let result0 = property_filter.get_result(0);
    let result1 = property_filter.get_result(1);

    assert_eq!(result0.unwrap(), false);
    assert_eq!(result1.unwrap(), true);
}

#[test]
fn test_string_concat_expression() {
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
    let result0 = property_filter.get_result(0);
    let result1 = property_filter.get_result(1);

    assert_eq!(result0.unwrap(), false);
    assert_eq!(result1.unwrap(), true);
}


#[test]
fn test_range_predicate_expression() {
    // WHERE 18 <= a.age <= 22;

    let exp0 = Var::new("age".to_owned());
    let exp1 = Const::new(array![JsonValue::Number(Number::from(18)), JsonValue::Number(Number::from(22))]);
    let exp = PredicateExpression::new(&exp0, &exp1, PredicateOperator::Range);


    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);
    let result0 = property_filter.get_result(0);
    let result1 = property_filter.get_result(1);

    assert_eq!(result0.unwrap(), true);
    assert_eq!(result1.unwrap(), false);
}


#[test]
fn test_error_boolean_expression() {
    // WHERE a.is_member;
    let exp = Var::new("age".to_owned());

    let property_graph = create_cached_property();

    let mut node_cache = HashNodeCache::new();
    let mut property_filter = NodeFilter::from_cache(&exp, &mut node_cache);

    property_filter.pre_fetch(&[0, 1], &property_graph);
    let result0 = property_filter.get_result(0);
    let result1 = property_filter.get_result(1);

    assert_eq!(result0.is_err(), true);
    assert_eq!(result1.is_err(), true);
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
