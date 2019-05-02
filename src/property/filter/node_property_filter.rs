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

// 1. Query the property graph with the given list of node ids/edge ids, by firstly getting the attribute
// 2. build the hash map according to the queried values
// 3. when running ,first pass the queried id to filter function, then get value with the hashmap.get(id), then pass value to get_result recursion.

use generic::IdType;
use property::filter::{Expression, NodeCache, PropertyResult};
use property::PropertyError;

pub fn filter_node<Id: IdType>(
    id: Id,
    node_property_cache: &impl NodeCache<Id>,
    expression: Box<Expression>,
) -> bool {
    let result = get_node_filter_result(id, node_property_cache, expression);
    if result.is_err() {
        println!("node {:?} has error {:?}", id, result.err().unwrap());
        false
    } else {
        let bool_result = result.unwrap();
//        println!("node {:?} got result {:?}", id, bool_result);
        bool_result
    }
}

pub fn get_node_filter_result<Id: IdType>(
    id: Id,
    node_property_cache: &impl NodeCache<Id>,
    expression: Box<Expression>,
) -> PropertyResult<bool> {
    let var = node_property_cache.get(id)?;
    let result = expression.get_value(&var)?;

    match result.as_bool() {
        Some(x) => Ok(x),
        None => Err(PropertyError::BooleanExpressionError),
    }
}
