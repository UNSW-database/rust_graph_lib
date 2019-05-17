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
use property::filter::{EdgeCache, Expression, NodeCache, PropertyResult};
use property::{PropertyCache, PropertyError, PropertyGraph};

pub fn filter_edge<Id: IdType, PG: PropertyGraph<Id>, NC: NodeCache<Id>, EC: EdgeCache<Id>>(
    id: (Id, Id),
    property_cache: &PropertyCache<Id, PG, NC, EC>,
    expression: &Expression,
) -> bool {
    let result = get_edge_filter_result(id, property_cache, expression);

    if result.is_err() {
        debug!("edge {:?} has error {:?}", id, result.err().unwrap());
        false
    } else {
        let bool_result = result.unwrap();
        bool_result
    }
}

pub fn get_edge_filter_result<
    Id: IdType,
    PG: PropertyGraph<Id>,
    NC: NodeCache<Id>,
    EC: EdgeCache<Id>,
>(
    id: (Id, Id),
    property_cache: &PropertyCache<Id, PG, NC, EC>,
    expression: &Expression,
) -> PropertyResult<bool> {
    let var = property_cache.get_edge_property(id.0, id.1)?;
    let result_cow = expression.get_value(var)?;
    let result = result_cow.as_ref();
    match result.as_bool() {
        Some(x) => Ok(x),
        None => Err(PropertyError::BooleanExpressionError),
    }
}
