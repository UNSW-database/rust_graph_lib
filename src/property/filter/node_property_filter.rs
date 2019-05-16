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

use generic::IdType;
use property::filter::{EdgeCache, Expression, NodeCache, PropertyResult};
use property::{PropertyCache, PropertyError, PropertyGraph};

pub fn filter_node<Id: IdType, PG: PropertyGraph<Id>, NC: NodeCache<Id>, EC: EdgeCache<Id>>(
    id: Id,
    property_cache: &PropertyCache<Id, PG, NC, EC>,
    expression: Box<Expression>,
) -> bool {
    let result = get_node_filter_result(id, property_cache, expression);
    if result.is_err() {
        debug!("node {:?} has error {:?}", id, result.err().unwrap());
        false
    } else {
        let bool_result = result.unwrap();
        bool_result
    }
}

pub fn get_node_filter_result<
    Id: IdType,
    PG: PropertyGraph<Id>,
    NC: NodeCache<Id>,
    EC: EdgeCache<Id>,
>(
    id: Id,
    property_cache: &PropertyCache<Id, PG, NC, EC>,
    expression: Box<Expression>,
) -> PropertyResult<bool> {
    let var = property_cache.get_node_property(id).unwrap();
    let result = expression.get_value(var)?;

    match result.as_bool() {
        Some(x) => Ok(x),
        None => Err(PropertyError::BooleanExpressionError),
    }
}
