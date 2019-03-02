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
use property::filter::PropertyResult;
use property::{PropertyGraph, PropertyError};
use property::filter::{Expression, EdgeCache};


pub struct EdgeFilter<'a, Id: IdType> {

    expression: &'a Expression,

    edge_property_cache: &'a mut EdgeCache<Id>,
}

impl<'a, Id: IdType> EdgeFilter<'a, Id> {
//    pub fn new(expression: &'static Expression) -> Self {
//        EdgeFilter {
//            expression,
//            edge_property_cache: &HashEdgeCache::new()
//        }
//    }

    pub fn from_cache(expression: &'a Expression, edge_property_cache: &'a mut EdgeCache<Id>) -> Self {
        EdgeFilter {
            expression,
            edge_property_cache
        }
    }

    pub fn pre_fetch(&mut self, ids: &[(Id, Id)], property_graph: &PropertyGraph<Id, Err=()>) -> PropertyResult<()> {

        for id in ids {
            if let Some(result) = property_graph.get_edge_property_all(id.0, id.1)? {
                self.edge_property_cache.set(id.0, id.1, result);
            }
        }
        Ok(())
    }

    pub fn get_result(&self, id: (Id, Id)) -> PropertyResult<bool> {
        let var = self.edge_property_cache.get(id.0, id.1)?;
        let result = self.expression.get_value(&var)?;

        match result.as_bool() {
            Some(x) => Ok(x),
            None => Err(PropertyError::BooleanExpressionError)
        }
    }
}

