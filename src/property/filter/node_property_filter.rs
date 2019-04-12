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
use property::{PropertyError, PropertyGraph};

pub struct NodeFilter<'a, Id: IdType> {
    expression: &'a Expression,

    node_property_cache: &'a mut NodeCache<Id>,
}

impl<'a, Id: IdType> NodeFilter<'a, Id> {
    //    pub fn new(expression: &'static Expression) -> Self {
    //        NodeFilter {
    //            expression,
    //            node_property_cache: &HashNodeCache::new()
    //        }
    //    }

    pub fn from_cache(
        expression: &'a Expression,
        node_property_cache: &'a mut NodeCache<Id>,
    ) -> Self {
        NodeFilter {
            expression,
            node_property_cache,
        }
    }

    pub fn pre_fetch(
        &mut self,
        ids: &[Id],
        property_graph: &PropertyGraph<Id>,
    ) -> PropertyResult<()> {
        for id in ids {
            if let Some(result) = property_graph.get_node_property_all(id.clone())? {
                self.node_property_cache.set(id.clone(), result);
            } else {
                self.node_property_cache
                    .set(id.clone(), json::JsonValue::Null);
            }
        }
        Ok(())
    }

    pub fn get_result(&self, id: Id) -> PropertyResult<bool> {
        let var = self.node_property_cache.get(id)?;
        let result = self.expression.get_value(&var)?;

        match result.as_bool() {
            Some(x) => Ok(x),
            None => Err(PropertyError::BooleanExpressionError),
        }
    }

    pub fn filter(&self, id: Id) -> bool {
        self.get_result(id).unwrap_or_default()
    }

    pub fn hard_coded_filter(&self, id: Id) -> bool {
        //a.is_member AND ((a.age MODULE 5 = 0) AND (18 <= a.age <= 35 AND ((a.name CONTAINS "a") OR (a.name CONTAINS "o"))))

        let result = self.node_property_cache.get(id);
        if result.is_err() {
            false
        } else {
            let value = result.unwrap();
            let is_member = value["is_member"].as_bool().unwrap();
            let age = value["age"].as_f64().unwrap();
            let name = value["name"].as_str().unwrap();

            is_member
                && ((age % 5.0 == 0.0)
                    && (age >= 18.0
                        && age <= 35.0
                        && ((name.contains("a")) || (name.contains("o")))))
        }
    }
}
