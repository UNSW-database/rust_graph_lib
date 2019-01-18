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
use property::PropertyGraph;
use property::expression::{Expression, VarMap, VarHashMap};


struct Filter<'a, 'b, Id: IdType + 'a> {

    expression: Box<Expression + 'a>,

    property_cache: Box<VarMap<Id> + 'b>,
}

impl<'a, 'b, Id: IdType + 'a> Filter<'a, 'b, Id> {
    pub fn new(expression: Box<Expression + 'a>) {
        Filter {
            expression,
            property_cache: Box::new(VarHashMap::new())
        }
    }

    pub fn from_cache(expression: Box<Expression + 'a>, property_values: Box<VarMap<Id> + 'b>) {
        Filter {
            expression,
            property_cache: property_values
        }
    }

    pub fn fetch_node_property(&mut self, ids: &[Id], property_graph: &PropertyGraph<Id>) {
        let attribute = self.expression.get_attribute();

        for id in ids.clone() {
            if let Ok(Some(result)) = property_graph.get_node_property(id, vec![attribute]) {
                self.property_cache.set_node(id, result[attribute]);
            }
        }
    }

    pub fn fetch_edge_property(&mut self, ids: &[(Id, Id)], property_graph: &PropertyGraph<Id>) {
        let attribute = self.expression.get_attribute();

        for id in ids.clone() {
            if let Ok(Some(result)) = property_graph.get_edge_property(id.0, id.1, vec![attribute]) {
                self.property_cache.set_node(id, result[attribute]);
            }
        }
    }

    // Rewrite with macro
    pub fn check_by_node(&self, id: Id) -> Result<bool, &'static str> {
        let var = self.property_cache.get_node(id)?;
        let result = self.expression.get_value(var)?;

        match result.as_bool() {
            Some(x) => Ok(x),
            None => Err("Invalid Json value, bool expected")
        }
    }

    // Rewrite with macro
    pub fn check_by_edge(&self, id: (Id, Id)) -> Result<bool, &'static str> {
        let var = self.property_cache.get_edge(id)?;
        let result = self.expression.get_value(var)?;

        match result.as_bool() {
            Some(x) => Ok(x),
            None => Err("Invalid Json value, bool expected")
        }
    }
}


//#[cfg(test)]
//mod test {
//    use super::*;
//    use json::{array, object};
//    use std::collections::HashMap;
//
//    #[test]
//    fn test_single_num_operator() {}
//}