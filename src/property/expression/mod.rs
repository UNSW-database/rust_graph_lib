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

// 1. Change to refcell
// 2. Add error types
// 3. Change cache to store json object (all attributes)
// 4. Add comments
// 5. exp ma ager prefetch

// 1. edge p f/ n p f
// 2. possible errors listed

pub mod filter;
pub mod value;
pub mod predicate;
pub mod operators;

use generic::IdType;
use json::JsonValue;

use std::collections::HashMap;


pub trait Expression {
    // Get the result of expression as a Json Value.
    fn get_value(&self, var: JsonValue) -> Result<JsonValue, &'static str>;

    fn get_attribute(&self) -> String;
}


pub trait VarMap<Id: IdType> {

    fn get_node(&self, id:Id) -> Result<JsonValue, &'static str>;

    fn set_node(&self, id:Id, value: JsonValue);

    fn get_edge(&self, id:(Id, Id)) -> Result<JsonValue, &'static str>;

    fn set_edge(&self, id:(Id, Id), value: JsonValue);
}


pub struct VarHashMap<Id: IdType> {
    node_map: HashMap<Id, JsonValue>,
    edge_map: HashMap<(Id, Id), JsonValue>
}

impl<Id: IdType> VarHashMap<Id> {
    pub fn new() {
        VarHashMap {
            node_map: HashMap::new(),
            edge_map: HashMap::new()
        }
    }
}

impl<Id: IdType> VarMap for VarHashMap<Id> {
    fn get_node(&self, id: Id) -> Result<JsonValue, &'static str> {
        match self.node_map.get(id) {
            Some(value) => Ok(value.clone()),
            None => Err("Node Id not found")
        }
    }

    fn set_node(&mut self, id: Id, value: JsonValue) {
        self.node_map.insert(id, value);
    }

    fn get_edge(&self, id: (Id, Id)) -> Result<JsonValue, &'static str> {
        match self.edge_map.get(id) {
            Some(value) => Ok(value.clone()),
            None => Err("Edge Id not found")
        }
    }

    fn set_edge(&mut self, id: (Id, Id), value: JsonValue) {
        self.edge_map.insert(id, value);
    }
}