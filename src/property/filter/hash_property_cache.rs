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
use hashbrown::HashMap;
use property::filter::{EdgeCache, NodeCache, PropertyResult};
use property::{PropertyError, PropertyGraph};

use serde_json::json;
use serde_json::Value as JsonValue;

pub struct HashNodeCache {
    node_map: Vec<JsonValue>,
}

impl HashNodeCache {
    pub fn new() -> Self {
        HashNodeCache {
            node_map: Vec::new(),
        }
    }
}

impl<Id: IdType> NodeCache<Id> for HashNodeCache {
    fn get(&self, id: Id) -> PropertyResult<&JsonValue> {
        if let Some(value) = self.node_map.get(id.id()) {
            Ok(value)
        } else {
            Err(PropertyError::NodeNotFoundError)
        }
    }

    fn set(&mut self, id: Id, value: JsonValue) -> bool {
        self.node_map[id.id()] = value;
        true
    }

    fn add(&mut self, value: JsonValue) -> Id {
        let length = self.node_map.len();
        self.node_map.push(value);
        Id::new(length)
    }
}

pub struct HashEdgeCache<Id: IdType> {
    edge_map: Vec<HashMap<Id, JsonValue>>,
}

impl<Id: IdType> HashEdgeCache<Id> {
    pub fn new() -> Self {
        HashEdgeCache {
            edge_map: Vec::new(),
        }
    }
}

impl<Id: IdType> EdgeCache<Id> for HashEdgeCache<Id> {
    fn get(&self, src: Id, dst: Id) -> PropertyResult<&JsonValue> {
        if let Some(value) = self.edge_map.get(src.id()) {
            if let Some(value) = self.edge_map[src.id()].get(&dst) {
                Ok(value)
            } else {
                Err(PropertyError::EdgeNotFoundError)
            }
        } else {
            Err(PropertyError::EdgeNotFoundError)
        }
    }

    fn set(&mut self, src: Id, dst: Id, value: JsonValue) -> bool {
        let mut result = false;
        let mut_target = self.edge_map.get_mut(src.id()).unwrap();
        mut_target.insert(dst, value);
        result
    }

    fn add(&mut self) -> Id {
        let length = self.edge_map.len();
        self.edge_map.push(HashMap::new());
        Id::new(length)
    }
}
