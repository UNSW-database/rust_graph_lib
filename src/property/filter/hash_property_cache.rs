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

use crate::generic::IdType;
use hashbrown::HashMap;
use crate::property::filter::{EdgeCache, NodeCache, PropertyResult};
use crate::property::PropertyError;

use serde_json::json;
use serde_json::Value as JsonValue;

pub struct HashNodeCache {
    node_map: Vec<JsonValue>,
}

impl HashNodeCache {
    pub fn new<Id: IdType>(max_id: Id) -> Self {
        HashNodeCache {
            node_map: vec![json!(null); max_id.id() + 1],
        }
    }
}

impl Default for HashNodeCache {
    fn default() -> Self {
        HashNodeCache { node_map: vec![] }
    }
}

impl<Id: IdType> NodeCache<Id> for HashNodeCache {
    fn get_mut(&mut self, id: Id) -> PropertyResult<&mut JsonValue> {
        if self.node_map.len() > id.id() {
            Ok(self.node_map.get_mut(id.id()).unwrap())
        } else {
            Err(PropertyError::NodeNotFoundError)
        }
    }
}

pub struct HashEdgeCache<Id: IdType> {
    edge_map: Vec<HashMap<Id, JsonValue>>,
}

impl<Id: IdType> HashEdgeCache<Id> {
    pub fn new(max_id: Id) -> Self {
        HashEdgeCache {
            edge_map: vec![HashMap::new(); max_id.id() + 1],
        }
    }
}

impl<Id: IdType> Default for HashEdgeCache<Id> {
    fn default() -> Self {
        HashEdgeCache { edge_map: vec![] }
    }
}

impl<Id: IdType> EdgeCache<Id> for HashEdgeCache<Id> {
    fn get_mut(&mut self, src: Id, dst: Id) -> PropertyResult<&mut JsonValue> {
        if self.edge_map.len() > src.id() {
            if let Some(value) = self.edge_map.get_mut(src.id()).unwrap().get_mut(&dst) {
                Ok(value)
            } else {
                Err(PropertyError::EdgeNotFoundError)
            }
        } else {
            Err(PropertyError::EdgeNotFoundError)
        }
    }
}
