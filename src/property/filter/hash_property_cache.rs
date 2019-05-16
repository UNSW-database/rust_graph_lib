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

pub struct HashNodeCache<Id: IdType> {
    node_map: HashMap<Id, JsonValue>,
}

impl<Id: IdType> HashNodeCache<Id> {
    pub fn new() -> Self {
        HashNodeCache {
            node_map: HashMap::new(),
        }
    }
}

impl<Id: IdType> NodeCache<Id> for HashNodeCache<Id> {
    fn get(&self, id: Id) -> PropertyResult<&JsonValue> {
        if let Some(value) = self.node_map.get(&id) {
            Ok(value)
        } else {
            Err(PropertyError::NodeNotFoundError)
        }
    }

    fn set(&mut self, id: Id, value: JsonValue) -> bool {
        let mut result = false;
        {
            if self.node_map.contains_key(&id) {
                result = true;
            }
        }
        self.node_map.insert(id, value);
        result
    }

    fn pre_fetch<P: PropertyGraph<Id>, I: IntoIterator<Item = Id>>(
        &mut self,
        ids: I,
        property_graph: &P,
    ) -> PropertyResult<()> {
        for id in ids {
            if let Some(result) = property_graph.get_node_property_all(id.clone())? {
                self.set(id.clone(), result);
            } else {
                self.set(id.clone(), json!(null));
            }
        }
        Ok(())
    }
}

pub struct HashEdgeCache<Id: IdType> {
    edge_map: HashMap<(Id, Id), JsonValue>,
}

impl<Id: IdType> HashEdgeCache<Id> {
    pub fn new() -> Self {
        HashEdgeCache {
            edge_map: HashMap::new(),
        }
    }
}

impl<Id: IdType> EdgeCache<Id> for HashEdgeCache<Id> {
    fn get(&self, src: Id, dst: Id) -> PropertyResult<&JsonValue> {
        if let Some(value) = self.edge_map.get(&(src, dst)) {
            Ok(value)
        } else {
            Err(PropertyError::EdgeNotFoundError)
        }
    }

    fn set(&mut self, src: Id, dst: Id, value: JsonValue) -> bool {
        let mut result = false;
        {
            if self.edge_map.contains_key(&(src, dst)) {
                result = true;
            }
        }
        self.edge_map.insert((src, dst), value);
        result
    }

    fn pre_fetch<P: PropertyGraph<Id>, I: IntoIterator<Item = (Id, Id)>>(
        &mut self,
        ids: I,
        property_graph: &P,
    ) -> PropertyResult<()> {
        for id in ids {
            if let Some(result) = property_graph.get_edge_property_all(id.0, id.1)? {
                self.set(id.0, id.1, result);
            } else {
                self.set(id.0, id.1, json!(null));
            }
        }
        Ok(())
    }
}
