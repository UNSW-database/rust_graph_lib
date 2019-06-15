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
use property::filter::{EdgeCache, NodeCache, PropertyResult, LruCache};
use property::PropertyError;

use serde_json::json;
use serde_json::Value as JsonValue;

pub struct LruNodeCache {
    node_map: Vec<JsonValue>,
    lru_indices: LruCache<usize>
}

impl LruNodeCache {
    pub fn new<Id: IdType>(max_id: Id, capacity: usize) -> Self {
        LruNodeCache {
            node_map: vec![json!(null); max_id.id() + 1],
            lru_indices: LruCache::new(capacity)
        }
    }
}

impl Default for LruNodeCache {
    fn default() -> Self {
        LruNodeCache {
            node_map: vec![],
            lru_indices: LruCache::new(0usize)
        }
    }
}

impl<Id: IdType> NodeCache<Id> for LruNodeCache {
    fn set(&mut self, id: Id, value: JsonValue) -> bool {
        if self.lru_indices.is_full() {
            let old_node = self.lru_indices.pop_lru().unwrap();
            self.node_map[old_node] = json!(null);
        }
        self.lru_indices.put(id.id());
        self.node_map[id.id()] = value;
        true
    }

    fn get_mut(&mut self, id: Id) -> PropertyResult<&mut JsonValue> {
        if self.node_map.len() > id.id() {
            if self.lru_indices.contains(&id.id()) {
                self.lru_indices.put(id.id());
            }
            Ok(self.node_map.get_mut(id.id()).unwrap())
        } else {
            Err(PropertyError::NodeNotFoundError)
        }
    }
}


pub struct LruEdgeCache<Id: IdType> {
    edge_map: Vec<HashMap<Id, JsonValue>>,
    lru_indices: LruCache<(usize, usize)>
}

impl<Id: IdType> LruEdgeCache<Id> {
    pub fn new(max_id: Id, capacity: usize) -> Self {
        LruEdgeCache {
            edge_map: vec![HashMap::new(); max_id.id() + 1],
            lru_indices: LruCache::new(capacity)
        }
    }
}

impl<Id: IdType> Default for LruEdgeCache<Id> {
    fn default() -> Self {
        LruEdgeCache {
            edge_map: vec![],
            lru_indices: LruCache::new(0usize)
        }
    }
}

impl<Id: IdType> EdgeCache<Id> for LruEdgeCache<Id> {
    fn set(&mut self, src: Id, dst: Id, value: JsonValue) -> bool {
        if self.lru_indices.is_full() {
            let (old_src, old_dst) = self.lru_indices.pop_lru().unwrap();
            self.edge_map.get_mut(old_src.id()).unwrap().insert(Id::new(old_dst), json!(null));
        }
        self.lru_indices.put((src.id(), dst.id()));
        self.edge_map.get_mut(src.id()).unwrap().insert(dst, value);
        true
    }
    fn get_mut(&mut self, src: Id, dst: Id) -> PropertyResult<&mut JsonValue> {
        if self.edge_map.len() > src.id() {
            if self.lru_indices.contains(&(src.id(), dst.id())) {
                self.lru_indices.put((src.id(), dst.id()));
            }

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
