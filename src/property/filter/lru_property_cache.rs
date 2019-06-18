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

use lru::LruCache;
use serde_json::json;
use serde_json::Value as JsonValue;

pub struct LruNodeCache {
    node_map: Vec<JsonValue>,
    lru_indices: LruCache<usize, usize>,
}

impl LruNodeCache {
    pub fn new(capacity: usize) -> Self {
        LruNodeCache {
            node_map: Vec::with_capacity(capacity),
            lru_indices: LruCache::new(capacity),
        }
    }

    pub fn resize(&mut self, capacity: usize) {
        if capacity < self.lru_indices.cap() {
            self.lru_indices =  LruCache::new(capacity);
            self.node_map = Vec::with_capacity(capacity);
        } else {
            let current_len = self.node_map.len().clone();
            self.lru_indices.resize(capacity);
            self.node_map.reserve_exact(capacity - current_len)
        }
    }
}

impl Default for LruNodeCache {
    fn default() -> Self {
        LruNodeCache {
            node_map: vec![],
            lru_indices: LruCache::new(0usize),
        }
    }
}

impl<Id: IdType> NodeCache<Id> for LruNodeCache {
    fn get_mut(&mut self, id: Id) -> PropertyResult<&mut JsonValue> {
        if !self.lru_indices.contains(&id.id()) {
            if self.lru_indices.cap() == self.lru_indices.len() {
                let index = self.lru_indices.pop_lru().unwrap().1;
                self.node_map[index] = json!(null);
                self.lru_indices.put(id.id(), index);
                Ok(self.node_map.get_mut(index).unwrap())
            } else {
                let index = self.node_map.len();
                self.node_map.push(json!(null));
                self.lru_indices.put(id.id(), index);
                Ok(self.node_map.get_mut(index).unwrap())
            }
        } else {
            Ok(self
                .node_map
                .get_mut(*self.lru_indices.get_mut(&id.id()).unwrap())
                .unwrap())
        }
    }
}

pub struct LruEdgeCache<Id: IdType> {
    edge_map: HashMap<Id, HashMap<Id, JsonValue>>,
    lru_indices: LruCache<(Id, Id), Id>,
}

impl<Id: IdType> LruEdgeCache<Id> {
    pub fn new(capacity: usize) -> Self {
        LruEdgeCache {
            edge_map: HashMap::new(),
            lru_indices: LruCache::new(capacity),
        }
    }

    pub fn resize(&mut self, capacity: usize) {
        if capacity < self.lru_indices.cap() {
            self.lru_indices =  LruCache::new(capacity);
            self.edge_map = HashMap::new();
        } else {
            self.lru_indices.resize(capacity);
        }
    }
}

impl<Id: IdType> Default for LruEdgeCache<Id> {
    fn default() -> Self {
        LruEdgeCache {
            edge_map: HashMap::new(),
            lru_indices: LruCache::new(0usize),
        }
    }
}

impl<Id: IdType> EdgeCache<Id> for LruEdgeCache<Id> {
    fn get_mut(&mut self, src: Id, dst: Id) -> PropertyResult<&mut JsonValue> {
        if !self.lru_indices.contains(&(src, dst)) {
            if self.lru_indices.cap() == self.lru_indices.len() {
                let (old_src, old_dst) = self.lru_indices.pop_lru().unwrap().0;
                self.edge_map.get_mut(&old_src).unwrap().remove(&old_dst);
                if self.edge_map.get(&old_src).unwrap().len() == 0 {
                    self.edge_map.remove(&old_src);
                }
            }
            let property_entry = self.edge_map.entry(src).or_insert(HashMap::new());
            self.lru_indices.put((src, dst), src);
            property_entry.insert(dst, json!(null));
            Ok(property_entry.get_mut(&dst).unwrap())
        } else {
            self.lru_indices.get_mut(&(src, dst));
            Ok(self.edge_map.get_mut(&src).unwrap().get_mut(&dst).unwrap())
        }
    }
}
