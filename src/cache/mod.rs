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

use hashbrown::{HashMap, HashSet};

use crate::generic::IdType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cache<Id: IdType> {
    cap: usize,
    size: usize,
    free: HashSet<Id>,
    reserved: HashSet<Id>,
    map: HashMap<Id, Vec<Id>>,
}

impl<Id: IdType> Cache<Id> {
    pub fn new(cap: usize) -> Self {
        Cache {
            cap,
            size: 0,
            free: HashSet::new(),
            reserved: HashSet::new(),
            map: HashMap::new(),
        }
    }

    pub fn get(&self, id: &Id) -> Option<&Vec<Id>> {
        self.map.get(id)
    }

    pub fn insert(&mut self, id: Id, value: Vec<Id>) {
        self.size += value.len();
        while self.size > self.cap && !self.free.is_empty() {
            let to_free = *self.free.iter().next().unwrap();
            self.free.remove(&to_free);

            let removed = self.map.remove(&to_free).unwrap();
            self.size -= removed.len();
        }

        self.map.insert(id, value);
    }

    pub fn contains_key(&self, id: &Id) -> bool {
        self.map.contains_key(id)
    }

    pub fn capacity(&self) -> usize {
        self.cap
    }

    pub fn size(&self) -> usize {
        self.size
    }

    pub fn reserve(&mut self, id: Id) {
        self.free.remove(&id);
        self.reserved.insert(id);
    }

    pub fn check_and_reserve(&mut self, id: Id) -> bool {
        self.reserve(id);

        self.contains_key(&id)
    }

    pub fn free_all(&mut self) {
        self.free.extend(self.reserved.drain());
    }
}
