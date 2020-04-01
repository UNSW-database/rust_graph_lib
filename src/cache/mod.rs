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

use std::mem::size_of;
use std::time::{Duration, Instant};

use byte_unit::Byte;
use hashbrown::{HashMap, HashSet};

use crate::generic::IdType;

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Cache<Id: IdType> {
    cap: usize,
    size: usize,
    free: HashSet<Id>,
    reserved: HashSet<Id>,
    map: HashMap<Id, Vec<Id>>,
    insert_time: Duration,
    reserve_time: Duration,
    free_time: Duration,
}

impl<Id: IdType> Cache<Id> {
    pub fn new(cap: usize) -> Self {
        info!("Cache capacity: {}", cap);

        Cache {
            cap,
            size: 0,
            free: HashSet::new(),
            reserved: HashSet::new(),
            map: HashMap::new(),
            insert_time: Duration::from_secs(0),
            reserve_time: Duration::from_secs(0),
            free_time: Duration::from_secs(0),
        }
    }

    pub fn with_bytes<S: AsRef<str>>(s: S) -> Self {
        let bytes = Byte::from_str(s).unwrap();
        let id_size = size_of::<Id>() as u128;
        let cap = bytes.get_bytes() / id_size;

        Self::new(cap as usize)
    }

    pub fn unbounded() -> Self {
        Self::new(std::usize::MAX)
    }

    pub fn get(&self, id: &Id) -> Option<&Vec<Id>> {
        self.map.get(id)
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

    pub fn insert(&mut self, id: Id, value: Vec<Id>) {
        let start = Instant::now();

        self.size += value.len();
        while self.size > self.cap && !self.free.is_empty() {
            let to_free = *self.free.iter().next().unwrap();
            self.free.remove(&to_free);

            let removed = self.map.remove(&to_free).unwrap();
            self.size -= removed.len();
        }

        self.map.insert(id, value);

        let elapsed = start.elapsed();
        self.insert_time += elapsed;
    }

    pub fn reserve(&mut self, id: Id) {
        let start = Instant::now();

        self.free.remove(&id);
        self.reserved.insert(id);

        let elapsed = start.elapsed();
        self.reserve_time += elapsed;
    }

    pub fn check_and_reserve(&mut self, id: Id) -> bool {
        self.reserve(id);

        self.contains_key(&id)
    }

    pub fn free_all(&mut self) {
        let start = Instant::now();

        self.free.extend(self.reserved.drain());

        let elapsed = start.elapsed();
        self.free_time += elapsed;
    }

    pub fn clear(&mut self) {
        self.free.clear();
        self.reserved.clear();
        self.map.clear();
        self.size = 0;
    }
}

impl<Id: IdType> Drop for Cache<Id> {
    fn drop(&mut self) {
        info!("---- Cache insert_time: {:?}", self.insert_time);
        info!("---- Cache reserve_time: {:?}", self.reserve_time);
        info!("---- Cache free_time: {:?}", self.free_time);

        self.clear();
    }
}
