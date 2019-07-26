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
use std::hash::Hash;
use std::iter::FromIterator;

use serde;

use crate::generic::{Iter, MapTrait, MutMapTrait};
use crate::io::{Deserialize, Serialize};
use crate::map::SetMap;

/// Less efficient but more compact.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct VecMap<L> {
    labels: Vec<L>,
}

impl<L: Hash + Eq> Serialize for VecMap<L> where L: serde::Serialize {}

impl<L: Hash + Eq> Deserialize for VecMap<L> where L: for<'de> serde::Deserialize<'de> {}

impl<L> VecMap<L> {
    pub fn new() -> Self {
        VecMap {
            labels: Vec::<L>::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        VecMap {
            labels: Vec::<L>::with_capacity(capacity),
        }
    }

    pub fn from_vec(labels: Vec<L>) -> Self {
        VecMap { labels }
    }

    pub fn shrink_to_fit(&mut self) {
        self.labels.shrink_to_fit();
    }

    pub fn clear(&mut self) {
        self.labels.clear();
    }
}

impl<L> Default for VecMap<L> {
    fn default() -> Self {
        VecMap::new()
    }
}

impl<L: Eq> MapTrait<L> for VecMap<L> {
    /// *O(1)*
    #[inline]
    fn get_item(&self, id: usize) -> Option<&L> {
        self.labels.get(id)
    }

    /// *O(n)*
    #[inline]
    fn find_index(&self, item: &L) -> Option<usize> {
        for (i, elem) in self.labels.iter().enumerate() {
            if elem == item {
                return Some(i);
            }
        }

        None
    }

    /// *O(n)*
    #[inline]
    fn contains(&self, item: &L) -> bool {
        self.find_index(item).is_some()
    }

    #[inline]
    fn items(&self) -> Iter<&L> {
        Iter::new(Box::new(self.labels.iter()))
    }

    #[inline]
    fn items_vec(self) -> Vec<L> {
        self.labels
    }

    /// *O(1)*
    #[inline]
    fn len(&self) -> usize {
        self.labels.len()
    }
}

impl<L: Eq> MutMapTrait<L> for VecMap<L> {
    /// *O(n)*
    #[inline]
    fn add_item(&mut self, item: L) -> usize {
        match self.find_index(&item) {
            Some(i) => i,
            None => {
                self.labels.push(item);

                self.len() - 1
            }
        }
    }

    /// *O(1)*
    #[inline]
    fn pop_item(&mut self) -> Option<L> {
        self.labels.pop()
    }
}

impl<L: Eq> FromIterator<L> for VecMap<L> {
    fn from_iter<T: IntoIterator<Item = L>>(iter: T) -> Self {
        let mut map = VecMap::new();

        for i in iter {
            map.add_item(i);
        }

        map
    }
}

impl<L: Eq> From<Vec<L>> for VecMap<L> {
    fn from(vec: Vec<L>) -> Self {
        VecMap::from_vec(vec)
    }
}

impl<'a, L: Eq + Clone> From<&'a Vec<L>> for VecMap<L> {
    fn from(vec: &'a Vec<L>) -> Self {
        VecMap::from_vec(vec.clone())
    }
}

impl<L: Hash + Eq> From<SetMap<L>> for VecMap<L> {
    fn from(set_map: SetMap<L>) -> Self {
        let data = set_map.items_vec();

        VecMap::from_vec(data)
    }
}

impl<'a, L: Hash + Eq + Clone> From<&'a SetMap<L>> for VecMap<L> {
    fn from(set_map: &'a SetMap<L>) -> Self {
        let data = set_map.clone().items_vec();

        VecMap::from_vec(data)
    }
}

#[macro_export]
macro_rules! vecmap {
    ( $( $x:expr ),* ) => {
        {
            let mut temp_map = VecMap::new();
            $(
                temp_map.add_item($x);
            )*
            temp_map
        }
    };
}
