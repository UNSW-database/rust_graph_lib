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
use std::hash::{Hash, Hasher};
use std::iter::FromIterator;

use fxhash::FxBuildHasher;
use indexmap::IndexSet;
use serde;

use crate::generic::{Iter, MapTrait, MutMapTrait};
use crate::io::serde::{Deserialize, Serialize};
use crate::map::VecMap;

type FxIndexSet<V> = IndexSet<V, FxBuildHasher>;

/// More efficient but less compact.
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct SetMap<L: Hash + Eq> {
    labels: FxIndexSet<L>,
}

impl<L: Hash + Eq> Serialize for SetMap<L> where L: serde::Serialize {}

impl<L: Hash + Eq> Deserialize for SetMap<L> where L: for<'de> serde::Deserialize<'de> {}

impl<L: Hash + Eq> SetMap<L> {
    pub fn new() -> Self {
        SetMap {
            labels: FxIndexSet::default(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        SetMap {
            labels: IndexSet::with_capacity_and_hasher(capacity, FxBuildHasher::default()),
        }
    }

    pub fn from_vec(vec: Vec<L>) -> Self {
        SetMap {
            labels: vec.into_iter().collect(),
        }
    }

    pub fn clear(&mut self) {
        self.labels.clear();
    }
}

impl<L: Hash + Eq> Default for SetMap<L> {
    fn default() -> Self {
        SetMap::new()
    }
}

impl<L: Hash + Eq> MapTrait<L> for SetMap<L> {
    /// *O(1)*
    #[inline]
    fn get_item(&self, id: usize) -> Option<&L> {
        self.labels.get_index(id)
    }

    /// *O(1)*
    #[inline]
    fn find_index(&self, item: &L) -> Option<usize> {
        match self.labels.get_full(item) {
            Some((i, _)) => Some(i),
            None => None,
        }
    }

    /// *O(1)*
    #[inline]
    fn contains(&self, item: &L) -> bool {
        self.labels.contains(item)
    }

    #[inline]
    fn items<'a>(&'a self) -> Iter<'a, &L> {
        Iter::new(Box::new(self.labels.iter()))
    }

    #[inline]
    fn items_vec(self) -> Vec<L> {
        self.labels.into_iter().collect()
    }

    /// *O(1)*
    #[inline]
    fn len(&self) -> usize {
        self.labels.len()
    }
}

impl<L: Hash + Eq> MutMapTrait<L> for SetMap<L> {
    /// *O(1)*
    #[inline]
    fn add_item(&mut self, item: L) -> usize {
        if self.labels.contains(&item) {
            self.labels.get_full(&item).unwrap().0
        } else {
            self.labels.insert(item);

            self.len() - 1
        }
    }

    /// *O(1)*
    #[inline]
    fn pop_item(&mut self) -> Option<L> {
        self.labels.pop()
    }
}

impl<L: Hash + Eq> Hash for SetMap<L> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for l in self.items() {
            l.hash(state);
        }
    }
}

impl<L: Hash + Eq> FromIterator<L> for SetMap<L> {
    fn from_iter<T: IntoIterator<Item = L>>(iter: T) -> Self {
        let mut map = SetMap::new();

        for i in iter {
            map.add_item(i);
        }

        map
    }
}

impl<L: Hash + Eq> From<Vec<L>> for SetMap<L> {
    fn from(vec: Vec<L>) -> Self {
        SetMap::from_vec(vec)
    }
}

impl<'a, L: Hash + Eq + Clone> From<&'a Vec<L>> for SetMap<L> {
    fn from(vec: &'a Vec<L>) -> Self {
        SetMap::from_vec(vec.clone())
    }
}

impl<L: Hash + Eq> From<VecMap<L>> for SetMap<L> {
    fn from(vec_map: VecMap<L>) -> Self {
        let data = vec_map.items_vec();

        SetMap::from_vec(data)
    }
}

impl<'a, L: Hash + Eq + Clone> From<&'a VecMap<L>> for SetMap<L> {
    fn from(vec_map: &'a VecMap<L>) -> Self {
        let data = vec_map.clone().items_vec();

        SetMap::from_vec(data)
    }
}

#[macro_export]
macro_rules! setmap {
    ( $( $x:expr ),* ) => {
        {
            let mut temp_map = SetMap::new();
            $(
                temp_map.add_item($x);
            )*
            temp_map
        }
    };
}
