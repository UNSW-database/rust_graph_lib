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
use crate::generic::Iter;

pub trait MapTrait<L> {
    fn get_item(&self, id: usize) -> Option<&L>;
    fn find_index(&self, item: &L) -> Option<usize>;

    fn contains(&self, item: &L) -> bool;

    fn items(&self) -> Iter<&L>;
    fn items_vec(self) -> Vec<L>;

    fn len(&self) -> usize;

    #[inline(always)]
    fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

pub trait MutMapTrait<L> {
    /// Add a new item to the map and return its index
    fn add_item(&mut self, item: L) -> usize;

    fn pop_item(&mut self) -> Option<L>;
}
