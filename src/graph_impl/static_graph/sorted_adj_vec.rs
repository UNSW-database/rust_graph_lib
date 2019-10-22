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

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct SortedAdjVec<Id: IdType> {
    label_or_type_offsets: Vec<usize>,
    neighbour_ids: Vec<Id>,
}

impl<Id: IdType> SortedAdjVec<Id> {
    pub fn new(offset: Vec<usize>) -> Self {
        let len = offset[offset.len() - 1];
        Self {
            label_or_type_offsets: offset,
            neighbour_ids: vec![IdType::new(0); len],
        }
    }

    pub fn get_neighbor_id(&self, idx: Id) -> Id {
        self.neighbour_ids[idx.id()]
    }

    pub fn set_neighbor_id(&mut self, neighbor_id: Id, idx: usize) {
        self.neighbour_ids[idx] = neighbor_id
    }

    pub fn get_offsets(&self) -> &Vec<usize> {
        self.label_or_type_offsets.as_ref()
    }

    pub fn get_neighbour_ids(&self) -> &Vec<Id> {
        self.neighbour_ids.as_ref()
    }

    pub fn sort(&mut self) {
        for i in 0..self.label_or_type_offsets.len() - 1 {
            let block = self.neighbour_ids
                [self.label_or_type_offsets[i]..self.label_or_type_offsets[i + 1]]
                .as_mut();
            block.sort();
        }
    }
}
