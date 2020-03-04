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
use graph_impl::multi_graph::plan::operator::extend::EI::Neighbours;
use itertools::Itertools;

#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct SortedAdjVec<Id: IdType> {
    label_offset: Vec<usize>,
    neighbour_ids: Vec<Id>,
}

impl<Id: IdType> SortedAdjVec<Id> {
    pub fn new(offset: Vec<usize>) -> Self {
        let len = offset[offset.len() - 1];
        Self {
            label_offset: offset,
            neighbour_ids: vec![IdType::new(0); len],
        }
    }

    pub fn get_neighbor_id(&self, idx: Id) -> Id {
        self.neighbour_ids[idx.id()]
    }

    pub fn set_neighbor_id(&mut self, neighbor_id: Id, idx: usize) {
        self.neighbour_ids[idx] = neighbor_id
    }

    pub fn set_neighbor_ids(&self, label_or_type: i32, neighbours: &mut Neighbours<Id>) {
        neighbours.ids = self.neighbour_ids.clone();
        neighbours.start_idx = self.label_offset[label_or_type as usize];
        neighbours.end_idx = self.label_offset[(label_or_type + 1) as usize];
    }

    pub fn get_offsets(&self) -> &Vec<usize> {
        self.label_offset.as_ref()
    }

    pub fn get_neighbor_ids(&self) -> &Vec<Id> {
        self.neighbour_ids.as_ref()
    }

    pub fn sort(&mut self) {
        for i in 0..self.label_offset.len() - 1 {
            let block = self.neighbour_ids[self.label_offset[i]..self.label_offset[i + 1]].as_mut();
            block.sort();
        }
    }

    pub fn intersect(
        &self,
        label_or_type: i32,
        some_neighbours: &mut Neighbours<Id>,
        neighbours: &mut Neighbours<Id>,
    ) -> usize {
        self.inner_intersect(
            some_neighbours,
            neighbours,
            &self.neighbour_ids,
            self.label_offset[label_or_type as usize],
            self.label_offset[(label_or_type + 1) as usize],
        );
        self.label_offset[(label_or_type + 1) as usize] - self.label_offset[label_or_type as usize]
    }

    fn inner_intersect(
        &self,
        some_neighbours: &mut Neighbours<Id>,
        neighbours: &mut Neighbours<Id>,
        neighbour_ids: &Vec<Id>,
        mut this_idx: usize,
        this_idx_end: usize,
    ) {
        neighbours.reset();
        let some_neighbour_ids = &some_neighbours.ids;
        let mut some_idx = some_neighbours.start_idx;
        let some_idx_end = some_neighbours.end_idx;
        while this_idx < this_idx_end && some_idx < some_idx_end {
            if neighbour_ids[this_idx] < some_neighbour_ids[some_idx] {
                this_idx += 1;
                while this_idx < this_idx_end
                    && neighbour_ids[this_idx] < some_neighbour_ids[some_idx]
                {
                    this_idx += 1;
                }
            } else if neighbour_ids[this_idx] > some_neighbour_ids[some_idx] {
                some_idx += 1;
                while some_idx < some_idx_end
                    && neighbour_ids[this_idx] > some_neighbour_ids[some_idx]
                {
                    some_idx += 1;
                }
            } else {
                neighbours.ids[neighbours.end_idx] = neighbour_ids[this_idx];
                neighbours.end_idx += 1;
                this_idx += 1;
                some_idx += 1;
            }
        }
    }
    pub fn len(&self) -> usize {
        self.neighbour_ids.len()
    }

    pub fn sub_len(&self, label: usize) -> usize {
        self.label_offset[label + 1] + self.label_offset[label]
    }
}
