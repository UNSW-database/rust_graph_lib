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
use crate::generic::IdType;
use crate::graph_impl::static_graph::EdgeVecTrait;

pub struct StaticEdgeIndexIter<'a, Id: IdType, L: IdType> {
    edge_vec: Box<&'a EdgeVecTrait<Id, L>>,
    curr_node: usize,
    curr_neighbor_index: usize,
    is_directed: bool,
}

impl<'a, Id: IdType, L: IdType> StaticEdgeIndexIter<'a, Id, L> {
    #[inline]
    pub fn new(edge_vec: Box<&'a EdgeVecTrait<Id, L>>, is_directed: bool) -> Self {
        StaticEdgeIndexIter {
            edge_vec,
            curr_node: 0,
            curr_neighbor_index: 0,
            is_directed,
        }
    }
}

impl<'a, Id: IdType, L: IdType> Iterator for StaticEdgeIndexIter<'a, Id, L> {
    type Item = (Id, Id);

    #[inline]
    fn next(&mut self) -> Option<Self::Item> {
        let mut node: usize;
        let mut neighbors: &[Id];

        loop {
            while self.edge_vec.has_node(Id::new(self.curr_node))
                && self.curr_neighbor_index >= self.edge_vec.degree(Id::new(self.curr_node))
            {
                self.curr_node += 1;
                self.curr_neighbor_index = 0;
            }

            node = self.curr_node;
            if !self.edge_vec.has_node(Id::new(node)) {
                return None;
            }

            neighbors = self.edge_vec.neighbors(Id::new(node));

            if !self.is_directed && neighbors[self.curr_neighbor_index] < Id::new(node) {
                match neighbors.binary_search(&Id::new(node)) {
                    Ok(index) => {
                        self.curr_neighbor_index = index;
                        break;
                    }
                    Err(index) => {
                        if index < neighbors.len() {
                            self.curr_neighbor_index = index;
                            break;
                        } else {
                            self.curr_node += 1;
                            self.curr_neighbor_index = 0;
                        }
                    }
                }
            } else {
                break;
            }
        }

        let neighbor = neighbors[self.curr_neighbor_index];
        let edge = (Id::new(node), neighbor);
        self.curr_neighbor_index += 1;

        Some(edge)
    }
}
