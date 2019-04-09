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
use std::path::PathBuf;

use csv::StringRecord;
use hashbrown::HashMap;
use regex::Regex;

use generic::{GraphTrait, GraphType, IdType, MutGraphTrait};
use graph_impl::graph_map::TypedGraphMap;

#[derive(Debug)]
pub struct Relation {
    start_label: String,
    target_label: String,
    edge_label: String,
    start_index: usize,
    target_index: usize,
    file_name_start: Regex,
}

impl Relation {
    pub fn new(
        start_label: &str,
        target_label: &str,
        edge_label: &str,
        start_index: usize,
        target_index: usize,
        file_name_start: &str,
    ) -> Self {
        Relation {
            start_label: start_label.to_owned(),
            target_label: target_label.to_owned(),
            edge_label: edge_label.to_owned(),
            start_index,
            target_index,
            file_name_start: Regex::new(&format!("{}{}", file_name_start, r"[_\d]*.csv")[..])
                .unwrap(),
        }
    }

    pub fn is_match(&self, path: &PathBuf) -> bool {
        let filename = path.as_path().file_name().unwrap().to_str().unwrap();

        self.file_name_start.is_match(filename)
    }

    pub fn add_edge<Id: IdType, Ty: GraphType>(
        &self,
        record: StringRecord,
        g: &mut TypedGraphMap<Id, String, String, Ty>,
        node_id_map: &mut HashMap<String, Id>,
    ) {
        let start_str_id = self.start_label.clone() + &record[self.start_index];
        let target_str_id = self.target_label.clone() + &record[self.target_index];

        let start_id = *node_id_map.entry(start_str_id).or_insert_with(|| {
            let i = if let Some(mut _i) = g.max_seen_id() {
                _i.increment();
                _i
            } else {
                Id::new(0)
            };
            g.add_node(i, Some(self.start_label.clone()));

            i
        });

        let target_id = *node_id_map.entry(target_str_id).or_insert_with(|| {
            let i = if let Some(mut _i) = g.max_seen_id() {
                _i.increment();
                _i
            } else {
                Id::new(0)
            };
            g.add_node(i, Some(self.target_label.clone()));

            i
        });

        g.add_edge(start_id, target_id, Some(self.edge_label.clone()));
    }
}
