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
use std::collections::HashMap;
use std::path::PathBuf;

use csv::StringRecord;
use regex::Regex;

use generic::{GraphTrait, GraphType, IdType, MutGraphTrait};
use graph_impl::graph_map::TypedGraphMap;

#[derive(Debug)]
pub struct Node {
    name: String,
    id_index: usize,
    label_index: usize,
    file_name_start: Regex,
}

impl Node {
    pub fn new(name: &str, id_index: usize, label_index: usize, file_name_start: &str) -> Self {
        Node {
            name: name.to_owned(),
            id_index,
            label_index,
            file_name_start: Regex::new(&format!(r"^{}[_\d]*.csv", file_name_start)[..]).unwrap(),
        }
    }

    pub fn is_match(&self, path: &PathBuf) -> bool {
        let filename = path.as_path().file_name().unwrap().to_str().unwrap();

        self.file_name_start.is_match(filename)
    }

    pub fn add_node<Id: IdType, Ty: GraphType>(
        &self,
        record: StringRecord,
        g: &mut TypedGraphMap<Id, String, String, Ty>,
        node_id_map: &mut HashMap<String, Id>,
    ) {
        let str_id = self.name.clone() + &record[self.id_index];

        let id = *node_id_map.entry(str_id).or_insert_with(|| {
            if let Some(i) = g.max_seen_id() {
                i.increment()
            } else {
                Id::new(0)
            }
        });

        g.add_node(id, Some(record[self.label_index].to_owned()));
    }
}
