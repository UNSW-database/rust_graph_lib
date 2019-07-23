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
/// Nodes:
/// node_id <sep> node_label(optional)
///
/// Edges:
/// src <sep> dst <sep> edge_label(optional)
///
/// **Note**: Rows that are unable to parse will be skipped.
use std::hash::Hash;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use csv::ReaderBuilder;
use generic::{IdType, MutGraphTrait};
use hdfs::{HdfsFs, HdfsFsCache};
use io::csv::record::{EdgeRecord, NodeRecord};
use io::read::Read;
use serde::Deserialize;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug)]
pub struct HDFSReader<'a, Id: IdType, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a = NL> {
    path_to_nodes: Vec<PathBuf>,
    path_to_edges: Vec<PathBuf>,
    separator: u8,
    has_headers: bool,
    // Whether the number of fields in records is allowed to change or not.
    is_flexible: bool,
    _ph: PhantomData<(&'a Id, &'a NL, &'a EL)>,
}

impl<'a, Id: IdType, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> Clone for HDFSReader<'a, Id, NL, EL> {
    fn clone(&self) -> Self {
        HDFSReader {
            path_to_nodes: self.path_to_nodes.clone(),
            path_to_edges: self.path_to_edges.clone(),
            separator: self.separator.clone(),
            has_headers: self.has_headers.clone(),
            is_flexible: self.is_flexible.clone(),
            _ph: PhantomData,
        }
    }
}

impl<'a, Id: IdType, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> HDFSReader<'a, Id, NL, EL> {
    pub fn new<P: AsRef<Path>>(path_to_nodes: Vec<P>, path_to_edges: Vec<P>) -> Self {
        HDFSReader {
            path_to_nodes: path_to_nodes
                .into_iter()
                .map(|p| p.as_ref().to_path_buf())
                .collect(),
            path_to_edges: path_to_edges
                .into_iter()
                .map(|p| p.as_ref().to_path_buf())
                .collect(),
            separator: b',',
            has_headers: true,
            is_flexible: false,
            _ph: PhantomData,
        }
    }

    pub fn with_separator(mut self, separator: &str) -> Self {
        let sep_string = match separator {
            "comma" => ",",
            "space" => " ",
            "tab" => "\t",
            "bar" => "|",
            other => other,
        };

        if sep_string.len() != 1 {
            panic!("Invalid separator {}.", sep_string);
        }

        let sep = sep_string.chars().next().unwrap() as u8;
        self.separator = sep;

        self
    }

    pub fn headers(mut self, has_headers: bool) -> Self {
        self.has_headers = has_headers;
        self
    }

    pub fn flexible(mut self, is_flexible: bool) -> Self {
        self.is_flexible = is_flexible;
        self
    }
}

impl<'a, Id: IdType, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> Read<'a, Id, NL, EL>
    for HDFSReader<'a, Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    /// **Note**: `path_to_nodes` or `path_to_edges` need to be formatted as `hdfs://localhost:9000/xx/xxx.csv`.
    fn read<G: MutGraphTrait<Id, NL, EL, L>, L: IdType>(&self, g: &mut G) {
        let hdfs_cache = Rc::new(RefCell::new(HdfsFsCache::new()));
        for path in self.path_to_nodes.clone() {
            let str_node_path = path.as_path().to_str().unwrap();
            info!("Adding nodes from {}", str_node_path);
            let fs: HdfsFs = hdfs_cache.borrow_mut().get(str_node_path).ok().unwrap();
            let hfile = fs.open(str_node_path).unwrap();
            if !hfile.is_readable() {
                warn!("{:?} are not avaliable!", str_node_path);
            }
            let rdr = ReaderBuilder::new()
                .has_headers(self.has_headers)
                .flexible(self.is_flexible)
                .delimiter(self.separator)
                .from_reader(hfile);

            for (i, result) in rdr.into_deserialize().enumerate() {
                match result {
                    Ok(_result) => {
                        let record: NodeRecord<Id, NL> = _result;
                        record.add_to_graph(g);
                    }
                    Err(e) => warn!("Line {:?}: Error when reading csv: {:?}", i + 1, e),
                }
            }
        }

        for path in self.path_to_edges.clone() {
            let str_edge_path = path.as_path().to_str().unwrap();
            info!("Adding edges from {}", str_edge_path);
            let fs: HdfsFs = hdfs_cache.borrow_mut().get(str_edge_path).ok().unwrap();
            let hfile = fs.open(str_edge_path).unwrap();
            if !hfile.is_readable() {
                warn!("{:?} are not avaliable!", str_edge_path);
            }
            let rdr = ReaderBuilder::new()
                .has_headers(self.has_headers)
                .flexible(self.is_flexible)
                .delimiter(self.separator)
                .from_reader(hfile);

            for (i, result) in rdr.into_deserialize().enumerate() {
                match result {
                    Ok(_result) => {
                        let record: EdgeRecord<Id, EL> = _result;
                        record.add_to_graph(g);
                    }
                    Err(e) => warn!("Line {:?}: Error when reading csv: {:?}", i + 1, e),
                }
            }
        }
    }
}
