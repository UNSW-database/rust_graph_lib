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
use generic::{IdType, Iter};
use hashbrown::HashMap;
use hdfs::{HdfsFs, HdfsFsCache};
use io::csv::reader::parse_prop_map;
use io::csv::record::{EdgeRecord, NodeRecord, PropEdgeRecord, PropNodeRecord};
use io::csv::JsonValue;
use io::{ReadGraph, ReadGraphTo};
use serde::Deserialize;
use serde_json::to_value;
use std::cell::RefCell;
use std::rc::Rc;

#[derive(Debug, Default)]
pub struct HDFSReader<'a, Id: IdType, NL: Hash + Eq, EL: Hash + Eq = NL> {
    path_to_nodes: Vec<PathBuf>,
    path_to_edges: Vec<PathBuf>,
    separator: u8,
    has_headers: bool,
    // Whether the number of fields in records is allowed to change or not.
    is_flexible: bool,
    map: HashMap<String, HdfsFs<'a>>,
    _ph: PhantomData<(Id, NL, EL)>,
}

impl<'a, Id: IdType, NL: Hash + Eq, EL: Hash + Eq> Clone for HDFSReader<'a, Id, NL, EL> {
    fn clone(&self) -> Self {
        HDFSReader {
            path_to_nodes: self.path_to_nodes.clone(),
            path_to_edges: self.path_to_edges.clone(),
            separator: self.separator.clone(),
            has_headers: self.has_headers.clone(),
            is_flexible: self.is_flexible.clone(),
            map: HashMap::new(),
            _ph: PhantomData,
        }
    }
}

impl<'a, Id: IdType, NL: Hash + Eq, EL: Hash + Eq> HDFSReader<'a, Id, NL, EL> {
    /// **Note**: `path_to_nodes` or `path_to_edges` need to be formatted as `hdfs://localhost:9000/xx/xxx.csv`.
    pub fn new<P: AsRef<Path>>(path_to_nodes: Vec<P>, path_to_edges: Vec<P>) -> Self {
        let mut reader = HDFSReader {
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
            map: HashMap::new(),
            _ph: PhantomData,
        };
        //storing fs into map cache
        let hdfs_cache = Rc::new(RefCell::new(HdfsFsCache::new()));
        for path_to_node in reader.path_to_nodes.clone() {
            let str_node_path = path_to_node.as_path().to_str().unwrap();
            let fs: HdfsFs = hdfs_cache.borrow_mut().get(str_node_path).ok().unwrap();
            reader.map.insert(String::from(str_node_path), fs);
        }
        for path_to_edge in reader.path_to_edges.clone() {
            let str_edge_path = path_to_edge.as_path().to_str().unwrap();
            let fs: HdfsFs = hdfs_cache.borrow_mut().get(str_edge_path).ok().unwrap();
            reader.map.insert(String::from(str_edge_path), fs);
        }
        reader
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

impl<'a, Id: IdType, NL: Hash + Eq, EL: Hash + Eq> ReadGraph<Id, NL, EL>
    for HDFSReader<'a, Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    fn node_iter(&self) -> Iter<(Id, Option<NL>)> {
        let vec = self.path_to_nodes.clone();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        let iter = vec
            .into_iter()
            .map(move |path_to_nodes| {
                let str_node_path = path_to_nodes.as_path().to_str().unwrap();
                info!("Reading nodes from {}", str_node_path);
                let fs = self.map.get(str_node_path).unwrap();
                let node_file_reader = fs.open(str_node_path).unwrap();
                if !node_file_reader.is_readable() {
                    warn!("{:?} are not avaliable!", str_node_path);
                }

                ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_reader(node_file_reader)
            })
            .map(|rdr| {
                rdr.into_deserialize()
                    .enumerate()
                    .filter_map(|(i, result)| match result {
                        Ok(_result) => {
                            let record: NodeRecord<Id, NL> = _result;
                            Some((record.id, record.label))
                        }
                        Err(e) => {
                            warn!("Line {:?}: Error when reading csv: {:?}", i + 1, e);
                            None
                        }
                    })
            })
            .flat_map(|x| x);;
        Iter::new(Box::new(iter))
    }

    fn edge_iter(&self) -> Iter<(Id, Id, Option<EL>)> {
        let vec = self.path_to_edges.clone();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        let iter = vec
            .into_iter()
            .map(move |path_to_edges| {
                let str_edge_path = path_to_edges.as_path().to_str().unwrap();
                info!("Reading edges from {}", str_edge_path);
                let fs = self.map.get(str_edge_path).unwrap();
                let edge_file_reader = fs.open(str_edge_path).unwrap();
                if !edge_file_reader.is_readable() {
                    warn!("{:?} are not avaliable!", str_edge_path);
                }

                ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_reader(edge_file_reader)
            })
            .map(|rdr| {
                rdr.into_deserialize()
                    .enumerate()
                    .filter_map(|(i, result)| match result {
                        Ok(_result) => {
                            let record: EdgeRecord<Id, EL> = _result;
                            Some((record.src, record.dst, record.label))
                        }
                        Err(e) => {
                            warn!("Line {:?}: Error when reading csv: {:?}", i + 1, e);
                            None
                        }
                    })
            })
            .flat_map(|x| x);

        Iter::new(Box::new(iter))
    }

    fn prop_node_iter(&self) -> Iter<(Id, Option<NL>, JsonValue)> {
        assert!(self.has_headers);

        let vec = self.path_to_nodes.clone();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        let iter = vec
            .into_iter()
            .map(move |path_to_nodes| {
                let str_node_path = path_to_nodes.as_path().to_str().unwrap();
                info!("Reading nodes from {}", str_node_path);
                let fs = self.map.get(str_node_path).unwrap();
                let node_file_reader = fs.open(str_node_path).unwrap();
                if !node_file_reader.is_readable() {
                    warn!("{:?} are not avaliable!", str_node_path);
                }

                ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_reader(node_file_reader)
            })
            .map(|rdr| {
                rdr.into_deserialize().enumerate().map(|(i, result)| {
                    let mut record: PropNodeRecord<Id, NL> =
                        result.expect(&format!("Error when reading line {}", i + 1));

                    parse_prop_map(&mut record.properties);
                    let prop = to_value(record.properties)
                        .expect(&format!("Error when parsing line {} to Json", i + 1));

                    (record.id, record.label, prop)
                })
            })
            .flat_map(|x| x);

        Iter::new(Box::new(iter))
    }

    fn prop_edge_iter(&self) -> Iter<(Id, Id, Option<EL>, JsonValue)> {
        let vec = self.path_to_edges.clone();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        let iter = vec
            .into_iter()
            .map(move |path_to_edges| {
                let str_edge_path = path_to_edges.as_path().to_str().unwrap();
                info!("Reading edges from {}", str_edge_path);
                let fs = self.map.get(str_edge_path).unwrap();
                let edge_file_reader = fs.open(str_edge_path).unwrap();
                if !edge_file_reader.is_readable() {
                    warn!("{:?} are not avaliable!", str_edge_path);
                }

                ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_reader(edge_file_reader)
            })
            .map(|rdr| {
                rdr.into_deserialize().enumerate().map(|(i, result)| {
                    let mut record: PropEdgeRecord<Id, EL> =
                        result.expect(&format!("Error when reading line {}", i + 1));

                    parse_prop_map(&mut record.properties);
                    let prop = to_value(record.properties)
                        .expect(&format!("Error when parsing line {} to Json", i + 1));

                    (record.src, record.dst, record.label, prop)
                })
            })
            .flat_map(|x| x);

        Iter::new(Box::new(iter))
    }
}

impl<'a, Id: IdType, NL: Hash + Eq, EL: Hash + Eq> ReadGraphTo<Id, NL, EL>
    for HDFSReader<'a, Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
}
