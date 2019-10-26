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

use crate::generic::{IdType, Iter};
use crate::io::csv::reader::parse_prop_map;
use crate::io::csv::record::{EdgeRecord, NodeRecord, PropEdgeRecord, PropNodeRecord};
use crate::io::csv::CborValue;
use crate::io::{ReadGraph, ReadGraphTo};
use csv::ReaderBuilder;
use hashbrown::HashMap;
use hdfs::{HdfsFs, HdfsFsCache};
use serde::Deserialize;
use serde_cbor::to_value;
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
                .flat_map(|p| list_hdfs_files(p))
                .collect(),
            path_to_edges: path_to_edges
                .into_iter()
                .flat_map(|p| list_hdfs_files(p))
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

impl<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static> ReadGraph<Id, NL, EL>
    for HDFSReader<'a, Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    fn get_node_iter(&self, idx: usize) -> Option<Iter<(Id, Option<NL>)>> {
        let node_file = self.path_to_nodes.get(idx).cloned();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        node_file
            .map(move |path_to_nodes| {
                let path_str = path_to_nodes.to_str().unwrap().to_owned();

                let fs = self.map.get(&path_str).unwrap();
                let node_file_reader = fs.open(&path_str).unwrap();
                if !node_file_reader.is_readable() {
                    warn!("{:?} are not avaliable!", &path_str);
                }

                let rdr = ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_reader(node_file_reader);

                (rdr, path_str)
            })
            .map(|(rdr, path)| {
                rdr.into_deserialize()
                    .enumerate()
                    .filter_map(move |(i, result)| {
                        if i == 0 {
                            info!("Reading nodes from {}", path);
                        }

                        match result {
                            Ok(_result) => {
                                let record: NodeRecord<Id, NL> = _result;

                                Some((record.id, record.label))
                            }
                            Err(e) => {
                                warn!("Line {:?}: Error when reading csv: {:?}", i + 1, e);

                                None
                            }
                        }
                    })
            })
            .map(|iter| Iter::new(Box::new(iter)))
    }

    fn get_edge_iter(&self, idx: usize) -> Option<Iter<(Id, Id, Option<EL>)>> {
        let edge_file = self.path_to_edges.get(idx).cloned();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        edge_file
            .map(move |path_to_edges| {
                let path_str = path_to_edges.to_str().unwrap().to_owned();

                let fs = self.map.get(&path_str).unwrap();
                let edge_file_reader = fs.open(&path_str).unwrap();
                if !edge_file_reader.is_readable() {
                    warn!("{:?} are not avaliable!", &path_str);
                }

                let rdr = ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_reader(edge_file_reader);

                (rdr, path_str)
            })
            .map(|(rdr, path)| {
                rdr.into_deserialize()
                    .enumerate()
                    .filter_map(move |(i, result)| {
                        if i == 0 {
                            info!("Reading edges from {}", path);
                        }

                        match result {
                            Ok(_result) => {
                                let record: EdgeRecord<Id, EL> = _result;

                                Some((record.src, record.dst, record.label))
                            }
                            Err(e) => {
                                warn!("Line {:?}: Error when reading csv: {:?}", i + 1, e);

                                None
                            }
                        }
                    })
            })
            .map(|iter| Iter::new(Box::new(iter)))
    }

    fn get_prop_node_iter(&self, idx: usize) -> Option<Iter<(Id, Option<NL>, CborValue)>> {
        assert!(self.has_headers);

        let node_file = self.path_to_nodes.get(idx).cloned();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        node_file
            .map(move |path_to_nodes| {
                let path_str = path_to_nodes.to_str().unwrap().to_owned();

                let fs = self.map.get(&path_str).unwrap();
                let node_file_reader = fs.open(&path_str).unwrap();
                if !node_file_reader.is_readable() {
                    warn!("{:?} are not avaliable!", &path_str);
                }

                let rdr = ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_reader(node_file_reader);

                (rdr, path_str)
            })
            .map(|(rdr, path)| {
                rdr.into_deserialize().enumerate().map(move |(i, result)| {
                    if i == 0 {
                        info!("Reading nodes from {}", path);
                    }

                    let mut record: PropNodeRecord<Id, NL> =
                        result.expect(&format!("Error when reading line {}", i + 1));

                    parse_prop_map(&mut record.properties);
                    let prop = to_value(record.properties)
                        .expect(&format!("Error when parsing line {} to Cbor", i + 1));

                    (record.id, record.label, prop)
                })
            })
            .map(|iter| Iter::new(Box::new(iter)))
    }

    fn get_prop_edge_iter(&self, idx: usize) -> Option<Iter<(Id, Id, Option<EL>, CborValue)>> {
        let edge_file = self.path_to_edges.get(idx).cloned();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        edge_file
            .map(move |path_to_edges| {
                let path_str = path_to_edges.to_str().unwrap().to_owned();

                let fs = self.map.get(&path_str).unwrap();
                let edge_file_reader = fs.open(&path_str).unwrap();
                if !edge_file_reader.is_readable() {
                    warn!("{:?} are not avaliable!", &path_str);
                }

                let rdr = ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_reader(edge_file_reader);

                (rdr, path_str)
            })
            .map(|(rdr, path)| {
                rdr.into_deserialize().enumerate().map(move |(i, result)| {
                    if i == 0 {
                        info!("Reading edges from {}", path);
                    }

                    let mut record: PropEdgeRecord<Id, EL> =
                        result.expect(&format!("Error when reading line {}", i + 1));

                    parse_prop_map(&mut record.properties);
                    let prop = to_value(record.properties)
                        .expect(&format!("Error when parsing line {} to Cbor", i + 1));

                    (record.src, record.dst, record.label, prop)
                })
            })
            .map(|iter| Iter::new(Box::new(iter)))
    }

    fn num_of_node_files(&self) -> usize {
        self.path_to_nodes.len()
    }

    fn num_of_edge_files(&self) -> usize {
        self.path_to_edges.len()
    }
}

impl<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static> ReadGraphTo<Id, NL, EL>
    for HDFSReader<'a, Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
}

/// enumerate files in a root directory `p`
fn list_hdfs_files<P: AsRef<Path>>(p: P) -> Vec<PathBuf> {
    let root_str_path = p.as_ref().to_str().unwrap();
    let hdfs_cache = Rc::new(RefCell::new(HdfsFsCache::new()));
    let fs: HdfsFs = hdfs_cache.borrow_mut().get(root_str_path).ok().unwrap();
    let root_file_status = fs.get_file_status(root_str_path);
    if root_file_status.is_err() {
        //open fail or other unknown error by libhdfs
        return vec![];
    }
    if root_file_status.unwrap().is_file() {
        //Path is a file
        return vec![p.as_ref().to_path_buf()];
    }

    //Directory Handler
    let mut pending_path = vec![root_str_path];
    let mut fold_path_vec = vec![];
    while pending_path.len() > 0 {
        let cur_file_path = pending_path.pop().unwrap();
        let file_status = fs.list_status(cur_file_path);
        if file_status.is_err() {
            //empty directory or other unknown error by libhdfs
            continue;
        }
        for status in file_status.unwrap().into_iter() {
            if status.is_directory() {
                pending_path.push(status.name());
            } else {
                fold_path_vec.push(Path::new(status.name()).to_path_buf());
            }
        }
    }

    fold_path_vec
}
