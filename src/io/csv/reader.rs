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
extern crate walkdir;

use std::collections::BTreeMap;
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

use self::walkdir::{DirEntry, WalkDir};
use csv::ReaderBuilder;
use generic::{IdType, Iter};
use io::csv::record::{EdgeRecord, NodeRecord, PropEdgeRecord, PropNodeRecord};
use io::csv::JsonValue;
use io::{ReadGraph, ReadGraphTo};
use itertools::Itertools;
use serde::Deserialize;
use serde_json::{from_str, to_value};

#[derive(Debug, Default)]
pub struct CSVReader<Id: IdType, NL: Hash + Eq, EL: Hash + Eq = NL> {
    path_to_nodes: Vec<PathBuf>,
    path_to_edges: Vec<PathBuf>,
    separator: u8,
    has_headers: bool,
    // Whether the number of fields in records is allowed to change or not.
    is_flexible: bool,
    _ph: PhantomData<(Id, NL, EL)>,
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> Clone for CSVReader<Id, NL, EL> {
    fn clone(&self) -> Self {
        CSVReader {
            path_to_nodes: self.path_to_nodes.clone(),
            path_to_edges: self.path_to_edges.clone(),
            separator: self.separator.clone(),
            has_headers: self.has_headers.clone(),
            is_flexible: self.is_flexible.clone(),
            _ph: PhantomData,
        }
    }
}

impl<Id: IdType, NL: Hash + Eq, EL: Hash + Eq> CSVReader<Id, NL, EL> {
    pub fn new<P: AsRef<Path>>(path_to_nodes: Vec<P>, path_to_edges: Vec<P>) -> Self {
        let mut path_to_nodes = path_to_nodes
            .into_iter()
            .flat_map(|p| list_files(p))
            .collect_vec();
        path_to_nodes.sort();

        let mut path_to_edges = path_to_edges
            .into_iter()
            .flat_map(|p| list_files(p))
            .collect_vec();
        path_to_edges.sort();

        CSVReader {
            path_to_nodes,
            path_to_edges,
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

impl<Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static> ReadGraph<Id, NL, EL>
    for CSVReader<Id, NL, EL>
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
                let str_node_path = path_to_nodes.as_path().to_str().unwrap();
                info!("Reading nodes from {}", str_node_path);

                ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_path(path_to_nodes.as_path())
                    .unwrap()
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
            .map(|iter| Iter::new(Box::new(iter)))
    }

    fn get_edge_iter(&self, idx: usize) -> Option<Iter<(Id, Id, Option<EL>)>> {
        let edge_file = self.path_to_edges.get(idx).cloned();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        edge_file
            .map(move |path_to_edges| {
                info!(
                    "Reading edges from {}",
                    path_to_edges.as_path().to_str().unwrap()
                );

                ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_path(path_to_edges.as_path())
                    .unwrap()
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
            .map(|iter| Iter::new(Box::new(iter)))
    }

    fn get_prop_node_iter(&self, idx: usize) -> Option<Iter<(Id, Option<NL>, JsonValue)>> {
        assert!(self.has_headers);

        let node_file = self.path_to_nodes.get(idx).cloned();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        node_file
            .map(move |path_to_nodes| {
                info!(
                    "Reading nodes from {}",
                    path_to_nodes.as_path().to_str().unwrap()
                );

                ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_path(path_to_nodes.as_path())
                    .unwrap()
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
            .map(|iter| Iter::new(Box::new(iter)))
    }

    fn get_prop_edge_iter(&self, idx: usize) -> Option<Iter<(Id, Id, Option<EL>, JsonValue)>> {
        assert!(self.has_headers);

        let edge_file = self.path_to_edges.get(idx).cloned();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        edge_file
            .map(move |path_to_edges| {
                info!(
                    "Reading edges from {}",
                    path_to_edges.as_path().to_str().unwrap()
                );

                ReaderBuilder::new()
                    .has_headers(has_headers)
                    .flexible(is_flexible)
                    .delimiter(separator)
                    .from_path(path_to_edges.as_path())
                    .unwrap()
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
            .map(|iter| Iter::new(Box::new(iter)))
    }

    fn num_of_node_files(&self) -> usize {
        self.path_to_nodes.len()
    }

    fn num_of_edge_files(&self) -> usize {
        self.path_to_edges.len()
    }
}

impl<Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static> ReadGraphTo<Id, NL, EL>
    for CSVReader<Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
}

pub fn parse_prop_map(props: &mut BTreeMap<String, JsonValue>) {
    for (_, json) in props.iter_mut() {
        if json.is_string() {
            let result = from_str::<JsonValue>(json.as_str().unwrap());
            if result.is_err() {
                continue;
            }

            let parsed = result.unwrap();

            *json = parsed
        }
    }
}

fn list_files<P: AsRef<Path>>(p: P) -> Vec<PathBuf> {
    let p = p.as_ref().to_path_buf();

    if p.is_dir() {
        let mut vec = WalkDir::new(p)
            .into_iter()
            .filter_entry(|e| !is_hidden(e))
            .filter_map(|e| e.ok())
            .map(|e| e.path().to_path_buf())
            .filter(|p| p.is_file())
            .collect::<Vec<_>>();
        vec.sort();

        vec
    } else {
        vec![p]
    }
}

fn is_hidden(entry: &DirEntry) -> bool {
    entry
        .file_name()
        .to_str()
        .map(|s| s.starts_with("."))
        .unwrap_or(false)
}
