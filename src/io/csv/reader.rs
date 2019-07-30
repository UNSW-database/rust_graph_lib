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

use csv::ReaderBuilder;
use generic::{IdType, Iter, MutGraphTrait};
use io::csv::record::{EdgeRecord, NodeRecord, PropEdgeRecord, PropNodeRecord};
use io::csv::JsonValue;
use io::read_graph::ReadGraph;
use serde::Deserialize;
use serde_json::{from_str, to_value};

#[derive(Debug)]
pub struct CSVReader<'a, Id: IdType, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a = NL> {
    path_to_nodes: Vec<PathBuf>,
    path_to_edges: Vec<PathBuf>,
    separator: u8,
    has_headers: bool,
    // Whether the number of fields in records is allowed to change or not.
    is_flexible: bool,
    _ph: PhantomData<(&'a Id, &'a NL, &'a EL)>,
}

impl<'a, Id: IdType, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> Clone for CSVReader<'a, Id, NL, EL> {
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

impl<'a, Id: IdType, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> CSVReader<'a, Id, NL, EL> {
    pub fn new<P: AsRef<Path>>(path_to_nodes: Vec<P>, path_to_edges: Vec<P>) -> Self {
        CSVReader {
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

impl<'a, Id: IdType, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> ReadGraph<'a, Id, NL, EL>
    for CSVReader<'a, Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    fn read<G: MutGraphTrait<Id, NL, EL, L>, L: IdType>(&self, g: &mut G) {
        for (n, label) in self.node_iter() {
            g.add_node(n, label);
        }

        for (s, d, label) in self.edge_iter() {
            g.add_edge(s, d, label);
        }
    }

    fn node_iter(&'a self) -> Iter<'a, (Id, Option<NL>)> {
        let vec = self.path_to_nodes.clone();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        let iter = vec
            .into_iter()
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
            .flat_map(|x| x);

        Iter::new(Box::new(iter))
    }

    fn edge_iter(&'a self) -> Iter<'a, (Id, Id, Option<EL>)> {
        let vec = self.path_to_edges.clone();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        let iter = vec
            .into_iter()
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
            .flat_map(|x| x);

        Iter::new(Box::new(iter))
    }

    fn prop_node_iter(&'a self) -> Iter<'a, (Id, Option<NL>, JsonValue)> {
        assert!(self.has_headers);

        let vec = self.path_to_nodes.clone();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        let iter = vec
            .into_iter()
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
            .flat_map(|x| x);

        Iter::new(Box::new(iter))
    }

    fn prop_edge_iter(&'a self) -> Iter<'a, (Id, Id, Option<EL>, JsonValue)> {
        assert!(self.has_headers);

        let vec = self.path_to_edges.clone();
        let has_headers = self.has_headers;
        let is_flexible = self.is_flexible;
        let separator = self.separator;

        let iter = vec
            .into_iter()
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
            .flat_map(|x| x);

        Iter::new(Box::new(iter))
    }
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
