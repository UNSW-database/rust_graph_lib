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
use std::hash::Hash;
use std::io::Result;
use std::marker::PhantomData;
use std::path::{Path, PathBuf};

use csv::ReaderBuilder;
use serde::Deserialize;

use generic::{IdType, Iter, MutGraphTrait};
use io::csv::record::{EdgeRecord, NodeRecord};

#[derive(Debug)]
pub struct CSVReader<'a, Id: IdType + 'a, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> {
    path_to_nodes: Option<PathBuf>,
    path_to_edges: PathBuf,
    separator: u8,
    has_headers: bool,
    // Whether the number of fields in records is allowed to change or not.
    is_flexible: bool,
    _ph: PhantomData<(&'a Id, &'a NL, &'a EL)>,
}

impl<'a, Id: IdType + 'a, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> Clone
    for CSVReader<'a, Id, NL, EL>
{
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

impl<'a, Id: IdType + 'a, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> CSVReader<'a, Id, NL, EL> {
    pub fn new<P: AsRef<Path>>(path_to_nodes: Option<P>, path_to_edges: P) -> Self {
        CSVReader {
            path_to_nodes: path_to_nodes.map(|x| x.as_ref().to_path_buf()),
            path_to_edges: path_to_edges.as_ref().to_path_buf(),
            separator: b',',
            has_headers: true,
            is_flexible: false,
            _ph: PhantomData,
        }
    }

    pub fn with_separator<P: AsRef<Path>>(
        path_to_nodes: Option<P>,
        path_to_edges: P,
        separator: &str,
    ) -> Self {
        let sep_string = match separator {
            "comma" => ",",
            "space" => " ",
            "tab" => "\t",
            other => other,
        };

        if sep_string.len() != 1 {
            panic!("Invalid separator {}.", sep_string);
        }

        CSVReader {
            path_to_nodes: path_to_nodes.map(|x| x.as_ref().to_path_buf()),
            path_to_edges: path_to_edges.as_ref().to_path_buf(),
            separator: sep_string.chars().next().unwrap() as u8,
            has_headers: true,
            is_flexible: false,
            _ph: PhantomData,
        }
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

impl<'a, Id: IdType + 'a, NL: Hash + Eq + 'a, EL: Hash + Eq + 'a> CSVReader<'a, Id, NL, EL>
where
    for<'de> Id: Deserialize<'de>,
    for<'de> NL: Deserialize<'de>,
    for<'de> EL: Deserialize<'de>,
{
    pub fn read<G: MutGraphTrait<Id, NL, EL, L>, L: IdType>(&self, g: &mut G) -> Result<()> {
        if let Some(ref path_to_nodes) = self.path_to_nodes {
            info!(
                "Adding nodes from {}",
                path_to_nodes.as_path().to_str().unwrap()
            );
            let rdr = ReaderBuilder::new()
                .has_headers(self.has_headers)
                .flexible(self.is_flexible)
                .delimiter(self.separator)
                .from_path(path_to_nodes.as_path())?;

            for result in rdr.into_deserialize() {
                match result {
                    Ok(_result) => {
                        let record: NodeRecord<Id, NL> = _result;
                        record.add_to_graph(g);
                    }
                    Err(e) => warn!("Error when reading csv: {:?}", e),
                }
            }
        }

        info!(
            "Adding edges from {}",
            self.path_to_edges.as_path().to_str().unwrap()
        );

        let rdr = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .flexible(self.is_flexible)
            .delimiter(self.separator)
            .from_path(self.path_to_edges.as_path())?;

        for result in rdr.into_deserialize() {
            match result {
                Ok(_result) => {
                    let record: EdgeRecord<Id, EL> = _result;
                    record.add_to_graph(g);
                }
                Err(e) => warn!("Error when reading csv: {:?}", e),
            }
        }

        Ok(())
    }

    pub fn node_iter(&self) -> Result<Iter<'a, (Id, Option<NL>)>> {
        if let Some(ref path_to_nodes) = self.path_to_nodes {
            info!(
                "Reading nodes from {}",
                path_to_nodes.as_path().to_str().unwrap()
            );
            let rdr = ReaderBuilder::new()
                .has_headers(self.has_headers)
                .flexible(self.is_flexible)
                .delimiter(self.separator)
                .from_path(path_to_nodes.as_path())?;

            let rdr = rdr.into_deserialize().filter_map(|result| match result {
                Ok(_result) => {
                    let record: NodeRecord<Id, NL> = _result;
                    Some((record.id, record.label))
                }
                Err(e) => {
                    warn!("Error when reading csv: {:?}", e);
                    None
                }
            });

            Ok(Iter::new(Box::new(rdr)))
        } else {
            Ok(Iter::empty())
        }
    }

    pub fn edge_iter(&self) -> Result<Iter<'a, (Id, Id, Option<EL>)>> {
        info!(
            "Reading edges from {}",
            self.path_to_edges.as_path().to_str().unwrap()
        );
        let rdr = ReaderBuilder::new()
            .has_headers(self.has_headers)
            .flexible(self.is_flexible)
            .delimiter(self.separator)
            .from_path(self.path_to_edges.as_path())?;

        let rdr = rdr.into_deserialize().filter_map(|result| match result {
            Ok(_result) => {
                let record: EdgeRecord<Id, EL> = _result;
                Some((record.start, record.target, record.label))
            }
            Err(e) => {
                warn!("Error when reading csv: {:?}", e);
                None
            }
        });

        Ok(Iter::new(Box::new(rdr)))
    }
}
