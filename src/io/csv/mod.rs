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
pub mod reader;
pub mod record;
pub mod writer;

use std::hash::Hash;
use std::io::Result;
use std::path::Path;

use serde::{Deserialize, Serialize};

use generic::{GeneralGraph, IdType, MutGraphTrait};
pub use io::csv::reader::GraphReader;
pub use io::csv::writer::GraphWriter;

pub fn write_to_csv<Id, NL, EL, P, L>(
    g: &GeneralGraph<Id, NL, EL, L>,
    path_to_nodes: P,
    path_to_edges: P,
) -> Result<()>
where
    Id: IdType + Serialize,
    NL: Hash + Eq + Serialize,
    EL: Hash + Eq + Serialize,
    L: IdType + Serialize,
    P: AsRef<Path>,
{
    GraphWriter::new(g, path_to_nodes, path_to_edges).write()
}

pub fn read_from_csv<Id, NL, EL, G, P>(
    g: &mut G,
    path_to_nodes: Option<P>,
    path_to_edges: P,
    separator: Option<&str>,
    has_headers: bool,
    is_flexible: bool,
) -> Result<()>
where
    for<'de> Id: IdType + Serialize + Deserialize<'de>,
    for<'de> NL: Hash + Eq + Serialize + Deserialize<'de>,
    for<'de> EL: Hash + Eq + Serialize + Deserialize<'de>,
    G: MutGraphTrait<Id, NL, EL>,
    P: AsRef<Path>,
{
    match separator {
        Some(sep) => GraphReader::with_separator(path_to_nodes, path_to_edges, sep)
            .headers(has_headers)
            .flexible(is_flexible)
            .read(g),
        None => GraphReader::new(path_to_nodes, path_to_edges)
            .headers(has_headers)
            .flexible(is_flexible)
            .read(g),
    }
}

//impl<Ty: GraphType, NL: Hash + Eq, EL: Hash + Eq> GraphReader<Ty, NL, EL> {
//    pub fn new<P: AsRef<Path>>(path_to_nodes: P, path_to_edges: P) -> Self {
