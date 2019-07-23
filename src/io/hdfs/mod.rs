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

use std::hash::Hash;
use std::path::Path;

use serde::{Deserialize, Serialize};
pub use serde_json::Value as JsonValue;

use generic::{IdType, MutGraphTrait};
use io::hdfs::reader::HDFSReader;
use io::read::Read;

pub fn read_from_hdfs<Id, NL, EL, G, P>(
    g: &mut G,
    path_to_nodes: Vec<P>,
    path_to_edges: Vec<P>,
    separator: Option<&str>,
    has_headers: bool,
    is_flexible: bool,
) where
    for<'de> Id: IdType + Serialize + Deserialize<'de>,
    for<'de> NL: Hash + Eq + Serialize + Deserialize<'de>,
    for<'de> EL: Hash + Eq + Serialize + Deserialize<'de>,
    G: MutGraphTrait<Id, NL, EL>,
    P: AsRef<Path>,
{
    let mut reader = HDFSReader::new(path_to_nodes, path_to_edges)
        .headers(has_headers)
        .flexible(is_flexible);

    if let Some(sep) = separator {
        reader = reader.with_separator(sep);
    }

    reader.read(g)
}
