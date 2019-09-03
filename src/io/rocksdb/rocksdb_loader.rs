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
use crate::generic::IdType;
use crate::io::{GraphLoader, ReadGraph};
use itertools::Itertools;
use rocksdb::Options;
use rocksdb::{WriteBatch, DB as Tree};
use serde::{Deserialize, Serialize};
use serde_cbor::{to_value, Value};
use std::collections::BTreeMap;
use std::hash::Hash;
use std::path::Path;

#[derive(Debug)]
pub struct RocksDBLoader {
    node_property: Tree,
    edge_property: Tree,
    is_directed: bool,
}

impl RocksDBLoader {
    pub fn new(node_path: &Path, edge_path: &Path, is_directed: bool) -> Self {
        Tree::destroy(&Options::default(), &node_path).expect("Destroy node db failed");
        Tree::destroy(&Options::default(), &edge_path).expect("Destroy edge db failed");

        let mut opts = Options::default();
        opts.create_if_missing(true);

        let node_tree = Tree::open(&opts, node_path).expect("Open node db failed");
        let edge_tree = Tree::open(&opts, edge_path).expect("Open edge db failed");
        RocksDBLoader {
            node_property: node_tree,
            is_directed,
            edge_property: edge_tree,
        }
    }
}

impl<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static> GraphLoader<'a, Id, NL, EL>
    for RocksDBLoader
where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    ///loading graph into tikv
    fn load(
        &self,
        reader: &'a (dyn ReadGraph<Id, NL, EL> + Sync),
        _thread_cnt: usize,
        batch_size: usize,
    ) {
        let chunks = reader.prop_node_iter().chunks(batch_size);
        for chunk in &chunks {
            let mut batch = WriteBatch::default();
            for mut x in chunk {
                let mut default_map = BTreeMap::new();
                let props_map = x.2.as_object_mut().unwrap_or(&mut default_map);
                if x.1.is_some() {
                    let label = to_value(x.1.unwrap());
                    if label.is_ok() {
                        let _ = batch.put(
                            bincode::serialize(&x.0).unwrap(),
                            serde_cbor::to_vec(&(Some(label.unwrap()), props_map)).unwrap(),
                        );
                        continue;
                    }
                }
                let _ = batch.put(
                    bincode::serialize(&(x.0)).unwrap(),
                    serde_cbor::to_vec(&(Option::<Value>::None, props_map)).unwrap(),
                );
            }
            self.node_property
                .write(batch)
                .expect("Insert node property failed!");
        }

        let chunks = reader.prop_edge_iter().chunks(batch_size);
        for chunk in &chunks {
            let mut batch = WriteBatch::default();
            for mut x in chunk {
                let mut default_map = BTreeMap::new();
                let props_map = x.3.as_object_mut().unwrap_or(&mut default_map);
                if x.2.is_some() {
                    let label = to_value(x.2.unwrap());
                    if label.is_ok() {
                        let _ = batch.put(
                            bincode::serialize(&(x.0, x.1)).unwrap(),
                            serde_cbor::to_vec(&(Some(label.unwrap()), props_map)).unwrap(),
                        );
                        continue;
                    }
                }
                let _ = batch.put(
                    bincode::serialize(&(x.0, x.1)).unwrap(),
                    serde_cbor::to_vec(&(Option::<Value>::None, props_map)).unwrap(),
                );
            }
            self.node_property
                .write(batch)
                .expect("Insert edges property failed!");
        }
    }
}
