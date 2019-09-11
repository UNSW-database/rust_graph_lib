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
use crate::generic::{IdType, Iter};
use crate::io::tikv::{serialize_edge_item, serialize_node_item};
use crate::io::{GraphLoader, ReadGraph};
use itertools::Itertools;
use rocksdb::Options;
use rocksdb::{WriteBatch, DB as Tree};
use serde::{Deserialize, Serialize};
use serde_cbor::Value;
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
        thread_cnt: usize,
        batch_size: usize,
    ) {
        let mut scope = scoped_threadpool::Pool::new(2);
        scope.scoped(|scope| {
            scope.execute(|| {
                parallel_nodes_loading(&self.node_property, reader, thread_cnt, batch_size);
            });
            scope.execute(|| {
                parallel_edges_loading(&self.edge_property, reader, thread_cnt, batch_size);
            });
        });
    }
}
fn parallel_nodes_loading<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static>(
    node_property: &Tree,
    reader: &'a (dyn ReadGraph<Id, NL, EL> + Sync),
    thread_cnt: usize,
    batch_size: usize,
) where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    let mut thread_pool = scoped_threadpool::Pool::new(thread_cnt as u32);
    let node_file_cnt = reader.num_of_node_files();
    thread_pool.scoped(|scope| {
        for tid in 0..thread_cnt {
            let batch_file_cnt = node_file_cnt / thread_cnt + 1;
            let start_index = tid * batch_file_cnt;
            scope.execute(move || {
                (start_index..start_index + batch_file_cnt).for_each(|fid| {
                    if let Some(node_iter) = reader.get_prop_node_iter(fid) {
                        load_nodes_to_tikv(node_property, node_iter, batch_size);
                    }
                });
            })
        }
    });
}

fn parallel_edges_loading<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static>(
    edge_property: &Tree,
    reader: &'a (dyn ReadGraph<Id, NL, EL> + Sync),
    thread_cnt: usize,
    batch_size: usize,
) where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    let mut thread_pool = scoped_threadpool::Pool::new(thread_cnt as u32);
    let edge_file_cnt = reader.num_of_edge_files();
    thread_pool.scoped(|scope| {
        for tid in 0..thread_cnt {
            let batch_file_cnt = edge_file_cnt / thread_cnt + 1;
            let start_index = tid * batch_file_cnt;
            scope.execute(move || {
                (start_index..start_index + batch_file_cnt).for_each(|fid| {
                    if let Some(edge_iter) = reader.get_prop_edge_iter(fid) {
                        load_edges_to_tikv(edge_property, edge_iter, batch_size);
                    }
                });
            })
        }
    });
}

fn load_nodes_to_tikv<'a, Id: IdType, NL: Hash + Eq + 'static>(
    node_property: &Tree,
    node_iter: Iter<(Id, Option<NL>, Value)>,
    batch_size: usize,
) where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
{
    let chunks = node_iter.chunks(batch_size);
    for chunk in &chunks {
        let mut batch = WriteBatch::default();
        for x in chunk {
            let pair = serialize_node_item(x);
            let _ = batch.put(pair.0, pair.1);
        }
        node_property
            .write(batch)
            .expect("Insert node property failed!");
    }
}

fn load_edges_to_tikv<Id: IdType, EL: Hash + Eq + 'static>(
    edge_property: &Tree,
    edge_iter: Iter<(Id, Id, Option<EL>, Value)>,
    batch_size: usize,
) where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    let chunks = edge_iter.chunks(batch_size);
    for chunk in &chunks {
        let mut batch = WriteBatch::default();
        for x in chunk {
            let pair = serialize_edge_item(x);
            let _ = batch.put(pair.0, pair.1);
        }
        edge_property
            .write(batch)
            .expect("Insert edges property failed!");
    }
}
