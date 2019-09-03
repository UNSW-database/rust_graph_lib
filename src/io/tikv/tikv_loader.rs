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
use crate::io::{GraphLoader, ReadGraph};
use futures::executor::{block_on, ThreadPoolBuilder};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_cbor::{to_value, Value};
use std::collections::BTreeMap;
use std::hash::Hash;
use tikv_client::raw::Client;
use tikv_client::Config;

#[derive(Debug)]
pub struct TikvLoader {
    node_property_config: Config,
    edge_property_config: Config,
    is_directed: bool,
}

impl TikvLoader {
    pub fn new(
        node_property_config: Config,
        edge_property_config: Config,
        is_directed: bool,
    ) -> Self {
        TikvLoader {
            node_property_config,
            edge_property_config,
            is_directed,
        }
    }
}

impl<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static> GraphLoader<'a, Id, NL, EL>
    for TikvLoader
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
        sub_thread_cnt: usize,
        batch_size: usize,
    ) {
        let mut scope = scoped_threadpool::Pool::new(2);
        scope.scoped(|scope| {
            scope.execute(|| {
                println!("Node thread running...");
                parallel_nodes_loading(
                    &self.node_property_config,
                    reader,
                    thread_cnt,
                    sub_thread_cnt,
                    batch_size,
                );
            });
            scope.execute(|| {
                println!("Edge thread running...");
                parallel_edges_loading(
                    &self.edge_property_config,
                    reader,
                    thread_cnt,
                    sub_thread_cnt,
                    batch_size,
                );
            });
        });
    }
}

/// Deliver files for threads.
fn parallel_nodes_loading<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static>(
    node_config: &Config,
    reader: &'a (dyn ReadGraph<Id, NL, EL> + Sync),
    thread_cnt: usize,
    sub_thread_cnt: usize,
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
                        load_nodes_to_tikv(
                            node_config.clone(),
                            node_iter,
                            sub_thread_cnt,
                            batch_size,
                        );
                    }
                });
            })
        }
    });
}

fn parallel_edges_loading<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static>(
    edge_config: &Config,
    reader: &'a (dyn ReadGraph<Id, NL, EL> + Sync),
    thread_cnt: usize,
    sub_thread_cnt: usize,
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
                        load_edges_to_tikv(
                            edge_config.clone(),
                            edge_iter,
                            sub_thread_cnt,
                            batch_size,
                        );
                    }
                });
            })
        }
    });
}

fn load_nodes_to_tikv<'a, Id: IdType, NL: Hash + Eq + 'static>(
    node_property_config: Config,
    node_iter: Iter<(Id, Option<NL>, Value)>,
    sub_thread_cnt: usize,
    batch_size: usize,
) where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
{
    let mut _pool = ThreadPoolBuilder::new()
        .pool_size(sub_thread_cnt)
        .create()
        .unwrap();
    let chunks = node_iter.chunks(batch_size);
    let client = Client::new(node_property_config).unwrap();
    for chunk in &chunks {
        let batch_nodes = chunk
            .map(|mut x| {
                let mut default_map = BTreeMap::new();
                let props_map = x.2.as_object_mut().unwrap_or(&mut default_map);
                if x.1.is_some() {
                    let label = to_value(x.1.unwrap());
                    if label.is_ok() {
                        return (
                            bincode::serialize(&x.0).unwrap(),
                            serde_cbor::to_vec(&(Some(label.unwrap()), props_map)).unwrap(),
                        );
                    }
                }
                (
                    bincode::serialize(&(x.0)).unwrap(),
                    serde_cbor::to_vec(&(Option::<Value>::None, props_map)).unwrap(),
                )
            })
            .collect_vec();
        block_on(async {
            let _ = client.batch_put(batch_nodes).await;
        });
    }
}
//https://github.com/tikv/tikv/issues/3611
fn load_edges_to_tikv<Id: IdType, EL: Hash + Eq + 'static>(
    edge_config: Config,
    edge_iter: Iter<(Id, Id, Option<EL>, Value)>,
    sub_thread_cnt: usize,
    batch_size: usize,
) where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    let mut _pool = ThreadPoolBuilder::new()
        .pool_size(sub_thread_cnt)
        .create()
        .unwrap();
    let chunks = edge_iter.chunks(batch_size);
    let client = Client::new(edge_config).unwrap();
    for chunk in &chunks {
        let batch_edges = chunk
            .map(|mut x| {
                let mut default_map = BTreeMap::new();
                let props_map = x.3.as_object_mut().unwrap_or(&mut default_map);
                if let Some(l) = x.2 {
                    let label = to_value(l);
                    if label.is_ok() {
                        return (
                            bincode::serialize(&(x.0, x.1)).unwrap(),
                            serde_cbor::to_vec(&(Some(label.unwrap()), props_map)).unwrap(),
                        );
                    }
                }
                (
                    bincode::serialize(&(x.0, x.1)).unwrap(),
                    serde_cbor::to_vec(&(Option::<Value>::None, props_map)).unwrap(),
                )
            })
            .collect_vec();
        block_on(async {
            let _ = client.batch_put(batch_edges).await;
        });
    }
}
