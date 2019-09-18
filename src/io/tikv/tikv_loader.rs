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
use crate::io::csv::CborValue as Value;
use crate::io::tikv::{serialize_edge_item, serialize_node_item};
use crate::io::{GraphLoader, ReadGraph};
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use std::hash::Hash;
use std::sync::Arc;
use tikv_client::raw::Client;
use tikv_client::Config;
use tokio::runtime::Runtime;

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
        batch_size: usize,
    ) {
        let mut scope = scoped_threadpool::Pool::new(2);
        let rt = Arc::new(Runtime::new().unwrap());
        let node_runtime = rt.clone();
        let edge_runtime = rt.clone();
        scope.scoped(|scope| {
            scope.execute(|| {
                parallel_nodes_loading(
                    &self.node_property_config,
                    reader,
                    node_runtime,
                    thread_cnt,
                    batch_size,
                );
            });
            scope.execute(|| {
                parallel_edges_loading(
                    &self.edge_property_config,
                    reader,
                    edge_runtime,
                    thread_cnt,
                    batch_size,
                );
            });
        });
        Arc::try_unwrap(rt).unwrap().shutdown_on_idle();
    }
}

/// Deliver files for threads.
fn parallel_nodes_loading<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static>(
    node_config: &Config,
    reader: &'a (dyn ReadGraph<Id, NL, EL> + Sync),
    rt: Arc<Runtime>,
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
            let node_runtime = rt.clone();
            scope.execute(move || {
                for fid in start_index..start_index + batch_file_cnt {
                    if let Some(node_iter) = reader.get_prop_node_iter(fid) {
                        load_nodes_to_tikv(
                            node_config.clone(),
                            node_iter,
                            node_runtime.clone(),
                            batch_size,
                        );
                    }
                }
            })
        }
    });
}

fn parallel_edges_loading<'a, Id: IdType, NL: Hash + Eq + 'static, EL: Hash + Eq + 'static>(
    edge_config: &Config,
    reader: &'a (dyn ReadGraph<Id, NL, EL> + Sync),
    rt: Arc<Runtime>,
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
            let edge_runtime = rt.clone();
            scope.execute(move || {
                for fid in start_index..start_index + batch_file_cnt {
                    if let Some(edge_iter) = reader.get_prop_edge_iter(fid) {
                        load_edges_to_tikv(
                            edge_config.clone(),
                            edge_iter,
                            edge_runtime.clone(),
                            batch_size,
                        );
                    }
                }
            })
        }
    });
}

fn load_nodes_to_tikv<'a, Id: IdType, NL: Hash + Eq + 'static>(
    node_config: Config,
    node_iter: Iter<(Id, Option<NL>, Value)>,
    rt: Arc<Runtime>,
    batch_size: usize,
) where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
{
    let client = Client::new(node_config.clone()).unwrap();
    let chunks = node_iter.chunks(batch_size);
    for chunk in &chunks {
        let batch_nodes = chunk.map(|x| serialize_node_item(x)).collect_vec();
        let local_client = client.clone();
        rt.spawn(async move {
            let _ = local_client.batch_put(batch_nodes).await;
        });
    }
}

//https://github.com/tikv/tikv/issues/3611
fn load_edges_to_tikv<Id: IdType, EL: Hash + Eq + 'static>(
    edge_config: Config,
    edge_iter: Iter<(Id, Id, Option<EL>, Value)>,
    rt: Arc<Runtime>,
    batch_size: usize,
) where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    let client = Client::new(edge_config.clone()).unwrap();
    let chunks = edge_iter.chunks(batch_size);
    for chunk in &chunks {
        let batch_edges = chunk.map(|x| serialize_edge_item(x)).collect_vec();
        let local_client = client.clone();
        rt.spawn(async move {
            let _ = local_client.batch_put(batch_edges).await;
        });
    }
}
