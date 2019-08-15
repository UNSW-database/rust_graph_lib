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
use crate::io::csv::CSVReader;
use crate::io::GraphLoader;
use itertools::Itertools;
use serde::{Deserialize, Serialize};
use serde_json::Map;
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

impl<'a, Id: IdType, NL: Hash + Eq, EL: Hash + Eq> GraphLoader<'a, Id, NL, EL> for TikvLoader
where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    ///loading graph into tikv
    fn load(&self, reader: CSVReader<'a, Id, NL, EL>) {
        futures::executor::block_on(async {
            let client = Client::new(self.node_property_config.clone()).unwrap();
            let nodes = reader
                .prop_node_iter()
                .map(|mut x| {
                    let mut default_map = Map::new();
                    let props_map = x.2.as_object_mut().unwrap_or(&mut default_map);
                    if x.1.is_some() {
                        let label = serde_json::to_value(x.1.unwrap());
                        if label.is_ok() {
                            props_map.insert(String::from(":LABEL"), label.unwrap());
                        }
                    }
                    (
                        bincode::serialize(&(x.0)).unwrap(),
                        serde_json::to_string(props_map).unwrap(),
                    )
                })
                .collect_vec();
            client
                .batch_put(nodes)
                .await
                .expect("Insert node property failed!");
        });

        futures::executor::block_on(async {
            let client = Client::new(self.edge_property_config.clone()).unwrap();
            let edges = reader
                .prop_edge_iter()
                .map(|mut x| {
                    let mut default_map = Map::new();
                    let props_map = x.3.as_object_mut().unwrap_or(&mut default_map);
                    if x.2.is_some() {
                        let label = serde_json::to_value(x.2.unwrap());
                        if label.is_ok() {
                            props_map.insert(String::from(":LABEL"), label.unwrap());
                        }
                    }
                    (
                        bincode::serialize(&(x.0, x.1)).unwrap(),
                        serde_json::to_string(props_map).unwrap(),
                    )
                })
                .collect_vec();
            client
                .batch_put(edges)
                .await
                .expect("Insert edge property failed!");
        });
    }
}
