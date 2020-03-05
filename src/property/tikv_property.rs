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
use std::mem::swap;

use crate::serde_json::Value as JsonValue;
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_cbor::{from_slice, to_vec};
use serde_json::to_value;


use tikv_client::{raw::Client, Config, KvPair, ColumnFamily, ToOwnedRange, RawClient};

use crate::generic::{IdType, Iter};
use crate::itertools::Itertools;
use crate::property::{PropertyError, PropertyGraph, ExtendTikvEdgeTrait, ExtendTikvNodeTrait};
use futures::executor::block_on;
use tokio::runtime::Runtime;
//use property::{ExtendTikvEdgeTrait, ExtendTikvNodeTrait};
use std::hash::{Hash, Hasher};
use std::future::Future;
use futures::future::Either;
use futures::StreamExt;
use futures::prelude::*;


use std::iter::FromIterator;
use fxhash::FxBuildHasher;
use indexmap::IndexSet;

use crate::generic::{MapTrait, MutMapTrait};
//use crate::io::{Deserialize, Serialize};
use crate::io::{Deserialize};
use crate::map::VecMap;

const MAX_RAW_KV_SCAN_LIMIT: u32 = 10240;

pub struct TikvProperty {
    node_client: Client,
    edge_client: Client,
    rt: Option<Runtime>,
    is_directed: bool,
    read_only: bool,
}

impl TikvProperty {
    /// New tikv-client with destroying all kv-pairs first if any
    pub fn new(
        node_property_config: Config,
        edge_property_config: Config,
        is_directed: bool,
    ) -> Result<Self, PropertyError> {
        let node_client =
            Client::new(node_property_config.clone()).expect("Connect to pd-server error!");
        let edge_client =
            Client::new(edge_property_config.clone()).expect("Connect to pd-server error!");
        let node_client_clone = node_client.clone();
        block_on(async move {
            node_client_clone
                .delete_range("".to_owned()..)
                .await
                .expect("Delete all node properties failed!");
        });

        let edge_client_clone = node_client.clone();
        block_on(async {
            edge_client_clone
                .delete_range("".to_owned()..)
                .await
                .expect("Delete all edge properties failed!");
        });

        let rt = Runtime::new().unwrap();
        Ok(TikvProperty {
            node_client,
            edge_client,
            rt: Some(rt),
            is_directed,
            read_only: false,
        })
    }

    pub fn open(
        node_property_config: Config,
        edge_property_config: Config,
        is_directed: bool,
        read_only: bool,
    ) -> Result<Self, PropertyError> {
        let rt = Runtime::new().unwrap();
        let node_client =
            Client::new(node_property_config.clone()).expect("Connect to pd-server error!");
        let edge_client =
            Client::new(edge_property_config.clone()).expect("Connect to pd-server error!");
        Ok(TikvProperty {
            node_client,
            edge_client,
            rt: Some(rt),
            is_directed,
            read_only,
        })
    }

    pub fn with_data<Id: IdType + Serialize + DeserializeOwned, N, E>(
        node_property_config: Config,
        edge_property_config: Config,
        node_property: N,
        edge_property: E,
        is_directed: bool,
    ) -> Result<Self, PropertyError>
    where
        N: Iterator<Item = (Id, JsonValue)>,
        E: Iterator<Item = ((Id, Id), JsonValue)>,
    {
        let mut prop = Self::new(node_property_config, edge_property_config, is_directed)?;
        prop.extend_node_property(node_property)?;
        prop.extend_edge_property(edge_property)?;

        Ok(prop)
    }

    #[inline(always)]
    fn swap_edge<Id: IdType>(&self, a: &mut Id, b: &mut Id) {
        if !self.is_directed && a > b {
            swap(a, b)
        }
    }

    fn get_property(
        &self,
        key: Vec<u8>,
        names: Vec<String>,
        is_node_property: bool,
    ) -> Result<Option<JsonValue>, PropertyError> {
        block_on(async {
            let client = if is_node_property {
                self.node_client.clone()
            } else {
                self.edge_client.clone()
            };
            let _value = client.get(key).await?;
            match _value {
                Some(value_bytes) => {
                    let value_parsed: JsonValue = from_slice((&value_bytes).into())?;
                    let mut result = BTreeMap::<String, JsonValue>::new();
                    for name in names {
                        if value_parsed.get(&name).is_some() {
                            result.insert(name.clone(), value_parsed[&name].clone());
                        }
                    }
                    Ok(Some(to_value(result)?))
                }
                None => Ok(None),
            }
        })
    }

    pub fn get_property_all(
        &self,
        key: Vec<u8>,
        is_node_property: bool,
    ) -> Result<Option<JsonValue>, PropertyError> {
        block_on(async {
            let client = if is_node_property {
                self.node_client.clone()
            } else {
                self.edge_client.clone()
            };
            let _value = client.get(key).await?;
            match _value {
                Some(value_bytes) => {
                    let value_parsed: JsonValue = from_slice((&value_bytes).into())?;
                    Ok(Some(value_parsed))
                }
                None => Ok(None),
            }
        })
    }

    #[inline]
    pub fn batch_get_node_property_all<Id: IdType + Serialize + DeserializeOwned>(
        &self,
        keys: Vec<Id>,
    ) -> Result<Option<Vec<(Id, JsonValue)>>, PropertyError> {
        let ids_bytes = keys
            .into_iter()
            .map(|x| bincode::serialize(&x).unwrap())
            .collect();
        self.batch_get_node_property(ids_bytes)
    }

    #[inline]
    pub fn batch_get_edge_property_all<Id: IdType + Serialize + DeserializeOwned>(
        &self,
        keys: Vec<(Id, Id)>,
    ) -> Result<Option<Vec<((Id, Id), JsonValue)>>, PropertyError> {
        let ids_bytes = keys
            .into_iter()
            .map(|x| {
                let (mut src, mut dst) = (x.0, x.1);
                self.swap_edge(&mut src, &mut dst);
                bincode::serialize(&(src, dst)).unwrap()
            })
            .collect();
        self.batch_get_edge_property(ids_bytes)
    }

    fn batch_get_node_property<Id: IdType + Serialize + DeserializeOwned>(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Option<Vec<(Id, JsonValue)>>, PropertyError> {
        block_on(async {
            let client = self.node_client.clone();
            let kv_pairs = client.batch_get(keys).await?;

            if kv_pairs.is_empty() {
                Ok(None)
            } else {
                let mut pairs_parsed = Vec::new();
                for kv_pair in kv_pairs {
                    let key_parsed = bincode::deserialize(kv_pair.key().into())?;
                    let value_parsed: JsonValue = from_slice(kv_pair.value().into())?;
                    pairs_parsed.push((key_parsed, value_parsed));
                }
                Ok(Some(pairs_parsed))
            }
        })
    }

    pub fn batch_get_edge_property<Id: IdType + Serialize + DeserializeOwned>(
        &self,
        keys: Vec<Vec<u8>>,
    ) -> Result<Option<Vec<((Id, Id), JsonValue)>>, PropertyError> {
        block_on(async {
            let client = self.edge_client.clone();
            let kv_pairs = client.batch_get(keys).await?;

            if kv_pairs.is_empty() {
                Ok(None)
            } else {
                let mut pairs_parsed = Vec::new();
                for kv_pair in kv_pairs {
                    let key_parsed = bincode::deserialize(kv_pair.key().into())?;
                    let value_parsed: JsonValue = from_slice(kv_pair.value().into())?;
                    pairs_parsed.push((key_parsed, value_parsed));
                }
                Ok(Some(pairs_parsed))
            }
        })
    }
}

impl<Id: IdType + Serialize + DeserializeOwned> PropertyGraph<Id> for TikvProperty {
    #[inline]
    fn get_node_property(
        &self,
        id: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let id_bytes = bincode::serialize(&id)?;
        self.get_property(id_bytes, names, true)
    }

    #[inline]
    fn get_edge_property(
        &self,
        mut src: Id,
        mut dst: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        self.swap_edge(&mut src, &mut dst);

        let id_bytes = bincode::serialize(&(src, dst))?;
        self.get_property(id_bytes, names, false)
    }

    #[inline]
    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>, PropertyError> {
        let id_bytes = bincode::serialize(&id)?;
        self.get_property_all(id_bytes, true)
    }

    #[inline]
    fn get_edge_property_all(
        &self,
        mut src: Id,
        mut dst: Id,
    ) -> Result<Option<JsonValue>, PropertyError> {
        self.swap_edge(&mut src, &mut dst);
        let id_bytes = bincode::serialize(&(src, dst))?;
        self.get_property_all(id_bytes, false)
    }

    fn insert_node_property(
        &mut self,
        id: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let names_bytes = to_vec(&prop)?;
        self.insert_node_raw(id, names_bytes)
    }

    fn insert_edge_property(
        &mut self,
        src: Id,
        dst: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let names_bytes = to_vec(&prop)?;
        self.insert_edge_raw(src, dst, names_bytes)
    }

    fn extend_node_property<I: IntoIterator<Item = (Id, JsonValue)>>(
        &mut self,
        props:  I,
    ) -> Result<(), PropertyError> {
        let props = props.into_iter().map(|x| (x.0, to_vec(&x.1).unwrap()));
        self.extend_node_raw(props)
    }

    fn extend_edge_property<I: IntoIterator<Item = ((Id, Id), JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        let props = props.into_iter().map(|x| (x.0, to_vec(&x.1).unwrap()));
        self.extend_edge_raw(props)
    }

    fn insert_node_raw(
        &mut self,
        id: Id,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        let id_bytes = bincode::serialize(&id)?;
        let value = self.get_node_property_all(id)?;

        let client = self.node_client.clone();
        block_on(async {
            client
                .put(id_bytes, prop)
                .await
                .expect("Insert node property failed!");
        });

        Ok(value)
    }

    fn insert_edge_raw(
        &mut self,
        mut src: Id,
        mut dst: Id,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        self.swap_edge(&mut src, &mut dst);

        let id_bytes = bincode::serialize(&(src, dst))?;
        let value = self.get_edge_property_all(src, dst)?;

        let client = self.edge_client.clone();
        block_on(async {
            client
                .put(id_bytes, prop)
                .await
                .expect("Insert edge property failed!");
        });

        Ok(value)
    }

    fn extend_node_raw<I: IntoIterator<Item = (Id, Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        let properties = props
            .into_iter()
            .map(|x| (bincode::serialize(&(x.0)).unwrap(), x.1))
            .collect_vec();

        let client = self.node_client.clone();
        self.rt.as_ref().unwrap().spawn(async move {
            client
                .batch_put(properties)
                .await
                .expect("Batch insert node property failed!");
        });

        Ok(())
    }

    fn extend_edge_raw<I: IntoIterator<Item = ((Id, Id), Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        let properties = props
            .into_iter()
            .map(|x| {
                let (mut src, mut dst) = x.0;
                self.swap_edge(&mut src, &mut dst);
                (bincode::serialize(&(src, dst)).unwrap(), x.1)
            })
            .collect_vec();

        let client = self.edge_client.clone();
        self.rt.as_ref().unwrap().spawn(async move {
            client
                .batch_put(properties)
                .await
                .expect("Batch insert node property failed!");
        });

        Ok(())
    }

    fn scan_node_property_all(&self) -> Iter<Result<(Id, JsonValue), PropertyError>> {
        let client = self.node_client.clone();
        block_on(async {
            let result: Vec<KvPair> = client.scan("".to_owned().., 2).await.unwrap();

            Iter::new(Box::new(result.into_iter().map(|pair| {
                let (id_bytes, value_bytes) = (pair.key(), pair.value());
                let id: Id = bincode::deserialize(id_bytes.into())?;
                let value_parsed: JsonValue = from_slice(value_bytes.into())?;

                Ok((id, value_parsed))
            })))
        })
    }

    fn scan_edge_property_all(&self) -> Iter<Result<((Id, Id), JsonValue), PropertyError>> {
        let client = self.edge_client.clone();
        block_on(async {
            let result: Vec<KvPair> = client.scan("".to_owned().., 2).await.unwrap();

            Iter::new(Box::new(result.into_iter().map(|pair| {
                let (id_bytes, value_bytes) = (pair.key(), pair.value());
                let id: (Id, Id) = bincode::deserialize(id_bytes.into())?;
                let value_parsed: JsonValue = from_slice(value_bytes.into())?;

                Ok((id, value_parsed))
            })))
        })
    }
}


impl <Id: IdType + Serialize + DeserializeOwned, EL: Hash + Eq + Serialize + DeserializeOwned, LabelId>ExtendTikvEdgeTrait<Id,EL,LabelId> for TikvProperty {
    fn insert_labeled_edge_property(&mut self, src: Id, dst: Id, label: EL, direction: bool, prop: JsonValue) -> Result<Option<JsonValue>, PropertyError> {
        let names_bytes = to_vec(&prop)?;

        let mut map = TikvSetMap::new();
        let label_id = label.map(|x| LabelId::new(self.edge_label_map.add_item(x)));
        self.insert_labeled_edge_raw(src, dst, label_id, direction,names_bytes)
    }

//    fn get_labeled_edge_property(&self, src: Id, dst: Id, label: EL, direction: bool, names: Vec<String>) -> Result<Option<JsonValue>, PropertyError> {
//        //self.swap_edge(&mut src, &mut dst);
//        let id_bytes = bincode::serialize(&(src, dst, label, direction))?;
//        self.get_property(id_bytes, names, false)
//    }

    fn insert_labeled_edge_raw(&mut self, src: Id, dst: Id, label: EL, direction: bool, prop: Vec<u8>) -> Result<Option<JsonValue>, PropertyError>
    {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        //self.swap_edge(&mut src, &mut dst);

        self.is_directed = true;
        //self.insert_labeled_edge_raw(src, dst, label, direction, prop);

        let mut map = TikvSetMap::new();
        let label_id = label.map(|x| LabelId::new(self.edge_label_map.add_item(x)));

        let id_bytes = bincode::serialize(&(src, dst, label_id, direction))?;

        let value = self.get_edge_property_all(src, dst)?;

        let client = self.edge_client.clone();
        block_on(async {
            client
                .put(id_bytes, prop)
                .await
                .expect("Insert edge property failed!");
        });

        Ok(value)
    }
}


impl <Id: IdType + Serialize + DeserializeOwned, EL: Hash + Eq + Serialize + DeserializeOwned, LabelId>ExtendTikvNodeTrait<Id,EL,LabelId> for TikvProperty {
    fn insert_labeled_node_property(&mut self, id: Id, label: EL, prop: JsonValue) -> Result<Option<JsonValue>, PropertyError> {
        let names_bytes = to_vec(&prop).unwrap();

        let mut map = TikvSetMap::new();
        let label_id = label.map(|x| LabelId::new(self.edge_label_map.add_item(x)));
        self.insert_labeled_node_raw(id, label_id, names_bytes)
    }

//    fn get_labeled_node_property(&self, id: Id, label: EL, names: Vec<String>) -> Result<Option<JsonValue>, PropertyError> {
//        let id_bytes = bincode::serialize(&(id, label))?;
//        self.get_property(id_bytes, names, true)
//    }

    fn insert_labeled_node_raw(&mut self, id: Id, label: EL, prop: Vec<u8>) -> Result<Option<JsonValue>, PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }
        let mut map = TikvSetMap::new();
        let label_id = label.map(|x| LabelId::new(self.edge_label_map.add_item(x)));

        let id_bytes = bincode::serialize(&(id, label_id))?;
        let value = self.get_node_property_all(id)?;

        let client = self.node_client.clone();
        block_on(async {
            client
                .put(id_bytes, prop)
                .await
                .expect("Insert node property failed!");
        });

        Ok(value)
    }
}

//Finished all tasks which are still in tokio::Runtime
impl Drop for TikvProperty {
    fn drop(&mut self) {
        self.rt.take().unwrap().shutdown_on_idle();
    }
}

#[cfg(test)]
mod test {
    extern crate tikv_client;

    use super::*;
    use serde_json::json;

    const NODE_PD_SERVER_ADDR: &str = "59.78.194.63:2379";
    const EDGE_PD_SERVER_ADDR: &str = "59.78.194.63:2379";

    #[test]
    fn test_insert_labeled_raw_node() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
            .unwrap();

        let new_prop = json!({"name":"kat"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_labeled_node_raw(6u32,1, raw_prop).unwrap();
        let key = bincode::serialize(&(6u32, 1)).unwrap();
        let node_property = graph.get_property_all(key,true).unwrap();

        assert_eq!(Some(json!({"name":"kat"})), node_property);
    }

    #[test]
    fn test_insert_labeled_raw_edge() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
            .unwrap();

        let new_prop = json!({"name":"jackson"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_labeled_edge_raw(4u32,9u32, 1,true, raw_prop).unwrap();
        let key = bincode::serialize(&(4u32, 9u32, 1, true)).unwrap();
        let node_property = graph.get_property_all(key, false).unwrap();

        assert_eq!(Some(json!({"name":"jackson"})), node_property);
    }

    #[test]
    fn test_insert_property_labeled_node() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
            .unwrap();

        let new_prop = json!({"name":"jackson"});

        graph.insert_labeled_node_property(8u32, 4, new_prop).unwrap();
        let key = bincode::serialize(&(8u32, 4)).unwrap();
        let node_property = graph.get_property_all(key, true).unwrap();

        assert_eq!(Some(json!({"name":"jackson"})), node_property);
    }

    #[test]
    fn test_insert_property_labeled_edge() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
            .unwrap();

        let new_prop = json!({"length":"15"});

        graph.insert_labeled_edge_property(5u32, 6u32, 3, true, new_prop).unwrap();
        let key = bincode::serialize(&(5u32, 6u32, 3, true)).unwrap();
        let edge_property = graph.get_property_all(key, false).unwrap();

        assert_eq!(Some(json!({"length":"15"})), edge_property);
    }

    #[test]
    fn test_insert_raw_edge() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
            .unwrap();

        let new_prop = json!({"length":"15"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_edge_raw(0u32, 1u32, raw_prop).unwrap();
        let node_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), node_property);
    }


    #[test]
    fn test_insert_raw_node() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        let new_prop = json!({"length":"15"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_node_raw(0u32, raw_prop).unwrap();
        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), node_property);
    }

    #[test]
    fn test_insert_property_node() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        let new_prop = json!({"name":"jack"});

        graph.insert_node_property(0u32, new_prop).unwrap();
        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_insert_property_edge() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        let new_prop = json!({"length":"15"});

        graph.insert_edge_property(0u32, 1u32, new_prop).unwrap();
        let node_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), node_property);
    }

    #[test]
    fn test_extend_raw_node() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        let new_prop = json!({"name":"jack"});
        let raw_prop = to_vec(&new_prop).unwrap();
        let raw_properties = vec![(0u32, raw_prop)].into_iter();
        graph.extend_node_raw(raw_properties).unwrap();

        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_extend_raw_edge() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        let new_prop = json!({"length":"15"});
        let raw_prop = to_vec(&new_prop).unwrap();
        let raw_properties = vec![((0u32, 1u32), raw_prop)].into_iter();
        graph.extend_edge_raw(raw_properties).unwrap();
        let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), edge_property);
    }

    #[test]
    fn test_extend_property_node() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        let new_prop = json!({"name":"jack"});

        let properties = vec![(0u32, new_prop)].into_iter();
        graph.extend_node_property(properties).unwrap();

        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_extend_property_edge() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        let new_prop = json!({"length":"15"});

        let properties = vec![((0u32, 1u32), new_prop)].into_iter();
        graph.extend_edge_property(properties).unwrap();
        let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), edge_property);
    }

    #[test]
    fn test_open_existing_db() {
        {
            let mut graph0 = TikvProperty::new(
                Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
                Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
                false,
            )
            .unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }

        let graph1 = TikvProperty::open(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
            true,
        )
        .unwrap();

        assert_eq!(
            graph1.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    #[test]
    fn test_open_writable_db() {
        {
            let mut graph0 = TikvProperty::new(
                Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
                Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
                false,
            )
            .unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }
        let mut graph1 = TikvProperty::open(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
            false,
        )
        .unwrap();
        graph1
            .insert_node_property(1u32, json!({"name": "tom"}))
            .unwrap();
        assert_eq!(
            graph1.get_node_property_all(1u32).unwrap(),
            Some(json!({"name": "tom"}))
        );
    }

    #[test]
    fn test_open_readonly_db() {
        {
            let mut graph0 = TikvProperty::new(
                Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
                Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
                false,
            )
            .unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }

        let mut graph1 = TikvProperty::open(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
            true,
        )
        .unwrap();
        assert_eq!(
            graph1.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );

        let err = graph1
            .insert_node_property(1u32, json!({"name": "tom"}))
            .is_err();
        assert_eq!(err, true);
    }

    #[test]
    fn test_scan_node_property() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        graph
            .insert_node_property(0u32, json!({"name": "jack"}))
            .unwrap();

        graph
            .insert_node_property(1u32, json!({"name": "tom"}))
            .unwrap();

        let mut iter = graph.scan_node_property_all();
        assert_eq!(
            (0u32, json!({"name": "jack"})),
            iter.next().unwrap().unwrap()
        );
        assert_eq!(
            (1u32, json!({"name": "tom"})),
            iter.next().unwrap().unwrap()
        );
    }

    #[test]
    fn test_scan_edge_property() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        graph
            .insert_edge_property(0u32, 1u32, json!({"length": "5"}))
            .unwrap();


        graph
            .insert_edge_property(1u32, 2u32, json!({"length": "10"}))
            .unwrap();

        let mut iter = graph.scan_edge_property_all();
        assert_eq!(
            ((0u32, 1u32), json!({"length": "5"})),
            iter.next().unwrap().unwrap()
        );
        assert_eq!(
            ((1u32, 2u32), json!({"length": "10"})),
            iter.next().unwrap().unwrap()
        );
    }
}

pub trait PrefixScan<Id: IdType, EL: Hash + Eq, LabelId: IdType = Id> {
    fn find_neighbors(&self, s_id: Vec<u8>) -> impl Future<Output = Result<Vec<KvPair>>>;
}

impl <Id: IdType, EL: Hash + Eq, LabelId>PrefixScan for Client {
    fn find_neighbors(&self, s_id: Vec<u8>) -> impl Future<Output = Result<Vec<KvPair>>>
    {
        // encode range boundary
        //let inclusive_range = "TiKV"..="TiDB";
        let direction = false; // TODO what if directed edges search?
        let left = bincode::serialize(&(s_id, IdType::new(0), direction, IdType::new(0))).unwrap();
        let right = bincode::serialize(&(s_id, IdType::max_value(), direction, IdType::max_value())).unwrap();

        futures::executor::block_on(async {
            let client = RawClient::new(Config::default()).unwrap();
            let inclusive_range = left..=right;
            let req = client.scan(inclusive_range.to_owned(), 2);
            let results: Vec<KvPair> = req.await.unwrap();

            let mut edges: Vec<u8> = Vec::new();
            for result in results {
                edges.push(u8::from(result.value()));
            }
            // TODO take the last few bits of every edge
            // TODO deserialize -> t_ids
            // t_ids
        })
    }
}

type FxIndexSet<V> = IndexSet<V, FxBuildHasher>;
/// More efficient but less compact.
/// SetMap for Tikv
#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct TikvSetMap<L: Hash + Eq> {
    labels: FxIndexSet<L>,
}

impl<L: Hash + Eq> Serialize for TikvSetMap<L> where L: serde::Serialize {}

impl<L: Hash + Eq> Deserialize for TikvSetMap<L> where L: for<'de> serde::Deserialize<'de> {}

impl<L: Hash + Eq> TikvSetMap<L> {
    pub fn new() -> Self {
        TikvSetMap {
            labels: FxIndexSet::default(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        TikvSetMap {
            labels: IndexSet::with_capacity_and_hasher(capacity, FxBuildHasher::default()),
        }
    }

    pub fn from_vec(vec: Vec<L>) -> Self {
        TikvSetMap {
            labels: vec.into_iter().collect(),
        }
    }

    pub fn clear(&mut self) {
        self.labels.clear();
    }
}

impl<L: Hash + Eq> Default for TikvSetMap<L> {
    fn default() -> Self {
        TikvSetMap::new()
    }
}

impl<L: Hash + Eq> MapTrait<L> for TikvSetMap<L> {
    /// *O(1)*
    #[inline]
    fn get_item(&self, id: usize) -> Option<&L> {
        self.labels.get_index(id)
    }

    /// *O(1)*
    #[inline]
    fn find_index(&self, item: &L) -> Option<usize> {
        match self.labels.get_full(item) {
            Some((i, _)) => Some(i),
            None => None,
        }
    }

    /// *O(1)*
    #[inline]
    fn contains(&self, item: &L) -> bool {
        self.labels.contains(item)
    }

    #[inline]
    fn items<'a>(&'a self) -> Iter<'a, &L> {
        Iter::new(Box::new(self.labels.iter()))
    }

    #[inline]
    fn items_vec(self) -> Vec<L> {
        self.labels.into_iter().collect()
    }

    /// *O(1)*
    #[inline]
    fn len(&self) -> usize {
        self.labels.len()
    }
}

impl<L: Hash + Eq> MutMapTrait<L> for TikvSetMap<L> {
    /// *O(1)*
    #[inline]
    fn add_item(&mut self, item: L) -> usize {
        if self.labels.contains(&item) {
            self.labels.get_full(&item).unwrap().0 //returns index and value
        } else {
            self.labels.insert(item);

            self.len() - 1
        }
    }

    /// *O(1)*
    #[inline]
    fn pop_item(&mut self) -> Option<L> {
        self.labels.pop()
    }
}

impl<L: Hash + Eq> Hash for TikvSetMap<L> {
    fn hash<H: Hasher>(&self, state: &mut H) {
        for l in self.items() {
            l.hash(state);
        }
    }
}

impl<L: Hash + Eq> FromIterator<L> for TikvSetMap<L> {
    fn from_iter<T: IntoIterator<Item = L>>(iter: T) -> Self {
        let mut map = TikvSetMap::new();

        for i in iter {
            map.add_item(i);
        }

        map
    }
}

impl<L: Hash + Eq> From<Vec<L>> for TikvSetMap<L> {
    fn from(vec: Vec<L>) -> Self {
        TikvSetMap::from_vec(vec)
    }
}

impl<'a, L: Hash + Eq + Clone> From<&'a Vec<L>> for TikvSetMap<L> {
    fn from(vec: &'a Vec<L>) -> Self {
        TikvSetMap::from_vec(vec.clone())
    }
}

impl<L: Hash + Eq> From<VecMap<L>> for TikvSetMap<L> {
    fn from(vec_map: VecMap<L>) -> Self {
        let data = vec_map.items_vec();

        TikvSetMap::from_vec(data)
    }
}

impl<'a, L: Hash + Eq + Clone> From<&'a VecMap<L>> for TikvSetMap<L> {
    fn from(vec_map: &'a VecMap<L>) -> Self {
        let data = vec_map.clone().items_vec();

        TikvSetMap::from_vec(data)
    }
}


//#[macro_export]
//macro_rules! setmap {
//    ( $( $x:expr ),* ) => {
//        {
//            let mut temp_map = TikvSetMap::new();
//            $(
//                temp_map.add_item($x);
//            )*
//            temp_map
//        }
//    };
//}

//impl <Id: IdType, EL: Hash + Eq>PrefixScan for Client {
//    fn prefix_scan(&self, id_bytes: Vec<u8>) -> impl Future<Output = Result<Vec<_>>> {
//        let src_id: (Id, EL) = bincode::deserialize(id_bytes.into())?;
//
//        let client = self.edge_client.qwclone();
//        let result: Vec<KvPair> = client.scan("".to_owned().., 2).await.unwrap();
//
//        let edges: Vec<_> = Iter::new(Box::new(result.into_iter().map(|pair| {
//            let (id_bytes, value_bytes) = (pair.key(), pair.value());
//            let edges: (Id, Id, EL, bool) = bincode::deserialize(id_bytes.into())?;
//            Ok(edges)
//        })));
//
//        let neighbors = edges.into_iter().map(|edge| edge[1]).collect();
//
//        for edge in edges match src_id[0] == edge[1] {
//            //match the src
//            //Ok(edge_src_id) => {}
//            Ok(src_id) => neignbors.push(edge_src_ids[1]),
//            Err(_0) => {}
//            _ => {}
//        };
//
//        neighbors
//    }
//}





//pub fn new_raw_prefix_scan_request(
//    prefix: Vec<u8>,
//    //limit: u32,
//    //key_only: bool,
//    cf: Option<ColumnFamily>,
//) -> kvrpcpb::RawScanRequest {
//    let limit = bincode::SizeLimit::Bounded(20);
//    let decoded: decoded_keys = bincode::deserialize(&id_bytes[..]).unwrap();
//
//    //let (start_key, end_key) = range.into().into_keys();
//
//    let mut req = kvrpcpb::RawScanRequest::default();
//    req.set_start_key(start_key.into());
//    req.set_end_key(end_key.unwrap_or_default().into());
//    req.set_limit(limit);
//    //req.set_key_only(key_only);
//    req.maybe_set_cf(cf);
//
//    req
//}


