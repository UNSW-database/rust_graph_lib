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

use tikv_client::{raw::Client, ColumnFamily, Config, Error, KvPair, RawClient, ToOwnedRange};

use crate::generic::{IdType, Iter};
use crate::itertools::Itertools;
use crate::property::{ExtendTikvEdgeTrait, ExtendTikvNodeTrait, PropertyError, PropertyGraph};
use futures::executor::block_on;
use tokio::runtime::Runtime;
//use property::{ExtendTikvEdgeTrait, ExtendTikvNodeTrait};
use futures::future::Either;
use futures::prelude::*;
use futures::StreamExt;
use std::future::Future;
use std::hash::{Hash, Hasher};

use fxhash::FxBuildHasher;
use indexmap::IndexSet;
use std::iter::FromIterator;

use crate::generic::{MapTrait, MutMapTrait};
//use crate::io::{Deserialize, Serialize};
use crate::generic;
use crate::io::Deserialize;
use crate::map::{SetMap, VecMap};

//use core::fmt::Display;
//use futures::core_reexport::fmt::Display;
//use serde::export::fmt::Display;
use std::fmt::{Debug, Display};

const MAX_PREFIX_SCAN_LIMIT: u32 = 10240;

pub struct TikvProperty<EL: Hash + Eq + Serialize + DeserializeOwned> {
    node_client: Client,
    edge_client: Client,
    rt: Option<Runtime>,
    is_directed: bool,
    read_only: bool,
    label_map: SetMap<EL>,
}

impl<EL: Hash + Eq + Serialize + DeserializeOwned> TikvProperty<EL> {
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
            label_map: SetMap::new(),
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
            label_map: SetMap::new(),
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

impl<Id: IdType + Serialize + DeserializeOwned, EL: Hash + Eq + Serialize + DeserializeOwned>
    PropertyGraph<Id> for TikvProperty<EL>
{
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
        props: I,
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

impl<Id: IdType + Serialize + DeserializeOwned, EL: Hash + Eq + Serialize + DeserializeOwned>
    ExtendTikvEdgeTrait<Id, EL> for TikvProperty<EL>
{
    fn insert_labeled_edge_property(
        &mut self,
        src: Id,
        dst: Id,
        label: EL,
        direction: bool,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let names_bytes = to_vec(&prop)?;
        self.insert_labeled_edge_raw(src, dst, label, direction, names_bytes)
    }

    fn insert_labeled_edge_raw(
        &mut self,
        mut src: Id,
        mut dst: Id,
        label: EL,
        direction: bool,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        if direction == true {
            self.is_directed = true;
        }

        self.swap_edge(&mut src, &mut dst);

        let label_id = self.label_map.add_item(label);
        //self.insert_labeled_edge_raw(src, dst, label, direction, prop);

        let id_bytes = bincode::serialize(&(src, direction, label_id, dst))?;

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

    fn get_edge_property_all_with_label(
        &mut self,
        src: Id,
        dst: Id,
        label: EL,
        direction: bool,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let label_id = self.label_map.add_item(label);
        let key = bincode::serialize(&(src, direction, label_id, dst))?;
        //let id_bytes = bincode::serialize(&id)?;
        self.get_property_all(key, true)
    }
}

impl<Id: IdType + Serialize + DeserializeOwned, EL: Hash + Eq + Serialize + DeserializeOwned>
    ExtendTikvNodeTrait<Id, EL> for TikvProperty<EL>
{
    fn insert_labeled_node_property(
        &mut self,
        id: Id,
        label: EL,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let names_bytes = to_vec(&prop).unwrap();
        self.insert_labeled_node_raw(id, label, names_bytes)
    }

    fn insert_labeled_node_raw(
        &mut self,
        id: Id,
        label: EL,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }
        let label_id = self.label_map.add_item(label);
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

    fn get_node_property_all_with_label(
        &mut self,
        id: Id,
        label: EL,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let label_id = self.label_map.add_item(label);
        let key = bincode::serialize(&(id, label_id))?;
        //let id_bytes = bincode::serialize(&id)?;
        self.get_property_all(key, true)
    }
}

#[cfg(test)]
mod test {
    extern crate tikv_client;

    use super::*;
    use serde_json::json;

    //    const NODE_PD_SERVER_ADDR: &str = "59.78.194.63:2379";
    //    const EDGE_PD_SERVER_ADDR: &str = "59.78.194.63:2379";
    const NODE_PD_SERVER_ADDR: &str = "127.0.0.1:2379";
    const EDGE_PD_SERVER_ADDR: &str = "127.0.0.1:2379";

    #[test]
    fn test_find_neighbors() {
        let mut graph = TikvProperty::new(
            Config::new(vec![NODE_PD_SERVER_ADDR.to_owned()]),
            Config::new(vec![EDGE_PD_SERVER_ADDR.to_owned()]),
            false,
        )
        .unwrap();

        let new_prop1 = json!({"edge":"eight to four,label one"});
        let raw_prop1 = to_vec(&new_prop1).unwrap();

        graph
            .insert_labeled_edge_raw(8u32, 4u32, 1, true, raw_prop1) // EL: 1
            .unwrap();

        let new_prop2 = json!({"edge":"eight to five,label two"});
        let raw_prop2 = to_vec(&new_prop2).unwrap();

        graph
            .insert_labeled_edge_raw(8u32, 5u32, 2, true, raw_prop2)
            .unwrap();

        let new_prop3 = json!({"edge":"nine to six,label three"});
        let raw_prop3 = to_vec(&new_prop3).unwrap();

        graph
            .insert_labeled_edge_raw(9u32, 6u32, 3, true, raw_prop3)
            .unwrap();

        let pairs_parsed = graph.find_neighbors(8u32, true, Some(1), Some(10)).unwrap();

        let mut expected_result = Vec::new();
        // expected_result.push(((8u32, true, &1, 4u32),json!({"edge":"eight to four,label one"})));
        expected_result.push((
            (8u32, true, &1, 4u32),
            json!({"edge":"eight to four,label one"}),
        ));

        assert_eq!(Some(expected_result), pairs_parsed);
    }

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

        graph.insert_labeled_node_raw(6u32, 1, raw_prop).unwrap();
        //let key = bincode::serialize(&(6u32, 1)).unwrap();
        let node_property = graph.get_node_property_all_with_label(6u32, 1).unwrap();

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

        graph
            .insert_labeled_edge_raw(4u32, 9u32, 1, true, raw_prop)
            .unwrap();

        let edge_property = graph
            .get_edge_property_all_with_label(4u32, 9u32, 1, true)
            .unwrap();

        assert_eq!(Some(json!({"name":"jackson"})), edge_property);
    }

    #[test]
    fn test_insert_raw_edge() {
        let mut graph = TikvProperty::<String>::new(
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
        let mut graph = TikvProperty::<String>::new(
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
        let mut graph = TikvProperty::<String>::new(
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
        let mut graph = TikvProperty::<String>::new(
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
        let mut graph = TikvProperty::<String>::new(
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
        let mut graph = TikvProperty::<String>::new(
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
        let mut graph = TikvProperty::<String>::new(
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
        let mut graph = TikvProperty::<String>::new(
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
            let mut graph0 = TikvProperty::<String>::new(
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

        let graph1 = TikvProperty::<String>::open(
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
            let mut graph0 = TikvProperty::<String>::new(
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
        let mut graph1 = TikvProperty::<String>::open(
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
            let mut graph0 = TikvProperty::<String>::new(
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

        let mut graph1 = TikvProperty::<String>::open(
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
        let mut graph = TikvProperty::<String>::new(
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
        let mut graph = TikvProperty::<String>::new(
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

pub trait PrefixScan<
    Id: IdType + Serialize + DeserializeOwned,
    EL: Hash + Eq + Serialize + DeserializeOwned + Display + Debug,
    LabelId: IdType = Id,
>
{
    /// find neighbors of src with given direction and label(Option)
    fn find_neighbors(
        &self,
        src: Id,
        direction: bool,
        label: Option<EL>,
        scan_limit: Option<u32>,
    ) -> Result<Option<Vec<((Id, bool, &EL, Id), JsonValue)>>, PropertyError>;
}

impl<
        Id: IdType + Serialize + DeserializeOwned,
        EL: Hash + Eq + Serialize + DeserializeOwned + Display + Debug,
    > PrefixScan<Id, EL> for TikvProperty<EL>
{
    fn find_neighbors(
        &self,
        src: Id,
        direction: bool,
        label: Option<EL>,
        scan_limit: Option<u32>,
    ) -> Result<Option<Vec<((Id, bool, &EL, Id), JsonValue)>>, PropertyError> {
        // if there's any, find certain labeled edges, or search through all labels.
        let (label_from, label_to) = if let Some(label) = label {
            let id = self.label_map.find_index(&label);
            //            println!("{:#?}", id);
            //            let label = self.label_map.get_item(id.unwrap());
            //            println!("{:#?}", label);
            if id == None {
                // Some(label).expect("There's no such label in the record!");
                return Err(PropertyError::NoLabelInMapError);
            }
            (id.unwrap(), id.unwrap())
        } else {
            (0, self.label_map.len())
        };

        //        println!("{:#?}", label_from);
        //        println!("{:#?}", label_to);
        // TODO: is usize range available?
        let left =
            bincode::serialize(&(src.id(), direction, label_from, usize::min_value())).unwrap();
        let right =
            bincode::serialize(&(src.id(), direction, label_to, usize::max_value())).unwrap();

        //        println!("{:#?}", left);
        //        println!("{:#?}", right);
        //println!("{:#?}", left..right);

        let limit = match scan_limit {
            Some(limit) => limit,
            None => 10240, //max scan limit is 10240
        };

        let client = self.edge_client.clone();
        block_on(async {
            //let client = RawClient::new(Config::default()).unwrap();
            let inclusive_range = left..=right;
            let req = client.scan(inclusive_range.to_owned(), limit);
            let kv_pairs = req.await?;
            if kv_pairs.is_empty() {
                Ok(None)
            } else {
                let mut pairs_parsed = Vec::new();
                for kv_pair in kv_pairs {
                    let key_parsed: (Id, bool, usize, Id) =
                        bincode::deserialize(kv_pair.key().into())?;
                    let label_id = key_parsed.2;
                    let label = self.label_map.get_item(label_id).unwrap();
                    let key: (Id, bool, &EL, Id) =
                        (key_parsed.0, key_parsed.1, label, key_parsed.3);
                    let value_parsed: JsonValue = from_slice(kv_pair.value().into())?;
                    pairs_parsed.push((key, value_parsed));
                }
                Ok(Some(pairs_parsed))
            }
        })
    }
}

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
