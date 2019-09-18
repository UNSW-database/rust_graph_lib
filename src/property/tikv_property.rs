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
use tikv_client::{raw::Client, Config};

use crate::generic::IdType;
use crate::itertools::Itertools;
use crate::property::{PropertyError, PropertyGraph};
use tokio::runtime::Runtime;

pub struct TikvProperty {
    client: Client,
    rt: Option<Runtime>,
    is_directed: bool,
    read_only: bool,
}

impl TikvProperty {
    /// New tikv-client with destroying all kv-pairs first if any
    pub fn new(client: Config, is_directed: bool) -> Result<Self, PropertyError> {
        let client = Client::new(client.clone()).expect("Connect to pd-server error!");
        let client_clone = client.clone();
        let rt = Runtime::new().unwrap();
        rt.block_on(async move {
            client_clone
                .delete_range("".to_owned()..)
                .await
                .expect("Delete all node properties failed!");
        });

        Ok(TikvProperty {
            client,
            rt: Some(rt),
            is_directed,
            read_only: false,
        })
    }

    pub fn open(
        client_config: Config,
        is_directed: bool,
        read_only: bool,
    ) -> Result<Self, PropertyError> {
        let rt = Runtime::new().unwrap();
        let client = Client::new(client_config.clone()).expect("Connect to pd-server error!");
        Ok(TikvProperty {
            client,
            rt: Some(rt),
            is_directed,
            read_only,
        })
    }

    pub fn with_data<Id: IdType + Serialize + DeserializeOwned, N, E>(
        client_config: Config,
        node_property: N,
        edge_property: E,
        is_directed: bool,
    ) -> Result<Self, PropertyError>
    where
        N: Iterator<Item = (Id, JsonValue)>,
        E: Iterator<Item = ((Id, Id), JsonValue)>,
    {
        let prop = Self::new(client_config, is_directed)?;
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
        self.rt.as_ref().unwrap().block_on(async {
            let client = if is_node_property {
                self.client.clone()
            } else {
                self.client.clone()
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
        self.rt.as_ref().unwrap().block_on(async {
            let client = if is_node_property {
                self.client.clone()
            } else {
                self.client.clone()
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
        self.rt.as_ref().unwrap().block_on(async {
            let client = self.client.clone();
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
        self.rt.as_ref().unwrap().block_on(async {
            let client = self.client.clone();
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
        &self,
        id: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let names_bytes = to_vec(&prop)?;
        self.insert_node_raw(id, names_bytes)
    }

    fn insert_edge_property(
        &self,
        src: Id,
        dst: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let names_bytes = to_vec(&prop)?;
        self.insert_edge_raw(src, dst, names_bytes)
    }

    fn extend_node_property<I: IntoIterator<Item = (Id, JsonValue)>>(
        &self,
        props: I,
    ) -> Result<(), PropertyError> {
        let props = props.into_iter().map(|x| (x.0, to_vec(&x.1).unwrap()));
        self.extend_node_raw(props)
    }

    fn extend_edge_property<I: IntoIterator<Item = ((Id, Id), JsonValue)>>(
        &self,
        props: I,
    ) -> Result<(), PropertyError> {
        let props = props.into_iter().map(|x| (x.0, to_vec(&x.1).unwrap()));
        self.extend_edge_raw(props)
    }

    fn insert_node_raw(&self, id: Id, prop: Vec<u8>) -> Result<Option<JsonValue>, PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        let id_bytes = bincode::serialize(&id)?;
        let value = self.get_node_property_all(id)?;

        let client = self.client.clone();
        self.rt.as_ref().unwrap().block_on(async {
            client
                .put(id_bytes, prop)
                .await
                .expect("Insert node property failed!");
        });

        Ok(value)
    }

    fn insert_edge_raw(
        &self,
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

        let client = self.client.clone();
        self.rt.as_ref().unwrap().block_on(async {
            client
                .put(id_bytes, prop)
                .await
                .expect("Insert edge property failed!");
        });

        Ok(value)
    }

    fn extend_node_raw<I: IntoIterator<Item = (Id, Vec<u8>)>>(
        &self,
        props: I,
    ) -> Result<(), PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        let properties = props
            .into_iter()
            .map(|x| (bincode::serialize(&(x.0)).unwrap(), x.1))
            .collect_vec();

        let client = self.client.clone();
        self.rt.as_ref().unwrap().spawn(async move {
            client
                .batch_put(properties)
                .await
                .expect("Batch insert node property failed!");
        });

        Ok(())
    }

    fn extend_edge_raw<I: IntoIterator<Item = ((Id, Id), Vec<u8>)>>(
        &self,
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

        let client = self.client.clone();
        self.rt.as_ref().unwrap().spawn(async move {
            client
                .batch_put(properties)
                .await
                .expect("Batch insert node property failed!");
        });

        Ok(())
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

    const PD_SERVER_ADDR: &str = "192.168.2.2:2379";

    #[test]
    fn test_insert_raw_node() {
        let mut graph =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        let new_prop = json!({"name":"jack"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_node_raw(0u32, raw_prop).unwrap();
        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_insert_raw_edge() {
        let mut graph =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        let new_prop = json!({"length":"15"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_edge_raw(0u32, 1u32, raw_prop).unwrap();
        let node_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), node_property);
    }

    #[test]
    fn test_insert_property_node() {
        let mut graph =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        let new_prop = json!({"name":"jack"});

        graph.insert_node_property(0u32, new_prop).unwrap();
        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_insert_property_edge() {
        let mut graph =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        let new_prop = json!({"length":"15"});

        graph.insert_edge_property(0u32, 1u32, new_prop).unwrap();
        let node_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), node_property);
    }

    #[test]
    fn test_extend_raw_node() {
        let mut graph =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        let new_prop = json!({"name":"jack"});
        let raw_prop = to_vec(&new_prop).unwrap();
        let raw_properties = vec![(0u32, raw_prop)].into_iter();
        graph.extend_node_raw(raw_properties).unwrap();

        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_extend_raw_edge() {
        let mut graph =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        let new_prop = json!({"length":"15"});
        let raw_prop = to_vec(&new_prop).unwrap();
        let raw_properties = vec![((0u32, 1u32), raw_prop)].into_iter();
        graph.extend_edge_raw(raw_properties).unwrap();
        let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), edge_property);
    }

    #[test]
    fn test_extend_property_node() {
        let mut graph =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        let new_prop = json!({"name":"jack"});

        let properties = vec![(0u32, new_prop)].into_iter();
        graph.extend_node_property(properties).unwrap();

        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_extend_property_edge() {
        let mut graph =
            TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

        let new_prop = json!({"length":"15"});

        let properties = vec![((0u32, 1u32), new_prop)].into_iter();
        graph.extend_edge_property(properties).unwrap();
        let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), edge_property);
    }

    #[test]
    fn test_open_existing_db() {
        {
            let mut graph0 =
                TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }

        let graph1 =
            TikvProperty::open(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false, true).unwrap();

        assert_eq!(
            graph1.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    #[test]
    fn test_open_writable_db() {
        {
            let mut graph0 =
                TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }
        let mut graph1 =
            TikvProperty::open(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false, false).unwrap();
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
            let mut graph0 =
                TikvProperty::new(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false).unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }

        let mut graph1 =
            TikvProperty::open(Config::new(vec![PD_SERVER_ADDR.to_owned()]), false, true).unwrap();
        assert_eq!(
            graph1.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );

        let err = graph1
            .insert_node_property(1u32, json!({"name": "tom"}))
            .is_err();
        assert_eq!(err, true);
    }
}
