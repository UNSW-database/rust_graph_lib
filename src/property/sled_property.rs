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

use std::mem::swap;
use std::path::Path;

use bincode;
use serde::Serialize;
use serde_cbor::{from_slice, to_vec};
use serde_json::to_value;
use serde_json::Value as JsonValue;
use sled::ConfigBuilder;
use sled::Db as Tree;
use std::collections::HashMap;

use generic::IdType;
use property::{PropertyError, PropertyGraph};

pub struct SledProperty {
    node_property: Tree,
    edge_property: Tree,
    is_directed: bool,
}

impl SledProperty {
    pub fn new(
        node_path: &Path,
        edge_path: &Path,
        is_directed: bool,
    ) -> Result<Self, PropertyError> {
        Ok(SledProperty {
            node_property: Tree::start_default(node_path)?,
            edge_property: Tree::start_default(edge_path)?,
            is_directed,
        })
    }

    pub fn with_data<Id: IdType + Serialize, N, E>(
        node_path: &Path,
        edge_path: &Path,
        node_property: N,
        edge_property: E,
        is_directed: bool,
    ) -> Result<Self, PropertyError>
    where
        N: Iterator<Item = (Id, JsonValue)>,
        E: Iterator<Item = ((Id, Id), JsonValue)>,
    {
        let node_config = ConfigBuilder::default().path(node_path.to_owned()).build();
        let edge_config = ConfigBuilder::default().path(edge_path.to_owned()).build();

        let node_tree = Tree::start(node_config.clone())?;
        let edge_tree = Tree::start(edge_config.clone())?;
        for (id, names) in node_property {
            let id_bytes = bincode::serialize(&id)?;
            let names_bytes = to_vec(&names)?;
            node_tree.set(id_bytes, names_bytes)?;
        }

        for (edge, names) in edge_property {
            let id_bytes = bincode::serialize(&edge)?;
            let names_bytes = to_vec(&names)?;
            edge_tree.set(id_bytes, names_bytes)?;
        }

        Ok(SledProperty {
            node_property: node_tree,
            edge_property: edge_tree,
            is_directed,
        })
    }

    pub fn flush(&self) -> Result<(), PropertyError> {
        self.node_property.flush()?;
        self.edge_property.flush()?;

        Ok(())
    }

    #[inline(always)]
    pub fn is_directed(&self) -> bool {
        self.is_directed
    }

    #[inline(always)]
    fn swap_edge<Id: IdType>(&self, a: &mut Id, b: &mut Id) {
        if !self.is_directed && a > b {
            swap(a, b)
        }
    }
}

impl<Id: IdType + Serialize> PropertyGraph<Id> for SledProperty {
    #[inline]
    fn get_node_property(
        &self,
        id: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let id_bytes = bincode::serialize(&id)?;
        let _value = self.node_property.get(&id_bytes)?;
        match _value {
            Some(value_bytes) => {
                let value_parsed: JsonValue = from_slice(&value_bytes)?;
                let mut result = HashMap::<String, JsonValue>::new();
                for name in names {
                    if value_parsed.get(&name).is_some() {
                        result.insert(name.clone(), value_parsed[&name].clone());
                    }
                }
                Ok(Some(to_value(result)?))
            }
            None => Ok(None),
        }
    }

    #[inline]
    fn get_edge_property(
        &self,
        mut src: Id,
        mut dst: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        let id_bytes = bincode::serialize(&(src, dst))?;
        let _value = self.edge_property.get(&id_bytes)?;
        match _value {
            Some(value_bytes) => {
                let value_parsed: JsonValue = from_slice(&value_bytes)?;
                let mut result = HashMap::<String, JsonValue>::new();
                for name in names {
                    if value_parsed.get(&name).is_some() {
                        result.insert(name.clone(), value_parsed[&name].clone());
                    }
                }
                Ok(Some(to_value(result)?))
            }
            None => Ok(None),
        }
    }

    #[inline]
    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>, PropertyError> {
        let id_bytes = bincode::serialize(&id)?;
        let _value = self.node_property.get(&id_bytes)?;
        match _value {
            Some(value_bytes) => {
                let value_parsed: JsonValue = from_slice(&value_bytes)?;
                Ok(Some(value_parsed.clone()))
            }
            None => Ok(None),
        }
    }

    #[inline]
    fn get_edge_property_all(
        &self,
        mut src: Id,
        mut dst: Id,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        let id_bytes = bincode::serialize(&(src, dst))?;
        let _value = self.edge_property.get(&id_bytes)?;
        match _value {
            Some(value_bytes) => {
                let value_parsed: JsonValue = from_slice(&value_bytes)?;
                Ok(Some(value_parsed.clone()))
            }
            None => Ok(None),
        }
    }
    fn insert_node_property(
        &mut self,
        id: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let id_bytes = bincode::serialize(&id)?;
        let names_bytes = to_vec(&prop)?;
        let _value = self.node_property.set(id_bytes, names_bytes)?;
        self.node_property.flush()?;

        match _value {
            Some(value_bytes) => {
                let value_parsed: JsonValue = from_slice(&value_bytes)?;
                Ok(Some(value_parsed))
            }
            None => Ok(None),
        }
    }

    fn insert_edge_property(
        &mut self,
        mut src: Id,
        mut dst: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        let id_bytes = bincode::serialize(&(src, dst))?;
        let names_bytes = to_vec(&prop)?;
        let _value = self.edge_property.set(id_bytes, names_bytes)?;
        self.edge_property.flush()?;

        match _value {
            Some(value_bytes) => {
                let value_parsed: JsonValue = from_slice(&value_bytes)?;
                Ok(Some(value_parsed))
            }
            None => Ok(None),
        }
    }

    fn extend_node_property<I: IntoIterator<Item = (Id, JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        for (id, prop) in props {
            let id_bytes = bincode::serialize(&id)?;
            let names_bytes = to_vec(&prop)?;
            let _value = self.node_property.set(id_bytes, names_bytes)?;
        }
        self.node_property.flush()?;

        Ok(())
    }

    fn extend_edge_property<I: IntoIterator<Item = ((Id, Id), JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        for (id, prop) in props {
            let (mut src, mut dst) = id;
            if !self.is_directed {
                self.swap_edge(&mut src, &mut dst);
            }

            let id_bytes = bincode::serialize(&(src, dst))?;
            let names_bytes = to_vec(&prop)?;
            let _value = self.edge_property.set(id_bytes, names_bytes)?;
        }
        self.edge_property.flush()?;

        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_json::json;
    use std::collections::HashMap;
    extern crate tempdir;

    #[test]
    fn test_undirected() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(
            0u32,
            json!({
            "name":"John",
            "age":12,
            "is_member":true,
            "scores":[9,8,10],
            }),
        );

        node_property.insert(
            1,
            json!({
            "name":"Marry",
            "age":13,
            "is_member":false,
            "scores":[10,10,9],
            }),
        );

        edge_property.insert(
            (0, 1),
            json!({
            "friend_since":"2018-11-15",
            }),
        );

        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let graph = SledProperty::with_data(
            node_path,
            edge_path,
            node_property.into_iter(),
            edge_property.into_iter(),
            false,
        )
        .unwrap();
        assert_eq!(
            graph
                .get_node_property(0u32, vec!["age".to_owned()])
                .unwrap(),
            Some(json!({"age":12}))
        );
        assert_eq!(
            graph
                .get_node_property(0u32, vec!["age".to_owned(), "name".to_owned()])
                .unwrap(),
            Some(json!({"age":12,"name":"John"}))
        );
        assert_eq!(
            graph
                .get_node_property(1u32, vec!["is_member".to_owned()])
                .unwrap(),
            Some(json!({"is_member":false}))
        );
        assert_eq!(
            graph
                .get_node_property(1u32, vec!["is_member".to_owned(), "scores".to_owned()])
                .unwrap(),
            Some(json!({"is_member":false,"scores":[10,10,9]}))
        );
        assert_eq!(
            graph
                .get_node_property(2u32, vec!["age".to_owned()])
                .unwrap(),
            None
        );
        assert_eq!(
            graph
                .get_node_property(0u32, vec!["age".to_owned(), "gender".to_owned()])
                .unwrap(),
            Some(json! ({
            "age":12
                 }))
        );
        assert_eq!(
            graph.get_node_property_all(0u32).unwrap(),
            Some(json!({
            "name":"John",
            "age":12,
            "is_member":true,
            "scores":json!([9,8,10]),
            }))
        );

        let edge_property = graph
            .get_edge_property(0u32, 1u32, vec!["friend_since".to_owned()])
            .unwrap()
            .unwrap();
        assert!(edge_property["friend_since"] == "2018-11-15");
        assert_eq!(edge_property.as_object().unwrap().len(), 1);
    }

    #[test]
    fn test_directed() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(0u32, json!({}));
        node_property.insert(1, json!({}));
        edge_property.insert((0, 1), json!({}));

        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let graph = SledProperty::with_data(
            node_path,
            edge_path,
            node_property.into_iter(),
            edge_property.into_iter(),
            false,
        )
        .unwrap();
        let edge_property = graph.get_edge_property_all(1u32, 0u32).unwrap();
        assert_eq!(Some(json!({})), edge_property);
    }
}
