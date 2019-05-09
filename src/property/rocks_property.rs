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
use std::path::Path;

use bincode;
use rocksdb::DB as Tree;
use rocksdb::{IteratorMode, Options, WriteBatch};
use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_cbor::{from_slice, to_vec};
use serde_json::to_value;
use serde_json::Value as JsonValue;

use generic::IdType;
pub use generic::Iter;
use property::{PropertyError, PropertyGraph};

pub struct RocksProperty {
    node_property: Tree,
    edge_property: Tree,
    is_directed: bool,
    read_only: bool,
}

impl RocksProperty {
    pub fn new<P: AsRef<Path> + Clone>(
        node_path: P,
        edge_path: P,
        is_directed: bool,
    ) -> Result<Self, PropertyError> {
        Tree::destroy(&Options::default(), node_path.clone())?;
        Tree::destroy(&Options::default(), edge_path.clone())?;

        let mut opts = Options::default();
        opts.create_if_missing(true);

        let node_tree = Tree::open(&opts, node_path)?;
        let edge_tree = Tree::open(&opts, edge_path)?;

        Ok(RocksProperty {
            node_property: node_tree,
            edge_property: edge_tree,
            is_directed,
            read_only: false,
        })
    }

    pub fn open<P: AsRef<Path>>(
        node_path: P,
        edge_path: P,
        is_directed: bool,
        read_only: bool,
    ) -> Result<Self, PropertyError> {
        let node_tree = Tree::open_default(node_path)?;
        let edge_tree = Tree::open_default(edge_path)?;

        Ok(RocksProperty {
            node_property: node_tree,
            edge_property: edge_tree,
            is_directed,
            read_only,
        })
    }

    pub fn flush(&self) -> Result<(), PropertyError> {
        if self.read_only {
            panic!("Trying to modify a read-only db.");
        }

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

impl<Id: IdType + Serialize + DeserializeOwned> PropertyGraph<Id> for RocksProperty {
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
        self.node_property.put(id_bytes, prop)?;
        self.node_property.flush()?;
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

        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        let id_bytes = bincode::serialize(&(src, dst))?;
        let value = self.get_edge_property_all(src, dst)?;
        self.edge_property.put(id_bytes, prop)?;
        self.edge_property.flush()?;
        Ok(value)
    }

    fn extend_node_raw<I: IntoIterator<Item = (Id, Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        let mut batch = WriteBatch::default();
        for (id, prop) in props {
            let id_bytes = bincode::serialize(&id)?;
            batch.put(id_bytes, prop)?;
        }

        self.node_property.write(batch)?;
        self.node_property.flush()?;
        Ok(())
    }

    fn extend_edge_raw<I: IntoIterator<Item = ((Id, Id), Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        if self.read_only {
            return Err(PropertyError::ModifyReadOnlyError);
        }

        let mut batch = WriteBatch::default();
        for (id, prop) in props {
            let (mut src, mut dst) = id;
            if !self.is_directed {
                self.swap_edge(&mut src, &mut dst);
            }

            let id_bytes = bincode::serialize(&(src, dst))?;
            batch.put(id_bytes, prop)?;
        }

        self.edge_property.write(batch)?;
        self.edge_property.flush()?;
        Ok(())
    }

    fn scan_node_property_all(&self) -> Result<Iter<(Id, JsonValue)>, PropertyError> {
        Ok(Iter::new(Box::new(
            self.node_property
                .iterator(IteratorMode::Start)
                .map(|(id_bytes, value_bytes)| {
                    let id: Id = bincode::deserialize(&id_bytes).unwrap();
                    let value_parsed: JsonValue = from_slice(&value_bytes).unwrap();

                    (id, value_parsed)
                }),
        )))
    }

    fn scan_edge_property_all(&self) -> Result<Iter<((Id, Id), JsonValue)>, PropertyError> {
        unimplemented!()
        //        let iterator: Iterator<Item = ((Id, Id), JsonValue)> = self
        //            .edge_property
        //            .iterator(IteratorMode::Start)
        //            .map(|x| {
        //                (
        //                    bincode::deserialize(&x.0).unwrap(),
        //                    from_slice(&x.1).unwrap(),
        //                )
        //            })
        //            .into();
        //        let boxed_iter = Box::new(iterator);
        //        let result = Iter::new(boxed_iter);
        //
        //        return Ok(result);
        //        let mut result = Vec::new();
        //        for (id_bytes, value_bytes) in self.edge_property.iterator(IteratorMode::Start) {
        //            let id: (Id, Id) = bincode::deserialize(&id_bytes)?;
        //            let value_parsed: JsonValue = from_slice(&value_bytes)?;
        //            result.push((id, value_parsed))
        //        }
        //        Ok(Iter::new(Box::new(result.into_iter())))
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use super::*;
    use serde_json::json;

    #[test]
    fn test_insert_raw_node() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

        let new_prop = json!({"name":"jack"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_node_raw(0u32, raw_prop).unwrap();
        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_insert_raw_edge() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

        let new_prop = json!({"length":"15"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_edge_raw(0u32, 1u32, raw_prop).unwrap();
        let node_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), node_property);
    }

    #[test]
    fn test_insert_property_node() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

        let new_prop = json!({"name":"jack"});

        graph.insert_node_property(0u32, new_prop).unwrap();
        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_insert_property_edge() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

        let new_prop = json!({"length":"15"});

        graph.insert_edge_property(0u32, 1u32, new_prop).unwrap();
        let node_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), node_property);
    }

    #[test]
    fn test_extend_raw_node() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

        let new_prop = json!({"name":"jack"});
        let raw_prop = to_vec(&new_prop).unwrap();
        let raw_properties = vec![(0u32, raw_prop)].into_iter();
        graph.extend_node_raw(raw_properties).unwrap();

        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_extend_raw_edge() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

        let new_prop = json!({"length":"15"});
        let raw_prop = to_vec(&new_prop).unwrap();
        let raw_properties = vec![((0u32, 1u32), raw_prop)].into_iter();
        graph.extend_edge_raw(raw_properties).unwrap();
        let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), edge_property);
    }

    #[test]
    fn test_extend_property_node() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

        let new_prop = json!({"name":"jack"});

        let properties = vec![(0u32, new_prop)].into_iter();
        graph.extend_node_property(properties).unwrap();

        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_extend_property_edge() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph = RocksProperty::new(node_path, edge_path, false).unwrap();

        let new_prop = json!({"length":"15"});

        let properties = vec![((0u32, 1u32), new_prop)].into_iter();
        graph.extend_edge_property(properties).unwrap();
        let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), edge_property);
    }

    #[test]
    fn test_open_existing_db() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        {
            let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }

        let graph1 = RocksProperty::open(node_path, edge_path, false, false).unwrap();
        assert_eq!(
            graph1.get_node_property_all(0u32).unwrap(),
            Some(json!({"name": "jack"}))
        );
    }

    #[test]
    fn test_open_writable_db() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        {
            let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }
        let mut graph1 = RocksProperty::open(node_path, edge_path, false, false).unwrap();
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
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        {
            let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();

            graph0
                .insert_node_property(0u32, json!({"name": "jack"}))
                .unwrap();

            assert_eq!(
                graph0.get_node_property_all(0u32).unwrap(),
                Some(json!({"name": "jack"}))
            );
        }

        let mut graph1 = RocksProperty::open(node_path, edge_path, false, true).unwrap();
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
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();

        graph0
            .insert_node_property(0u32, json!({"name": "jack"}))
            .unwrap();

        graph0
            .insert_node_property(1u32, json!({"name": "tom"}))
            .unwrap();

        let mut iter = graph0.scan_node_property_all().unwrap();
        assert_eq!((0u32, json!({"name": "jack"})), iter.next().unwrap());
        assert_eq!((1u32, json!({"name": "tom"})), iter.next().unwrap());
    }

    #[test]
    fn test_scan_edge_property() {
        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let mut graph0 = RocksProperty::new(node_path, edge_path, false).unwrap();

        graph0
            .insert_edge_property(0u32, 1u32, json!({"length": "5"}))
            .unwrap();

        graph0
            .insert_edge_property(1u32, 2u32, json!({"length": "10"}))
            .unwrap();

        let mut iter = graph0.scan_edge_property_all().unwrap();
        assert_eq!(((0u32, 1u32), json!({"length": "5"})), iter.next().unwrap());
        assert_eq!(
            ((1u32, 2u32), json!({"length": "10"})),
            iter.next().unwrap()
        );
    }
}
