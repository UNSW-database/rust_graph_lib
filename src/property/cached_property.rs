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

use hashbrown::HashMap;

use serde_cbor::from_slice;
use serde_json::to_value;
use serde_json::Value as JsonValue;

use generic::{DefaultId, IdType};
use property::{PropertyError, PropertyGraph};

#[derive(Debug, PartialEq, Clone, Serialize, Deserialize)]
pub struct CachedProperty<Id: IdType = DefaultId> {
    node_property: HashMap<Id, JsonValue>,
    edge_property: HashMap<(Id, Id), JsonValue>,
    is_directed: bool,
}

impl<Id: IdType> CachedProperty<Id> {
    pub fn new(is_directed: bool) -> Self {
        CachedProperty {
            node_property: HashMap::new(),
            edge_property: HashMap::new(),
            is_directed,
        }
    }

    pub fn with_capacity(num_of_nodes: usize, num_of_edges: usize, is_directed: bool) -> Self {
        CachedProperty {
            node_property: HashMap::with_capacity(num_of_nodes),
            edge_property: HashMap::with_capacity(num_of_edges),
            is_directed,
        }
    }

    pub fn with_data<
        N: IntoIterator<Item = (Id, JsonValue)>,
        E: IntoIterator<Item = ((Id, Id), JsonValue)>,
    >(
        node_property: N,
        edge_property: E,
        is_directed: bool,
    ) -> Self {
        CachedProperty {
            node_property: node_property.into_iter().collect(),
            edge_property: edge_property.into_iter().collect(),
            is_directed,
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.node_property.shrink_to_fit();
        self.edge_property.shrink_to_fit();
    }

    #[inline(always)]
    pub fn is_directed(&self) -> bool {
        self.is_directed
    }

    #[inline(always)]
    fn swap_edge(&self, a: &mut Id, b: &mut Id) {
        if !self.is_directed && a > b {
            swap(a, b)
        }
    }
}

impl<Id: IdType> PropertyGraph<Id> for CachedProperty<Id> {
    #[inline]
    fn get_node_property(
        &self,
        id: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        match self.node_property.get(&id) {
            Some(value) => {
                let mut result = HashMap::<String, JsonValue>::new();
                for name in names {
                    if value.get(&name).is_some() {
                        result.insert(name.clone(), value[&name].clone());
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

        match self.edge_property.get(&(src, dst)) {
            Some(value) => {
                let mut result = HashMap::<String, JsonValue>::new();
                for name in names {
                    if value.get(&name).is_some() {
                        result.insert(name.clone(), value[&name].clone());
                    }
                }
                Ok(Some(to_value(result)?))
            }
            None => Ok(None),
        }
    }

    #[inline]
    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>, PropertyError> {
        match self.node_property.get(&id) {
            Some(value) => Ok(Some(value.clone())),
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

        match self.edge_property.get(&(src, dst)) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }
    fn insert_node_property(
        &mut self,
        id: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let value = self.node_property.insert(id, prop);
        Ok(value)
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

        let value = self.edge_property.insert((src, dst), prop);
        Ok(value)
    }

    fn extend_node_property<I: IntoIterator<Item = (Id, JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        Ok(self.node_property.extend(props))
    }

    fn extend_edge_property<I: IntoIterator<Item = ((Id, Id), JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        let is_directed = self.is_directed;
        let props = props.into_iter().map(|x| {
            let (mut src, mut dst) = x.0;
            let prop = x.1;

            if is_directed && src > dst {
                swap(&mut src, &mut dst);
            }
            ((src, dst), prop)
        });

        Ok(self.edge_property.extend(props))
    }

    fn insert_node_raw(
        &mut self,
        id: Id,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let value_parsed: JsonValue = from_slice(&prop)?;
        self.insert_node_property(id, value_parsed)
    }

    fn insert_edge_raw(
        &mut self,
        src: Id,
        dst: Id,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        let value_parsed: JsonValue = from_slice(&prop)?;
        self.insert_edge_property(src, dst, value_parsed)
    }

    fn extend_node_raw<I: IntoIterator<Item = (Id, Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        let props = props
            .into_iter()
            .map(|x| (x.0, from_slice(&(x.1)).unwrap()));
        self.extend_node_property(props)
    }

    fn extend_edge_raw<I: IntoIterator<Item = ((Id, Id), Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError> {
        let props = props.into_iter().map(|x| (x.0, from_slice(&x.1).unwrap()));
        self.extend_edge_property(props)
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use serde_cbor::to_vec;
    use serde_json::json;

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
            "scores":json!([9,8,10]),
            }),
        );

        node_property.insert(
            1,
            json!({
            "name":"Marry",
            "age":13,
            "is_member":false,
            "scores":json!([10,10,9]),
            }),
        );

        edge_property.insert(
            (0, 1),
            json!({
            "friend_since":"2018-11-15",
            }),
        );

        let graph = CachedProperty::with_data(node_property, edge_property, false);

        assert_eq!(
            graph.get_node_property(0, vec!["age".to_owned()]).unwrap(),
            Some(json!({"age":12}))
        );
        assert_eq!(
            graph
                .get_node_property(0, vec!["age".to_owned(), "name".to_owned()])
                .unwrap(),
            Some(json!({"age":12,"name":"John"}))
        );
        assert_eq!(
            graph
                .get_node_property(1, vec!["is_member".to_owned()])
                .unwrap(),
            Some(json!({"is_member":false}))
        );
        assert_eq!(
            graph
                .get_node_property(1, vec!["is_member".to_owned(), "scores".to_owned()])
                .unwrap(),
            Some(json!({"is_member":false,"scores":[10,10,9]}))
        );
        assert_eq!(
            graph.get_node_property(2, vec!["age".to_owned()]).unwrap(),
            None
        );
        assert_eq!(
            graph
                .get_node_property(0, vec!["age".to_owned(), "gender".to_owned()])
                .unwrap(),
            Some(json!({
            "age":12
                 }))
        );
        assert_eq!(
            graph.get_node_property_all(0).unwrap(),
            Some(json!({
            "name":"John",
            "age":12,
            "is_member":true,
            "scores":[9,8,10],
            }))
        );

        let edge_property = graph
            .get_edge_property(0, 1, vec!["friend_since".to_owned()])
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
        let graph = CachedProperty::with_data(node_property, edge_property, true);

        assert_eq!(graph.get_edge_property_all(1, 0).unwrap(), None);
    }

    #[test]
    fn test_insert_raw_node() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(0u32, json!({}));
        node_property.insert(1, json!({}));
        edge_property.insert((0, 1), json!({}));

        let mut graph = CachedProperty::with_data(node_property, edge_property, true);

        let new_prop = json!({"name":"jack"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_node_raw(0u32, raw_prop).unwrap();
        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_insert_raw_edge() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(0u32, json!({}));
        node_property.insert(1, json!({}));
        edge_property.insert((0, 1), json!({}));

        let mut graph = CachedProperty::with_data(node_property, edge_property, true);

        let new_prop = json!({"length":"5"});
        let raw_prop = to_vec(&new_prop).unwrap();

        graph.insert_edge_raw(0u32, 1u32, raw_prop).unwrap();
        let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"5"})), edge_property);
    }

    #[test]
    fn test_extend_raw_node() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(0u32, json!({}));
        node_property.insert(1, json!({}));
        edge_property.insert((0, 1), json!({}));

        let mut graph = CachedProperty::with_data(node_property, edge_property, true);

        let new_prop = json!({"name":"jack"});
        let raw_prop = to_vec(&new_prop).unwrap();

        let raw_properties = vec![(0u32, raw_prop)].into_iter();
        graph.extend_node_raw(raw_properties).unwrap();
        let node_property = graph.get_node_property_all(0u32).unwrap();

        assert_eq!(Some(json!({"name":"jack"})), node_property);
    }

    #[test]
    fn test_extend_raw_edge() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(0u32, json!({}));
        node_property.insert(1, json!({}));
        edge_property.insert((0, 1), json!({}));

        let mut graph = CachedProperty::with_data(node_property, edge_property, true);

        let new_prop = json!({"length":"15"});
        let raw_prop = to_vec(&new_prop).unwrap();

        let raw_properties = vec![((0u32, 1u32), raw_prop)].into_iter();
        graph.extend_edge_raw(raw_properties).unwrap();
        let edge_property = graph.get_edge_property_all(0u32, 1u32).unwrap();

        assert_eq!(Some(json!({"length":"15"})), edge_property);
    }
}
