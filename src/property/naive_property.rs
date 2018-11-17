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
use std::collections::HashMap;
use std::fmt;
use std::hash::BuildHasher;
use std::mem::swap;

use fnv::{FnvBuildHasher, FnvHashMap};
use json::{parse, stringify, JsonValue};
use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::ser::{Serialize, Serializer};

use generic::{DefaultId, IdType};
use io::serde;
use property::PropertyGraph;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NaiveProperty<Id: IdType = DefaultId> {
    node_property: FnvHashMap<Id, JsonValue>,
    edge_property: FnvHashMap<(Id, Id), JsonValue>,
    is_directed: bool,
}

impl<Id: IdType> serde::Serialize for NaiveProperty<Id> where Id: Serialize {}

impl<Id: IdType> serde::Deserialize for NaiveProperty<Id> where Id: for<'de> Deserialize<'de> {}

impl<Id: IdType> NaiveProperty<Id> {
    pub fn new(is_directed: bool) -> Self {
        NaiveProperty {
            node_property: FnvHashMap::default(),
            edge_property: FnvHashMap::default(),
            is_directed,
        }
    }

    pub fn with_capacity(num_of_nodes: usize, num_of_edges: usize, is_directed: bool) -> Self {
        NaiveProperty {
            node_property: HashMap::with_capacity_and_hasher(
                num_of_nodes,
                FnvBuildHasher::default(),
            ),
            edge_property: HashMap::with_capacity_and_hasher(
                num_of_edges,
                FnvBuildHasher::default(),
            ),
            is_directed,
        }
    }

    pub fn with_data<S: BuildHasher>(
        node_property: HashMap<Id, JsonValue, S>,
        edge_property: HashMap<(Id, Id), JsonValue, S>,
        is_directed: bool,
    ) -> Self {
        NaiveProperty {
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

impl<Id: IdType> PropertyGraph<Id> for NaiveProperty<Id> {
    #[inline]
    fn get_node_property(&self, id: Id, names: Vec<String>) -> Result<Option<JsonValue>, ()> {
        match self.node_property.get(&id) {
            Some(value) => {
                let mut result = JsonValue::new_object();
                for name in names {
                    if !value.has_key(&name) {
                        return Ok(None);
                    }
                    result[name] = value[&name].clone();
                }
                Ok(Some(result))
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
    ) -> Result<Option<JsonValue>, ()> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        match self.edge_property.get(&(src, dst)) {
            Some(value) => {
                let mut result = JsonValue::new_object();
                for name in names {
                    if !value.has_key(&name) {
                        return Ok(None);
                    }
                    result[name] = value[&name].clone();
                }
                Ok(Some(result))
            }
            None => Ok(None),
        }
    }

    #[inline]
    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>, ()> {
        match self.node_property.get(&id) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }

    #[inline]
    fn get_edge_property_all(&self, mut src: Id, mut dst: Id) -> Result<Option<JsonValue>, ()> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        match self.edge_property.get(&(src, dst)) {
            Some(value) => Ok(Some(value.clone())),
            None => Ok(None),
        }
    }
}

struct SerdeJsonValue {
    pub json: JsonValue,
}

impl SerdeJsonValue {
    pub fn new(json: &JsonValue) -> Self {
        SerdeJsonValue { json: json.clone() }
    }

    pub fn unwrap(self) -> JsonValue {
        self.json
    }
}

impl Serialize for SerdeJsonValue {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        serializer.serialize_str(&stringify(self.json.clone()))
    }
}

struct SerdeJsonValueVisitor;

impl<'de> Visitor<'de> for SerdeJsonValueVisitor {
    type Value = SerdeJsonValue;

    fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.write_str("a JSON string")
    }

    fn visit_str<E>(self, valve: &str) -> Result<Self::Value, E>
    where
        E: Error,
    {
        match parse(valve) {
            Ok(json) => Ok(SerdeJsonValue { json }),
            Err(e) => Err(E::custom(format!("{:?}", e))),
        }
    }
}

impl<'de> Deserialize<'de> for SerdeJsonValue {
    fn deserialize<D>(deserializer: D) -> Result<SerdeJsonValue, D::Error>
    where
        D: Deserializer<'de>,
    {
        deserializer.deserialize_str(SerdeJsonValueVisitor)
    }
}

#[derive(Serialize, Deserialize)]
struct SerdeNaiveProperty<Id: IdType> {
    node_property: Vec<(Id, SerdeJsonValue)>,
    edge_property: Vec<((Id, Id), SerdeJsonValue)>,
    is_directed: bool,
}

impl<Id: IdType> SerdeNaiveProperty<Id> {
    pub fn new(property: &NaiveProperty<Id>) -> Self {
        SerdeNaiveProperty {
            node_property: property
                .node_property
                .iter()
                .map(|(i, j)| (*i, SerdeJsonValue::new(j)))
                .collect(),
            edge_property: property
                .edge_property
                .iter()
                .map(|(i, j)| (*i, SerdeJsonValue::new(j)))
                .collect(),
            is_directed: property.is_directed,
        }
    }

    pub fn unwrap(self) -> NaiveProperty<Id> {
        NaiveProperty {
            node_property: self
                .node_property
                .into_iter()
                .map(|(i, j)| (i, j.unwrap()))
                .collect(),
            edge_property: self
                .edge_property
                .into_iter()
                .map(|(i, j)| (i, j.unwrap()))
                .collect(),
            is_directed: self.is_directed,
        }
    }
}

impl<Id: IdType> Serialize for NaiveProperty<Id>
where
    Id: Serialize,
{
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let property = SerdeNaiveProperty::new(&self);
        property.serialize(serializer)
    }
}

impl<'de, Id: IdType> Deserialize<'de> for NaiveProperty<Id>
where
    Id: Deserialize<'de>,
{
    fn deserialize<D>(deserializer: D) -> Result<NaiveProperty<Id>, D::Error>
    where
        D: Deserializer<'de>,
    {
        let property = SerdeNaiveProperty::deserialize(deserializer)?;
        Ok(property.unwrap())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use json::{array, object};

    #[test]
    fn test_undirected() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(
            0u32,
            object!(
            "name"=>"John",
            "age"=>12,
            "is_member"=>true,
            "scores"=>array![9,8,10],
            ),
        );

        node_property.insert(
            1,
            object!(
            "name"=>"Marry",
            "age"=>13,
            "is_member"=>false,
            "scores"=>array![10,10,9],
            ),
        );

        edge_property.insert(
            (0, 1),
            object!(
            "friend_since"=>"2018-11-15",
            ),
        );

        let graph = NaiveProperty::with_data(node_property, edge_property, false);

        assert_eq!(
            graph.get_node_property(0, vec!["age".to_owned()]).unwrap(),
            Some(object!("age"=>12))
        );
        assert_eq!(
            graph
                .get_node_property(0, vec!["age".to_owned(), "name".to_owned()])
                .unwrap(),
            Some(object!("age"=>12,"name"=>"John"))
        );
        assert_eq!(
            graph
                .get_node_property(1, vec!["is_member".to_owned()])
                .unwrap(),
            Some(object!("is_member"=>false))
        );
        assert_eq!(
            graph
                .get_node_property(1, vec!["is_member".to_owned(), "scores".to_owned()])
                .unwrap(),
            Some(object!("is_member"=>false,"scores"=>array![10,10,9]))
        );
        assert_eq!(
            graph.get_node_property(2, vec!["age".to_owned()]).unwrap(),
            None
        );
        assert_eq!(
            graph
                .get_node_property(0, vec!["age".to_owned(), "gender".to_owned()])
                .unwrap(),
            None
        );
        assert_eq!(
            graph.get_node_property_all(0).unwrap(),
            Some(object!(
            "name"=>"John",
            "age"=>12,
            "is_member"=>true,
            "scores"=>array![9,8,10],
            ))
        );

        let edge_property = graph
            .get_edge_property(0, 1, vec!["friend_since".to_owned()])
            .unwrap()
            .unwrap();
        assert!(edge_property["friend_since"] == "2018-11-15");
        assert_eq!(edge_property.len(), 1);
    }

    #[test]
    fn test_directed() {
        let mut node_property = FnvHashMap::default();
        let mut edge_property = FnvHashMap::default();

        node_property.insert(0u32, object!());
        node_property.insert(1, object!());
        edge_property.insert((0, 1), object!());
        let graph = NaiveProperty::with_data(node_property, edge_property, true);

        assert_eq!(graph.get_edge_property_all(1, 0), Ok(None));
    }

}
