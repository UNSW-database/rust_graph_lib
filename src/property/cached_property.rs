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
use std::fmt;
use std::hash::BuildHasher;
use std::mem::swap;

use hashbrown::HashMap;
use serde::de::{Deserialize, Deserializer, Error, Visitor};
use serde::ser::{Serialize, Serializer};
use serde_json::from_str;
use serde_json::json;
use serde_json::to_value;
use serde_json::Value as JsonValue;

use generic::{DefaultId, IdType};
use io::serde;
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

        let graph = CachedProperty::with_data(node_property, edge_property, false);

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
            Some(object! {
            "age"=>12
                 })
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
        let graph = CachedProperty::with_data(node_property, edge_property, true);

        assert_eq!(graph.get_edge_property_all(1, 0).unwrap(), None);
    }

}
