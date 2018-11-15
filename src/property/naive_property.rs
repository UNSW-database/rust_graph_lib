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
use std::hash::BuildHasher;
use std::mem::swap;

use fnv::{FnvBuildHasher, FnvHashMap};
use json::JsonValue;
use serde;

use generic::{DefaultId, IdType};
use io::serde::{Deserialize, Serialize};
use property::PropertyGraph;

#[derive(Debug, PartialEq, Eq, Clone)]
pub struct NaiveProperty<Id: IdType = DefaultId> {
    pub(crate) node_property: FnvHashMap<Id, JsonValue>,
    pub(crate) edge_property: FnvHashMap<(Id, Id), JsonValue>,
    pub(crate) is_directed: bool,
}

impl<Id: IdType> Serialize for NaiveProperty<Id> where Id: serde::Serialize {}

impl<Id: IdType> Deserialize for NaiveProperty<Id> where Id: for<'de> serde::Deserialize<'de> {}

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
    fn has_node(&self, id: Id) -> bool {
        self.node_property.contains_key(&id)
    }

    fn has_edge(&self, mut src: Id, mut dst: Id) -> bool {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        self.edge_property.contains_key(&(src, dst))
    }

    fn get_node_property(&self, id: Id, names: Vec<String>) -> Option<JsonValue> {
        match self.node_property.get(&id) {
            Some(value) => {
                let mut result = JsonValue::new_object();
                for name in names {
                    if !value.has_key(&name) {
                        return None;
                    }
                    result[name] = value[&name].clone();
                }
                Some(result)
            }
            None => None,
        }
    }

    fn get_edge_property(&self, mut src: Id, mut dst: Id, names: Vec<String>) -> Option<JsonValue> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        match self.edge_property.get(&(src, dst)) {
            Some(value) => {
                let mut result = JsonValue::new_object();
                for name in names {
                    if !value.has_key(&name) {
                        return None;
                    }
                    result[name] = value[&name].clone();
                }
                Some(result)
            }
            None => None,
        }
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

        assert!(graph.has_node(0));
        assert!(graph.has_node(1));
        assert!(!graph.has_node(2));

        assert_eq!(
            graph.get_node_property(0, vec!["age".to_owned()]),
            Some(object!("age"=>12))
        );
        assert_eq!(
            graph.get_node_property(0, vec!["age".to_owned(), "name".to_owned()]),
            Some(object!("age"=>12,"name"=>"John"))
        );
        assert_eq!(
            graph.get_node_property(1, vec!["is_member".to_owned()]),
            Some(object!("is_member"=>false))
        );
        assert_eq!(
            graph.get_node_property(1, vec!["is_member".to_owned(), "scores".to_owned()]),
            Some(object!("is_member"=>false,"scores"=>array![10,10,9]))
        );
        assert_eq!(graph.get_node_property(2, vec!["age".to_owned()]), None);
        assert_eq!(
            graph.get_node_property(0, vec!["age".to_owned(), "gender".to_owned()]),
            None
        );

        assert!(graph.has_edge(0, 1));
        assert!(graph.has_edge(1, 0));

        let edge_property = graph
            .get_edge_property(0, 1, vec!["friend_since".to_owned()])
            .unwrap();
        assert!(edge_property["friend_since"] == "2018-11-15");
        assert_eq!(edge_property.len(), 1);
    }

    #[test]
    fn test_directed() {
        let mut node_property = FnvHashMap::default();
        let mut edge_property = FnvHashMap::default();

        node_property.insert(
            0u32,
            object!(
            "name"=>"John",
            ),
        );

        node_property.insert(
            1,
            object!(
            "name"=>"Marry",
            ),
        );

        edge_property.insert(
            (0, 1),
            object!(
            "friend_since"=>"2018-11-15",
            ),
        );

        let graph = NaiveProperty::with_data(node_property, edge_property, true);

        assert!(graph.has_edge(0, 1));
        assert!(!graph.has_edge(1, 0));
    }

}
