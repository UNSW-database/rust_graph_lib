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
use std::path::Path;
use std::string::ToString;

use sled::{Tree, ConfigBuilder};
use json::JsonValue;

use generic::{DefaultId, IdType};
use property::PropertyGraph;

pub struct SledProperty{
    node_property: Tree,
    edge_property: Tree,
    is_directed: bool,
}

impl SledProperty {
    pub fn new(is_directed: bool,store_path: &Path) -> Self {
        SledProperty {
            node_property: Tree::start_default(store_path).unwrap(),
            edge_property: Tree::start_default(store_path).unwrap(),
            is_directed,
        }
    }

    pub fn with_data <Id: IdType + ToString, S: BuildHasher>(store_path: &Path, node_property: HashMap<Id, JsonValue, S>,
                     edge_property: HashMap<(Id, Id), JsonValue, S>, is_directed: bool) -> Self {
        let config = ConfigBuilder::default()
            .path(store_path.to_owned())
            .build();

        let node_tree = Tree::start(config.clone()).unwrap();
        let edge_tree = Tree::start(config.clone()).unwrap();

        for (id, names) in node_property.iter(){
            let id_str = id.to_string();
            let id_bytes = id_str.as_bytes();
            let names_str = names.dump();
            let names_bytes = names_str.as_bytes();
            node_tree.set(id_bytes, names_bytes.to_vec());// may be error
        }

        for (edge, names) in edge_property.iter(){
            let id = vec![edge.0.to_string(), edge.1.to_string()];
            let id_str = id.join(",");
            let id_bytes = id_str.as_bytes();
            let names_str = names.dump();
            let names_bytes = names_str.as_bytes();
            edge_tree.set(id_bytes, names_bytes.to_vec()); // may be error
        }

        SledProperty {
            node_property: node_tree,
            edge_property: edge_tree,
            is_directed,
        }
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

impl<Id: IdType + ToString> PropertyGraph<Id> for SledProperty {
    #[inline]
    fn get_node_property(&self, id: Id, names: Vec<String>) -> Result<Option<JsonValue>, ()> {
        let id_str = id.to_string();
        let id_bytes = id_str.as_bytes();
        match self.node_property.get(&id_bytes) {
            Ok(_value) => {
                match _value{
                    Some(value_bytes) => {
                        let value = String::from_utf8(value_bytes.to_vec()).unwrap();
                        let value_parsed = json::parse(&value).unwrap();
                        let mut result = JsonValue::new_object();
                        for name in names {
                            if value_parsed.has_key(&name) {
                                result[name] = value_parsed[&name].clone();
                            }
                        }
                        Ok(Some(result))
                    }
                    None => Ok(None),
                }
            },
            Err(_) => {panic!("There is some error!")},

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

        let id = vec![src.to_string(), dst.to_string()];
        let id_str = id.join(",");
        let id_bytes = id_str.as_bytes();
        match self.edge_property.get(&id_bytes) {
            Ok(_value) => {
                match _value{
                    Some(value_bytes) => {
                        let value = String::from_utf8(value_bytes.to_vec()).unwrap();
                        let value_parsed = json::parse(&value).unwrap();
                        let mut result = JsonValue::new_object();
                        for name in names {
                            if value_parsed.has_key(&name) {
                                result[name] = value_parsed[&name].clone();
                            }
                        }
                        Ok(Some(result))
                    }
                    None => Ok(None),
                }
            },
            Err(_) => {panic!("There is some error!")},

        }
    }

    #[inline]
    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>, ()> {
        let id_str = id.to_string();
        let id_bytes = id_str.as_bytes();
        match self.node_property.get(&id_bytes) {
            Ok(_value) => {
                match _value{
                    Some(value_bytes) => {
                        let value = String::from_utf8(value_bytes.to_vec()).unwrap();
                        let value_parsed = json::parse(&value).unwrap();
                        Ok(Some(value_parsed.clone()))
                    }
                    None => Ok(None),
                }
            },
            Err(_) => {panic!("There is some error!")},

        }
    }

    #[inline]
    fn get_edge_property_all(&self, mut src: Id, mut dst: Id) -> Result<Option<JsonValue>, ()> {
        if !self.is_directed {
            self.swap_edge(&mut src, &mut dst);
        }

        let id = vec![src.to_string(), dst.to_string()];
        let id_str = id.join(",");
        let id_bytes = id_str.as_bytes();
        match self.edge_property.get(&id_bytes) {
            Ok(_value) => {
                match _value{
                    Some(value_bytes) => {
                        let value = String::from_utf8(value_bytes.to_vec()).unwrap();
                        let value_parsed = json::parse(&value).unwrap();
                        Ok(Some(value_parsed.clone()))
                    }
                    None => Ok(None),
                }
            },
            Err(_) => {panic!("There is some error!")},

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

        let path = Path::new("/home/wangran/RustProjects/PatMatch/PropertyGraph/test_data/undirected");
        let graph = SledProperty::with_data(path,node_property, edge_property, false);
        assert_eq!(
            graph.get_node_property(0u32, vec!["age".to_owned()]).unwrap(),
            Some(object!("age"=>12))
        );
        assert_eq!(
            graph
                .get_node_property(0u32, vec!["age".to_owned(), "name".to_owned()])
                .unwrap(),
            Some(object!("age"=>12,"name"=>"John"))
        );
        assert_eq!(
            graph
                .get_node_property(1u32, vec!["is_member".to_owned()])
                .unwrap(),
            Some(object!("is_member"=>false))
        );
        assert_eq!(
            graph
                .get_node_property(1u32, vec!["is_member".to_owned(), "scores".to_owned()])
                .unwrap(),
            Some(object!("is_member"=>false,"scores"=>array![10,10,9]))
        );
        assert_eq!(
            graph.get_node_property(2u32, vec!["age".to_owned()]).unwrap(),
            None
        );
        assert_eq!(
            graph
                .get_node_property(0u32, vec!["age".to_owned(), "gender".to_owned()])
                .unwrap(),
            Some(object!{
            "age"=>12
                 })
        );
        assert_eq!(
            graph.get_node_property_all(0u32).unwrap(),
            Some(object!(
            "name"=>"John",
            "age"=>12,
            "is_member"=>true,
            "scores"=>array![9,8,10],
            ))
        );

        let edge_property = graph
            .get_edge_property(0u32, 1u32, vec!["friend_since".to_owned()])
            .unwrap()
            .unwrap();
        assert!(edge_property["friend_since"] == "2018-11-15");
        assert_eq!(edge_property.len(), 1);
    }

    #[test]
    fn test_directed() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(0u32, object!());
        node_property.insert(1, object!());
        edge_property.insert((0, 1), object!());
        let path = Path::new("/home/wangran/RustProjects/PatMatch/PropertyGraph/test_data/directed");
        let graph = SledProperty::with_data(path,node_property, edge_property, true);

        assert_eq!(graph.get_edge_property_all(1u32, 0u32), Ok(None));
    }
}