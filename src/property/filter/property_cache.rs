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
use std::marker::PhantomData;
use std::sync::Arc;

use crate::generic::{DefaultId, IdType};
use crate::property::filter::{EdgeCache, LruEdgeCache, LruNodeCache, NodeCache, PropertyResult};
use crate::property::{PropertyError, PropertyGraph, RocksProperty};

use serde_json::json;
use serde_json::Value as JsonValue;
use std::marker::{Send, Sync};
use std::mem::swap;

pub struct PropertyCache<
    Id: IdType = DefaultId,
    PG: PropertyGraph<Id> = RocksProperty,
    NC: NodeCache<Id> = LruNodeCache,
    EC: EdgeCache<Id> = LruEdgeCache<Id>,
> {
    property_graph: Option<Arc<PG>>,
    node_cache: NC,
    edge_cache: EC,
    phantom: PhantomData<Id>,
    node_disabled: bool,
    edge_disabled: bool,
}

unsafe impl Sync for PropertyCache {}

unsafe impl Send for PropertyCache {}

impl<Id: IdType, PG: PropertyGraph<Id>> PropertyCache<Id, PG> {
    pub fn new(
        property_graph: Option<Arc<PG>>,
        capacity: usize,
        node_disabled: bool,
        edge_disabled: bool,
    ) -> Self {
        PropertyCache {
            property_graph,
            node_cache: if node_disabled {
                LruNodeCache::default()
            } else {
                LruNodeCache::new(capacity)
            },
            edge_cache: if edge_disabled {
                LruEdgeCache::default()
            } else {
                LruEdgeCache::new(capacity)
            },
            phantom: PhantomData,
            node_disabled,
            edge_disabled,
        }
    }

    pub fn resize(&mut self, capacity: usize) {
        self.node_cache.resize(capacity);
        self.edge_cache.resize(capacity);
    }
}

impl<Id: IdType, PG: PropertyGraph<Id>, NC: NodeCache<Id>, EC: EdgeCache<Id>>
    PropertyCache<Id, PG, NC, EC>
{
    pub fn pre_fetch<NI: IntoIterator<Item = Id>, EI: IntoIterator<Item = (Id, Id)>>(
        &mut self,
        nodes: NI,
        edges: EI,
    ) -> PropertyResult<()> {
        if self.is_disabled() {
            panic!("Property Graph Disabled.")
        }
        let mut_node_cache = &mut self.node_cache;
        let mut_edge_cache = &mut self.edge_cache;
        let property_graph = self.property_graph.clone().unwrap();
        let node_disabled = self.node_disabled;
        let edge_disabled = self.edge_disabled;

        if !node_disabled {
            for node in nodes.into_iter() {
                let mut value = json!(null);
                if let Some(result) = property_graph.get_node_property_all(node)? {
                    value = result;
                }
                *mut_node_cache.get_mut(node)? = value;
            }
        }

        if !edge_disabled {
            for edge in edges.into_iter() {
                let (mut src, mut dst) = edge;
                let mut value = json!(null);
                if let Some(result) = property_graph.get_edge_property_all(src, dst)? {
                    value = result;
                }
                if src > dst {
                    swap(&mut src, &mut dst);
                }
                *mut_edge_cache.get_mut(src, dst)? = value;
            }
        }

        Ok(())
    }

    pub fn get_node_property(&mut self, id: Id) -> PropertyResult<&JsonValue> {
        if self.is_disabled() {
            panic!("Property Graph Disabled.")
        }
        let property_graph = self.property_graph.clone().unwrap();
        let value = self.node_cache.get_mut(id)?;

        if *value == json!(null) {
            if let Some(result) = property_graph.get_node_property_all(id)? {
                *value = result;
                Ok(value)
            } else {
                Err(PropertyError::NodeNotFoundError)
            }
        } else {
            Ok(value)
        }
    }

    pub fn get_edge_property(&mut self, mut src: Id, mut dst: Id) -> PropertyResult<&JsonValue> {
        if self.is_disabled() {
            panic!("Property Graph Disabled.")
        }
        if src > dst {
            swap(&mut src, &mut dst);
        }

        let property_graph = self.property_graph.clone().unwrap();
        let value = self.edge_cache.get_mut(src, dst)?;

        if *value == json!(null) {
            if let Some(result) = property_graph.get_edge_property_all(src, dst)? {
                *value = result;
                Ok(value)
            } else {
                Err(PropertyError::EdgeNotFoundError)
            }
        } else {
            Ok(value)
        }
    }

    pub fn is_disabled(&self) -> bool {
        self.property_graph.is_none()
    }

    pub fn is_node_disabled(&self) -> bool {
        self.node_disabled
    }

    pub fn is_edge_disabled(&self) -> bool {
        self.edge_disabled
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use super::*;
    use crate::property::RocksProperty as DefaultProperty;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_node_edge_property() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(5u32, json!({"age": 5}));
        node_property.insert(1, json!({"age": 10}));
        node_property.insert(2, json!({"age": 15}));
        edge_property.insert((5u32, 1u32), json!({"length": 7}));
        edge_property.insert((1, 2), json!({"length": 8}));
        edge_property.insert((2, 5), json!({"length": 9}));

        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let graph = DefaultProperty::with_data(
            node_path,
            edge_path,
            node_property.clone().into_iter(),
            edge_property.clone().into_iter(),
            true,
        )
        .unwrap();

        let mut property_cache = PropertyCache::new(Some(Arc::new(graph)), 6, false, false);
        property_cache
            .pre_fetch(
                vec![5u32, 1u32, 2u32].into_iter(),
                vec![(5u32, 1u32), (1u32, 2u32), (2u32, 5u32)].into_iter(),
            )
            .unwrap();
        for (key, value) in node_property.into_iter() {
            assert!(property_cache.get_node_property(key).is_ok());
            assert_eq!(*property_cache.get_node_property(key).unwrap(), value);
        }
        for (key, value) in edge_property.into_iter() {
            assert!(property_cache.get_edge_property(key.0, key.1).is_ok());
            assert_eq!(
                *property_cache.get_edge_property(key.0, key.1).unwrap(),
                value
            );
        }
    }

    #[test]
    fn test_new_disabled_property_cache() {
        let property_cache: PropertyCache<u32, DefaultProperty> =
            PropertyCache::new(None, 10, false, false);
        assert_eq!(property_cache.is_disabled(), true);
    }
}
