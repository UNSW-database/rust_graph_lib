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

use generic::{DefaultId, IdType};
use property::filter::{EdgeCache, HashEdgeCache, HashNodeCache, NodeCache, PropertyResult};
use property::{PropertyGraph, SledProperty};

use serde_json::Value as JsonValue;


pub struct PropertyCache<
    Id: IdType = DefaultId,
    PG: PropertyGraph<Id> = SledProperty,
    NC: NodeCache<Id> = HashNodeCache<Id>,
    EC: EdgeCache<Id> = HashEdgeCache<Id>,
> {
    property_graph: Option<Arc<PG>>,
    node_cache: NC,
    edge_cache: EC,
    phantom: PhantomData<Id>,
}

impl<Id: IdType, PG: PropertyGraph<Id>> PropertyCache<Id, PG> {
    pub fn new_default(property_graph: Option<Arc<PG>>) -> Self {
        PropertyCache {
            property_graph,
            node_cache: HashNodeCache::new(),
            edge_cache: HashEdgeCache::new(),
            phantom: PhantomData,
        }
    }
}

impl<Id: IdType, PG: PropertyGraph<Id>, NC: NodeCache<Id>, EC: EdgeCache<Id>>
    PropertyCache<Id, PG, NC, EC>
{
    pub fn new(property_graph: Option<Arc<PG>>, node_cache: NC, edge_cache: EC) -> Self {
        PropertyCache {
            property_graph,
            node_cache,
            edge_cache,
            phantom: PhantomData,
        }
    }

    pub fn pre_fetch<NI: IntoIterator<Item = Id>, EI: IntoIterator<Item = (Id, Id)>>(
        &mut self,
        nodes: NI,
        edges: EI,
    ) -> PropertyResult<()> {
        if self.is_disabled() {
            panic!("Property Graph Disabled.")
        }
        let property_graph = self.property_graph.clone().unwrap();
        self.node_cache.pre_fetch(nodes, property_graph.as_ref())?;
        self.edge_cache.pre_fetch(edges, property_graph.as_ref())?;
        Ok(())
    }

    pub fn get_node_property(&self, id: Id) -> PropertyResult<JsonValue> {
        if self.is_disabled() {
            panic!("Property Graph Disabled.")
        }
        self.node_cache.get(id)
    }

    pub fn get_edge_property(&self, src: Id, dst: Id) -> PropertyResult<JsonValue> {
        if self.is_disabled() {
            panic!("Property Graph Disabled.")
        }
        self.edge_cache.get(src, dst)
    }

    pub fn is_disabled(&self) -> bool {
        self.property_graph.is_none()
    }
}

#[cfg(test)]
mod test {
    extern crate tempdir;

    use super::*;
    use property::filter::{HashEdgeCache, HashNodeCache};
    use property::SledProperty;
    use serde_json::json;
    use std::collections::HashMap;

    #[test]
    fn test_all_node_edge_property() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(0u32, json!({"age": 5}));
        node_property.insert(1, json!({"age": 10}));
        node_property.insert(2, json!({"age": 15}));
        edge_property.insert((0u32, 1u32), json!({"length": 7}));
        edge_property.insert((1, 2), json!({"length": 8}));
        edge_property.insert((2, 0), json!({"length": 9}));

        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let graph = SledProperty::with_data(
            node_path,
            edge_path,
            node_property.clone().into_iter(),
            edge_property.clone().into_iter(),
            true,
        )
        .unwrap();

        let mut property_cache = PropertyCache::new(
            Some(Arc::new(graph)),
            HashNodeCache::new(),
            HashEdgeCache::new(),
        );
        property_cache
            .pre_fetch(
                vec![0u32, 1u32, 2u32].into_iter(),
                vec![(0u32, 1u32), (1u32, 2u32), (2u32, 0u32)].into_iter(),
            )
            .unwrap();
        for (key, value) in node_property.into_iter() {
            assert!(property_cache.get_node_property(key).is_ok());
            assert_eq!(property_cache.get_node_property(key).unwrap(), value);
        }
        for (key, value) in edge_property.into_iter() {
            assert!(property_cache.get_edge_property(key.0, key.1).is_ok());
            assert_eq!(
                property_cache.get_edge_property(key.0, key.1).unwrap(),
                value
            );
        }
    }

    #[test]
    fn test_new_default_property_cache() {
        let mut node_property = HashMap::new();
        let mut edge_property = HashMap::new();

        node_property.insert(0u32, json!({"age": 5}));
        node_property.insert(1, json!({"age": 10}));
        node_property.insert(2, json!({"age": 15}));
        edge_property.insert((0u32, 1u32), json!({"length": 7}));
        edge_property.insert((1, 2), json!({"length": 8}));
        edge_property.insert((2, 0), json!({"length": 9}));

        let node = tempdir::TempDir::new("node").unwrap();
        let edge = tempdir::TempDir::new("edge").unwrap();

        let node_path = node.path();
        let edge_path = edge.path();

        let graph = SledProperty::with_data(
            node_path,
            edge_path,
            node_property.clone().into_iter(),
            edge_property.clone().into_iter(),
            true,
        )
        .unwrap();

        let mut property_cache = PropertyCache::new_default(Some(Arc::new(graph)));

        property_cache
            .pre_fetch(
                vec![0u32, 1u32, 2u32].into_iter(),
                vec![(0u32, 1u32), (1u32, 2u32), (2u32, 0u32)].into_iter(),
            )
            .unwrap();
        for (key, value) in node_property.into_iter() {
            assert!(property_cache.get_node_property(key).is_ok());
            assert_eq!(property_cache.get_node_property(key).unwrap(), value);
        }
        for (key, value) in edge_property.into_iter() {
            assert!(property_cache.get_edge_property(key.0, key.1).is_ok());
            assert_eq!(
                property_cache.get_edge_property(key.0, key.1).unwrap(),
                value
            );
        }
    }

    #[test]
    fn test_new_disabled_property_cache() {
        let property_cache: PropertyCache<u32, SledProperty> = PropertyCache::new_default(None);
        assert_eq!(property_cache.is_disabled(), true);
    }
}
