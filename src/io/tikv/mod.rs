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

pub mod tikv_loader;
use crate::generic::IdType;
use serde::{Deserialize, Serialize};
use serde_cbor::{to_value, Value};
use std::hash::Hash;

pub fn serialize_node_item<'a, Id: IdType, NL: Hash + Eq + 'static>(
    mut x: (Id, Option<NL>, Value),
) -> (Vec<u8>, Vec<u8>)
where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> NL: Deserialize<'de> + Serialize,
{
    let mut default_map = BTreeMap::new();
    let props_map = x.2.as_object_mut().unwrap_or(&mut default_map);
    if x.1.is_some() {
        let label = to_value(x.1.unwrap());
        if label.is_ok() {
            return (
                bincode::serialize(&x.0).unwrap(),
                serde_cbor::to_vec(&(Some(label.unwrap()), props_map)).unwrap(),
            );
        }
    }
    (
        bincode::serialize(&(x.0)).unwrap(),
        serde_cbor::to_vec(&(Option::<Value>::None, props_map)).unwrap(),
    )
}

pub fn serialize_edge_item<'a, Id: IdType, EL: Hash + Eq + 'static>(
    mut x: (Id, Id, Option<EL>, Value),
) -> (Vec<u8>, Vec<u8>)
where
    for<'de> Id: Deserialize<'de> + Serialize,
    for<'de> EL: Deserialize<'de> + Serialize,
{
    let mut default_map = BTreeMap::new();
    let props_map = x.3.as_object_mut().unwrap_or(&mut default_map);
    if let Some(l) = x.2 {
        let label = to_value(l);
        if label.is_ok() {
            return (
                bincode::serialize(&(x.0, x.1)).unwrap(),
                serde_cbor::to_vec(&(Some(label.unwrap()), props_map)).unwrap(),
            );
        }
    }
    (
        bincode::serialize(&(x.0, x.1)).unwrap(),
        serde_cbor::to_vec(&(Option::<Value>::None, props_map)).unwrap(),
    )
}
