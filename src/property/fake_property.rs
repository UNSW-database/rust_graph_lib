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

use std::path::Path;

use serde::de::DeserializeOwned;
use serde::Serialize;
use serde_json::Value as JsonValue;

use generic::{IdType, Iter};
use property::{PropertyError, PropertyGraph};

#[derive(Debug, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct FakeProperty;

impl FakeProperty {
    pub fn new<P: AsRef<Path>>(_node_path: P, _edge_path: P, _is_directed: bool) -> Self {
        FakeProperty
    }
}

impl<Id: IdType + Serialize + DeserializeOwned> PropertyGraph<Id> for FakeProperty {
    #[inline]
    fn get_node_property(
        &self,
        _id: Id,
        _names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        Ok(None)
    }

    #[inline]
    fn get_edge_property(
        &self,
        mut _src: Id,
        mut _dst: Id,
        _names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        Ok(None)
    }

    #[inline]
    fn get_node_property_all(&self, _id: Id) -> Result<Option<JsonValue>, PropertyError> {
        Ok(None)
    }

    #[inline]
    fn get_edge_property_all(
        &self,
        mut _src: Id,
        mut _dst: Id,
    ) -> Result<Option<JsonValue>, PropertyError> {
        Ok(None)
    }

    fn insert_node_property(
        &mut self,
        _id: Id,
        _prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        Ok(None)
    }

    fn insert_edge_property(
        &mut self,
        _src: Id,
        _dst: Id,
        _prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError> {
        Ok(None)
    }

    fn extend_node_property<I: IntoIterator<Item = (Id, JsonValue)>>(
        &mut self,
        _props: I,
    ) -> Result<(), PropertyError> {
        Ok(())
    }

    fn extend_edge_property<I: IntoIterator<Item = ((Id, Id), JsonValue)>>(
        &mut self,
        _props: I,
    ) -> Result<(), PropertyError> {
        Ok(())
    }

    fn insert_node_raw(
        &mut self,
        _id: Id,
        _prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        Ok(None)
    }

    fn insert_edge_raw(
        &mut self,
        mut _src: Id,
        mut _dst: Id,
        _prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError> {
        Ok(None)
    }

    fn extend_node_raw<I: IntoIterator<Item = (Id, Vec<u8>)>>(
        &mut self,
        _props: I,
    ) -> Result<(), PropertyError> {
        Ok(())
    }

    fn extend_edge_raw<I: IntoIterator<Item = ((Id, Id), Vec<u8>)>>(
        &mut self,
        _props: I,
    ) -> Result<(), PropertyError> {
        Ok(())
    }

    fn scan_node_property_all(&self) -> Iter<Result<(Id, JsonValue), PropertyError>> {
        Iter::empty()
    }

    fn scan_edge_property_all(&self) -> Iter<Result<((Id, Id), JsonValue), PropertyError>> {
        Iter::empty()
    }
}
