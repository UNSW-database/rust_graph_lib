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
pub mod cached_property;
pub mod fake_property;
//pub mod filter;
//pub mod property_parser;
//pub mod result_parser;
//pub mod rocks_property;
pub mod tikv_property;
//pub mod sled_property;

pub use crate::property::cached_property::CachedProperty;
pub use crate::property::fake_property::FakeProperty;
//pub use crate::property::filter::PropertyCache;
//pub use crate::property::property_parser::parse_property;
//pub use crate::property::property_parser::parse_property_tree;
//pub use crate::property::property_parser::ExpressionCache;
//pub use crate::property::result_parser::{parse_result_blueprint, NodeElement, ResultBlueprint};
//pub use crate::property::rocks_property::RocksProperty;
//pub use property::sled_property::SledProperty;

use crate::generic::IdType;
pub use crate::generic::Iter;
use serde_json::Value as JsonValue;

pub trait PropertyGraph<Id: IdType> {
    fn get_node_property(
        &self,
        id: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError>;
    fn get_edge_property(
        &self,
        src: Id,
        dst: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, PropertyError>;
    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>, PropertyError>;
    fn get_edge_property_all(&self, src: Id, dst: Id) -> Result<Option<JsonValue>, PropertyError>;

    fn insert_node_property(
        &mut self,
        id: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError>;
    fn insert_edge_property(
        &mut self,
        src: Id,
        dst: Id,
        prop: JsonValue,
    ) -> Result<Option<JsonValue>, PropertyError>;

    fn extend_node_property<I: IntoIterator<Item = (Id, JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError>;
    fn extend_edge_property<I: IntoIterator<Item = ((Id, Id), JsonValue)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError>;

    fn insert_node_raw(
        &mut self,
        id: Id,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError>;
    fn insert_edge_raw(
        &mut self,
        src: Id,
        dst: Id,
        prop: Vec<u8>,
    ) -> Result<Option<JsonValue>, PropertyError>;

    fn extend_node_raw<I: IntoIterator<Item = (Id, Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError>;
    fn extend_edge_raw<I: IntoIterator<Item = ((Id, Id), Vec<u8>)>>(
        &mut self,
        props: I,
    ) -> Result<(), PropertyError>;

    fn scan_node_property_all(&self) -> Iter<Result<(Id, JsonValue), PropertyError>>;

    fn scan_edge_property_all(&self) -> Iter<Result<((Id, Id), JsonValue), PropertyError>>;
}

#[derive(Debug)]
pub enum PropertyError {
    //SledError(sled::Error<()>),
    ModifyReadOnlyError,
    //RocksError(rocksdb::Error),
    TiKVError(tikv_client::Error),
    BincodeError(std::boxed::Box<bincode::ErrorKind>),
    JsonError(serde_json::Error),
    CborError(serde_cbor::error::Error),
    DBNotFoundError,
    LruZeroCapacity,

    JsonObjectFieldError,
    BooleanExpressionError,
    StringExpressionError,
    NumberExpressionError,
    EdgeNotFoundError,
    NodeNotFoundError,
    UnknownError,
    CrossComparisonError,
}

//impl From<sled::Error<()>> for PropertyError {
//    fn from(error: sled::Error<()>) -> Self {
//        PropertyError::SledError(error)
//    }
//}

//impl From<rocksdb::Error> for PropertyError {
//    fn from(error: rocksdb::Error) -> Self {
//        PropertyError::RocksError(error)
//    }
//}

impl From<std::boxed::Box<bincode::ErrorKind>> for PropertyError {
    fn from(error: std::boxed::Box<bincode::ErrorKind>) -> Self {
        PropertyError::BincodeError(error)
    }
}

impl From<serde_json::Error> for PropertyError {
    fn from(error: serde_json::Error) -> Self {
        PropertyError::JsonError(error)
    }
}

impl From<serde_cbor::error::Error> for PropertyError {
    fn from(error: serde_cbor::error::Error) -> Self {
        PropertyError::CborError(error)
    }
}

impl From<tikv_client::Error> for PropertyError {
    fn from(error: tikv_client::Error) -> Self {
        PropertyError::TiKVError(error)
    }
}

impl From<()> for PropertyError {
    fn from(_error: ()) -> Self {
        PropertyError::UnknownError
    }
}
