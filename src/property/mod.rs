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
pub mod sled_property;
pub mod filter;

pub use property::cached_property::CachedProperty;
pub use property::sled_property::SledProperty;

use generic::IdType;
use json::JsonValue;

pub trait PropertyGraph<Id: IdType> {
    type Err;

    fn get_node_property(&self, id: Id, names: Vec<String>)
        -> Result<Option<JsonValue>, Self::Err>;
    fn get_edge_property(
        &self,
        src: Id,
        dst: Id,
        names: Vec<String>,
    ) -> Result<Option<JsonValue>, Self::Err>;
    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>, Self::Err>;
    fn get_edge_property_all(&self, src: Id, dst: Id) -> Result<Option<JsonValue>, Self::Err>;

    //    fn scan_node_property(&self,names: Vec<String>) -> Iter<Result<Option<JsonValue>, E>>;
    //    fn scan_edge_property(&self,names: Vec<String>) -> Iter<Result<Option<JsonValue>, E>>;
    //    fn scan_node_property_all(&self,names: Vec<String>) -> Iter<Result<Option<JsonValue>, E>>;
    //    fn scan_edge_property_all(&self,names: Vec<String>) -> Iter<Result<Option<JsonValue>, E>>;
}

#[derive(Debug)]
pub enum PropertyError{
    SledError(sled::Error<()>),
    BincodeError(std::boxed::Box<bincode::ErrorKind>),
    FromUtf8Error(std::string::FromUtf8Error),
    JsonError(json::Error),

    JsonObjectFieldError,
    BooleanExpressionError,
    StringExpressionError,
    NumberExpressionError,
    EdgeNotFoundError,
    NodeNotFoundError,
    UnknownError,
}

impl From<sled::Error<()>> for PropertyError{
    fn from(error: sled::Error<()>) -> Self {
        PropertyError::SledError(error)
    }
}
impl From<std::boxed::Box<bincode::ErrorKind>> for PropertyError{
    fn from(error: std::boxed::Box<bincode::ErrorKind>) -> Self {
        PropertyError::BincodeError(error)
    }
}

impl From<std::string::FromUtf8Error> for PropertyError{
    fn from(error: std::string::FromUtf8Error) -> Self {
        PropertyError::FromUtf8Error(error)
    }
}

impl From<json::Error> for PropertyError{
    fn from(error: json::Error) -> Self {
        PropertyError::JsonError(error)
    }
}

impl From<()> for PropertyError{
    fn from(_error: ()) -> Self {
        PropertyError::UnknownError
    }
}
