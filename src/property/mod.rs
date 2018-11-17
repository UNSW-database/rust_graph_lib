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
pub mod naive_property;
mod serde;

pub use property::naive_property::NaiveProperty;

use generic::IdType;
use json::JsonValue;

pub type Result<T> = ::std::result::Result<T, Error>;

#[derive(Debug, Clone, Eq, PartialEq)]
pub enum Error {
    Custom(String)
}

pub trait PropertyGraph<Id: IdType> {
    fn get_node_property(&self, id: Id, names: Vec<String>) -> Result<Option<JsonValue>>;
    fn get_edge_property(&self, src: Id, dst: Id, names: Vec<String>) -> Result<Option<JsonValue>>;
    fn get_node_property_all(&self, id: Id) -> Result<Option<JsonValue>>;
    fn get_edge_property_all(&self, src: Id, dst: Id) -> Result<Option<JsonValue>>;

    #[inline(always)]
    fn has_node(&self, id: Id) -> Result<bool> {
        Ok(self.get_node_property_all(id)?.is_some())
    }

    #[inline(always)]
    fn has_edge(&self, src: Id, dst: Id) -> Result<bool> {
        Ok(self.get_edge_property_all(src, dst)?.is_some())
    }
}
