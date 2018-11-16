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
pub mod node;
pub mod relation;
pub mod scheme;

pub use io::ldbc::scheme::Scheme;

use generic::{GraphType, IdType};
use graph_impl::TypedGraphMap;
use seahash::SeaHasher;
use std::collections::HashMap;
use std::hash::BuildHasher;
use std::path::Path;

pub type SeaHashMap<K, V> = HashMap<K, V, SeaHashBuilder>;

pub fn read_ldbc_from_path<Id: IdType, Ty: GraphType, P: AsRef<Path>>(
    path: P,
) -> TypedGraphMap<Id, String, String, Ty> {
    self::scheme::Scheme::init().from_path(path).unwrap()
}

#[derive(Default)]
pub struct SeaHashBuilder;

impl BuildHasher for SeaHashBuilder {
    type Hasher = SeaHasher;
    fn build_hasher(&self) -> SeaHasher {
        SeaHasher::default()
    }
}
