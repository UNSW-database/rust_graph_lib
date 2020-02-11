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
#[macro_use]
extern crate log;
#[macro_use]
extern crate serde_derive;
//#[macro_use]
//extern crate cdrs_helpers_derive;

pub mod algorithm;
pub mod generic;
pub mod graph_gen;
pub mod graph_impl;
pub mod io;
pub mod map;
pub mod prelude;

pub use crate::graph_impl::{
    DiGraphMap, DiStaticGraph, GraphMap, StaticGraph, UnGraphMap, UnStaticGraph,
};

pub static VERSION: &str = env!("CARGO_PKG_VERSION");
pub static NAME: &str = env!("CARGO_PKG_NAME");
