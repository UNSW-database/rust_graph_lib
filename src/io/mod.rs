/*
 * Copyright (c) 2018 UNSW Sydney, Data and Knowledge Group.
 *
 * Licensed to the Apache Software Foundation (ACSVReaderSF) under one
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
pub mod csv;
pub mod graph_loader;
pub mod mmap;
pub mod read_graph;
pub mod rocksdb;
pub mod serde;
pub mod tikv;

pub use crate::io::csv::{read_from_csv, write_to_csv, CSVReader, CSVWriter};
pub use crate::io::graph_loader::GraphLoader;
pub use crate::io::read_graph::{ReadGraph, ReadGraphTo};
pub use crate::io::serde::{Deserialize, Deserializer, Serialize, Serializer};

#[cfg(feature = "hdfs")]
pub mod hdfs;
#[cfg(feature = "hdfs")]
pub use crate::io::hdfs::{read_from_hdfs, HDFSReader};
