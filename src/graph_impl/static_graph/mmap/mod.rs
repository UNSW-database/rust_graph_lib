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
//! This file defines a mmap version of `StaticGraph`, so that when the graph is huge,
//! we can rely on mmap to save physical memory usage.

pub mod edge_vec_mmap;
pub mod graph_mmap;

pub use graph_impl::static_graph::mmap::edge_vec_mmap::EdgeVecMmap;
pub use graph_impl::static_graph::mmap::graph_mmap::StaticGraphMmap;
