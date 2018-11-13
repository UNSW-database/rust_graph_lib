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
use std::fs::metadata;

use generic::IdType;
use graph_impl::static_graph::EdgeVecTrait;
use io::mmap::TypedMemoryMap;

/// A mmap version of `EdgeVec`.
pub struct EdgeVecMmap<Id: IdType, L: IdType> {
    offsets: TypedMemoryMap<usize>,
    edges: TypedMemoryMap<Id>,
    labels: Option<TypedMemoryMap<L>>,
}

impl<Id: IdType, L: IdType> EdgeVecMmap<Id, L> {
    pub fn new(prefix: &str) -> Self {
        let offsets_file = format!("{}.offsets", prefix);
        let edges_file = format!("{}.edges", prefix);
        let labels_file = format!("{}.labels", prefix);

        if metadata(&labels_file).is_ok() {
            EdgeVecMmap {
                offsets: TypedMemoryMap::new(&offsets_file),
                edges: TypedMemoryMap::new(&edges_file),
                labels: Some(TypedMemoryMap::new(&labels_file)),
            }
        } else {
            EdgeVecMmap {
                offsets: TypedMemoryMap::new(&offsets_file),
                edges: TypedMemoryMap::new(&edges_file),
                labels: None,
            }
        }
    }
}

impl<Id: IdType, L: IdType> EdgeVecTrait<Id, L> for EdgeVecMmap<Id, L> {
    #[inline(always)]
    fn get_offsets(&self) -> &[usize] {
        &self.offsets[..]
    }

    #[inline(always)]
    fn get_edges(&self) -> &[Id] {
        &self.edges[..]
    }

    #[inline(always)]
    fn get_labels(&self) -> &[L] {
        match self.labels {
            Some(ref labels) => &labels[..],
            None => &[],
        }
    }
}
