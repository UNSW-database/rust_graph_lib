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
use generic::IdType;
use io::mmap::dump;
use std::fs::File;
use std::io::Result;
use std::ops::Add;

/// With the node indexed from 0 .. num_nodes - 1, we can maintain the edges in a compact way,
/// using `offset` and `edges`, in which `offset[node]` maintain the start index of the given
/// node's neighbors in `edges`. Thus, the node's neighbors is maintained in:
/// `edges[offsets[node]]` (included) to `edges[offsets[node+1]]` (excluded),
///
/// *Note*: The edges must be sorted according to the starting node, that is,
/// The sub-vector from `edges[offsets[node]]` (included) to `edges[offsets[node + 1]]` (excluded)
/// for any `node` should be sorted.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct EdgeVec<Id: IdType, L: IdType = Id> {
    offsets: Vec<usize>,
    edges: Vec<Id>,
    labels: Option<Vec<L>>,
}

pub trait EdgeVecTrait<Id: IdType, L: IdType> {
    fn get_offsets(&self) -> &[usize];
    fn get_edges(&self) -> &[Id];
    fn get_labels(&self) -> &[L];

    #[inline]
    fn num_nodes(&self) -> usize {
        self.get_offsets().len() - 1
    }

    #[inline]
    fn num_edges(&self) -> usize {
        self.get_edges().len()
    }

    #[inline]
    fn neighbors(&self, node: Id) -> &[Id] {
        assert!(self.has_node(node));
        let start = self.get_offsets()[node.id()].id();
        let end = self.get_offsets()[node.id() + 1].id();

        &self.get_edges()[start..end]
    }

    #[inline]
    fn degree(&self, node: Id) -> usize {
        assert!(self.has_node(node));
        let start = self.get_offsets()[node.id()].id();
        let end = self.get_offsets()[node.id() + 1].id();

        end - start
    }

    #[inline]
    fn has_node(&self, node: Id) -> bool {
        node.id() < self.num_nodes()
    }

    #[inline]
    fn find_edge_index(&self, start: Id, target: Id) -> Option<usize> {
        if !(self.has_node(start) && self.has_node(target)) {
            None
        } else {
            let neighbors = self.neighbors(start);
            let found = neighbors.binary_search(&target);
            match found {
                Err(_) => None,
                Ok(idx) => Some(self.get_offsets()[start.id()].id() + idx),
            }
        }
    }

    #[inline]
    fn find_edge_label_id(&self, start: Id, target: Id) -> Option<&L> {
        let labels = self.get_labels();

        if labels.is_empty() {
            return None;
        }

        let idx_opt = self.find_edge_index(start, target);
        match idx_opt {
            None => None,
            Some(idx) => labels.get(idx),
        }
    }

    #[inline]
    fn has_edge(&self, start: Id, target: Id) -> bool {
        self.find_edge_index(start, target).is_some()
    }
}

impl<Id: IdType, L: IdType> EdgeVec<Id, L> {
    pub fn new(offsets: Vec<usize>, edges: Vec<Id>) -> Self {
        EdgeVec {
            offsets,
            edges,
            labels: None,
        }
    }

    pub fn with_labels(offsets: Vec<usize>, edges: Vec<Id>, labels: Vec<L>) -> Self {
        if edges.len() != labels.len() {
            panic!(
                "Unequal length: there are {} edges, but {} labels",
                edges.len(),
                labels.len()
            );
        }
        EdgeVec {
            offsets,
            edges,
            labels: Some(labels),
        }
    }

    pub fn from_raw(offsets: Vec<usize>, edges: Vec<Id>, labels: Option<Vec<L>>) -> Self {
        match labels {
            Some(labels) => EdgeVec::with_labels(offsets, edges, labels),
            None => EdgeVec::new(offsets, edges),
        }
    }

    pub fn remove_labels(&mut self) {
        self.labels = None;
    }

    pub fn clear(&mut self) {
        self.offsets.clear();
        self.edges.clear();
        if let Some(ref mut labels) = self.labels {
            labels.clear();
        }
    }

    pub fn shrink_to_fit(&mut self) {
        self.offsets.shrink_to_fit();
        self.edges.shrink_to_fit();
        if let Some(ref mut labels) = self.labels {
            labels.shrink_to_fit();
        }
    }

    /// Dump self to bytearray in order to be deserialised as `EdgeVecMmap`.
    pub fn dump_mmap(&self, prefix: &str) -> Result<()> {
        let offsets_file = format!("{}.offsets", prefix);
        let edges_file = format!("{}.edges", prefix);
        let labels_file = format!("{}.labels", prefix);

        unsafe {
            dump(self.get_offsets(), File::create(offsets_file)?)?;
            dump(self.get_edges(), File::create(edges_file)?)?;

            if !self.get_labels().is_empty() {
                dump(self.get_labels(), File::create(labels_file)?)
            } else {
                Ok(())
            }
        }
    }
}

impl<Id: IdType, L: IdType> EdgeVecTrait<Id, L> for EdgeVec<Id, L> {
    #[inline]
    fn get_offsets(&self) -> &[usize] {
        &self.offsets
    }

    #[inline]
    fn get_edges(&self) -> &[Id] {
        &self.edges
    }

    #[inline]
    fn get_labels(&self) -> &[L] {
        match self.labels {
            Some(ref labels) => &labels[..],
            None => &[],
        }
    }
}

impl<Id: IdType, L: IdType> Default for EdgeVec<Id, L> {
    fn default() -> Self {
        EdgeVec::new(Vec::new(), Vec::new())
    }
}

/// Merge two label vectors. Let the smaller vector be S and larger one be T.
/// The result will be S concatenating the residual part of T after remove the prior
/// elements of length as S.
///
/// # Panic
///
/// One label has value but the other does not.
///
fn _merge_labels<L: IdType>(labels1: Option<Vec<L>>, labels2: Option<Vec<L>>) -> Option<Vec<L>> {
    match (labels1, labels2) {
        (None, None) => None,
        (Some(_l1), Some(_l2)) => {
            let (smaller, larger) = if _l1.len() <= _l2.len() {
                (_l1, _l2)
            } else {
                (_l2, _l1)
            };

            let len = smaller.len();
            let mut result = Vec::with_capacity(larger.len());
            result.extend(
                smaller.into_iter()
            );
            result.extend(
                larger.into_iter().skip(len)
            );

            Some(result)
        },
        _ => panic!("Could not merge `Some` labels with `None`.")
    }
}

fn _merge_offset(offsets1: Vec<usize>, offsets2: Vec<usize>) -> Vec<usize> {
    let (off1, off2) = if offsets1.len() <= offsets2.len() {
        (offsets1, offsets2)
    } else {
        (offsets2, offsets1)
    };
    assert!(!off1.is_empty());
    let len = off1.len();
    let last = off1[len - 1];
    let mut offsets = Vec::with_capacity(off2.len());
    for (e1, e2) in off1.into_iter().zip(off2.iter()) {
        offsets.push(e1 + e2);
    }

    offsets.extend(off2.into_iter().skip(len).map(|x| x + last));

    offsets
}

impl<Id: IdType, L: IdType> Add for EdgeVec<Id, L> {
    type Output = EdgeVec<Id, L>;

    fn add(self, other: EdgeVec<Id, L>) -> Self::Output {
        unimplemented!()
    }
}
