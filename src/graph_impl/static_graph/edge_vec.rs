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
use crate::generic::{IdType, Iter};
use itertools::Itertools;
use std::ops::Add;

static BITS_U32: usize = 32;

/// To main the offset in the more compact form, instead of directly using `Vec<usize>`,
/// we introduce a two-level index, the base level records the base offset using `Vec<u32>`,
/// if an `u32` is overflow, we shift the overflow bits' information to the second level.
/// The second-level index is a `Vec<usize>`, where the `i-th` element maintains the offsets in
/// the base level that has the overflow bits represent the number `i`. For example, if we have
/// the second-level index as `vec![1000, 50000, 600]`, it means that the first 1000 elements
/// in the `base_level` index has offset bit `0` (no offset), while from 1000 ~ 50000, has offset
/// bit `1`, meaning there actual offset should be `1 << 32 | base_level[i]`.
#[derive(Debug, Hash, PartialEq, Eq, Clone, Serialize, Deserialize)]
pub struct OffsetIndex {
    /// The base-level index
    base_level: Vec<u32>,
    /// The second-level index
    second_level: Vec<usize>,
}

impl From<Vec<usize>> for OffsetIndex {
    fn from(offsets: Vec<usize>) -> Self {
        let mut offset_idx = Self::with_capacity(offsets.len());

        for offset in offsets {
            offset_idx.push(offset);
        }

        offset_idx
    }
}

impl OffsetIndex {
    pub fn new() -> Self {
        Self {
            base_level: Vec::new(),
            second_level: Vec::new(),
        }
    }

    pub fn with_capacity(capacity: usize) -> Self {
        Self {
            base_level: Vec::with_capacity(capacity),
            second_level: Vec::new(),
        }
    }

    pub fn len(&self) -> usize {
        self.base_level.len()
    }

    pub fn clear(&mut self) {
        self.base_level.clear();
        self.second_level.clear();
    }

    pub fn is_empty(&self) -> bool {
        self.base_level.is_empty()
    }

    pub fn index(&self, index: usize) -> usize {
        if index < self.len() {
            let mut oflow_bit = 0;
            for (i, &off) in self.second_level.iter().enumerate() {
                if index < off {
                    oflow_bit = i;
                    break;
                }
            }
            oflow_bit << BITS_U32 | self.base_level[index] as usize
        } else {
            panic!("index {} is overflowed", index)
        }
    }

    pub fn last(&self) -> Option<usize> {
        if self.is_empty() {
            None
        } else {
            Some(self.index(self.len() - 1))
        }
    }

    pub fn push(&mut self, offset: usize) {
        let u32_part = offset as u32;
        let oflow_part = offset >> BITS_U32;

        let last_off = self.last().unwrap_or(0);
        assert!(last_off <= offset);

        if self.is_empty() {
            self.second_level.push(0);
        } else {
            let last_off_oflow = self.second_level[last_off >> BITS_U32];

            while self.second_level.len() < oflow_part + 1 {
                self.second_level.push(last_off_oflow);
            }
        }

        self.second_level[oflow_part] += 1;
        self.base_level.push(u32_part);
    }

    pub fn shrink_to_fit(&mut self) {
        self.base_level.shrink_to_fit();
        self.second_level.shrink_to_fit();
    }

    fn extend<T: IntoIterator<Item = usize>>(&mut self, iter: T) {
        for elem in iter.into_iter() {
            self.push(elem);
        }
    }

    pub fn iter(&self) -> Iter<usize> {
        let len = self.len();
        Iter::new(Box::new((0..len).map(move |i| self.index(i))))
    }
}

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
    offsets: OffsetIndex,
    edges: Vec<Id>,
    labels: Option<Vec<L>>,
}

pub trait EdgeVecTrait<Id: IdType, L: IdType> {
    fn get_offsets(&self) -> &OffsetIndex;
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
        //        if !self.has_node(node) {
        //            error!("Node {:?} does not exist", node);
        //            return &[];
        //        }
        let start = self.get_offsets().index(node.id());
        let end = self.get_offsets().index(node.id() + 1);

        &self.get_edges()[start..end]
    }

    #[inline]
    fn degree(&self, node: Id) -> usize {
        assert!(self.has_node(node));
        //        if !self.has_node(node) {
        //            error!("Node {:?} does not exist", node);
        //            return 0;
        //        }
        let start = self.get_offsets().index(node.id());
        let end = self.get_offsets().index(node.id() + 1);

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
                Ok(idx) => Some(self.get_offsets().index(start.id()) + idx),
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
            offsets: offsets.into(),
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
            offsets: offsets.into(),
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

    pub fn from_raw_index(offsets: OffsetIndex, edges: Vec<Id>, labels: Option<Vec<L>>) -> Self {
        EdgeVec {
            offsets,
            edges,
            labels,
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
}

impl<Id: IdType, L: IdType> EdgeVecTrait<Id, L> for EdgeVec<Id, L> {
    #[inline]
    fn get_offsets(&self) -> &OffsetIndex {
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
        EdgeVec::new(vec![0], Vec::new())
    }
}

/// Add two `EdgeVec`s following the rules:
/// * The `edges` will be the merged vector, duplication will be removed.
/// * The `labels` if some, will be the merged vector. We assume that the label is the same
///   for two same edges (same `src` and `dst`) is the same, hence the label will be randomly
///   picked up in either `EdgeVec`. If they contain different labels, it will end with indefinite
///   results.
/// * The `offsets` will be of the length of the longer one, and reshifted according to the
///   merged `edges`.
///
/// # Panic
///
/// One `EdgeVec` has `Some(labels)`, but the other has `None`.
impl<Id: IdType, L: IdType> Add for EdgeVec<Id, L> {
    type Output = EdgeVec<Id, L>;

    fn add(self, other: EdgeVec<Id, L>) -> Self::Output {
        assert_eq!(self.labels.is_some(), other.labels.is_some());
        let (smaller, larger) = if self.offsets.len() <= other.offsets.len() {
            (self, other)
        } else {
            (other, self)
        };

        if smaller.offsets.is_empty() {
            return larger;
        }

        //        let len = smaller.edges.len() + larger.edges.len();
        let s_num_nodes = smaller.offsets.len() - 1;
        let num_nodes = larger.offsets.len() - 1;

        let mut edges = Vec::new(); //Vec::with_capacity(len);
        let mut labels = Vec::new(); //Vec::with_capacity(len);
        let mut offsets = OffsetIndex::new(); //Vec::with_capacity(num_nodes + 1);
        offsets.push(0);

        for node in 0..s_num_nodes {
            let (s1, e1) = (smaller.offsets.index(node), smaller.offsets.index(node + 1));
            let (s2, e2) = (larger.offsets.index(node), larger.offsets.index(node + 1));
            let mut num_nbrs = 0;

            if smaller.labels.is_none() {
                let merged_nbrs = smaller
                    .edges
                    .iter()
                    .skip(s1)
                    .take(e1 - s1)
                    .merge(larger.edges.iter().skip(s2).take(e2 - s2))
                    .dedup();

                for &nbr in merged_nbrs {
                    edges.push(nbr);
                    num_nbrs += 1;
                }
            } else {
                let merged_nbrs = smaller
                    .edges
                    .iter()
                    .skip(s1)
                    .take(e1 - s1)
                    .zip(
                        smaller
                            .labels
                            .as_ref()
                            .unwrap()
                            .iter()
                            .skip(s1)
                            .take(e1 - s1),
                    )
                    .merge(
                        larger.edges.iter().skip(s2).take(e2 - s2).zip(
                            larger
                                .labels
                                .as_ref()
                                .unwrap()
                                .iter()
                                .skip(s2)
                                .take(e2 - s2),
                        ),
                    )
                    .unique_by(|x| x.0);

                for (&nbr, &lab) in merged_nbrs {
                    edges.push(nbr);
                    labels.push(lab);
                    num_nbrs += 1;
                }
            }

            let offset = offsets.last().unwrap() + num_nbrs;
            offsets.push(offset);
        }

        if s_num_nodes < num_nodes {
            let last_off = larger.offsets.index(s_num_nodes);
            edges.extend(larger.edges.iter().skip(last_off));
            if larger.labels.is_some() {
                labels.extend(larger.labels.as_ref().unwrap().iter().skip(last_off));
            }

            let extra_off = offsets.last().unwrap() - larger.offsets.index(s_num_nodes);
            offsets.extend(
                larger
                    .offsets
                    .iter()
                    .skip(s_num_nodes + 1)
                    .map(|x| x + extra_off),
            );
        }

        EdgeVec {
            offsets,
            edges,
            labels: if smaller.labels.is_none() {
                None
            } else {
                Some(labels)
            },
        }
    }
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn test_offset_index() {
        let offset_index = OffsetIndex {
            base_level: vec![0, 1, 2, 3, 4, 5],
            second_level: vec![3, 5, 6],
        };

        let exp_offsets = vec![
            0_usize,
            1,
            2,
            1 << BITS_U32 | 3,
            1 << BITS_U32 | 4,
            2 << BITS_U32 | 5,
        ];
        let mut i = 0;
        while i < offset_index.len() {
            let offset = offset_index.index(i);
            assert_eq!(exp_offsets[i], offset);
            i += 1;
        }

        let mut offset_index = OffsetIndex::new();
        offset_index.push(0);
        offset_index.push(1);
        assert_eq!(offset_index.len(), 2);
        assert_eq!(offset_index.index(0), 0);
        assert_eq!(offset_index.index(1), 1);

        offset_index.push(1 << BITS_U32 | 3);
        assert_eq!(offset_index.last().unwrap(), 1 << BITS_U32 | 3);

        offset_index.push(5 << BITS_U32 | 4);
        assert_eq!(offset_index.last().unwrap(), 5 << BITS_U32 | 4);

        let offset_index = OffsetIndex::from(exp_offsets.clone());
        let mut i = 0;
        while i < offset_index.len() {
            let offset = offset_index.index(i);
            assert_eq!(exp_offsets[i], offset);
            i += 1;
        }
    }

    #[test]
    fn test_merge() {
        let edges1 = EdgeVec::<u32>::new(vec![0, 2, 4, 6, 8], vec![1_u32, 3, 0, 2, 1, 3, 0, 2]);

        let edges2 = EdgeVec::<u32>::new(vec![0, 1, 2, 3, 4], vec![2_u32, 3, 0, 1]);

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 3, 6, 9, 12].into());

        assert_eq!(edges.edges, vec![1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2]);

        assert!(edges.labels.is_none());
    }

    #[test]
    fn test_merge_with_label() {
        let edges1 = EdgeVec::<u32>::with_labels(
            vec![0, 2, 4, 6, 8],
            vec![1_u32, 3, 0, 2, 1, 3, 0, 2],
            vec![1, 3, 1, 3, 3, 5, 3, 5],
        );

        let edges2 = EdgeVec::<u32>::with_labels(
            vec![0, 1, 2, 3, 4],
            vec![2_u32, 3, 0, 1],
            vec![2, 4, 2, 4],
        );

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 3, 6, 9, 12].into());

        assert_eq!(edges.edges, vec![1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2]);

        assert_eq!(edges.labels, Some(vec![1, 2, 3, 1, 3, 4, 2, 3, 5, 3, 4, 5]));
    }

    #[test]
    fn test_merge_with_comm_edges() {
        let edges1 = EdgeVec::<u32>::with_labels(
            vec![0, 2, 4, 6, 8],
            vec![1_u32, 3, 0, 2, 1, 3, 0, 2],
            vec![1, 3, 1, 3, 3, 5, 3, 5],
        );

        let edges2 = EdgeVec::<u32>::with_labels(
            vec![0, 2, 5, 7, 8],
            vec![1_u32, 2, 0, 2, 3, 0, 1, 1],
            vec![1, 2, 1, 3, 4, 2, 3, 4],
        );

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 3, 6, 9, 12].into());

        assert_eq!(edges.edges, vec![1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2]);

        assert_eq!(edges.labels, Some(vec![1, 2, 3, 1, 3, 4, 2, 3, 5, 3, 4, 5]));
    }

    #[test]
    fn test_merge_with_more_nodes() {
        let edges1 = EdgeVec::<u32>::with_labels(
            vec![0, 2, 4, 6],
            vec![1_u32, 2, 0, 2, 0, 1],
            vec![1, 2, 1, 3, 2, 3],
        );

        let edges2 = EdgeVec::<u32>::with_labels(
            vec![0, 1, 2, 3, 6, 7],
            vec![3_u32, 3, 3, 0, 1, 2, 2],
            vec![3, 4, 5, 3, 4, 5, 6],
        );

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 3, 6, 9, 12, 13].into());

        assert_eq!(edges.edges, vec![1, 2, 3, 0, 2, 3, 0, 1, 3, 0, 1, 2, 2]);

        assert_eq!(
            edges.labels,
            Some(vec![1, 2, 3, 1, 3, 4, 2, 3, 5, 3, 4, 5, 6])
        );
    }

    #[test]
    fn test_merge_with_empty_edges() {
        let edges1 = EdgeVec::<u32>::new(vec![0, 0], vec![]);

        let edges2 = EdgeVec::<u32>::new(vec![0, 1, 2, 3, 6, 7], vec![3_u32, 3, 3, 0, 1, 2, 2]);

        let edges = edges1 + edges2;

        assert_eq!(edges.offsets, vec![0, 1, 2, 3, 6, 7].into());

        assert_eq!(edges.edges, vec![3_u32, 3, 3, 0, 1, 2, 2]);
    }

}
